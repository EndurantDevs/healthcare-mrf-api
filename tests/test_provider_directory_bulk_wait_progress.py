# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pending export observations preserve resource and work ownership."""

import asyncio
import datetime
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import live_progress
from tests.provider_directory_fhir_bulk_test_support import _Response
from tests.test_provider_directory_fhir_reviewed_serial_stop import _runner

importer = importlib.import_module("process.provider_directory_fhir")
NOW = datetime.datetime(2026, 8, 1, 12, tzinfo=datetime.UTC)
SOURCE = {"source_id": "directory-alpha", "api_base": "https://directory.example.test/fhir"}


@pytest.fixture
def deadline_transport(monkeypatch):
    clock_values = [NOW]
    monotonic_start, real_sleep = importer.time.monotonic(), asyncio.sleep

    async def advance(seconds):
        while seconds > 0:
            step = min(seconds, 10)
            clock_values[0] += datetime.timedelta(seconds=step)
            seconds -= step
            await real_sleep(0)
            await real_sleep(0)

    monkeypatch.setattr(importer, "_bulk_export_now_utc", lambda: clock_values[0])
    monkeypatch.setattr(importer.time, "monotonic", lambda: monotonic_start + (clock_values[0] - NOW).total_seconds())
    monkeypatch.setattr(asyncio, "sleep", advance)
    monkeypatch.setattr(importer, "_refresh_bulk_export_rows_written", AsyncMock())
    monkeypatch.setattr(importer, "_persist_bulk_export_next_poll_at", AsyncMock())
    return SimpleNamespace(
        clock_values=clock_values,
        advance=advance,
        source=dict(SOURCE),
        session=SimpleNamespace(get=Mock()),
        identity=SimpleNamespace(start_url=SOURCE["api_base"] + "/$export", resource_type="PractitionerRole"),
        checkpoint={"accepted_at": NOW - datetime.timedelta(seconds=40)},
        probe=AsyncMock(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase,delay", [("retry", 30), ("pacing", 30), ("read", 30), ("late", 30), ("retry", 7), ("read", 7)]
)
async def test_pending_deadline_bounds_real_status_transport(deadline_transport, phase, delay):
    case = deadline_transport
    ready = _Response(body=b'{"output":[]}')

    async def read():
        if phase == "read":
            await case.advance(delay)
        elif phase == "late":
            case.clock_values[0] += datetime.timedelta(seconds=delay)
        return ready.body

    ready.read = AsyncMock(side_effect=read)
    responses = [_Response(status=429, headers={"Retry-After": str(delay)}), ready] if phase == "retry" else [ready]
    case.session.get.side_effect = responses
    if phase == "pacing":
        case.source["_provider_directory_request_pacing_state"] = {
            "retry_after_monotonic": importer.time.monotonic() + delay
        }
    poll_result = await importer._poll_bulk_capability_refresh_request(
        case.session,
        case.source,
        case.identity,
        "/status/accepted",
        timeout=180,
        max_pending_seconds=60,
        runtime_probe=case.probe,
        checkpoint=case.checkpoint,
    )
    if delay > 20:
        assert poll_result == (None, importer.BULK_EXPORT_STATUS_DEADLINE_EXCEEDED, 1)
        assert case.session.get.call_count == (0 if phase == "pacing" else 1)
        if phase != "late":
            assert case.clock_values[0] == NOW + datetime.timedelta(seconds=20)
    else:
        assert poll_result == ({"output": []}, None, 1)
        assert case.session.get.call_count == (2 if phase == "retry" else 1)
    assert all(call.kwargs["timeout"].total <= 20 for call in case.session.get.call_args_list)
    assert case.checkpoint["accepted_at"] == NOW - datetime.timedelta(seconds=40)
    assert importer.bulk_parallel.state(importer, case.source)["requests"].active == 0
    importer._refresh_bulk_export_rows_written.assert_awaited_once_with(case.identity)
    importer._persist_bulk_export_next_poll_at.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_kind", ["task_cancel", "cancel", "control", "timeout", *sorted(importer.BULK_EXPORT_FENCING_ERRORS)]
)
async def test_pending_deadline_preserves_control_errors(monkeypatch, deadline_transport, failure_kind):
    case = deadline_transport
    failure = {
        "task_cancel": asyncio.CancelledError(),
        "cancel": importer.ImportCancelledError("cancelled"),
        "control": importer.ResourceImportControlPlaneFailure("control-failed"),
        "timeout": TimeoutError("upstream timeout"),
    }.get(failure_kind, RuntimeError(failure_kind))
    monkeypatch.setattr(importer, "_bulk_http_get_json", AsyncMock(side_effect=failure))
    with pytest.raises(type(failure)) as raised:
        await importer._poll_bulk_capability_refresh_request(
            case.session,
            case.source,
            case.identity,
            "/status/accepted",
            timeout=180,
            max_pending_seconds=60,
            runtime_probe=case.probe,
            checkpoint=case.checkpoint,
        )
    assert raised.value is failure
    importer._refresh_bulk_export_rows_written.assert_not_awaited()
    importer._persist_bulk_export_next_poll_at.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [True, False])
@pytest.mark.parametrize(
    "final_response", [(200, {}, {"output": []}, None), (500, {}, None, None), (202, {}, None, "network")]
)
async def test_only_actual_pending_responses_publish_wait_then_clear(monkeypatch, durable, final_response):
    monkeypatch.setattr(importer, "_bulk_export_now_utc", lambda: NOW)
    monkeypatch.setattr(importer, "_bulk_export_poll_settings", lambda: (3, 0))
    monkeypatch.setattr(importer.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(importer, "_bulk_http_get_json", AsyncMock(side_effect=[(202, {}, None, None), final_response]))
    callback = AsyncMock()
    poll_by_name = dict(resource_type="PractitionerRole", timeout=3)
    token = importer.fhir_response.BULK_EXPORT_WAIT_CALLBACK.set(callback)
    try:
        if durable:
            await importer._bulk_export_poll_manifest(
                object(), SOURCE, "https://directory.example.test/status", **poll_by_name
            )
        else:
            await importer._bulk_export_poll_outputs(
                object(), SOURCE, "https://directory.example.test/status", **poll_by_name
            )
    finally:
        importer.fhir_response.BULK_EXPORT_WAIT_CALLBACK.reset(token)
    assert callback.await_args_list[0].args == (
        {
            "resource": "PractitionerRole",
            "http_status": 202,
            "observed_at": NOW.isoformat(),
            "next_poll_at": NOW.isoformat(),
        },
    )
    assert callback.await_args_list[1].args == (None,)
    assert callback.await_count == 2


def test_wait_observation_is_not_row_progress():
    previous_by_field = {
        "run_id": "directory-wait-observation",
        "stage_ordinal": 3,
        "done": 0,
        "total": 1,
        "counters": {"resource_rows_written": 27},
        "progress_seq": 7,
        "event_seq": 11,
        "progressed_at": "2026-08-01T11:00:00Z",
    }
    incoming_by_field = {**previous_by_field, "detail": {"bulk_export_wait": {"http_status": 202}}}
    live_progress._sequence_progress(incoming_by_field, previous_by_field, now=NOW, succeeded=False)
    assert incoming_by_field["progress_seq"] == 7
    assert incoming_by_field["event_seq"] > 11
    assert incoming_by_field["progressed_at"] == previous_by_field["progressed_at"]
    assert incoming_by_field["counters"] == previous_by_field["counters"]


@pytest.mark.asyncio
async def test_wait_cursors_clear_independently_without_changing_resource_or_counts():
    runner = _runner(SOURCE, ["Practitioner", "PractitionerRole"])
    runner.active_group_by_key[1] = {"source_ids": [SOURCE["source_id"]]}
    await runner.activate(("Practitioner", "PractitionerRole"))
    for resource in ["Practitioner", "PractitionerRole"]:
        await runner.report_bulk_wait(resource, importer.fhir_response.bulk_export_wait_observation(resource, NOW, NOW))
    await runner.report_bulk_wait("Practitioner", None)
    group = runner.active_group_by_key[1]
    assert list(group["bulk_export_wait"]) == ["PractitionerRole"]
    assert group["current_resources"] == ["Practitioner", "PractitionerRole"]
    assert group["source_ids"] == [SOURCE["source_id"]]
    assert runner.count_by_resource == {"Practitioner": 0, "PractitionerRole": 0}


@pytest.mark.asyncio
async def test_parallel_fetch_scopes_keep_wait_ownership_separate(monkeypatch):
    resource_types = ("Practitioner", "PractitionerRole")
    runner = _runner(SOURCE, resource_types)
    runner.active_group_by_key[1] = {}
    await runner.activate(resource_types)
    observed_by_resource = {resource: asyncio.Event() for resource in resource_types}
    release_by_resource = {resource: asyncio.Event() for resource in resource_types}

    async def fetch_rows(resource, _source):
        callback = importer.fhir_response.BULK_EXPORT_WAIT_CALLBACK.get()
        assert callback is not None
        await callback(importer.fhir_response.bulk_export_wait_observation(resource, NOW, NOW))
        observed_by_resource[resource].set()
        await release_by_resource[resource].wait()

    monkeypatch.setattr(runner, "_fetch_rows", fetch_rows)
    tasks_by_resource = {
        resource: asyncio.create_task(runner.fetch(resource, SOURCE, report_start=False)) for resource in resource_types
    }
    try:
        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in observed_by_resource.values())), 1)
        group = runner.active_group_by_key[1]
        assert set(group["bulk_export_wait"]) == set(resource_types)
        assert importer.fhir_response.BULK_EXPORT_WAIT_CALLBACK.get() is None
        release_by_resource["Practitioner"].set()
        await asyncio.wait_for(tasks_by_resource["Practitioner"], 1)
        assert set(group["bulk_export_wait"]) == {"PractitionerRole"}
        tasks_by_resource["PractitionerRole"].cancel()
        with pytest.raises(asyncio.CancelledError):
            await tasks_by_resource["PractitionerRole"]
        assert group["bulk_export_wait"] == {}
        assert group["current_resources"] == list(resource_types)
        assert runner.count_by_resource == dict.fromkeys(resource_types, 0)
    finally:
        for task in tasks_by_resource.values():
            task.cancel()
        await asyncio.gather(*tasks_by_resource.values(), return_exceptions=True)
    assert importer.fhir_response.BULK_EXPORT_WAIT_CALLBACK.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_fails", [True, False])
@pytest.mark.parametrize("failure", [RuntimeError("transport"), asyncio.CancelledError()])
async def test_fetch_failure_or_cancellation_clears_wait(monkeypatch, failure, cleanup_fails):
    runner = _runner(SOURCE, ["Practitioner"])
    runner.active_group_by_key[1] = {}
    if cleanup_fails:
        runner.report_progress.side_effect = [None, None, RuntimeError("status cleanup")]

    async def fetch_rows(*_args):
        await runner.report_bulk_wait(
            "Practitioner", importer.fhir_response.bulk_export_wait_observation("Practitioner", NOW, NOW)
        )
        raise failure

    monkeypatch.setattr(runner, "_fetch_rows", fetch_rows)
    with pytest.raises(type(failure)) as raised:
        await runner.fetch("Practitioner", SOURCE, report_start=True)
    assert raised.value is failure
    assert runner.active_group_by_key[1]["bulk_export_wait"] == {}
    assert importer.fhir_response.BULK_EXPORT_WAIT_CALLBACK.get() is None


def test_normal_heartbeat_preserves_wait_detail_and_real_row_progress():
    previous_by_field = {
        "run_id": "directory-heartbeat-observation",
        "attempt_id": "attempt-a",
        "attempt_started_at": "2026-08-01T10:00:00Z",
        "importer": "provider-directory-fhir",
        "source": "import-live-progress",
        "status": "running",
        "unit": "steps",
        "done": 0,
        "total": 1,
        "pct": 0.0,
        "stage_ordinal": 3,
        "phase": "provider-directory importing resources",
        "message": "importing resources",
        "counters": {"resource_rows_written": 27},
        "progress_seq": 7,
        "event_seq": 11,
        "progressed_at": "2026-08-01T11:00:00Z",
        "detail": {
            "active_source_groups": [
                {
                    "sample_source_id": SOURCE["source_id"],
                    "current_resource": "PractitionerRole",
                    "bulk_export_wait": {
                        "PractitionerRole": importer.fhir_response.bulk_export_wait_observation(
                            "PractitionerRole", NOW, NOW
                        )
                    },
                }
            ]
        },
    }
    heartbeat = live_progress._merged_live_progress_candidate(
        run_id=previous_by_field["run_id"],
        context={"attempt_id": "attempt-a", "attempt_started_at": "2026-08-01T10:00:00Z"},
        progress_by_field={"source": "engine-heartbeat", "status": "running"},
        observed_at="2026-08-01T12:00:15Z",
        now=NOW.replace(tzinfo=None) + datetime.timedelta(seconds=15),
        previous=previous_by_field,
    )
    assert heartbeat["progress_seq"] == 7
    assert heartbeat["event_seq"] > 11
    assert heartbeat["progressed_at"] == previous_by_field["progressed_at"]
    accepted_event = live_progress._status_event_for_accepted_progress(
        {"status": "running", "progress": {"detail": previous_by_field["detail"]}}, heartbeat
    )
    assert accepted_event["progress"]["progressed_at"] == previous_by_field["progressed_at"]
    assert accepted_event["progress"]["observed_at"] == "2026-08-01T12:00:15Z"
    event = live_progress._default_status_event(heartbeat)
    assert event["progress"]["detail"] == previous_by_field["detail"]
    assert event["progress"]["counters"] == previous_by_field["counters"]


@pytest.mark.asyncio
async def test_cleanup_failure_without_primary_error_keeps_reporting_failure_semantics():
    callback = AsyncMock(side_effect=RuntimeError("status cleanup"))
    with pytest.raises(RuntimeError, match="status cleanup"):
        async with importer.fhir_response.bulk_export_wait_scope(callback):
            assert callback.await_count == 0
    assert importer.fhir_response.BULK_EXPORT_WAIT_CALLBACK.get() is None


@pytest.mark.asyncio
async def test_handled_outer_error_does_not_hide_cleanup_failure():
    callback = AsyncMock(side_effect=RuntimeError("status cleanup"))
    try:
        raise LookupError("handled outer error")
    except LookupError:
        with pytest.raises(RuntimeError, match="status cleanup"):
            async with importer.fhir_response.bulk_export_wait_scope(callback):
                callback.assert_not_awaited()
    callback.assert_awaited_once_with(None)
    assert importer.fhir_response.BULK_EXPORT_WAIT_CALLBACK.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [True, False])
@pytest.mark.parametrize(
    "status,error,payload,expected",
    [
        (500, None, None, "bulk_export_status_http_500"),
        (202, "network", None, "network"),
        (200, None, {"output": [], "error": [{"type": "OperationOutcome"}]}, "bulk_export_error_OperationOutcome"),
    ],
)
async def test_upstream_error_survives_failed_status_clear(monkeypatch, durable, status, error, payload, expected):
    monkeypatch.setattr(importer, "_bulk_export_now_utc", lambda: NOW)
    monkeypatch.setattr(importer, "_bulk_export_poll_settings", lambda: (2, 0))
    monkeypatch.setattr(importer.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(
        importer, "_bulk_http_get_json", AsyncMock(side_effect=[(202, {}, None, None), (status, {}, payload, error)])
    )
    callback = AsyncMock(side_effect=[None, RuntimeError("status cleanup")])
    token = importer.fhir_response.BULK_EXPORT_WAIT_CALLBACK.set(callback)
    try:
        poll = importer._bulk_export_poll_manifest if durable else importer._bulk_export_poll_outputs
        _, observed_error, _ = await poll(
            object(), SOURCE, "https://directory.example.test/status", resource_type="PractitionerRole", timeout=3
        )
    finally:
        importer.fhir_response.BULK_EXPORT_WAIT_CALLBACK.reset(token)
    assert observed_error == expected
