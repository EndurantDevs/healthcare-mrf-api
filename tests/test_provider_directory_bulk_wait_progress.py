# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pending export observations preserve resource and work ownership."""

import asyncio
import datetime
import importlib
from unittest.mock import AsyncMock

import pytest

from process import live_progress
from tests.test_provider_directory_fhir_reviewed_serial_stop import _runner

importer = importlib.import_module("process.provider_directory_fhir")
NOW = datetime.datetime(2026, 8, 1, 12, tzinfo=datetime.UTC)
SOURCE = {"source_id": "directory-alpha", "api_base": "https://directory.example.test/fhir"}


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
