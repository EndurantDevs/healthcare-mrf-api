# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained REST failures retry independently without changing cursor custody."""

import asyncio
import copy
import datetime
from contextlib import nullcontext
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest

from process import provider_directory_resource_retry as retry
from tests.test_process_provider_directory_fhir_unit import _stub_resource_import_metadata, importer
from tests.test_provider_directory_fhir_reviewed_serial_stop import _runner

BASE = "https://directory.example.test/fhir"


def _source():
    context = importer.PaginationCheckpointContext(
        canonical_api_base=BASE,
        source_scope_hash="scope-example",
        source_ids=("source-example",),
        owner_run_id="run-example",
        acquisition_root_run_id="root-example",
        endpoint_id="endpoint-example",
        dataset_id="dataset-example",
        lineage_verified=True,
        allow_retained_pagination_reset=False,
    )
    return {
        "source_id": "source-example",
        "api_base": BASE,
        "canonical_api_base": BASE,
        "_endpoint_id": context.endpoint_id,
        "_endpoint_dataset_id": context.dataset_id,
        "_pagination_checkpoint_context": context,
        "_provider_directory_request_pacing_state": {},
    }


def _scan(resources=("Organization",)):
    runner = _runner(_source(), resources)
    runner.options = replace(runner.options, requested_resource_concurrency=2, page_count=2)
    runner.effective_concurrency = 2
    runner.active_group_by_key[1] = {}
    runner.persist_rows.side_effect = lambda _model, rows: len(rows)
    return runner


def _result(resource="Organization", *, complete=False, error="http_500", pages=1, rows=2, written=0, **changes):
    result = importer.ResourceFetchResult(
        model=importer.RESOURCE_MODELS_BY_TYPE[resource],
        rows=[],
        rows_fetched=rows,
        rows_written=written,
        pages_fetched=pages,
        complete=complete,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=not complete,
        error=error,
        fetch_mode="checkpointed_paged",
        fetch_diagnostic={"status": 500, "response_class": "transient_server_error"},
    )
    return replace(result, **changes)


@pytest.mark.asyncio
async def test_direct_record_without_active_group_preserves_result_accounting():
    runner = _scan()
    runner.active_group_by_key.clear()
    result = _result(complete=True, error=None, written=2)
    assert await runner.record("Organization", runner.source_record, result) is None
    assert runner.count_by_resource["Organization"] == 2
    assert runner.stats_by_resource["Organization"]["rows_fetched"] == 2
    diagnostic = runner.diagnostic_by_resource["Organization"]
    assert diagnostic["complete"] and diagnostic["resource_scan"] is None
    runner.mark_stale_ready.assert_awaited_once_with("Organization", result)
    runner.report_progress.assert_not_awaited()
    assert runner.active_group_by_key == {}


@pytest.mark.asyncio
async def test_settled_owned_empty_group_propagates_progress_failure():
    runner = _scan()
    failure = importer.ResourceImportProgressFailure("unavailable")
    runner.report_progress.side_effect = failure
    result = _result(complete=True, error=None, written=2)
    with pytest.raises(importer.ResourceImportProgressFailure) as observed:
        await runner.record("Organization", runner.source_record, result)
    assert observed.value is failure
    runner.report_progress.assert_awaited_once_with(force=True)
    assert runner.active_group_by_key[1]["resource_scans"]["Organization"]["state"] == "complete"
    runner.mark_stale_ready.assert_not_awaited()


@pytest.mark.asyncio
async def test_progress_writes_capture_and_persist_resource_states_in_order(monkeypatch):
    _stub_resource_import_metadata(monkeypatch)
    monkeypatch.setattr(importer, "time", SimpleNamespace(monotonic=lambda: 10.0))
    started, release, second_requested = asyncio.Event(), asyncio.Event(), asyncio.Event()
    snapshots = []

    async def progress(_done, _total, _counts, details):
        groups = details["active_source_groups"]
        state_by_resource = (
            {resource: scan["state"] for resource, scan in groups[0].get("resource_scans", {}).items()}
            if groups
            else {}
        )
        if state_by_resource == {"Organization": "fetching"}:
            started.set()
            await release.wait()
        if state_by_resource:
            snapshots.append(state_by_resource)

    async def scan(runner, _resources):
        report_progress = runner.report_progress
        await report_progress()

        async def signal_progress(**options):
            if "Location" in runner.active_group_by_key[runner.group_key]["resource_scans"]:
                second_requested.set()
            await report_progress(**options)

        runner.report_progress = signal_progress
        state = retry.ResourceRetryState(None)
        tasks = [asyncio.create_task(retry.report(importer, runner, "Organization", "fetching", state))]
        try:
            await asyncio.wait_for(started.wait(), 1)
            await asyncio.wait_for(runner.partial_progress(runner.group_key, "Location", 1), 1)
            tasks.append(asyncio.create_task(retry.report(importer, runner, "Location", "fetching", state)))
            await asyncio.wait_for(second_requested.wait(), 1)
            assert not snapshots and not tasks[-1].done()
            release.set()
            await asyncio.wait_for(asyncio.gather(*tasks), 1)
        finally:
            release.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    monkeypatch.setattr(importer.ResourceGroupScanRunner, "scan", scan)
    await importer._import_resources(
        [_source()],
        resources=["Organization", "Location"],
        per_resource_limit=0,
        page_limit=0,
        page_count=2,
        timeout=3,
        run_id="run-example",
        progress_callback=progress,
    )
    assert snapshots == [{"Organization": "fetching"}, {"Organization": "fetching", "Location": "fetching"}]


@pytest.mark.asyncio
async def test_due_partial_reports_recheck_throttle_after_queued_writer(monkeypatch):
    _stub_resource_import_metadata(monkeypatch)
    clock = SimpleNamespace(value=10.0)
    monkeypatch.setattr(importer, "time", SimpleNamespace(monotonic=lambda: clock.value))
    blocked, release = asyncio.Event(), asyncio.Event()
    snapshots = []

    async def progress(_done, _total, counts, details):
        groups = details["active_source_groups"]
        if not groups or "Organization" not in groups[0].get("resource_scans", {}):
            return
        if not blocked.is_set():
            blocked.set()
            await release.wait()
        snapshots.append(counts["Organization"])

    async def scan(runner, _resources):
        state = retry.ResourceRetryState(None)
        tasks = [asyncio.create_task(retry.report(importer, runner, "Organization", "fetching", state))]
        try:
            await asyncio.wait_for(blocked.wait(), 1)
            clock.value = 100.0  # both partial reporters pass the cheap precheck, then queue behind the writer
            tasks.extend(
                asyncio.create_task(runner.partial_progress(runner.group_key, "Organization", 1)) for _ in range(2)
            )
            await asyncio.sleep(0)
            assert not snapshots and all(not task.done() for task in tasks)
            release.set()
            await asyncio.wait_for(asyncio.gather(*tasks), 1)
            assert all(task.done() for task in tasks)
            runner.count_by_resource["Organization"] = 2
        finally:
            release.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    monkeypatch.setattr(importer.ResourceGroupScanRunner, "scan", scan)
    counts = await importer._import_resources(
        [_source()],
        resources=["Organization"],
        per_resource_limit=0,
        page_limit=0,
        page_count=2,
        timeout=3,
        run_id="run-example",
        progress_callback=progress,
    )
    assert counts == {"Organization": 2}
    assert snapshots == [0, 2]


@pytest.mark.asyncio
async def test_retry_progress_and_completion_do_not_wait_for_blocked_sibling(monkeypatch):
    runner = _scan(("Organization", "Location", "HealthcareService"))
    waiting = asyncio.Event()
    resume = asyncio.Event()
    sibling_started = asyncio.Event()
    third_started = asyncio.Event()
    release_sibling, retried, complete = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = dict.fromkeys(runner.count_by_resource, 0)
    snapshots = []

    async def progress(**_options):
        snapshots.append(copy.deepcopy(runner.active_group_by_key[1].get("resource_scans", {})))
        if snapshots[-1].get("Organization", {}).get("state") == "complete":
            complete.set()

    async def backoff(_directory, _runner, delay):
        assert delay >= 299
        waiting.set()
        await resume.wait()

    async def fetch(resource, _source, **_options):
        calls[resource] += 1
        if resource == "Location":
            sibling_started.set()
            await release_sibling.wait()
        if resource == "HealthcareService":
            third_started.set()
        if resource == "Organization" and calls[resource] == 1:
            await sibling_started.wait()
            return _result(written=2)
        if resource == "Organization":
            assert not release_sibling.is_set()
            retried.set()
        return _result(resource, complete=True, error=None, written=1)

    monkeypatch.setattr(retry, "wait", backoff)
    monkeypatch.setattr(runner, "_fetch_rows", fetch)
    runner.report_progress.side_effect = progress
    wave = asyncio.create_task(runner.wave(tuple(calls), 2))
    try:
        await asyncio.wait_for(asyncio.gather(waiting.wait(), third_started.wait()), 1)
        waiting_state = next(
            snapshot["Organization"]
            for snapshot in snapshots
            if snapshot.get("Organization", {}).get("state") == "retry_wait"
        )
        assert waiting_state["error"] == "http_500" and waiting_state["retry_not_before"]
        assert not wave.done() and calls["Organization"] == 1
        resume.set()
        await asyncio.wait_for(retried.wait(), 1)
        await asyncio.wait_for(complete.wait(), 1)
        assert not wave.done() and runner.count_by_resource["Organization"] == 3
        release_sibling.set()
        await asyncio.wait_for(wave, 1)
    finally:
        wave.cancel()
        await asyncio.gather(wave, return_exceptions=True)
    assert calls == {"Organization": 2, "Location": 1, "HealthcareService": 1}


def _bundle(*ids, next_url=None):
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [{"resource": {"resourceType": "Organization", "id": value}} for value in ids],
        "link": [{"relation": "next", "url": next_url}] if next_url else [],
    }


async def _assert_complete_cursor(runner):
    resume_entries = set()
    await importer._finalize_source_pagination_checkpoints(
        runner.source_record,
        runner.diagnostic_by_resource,
        resume_entries,
        require_complete_resources=True,
        retain_complete_checkpoint=True,
    )
    assert not resume_entries and runner.diagnostic_by_resource["Organization"]["error"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("retained", [False, True])
async def test_real_reader_resumes_opaque_checkpoint_and_counts_only_attempt_writes(monkeypatch, retained):
    runner = _scan()
    cursor = BASE + "/Organization?_getpages=opaque-example"
    checkpoint_by_field = {
        "next_url": cursor if retained else BASE + "/Organization?_count=2",
        "pages_processed": int(retained),
        "rows_processed": 2 if retained else 0,
        "recent_url_hashes": (),
        "complete": False,
    }
    requests, writes = [], []

    async def load(*_args, **_options):
        return importer.PaginationResumeState(
            **checkpoint_by_field, resumed=bool(checkpoint_by_field["pages_processed"])
        )

    async def save(_context, _resource, **values):
        checkpoint_by_field.update({key: values[key] for key in ("next_url", "pages_processed", "rows_processed")})
        checkpoint_by_field["recent_url_hashes"] = tuple(values["recent_url_hashes"])
        checkpoint_by_field["complete"] = values["next_url"] is None

    async def fetch(source, url, **_options):
        requests.append(url)
        is_failure = len(requests) == (1 if retained else 2)
        response = (
            (500, None, None, 7)
            if is_failure
            else (
                200,
                _bundle("c", "d") if retained or len(requests) > 1 else _bundle("a", "b", next_url=cursor),
                None,
                5,
            )
        )
        importer._record_terminal_source_fetch_diagnostic(source, url, response, retry_count=2)
        return response

    async def persist(_model, rows):
        writes.extend(row["resource_id"] for row in rows)
        return len(rows)

    runner.persist_rows.side_effect = persist
    monkeypatch.setattr(retry, "wait", AsyncMock())
    monkeypatch.setattr(importer, "_load_or_initialize_pagination_checkpoint", load)
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", save)
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer, "source_checkpoint_transaction", lambda *_args, **_options: nullcontext())
    reset = AsyncMock()
    monkeypatch.setattr(importer, "_reset_pagination_checkpoint", reset)
    await runner.import_one("Organization", report_start=False)
    assert requests == ([cursor, cursor] if retained else [BASE + "/Organization?_count=2", cursor, cursor])
    assert writes == (["c", "d"] if retained else ["a", "b", "c", "d"]) and checkpoint_by_field["complete"]
    assert checkpoint_by_field["rows_processed"] == 4 and runner.count_by_resource["Organization"] == len(writes)
    assert runner.stats_by_resource["Organization"]["pages_fetched"] == 2
    assert runner.stats_by_resource["Organization"]["rows_fetched"] == 4
    assert runner.stats_by_resource["Organization"]["sources_attempted"] == 1
    await _assert_complete_cursor(runner)
    reset.assert_not_awaited()


@pytest.mark.asyncio
async def test_permanent_failure_is_bounded_visible_and_preserves_partial_result(monkeypatch):
    runner = _scan()
    fetch = AsyncMock(return_value=_result(written=0, source_fetch_elapsed_ms=7))
    monkeypatch.setattr(runner, "_fetch_rows", fetch)
    monkeypatch.setattr(retry, "wait", AsyncMock())
    monkeypatch.setattr(importer, "_source_fetch_retry_attempts", lambda: 3)
    await runner.import_one("Organization", report_start=False)
    state = runner.active_group_by_key[1]["resource_scans"]["Organization"]
    assert fetch.await_count == 3 and state["state"] == "exhausted" and state["error"] == "http_500"
    assert state["consecutive_no_progress"] == 3 and not state["complete"]
    diagnostic = runner.diagnostic_by_resource["Organization"]
    assert diagnostic["resource_scan"] == state
    assert diagnostic["next_url_remaining"] and not diagnostic["complete"]
    assert diagnostic["pages_fetched"] == 1 and diagnostic["rows_fetched"] == 2
    assert diagnostic["source_fetch_elapsed_ms"] == 21


@pytest.mark.asyncio
async def test_progress_cannot_reset_finite_total_budget_without_a_deadline(monkeypatch):
    runner = _scan()
    fetch = AsyncMock(side_effect=[_result(pages=count, rows=count * 2) for count in range(1, 6)])
    monkeypatch.setattr(runner, "_fetch_rows", fetch)
    monkeypatch.setattr(retry, "wait", AsyncMock())
    monkeypatch.setattr(importer, "_source_fetch_retry_attempts", lambda: 3)
    await runner.import_one("Organization", report_start=False)
    state = runner.active_group_by_key[1]["resource_scans"]["Organization"]
    assert fetch.await_count == 3 and state["state"] == "exhausted" and state["consecutive_no_progress"] == 0


@pytest.mark.asyncio
async def test_deadline_is_shared_across_attempts_and_durable_progress_resets_failure_budget(monkeypatch):
    runner = _scan()
    runner.options = replace(runner.options, resource_deadline_seconds=1000)
    clock, remaining = SimpleNamespace(value=10.0), []
    monkeypatch.setattr(importer, "time", SimpleNamespace(monotonic=lambda: clock.value))
    monkeypatch.setattr(importer, "_source_fetch_retry_attempts", lambda: 2)

    async def fetch(_resource, _source, **options):
        remaining.append(options["deadline_seconds"])
        clock.value += 100
        return _result(pages=len(remaining), rows=len(remaining) * 2)

    async def wait(_directory, _runner, delay):
        clock.value += delay

    monkeypatch.setattr(runner, "_fetch_rows", fetch)
    monkeypatch.setattr(retry, "wait", wait)
    await runner.import_one("Organization", report_start=False)
    assert remaining == [1000, 600, 200]
    state = runner.active_group_by_key[1]["resource_scans"]["Organization"]
    assert state["state"] == "exhausted" and state["consecutive_no_progress"] == 0


@pytest.mark.asyncio
async def test_retry_after_waits_for_later_shared_boundary():
    runner = _scan()
    source = runner.source_record
    source["_provider_directory_request_pacing_state"]["retry_after_monotonic"] = importer.time.monotonic() + 0.04
    boundary = datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=0.01)
    delay, timestamp = retry.retry_boundary(importer, source, _result(retry_not_before=boundary.isoformat()))
    started = importer.time.monotonic()
    await retry.wait(importer, runner, delay)
    assert importer.time.monotonic() - started >= 0.03
    assert datetime.datetime.fromisoformat(timestamp) >= boundary


@pytest.mark.asyncio
async def test_retry_boundary_reaches_existing_durable_and_terminal_audit_metrics(monkeypatch):
    from process.control_lifecycle import _terminal_metrics_from_context

    runner = _scan()
    metrics_by_field = {"active_source_groups": [runner.active_group_by_key[1]]}
    persist = AsyncMock()
    monkeypatch.setattr(importer, "_persist_provider_directory_progress", persist)

    async def progress(**_options):
        await importer._mark_provider_directory_progress(
            "run-example",
            phase="provider-directory importing resources",
            done=0,
            total=1,
            message="resource retry",
            metrics=metrics_by_field,
        )

    runner.report_progress.side_effect = progress
    boundary = (datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=60)).isoformat()
    await retry.report(
        importer, runner, "Organization", "retry_wait", retry.ResourceRetryState(None), _result(), boundary
    )
    stored = persist.await_args.kwargs["metrics"]
    assert stored[importer.SOURCE_RETRY_NOT_BEFORE_METRIC] == boundary
    assert stored["provider_directory_retry_not_before_by_resource"] == {"Organization": boundary}
    terminal = _terminal_metrics_from_context({"audit": metrics_by_field})
    assert terminal["audit"][importer.SOURCE_RETRY_NOT_BEFORE_METRIC] == boundary


@pytest.mark.asyncio
@pytest.mark.parametrize("pacing", [None, float("inf"), 1e300])
async def test_long_default_wait_yields_without_shortening_the_durable_retry_fence(monkeypatch, pacing):
    runner = _scan()
    if pacing is not None:
        runner.source_record["_provider_directory_request_pacing_state"]["retry_after_monotonic"] = pacing
    boundary = (datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=2)).isoformat()
    fetch = AsyncMock(return_value=_result(retry_not_before=boundary))
    wait, persist = AsyncMock(), AsyncMock()
    metrics_by_field = {"active_source_groups": [runner.active_group_by_key[1]]}

    async def progress(**_options):
        await importer._mark_provider_directory_progress(
            "run-example",
            phase="provider-directory importing resources",
            done=0,
            total=1,
            message="resource retry",
            metrics=metrics_by_field,
        )

    runner.report_progress.side_effect = progress
    monkeypatch.setattr(runner, "_fetch_rows", fetch)
    monkeypatch.setattr(retry, "wait", wait)
    monkeypatch.setattr(importer, "_persist_provider_directory_progress", persist)
    await runner.import_one("Organization", report_start=False)
    fetch.assert_awaited_once()
    wait.assert_not_awaited()
    diagnostic = runner.diagnostic_by_resource["Organization"]
    assert diagnostic["resource_scan"]["state"] == "exhausted" and diagnostic["error"] == "http_500"
    assert not diagnostic["complete"] and diagnostic["next_url_remaining"]
    assert diagnostic["retry_not_before"] == boundary
    stored = persist.await_args.kwargs["metrics"]
    assert stored[importer.SOURCE_RETRY_NOT_BEFORE_METRIC] == boundary
    assert stored["provider_directory_retry_not_before_by_resource"] == {"Organization": boundary}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("retained", "explicit", "expected"),
    [
        ("2099-01-02T03:04:05+02:00", None, "2099-01-02T01:04:05+00:00"),
        ("response details", None, None),
        ("2099-01-02T01:04:05", None, None),
        ("2099-01-02T01:04:05+00:00", "2099-01-03T00:00:00+00:00", "2099-01-03T00:00:00+00:00"),
    ],
)
async def test_progress_retains_only_valid_fences_and_keeps_explicit_boundaries(retained, explicit, expected):
    runner = _scan()
    snapshot = await retry.report(
        importer,
        runner,
        "Organization",
        "exhausted",
        retry.ResourceRetryState(None),
        _result(retry_not_before=retained),
        explicit,
    )
    assert snapshot["retry_not_before"] == expected
    metrics_by_field = {"active_source_groups": [runner.active_group_by_key[1]]}
    retry.update_metrics(importer, metrics_by_field)
    if expected is None:
        assert importer.SOURCE_RETRY_NOT_BEFORE_METRIC not in metrics_by_field
        assert "provider_directory_retry_not_before_by_resource" not in metrics_by_field
    else:
        assert metrics_by_field[importer.SOURCE_RETRY_NOT_BEFORE_METRIC] == expected
    runner.report_progress.assert_awaited_once_with(force=True)


@pytest.mark.asyncio
async def test_queued_retry_keeps_original_deadline_and_partial_accounting(monkeypatch):
    runner = _scan()
    runner.options = replace(runner.options, resource_deadline_seconds=1000)
    clock, queued = SimpleNamespace(value=10.0), asyncio.Event()
    monkeypatch.setattr(importer, "time", SimpleNamespace(monotonic=lambda: clock.value))

    class ResourceSlots(asyncio.Semaphore):
        async def acquire(self):
            if self.locked():
                queued.set()
            return await super().acquire()

    slots = ResourceSlots(1)
    occupied, release = asyncio.Event(), asyncio.Event()
    contenders = []
    partial_result = _result(written=2, pages=3, rows=6, source_fetch_elapsed_ms=7)
    fetch = AsyncMock(return_value=partial_result)

    async def occupy_slot():
        async with slots:
            occupied.set()
            await release.wait()

    async def backoff(_directory, _runner, delay):
        contenders.append(asyncio.create_task(occupy_slot()))
        await occupied.wait()
        clock.value += delay

    monkeypatch.setattr(runner, "_fetch_rows", fetch)
    monkeypatch.setattr(retry, "wait", backoff)
    task = asyncio.create_task(runner.import_one("Organization", report_start=False, resource_semaphore=slots))
    try:
        await asyncio.wait_for(queued.wait(), 1)
        assert not task.done()
        clock.value = 1011.0
        release.set()
        await asyncio.wait_for(task, 1)
    finally:
        release.set()
        for pending in (task, *contenders):
            pending.cancel()
        await asyncio.gather(task, *contenders, return_exceptions=True)
    fetch.assert_awaited_once()
    assert fetch.await_args.kwargs["deadline_seconds"] == 1000
    diagnostic = runner.diagnostic_by_resource["Organization"]
    assert diagnostic["resource_scan"]["state"] == "exhausted" and diagnostic["error"] == "http_500"
    assert not diagnostic["complete"] and diagnostic["next_url_remaining"]
    assert runner.count_by_resource["Organization"] == diagnostic["rows_written"] == 2
    assert diagnostic["pages_fetched"] == 3 and diagnostic["rows_fetched"] == 6
    assert diagnostic["source_fetch_elapsed_ms"] == 7
    assert (
        fetch.await_args.args[1]["_pagination_checkpoint_context"]
        is runner.source_record["_pagination_checkpoint_context"]
    )


@pytest.mark.parametrize(
    ("delay", "deadline", "exhausted"),
    [(600, 0, False), (601, 0, True), (1200, 1800, False), (1200, 1200, True)],
)
def test_default_wait_limit_preserves_explicit_resource_deadlines(delay, deadline, exhausted):
    deadline_at = importer.time.monotonic() + deadline if deadline else None
    state = retry.ResourceRetryState(deadline_at, attempt=1)
    assert state.is_exhausted(importer, delay) is exhausted


def test_canonical_retry_fence_preserves_other_groups_and_clears_expired_recovery(monkeypatch):
    now = datetime.datetime.now(datetime.UTC)
    clock = SimpleNamespace(now=now)

    class Clock(datetime.datetime):
        @classmethod
        def now(cls, _timezone):
            return clock.now

    monkeypatch.setattr(retry, "datetime", SimpleNamespace(datetime=Clock, UTC=datetime.UTC))
    boundary = (now + datetime.timedelta(seconds=60)).isoformat()
    key = "provider_directory_retry_not_before_by_resource"
    metrics_by_field = {
        key: {"Organization": boundary},
        importer.SOURCE_RETRY_NOT_BEFORE_METRIC: boundary,
        "active_source_groups": [{"resource_scans": {"Organization": {"state": "complete", "retry_not_before": None}}}],
    }
    retry.update_metrics(importer, metrics_by_field)
    assert metrics_by_field[key] == {"Organization": boundary}
    metrics_by_field[key]["Organization"] = "2000-01-01T00:00:00+00:00"
    retry.update_metrics(importer, metrics_by_field)
    assert metrics_by_field[key] == {} and metrics_by_field[importer.SOURCE_RETRY_NOT_BEFORE_METRIC] == boundary
    metrics_by_field[key]["Organization"] = boundary
    clock.now += datetime.timedelta(seconds=61)
    retry.update_metrics(importer, metrics_by_field)
    assert metrics_by_field[key] == {} and metrics_by_field[importer.SOURCE_RETRY_NOT_BEFORE_METRIC] is None


def test_stronger_unassociated_canonical_fence_survives_a_shorter_resource_wait():
    now = datetime.datetime.now(datetime.UTC)
    stronger = (now + datetime.timedelta(seconds=120)).isoformat()
    shorter = (now + datetime.timedelta(seconds=60)).isoformat()
    metrics_by_field = {
        importer.SOURCE_RETRY_NOT_BEFORE_METRIC: stronger,
        "active_source_groups": [
            None,
            {"resource_scans": None},
            {
                "resource_scans": {
                    "Location": None,
                    "Organization": {"state": "retry_wait", "retry_not_before": shorter},
                }
            },
        ],
    }
    retry.update_metrics(importer, metrics_by_field)
    assert metrics_by_field[importer.SOURCE_RETRY_NOT_BEFORE_METRIC] == stronger
    assert metrics_by_field["provider_directory_retry_not_before_by_resource"] == {"Organization": shorter}
    metrics_by_field["active_source_groups"] = None
    retry.update_metrics(importer, metrics_by_field)
    assert metrics_by_field[importer.SOURCE_RETRY_NOT_BEFORE_METRIC] == stronger


@pytest.mark.asyncio
async def test_already_canceled_scan_does_not_publish_fetching_or_fetch(monkeypatch):
    runner = _scan()
    fetch = AsyncMock()
    monkeypatch.setattr(runner, "_fetch_rows", fetch)
    monkeypatch.setattr(
        importer,
        "_raise_if_resource_import_cancelled",
        AsyncMock(side_effect=importer.ImportCancelledError("canceled")),
    )
    with pytest.raises(importer.ImportCancelledError):
        await runner.import_one("Organization", report_start=False)
    runner.report_progress.assert_not_awaited()
    fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_resource_retry_does_not_send_before_its_not_before_time(monkeypatch):
    runner = _scan()
    calls = []
    boundary = datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=0.04)

    async def fetch(_resource, _source, **_options):
        calls.append(datetime.datetime.now(datetime.UTC))
        return _result(retry_not_before=boundary.isoformat()) if len(calls) == 1 else _result(complete=True, error=None)

    monkeypatch.setattr(runner, "_fetch_rows", fetch)
    await asyncio.wait_for(runner.import_one("Organization", report_start=False), 1)
    assert len(calls) == 2 and calls[1] >= boundary


@pytest.mark.asyncio
async def test_cancellation_during_backoff_joins_active_sibling(monkeypatch):
    runner = _scan(("Organization", "Location"))
    waiting = asyncio.Event()
    sibling_started = asyncio.Event()
    sibling_canceled = asyncio.Event()

    async def progress(**_options):
        if runner.active_group_by_key[1].get("resource_scans", {}).get("Organization", {}).get("state") == "retry_wait":
            waiting.set()

    async def fetch(resource, _source, **_options):
        if resource == "Organization":
            await sibling_started.wait()
            return _result()
        sibling_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            sibling_canceled.set()

    monkeypatch.setattr(runner, "_fetch_rows", fetch)
    runner.report_progress.side_effect = progress
    wave = asyncio.create_task(runner.wave(("Organization", "Location"), 2))
    try:
        await asyncio.wait_for(waiting.wait(), 1)
        wave.cancel()
        with pytest.raises(asyncio.CancelledError):
            await wave
        assert sibling_canceled.is_set()
        assert not any(
            task.get_name() == "provider-directory-resource-scan" and not task.done() for task in asyncio.all_tasks()
        )
    finally:
        wave.cancel()
        await asyncio.gather(wave, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        asyncio.CancelledError(),
        importer.ImportCancelledError("canceled"),
        importer.ResourceImportControlPlaneFailure("unavailable"),
        importer.ResourceImportProgressFailure("unavailable"),
        asyncpg.PostgresError("unavailable"),
    ],
)
async def test_fatal_failures_are_never_converted_to_resource_retries(monkeypatch, failure):
    runner = _scan()
    fetch = AsyncMock(side_effect=failure)
    wait = AsyncMock()
    monkeypatch.setattr(runner, "_fetch_rows", fetch)
    monkeypatch.setattr(retry, "wait", wait)
    with pytest.raises(type(failure)):
        await runner.import_one("Organization", report_start=False)
    fetch.assert_awaited_once()
    wait.assert_not_awaited()


@pytest.mark.parametrize(
    "changes",
    [
        {"error": "http_400", "fetch_diagnostic": None},
        {"error": "http_410", "fetch_diagnostic": None},
        {"error": "non_bundle_payload", "fetch_diagnostic": None},
        {"fetch_mode": "bulk_ndjson"},
        {"fetch_mode": "current_version_census"},
        {"fetch_mode": "partitioned_paged"},
        {"fetch_mode": "source_start_refused"},
        {"page_limit_reached": True},
        {"deadline_reached": True},
        {"complete": True},
        {"next_url_remaining": False},
    ],
)
def test_nonretryable_and_specialized_failures_do_not_become_end_of_data(changes):
    runner = _scan()
    result = _result(**changes)
    assert not retry.retryable(importer, runner, runner.source_record, result)
    if not result.complete:
        assert not importer._is_resource_fetch_complete_for_publish(result)


@pytest.mark.parametrize("status", [423, 429, 500, 502, 503, 504])
def test_existing_transient_http_classes_keep_their_checkpoint_retry(status):
    runner = _scan()
    result = _result(
        error=f"http_{status}",
        fetch_diagnostic={"status": status, "response_class": importer._source_fetch_response_class(status, None)},
    )
    assert retry.retryable(importer, runner, runner.source_record, result)


def test_transport_retry_excludes_quota_and_mismatched_http_evidence():
    runner = _scan()
    result = _result(
        error="TimeoutError", fetch_diagnostic={"status": None, "response_class": "transient_transport_error"}
    )
    assert retry.retryable(importer, runner, runner.source_record, result)
    for refused in (replace(result, error=importer.SOURCE_QUOTA_EXHAUSTED_ERROR), _result(error="http_400")):
        assert not retry.retryable(importer, runner, runner.source_record, refused)


def test_progress_errors_are_sanitized_and_invalid_deadlines_refuse_retry():
    runner = _scan()
    result = _result(error="response included private request details", retry_not_before="untrusted deadline")
    assert retry.safe_error(result) == "source_fetch_error"
    assert retry.retry_boundary(importer, runner.source_record, result) is None


@pytest.mark.parametrize(
    "error",
    [
        "TimeoutError",
        "ServerTimeoutError",
        "ClientPayloadError",
        "ClientConnectorError",
        "ConnectionResetError",
        "non_bundle_payload",
        "deadline_reached",
    ],
)
def test_known_error_classes_remain_fixed_and_unknown_details_are_redacted(error):
    assert retry.safe_error(_result(error=error)) == error
    assert retry.safe_error(_result(error=error + ": response details")) == "source_fetch_error"


def test_resource_retry_requires_the_existing_no_reset_contract():
    runner = _scan()
    runner.source_record["_pagination_checkpoint_context"] = replace(
        runner.source_record["_pagination_checkpoint_context"],
        allow_retained_pagination_reset=True,
    )
    assert not retry.retryable(importer, runner, runner.source_record, _result())
