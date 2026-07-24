# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Changed-code coverage for PTG import orchestration helpers."""

from __future__ import annotations

import asyncio
import datetime
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from sqlalchemy.sql.dml import Insert

from db.connection import InsertAdapter
from tests.ptg_v4_import_orchestration_support import (
    AllResult,
    FirstResult,
    RecordingSession,
    TaskState,
    transaction_factory,
)
process_ptg = importlib.import_module("process.ptg")

@pytest.mark.asyncio
async def test_terminal_stage_names_are_returned_in_database_order(monkeypatch):
    """Expose the retained stage names that a terminal retry must release."""

    session = RecordingSession([AllResult([("stage_a",), ("stage_b",)])])

    stage_names = await process_ptg._registered_terminal_stage_names(
        session,
        schema='"mrf"',
        snapshot_id="snapshot-one",
        internal_run_id="run-one",
    )

    assert stage_names == ["stage_a", "stage_b"]
    assert session.executions[0][1] == {
        "snapshot_id": "snapshot-one",
        "internal_run_id": "run-one",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("snapshot_fields", "message"),
    [
        ({"import_run_id": "run-one"}, "changed its attempt pair"),
        (
            {"snapshot_id": "snapshot-one", "import_run_id": "other-run"},
            "changed its attempt pair",
        ),
    ],
)
async def test_terminal_retry_rejects_an_inexact_attempt(snapshot_fields, message):
    """Refuse terminal cleanup unless both attempt coordinates still match."""

    with pytest.raises(RuntimeError, match=message):
        await process_ptg._finalize_resumed_terminal_attempt(
            snapshot_fields,
            internal_run_id="run-one",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_row", [None, ("run-one",)])
async def test_terminal_retry_requires_a_terminal_row_before_stage_drop(
    monkeypatch,
    terminal_row,
):
    """Drop retained stages only after the terminal transition succeeds."""

    session = RecordingSession(
        [FirstResult(terminal_row), AllResult([("stage_b",), ("stage_a",)])]
    )
    stage_drop = AsyncMock()
    monkeypatch.setattr(process_ptg.db, "transaction", transaction_factory(session))
    monkeypatch.setattr(process_ptg, "lock_writable_snapshot", AsyncMock())
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_table_names", stage_drop)

    coroutine = process_ptg._finalize_resumed_terminal_attempt(
        {"snapshot_id": "snapshot-one", "import_run_id": "run-one"},
        internal_run_id="run-one",
    )
    if terminal_row is None:
        with pytest.raises(RuntimeError, match="not finalizable"):
            await coroutine
        stage_drop.assert_not_awaited()
    else:
        await coroutine
        stage_drop.assert_awaited_once_with(
            ["stage_b", "stage_a"],
            snapshot_id="snapshot-one",
            internal_run_id="run-one",
        )


@pytest.mark.asyncio
async def test_plan_month_write_locks_distinct_snapshot_ids_in_sorted_order(
    monkeypatch,
):
    """Fence each distinct snapshot before the plan-month batch is executed."""

    session = RecordingSession([FirstResult(None)])
    snapshot_lock = AsyncMock()
    monkeypatch.setattr(process_ptg.db, "transaction", transaction_factory(session))
    monkeypatch.setattr(
        process_ptg.db,
        "_execution_session",
        transaction_factory(session),
    )
    monkeypatch.setattr(process_ptg, "lock_writable_snapshot", snapshot_lock)

    await process_ptg._push_fenced_ptg2_plan_months(
        [
            {"snapshot_id": "snapshot-z", "plan_id": "z"},
            {"snapshot_id": "", "plan_id": "ignored"},
            {"snapshot_id": "snapshot-a", "plan_id": "a"},
            {"snapshot_id": "snapshot-z", "plan_id": "duplicate"},
        ]
    )

    assert [call.kwargs["snapshot_id"] for call in snapshot_lock.await_args_list] == [
        "snapshot-a",
        "snapshot-z",
    ]
    assert len(session.executions) == 1
    assert isinstance(session.executions[0][0], Insert)
    assert not isinstance(session.executions[0][0], InsertAdapter)


@pytest.mark.asyncio
async def test_empty_plan_month_batch_executes_without_snapshot_locks(monkeypatch):
    """Allow the empty batch while avoiding a meaningless snapshot lock."""

    session = RecordingSession([FirstResult(None)])
    snapshot_lock = AsyncMock()
    monkeypatch.setattr(process_ptg.db, "transaction", transaction_factory(session))
    monkeypatch.setattr(
        process_ptg.db,
        "_execution_session",
        transaction_factory(session),
    )
    monkeypatch.setattr(process_ptg, "lock_writable_snapshot", snapshot_lock)

    await process_ptg._push_fenced_ptg2_plan_months([])

    snapshot_lock.assert_not_awaited()
    assert len(session.executions) == 1
    assert isinstance(session.executions[0][0], Insert)
    assert not isinstance(session.executions[0][0], InsertAdapter)


@pytest.mark.asyncio
async def test_plan_month_writes_route_through_the_fenced_adapter_path(monkeypatch):
    """Keep plan-month writes on the transaction-safe adapter boundary."""

    entries = [{"snapshot_id": "snapshot-one", "plan_id": "plan-one"}]
    push_fenced = AsyncMock()
    monkeypatch.setattr(
        process_ptg,
        "_push_fenced_ptg2_plan_months",
        push_fenced,
    )

    result = await process_ptg._push_ptg2_objects(
        entries,
        process_ptg.PTG2PlanMonth,
    )

    assert result is None
    push_fenced.assert_awaited_once_with(entries)


_CLEANUP_BOUNDARIES = (
    "_drop_ptg2_snapshot_tables_for_manifest",
    "delete_ptg2_artifacts_for_snapshot",
    "delete_unpublished_snapshot_sources",
    "_delete_allowed_snapshot_rows",
)


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_boundary", _CLEANUP_BOUNDARIES)
async def test_cleanup_continues_after_each_ordinary_boundary_failure(
    monkeypatch,
    failing_boundary,
):
    """Best-effort cleanup reaches later stores after an ordinary failure."""

    cleanup_by_name = {
        name: AsyncMock(
            side_effect=RuntimeError(f"{name} unavailable")
            if name == failing_boundary
            else None
        )
        for name in _CLEANUP_BOUNDARIES
    }
    for name, cleanup in cleanup_by_name.items():
        monkeypatch.setattr(process_ptg, name, cleanup)

    await process_ptg._cleanup_failed_ptg2_source_state(
        serving_index={"table": "candidate"},
        snapshot_id="snapshot-one",
        internal_run_id="run-one",
    )

    assert all(cleanup.await_count == 1 for cleanup in cleanup_by_name.values())


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_boundary", _CLEANUP_BOUNDARIES)
async def test_cleanup_translates_each_fence_and_stops_destructive_work(
    monkeypatch,
    failing_boundary,
):
    """Surface reconciliation fences instead of continuing destructive cleanup."""

    cleanup_by_name = {name: AsyncMock() for name in _CLEANUP_BOUNDARIES}
    cleanup_by_name[failing_boundary].side_effect = (
        process_ptg.StaleMetadataFenceError("reconciled")
    )
    for name, cleanup in cleanup_by_name.items():
        monkeypatch.setattr(process_ptg, name, cleanup)

    with pytest.raises(process_ptg.StaleMetadataFenceError, match="durably fenced"):
        await process_ptg._cleanup_failed_ptg2_source_state(
            serving_index=None,
            snapshot_id="snapshot-one",
            internal_run_id="run-one",
        )

    failure_index = _CLEANUP_BOUNDARIES.index(failing_boundary)
    assert [
        cleanup_by_name[name].await_count for name in _CLEANUP_BOUNDARIES
    ] == [1 if index <= failure_index else 0 for index in range(4)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("table_class", "copy_name"),
    [
        (process_ptg.PTG2PriceSet, "_copy_ignore_ptg2_objects"),
        (process_ptg.PTG2ServingRate, "_copy_insert_ptg2_objects"),
        (SimpleNamespace(__tablename__="bulk_table"), "_copy_upsert_ptg2_objects"),
    ],
)
async def test_copy_fences_never_fall_back_to_direct_writes(
    monkeypatch,
    table_class,
    copy_name,
):
    """Keep all COPY boundary fences fatal instead of invoking fallback writes."""

    direct_write = AsyncMock()
    monkeypatch.setattr(process_ptg, "_env_bool", lambda *_arguments: True)
    monkeypatch.setattr(process_ptg, "_env_int", lambda *_arguments: 1)
    monkeypatch.setattr(
        process_ptg,
        copy_name,
        AsyncMock(side_effect=process_ptg.StaleMetadataFenceError("reconciled")),
    )
    monkeypatch.setattr(process_ptg, "_push_fenced_ptg2_objects_direct", direct_write)

    with pytest.raises(process_ptg.StaleMetadataFenceError, match="durably fenced"):
        await process_ptg._push_ptg2_objects([{"snapshot_id": "snapshot-one"}], table_class)

    direct_write.assert_not_awaited()


@pytest.mark.asyncio
async def test_failure_persistence_records_stage_release_and_final_timings(monkeypatch):
    """Persist snapshot, run, stage release, then the final timing contract."""

    pushed_batches = []
    stage_drop = AsyncMock()
    monotonic_values = iter([12.0, 15.0])

    async def record_push(rows, table_class, **_options):
        pushed_batches.append((table_class, rows[0]))

    monkeypatch.setattr(process_ptg.db, "transaction", transaction_factory(object()))
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", record_push)
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_table_names", stage_drop)
    monkeypatch.setattr(process_ptg, "_ptg2_monotonic", lambda: next(monotonic_values))

    report = await process_ptg._mark_ptg2_import_failed(
        "run-one",
        "snapshot-one",
        datetime.date(2026, 7, 1),
        datetime.datetime(2026, 7, 1),
        "scan failed",
        report={"timings": {"total_seconds": 999, "download_seconds": 2}},
        manifest_stage_table="candidate",
        import_started_monotonic=5.0,
        failure_handling_started_monotonic=10.0,
    )

    assert [table for table, _row in pushed_batches] == [
        process_ptg.PTG2Snapshot,
        process_ptg.PTG2ImportRun,
        process_ptg.PTG2ImportRun,
    ]
    assert report["timings"] == {
        "download_seconds": 2,
        "failure_state_persistence_seconds": 3.0,
        "failure_handling_seconds": 5.0,
        "total_seconds": 10.0,
    }
    stage_drop.assert_awaited_once()


@pytest.mark.asyncio
async def test_failure_persistence_can_preserve_a_published_snapshot(monkeypatch):
    """Update only the run when the candidate may already serve live traffic."""

    pushed_classes = []

    async def record_push(_rows, table_class, **_options):
        pushed_classes.append(table_class)

    monkeypatch.setattr(process_ptg.db, "transaction", transaction_factory(object()))
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", record_push)

    report = await process_ptg._mark_ptg2_import_failed(
        "run-one",
        "snapshot-one",
        datetime.date(2026, 7, 1),
        datetime.datetime(2026, 7, 1),
        "late failure",
        should_preserve_published_snapshot=True,
    )

    assert pushed_classes == [process_ptg.PTG2ImportRun]
    assert report["timings"] == {}
    assert report["snapshot_id"] == "snapshot-one"


@pytest.mark.asyncio
@pytest.mark.parametrize("fenced", [False, True])
async def test_failure_persistence_distinguishes_storage_errors_from_fences(
    monkeypatch,
    fenced,
):
    """Return incomplete for ordinary storage errors but rethrow a fence."""

    storage_error = (
        process_ptg.StaleMetadataFenceError("reconciled")
        if fenced
        else RuntimeError("database offline")
    )
    monkeypatch.setattr(process_ptg.db, "transaction", transaction_factory(object()))
    monkeypatch.setattr(
        process_ptg,
        "_push_ptg2_objects",
        AsyncMock(side_effect=storage_error),
    )

    call = process_ptg._mark_ptg2_import_failed(
        "run-one",
        "snapshot-one",
        datetime.date(2026, 7, 1),
        datetime.datetime(2026, 7, 1),
        "failure",
    )
    if fenced:
        with pytest.raises(process_ptg.StaleMetadataFenceError, match="durably fenced"):
            await call
    else:
        assert await call is None


def test_source_version_helpers_ignore_empty_and_duplicate_results():
    """Return stable source evidence while deduplicating equivalent identities."""

    assert process_ptg._source_version_summary(None) == {}
    source_version = SimpleNamespace(
        source_identity_hash="identity",
        source_file_version_id="version",
        canonical_url="https://example.test/canonical",
        raw_sha256="a" * 64,
        logical_sha256=None,
        logical_hash_deferred=True,
        content_length=42,
        etag="etag",
        last_modified="today",
    )
    summary = process_ptg._source_version_summary(source_version)
    files = [
        {"summary": "invalid"},
        {"summary": {}},
        {"url": "fallback", "summary": {"source_file_version_id": "version"}},
        {"url": "duplicate", "summary": {"engine_source_file_version_id": "version"}},
        {"summary": summary, "source_type": "in_network", "file_id": 7},
    ]

    versions = process_ptg._ptg2_source_file_versions_from_results(files)

    assert len(versions) == 2
    assert versions[0]["canonical_url"] == "fallback"
    assert versions[1]["engine_source_identity_hash"] == "identity"
    assert versions[1]["logical_hash_deferred"] is True


@pytest.mark.asyncio
async def test_source_dictionary_publishes_traces_and_checks_the_final_seal(
    monkeypatch,
):
    """Persist trace rows and return assignments when the source seal matches."""

    assignments = (("logical", "physical"),)
    trace_rows = [{"trace_set_hash": "trace"}]
    identity = SimpleNamespace(
        source_identities=("physical",),
        logical_plans=("logical",),
        coverage_scope_id="coverage",
    )
    push = AsyncMock()
    publish = AsyncMock(return_value=[{"raw_container_sha256": "a" * 64}])
    expected = process_ptg.shared_source_set_metadata(["a" * 64])
    monkeypatch.setattr(
        process_ptg,
        "shared_snapshot_source_assignments",
        lambda *_arguments, **_options: (assignments, trace_rows),
    )
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", push)
    monkeypatch.setattr(process_ptg, "publish_shared_v3_snapshot_sources", publish)

    published_assignments = await process_ptg._publish_shared_v3_source_dictionary(
        shared_input_identity=identity,
        identity_trace_pairs=({"raw_container_sha256": "a" * 64},),
        snapshot_id="snapshot-one",
        expected_source_set=expected,
    )

    assert published_assignments == assignments
    assert "created_at" in push.await_args.args[0][0]
    assert publish.await_args.kwargs["coverage_scope_id"] == "coverage"


@pytest.mark.asyncio
async def test_source_dictionary_rejects_a_changed_publication_seal(monkeypatch):
    """Reject publication when stored physical sources differ from preflight."""

    identity = SimpleNamespace(
        source_identities=(),
        logical_plans=(),
        coverage_scope_id="coverage",
    )
    monkeypatch.setattr(
        process_ptg,
        "shared_snapshot_source_assignments",
        lambda *_arguments, **_options: ((), []),
    )
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", AsyncMock())
    monkeypatch.setattr(
        process_ptg,
        "publish_shared_v3_snapshot_sources",
        AsyncMock(return_value=[{"raw_container_sha256": "b" * 64}]),
    )

    with pytest.raises(RuntimeError, match="source-set seal changed"):
        await process_ptg._publish_shared_v3_source_dictionary(
            shared_input_identity=identity,
            identity_trace_pairs=(),
            snapshot_id="snapshot-one",
            expected_source_set=process_ptg.shared_source_set_metadata(["a" * 64]),
        )


@pytest.mark.asyncio
async def test_heartbeat_logs_an_ordinary_failure_then_retries(monkeypatch, caplog):
    """Keep the lease alive after a transient heartbeat storage failure."""

    sleep_state_by_name = {"count": 0}

    async def bounded_sleep(_seconds):
        sleep_state_by_name["count"] += 1
        if sleep_state_by_name["count"] == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(process_ptg.asyncio, "sleep", bounded_sleep)
    monkeypatch.setattr(
        process_ptg,
        "lock_writable_snapshot",
        AsyncMock(side_effect=RuntimeError("database offline")),
    )
    monkeypatch.setattr(
        process_ptg.db,
        "transaction",
        transaction_factory(RecordingSession()),
    )

    with pytest.raises(asyncio.CancelledError):
        await process_ptg._heartbeat_ptg2_import_run("run-one")

    assert sleep_state_by_name["count"] == 2
    assert caplog.text.count("Failed to persist PTG2 import heartbeat for run-one") == 1


@pytest.mark.asyncio
async def test_heartbeat_rethrows_a_reconciliation_fence(monkeypatch):
    """Stop heartbeat work immediately when reconciliation owns the attempt."""

    monkeypatch.setattr(process_ptg.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(
        process_ptg,
        "lock_writable_snapshot",
        AsyncMock(side_effect=process_ptg.StaleMetadataFenceError("reconciled")),
    )
    monkeypatch.setattr(
        process_ptg.db,
        "transaction",
        transaction_factory(RecordingSession()),
    )

    with pytest.raises(process_ptg.StaleMetadataFenceError, match="durably fenced"):
        await process_ptg._heartbeat_ptg2_import_run("run-one")


@pytest.mark.asyncio
async def test_heartbeat_stop_handles_absent_done_and_running_tasks():
    """Join every heartbeat task state without leaking cancellation."""

    await process_ptg._stop_ptg2_import_heartbeat(None)
    completed = TaskState(done=True)
    await process_ptg._stop_ptg2_import_heartbeat(completed)
    cancelled = TaskState(done=True, cancelled=True)
    await process_ptg._stop_ptg2_import_heartbeat(cancelled)
    running = TaskState(done=False)
    await process_ptg._stop_ptg2_import_heartbeat(running)

    assert completed.result_count == 1
    assert cancelled.result_count == 0
    assert running.cancel_count == 1
