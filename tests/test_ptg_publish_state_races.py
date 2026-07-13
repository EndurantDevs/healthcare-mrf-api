import asyncio
import datetime
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import source_pointers


process_ptg = importlib.import_module("process.ptg")

SERVING_INDEX = {
    "storage": "manifest_snapshot",
    "table": "mrf.ptg2_manifest_serving_candidate",
    "price_atom_table": "mrf.ptg2_price_atom_candidate",
    "rate_count": 7,
    "serving_rates": 7,
}


class _Result:
    def __init__(self, first_entry):
        self.first_entry = first_entry

    def first(self):
        return self.first_entry


class _Transaction:
    def __init__(self, session):
        self.session = session
        self.entered = 0

    async def __aenter__(self):
        self.entered += 1
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _run_source_import():
    return asyncio.run(
        process_ptg.main(
            in_network_url="https://example.test/rates.json.gz",
            import_month="2026-07",
            import_id="publish_race_candidate",
            source_key="source_a",
        )
    )


def test_deterministic_rerun_returns_already_published_without_table_work(monkeypatch):
    calls = []
    create_stage = AsyncMock()
    cleanup = AsyncMock()
    publish_pointers = AsyncMock(
        return_value={"status": "promoted", "global_pointer": "reconciled"}
    )

    async def push(object_entries, cls, **_kwargs):
        calls.append((cls, object_entries[0]))
        assert cls is process_ptg.PTG2Snapshot
        return {
            **object_entries[0],
            "status": process_ptg.PTG2_STATUS_PUBLISHED,
            "previous_snapshot_id": "snap_previous",
            "manifest": {
                "serving_index": dict(SERVING_INDEX),
                "serving_rates": 7,
            },
        }

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(
        process_ptg,
        "_current_source_snapshot_id",
        AsyncMock(return_value="snap_previous"),
    )
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", push)
    monkeypatch.setattr(process_ptg, "_publish_ptg2_source_pointers", publish_pointers)
    monkeypatch.setattr(
        process_ptg.db,
        "transaction",
        lambda: _Transaction(SimpleNamespace()),
    )
    monkeypatch.setattr(process_ptg, "_acquire_source_pointer_gc_lock", AsyncMock())
    monkeypatch.setattr(
        process_ptg,
        "_missing_snapshot_serving_resources",
        AsyncMock(return_value=([], [])),
    )
    monkeypatch.setattr(
        process_ptg,
        "_create_ptg2_manifest_serving_stage_table",
        create_stage,
    )
    monkeypatch.setattr(
        process_ptg,
        "_drop_ptg2_snapshot_tables_for_manifest",
        cleanup,
    )

    rerun_result = _run_source_import()

    assert rerun_result["status"] == "succeeded"
    assert rerun_result["publish_status"] == "already_published"
    assert rerun_result["already_published"] is True
    assert rerun_result["serving_rates"] == 7
    assert rerun_result["pointer_reconciliation"]["global_pointer"] == "reconciled"
    assert len(calls) == 1
    assert calls[0][1]["status"] == process_ptg.PTG2_STATUS_BUILDING
    create_stage.assert_not_awaited()
    cleanup.assert_not_awaited()
    assert publish_pointers.await_args.kwargs["previous_snapshot_id"] == "snap_previous"
    assert publish_pointers.await_args.kwargs["snapshot_attributes"]["status"] == "published"


def test_building_snapshot_collision_fails_closed(monkeypatch):
    calls = []
    create_stage = AsyncMock()
    cleanup = AsyncMock()

    async def push(object_entries, cls, **_kwargs):
        calls.append((cls, object_entries[0]))
        assert cls is process_ptg.PTG2Snapshot
        return {
            **object_entries[0],
            "status": process_ptg.PTG2_STATUS_BUILDING,
            "snapshot_claim_status": "existing",
        }

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(
        process_ptg,
        "_current_source_snapshot_id",
        AsyncMock(return_value="snap_previous"),
    )
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", push)
    monkeypatch.setattr(
        process_ptg,
        "_create_ptg2_manifest_serving_stage_table",
        create_stage,
    )
    monkeypatch.setattr(
        process_ptg,
        "_drop_ptg2_snapshot_tables_for_manifest",
        cleanup,
    )

    with pytest.raises(
        process_ptg.PTG2SnapshotInProgressConflict,
        match="already being built",
    ):
        _run_source_import()

    assert len(calls) == 1
    create_stage.assert_not_awaited()
    cleanup.assert_not_awaited()


def test_source_pointer_read_failure_leaves_no_building_claim(monkeypatch):
    push = AsyncMock()
    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(
        process_ptg,
        "_current_source_snapshot_id",
        AsyncMock(side_effect=RuntimeError("database unavailable")),
    )
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", push)

    with pytest.raises(RuntimeError, match="database unavailable"):
        _run_source_import()

    push.assert_not_awaited()


def test_source_pointer_compare_and_swap_rejects_stale_candidate(monkeypatch):
    executed_statements = []

    class ConflictSession:
        async def execute(self, statement, params=None):
            sql = str(statement)
            executed_statements.append((sql, params or {}))
            if "status = 'published'" in sql:
                return _Result(("snap_stale",))
            return _Result(None)

    transaction = _Transaction(ConflictSession())
    monkeypatch.setattr(source_pointers, "_source_plan_rows", AsyncMock(return_value=[]))
    monkeypatch.setattr(source_pointers.db, "transaction", lambda: transaction)

    with pytest.raises(
        source_pointers.PTG2SourcePointerConflict,
        match="changed after import planning",
    ):
        asyncio.run(
            source_pointers._publish_ptg2_source_pointers(
                source_key="source_a",
                snapshot_id="snap_stale",
                previous_snapshot_id="snap_observed",
                import_month=datetime.date(2026, 7, 1),
                updated_at=datetime.datetime(2026, 7, 10),
            )
        )

    assert transaction.entered == 1
    assert len(executed_statements) == 3
    assert "pg_advisory_xact_lock" in executed_statements[0][0]
    assert executed_statements[0][1] == {
        "publish_lock_key": source_pointers.PTG2_SOURCE_POINTER_GC_LOCK_KEY
    }
    assert "status = 'published'" in executed_statements[1][0]
    assert "IS NOT DISTINCT FROM :previous_snapshot_id" in executed_statements[2][0]
    assert "DELETE FROM" not in executed_statements[2][0]
