# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
import importlib
from unittest.mock import AsyncMock

import pytest


process_ptg = importlib.import_module("process.ptg")
source_pointers = importlib.import_module("process.ptg_parts.source_pointers")


class _Transaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _ExcludedValues:
    def __getattr__(self, name):
        return f"excluded.{name}"


class _InsertStatement:
    excluded = _ExcludedValues()

    def __init__(self, conflict_options_by_name):
        self.conflict_options_by_name = conflict_options_by_name

    def values(self, _row):
        return self

    def on_conflict_do_update(self, **kwargs):
        self.conflict_options_by_name.update(kwargs)
        return self

    def returning(self, *_columns):
        return self

    async def first(self):
        return None


class _ExistingSnapshotQuery:
    def where(self, *_args):
        return self

    async def first(self):
        return {
            "snapshot_id": "snap_building",
            "status": process_ptg.PTG2_STATUS_BUILDING,
            "manifest": {},
        }


class _ExistingReconciledRunQuery:
    def where(self, *_args):
        return self

    async def first(self):
        return {
            "import_run_id": "run-reconciled",
            "status": process_ptg.PTG2_STATUS_FAILED,
            "report": {
                process_ptg.PTG2_V4_STALE_METADATA_MARKER: {
                    "status": "reconciled"
                }
            },
        }


class _ClaimSession:
    def __init__(self, executed_statements):
        self.executed_statements = executed_statements

    async def execute(self, statement, params=None):
        self.executed_statements.append((str(statement), params or {}))


def test_building_snapshot_claim_only_reclaims_failed_candidate(monkeypatch):
    """Serialize a deterministic retry claim with pointer cleanup."""

    conflict_options_by_name = {}
    executed_statements = []

    monkeypatch.setattr(
        process_ptg.db,
        "insert",
        lambda *_args: _InsertStatement(conflict_options_by_name),
    )
    monkeypatch.setattr(process_ptg.db, "select", lambda *_args: _ExistingSnapshotQuery())
    monkeypatch.setattr(
        process_ptg.db,
        "transaction",
        lambda: _Transaction(_ClaimSession(executed_statements)),
    )

    snapshot_state = asyncio.run(
        process_ptg._push_ptg2_snapshot_preserving_publication(
            {
                "snapshot_id": "snap_building",
                "status": process_ptg.PTG2_STATUS_BUILDING,
            }
        )
    )

    status_clause = tuple(conflict_options_by_name["where"].clauses)[0]
    assert status_clause.right.value == process_ptg.PTG2_STATUS_FAILED
    assert process_ptg.PTG2_V4_STALE_METADATA_MARKER in (
        conflict_options_by_name["where"].compile().params.values()
    )
    assert snapshot_state["snapshot_claim_status"] == "existing"
    assert len(executed_statements) == 5
    assert "set_config('lock_timeout'" in executed_statements[0][0]
    assert "pg_advisory_xact_lock_shared" in executed_statements[1][0]
    assert "pg_advisory_xact_lock" in executed_statements[2][0]
    assert all(
        "guard_ptg2_v4_attempt" in statement
        for statement, _parameters in executed_statements[3:]
    )


def test_import_run_upsert_cannot_overwrite_reconciliation_marker(
    monkeypatch,
):
    """Fence a paused worker when its completion upsert resumes."""

    conflict_options_by_name = {}
    monkeypatch.setattr(
        process_ptg.db,
        "insert",
        lambda *_args: _InsertStatement(conflict_options_by_name),
    )
    monkeypatch.setattr(
        process_ptg.db,
        "select",
        lambda *_args: _ExistingReconciledRunQuery(),
    )
    executed_statements = []
    monkeypatch.setattr(
        process_ptg.db,
        "transaction",
        lambda: _Transaction(_ClaimSession(executed_statements)),
    )

    with pytest.raises(
        process_ptg.StaleMetadataFenceError,
        match="metadata-reconciled",
    ):
        asyncio.run(
            process_ptg._push_fenced_import_run(
                {
                    "import_run_id": "run-reconciled",
                    "status": process_ptg.PTG2_STATUS_VALIDATED,
                }
            )
        )

    assert process_ptg.PTG2_V4_STALE_METADATA_MARKER in (
        conflict_options_by_name["where"].compile().params.values()
    )
    assert len(executed_statements) == 1
    assert "guard_ptg2_v4_attempt" in executed_statements[0][0]


def test_published_reconciliation_rejects_cleaned_resources_under_lock(monkeypatch):
    """Reject cleaned storage inside the bounded compatibility transaction."""
    session = object()
    lock_events, missing_resources, pointer_cas = (
        _install_cleaned_resource_reconciliation_fakes(monkeypatch, session)
    )
    with pytest.raises(RuntimeError, match="serving resources are missing"):
        asyncio.run(
            process_ptg._reconcile_already_published_snapshot(
                snapshot_attributes={
                    "snapshot_id": "snap_published",
                    "previous_snapshot_id": None,
                    "manifest": {
                        "serving_index": {
                            "storage": "manifest_snapshot",
                            "source_key": "source_a",
                        }
                    },
                },
                snapshot_id="snap_published",
                source_key="source_a",
                import_month=datetime.date(2026, 7, 1),
            )
        )
    assert lock_events == [
        ("source_fence", "source_a"),
        ("snapshot", "mrf", "snap_published"),
    ]
    missing_resources.assert_awaited_once_with(
        "mrf",
        "snap_published",
        {"storage": "manifest_snapshot", "source_key": "source_a"},
    )
    pointer_cas.assert_not_awaited()


def _install_cleaned_resource_reconciliation_fakes(monkeypatch, session):
    """Install observable source and snapshot fences around missing storage."""
    lock_events = []

    async def acquire_source_fence(_session, *, source_key):
        assert _session is session
        lock_events.append(("source_fence", source_key))

    async def lock_snapshot(_session, _db, *, schema_name, snapshot_id):
        assert _session is session
        lock_events.append(("snapshot", schema_name, snapshot_id))

    missing_resources = AsyncMock(
        return_value=(["ptg2_serving_binary_removed"], [])
    )
    pointer_cas = AsyncMock()
    monkeypatch.setattr(
        source_pointers.db,
        "transaction",
        lambda: _Transaction(session),
    )
    monkeypatch.setattr(
        source_pointers,
        "resolve_ptg2_schema",
        lambda: "mrf",
    )
    monkeypatch.setattr(
        source_pointers,
        "_acquire_source_pointer_gc_lock",
        acquire_source_fence,
    )
    monkeypatch.setattr(
        source_pointers,
        "lock_writable_snapshot",
        lock_snapshot,
    )
    monkeypatch.setattr(
        process_ptg,
        "_missing_snapshot_serving_resources",
        missing_resources,
    )
    monkeypatch.setattr(
        source_pointers,
        "_compare_and_swap_source_pointer",
        pointer_cas,
    )
    monkeypatch.setattr(
        process_ptg,
        "_current_source_snapshot_id",
        AsyncMock(return_value="snap_published"),
    )
    return lock_events, missing_resources, pointer_cas
