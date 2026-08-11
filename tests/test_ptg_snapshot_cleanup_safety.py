# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_source_snapshot_gc as snapshot_gc
from process.ptg_parts import snapshot_cleanup
from process.ptg_parts import source_snapshot_control


def _snapshot_row(snapshot_id, status, table_name, previous_snapshot_id=None):
    return {
        "snapshot_id": snapshot_id,
        "previous_snapshot_id": previous_snapshot_id,
        "status": status,
        "manifest": {
            "serving_index": {
                "source_key": "source_a",
                "table": f"mrf.{table_name}",
            }
        },
    }


class _StaticCleanupConnection:
    def __init__(self, current_snapshot_ids, snapshot_rows):
        self.current_snapshot_ids = current_snapshot_ids
        self.snapshot_rows = snapshot_rows
        self.status_calls = []
        self.pointer_sql = ""
        self.manifest_sql = ""

    async def status(self, statement, **params):
        self.status_calls.append((statement, params))
        return 1

    async def all(self, statement, **params):
        if "current_refs" in statement:
            self.pointer_sql = statement
            assert params == {"source_key": "source_a"}
            return [
                {"snapshot_id": snapshot_id}
                for snapshot_id in self.current_snapshot_ids
            ]
        if "ptg2_snapshot" in statement:
            assert "pg_advisory_xact_lock" in self.status_calls[0][0]
            self.manifest_sql = statement
            assert params == {}
            return self.snapshot_rows
        raise AssertionError(statement)


class _AcquireConnection:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _CleanupDB:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _AcquireConnection(self.connection)


def _dropped_table_statements(connection):
    return [statement for statement, _params in connection.status_calls if "DROP TABLE" in statement]


def test_rollback_cleanup_ignores_legacy_layouts(monkeypatch):
    snapshot_rows = [
        _snapshot_row("snap_a", "published", "ptg2_serving_a"),
        _snapshot_row("snap_b", "published", "ptg2_serving_b", "snap_a"),
        _snapshot_row("snap_old", "published", "ptg2_serving_old"),
    ]
    connection = _StaticCleanupConnection(["snap_a", "snap_b"], snapshot_rows)
    monkeypatch.setattr(snapshot_cleanup, "db", _CleanupDB(connection))
    monkeypatch.setenv(snapshot_cleanup.PTG2_SOURCE_SNAPSHOT_RETAIN_LINEAGE_ENV, "1")

    asyncio.run(
        snapshot_cleanup._cleanup_old_ptg2_source_tables(
            "source_a",
            {"snap_a"},
            lock_pointer_state=True,
        )
    )

    assert connection.pointer_sql == ""
    assert connection.manifest_sql == ""
    assert connection.status_calls == []
    assert _dropped_table_statements(connection) == []


def test_locked_cleanup_never_drops_legacy_tables(monkeypatch):
    snapshot_rows = [
        _snapshot_row("snap_current", "published", "ptg2_serving_current"),
        _snapshot_row("snap_retry", "building", "ptg2_serving_retry"),
        _snapshot_row("snap_old", "failed", "ptg2_serving_old"),
    ]
    connection = _StaticCleanupConnection(["snap_current"], snapshot_rows)
    monkeypatch.setattr(snapshot_cleanup, "db", _CleanupDB(connection))
    monkeypatch.setenv(snapshot_cleanup.PTG2_SOURCE_SNAPSHOT_RETAIN_LINEAGE_ENV, "1")

    asyncio.run(
        snapshot_cleanup._cleanup_old_ptg2_source_tables(
            "source_a",
            {"snap_current"},
            lock_pointer_state=True,
        )
    )

    assert connection.pointer_sql == ""
    assert connection.manifest_sql == ""
    assert connection.status_calls == []
    assert _dropped_table_statements(connection) == []


@asynccontextmanager
async def _transaction():
    yield object()


@pytest.mark.asyncio
async def test_unlocked_cleanup_uses_only_source_table_cleanup(monkeypatch) -> None:
    """Default cleanup stays within the bounded source-table path."""

    cleanup = AsyncMock()
    monkeypatch.setattr(snapshot_cleanup, "_cleanup_source_tables", cleanup)
    await snapshot_cleanup._cleanup_old_ptg2_source_tables(
        "source-a",
        {"current"},
    )
    cleanup.assert_awaited_once_with(
        snapshot_cleanup.db,
        source_key="source-a",
        keep_snapshot_ids={"current"},
    )


@pytest.mark.asyncio
async def test_exact_retirement_keeps_source_key_boundary(monkeypatch) -> None:
    """Exact retirement keeps every pointer delete source-scoped."""

    monkeypatch.setattr(source_snapshot_control.db, "transaction", _transaction)
    monkeypatch.setattr(
        source_snapshot_control,
        "_lock_source_pointer_gc",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(return_value={"snapshot_id": "candidate"}),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "retirement_manifest_source_key",
        lambda _snapshot, _source_key: "manifest-source",
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "validate_retirement_shared_layout",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_current_references",
        AsyncMock(side_effect=[{}, {}]),
    )
    pointer_deletes = AsyncMock(return_value=(2, 1))
    monkeypatch.setattr(
        source_snapshot_control,
        "_delete_retired_source_pointers",
        pointer_deletes,
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_clear_ptg2_snapshot_cache",
        lambda: None,
    )

    retirement_result = await source_snapshot_control.retire_ptg2_source_snapshot(
        snapshot_id="candidate",
        source_key="manifest-source",
    )

    assert retirement_result["source_key"] == "manifest-source"
    assert retirement_result["deleted_plan_pointers"] == 2
    assert retirement_result["deleted_source_pointers"] == 1
    pointer_deletes.assert_awaited_once_with(
        "mrf",
        snapshot_id="candidate",
        source_key="manifest-source",
    )


def test_every_cleanup_retention_query_includes_direct_release_bindings() -> None:
    """Alternate cleanup paths retain partial release projections too."""

    assert "plan_release_snapshot_binding" in snapshot_gc._CURRENT_SNAPSHOT_IDS_SQL
    source_query = snapshot_cleanup._CURRENT_SOURCE_POINTER_SNAPSHOT_IDS_SQL
    assert "plan_release_snapshot_binding" in source_query
    assert "binding.source_key = :source_key" in source_query


def test_invalid_source_lineage_limit_uses_safe_default(monkeypatch) -> None:
    """Malformed retention configuration cannot broaden cleanup."""

    monkeypatch.setenv(
        snapshot_cleanup.PTG2_SOURCE_SNAPSHOT_RETAIN_LINEAGE_ENV,
        "not-a-number",
    )

    assert snapshot_cleanup._source_snapshot_lineage_limit() == 4
