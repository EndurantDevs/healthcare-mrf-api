# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
from unittest.mock import AsyncMock

from process.ptg_parts import ptg2_source_snapshot_gc as snapshot_gc


def _strict_index(**values):
    return {
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        **values,
    }


class _Executor:
    def __init__(self, snapshot_rows, current_snapshot_ids=()):
        self.snapshot_rows = snapshot_rows
        self.current_snapshot_ids = current_snapshot_ids
        self.status_calls = []

    async def all(self, statement, **_params):
        if "SELECT DISTINCT snapshot_id" in statement:
            return [{"snapshot_id": value} for value in self.current_snapshot_ids]
        if 'FROM "mrf".ptg2_snapshot' in statement:
            return self.snapshot_rows
        raise AssertionError(statement)

    async def status(self, statement, **params):
        self.status_calls.append((statement, params))
        return 1


class _Acquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _DB:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _Acquire(self.connection)


def test_gc_plan_rejects_non_v3_manifests_without_table_discovery():
    executor = _Executor([
        {
            "snapshot_id": "legacy",
            "status": "failed",
            "source_key": "source_a",
            "serving_index": {"storage": "manifest_snapshot", "table": "ptg2_serving_old"},
        },
    ])

    plan = asyncio.run(snapshot_gc.build_ptg2_source_snapshot_gc_plan(executor=executor))

    assert plan.candidate_snapshot_ids == ()
    assert plan.tables == ()


def test_gc_plan_selects_only_unreferenced_strict_v3_snapshots():
    executor = _Executor(
        [
            {
                "snapshot_id": "current",
                "status": "published",
                "source_key": "source_a",
                "serving_index": _strict_index(),
            },
            {
                "snapshot_id": "failed",
                "status": "failed",
                "source_key": "source_a",
                "serving_index": _strict_index(),
            },
            {
                "snapshot_id": "stale",
                "status": "building",
                "stale_building": True,
                "source_key": "source_b",
                "serving_index": _strict_index(),
            },
        ],
        current_snapshot_ids=("current",),
    )

    plan = asyncio.run(snapshot_gc.build_ptg2_source_snapshot_gc_plan(executor=executor))

    assert plan.candidate_snapshot_ids == ("failed", "stale")
    assert plan.shared_snapshot_ids == ("failed", "stale")
    assert plan.tables == ()


def test_execute_gc_deletes_v3_metadata_with_strict_sql_admission(monkeypatch):
    connection = _Executor([
        {
            "snapshot_id": "failed",
            "status": "failed",
            "source_key": "source_a",
            "serving_index": _strict_index(),
        },
    ])
    monkeypatch.setattr(snapshot_gc, "db", _DB(connection))

    async def ensure(_schema_name=None):
        return None

    monkeypatch.setattr(snapshot_gc, "ensure_ptg2_artifact_blob_table", ensure)
    release = AsyncMock()
    monkeypatch.setattr(
        snapshot_gc,
        "release_unbound_ptg2_shared_layouts",
        release,
    )
    plan = asyncio.run(snapshot_gc.execute_ptg2_source_snapshot_gc_plan())

    assert plan.candidate_snapshot_ids == ("failed",)
    statements = [statement for statement, _params in connection.status_calls]
    assert not any("DROP TABLE IF EXISTS" in statement for statement in statements)
    snapshot_delete = next(statement for statement in statements if "DELETE FROM \"mrf\".ptg2_snapshot" in statement)
    assert "arch_version" in snapshot_delete
    assert "storage_generation" in snapshot_delete
    release.assert_awaited_once_with(
        schema_name="mrf",
        executor=connection,
        require_shared=True,
    )
