import asyncio

import pytest

from process.ptg_parts import ptg2_source_snapshot_gc as snapshot_gc


class _FakeExecutor:
    def __init__(self):
        self.status_calls = []

    async def all(self, statement, **params):
        if "SELECT DISTINCT snapshot_id" in statement:
            return [{"snapshot_id": "snap_current"}]
        if 'FROM "mrf".ptg2_snapshot' in statement:
            return [
                {
                    "snapshot_id": "snap_current",
                    "source_key": "source_a",
                    "serving_index": {
                        "storage": "manifest_snapshot",
                        "table": "mrf.ptg2_serving_current",
                        "price_atom_table": "mrf.ptg2_price_atom_current",
                    },
                },
                {
                    "snapshot_id": "snap_old",
                    "source_key": "source_a",
                    "serving_index": {
                        "storage": "manifest_snapshot",
                        "table": "mrf.ptg2_serving_old",
                        "price_atom_table": "mrf.ptg2_price_atom_old",
                        "provider_group_member_table": "mrf.ptg2_provider_group_member_old",
                        "ignored_table": "mrf.provider_directory_canonical_resource",
                    },
                },
                {
                    "snapshot_id": "snap_old_missing_physical_table",
                    "source_key": "source_b",
                    "serving_index": {
                        "storage": "manifest_snapshot",
                        "table": "mrf.ptg2_serving_missing",
                    },
                },
            ]
        if "FROM pg_class c" in statement:
            assert params["table_names"] == [
                "ptg2_price_atom_old",
                "ptg2_provider_group_member_old",
                "ptg2_serving_missing",
                "ptg2_serving_old",
            ]
            return [
                {"table_name": "ptg2_price_atom_old", "bytes": 20},
                {"table_name": "ptg2_provider_group_member_old", "bytes": 30},
                {"table_name": "ptg2_serving_old", "bytes": 100},
            ]
        raise AssertionError(statement)

    async def status(self, statement, **params):
        self.status_calls.append((statement, params))
        return 1


class _FakeAcquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeDB:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _FakeAcquire(self.connection)


def test_build_ptg2_source_snapshot_gc_plan_targets_only_non_current_manifest_tables():
    executor = _FakeExecutor()

    plan = asyncio.run(snapshot_gc.build_ptg2_source_snapshot_gc_plan(executor=executor))

    assert plan.current_snapshot_ids == ("snap_current",)
    assert plan.candidate_snapshot_ids == ("snap_old", "snap_old_missing_physical_table")
    assert [(table.snapshot_id, table.table_name, table.bytes) for table in plan.tables] == [
        ("snap_old", "ptg2_serving_old", 100),
        ("snap_old", "ptg2_price_atom_old", 20),
        ("snap_old", "ptg2_provider_group_member_old", 30),
    ]
    assert plan.table_count == 3
    assert plan.total_bytes == 150


def test_validate_ptg2_source_snapshot_gc_plan_enforces_bounds():
    plan = snapshot_gc.PTG2SourceSnapshotGCPlan(
        current_snapshot_ids=(),
        candidate_snapshot_ids=("snap_a", "snap_b"),
        tables=(
            snapshot_gc.PTG2SnapshotGCTable("snap_a", "source_a", "ptg2_serving_a", 10),
            snapshot_gc.PTG2SnapshotGCTable("snap_b", "source_b", "ptg2_serving_b", 20),
        ),
    )

    with pytest.raises(RuntimeError, match="candidate snapshot count"):
        snapshot_gc.validate_ptg2_source_snapshot_gc_plan(plan, max_snapshots=1, max_tables=10, max_bytes=100)
    with pytest.raises(RuntimeError, match="candidate table count"):
        snapshot_gc.validate_ptg2_source_snapshot_gc_plan(plan, max_snapshots=10, max_tables=1, max_bytes=100)
    with pytest.raises(RuntimeError, match="candidate bytes"):
        snapshot_gc.validate_ptg2_source_snapshot_gc_plan(plan, max_snapshots=10, max_tables=10, max_bytes=1)


def test_execute_ptg2_source_snapshot_gc_plan_recomputes_and_deletes_metadata(monkeypatch):
    connection = _FakeExecutor()
    monkeypatch.setattr(snapshot_gc, "db", _FakeDB(connection))

    # ensure_ptg2_artifact_blob_table resolves `db` inside ptg2_artifact_blobs, so the
    # snapshot_gc.db patch above does not cover it: unpatched it DDLs the real local
    # database and fails whenever an earlier test leaves the shared asyncpg pool on a
    # dead event loop (suite-order flake).
    ensure_calls: list[str | None] = []

    async def _fake_ensure_blob_table(schema_name=None):
        ensure_calls.append(schema_name)

    monkeypatch.setattr(snapshot_gc, "ensure_ptg2_artifact_blob_table", _fake_ensure_blob_table)

    plan = asyncio.run(snapshot_gc.execute_ptg2_source_snapshot_gc_plan(max_bytes=1024))

    assert ensure_calls == ["mrf"]

    assert plan.table_count == 3
    status_statements = [statement for statement, _params in connection.status_calls]
    assert "set_config('lock_timeout'" in status_statements[0]
    assert any('DROP TABLE IF EXISTS "mrf"."ptg2_serving_old"' in statement for statement in status_statements)
    assert any("DELETE FROM \"mrf\".ptg2_artifact_manifest" in statement for statement in status_statements)
    assert any("DELETE FROM \"mrf\".ptg2_snapshot" in statement for statement in status_statements)
