import asyncio

import pytest

from process.ptg_parts import ptg2_source_snapshot_gc as snapshot_gc


class _FakeExecutor:
    def __init__(self):
        self.status_calls = []

    async def all(self, statement, **params):
        if "SELECT DISTINCT snapshot_id" in statement:
            assert statement.count("previous_snapshot_id AS snapshot_id") == 3
            return [{"snapshot_id": "snap_current"}]
        if 'FROM "mrf".ptg2_snapshot' in statement:
            assert "status IN ('published', 'failed')" not in statement
            assert "manifest_snapshot" not in statement
            return [
                {
                    "snapshot_id": "snap_current",
                    "status": "published",
                    "source_key": "source_a",
                    "serving_index": {
                        "storage": "manifest_snapshot",
                        "table": "mrf.ptg2_serving_current",
                        "price_atom_table": "mrf.ptg2_price_atom_current",
                    },
                },
                {
                    "snapshot_id": "snap_old",
                    "status": "failed",
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
                    "status": "failed",
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


class _ArchitectureOwnerExecutor:
    def __init__(
        self,
        *,
        current_snapshot_ids,
        snapshot_manifests,
        bytes_by_table_name,
    ):
        self.current_snapshot_ids = current_snapshot_ids
        self.snapshot_manifests = snapshot_manifests
        self.bytes_by_table_name = bytes_by_table_name

    async def all(self, statement, **params):
        if "SELECT DISTINCT snapshot_id" in statement:
            assert statement.count("previous_snapshot_id AS snapshot_id") == 3
            return [
                {"snapshot_id": snapshot_id}
                for snapshot_id in self.current_snapshot_ids
            ]
        if 'FROM "mrf".ptg2_snapshot' in statement:
            assert "status IN ('published', 'failed')" not in statement
            assert "manifest_snapshot" not in statement
            return list(self.snapshot_manifests)
        if "FROM pg_class c" in statement:
            assert params["table_names"] == sorted(self.bytes_by_table_name)
            return [
                {"table_name": table_name, "bytes": table_bytes}
                for table_name, table_bytes in self.bytes_by_table_name.items()
            ]
        raise AssertionError(statement)


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


def test_gc_protects_current_compact_table():
    executor = _ArchitectureOwnerExecutor(
        current_snapshot_ids=("snap_current_compact",),
        snapshot_manifests=(
            {
                "snapshot_id": "snap_old_manifest",
                "status": "failed",
                "source_key": "source_a",
                "serving_index": {
                    "storage": "manifest_snapshot",
                    "table": "mrf.ptg2_serving_rate_compact_shared",
                },
            },
            {
                "snapshot_id": "snap_current_compact",
                "status": "published",
                "source_key": "source_b",
                "serving_index": {
                    "storage": "db_compact_snapshot",
                    "table": "mrf.ptg2_serving_rate_compact_shared",
                },
            },
        ),
        bytes_by_table_name={},
    )

    plan = asyncio.run(snapshot_gc.build_ptg2_source_snapshot_gc_plan(executor=executor))

    assert plan.current_snapshot_ids == ("snap_current_compact",)
    assert plan.candidate_snapshot_ids == ("snap_old_manifest",)
    assert plan.tables == ()


def test_gc_protects_future_storage_owner():
    executor = _ArchitectureOwnerExecutor(
        current_snapshot_ids=("snap_current_manifest",),
        snapshot_manifests=(
            {
                "snapshot_id": "snap_current_manifest",
                "status": "published",
                "source_key": "source_a",
                "serving_index": {
                    "storage": "manifest_snapshot",
                    "table": "mrf.ptg2_serving_current",
                },
            },
            {
                "snapshot_id": "snap_old_manifest",
                "status": "failed",
                "source_key": "source_a",
                "serving_index": {
                    "storage": "manifest_snapshot",
                    "table": "mrf.ptg2_serving_shared",
                    "price_atom_table": "mrf.ptg2_price_atom_old",
                },
            },
            {
                "snapshot_id": "snap_future_owner",
                "status": "published",
                "source_key": "source_b",
                "serving_index": {
                    "storage": "future_snapshot_layout",
                    "materialized_tables": {
                        "serving": "mrf.ptg2_serving_shared",
                    },
                },
            },
        ),
        bytes_by_table_name={"ptg2_price_atom_old": 20},
    )

    plan = asyncio.run(snapshot_gc.build_ptg2_source_snapshot_gc_plan(executor=executor))

    assert plan.candidate_snapshot_ids == ("snap_old_manifest",)
    assert [(table.snapshot_id, table.table_name) for table in plan.tables] == [
        ("snap_old_manifest", "ptg2_price_atom_old")
    ]


def test_build_ptg2_source_snapshot_gc_plan_retains_three_snapshot_lineage():
    class LineageExecutor(_FakeExecutor):
        async def all(self, statement, **params):
            if "SELECT DISTINCT snapshot_id" in statement:
                return [{"snapshot_id": "snap_current"}]
            if 'FROM "mrf".ptg2_snapshot' in statement:
                return [
                    {
                        "snapshot_id": "snap_current",
                        "status": "published",
                        "previous_snapshot_id": "snap_previous",
                        "source_key": "source_a",
                        "serving_index": {"storage": "manifest_snapshot", "table": "mrf.ptg2_serving_current"},
                    },
                    {
                        "snapshot_id": "snap_previous",
                        "status": "published",
                        "previous_snapshot_id": "snap_third",
                        "source_key": "source_a",
                        "serving_index": {"storage": "manifest_snapshot", "table": "mrf.ptg2_serving_previous"},
                    },
                    {
                        "snapshot_id": "snap_third",
                        "status": "published",
                        "previous_snapshot_id": "snap_fourth",
                        "source_key": "source_a",
                        "serving_index": {"storage": "manifest_snapshot", "table": "mrf.ptg2_serving_third"},
                    },
                    {
                        "snapshot_id": "snap_fourth",
                        "status": "published",
                        "previous_snapshot_id": None,
                        "source_key": "source_a",
                        "serving_index": {"storage": "manifest_snapshot", "table": "mrf.ptg2_serving_fourth"},
                    },
                ]
            if "FROM pg_class c" in statement:
                assert params["table_names"] == ["ptg2_serving_fourth"]
                return [{"table_name": "ptg2_serving_fourth", "bytes": 100}]
            raise AssertionError(statement)

    plan = asyncio.run(
        snapshot_gc.build_ptg2_source_snapshot_gc_plan(
            executor=LineageExecutor(),
            retain_current_lineage=3,
        )
    )

    assert plan.candidate_snapshot_ids == ("snap_fourth",)
    assert [(table.snapshot_id, table.table_name) for table in plan.tables] == [
        ("snap_fourth", "ptg2_serving_fourth")
    ]


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
    snapshot_deletes = [statement for statement in status_statements if "DELETE FROM \"mrf\".ptg2_snapshot" in statement]
    assert len(snapshot_deletes) == 1
    assert "status IN ('published', 'failed')" in snapshot_deletes[0]


def test_execute_gc_replans_after_concurrent_snapshot_promotion(monkeypatch):
    events = []

    class InterleavingExecutor(_FakeExecutor):
        def __init__(self):
            super().__init__()
            self.publisher_committed = False

        async def status(self, statement, **params):
            self.status_calls.append((statement, params))
            if "pg_advisory_xact_lock" in statement:
                events.append("publish_lock_acquired")
                self.publisher_committed = True
            return 1

        async def all(self, statement, **params):
            if "SELECT DISTINCT snapshot_id" in statement:
                events.append("pointer_state_read")
                assert self.publisher_committed is True
                return [{"snapshot_id": "snap_promoted"}]
            if 'FROM "mrf".ptg2_snapshot' in statement:
                return [
                    {
                        "snapshot_id": "snap_promoted",
                        "status": "published",
                        "source_key": "source_a",
                        "serving_index": {
                            "storage": "manifest_snapshot",
                            "table": "mrf.ptg2_serving_promoted",
                        },
                    }
                ]
            raise AssertionError(statement)

    connection = InterleavingExecutor()
    monkeypatch.setattr(snapshot_gc, "db", _FakeDB(connection))
    monkeypatch.setattr(snapshot_gc, "ensure_ptg2_artifact_blob_table", _fake_ensure)

    plan = asyncio.run(snapshot_gc.execute_ptg2_source_snapshot_gc_plan(max_bytes=1024))

    assert events == ["publish_lock_acquired", "pointer_state_read"]
    assert plan.current_snapshot_ids == ("snap_promoted",)
    assert plan.candidate_snapshot_ids == ()
    destructive_statements = [
        statement
        for statement, _params in connection.status_calls
        if "DROP TABLE" in statement or "DELETE FROM" in statement
    ]
    assert destructive_statements == []


def test_execute_gc_replans_after_failed_snapshot_is_reclaimed(monkeypatch):
    class ReclaimedSnapshotExecutor(_FakeExecutor):
        def __init__(self):
            super().__init__()
            self.reclaimed_as_building = False

        async def status(self, statement, **params):
            self.status_calls.append((statement, params))
            if "LOCK TABLE \"mrf\".ptg2_snapshot" in statement:
                self.reclaimed_as_building = True
            return 1

        async def all(self, statement, **params):
            if "SELECT DISTINCT snapshot_id" in statement:
                return []
            if 'FROM "mrf".ptg2_snapshot' in statement:
                return [
                    {
                        "snapshot_id": "snap_retry",
                        "status": "building" if self.reclaimed_as_building else "failed",
                        "source_key": "source_a",
                        "serving_index": {
                            "storage": "manifest_snapshot",
                            "table": "mrf.ptg2_serving_retry",
                        },
                    }
                ]
            if "FROM pg_class c" in statement:
                return [{"table_name": "ptg2_serving_retry", "bytes": 100}]
            raise AssertionError(statement)

    connection = ReclaimedSnapshotExecutor()
    dry_run_plan = asyncio.run(snapshot_gc.build_ptg2_source_snapshot_gc_plan(executor=connection))
    assert dry_run_plan.candidate_snapshot_ids == ("snap_retry",)

    monkeypatch.setattr(snapshot_gc, "db", _FakeDB(connection))
    monkeypatch.setattr(snapshot_gc, "ensure_ptg2_artifact_blob_table", _fake_ensure)
    executed_plan = asyncio.run(snapshot_gc.execute_ptg2_source_snapshot_gc_plan(max_bytes=1024))

    assert executed_plan.candidate_snapshot_ids == ()
    destructive_statements = [
        statement
        for statement, _params in connection.status_calls
        if "DROP TABLE" in statement or "DELETE FROM" in statement
    ]
    assert destructive_statements == []


async def _fake_ensure(_schema_name=None):
    return None
