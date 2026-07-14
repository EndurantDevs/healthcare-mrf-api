# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import os
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.connection import Database
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
        self.all_calls = []
        self.present_tables = set(
            snapshot_gc.PTG2_V3_MIGRATION_OWNED_TABLE_NAMES
        )

    async def all(self, statement, **params):
        self.all_calls.append((statement, params))
        if "FROM information_schema.tables" in statement:
            return [
                {"table_name": table_name}
                for table_name in sorted(self.present_tables)
            ]
        if "SELECT DISTINCT snapshot_id" in statement:
            return [{"snapshot_id": value} for value in self.current_snapshot_ids]
        if "SELECT DISTINCT snapshot_key" in statement:
            return [{"snapshot_key": 10}]
        if 'FROM "mrf".ptg2_snapshot' in statement:
            return self.snapshot_rows
        if "WITH eligible_layouts AS MATERIALIZED" in statement:
            return [
                {
                    "logical_layout_count": 0,
                    "candidate_hash_count": 0,
                    "stored_bytes": 0,
                }
            ]
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


def test_gc_plan_never_age_deletes_validated_candidate_without_attestation():
    executor = _Executor(
        [
            {
                "snapshot_id": "validated-awaiting-audit",
                "status": "validated",
                # Exercise the Python admission guard independently of the SQL
                # status predicate used to compute stale_building.
                "stale_building": True,
                "source_key": "source-a",
                "serving_index": _strict_index(),
            }
        ]
    )

    plan = asyncio.run(
        snapshot_gc.build_ptg2_source_snapshot_gc_plan(executor=executor)
    )

    assert plan.candidate_snapshot_ids == ()
    assert plan.shared_snapshot_ids == ()
    assert plan.total_bytes == 0


def test_gc_stale_sql_compares_utc_naive_timestamps_in_utc():
    executor = _Executor([])

    asyncio.run(snapshot_gc.build_ptg2_source_snapshot_gc_plan(executor=executor))

    snapshot_query = next(
        statement
        for statement, _params in executor.all_calls
        if 'FROM "mrf".ptg2_snapshot AS snapshot' in statement
    )
    assert snapshot_query.count("timezone('UTC', transaction_timestamp())") == 2
    assert "'-infinity'::timestamp" in snapshot_query
    assert "candidate_audit_attestation" not in snapshot_query


def test_gc_schema_resolution_matches_alembic_alias_rules(monkeypatch):
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.setenv("DB_SCHEMA", "legacy_ptg")
    assert snapshot_gc.resolve_ptg2_schema() == "legacy_ptg"

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_ptg")
    with pytest.raises(RuntimeError, match="must identify the same schema"):
        snapshot_gc.resolve_ptg2_schema()


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
        layout_keys=(10,),
    )


@pytest.mark.asyncio
async def test_real_postgres_stale_cutoff_is_utc_naive_under_non_utc_session(
    monkeypatch,
):
    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip(
            "set HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST=1 for the isolated "
            "PostgreSQL test"
        )

    database = Database()
    schema_name = f"ptg2_source_gc_utc_{uuid.uuid4().hex}"
    schema = f'"{schema_name}"'
    monkeypatch.setattr(
        snapshot_gc,
        "require_ptg2_v3_migration_owned_tables",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        snapshot_gc,
        "build_ptg2_shared_layout_release_plan",
        AsyncMock(
            return_value=SimpleNamespace(
                logical_layout_count=0,
                candidate_hash_count=0,
                stored_bytes=0,
            )
        ),
    )
    await database.connect()
    try:
        async with database.acquire() as connection:
            await connection.status(f"CREATE SCHEMA {schema}")
            await connection.status(
                f"""
                CREATE TABLE {schema}.ptg2_current_snapshot (
                    slot varchar(32) PRIMARY KEY,
                    snapshot_id varchar(96),
                    previous_snapshot_id varchar(96)
                )
                """
            )
            await connection.status(
                f"""
                CREATE TABLE {schema}.ptg2_current_source_snapshot (
                    source_key varchar(96) PRIMARY KEY,
                    snapshot_id varchar(96),
                    previous_snapshot_id varchar(96)
                )
                """
            )
            await connection.status(
                f"""
                CREATE TABLE {schema}.ptg2_current_plan_source (
                    plan_source_key varchar(96) PRIMARY KEY,
                    snapshot_id varchar(96),
                    previous_snapshot_id varchar(96)
                )
                """
            )
            await connection.status(
                f"""
                CREATE TABLE {schema}.ptg2_import_run (
                    import_run_id varchar(96) PRIMARY KEY,
                    status varchar(32) NOT NULL,
                    started_at timestamp without time zone,
                    heartbeat_at timestamp without time zone
                )
                """
            )
            await connection.status(
                f"""
                CREATE TABLE {schema}.ptg2_snapshot (
                    snapshot_id varchar(96) PRIMARY KEY,
                    import_run_id varchar(96),
                    status varchar(32) NOT NULL,
                    previous_snapshot_id varchar(96),
                    manifest jsonb NOT NULL,
                    created_at timestamp without time zone NOT NULL
                )
                """
            )
            await connection.status("SET TIME ZONE 'Europe/Prague'")
            await connection.status(
                f"""
                INSERT INTO {schema}.ptg2_import_run
                    (import_run_id, status, started_at, heartbeat_at)
                VALUES
                    ('run-fresh', 'failed',
                     timezone('UTC', transaction_timestamp()),
                     timezone('UTC', transaction_timestamp()))
                """
            )
            await connection.status(
                f"""
                INSERT INTO {schema}.ptg2_snapshot
                    (snapshot_id, import_run_id, status, previous_snapshot_id,
                     manifest, created_at)
                VALUES
                    ('fresh-building', 'run-fresh', 'building', NULL,
                     jsonb_build_object(
                         'serving_index',
                         jsonb_build_object(
                             'arch_version', 'postgres_binary_v3',
                             'storage_generation', 'shared_blocks_v3',
                             'source_key', 'source-a'
                         )
                     ),
                     timezone('UTC', transaction_timestamp())
                         - INTERVAL '30 minutes')
                """
            )

            fresh_plan = await snapshot_gc.build_ptg2_source_snapshot_gc_plan(
                schema_name=schema_name,
                executor=connection,
                stale_build_seconds=3_600,
            )
            assert fresh_plan.candidate_snapshot_ids == ()

            await connection.status(
                f"""
                UPDATE {schema}.ptg2_snapshot
                   SET created_at = timezone('UTC', transaction_timestamp())
                                    - INTERVAL '2 hours'
                 WHERE snapshot_id = 'fresh-building'
                """
            )
            stale_plan = await snapshot_gc.build_ptg2_source_snapshot_gc_plan(
                schema_name=schema_name,
                executor=connection,
                stale_build_seconds=3_600,
            )
            assert stale_plan.candidate_snapshot_ids == ("fresh-building",)
    finally:
        try:
            async with database.acquire() as connection:
                await connection.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()
