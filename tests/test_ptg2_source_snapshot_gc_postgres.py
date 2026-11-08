# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL time-zone proof for source-snapshot garbage collection."""

from __future__ import annotations

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.connection import Database
from process.ptg_parts import ptg2_source_snapshot_gc as snapshot_gc
from process.ptg_parts import source_snapshot_control
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_lifecycle_lock, lifecycle_database_sqlstate
from process.ptg_parts.source_snapshot_references import source_binding_schema
from tests.ptg2_shared_gc_postgres_support import _insert_gc_layout_fixture
from tests.ptg2_shared_gc_schema_support import _create_gc_block_schema, _create_gc_layout_schema


async def _create_gc_pointer_tables(connection, schema: str) -> None:
    await connection.status(
        f"""
        CREATE TABLE {schema}.ptg2_current_snapshot (
            slot varchar(32) PRIMARY KEY,
            snapshot_id varchar(96),
            previous_snapshot_id varchar(96)
        )
        """
    )
    for table_name, key_name in (
        ("ptg2_current_source_snapshot", "source_key"),
        ("ptg2_current_plan_source", "plan_source_key"),
    ):
        await connection.status(
            f"""
            CREATE TABLE {schema}.{table_name} (
                {key_name} varchar(96) PRIMARY KEY,
                snapshot_id varchar(96),
                previous_snapshot_id varchar(96)
            )
            """
        )
    await connection.status(
        f"""
        CREATE TABLE {schema}.ptg2_snapshot_pin (
            snapshot_id varchar(96) PRIMARY KEY
        )
        """
    )
    await connection.status(f"CREATE TABLE {schema}.plan_release_snapshot_pin (snapshot_id varchar(128) PRIMARY KEY)")
    await connection.status(
        f"""
        CREATE TABLE {schema}.plan_release_snapshot_binding (
            snapshot_id varchar(96) PRIMARY KEY
        )
        """
    )


async def _create_gc_lifecycle_tables(connection, schema: str) -> None:
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


async def _insert_fresh_build(connection, schema: str) -> None:
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
             timezone('UTC', transaction_timestamp()) - INTERVAL '30 minutes')
        """
    )


async def _assert_stale_cutoff(connection, schema: str, schema_name: str) -> None:
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
    await connection.status(
        f"""
        INSERT INTO {schema}.plan_release_snapshot_binding (snapshot_id)
        VALUES ('fresh-building')
        """
    )
    protected_plan = await snapshot_gc.build_ptg2_source_snapshot_gc_plan(
        schema_name=schema_name,
        executor=connection,
        stale_build_seconds=3_600,
    )
    assert protected_plan.candidate_snapshot_ids == ()
    await connection.status(
        f"""
        DELETE FROM {schema}.plan_release_snapshot_binding
         WHERE snapshot_id = 'fresh-building'
        """
    )
    stale_plan = await snapshot_gc.build_ptg2_source_snapshot_gc_plan(
        schema_name=schema_name,
        executor=connection,
        stale_build_seconds=3_600,
    )
    assert stale_plan.candidate_snapshot_ids == ("fresh-building",)


@pytest.mark.asyncio
async def test_real_postgres_stale_cutoff_is_utc_naive_under_non_utc_session(
    monkeypatch,
) -> None:
    """Use UTC-naive cutoffs even when the PostgreSQL session is non-UTC."""

    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST=1 for the isolated PostgreSQL test")
    database = Database()
    schema_name = f"ptg2_source_gc_utc_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", schema_name + "_absent")
    schema = f'"{schema_name}"'
    monkeypatch.setattr(
        snapshot_gc,
        "require_migration_owned_tables",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        snapshot_gc,
        "build_shared_layout_release_plan",
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
            await _create_gc_pointer_tables(connection, schema)
            await _create_gc_lifecycle_tables(connection, schema)
            await _insert_fresh_build(connection, schema)
            await _assert_stale_cutoff(connection, schema, schema_name)
    finally:
        try:
            async with database.acquire() as connection:
                await connection.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()


async def _create_source_gc_fixture(connection, schema, control):
    """Reuse native shared-layout storage and add the snapshot-control relations."""
    await _create_gc_layout_schema(connection, schema)
    await _create_gc_block_schema(connection, schema)
    await _create_gc_pointer_tables(connection, schema)
    await connection.status(
        f"ALTER TABLE {schema}.ptg2_snapshot_pin ADD COLUMN owner_type text,ADD COLUMN owner_id text"
    )
    await connection.status(f"ALTER TABLE {schema}.plan_release_snapshot_pin ADD COLUMN serving_revision_id text")
    await connection.status(
        f"ALTER TABLE {schema}.plan_release_snapshot_binding ADD COLUMN serving_revision_id text,"
        "ADD COLUMN role text,ADD COLUMN binding_ordinal integer"
    )
    await connection.status(
        f"ALTER TABLE {schema}.ptg2_snapshot ADD COLUMN import_run_id text, "
        "ADD COLUMN status text NOT NULL DEFAULT 'published', ADD COLUMN previous_snapshot_id text, "
        "ADD COLUMN created_at timestamp NOT NULL DEFAULT timezone('UTC',transaction_timestamp())"
    )
    await connection.status(
        f"CREATE TABLE {schema}.ptg2_import_run(import_run_id text PRIMARY KEY,status text,"
        "started_at timestamp,heartbeat_at timestamp)"
    )
    await connection.status(f"ALTER TABLE {schema}.ptg2_v3_snapshot_scope ADD COLUMN snapshot_id text")
    await connection.status(
        f"CREATE TABLE {schema}.ptg2_artifact_manifest(artifact_id text PRIMARY KEY,snapshot_id text)"
    )
    await connection.status(f"CREATE TABLE {schema}.ptg2_artifact_blob_chunk(artifact_id text,payload bytea)")
    await connection.status(
        f"CREATE TABLE {schema}.plan_release_serving_revision(serving_revision_id text PRIMARY KEY,source_manifest jsonb)"
    )
    await connection.status(
        f"CREATE TABLE {control}.hp_plan_serving_revision(serving_revision_id text PRIMARY KEY,status text)"
    )
    await connection.status(
        f"CREATE TABLE {control}.hp_plan_release_binding(serving_revision_id text,role text,ordinal integer,"
        "snapshot_id text,PRIMARY KEY(serving_revision_id,role,ordinal)) PARTITION BY LIST(serving_revision_id)"
    )
    await connection.status(
        f"CREATE TABLE {control}.retained_cohort PARTITION OF {control}.hp_plan_release_binding FOR VALUES IN ('retired')"
    )
    await _insert_gc_layout_fixture(connection, schema)
    await connection.status(
        f"UPDATE {schema}.ptg2_snapshot SET manifest=jsonb_build_object('serving_index',jsonb_build_object("
        "'arch_version','postgres_binary_v3','storage_generation','shared_blocks_v3','source_key','source-a'))"
    )
    await connection.status(
        f"INSERT INTO {schema}.ptg2_v3_snapshot_scope(snapshot_key,snapshot_id) VALUES(10,'selected-snapshot')"
    )
    await connection.status(f"INSERT INTO {schema}.ptg2_artifact_manifest VALUES('artifact','selected-snapshot')")
    await connection.status(f"INSERT INTO {schema}.ptg2_artifact_blob_chunk VALUES('artifact',decode('abcd','hex'))")


@asynccontextmanager
async def _source_gc_scope(monkeypatch):
    """Own exact disposable schemas and keep the production transaction executor."""
    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST=1 for the isolated PostgreSQL test")
    database = Database()
    schema_name = f"source_gc_{uuid.uuid4().hex}"
    control_name = schema_name + "_control"
    schema, control = f'"{schema_name}"', f'"{control_name}"'
    monkeypatch.setenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", control_name)
    monkeypatch.setattr(snapshot_gc, "db", database)
    monkeypatch.setattr(source_snapshot_control, "db", database)
    await database.connect()
    try:
        async with database.acquire() as connection:
            await connection.status(f"CREATE SCHEMA {schema}")
            await connection.status(f"CREATE SCHEMA {control}")
            await _create_source_gc_fixture(connection, schema, control)
        yield database, schema_name, schema, control
    finally:
        try:
            async with database.acquire() as connection:
                await connection.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
                await connection.status(f"DROP SCHEMA IF EXISTS {control} CASCADE")
        finally:
            await database.disconnect()


async def _source_gc_inventory(database, schema):
    """Capture real rows so rollback checks payload as well as control counts."""
    async with database.acquire() as connection:
        return {
            table: await connection.all(f"SELECT * FROM {schema}.{table}")
            for table in (
                "ptg2_snapshot",
                "ptg2_v3_snapshot_scope",
                "ptg2_v3_snapshot_binding",
                "ptg2_artifact_manifest",
                "ptg2_artifact_blob_chunk",
                "ptg2_v3_snapshot_layout",
                "ptg2_v3_code",
            )
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("padded_manifest", [False, True])
async def test_native_gc_exact_delete_rollback(monkeypatch, padded_manifest):
    """Actual GC either deletes the selected snapshot or rolls back all its payload."""
    async with _source_gc_scope(monkeypatch) as (database, schema_name, schema, _control):
        if padded_manifest:
            async with database.acquire() as connection:
                await connection.status(
                    f"UPDATE {schema}.ptg2_snapshot SET manifest=jsonb_set(manifest,"
                    "'{serving_index,arch_version}','\" postgres_binary_v3 \"'::jsonb)"
                )
        before = await _source_gc_inventory(database, schema)
        if padded_manifest:
            with pytest.raises(RuntimeError, match="snapshot deletion set changed"):
                await snapshot_gc.execute_source_gc_plan(schema_name=schema_name)
            assert await _source_gc_inventory(database, schema) == before
        else:
            plan = await snapshot_gc.execute_source_gc_plan(schema_name=schema_name)
            assert plan.candidate_snapshot_ids == ("selected-snapshot",)
            after = await _source_gc_inventory(database, schema)
            assert all(not rows for table, rows in after.items() if table != "ptg2_v3_snapshot_layout")
            assert [row.snapshot_key for row in after["ptg2_v3_snapshot_layout"]] == [20]


@pytest.mark.asyncio
async def test_native_gc_retains_source_commit(monkeypatch):
    """A fenced Source commit becomes visible before GC checks retained history."""
    async with _source_gc_scope(monkeypatch) as (database, schema_name, schema, control):
        before = await _source_gc_inventory(database, schema)
        async with database.acquire() as publisher:
            await acquire_ptg2_lifecycle_lock(publisher)
            await publisher.status(f"INSERT INTO {control}.hp_plan_serving_revision VALUES('retired','retired')")
            await publisher.status(
                f"INSERT INTO {control}.hp_plan_release_binding VALUES('retired','in_network',0,'selected-snapshot')"
            )
            collection = asyncio.create_task(snapshot_gc.execute_source_gc_plan(schema_name=schema_name))
            try:
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(asyncio.shield(collection), timeout=0.05)
            except BaseException:
                collection.cancel()
                await asyncio.gather(collection, return_exceptions=True)
                raise
        plan = await collection
        assert plan.current_snapshot_ids == ("selected-snapshot",)
        assert plan.candidate_snapshot_ids == ()
        assert await _source_gc_inventory(database, schema) == before
        references = await source_snapshot_control._current_references(schema_name, "selected-snapshot")
        assert references["plan_release_bindings"] == ["retired:in_network:0"]


@pytest.mark.asyncio
async def test_native_gc_optional_source_storage(monkeypatch):
    """Control-only installations remain supported; partial or claimed storage refuses."""
    async with _source_gc_scope(monkeypatch) as (database, schema_name, schema, control):
        async with database.acquire() as connection:
            await connection.status(f"DROP TABLE {control}.hp_plan_release_binding CASCADE")
            with pytest.raises(RuntimeError, match="Source release binding storage is missing"):
                await source_binding_schema(connection, schema_name)
            await connection.status(f"DROP TABLE {control}.hp_plan_serving_revision")
            assert await source_binding_schema(connection, schema_name) is None
            for marker in ('"source-plan-release-bindings-v1"', '"unknown"', "null", "{}"):
                await connection.status(
                    f"INSERT INTO {schema}.plan_release_serving_revision VALUES('claimed',"
                    "jsonb_build_object('binding_storage',CAST(:marker AS jsonb))) "
                    "ON CONFLICT(serving_revision_id) DO UPDATE SET source_manifest=excluded.source_manifest",
                    marker=marker,
                )
                with pytest.raises(RuntimeError, match="Claimed Source release binding storage is missing"):
                    await source_binding_schema(connection, schema_name)
            await connection.status(f"DELETE FROM {schema}.plan_release_serving_revision")
        plan = await snapshot_gc.execute_source_gc_plan(schema_name=schema_name)
        assert plan.candidate_snapshot_ids == ("selected-snapshot",)


@pytest.mark.asyncio
async def test_native_source_permission_failure_propagates(monkeypatch):
    """Existing unreadable Source bindings cannot be treated as absent."""
    from sqlalchemy.exc import DBAPIError

    async with _source_gc_scope(monkeypatch) as (database, schema_name, schema, control):
        role = schema_name + "_reader"
        async with database.transaction() as session:
            with pytest.raises(DBAPIError) as denied:
                async with session.begin_nested():
                    connection = source_snapshot_control._TransactionExecutor(session)
                    await connection.status(f'CREATE ROLE "{role}" NOLOGIN')
                    await connection.status(f'GRANT USAGE ON SCHEMA {schema},{control} TO "{role}"')
                    await connection.status(f'GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO "{role}"')
                    await connection.status(f'SET LOCAL ROLE "{role}"')
                    await snapshot_gc._load_current_snapshot_ids(connection, schema_name)
            assert lifecycle_database_sqlstate(denied.value) == "42501"
            assert await session.scalar(database.text("SELECT to_regrole(:role)"), {"role": role}) is None
