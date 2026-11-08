# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from db.connection import Database
from process.ptg_parts import ptg2_shared_gc as shared_gc
from process.ptg_parts import ptg2_source_snapshot_gc as source_snapshot_gc
from process.ptg_parts import snapshot_cleanup

from tests.ptg2_shared_gc_test_support import (
    _SharedGCExecutor,
    _SourceGCProjectionExecutor,
    _hash,
    _patch_v4_abandonment_pipeline,
)
from tests.ptg2_shared_gc_postgres_support import (
    _assert_completed_v4_abandonment,
    _assert_gc_fixture_storage,
    _assert_partial_v4_abandonment,
    _assert_v4_history_cleanup_defers,
    _cancel_after_build_pin_batch,
    _create_gc_block_schema,
    _create_gc_layout_schema,
    _create_snapshot_history_gc,
    _create_v4_abandonment_schema,
    _drop_test_schema_and_disconnect,
    _insert_gc_block_fixture,
    _insert_gc_layout_fixture,
    _insert_v4_abandonment_fixture,
    _release_and_sweep_gc_fixture,
    _snapshot_history_gc_scope,
)


@pytest.mark.asyncio
async def test_real_postgres_v4_abandonment_resumes_after_cancellation(
    monkeypatch,
):
    """A canceled bounded cleanup keeps reachability and resumes exactly."""

    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST=1")

    database = Database()
    schema_name = f"ptg2_v4_abandon_{uuid.uuid4().hex}"
    schema = f'"{schema_name}"'
    build_token = "a" * 32
    await database.connect()
    monkeypatch.setattr(shared_gc, "db", database)
    try:
        async with database.acquire() as connection:
            await connection.status(f"CREATE SCHEMA {schema}")
            await _create_v4_abandonment_schema(connection, schema)
            block_hashes = await _insert_v4_abandonment_fixture(
                connection,
                schema,
                build_token=build_token,
            )
            await _create_snapshot_history_gc(connection, schema, (77, 78))

        with pytest.raises(asyncio.CancelledError):
            await shared_gc.abandon_owned_v4_layout(
                schema_name=schema_name,
                snapshot_key=77,
                build_token=build_token,
                grace_seconds=60,
                progress_callback=_cancel_after_build_pin_batch,
                options=shared_gc.PTG2V4AbandonmentOptions(batch_rows=2),
            )
        await _assert_partial_v4_abandonment(
            database,
            schema,
            build_token=build_token,
        )

        await _assert_v4_history_cleanup_defers(schema_name, build_token)
        resumed = await shared_gc.abandon_owned_v4_layout(
            schema_name=schema_name,
            snapshot_key=77,
            build_token=build_token,
            grace_seconds=60,
            options=shared_gc.PTG2V4AbandonmentOptions(batch_rows=2),
        )

        assert resumed == shared_gc.PTG2SharedLayoutGCStats(1, 5, 5)
        await _assert_completed_v4_abandonment(
            database,
            schema,
            block_hashes,
        )
    finally:
        await _drop_test_schema_and_disconnect(database, schema)


@pytest.mark.asyncio
async def test_real_postgres_history_cleanup_is_bounded_and_keeps_other_snapshots():
    """Each committed set-delete advances only its admitted snapshot and heap budget."""
    async with _snapshot_history_gc_scope() as (database, schema_name, schema):
        async with database.acquire() as connection:
            await connection.status(f"DELETE FROM {schema}.ptg2_v3_snapshot_binding WHERE snapshot_key=10")
        for remaining in (3, 1, 0):
            async with database.acquire() as connection:
                stats = await shared_gc._release_layouts_ready(
                    connection, schema_name=schema_name, building_max_age_seconds=21600,
                    grace_seconds=60, max_layouts=1, layout_keys=(10,), dense_batch_rows=2,
                )
                assert stats.logical_layout_count == int(remaining == 0)
            async with database.acquire() as connection:
                for table in ("ptg2_v4_npi_scope", "ptg2_v4_provider_component"):
                    assert await connection.scalar(f"SELECT count(*) FROM {schema}.{table} WHERE snapshot_key=10") == remaining
                    assert await connection.scalar(f"SELECT count(*) FROM {schema}.{table} WHERE snapshot_key=20") == 5
        async with database.acquire() as connection:
            assert await connection.scalar(f"SELECT count(*) FROM {schema}.ptg2_v3_snapshot_layout WHERE snapshot_key=20") == 1


@pytest.mark.asyncio
async def test_real_postgres_cleanup_fences_gc_without_locking_each_cas_row():
    """A cleanup step excludes the sweeper while leaving referenced CAS rows unlocked."""
    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip("requires disposable PostgreSQL GC tests")
    database, contender = Database(), Database()
    schema_name = f"ptg_gc_fence_{uuid.uuid4().hex}"
    schema = f'"{schema_name}"'
    await database.connect()
    await contender.connect()
    try:
        async with database.acquire() as connection:
            await connection.status(f"CREATE SCHEMA {schema}")
            await connection.status(f"CREATE TABLE {schema}.ptg2_v3_block(block_hash bytea PRIMARY KEY,stored_byte_count bigint)")
            await connection.status(f"INSERT INTO {schema}.ptg2_v3_block VALUES(:hash,7)", hash=_hash(44))
        context = shared_gc._owned_v4_abandonment_context(
            schema_name=schema_name, snapshot_key=77, build_token="owned",
            grace_seconds=60, options=shared_gc.PTG2V4AbandonmentOptions(),
        )

        async def inspect_step(connection):
            assert await shared_gc._owned_v4_stored_bytes(connection, context=context, block_hashes=(_hash(44),)) == 7
            async with contender.acquire() as other:
                assert not await other.scalar("SELECT pg_try_advisory_xact_lock(hashtext('ptg2_source_pointer_gc_v1'))")
                assert await other.scalar(f"SELECT stored_byte_count FROM {schema}.ptg2_v3_block FOR UPDATE NOWAIT") == 7

        async with database.acquire() as connection:
            await shared_gc._run_owned_v4_step(inspect_step, context=context, executor=connection, step_guard=None)
        async with contender.acquire() as other:
            assert await other.scalar("SELECT pg_try_advisory_xact_lock(hashtext('ptg2_source_pointer_gc_v1'))")
    finally:
        await contender.disconnect()
        await _drop_test_schema_and_disconnect(database, schema)


@pytest.mark.asyncio
async def test_real_postgres_candidate_scoped_release_and_sweep_sql():
    """Exercise candidate-scoped layout release and block sweep in PostgreSQL."""

    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip(
            "set HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST=1 for the isolated PostgreSQL test"
        )

    database = Database()
    schema_name = f"ptg2_shared_gc_test_{uuid.uuid4().hex}"
    schema = f'"{schema_name}"'
    block_hash = _hash(20)
    unrelated_hash = _hash(21)
    await database.connect()
    try:
        async with database.acquire() as connection:
            await connection.status(f"CREATE SCHEMA {schema}")
            await _create_gc_layout_schema(connection, schema)
            await _create_gc_block_schema(connection, schema)
            await _insert_gc_layout_fixture(connection, schema)
            await _insert_gc_block_fixture(
                connection,
                schema,
                block_hash,
                unrelated_hash,
            )
        await _release_and_sweep_gc_fixture(
            database,
            schema_name,
            schema,
            block_hash,
        )
        await _assert_gc_fixture_storage(database, schema)
    finally:
        try:
            async with database.acquire() as connection:
                await connection.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()


@pytest.mark.asyncio
async def test_real_postgres_history_cleanup_rejects_bound_pinned_and_unauthorized():
    """The definer repeats layout admission and rejects unrelated role authority."""
    async with _snapshot_history_gc_scope() as (database, _schema_name, schema):
        cleanup_sql = f"SELECT {schema}.delete_ptg_snapshot_history(10,2,21600)"
        async with database.acquire() as connection:
            assert not await connection.scalar(cleanup_sql)
            await connection.status(f"DELETE FROM {schema}.ptg2_v3_snapshot_binding WHERE snapshot_key=10")
            await connection.status(
                f"INSERT INTO {schema}.ptg2_block_build_pin VALUES(10,'active','pin',:hash,now()+INTERVAL '1 hour')",
                hash=_hash(99),
            )
            assert not await connection.scalar(cleanup_sql)
            assert await connection.scalar(f"SELECT count(*) FROM {schema}.ptg2_v4_npi_scope WHERE snapshot_key=10") == 5
        role_name = f"ptg_gc_reader_{uuid.uuid4().hex}"
        async with database.acquire() as connection:
            await connection.status(f"CREATE ROLE {role_name} NOLOGIN")
            await connection.status(f"GRANT USAGE ON SCHEMA {schema} TO {role_name}")
        try:
            with pytest.raises(Exception, match="ptg_snapshot_gc_authority"):
                async with database.acquire() as connection:
                    await connection.status(f"SET LOCAL ROLE {role_name}")
                    await connection.scalar(cleanup_sql)
        finally:
            async with database.acquire() as connection:
                await connection.status(f"REVOKE USAGE ON SCHEMA {schema} FROM {role_name}")
                await connection.status(f"DROP ROLE {role_name}")


@pytest.mark.asyncio
async def test_real_postgres_history_cleanup_aggregate_budget_and_timeout_rollback():
    """A timeout undoes one batch while earlier bounded progress stays committed."""
    async with _snapshot_history_gc_scope() as (database, schema_name, schema):
        async with database.acquire() as connection:
            await connection.status(f"DELETE FROM {schema}.ptg2_v3_snapshot_binding")
            assert not await shared_gc._empty_snapshot_history_keys(
                connection, schema_name=schema_name, snapshot_keys=(10,20), batch_rows=3,
                building_max_age_seconds=21600,
            )
            assert await connection.scalar(f"SELECT count(*) FROM {schema}.ptg2_v4_npi_scope") == 8
        with pytest.raises(Exception, match="statement timeout"):
            async with database.acquire() as connection:
                await connection.scalar(f"SELECT {schema}.delete_ptg_snapshot_history(10,2,21600)")
                await connection.status("SET LOCAL statement_timeout='30ms'")
                await connection.scalar("SELECT pg_sleep(1)")
        async with database.acquire() as connection:
            for table_name in ("ptg2_v4_npi_scope", "ptg2_v4_provider_component"):
                assert await connection.scalar(f"SELECT count(*) FROM {schema}.{table_name}") == 8


@pytest.mark.asyncio
async def test_real_postgres_history_cleanup_detects_private_catalog_for_lifecycle_role():
    """A function-only lifecycle role detects private storage and drains bounded batches."""
    async with _snapshot_history_gc_scope() as (database, schema_name, schema):
        role_name = f"ptg_gc_lifecycle_{uuid.uuid4().hex}"
        signature = f"{schema}.delete_ptg_snapshot_history(bigint,integer,integer,text)"
        async with database.acquire() as connection:
            await connection.status(f"CREATE ROLE {role_name} NOLOGIN")
            await connection.status(f"GRANT USAGE ON SCHEMA {schema} TO {role_name}")
            await connection.status(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")
            await connection.status(f"GRANT EXECUTE ON FUNCTION {signature} TO {role_name}")
            await connection.status(
                f"INSERT INTO {schema}.ptg2_snapshot_lifecycle_writer VALUES(:role)", role=role_name,
            )
            await connection.status(f"DELETE FROM {schema}.ptg2_v3_snapshot_binding WHERE snapshot_key=10")
            for remaining in (3, 1, 0):
                await connection.status(f"SET LOCAL ROLE {role_name}")
                assert await connection.scalar(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema=:schema "
                    "AND table_name='ptg2_snapshot_lifecycle_writer'", schema=schema_name,
                ) == 0
                empty_keys = await shared_gc._empty_snapshot_history_keys(
                    connection, schema_name=schema_name, snapshot_keys=(10,), batch_rows=2,
                    building_max_age_seconds=21600,
                )
                assert empty_keys == ([10] if remaining == 0 else [])
                await connection.status("RESET ROLE")
                for table in ("ptg2_v4_npi_scope", "ptg2_v4_provider_component"):
                    assert await connection.scalar(
                        f"SELECT count(*) FROM {schema}.{table} WHERE snapshot_key=10"
                    ) == remaining
                    assert await connection.scalar(
                        f"SELECT count(*) FROM {schema}.{table} WHERE snapshot_key=20"
                    ) == 5
            await connection.status(f"REVOKE EXECUTE ON FUNCTION {signature} FROM {role_name}")
            await connection.status(f"REVOKE USAGE ON SCHEMA {schema} FROM {role_name}")
            await connection.status(f"DROP ROLE {role_name}")
