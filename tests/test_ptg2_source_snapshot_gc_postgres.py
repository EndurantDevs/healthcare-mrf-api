# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL time-zone proof for source-snapshot garbage collection."""

from __future__ import annotations

import os
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.connection import Database
from process.ptg_parts import ptg2_source_snapshot_gc as snapshot_gc


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
        pytest.skip(
            "set HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST=1 for the isolated "
            "PostgreSQL test"
        )
    database = Database()
    schema_name = f"ptg2_source_gc_utc_{uuid.uuid4().hex}"
    schema = f'"{schema_name}"'
    monkeypatch.setattr(
        snapshot_gc,
        "require_migration_owned_tables",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        snapshot_gc,
        "build_shared_layout_release_plan",
        AsyncMock(return_value=SimpleNamespace(
            logical_layout_count=0,
            candidate_hash_count=0,
            stored_bytes=0,
        )),
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
