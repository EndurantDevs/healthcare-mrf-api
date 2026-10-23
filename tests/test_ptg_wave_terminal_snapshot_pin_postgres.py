# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL crash/replay proof for ordinary terminal manifest pins."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.ptg_parts import source_snapshot_reference_lifetime as lifetime
from process.ptg_parts.ptg2_lifecycle_lock import lifecycle_database_sqlstate
from process.ptg_parts.ptg_wave_terminal_snapshot_pin import (
    ORDINARY_TERMINAL_PIN_OWNER_TYPE,
    delete_ordinary_terminal_snapshot_pin,
    insert_ordinary_terminal_snapshot_pin,
)
from process.ptg_wave_receipt_contract import ordinary_cutover_id
from tests.test_ptg_wave_recovery_storage_postgres import _dsn, _quote

OPERATION_ID = "8" * 64
SNAPSHOT_ID = "ordinary-terminal-snapshot"
INTERNAL_RUN_ID = "ordinary-terminal-engine-run"


def _options() -> dict[str, object]:
    return {
        "ordinary_cutover_operation_id": OPERATION_ID,
        "ordinary_cutover_id": ordinary_cutover_id(OPERATION_ID),
        "ordinary_cutover_member_ordinal": 7,
    }


async def _create_terminal_pin_schema(engine, schema: str) -> None:
    statements = (
        f"CREATE TABLE {schema}.ptg2_import_run (import_run_id varchar(96) PRIMARY KEY, options json NOT NULL)",
        f"CREATE TABLE {schema}.ptg2_snapshot ("
        "snapshot_id varchar(128) PRIMARY KEY, "
        "import_run_id varchar(96) NOT NULL, manifest json NOT NULL)",
        f"CREATE TABLE {schema}.ptg2_snapshot_pin ("
        "owner_type varchar(48) NOT NULL, owner_id varchar(96) NOT NULL, "
        "snapshot_id varchar(128) NOT NULL REFERENCES "
        f"{schema}.ptg2_snapshot(snapshot_id) ON DELETE RESTRICT, "
        "reason varchar(256), created_at timestamptz NOT NULL, "
        "PRIMARY KEY (owner_type, owner_id, snapshot_id))",
        f"CREATE TABLE {schema}.terminal_receipt (member_ordinal integer PRIMARY KEY)",
    )
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await connection.execute(text(f"CREATE SCHEMA {schema}"))
        for statement in statements:
            await connection.execute(text(statement))
        await connection.execute(
            text(
                f"INSERT INTO {schema}.ptg2_import_run (import_run_id,options) "
                'VALUES (:run_id,\'{"source_key":"synthetic_source"}\'::jsonb)'
            ),
            {"run_id": INTERNAL_RUN_ID},
        )
        await connection.execute(
            text(
                f"INSERT INTO {schema}.ptg2_snapshot "
                "(snapshot_id, import_run_id, manifest) VALUES (:snapshot_id, :run_id, "
                '\'{"serving_index":{"source_key":"synthetic_source"}}\'::jsonb)'
            ),
            {"snapshot_id": SNAPSHOT_ID, "run_id": INTERNAL_RUN_ID},
        )


async def _insert_and_assert_terminal_pin(sessions, schema_name, schema) -> None:
    async with sessions.begin() as session:
        pin = await insert_ordinary_terminal_snapshot_pin(
            session,
            schema_name=schema_name,
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
            options=_options(),
        )
        assert pin is not None
        assert pin.owner_id == f"{OPERATION_ID}:7"
    async with sessions() as session:
        assert (
            await session.scalar(
                text(f"SELECT COUNT(*) FROM {schema}.ptg2_snapshot_pin WHERE owner_type = :owner_type"),
                {"owner_type": ORDINARY_TERMINAL_PIN_OWNER_TYPE},
            )
            == 1
        )


async def _assert_terminal_pin_release_rolls_back(
    sessions,
    schema_name: str,
    schema: str,
) -> None:
    with pytest.raises(RuntimeError, match="crash before receipt commit"):
        async with sessions.begin() as session:
            await session.execute(text(f"INSERT INTO {schema}.terminal_receipt (member_ordinal) VALUES (7)"))
            assert (
                await delete_ordinary_terminal_snapshot_pin(
                    session,
                    schema_name=schema_name,
                    operation_id=OPERATION_ID,
                    member_ordinal=7,
                    snapshot_id=SNAPSHOT_ID,
                )
                == 1
            )
            raise RuntimeError("crash before receipt commit")
    async with sessions() as session:
        assert await session.scalar(text(f"SELECT COUNT(*) FROM {schema}.ptg2_snapshot_pin")) == 1
        assert await session.scalar(text(f"SELECT COUNT(*) FROM {schema}.terminal_receipt")) == 0


async def _commit_terminal_receipt_and_release_pin(
    sessions,
    schema_name: str,
    schema: str,
) -> None:
    async with sessions.begin() as session:
        await session.execute(text(f"INSERT INTO {schema}.terminal_receipt (member_ordinal) VALUES (7)"))
        assert (
            await delete_ordinary_terminal_snapshot_pin(
                session,
                schema_name=schema_name,
                operation_id=OPERATION_ID,
                member_ordinal=7,
                snapshot_id=SNAPSHOT_ID,
            )
            == 1
        )
    async with sessions.begin() as session:
        assert (
            await delete_ordinary_terminal_snapshot_pin(
                session,
                schema_name=schema_name,
                operation_id=OPERATION_ID,
                member_ordinal=7,
                snapshot_id=SNAPSHOT_ID,
            )
            == 0
        )


@pytest.mark.asyncio
async def test_pin_spans_completion_to_receipt_and_crash_rolls_back_release():
    """Prove the terminal pin survives completion and receipt rollback."""
    schema_name = "ptg_terminal_manifest_pin"
    schema = _quote(schema_name)
    engine = create_async_engine(_dsn().replace("postgresql://", "postgresql+asyncpg://", 1))
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await _create_terminal_pin_schema(engine, schema)

    try:
        await _insert_and_assert_terminal_pin(sessions, schema_name, schema)
        await _assert_terminal_pin_release_rolls_back(
            sessions,
            schema_name,
            schema,
        )
        await _commit_terminal_receipt_and_release_pin(
            sessions,
            schema_name,
            schema,
        )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("table", ["ptg2_import_run", "ptg2_snapshot"])
async def test_source_lifetime_stabilizes_both_header_sources_until_commit(table):
    schema_name = "ptg_terminal_source_headers"
    schema = _quote(schema_name)
    engine = create_async_engine(_dsn().replace("postgresql://", "postgresql+asyncpg://", 1))
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    field = "options" if table == "ptg2_import_run" else "manifest"
    mutation = text(f"UPDATE {schema}.{table} SET {field}={field}::jsonb||jsonb_build_object('other',true)")
    try:
        await _create_terminal_pin_schema(engine, schema)
        async with sessions.begin() as session:
            assert (
                await lifetime.acquire_snapshot_reference_lifetime(
                    session, schema_name=schema_name, snapshot_id=SNAPSHOT_ID, internal_run_id=INTERNAL_RUN_ID
                )
                == "synthetic_source"
            )
            with pytest.raises(DBAPIError) as refused:
                async with engine.begin() as other:
                    await other.execute(text("SET LOCAL lock_timeout='100ms'"))
                    await other.execute(mutation)
            assert lifecycle_database_sqlstate(refused.value) == "55P03"
        async with engine.begin() as other:
            await other.execute(mutation)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await engine.dispose()


@pytest.mark.asyncio
async def test_repeatable_read_source_change_is_refused_after_lifetime_fence(monkeypatch):
    schema_name = "ptg_terminal_source_drift"
    schema = _quote(schema_name)
    engine = create_async_engine(_dsn().replace("postgresql://", "postgresql+asyncpg://", 1))
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    observed, continue_lock = asyncio.Event(), asyncio.Event()
    original_lock = lifetime.acquire_ptg2_source_lifecycle_lock

    async def hold_before_fence(session, **options):
        observed.set()
        await continue_lock.wait()
        return await original_lock(session, **options)

    monkeypatch.setattr(lifetime, "acquire_ptg2_source_lifecycle_lock", hold_before_fence)
    waiter = None
    try:
        await _create_terminal_pin_schema(engine, schema)
        with pytest.raises(lifetime.SnapshotReferenceIdentityError) as refused:
            async with sessions.begin() as session:
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                waiter = asyncio.create_task(
                    lifetime.acquire_snapshot_reference_lifetime(
                        session, schema_name=schema_name, snapshot_id=SNAPSHOT_ID, internal_run_id=INTERNAL_RUN_ID
                    )
                )
                await asyncio.wait_for(observed.wait(), timeout=3)
                async with engine.begin() as other:
                    await other.execute(
                        text(f'UPDATE {schema}.ptg2_import_run SET options=\'{{"source_key":"other_source"}}\'::jsonb')
                    )
                continue_lock.set()
                await asyncio.wait_for(waiter, timeout=3)
        assert lifecycle_database_sqlstate(refused.value) == "40001"
    finally:
        continue_lock.set()
        if waiter is not None and not waiter.done():
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
        async with engine.begin() as connection:
            await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await engine.dispose()
