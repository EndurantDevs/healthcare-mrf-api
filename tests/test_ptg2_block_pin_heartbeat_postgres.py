# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable-PostgreSQL proof for token-fenced block-pin heartbeats."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process.ptg_parts import ptg2_block_build_pins as build_pins
from process.ptg_parts import ptg2_shared_gc as shared_gc
from tests.test_ptg_wave_recovery_storage_postgres import _dsn, _quote


def _database():
    engine = create_async_engine(
        _dsn().replace("postgresql://", "postgresql+asyncpg://", 1),
        pool_size=6,
        max_overflow=0,
        pool_timeout=2,
    )
    sessions = async_sessionmaker(
        engine,
        expire_on_commit=False,
        autoflush=False,
    )
    return engine, Database(engine=engine, session_factory=sessions), sessions


async def _create_schema(engine, schema: str) -> None:
    statements = (
        f"CREATE TABLE {schema}.ptg2_v3_snapshot_layout ("
        "snapshot_key bigint PRIMARY KEY, build_token varchar(96) NOT NULL, "
        "state varchar(16) NOT NULL)",
        f"CREATE TABLE {schema}.ptg2_v3_block ("
        "block_hash bytea PRIMARY KEY, stored_byte_count bigint NOT NULL)",
        f"CREATE TABLE {schema}.ptg2_v3_snapshot_block ("
        "snapshot_key bigint NOT NULL, block_hash bytea NOT NULL, "
        "PRIMARY KEY (snapshot_key, block_hash))",
        f"CREATE TABLE {schema}.ptg2_v3_gc_candidate ("
        "block_hash bytea PRIMARY KEY, eligible_at timestamptz NOT NULL, "
        "queued_at timestamptz NOT NULL)",
        f"CREATE TABLE {schema}.ptg2_block_build_pin ("
        "snapshot_key bigint NOT NULL REFERENCES "
        f"{schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE, "
        "build_token varchar(96) NOT NULL, pin_token varchar(96) NOT NULL, "
        "block_hash bytea NOT NULL, created_at timestamptz NOT NULL, "
        "heartbeat_at timestamptz NOT NULL, lease_until timestamptz NOT NULL, "
        "PRIMARY KEY (snapshot_key, pin_token, block_hash))",
    )
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await connection.execute(text(f"CREATE SCHEMA {schema}"))
        for statement in statements:
            await connection.execute(text(statement))
        await connection.execute(
            text(
                f"INSERT INTO {schema}.ptg2_v3_snapshot_layout "
                "VALUES (1, 'build-a', 'building')"
            )
        )
        await connection.execute(
            text(
                f"INSERT INTO {schema}.ptg2_v3_block "
                "SELECT requested.block_hash, 10 "
                "FROM unnest(CAST(:hashes AS bytea[])) "
                "AS requested(block_hash)"
            ),
            {"hashes": [b"a" * 32, b"b" * 32]},
        )


async def _eligible_hashes(sessions, schema_name: str) -> set[bytes]:
    async with sessions() as session:
        result = await session.execute(
            text(shared_gc._eligible_blocks_sql(schema_name, lock_rows=False)),
            {"max_bytes": 100, "max_rows": 10},
        )
        return {bytes(row[0]) for row in result.all()}


async def _install_pinned_hashes(
    sessions,
    engine,
    schema_name: str,
    schema: str,
) -> tuple[bytes, bytes]:
    """Persist one build's block pins and make both blocks GC candidates."""

    hashes = (b"a" * 32, b"b" * 32)
    async with sessions.begin() as session:
        assert await build_pins.pin_shared_block_hashes(
            session,
            schema_name=schema_name,
            snapshot_key=1,
            build_token="build-a",
            pin_token="attempt-a",
            block_hashes=hashes,
        ) == 2
    async with engine.begin() as connection:
        await connection.execute(
            text(
                f"INSERT INTO {schema}.ptg2_v3_gc_candidate "
                "SELECT requested.block_hash, now(), now() "
                "FROM unnest(CAST(:hashes AS bytea[])) "
                "AS requested(block_hash)"
            ),
            {"hashes": list(hashes)},
        )
    return hashes


async def _assert_live_heartbeat_protection(
    sessions,
    schema_name: str,
    schema: str,
) -> None:
    """Prove one refreshed group anchor protects all sibling pins."""

    lease = build_pins.SharedBlockBuildPinLease(
        schema_name=schema_name,
        snapshot_key=1,
        build_token="build-a",
        pin_token="attempt-a",
    )
    await lease.start()
    await asyncio.sleep(1.2)
    lease.require_live()
    async with sessions() as observer:
        live_count = await observer.scalar(
            text(
                f"SELECT COUNT(*) FROM {schema}.ptg2_block_build_pin "
                "WHERE lease_until > transaction_timestamp()"
            )
        )
    assert live_count == 1
    assert await _eligible_hashes(sessions, schema_name) == set()
    await lease.close()


async def _assert_expiry_replay_and_attach(
    sessions,
    local_db,
    schema_name: str,
    schema: str,
    hashes: tuple[bytes, bytes],
) -> None:
    """Prove crash expiry releases pins and replay atomically attaches them."""

    await asyncio.sleep(1.2)
    assert await _eligible_hashes(sessions, schema_name) == set(hashes)
    async with sessions.begin() as session:
        assert await build_pins.is_pin_lease_renewed(
            session,
            schema_name=schema_name,
            snapshot_key=1,
            build_token="build-a",
            pin_token="attempt-a",
        )
    replay_lease = build_pins.SharedBlockBuildPinLease(
        schema_name=schema_name,
        snapshot_key=1,
        build_token="build-a",
        pin_token="attempt-a",
    )
    await replay_lease.start()
    await asyncio.sleep(1.2)
    assert await _eligible_hashes(sessions, schema_name) == set()
    async with sessions.begin() as session:
        await session.execute(
            text(
                f"INSERT INTO {schema}.ptg2_v3_snapshot_block "
                "SELECT 1, requested.block_hash "
                "FROM unnest(CAST(:hashes AS bytea[])) "
                "AS requested(block_hash)"
            ),
            {"hashes": list(hashes)},
        )
        assert await build_pins.delete_shared_block_build_pins(
            session,
            schema_name=schema_name,
            snapshot_key=1,
            build_token="build-a",
            pin_token="attempt-a",
        ) == 2
        await replay_lease.close()
    assert await local_db.scalar(
        f"SELECT COUNT(*) FROM {schema}.ptg2_block_build_pin"
    ) == 0
    assert await _eligible_hashes(sessions, schema_name) == set()


@pytest.mark.asyncio
async def test_live_group_heartbeat_protects_expired_siblings_until_attach(
    monkeypatch,
):
    """One token-fenced anchor protects its group, then crash expiry releases it."""

    schema_name = "ptg_block_pin_heartbeat"
    schema = _quote(schema_name)
    engine, local_db, sessions = _database()
    monkeypatch.setenv(build_pins.PTG2_BLOCK_BUILD_PIN_LEASE_SECONDS_ENV, "1")
    monkeypatch.setattr(build_pins, "db", local_db)
    await _create_schema(engine, schema)
    hashes = await _install_pinned_hashes(
        sessions,
        engine,
        schema_name,
        schema,
    )
    await _assert_live_heartbeat_protection(sessions, schema_name, schema)
    await _assert_expiry_replay_and_attach(
        sessions,
        local_db,
        schema_name,
        schema,
        hashes,
    )

    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    await engine.dispose()
