# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable-PostgreSQL proof for durable legacy pointer projection."""

from __future__ import annotations

import asyncio
import datetime
import time
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process.ptg_parts import ptg2_legacy_global_projection_queue as projection
from process.ptg_parts import ptg2_lifecycle_lock as lifecycle
from tests.test_ptg_wave_recovery_storage_postgres import (
    _dsn,
    _load_migration,
    _quote,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / (
    "20260810160000_ptg2_legacy_global_projection_queue.py"
)


def _database():
    engine = create_async_engine(
        _dsn().replace("postgresql://", "postgresql+asyncpg://", 1),
        pool_size=30,
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
        f"CREATE TABLE {schema}.ptg2_snapshot ("
        "snapshot_id varchar(96) PRIMARY KEY, status text NOT NULL, "
        "published_at timestamptz)",
        f"CREATE TABLE {schema}.ptg2_current_source_snapshot ("
        "source_key varchar(96) PRIMARY KEY, snapshot_id varchar(96), "
        "previous_snapshot_id varchar(96), updated_at timestamptz)",
        f"CREATE TABLE {schema}.ptg2_current_plan_source ("
        "plan_source_key varchar(96) PRIMARY KEY, source_key varchar(96), "
        "snapshot_id varchar(96), previous_snapshot_id varchar(96), "
        "updated_at timestamptz)",
        f"CREATE TABLE {schema}.ptg2_current_snapshot ("
        "slot varchar(32) PRIMARY KEY, snapshot_id varchar(96), "
        "previous_snapshot_id varchar(96), updated_at timestamptz)",
        f"CREATE TABLE {schema}.ptg2_legacy_global_pointer_projection_queue ("
        "source_key varchar(96) PRIMARY KEY, "
        "requested_generation bigint NOT NULL DEFAULT 1, "
        "applied_generation bigint NOT NULL DEFAULT 0, "
        "available_at timestamptz NOT NULL DEFAULT now(), "
        "lease_token varchar(64), lease_until timestamptz, "
        "created_at timestamptz NOT NULL DEFAULT now(), "
        "updated_at timestamptz NOT NULL DEFAULT now())",
    )
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await connection.execute(text(f"CREATE SCHEMA {schema}"))
        for statement in statements:
            await connection.execute(text(statement))
        await connection.execute(
            text(
                f"INSERT INTO {schema}.ptg2_current_snapshot "
                "VALUES ('current', NULL, NULL, now())"
            )
        )


async def _publish_source(sessions, schema_name: str, schema: str, ordinal: int):
    source_key = f"source-{ordinal}"
    snapshot_id = f"snapshot-{ordinal}"
    published_at = datetime.datetime(2026, 8, 10, tzinfo=datetime.UTC) + (
        datetime.timedelta(seconds=ordinal)
    )
    async with sessions.begin() as session:
        await session.execute(
            text(
                f"INSERT INTO {schema}.ptg2_snapshot VALUES "
                "(:snapshot_id, 'published', :published_at)"
            ),
            {"snapshot_id": snapshot_id, "published_at": published_at},
        )
        await session.execute(
            text(
                f"INSERT INTO {schema}.ptg2_current_source_snapshot VALUES "
                "(:source_key, :snapshot_id, NULL, :published_at)"
            ),
            {
                "source_key": source_key,
                "snapshot_id": snapshot_id,
                "published_at": published_at,
            },
        )
        await session.execute(
            text(
                f"INSERT INTO {schema}.ptg2_current_plan_source VALUES "
                "(:plan_source_key, :source_key, :snapshot_id, NULL, "
                ":published_at)"
            ),
            {
                "plan_source_key": f"plan-{ordinal}",
                "source_key": source_key,
                "snapshot_id": snapshot_id,
                "published_at": published_at,
            },
        )
        await projection.mark_legacy_global_projection_dirty(
            session,
            schema_name=schema_name,
            source_key=source_key,
        )


async def _drain_all() -> projection.PTG2LegacyGlobalProjectionDrain:
    claimed = reconciled = deferred = lease_lost = 0
    while True:
        result = await projection.drain_legacy_global_projection_queue(
            max_requests=8
        )
        claimed += result.claimed
        reconciled += result.reconciled
        deferred += result.deferred
        lease_lost += result.lease_lost
        if result.claimed == 0:
            return projection.PTG2LegacyGlobalProjectionDrain(
                claimed=claimed,
                reconciled=reconciled,
                deferred=deferred,
                lease_lost=lease_lost,
            )


async def _assert_source_commits_while_singleton_locked(
    sessions,
    schema_name: str,
    schema: str,
    local_db,
) -> None:
    """Hold the singleton while all source-local commits and deferrals finish."""

    holder = sessions()
    holder_transaction = await holder.begin()
    await holder.execute(
        text(
            f"UPDATE {schema}.ptg2_current_snapshot SET updated_at = now() "
            "WHERE slot = 'current'"
        )
    )
    started = time.monotonic()
    await asyncio.wait_for(
        asyncio.gather(
            *(
                _publish_source(sessions, schema_name, schema, ordinal)
                for ordinal in range(24)
            )
        ),
        timeout=2,
    )
    assert time.monotonic() - started < 2
    assert await local_db.scalar(
        f"SELECT COUNT(*) FROM {schema}.ptg2_current_source_snapshot"
    ) == 24
    blocked = await asyncio.gather(
        *(
            projection.drain_legacy_global_projection_queue(
                max_requests=1,
                source_key=f"source-{ordinal}",
            )
            for ordinal in range(24)
        )
    )
    assert sum(
        projection_result.deferred for projection_result in blocked
    ) == 24
    await holder_transaction.rollback()
    await holder.close()


async def _assert_generation_race_replays(
    sessions,
    schema_name: str,
    schema: str,
    local_db,
) -> None:
    """Leave a newer dirty generation ready when an older lease finishes."""

    async with sessions.begin() as session:
        await projection.mark_legacy_global_projection_dirty(
            session, schema_name=schema_name, source_key="source-0"
        )
    claim = await projection._claim_projection(source_key="source-0")
    assert claim is not None
    async with sessions.begin() as session:
        await projection.mark_legacy_global_projection_dirty(
            session, schema_name=schema_name, source_key="source-0"
        )
    assert await projection._is_authoritative_projection_applied()
    assert await projection._is_projection_finish_committed(
        claim, reconciled=True
    )
    generation_rows = await local_db.all(
        f"SELECT requested_generation, applied_generation "
        f"FROM {schema}.ptg2_legacy_global_pointer_projection_queue "
        "WHERE source_key = 'source-0'"
    )
    assert [
        tuple(generation_row) for generation_row in generation_rows
    ] == [(3, 2)]
    assert (
        await projection.drain_legacy_global_projection_queue(
            max_requests=1, source_key="source-0"
        )
    ).reconciled == 1


async def _assert_expired_claim_replays(
    engine,
    sessions,
    schema_name: str,
    schema: str,
) -> None:
    """Replay an applied projection whose lease expired before its finish CAS."""

    async with sessions.begin() as session:
        await projection.mark_legacy_global_projection_dirty(
            session, schema_name=schema_name, source_key="source-1"
        )
    crash_claim = await projection._claim_projection(source_key="source-1")
    assert crash_claim is not None
    assert await projection._is_authoritative_projection_applied()
    async with engine.begin() as connection:
        await connection.execute(
            text(
                f"UPDATE {schema}.ptg2_legacy_global_pointer_projection_queue "
                "SET lease_until = now() - INTERVAL '1 second' "
                "WHERE source_key = 'source-1'"
            )
        )
    crash_replay = await projection.drain_legacy_global_projection_queue(
        max_requests=1, source_key="source-1"
    )
    assert crash_replay.reconciled == 1
    assert not await projection._is_projection_finish_committed(
        crash_claim, reconciled=True
    )


async def _assert_allowed_only_source_is_excluded(
    sessions,
    schema_name: str,
    schema: str,
    local_db,
) -> None:
    """Keep a source without logical-plan publication out of compatibility."""

    async with sessions.begin() as session:
        await session.execute(
            text(
                f"INSERT INTO {schema}.ptg2_snapshot VALUES "
                "('allowed-only', 'published', '2026-08-11T00:00:00Z')"
            )
        )
        await session.execute(
            text(
                f"INSERT INTO {schema}.ptg2_current_source_snapshot VALUES "
                "('allowed-source', 'allowed-only', NULL, "
                "'2026-08-11T00:00:00Z')"
            )
        )
        await projection.mark_legacy_global_projection_dirty(
            session, schema_name=schema_name, source_key="allowed-source"
        )
    allowed_replay = await projection.drain_legacy_global_projection_queue(
        max_requests=1, source_key="allowed-source"
    )
    assert allowed_replay.reconciled == 1
    assert await local_db.scalar(
        f"SELECT snapshot_id FROM {schema}.ptg2_current_snapshot "
        "WHERE slot = 'current'"
    ) == "snapshot-23"


@pytest.mark.asyncio
async def test_held_singleton_never_gates_sources_and_queue_replays(monkeypatch):
    """Twenty-four source commits finish while a durable projector is blocked."""

    schema_name = "ptg_legacy_projection_queue"
    schema = _quote(schema_name)
    engine, local_db, sessions = _database()
    monkeypatch.setattr(projection, "db", local_db)
    monkeypatch.setattr(projection, "resolve_ptg2_schema", lambda: schema_name)
    monkeypatch.setattr(lifecycle, "db", local_db)
    monkeypatch.setattr(projection, "_RETRY_SECONDS", 0)
    await _create_schema(engine, schema)

    await _assert_source_commits_while_singleton_locked(
        sessions,
        schema_name,
        schema,
        local_db,
    )

    replay = await _drain_all()
    assert replay.reconciled == 24
    assert await local_db.scalar(
        f"SELECT snapshot_id FROM {schema}.ptg2_current_snapshot "
        "WHERE slot = 'current'"
    ) == "snapshot-23"

    await _assert_generation_race_replays(
        sessions,
        schema_name,
        schema,
        local_db,
    )
    await _assert_expired_claim_replays(
        engine,
        sessions,
        schema_name,
        schema,
    )

    await _assert_allowed_only_source_is_excluded(
        sessions,
        schema_name,
        schema,
        local_db,
    )
    assert engine.pool.checkedout() == 0
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    await engine.dispose()


def test_legacy_projection_migration_is_source_local_and_guarded(monkeypatch):
    migration = _load_migration(MIGRATION_PATH)
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "ptg_projection_contract")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    migration.downgrade()
    sql = "\n".join(statements)
    assert "PRIMARY KEY (source_key)" in sql
    assert "requested_generation" in sql
    assert "FOR UPDATE" not in sql
    assert "refusing to downgrade pending legacy global projection work" in sql
