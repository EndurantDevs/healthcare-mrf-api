# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable-PostgreSQL proof for the durable non-gating plan outbox."""

from __future__ import annotations

import asyncio
import datetime
import json
import time
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process.ptg_parts import ptg2_lifecycle_lock as lifecycle
from process.ptg_parts import ptg2_plan_catalog as catalog
from process.ptg_parts import ptg2_plan_catalog_outbox as outbox
from tests.test_ptg_wave_recovery_storage_postgres import (
    _dsn,
    _load_migration,
    _quote,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / (
    "20260810140000_ptg2_plan_catalog_outbox.py"
)


def _plan_row() -> dict[str, object]:
    return {
        "plan_hash": "a" * 64,
        "hash_prefix": "a" * 16,
        "plan_id": "plan-neutral",
        "plan_id_type": "ein",
        "plan_name": "Synthetic Plan",
        "plan_market_type": "group",
        "issuer_name": "Synthetic Issuer",
        "plan_sponsor_name": "Synthetic Sponsor",
        "canonical_payload": {
            "plan_id": "plan-neutral",
            "plan_market_type": "group",
        },
        "created_at": datetime.datetime(2026, 8, 10, tzinfo=datetime.UTC),
    }


def _alias_row() -> dict[str, object]:
    return {
        "alias_hash": "b" * 64,
        "plan_hash": "a" * 64,
        "alias_type": "plan_id",
        "alias_value": "plan-neutral",
        "created_at": datetime.datetime(2026, 8, 10, tzinfo=datetime.UTC),
    }


def _numbered_plan_row(ordinal: int) -> dict[str, object]:
    plan_hash = f"{ordinal:064x}"
    return {
        **_plan_row(),
        "plan_hash": plan_hash,
        "hash_prefix": plan_hash[:16],
        "plan_id": f"plan-{ordinal}",
        "plan_name": f"Synthetic Plan {ordinal}",
        "canonical_payload": {"plan_id": f"plan-{ordinal}"},
    }


def _numbered_alias_row(ordinal: int) -> dict[str, object]:
    return {
        **_alias_row(),
        "alias_hash": f"{ordinal + 10_000:064x}",
        "plan_hash": f"{ordinal:064x}",
        "alias_value": f"plan-{ordinal}",
    }


async def _enqueue_source(sessions, schema: str, source_number: int) -> str:
    snapshot_id = f"source-{source_number}-snapshot"
    async with sessions.begin() as session:
        await session.execute(
            text(
                f"INSERT INTO {schema}.ptg2_plan_month "
                "(plan_month_id, snapshot_id, plan_hash, import_month) "
                "VALUES (:plan_month_id, :snapshot_id, :plan_hash, "
                "DATE '2026-08-01')"
            ),
            {
                "plan_month_id": f"source-{source_number}-month",
                "snapshot_id": snapshot_id,
                "plan_hash": "a" * 64,
            },
        )
        request = await outbox.enqueue_immutable_plan_catalog(
            session,
            snapshot_id=snapshot_id,
            plan_rows=[_plan_row()],
            alias_rows=[_alias_row()],
        )
    return request.request_id


async def _create_plan_catalog_tables(engine, schema: str) -> None:
    statements = (
        f"CREATE TABLE {schema}.ptg2_plan ("
        "plan_hash varchar(64) PRIMARY KEY, hash_prefix varchar(16), "
        "plan_id varchar(64), plan_id_type varchar(32), plan_name text, "
        "plan_market_type varchar(32), issuer_name text, "
        "plan_sponsor_name text, canonical_payload json, "
        "created_at timestamptz)",
        f"CREATE TABLE {schema}.ptg2_plan_alias ("
        "alias_hash varchar(64) PRIMARY KEY, plan_hash varchar(64), "
        "alias_type varchar(32), alias_value text, created_at timestamptz)",
        f"CREATE TABLE {schema}.ptg2_plan_month ("
        "plan_month_id text PRIMARY KEY, snapshot_id text, "
        "plan_hash varchar(64), import_month date)",
        f"CREATE TABLE {schema}.ptg2_plan_catalog_outbox ("
        "request_id varchar(64) PRIMARY KEY, "
        "snapshot_id varchar(96) NOT NULL, "
        "chunk_index integer NOT NULL, chunk_count integer NOT NULL, "
        "payload_sha256 varchar(64) NOT NULL, "
        "plan_rows jsonb NOT NULL, alias_rows jsonb NOT NULL, "
        "plan_count integer NOT NULL, alias_count integer NOT NULL, "
        "payload_bytes integer NOT NULL, "
        "attempt_count integer NOT NULL DEFAULT 0, "
        "available_at timestamptz NOT NULL DEFAULT now(), "
        "lease_token varchar(64), lease_until timestamptz, "
        "terminal_error_code varchar(64), terminal_at timestamptz, "
        "created_at timestamptz NOT NULL DEFAULT now(), "
        "updated_at timestamptz NOT NULL DEFAULT now(), "
        "UNIQUE (snapshot_id, chunk_index))",
    )
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await connection.execute(text(f"CREATE SCHEMA {schema}"))
        for statement in statements:
            await connection.execute(text(statement))


async def _hold_shared_plan_row(sessions, schema: str, plan_row: dict):
    holder = sessions()
    holder_transaction = await holder.begin()
    await holder.execute(
        text(
            f"INSERT INTO {schema}.ptg2_plan "
            "(plan_hash, hash_prefix, plan_id, plan_id_type, plan_name, "
            "plan_market_type, issuer_name, plan_sponsor_name, "
            "canonical_payload, created_at) VALUES "
            "(:plan_hash, :hash_prefix, :plan_id, :plan_id_type, :plan_name, "
            ":plan_market_type, :issuer_name, :plan_sponsor_name, "
            "CAST(:canonical_payload AS json), :created_at)"
        ),
        {
            **plan_row,
            "canonical_payload": json.dumps(
                plan_row["canonical_payload"],
                sort_keys=True,
                separators=(",", ":"),
            ),
        },
    )
    return holder, holder_transaction


async def _assert_contended_catalog_drains_defer(
    sessions,
    schema: str,
    local_db,
) -> None:
    request_ids = await asyncio.gather(
        *(_enqueue_source(sessions, schema, number) for number in range(1, 24))
    )
    assert await local_db.scalar(
        f"SELECT COUNT(*) FROM {schema}.ptg2_plan_month"
    ) == 23
    assert await local_db.scalar(
        f"SELECT COUNT(*) FROM {schema}.ptg2_plan_catalog_outbox"
    ) == 23
    started = time.monotonic()
    drains = await asyncio.gather(
        *(
            outbox.drain_immutable_plan_catalog_outbox(
                max_requests=1,
                request_id=request_id,
            )
            for request_id in request_ids
        )
    )
    assert time.monotonic() - started < 2
    assert sum(drain_result.deferred for drain_result in drains) == 23
    assert await local_db.scalar(
        f"SELECT COUNT(*) FROM {schema}.ptg2_plan_catalog_outbox"
    ) == 23


async def _assert_catalog_replay(schema: str, local_db) -> None:
    replay = await outbox.drain_immutable_plan_catalog_outbox(max_requests=64)
    assert replay.persisted == 23
    assert await local_db.scalar(
        f"SELECT COUNT(*) FROM {schema}.ptg2_plan_catalog_outbox"
    ) == 0
    assert await local_db.scalar(
        f"SELECT COUNT(*) FROM {schema}.ptg2_plan_alias"
    ) == 1


async def _assert_catalog_crash_gap_replay(
    sessions,
    schema: str,
    local_db,
) -> None:
    crash_request = await _enqueue_source(sessions, schema, 24)
    assert await local_db.scalar(
        f"SELECT COUNT(*) FROM {schema}.ptg2_plan_catalog_outbox "
        f"WHERE request_id = '{crash_request}'"
    ) == 1
    crash_replay = await outbox.drain_immutable_plan_catalog_outbox(
        max_requests=1,
        request_id=crash_request,
    )
    assert crash_replay.persisted == 1
    assert await local_db.scalar(
        f"SELECT COUNT(*) FROM {schema}.ptg2_plan_catalog_outbox"
    ) == 0


async def _assert_conflicting_catalog_rejected(plan_row: dict) -> None:
    conflicting_plan_by_field = {**plan_row, "plan_name": "Conflicting Plan"}
    with pytest.raises(catalog.PTG2PlanCatalogConflict):
        await catalog.attempt_publish_immutable_plan_catalog(
            plan_rows=[conflicting_plan_by_field],
            alias_rows=[],
        )


@pytest.mark.asyncio
async def test_held_shared_plan_insert_does_not_block_twenty_three_sources(
    monkeypatch,
):
    """Prove shared plan contention defers catalog work without gating sources."""
    schema_name = "ptg_plan_catalog_hol"
    schema = _quote(schema_name)
    engine = create_async_engine(
        _dsn().replace("postgresql://", "postgresql+asyncpg://", 1),
        pool_size=5,
        max_overflow=0,
        pool_timeout=2,
    )
    sessions = async_sessionmaker(
        engine,
        expire_on_commit=False,
        autoflush=False,
    )
    local_db = Database(engine=engine, session_factory=sessions)
    monkeypatch.setattr(catalog, "db", local_db)
    monkeypatch.setattr(outbox, "db", local_db)
    monkeypatch.setattr(catalog, "resolve_ptg2_schema", lambda: schema_name)
    monkeypatch.setattr(outbox, "resolve_ptg2_schema", lambda: schema_name)
    monkeypatch.setattr(lifecycle, "db", local_db)
    monkeypatch.setattr(outbox, "_OUTBOX_RETRY_SECONDS", 0)

    await _create_plan_catalog_tables(engine, schema)
    plan_row = _plan_row()
    holder, holder_transaction = await _hold_shared_plan_row(
        sessions,
        schema,
        plan_row,
    )

    try:
        await _assert_contended_catalog_drains_defer(sessions, schema, local_db)
        await holder_transaction.commit()
        await holder.close()
        holder = None
        await _assert_catalog_replay(schema, local_db)
        await _assert_catalog_crash_gap_replay(sessions, schema, local_db)
        await _assert_conflicting_catalog_rejected(plan_row)
    finally:
        if holder is not None:
            await holder_transaction.rollback()
            await holder.close()
        async with engine.begin() as connection:
            await connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        await engine.dispose()


def test_plan_catalog_outbox_migration_is_source_local_and_replayable(
    monkeypatch,
) -> None:
    migration = _load_migration(MIGRATION_PATH)
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "ptg2_plan_catalog_contract")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    sql = "\n".join(statements)
    assert migration.down_revision == "20260810130000_ptg2_block_build_pins"
    assert "PRIMARY KEY (request_id)" in sql
    assert "UNIQUE (snapshot_id, chunk_index)" in sql
    assert "plan_rows jsonb NOT NULL" in sql
    assert "alias_rows jsonb NOT NULL" in sql
    assert "lease_until" in sql
    assert "payload_bytes BETWEEN 1 AND 524288" in sql
    assert "terminal_error_code" in sql
    migration.downgrade()
    assert "refusing to downgrade pending, leased, or poisoned" in "\n".join(
        statements
    )


async def _enqueue_catalog(
    sessions,
    *,
    snapshot_id: str,
    plans: list[dict[str, object]],
    aliases: list[dict[str, object]],
):
    async with sessions.begin() as session:
        return await outbox.enqueue_immutable_plan_catalog(
            session,
            snapshot_id=snapshot_id,
            plan_rows=plans,
            alias_rows=aliases,
        )


async def _prepare_large_catalog(sessions, schema: str, local_db):
    """Enqueue one chunked source plus eight single-chunk peers."""

    huge_plans = [_numbered_plan_row(ordinal) for ordinal in range(1, 21)]
    huge_aliases = [_numbered_alias_row(ordinal) for ordinal in range(1, 21)]
    huge_request = await _enqueue_catalog(
        sessions,
        snapshot_id="large-oldest",
        plans=huge_plans,
        aliases=huge_aliases,
    )
    assert len(huge_request.request_ids) == 2
    for ordinal in range(101, 109):
        await _enqueue_catalog(
            sessions,
            snapshot_id=f"peer-{ordinal}",
            plans=[_numbered_plan_row(ordinal)],
            aliases=[_numbered_alias_row(ordinal)],
        )
    bounds = await local_db.all(
        f"SELECT plan_count, alias_count, payload_bytes "
        f"FROM {schema}.ptg2_plan_catalog_outbox"
    )
    assert all(
        0 <= int(bound_row[0]) <= 16
        and 0 <= int(bound_row[1]) <= 128
        and 1 <= int(bound_row[2]) <= 512 * 1024
        for bound_row in bounds
    )
    return huge_plans


async def _assert_fair_catalog_drain(
    sessions,
    schema: str,
    local_db,
    huge_plans,
) -> None:
    """Defer the contended chunk while seven peers and later work persist."""

    holder, holder_transaction = await _hold_shared_plan_row(
        sessions,
        schema,
        huge_plans[0],
    )
    first_drain = await outbox.drain_immutable_plan_catalog_outbox(max_requests=8)
    assert (first_drain.claimed, first_drain.persisted, first_drain.deferred) == (
        8,
        7,
        1,
    )
    assert await local_db.scalar(
        f"SELECT COUNT(*) FROM {schema}.ptg2_plan_catalog_outbox"
    ) == 3
    await holder_transaction.commit()
    await holder.close()
    assert (
        await outbox.drain_immutable_plan_catalog_outbox(max_requests=8)
    ).persisted == 2
    assert (
        await outbox.drain_immutable_plan_catalog_outbox(max_requests=8)
    ).persisted == 1


async def _assert_catalog_lease_fence(engine, sessions, schema: str) -> None:
    """Reject a stale claim after its deterministic lease is reclaimed."""

    lease_request = await _enqueue_catalog(
        sessions,
        snapshot_id="lease-fence",
        plans=[_numbered_plan_row(201)],
        aliases=[],
    )
    stale_claim = await outbox._claim_request(request_id=lease_request.request_id)
    assert stale_claim is not None
    async with engine.begin() as connection:
        await connection.execute(
            text(
                f"UPDATE {schema}.ptg2_plan_catalog_outbox "
                "SET lease_until = now() - INTERVAL '1 second' "
                "WHERE request_id = :request_id"
            ),
            {"request_id": lease_request.request_id},
        )
    live_claim = await outbox._claim_request(request_id=lease_request.request_id)
    assert live_claim is not None
    assert not await outbox._is_claim_finish_committed(stale_claim, persisted=True)
    assert await outbox._is_claim_finish_committed(live_claim, persisted=True)


async def _assert_poison_does_not_block_peer(
    sessions,
    schema: str,
    local_db,
) -> None:
    """Quarantine an immutable conflict while the next source persists."""

    conflicting_plan = _numbered_plan_row(301)
    async with sessions.begin() as session:
        await session.execute(
            text(
                f"INSERT INTO {schema}.ptg2_plan "
                "(plan_hash, hash_prefix, plan_id, plan_id_type, plan_name, "
                "plan_market_type, issuer_name, plan_sponsor_name, "
                "canonical_payload, created_at) VALUES "
                "(:plan_hash, :hash_prefix, :plan_id, :plan_id_type, "
                "'Conflicting Name', :plan_market_type, :issuer_name, "
                ":plan_sponsor_name, CAST(:canonical_payload AS json), "
                ":created_at)"
            ),
            {
                **conflicting_plan,
                "canonical_payload": '{"plan_id":"plan-301"}',
            },
        )
    await _enqueue_catalog(
        sessions,
        snapshot_id="poison-first",
        plans=[conflicting_plan],
        aliases=[],
    )
    await _enqueue_catalog(
        sessions,
        snapshot_id="poison-peer",
        plans=[_numbered_plan_row(302)],
        aliases=[],
    )
    poison_drain = await outbox.drain_immutable_plan_catalog_outbox(max_requests=2)
    assert (poison_drain.poisoned, poison_drain.persisted) == (1, 1)
    assert await local_db.scalar(
        f"SELECT terminal_error_code FROM {schema}.ptg2_plan_catalog_outbox "
        "WHERE snapshot_id = 'poison-first'"
    ) == "immutable_conflict"


@pytest.mark.asyncio
async def test_large_oldest_catalog_is_chunked_fair_and_token_fenced(monkeypatch):
    """A contended multi-chunk source cannot monopolize peers or a stale lease."""

    schema_name = "ptg_plan_catalog_chunk_fairness"
    schema = _quote(schema_name)
    engine = create_async_engine(
        _dsn().replace("postgresql://", "postgresql+asyncpg://", 1),
        pool_size=8,
        max_overflow=0,
        pool_timeout=2,
    )
    sessions = async_sessionmaker(
        engine,
        expire_on_commit=False,
        autoflush=False,
    )
    local_db = Database(engine=engine, session_factory=sessions)
    monkeypatch.setattr(catalog, "db", local_db)
    monkeypatch.setattr(outbox, "db", local_db)
    monkeypatch.setattr(catalog, "resolve_ptg2_schema", lambda: schema_name)
    monkeypatch.setattr(outbox, "resolve_ptg2_schema", lambda: schema_name)
    monkeypatch.setattr(lifecycle, "db", local_db)
    monkeypatch.setattr(outbox, "_OUTBOX_RETRY_SECONDS", 0)
    await _create_plan_catalog_tables(engine, schema)

    huge_plans = await _prepare_large_catalog(sessions, schema, local_db)
    await _assert_fair_catalog_drain(sessions, schema, local_db, huge_plans)
    await _assert_catalog_lease_fence(engine, sessions, schema)
    await _assert_poison_does_not_block_peer(sessions, schema, local_db)

    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    await engine.dispose()
