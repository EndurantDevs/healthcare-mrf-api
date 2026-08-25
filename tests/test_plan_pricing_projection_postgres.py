# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real PostgreSQL concurrency proof for immutable plan-pricing sealing."""

from __future__ import annotations

import asyncio
import importlib.util
import os
import re
import uuid
from pathlib import Path

import pytest


asyncpg = pytest.importorskip("asyncpg")

POSTGRES_DSN_ENV = "HLTHPRT_PLAN_PRICING_PROJECTION_POSTGRES_DSN"
TEST_DATABASE_PATTERN = re.compile(r"(?:^|[_-])test(?:[_-]|$)", re.IGNORECASE)
MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260825150000_plan_pricing_card_projection.py"
)


def _migration_statements(monkeypatch, schema: str) -> list[str]:
    module_spec = importlib.util.spec_from_file_location(
        f"plan_pricing_projection_migration_{schema}",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    return statements


async def _candidate(
    connection,
    schema: str,
    projection_id: str,
    digest: str,
) -> None:
    await connection.execute(
        f"""
        INSERT INTO {schema}.plan_pricing_projection_candidate (
            projection_id, contract_version, binding_manifest_digest,
            binding_manifest, provider_signature, state
        ) VALUES ($1, 'plan_pricing_card_v2', $2, '[]'::jsonb, $2, 'building')
        """,
        projection_id,
        digest,
    )


async def _card(connection, schema: str, projection_id: str) -> None:
    await connection.execute(
        f"""
        INSERT INTO {schema}.plan_pricing_card (
            projection_id, code_system, code, geo_cell, npi,
            minimum_negotiated_rate, maximum_negotiated_rate,
            rate_count, fragment
        ) VALUES ($1, 'CPT', '27447', '00001', 1234567890, 1, 1, 1, $2)
        """,
        projection_id,
        b"{}",
    )


async def _seal(
    connection,
    schema: str,
    projection_id: str,
    digest: str,
) -> None:
    await connection.execute(
        f"""
        UPDATE {schema}.plan_pricing_projection_candidate
           SET state = 'ready',
               content_digest = $2,
               card_row_count = 0,
               aggregate_row_count = 0,
               fragment_byte_count = 0,
               build_seconds = 0,
               completed_at = transaction_timestamp()
         WHERE projection_id = $1
        """,
        projection_id,
        digest,
    )


@pytest.mark.asyncio
async def test_ready_seal_serializes_against_child_writes(monkeypatch):
    dsn = os.getenv(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")

    admin = await asyncpg.connect(dsn)
    database_name = await admin.fetchval("SELECT current_database()")
    if TEST_DATABASE_PATTERN.search(str(database_name)) is None:
        await admin.close()
        pytest.fail(f"{POSTGRES_DSN_ENV} must target an explicit test database")

    schema = f"plan_pricing_guard_{uuid.uuid4().hex[:12]}"
    connections = []
    tasks: list[asyncio.Task] = []
    try:
        await admin.execute(f"CREATE SCHEMA {schema}")
        await admin.execute(
            f"""
            CREATE TABLE {schema}.geo_zip_lookup (
                zip_code varchar(5),
                latitude double precision,
                longitude double precision
            )
            """
        )
        for statement in _migration_statements(monkeypatch, schema):
            await admin.execute(statement)

        # A seal that owns the parent lock makes a later child reject.
        projection_one = "1" * 64
        digest_one = "a" * 64
        await _candidate(admin, schema, projection_one, digest_one)
        sealer = await asyncpg.connect(dsn)
        child_writer = await asyncpg.connect(dsn)
        connections.extend((sealer, child_writer))
        seal_transaction = sealer.transaction()
        child_transaction = child_writer.transaction()
        await seal_transaction.start()
        await _seal(sealer, schema, projection_one, digest_one)
        await child_transaction.start()
        child_task = asyncio.create_task(
            _card(child_writer, schema, projection_one)
        )
        tasks.append(child_task)
        await asyncio.sleep(0.05)
        assert not child_task.done()
        await seal_transaction.commit()
        with pytest.raises(asyncpg.RaiseError, match="immutable"):
            await asyncio.wait_for(child_task, timeout=2)
        await child_transaction.rollback()

        # A child that owns the parent lock makes the seal re-count and reject.
        projection_two = "2" * 64
        digest_two = "b" * 64
        await _candidate(admin, schema, projection_two, digest_two)
        child_writer = await asyncpg.connect(dsn)
        sealer = await asyncpg.connect(dsn)
        connections.extend((child_writer, sealer))
        child_transaction = child_writer.transaction()
        seal_transaction = sealer.transaction()
        await child_transaction.start()
        await _card(child_writer, schema, projection_two)
        await seal_transaction.start()
        seal_task = asyncio.create_task(
            _seal(sealer, schema, projection_two, digest_two)
        )
        tasks.append(seal_task)
        await asyncio.sleep(0.05)
        assert not seal_task.done()
        await child_transaction.commit()
        with pytest.raises(asyncpg.RaiseError, match="receipt counts"):
            await asyncio.wait_for(seal_task, timeout=2)
        await seal_transaction.rollback()
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for connection in connections:
            if not connection.is_closed():
                await connection.close()
        await admin.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await admin.close()
