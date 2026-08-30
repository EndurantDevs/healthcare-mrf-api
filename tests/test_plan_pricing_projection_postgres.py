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
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.ext.asyncio import create_async_engine

from api import plan_pricing_projection_contract as projection_contract

asyncpg = pytest.importorskip("asyncpg")

POSTGRES_DSN_ENV = "HLTHPRT_PLAN_PRICING_PROJECTION_POSTGRES_DSN"
TEST_DATABASE_PATTERN = re.compile(r"(?:^|[_-])test(?:[_-]|$)", re.IGNORECASE)
MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260825150000_plan_pricing_card_projection.py"
)
def _load_projection_migration(label: str):
    module_spec = importlib.util.spec_from_file_location(
        f"plan_pricing_projection_migration_{label}",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _migration_statements(monkeypatch, schema: str) -> list[str]:
    migration = _load_projection_migration(schema)
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setattr(migration, "_create_zip_index", lambda _schema: None)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    return statements


def _sqlalchemy_async_dsn(database_dsn: str) -> str:
    if database_dsn.startswith("postgresql://"):
        return database_dsn.replace(
            "postgresql://", "postgresql+asyncpg://", 1
        )
    if database_dsn.startswith("postgres://"):
        return database_dsn.replace(
            "postgres://", "postgresql+asyncpg://", 1
        )
    return database_dsn


async def _run_zip_index_upgrade(
    async_engine,
    migration,
    monkeypatch,
    schema: str,
) -> None:
    async with async_engine.connect() as async_connection:
        def upgrade(sync_connection):
            context = MigrationContext.configure(sync_connection)
            monkeypatch.setattr(migration, "op", Operations(context))
            with context.begin_transaction():
                migration._create_zip_index(schema)

        await async_connection.run_sync(upgrade)


async def _zip_index_is_valid(
    connection,
    schema: str,
    index_name: str,
) -> bool | None:
    return await connection.fetchval(
        """
        SELECT index_record.indisvalid
          FROM pg_catalog.pg_index AS index_record
          JOIN pg_catalog.pg_class AS index_class
            ON index_class.oid = index_record.indexrelid
          JOIN pg_catalog.pg_namespace AS index_namespace
            ON index_namespace.oid = index_class.relnamespace
         WHERE index_namespace.nspname = $1
           AND index_class.relname = $2
        """,
        schema,
        index_name,
    )


async def _create_interrupted_zip_index(
    connection,
    schema: str,
    index_name: str,
) -> None:
    quoted_schema = f'"{schema}"'
    await connection.execute(f"CREATE SCHEMA {quoted_schema}")
    await connection.execute(
        f"""
        CREATE TABLE {quoted_schema}.geo_zip_lookup (
            zip_code varchar(5) NOT NULL,
            latitude double precision NOT NULL,
            longitude double precision NOT NULL
        )
        """
    )
    await connection.execute(
        f"INSERT INTO {quoted_schema}.geo_zip_lookup VALUES "
        "('10001', 40.1, -73.1), ('10001', 40.2, -73.2)"
    )
    with pytest.raises(asyncpg.UniqueViolationError):
        await connection.execute(
            f'CREATE UNIQUE INDEX CONCURRENTLY "{index_name}" ON '
            f"{quoted_schema}.geo_zip_lookup (zip_code)"
        )


async def _create_import_run_stub(connection, schema: str) -> None:
    await connection.execute(
        f"""
        CREATE TABLE {schema}.import_run (
            run_id varchar(64) PRIMARY KEY,
            importer varchar(64) NOT NULL,
            idempotency_key varchar(160)
        )
        """
    )


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


async def _assert_seal_blocks_later_child(
    admin,
    dsn: str,
    schema: str,
    connections: list,
    tasks: list[asyncio.Task],
) -> None:
    projection_id = "1" * 64
    digest = "a" * 64
    await _candidate(admin, schema, projection_id, digest)
    sealer = await asyncpg.connect(dsn)
    child_writer = await asyncpg.connect(dsn)
    connections.extend((sealer, child_writer))
    seal_transaction = sealer.transaction()
    child_transaction = child_writer.transaction()
    await seal_transaction.start()
    await _seal(sealer, schema, projection_id, digest)
    await child_transaction.start()
    child_task = asyncio.create_task(_card(child_writer, schema, projection_id))
    tasks.append(child_task)
    await asyncio.sleep(0.05)
    assert not child_task.done()
    await seal_transaction.commit()
    with pytest.raises(asyncpg.RaiseError, match="immutable"):
        await asyncio.wait_for(child_task, timeout=2)
    await child_transaction.rollback()


async def _assert_child_blocks_incorrect_seal(
    admin,
    dsn: str,
    schema: str,
    connections: list,
    tasks: list[asyncio.Task],
) -> None:
    projection_id = "2" * 64
    digest = "b" * 64
    await _candidate(admin, schema, projection_id, digest)
    child_writer = await asyncpg.connect(dsn)
    sealer = await asyncpg.connect(dsn)
    connections.extend((child_writer, sealer))
    child_transaction = child_writer.transaction()
    seal_transaction = sealer.transaction()
    await child_transaction.start()
    await _card(child_writer, schema, projection_id)
    await seal_transaction.start()
    seal_task = asyncio.create_task(_seal(sealer, schema, projection_id, digest))
    tasks.append(seal_task)
    await asyncio.sleep(0.05)
    assert not seal_task.done()
    await child_transaction.commit()
    with pytest.raises(asyncpg.RaiseError, match="receipt counts"):
        await asyncio.wait_for(seal_task, timeout=2)
    await seal_transaction.rollback()


async def _assert_direct_ready_insert_rechecks_counts(
    admin,
    schema: str,
) -> None:
    with pytest.raises(asyncpg.RaiseError, match="receipt counts"):
        await admin.execute(
            f"""
            INSERT INTO {schema}.plan_pricing_projection_candidate (
                projection_id, contract_version, binding_manifest_digest,
                binding_manifest, provider_signature, state, content_digest,
                card_row_count, aggregate_row_count, fragment_byte_count,
                build_seconds, completed_at
            ) VALUES (
                $1, 'plan_pricing_card_v2', $2, '[]'::jsonb, $2, 'ready', $2,
                1, 0, 0, 0, transaction_timestamp()
            )
            """,
            "3" * 64,
            "c" * 64,
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
        await _create_import_run_stub(admin, schema)
        for statement in _migration_statements(monkeypatch, schema):
            await admin.execute(statement)

        await _assert_seal_blocks_later_child(
            admin, dsn, schema, connections, tasks
        )
        await _assert_child_blocks_incorrect_seal(
            admin, dsn, schema, connections, tasks
        )
        await _assert_direct_ready_insert_rechecks_counts(admin, schema)
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


@pytest.mark.asyncio
async def test_provider_generation_lock_blocks_zip_relation_replacement(
    monkeypatch,
):
    dsn = os.getenv(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")

    admin = await asyncpg.connect(dsn)
    database_name = await admin.fetchval("SELECT current_database()")
    if TEST_DATABASE_PATTERN.search(str(database_name)) is None:
        await admin.close()
        pytest.fail(f"{POSTGRES_DSN_ENV} must target an explicit test database")
    schema = f"plan_pricing_provider_lock_{uuid.uuid4().hex[:12]}"
    quoted_schema = f'"{schema}"'
    async_engine = create_async_engine(_sqlalchemy_async_dsn(dsn))
    replacement = await asyncpg.connect(dsn)
    replacement_task = None
    try:
        await admin.execute(f"CREATE SCHEMA {quoted_schema}")
        for relation in projection_contract.PROVIDER_RELATIONS:
            await admin.execute(
                f'CREATE TABLE {quoted_schema}."{relation}" (value integer)'
            )
        monkeypatch.setattr(projection_contract, "SCHEMA", schema)
        monkeypatch.setattr(
            projection_contract.geo_projection,
            "projection_dependency_lock_sql",
            lambda _schema: "SELECT 1",
        )

        async with async_engine.connect() as lock_connection:
            transaction = await lock_connection.begin()
            await projection_contract.lock_provider_generation(lock_connection)
            replacement_task = asyncio.create_task(
                replacement.execute(
                    f"ALTER TABLE {quoted_schema}.geo_zip_lookup "
                    "ADD COLUMN replacement_marker integer"
                )
            )
            await asyncio.sleep(0.05)
            assert replacement_task.done() is False
            await transaction.rollback()
            await asyncio.wait_for(replacement_task, timeout=2)
    finally:
        if replacement_task is not None and not replacement_task.done():
            replacement_task.cancel()
            await asyncio.gather(replacement_task, return_exceptions=True)
        await replacement.close()
        await async_engine.dispose()
        await admin.execute(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE")
        await admin.close()


@pytest.mark.asyncio
async def test_interrupted_zip_index_is_rebuilt_concurrently(monkeypatch):
    dsn = os.getenv(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")

    admin = await asyncpg.connect(dsn)
    database_name = await admin.fetchval("SELECT current_database()")
    if TEST_DATABASE_PATTERN.search(str(database_name)) is None:
        await admin.close()
        pytest.fail(f"{POSTGRES_DSN_ENV} must target an explicit test database")
    schema = f"plan_pricing_index_{uuid.uuid4().hex[:12]}"
    quoted_schema = f'"{schema}"'
    migration = _load_projection_migration(schema)
    async_engine = create_async_engine(_sqlalchemy_async_dsn(dsn))
    try:
        await _create_interrupted_zip_index(
            admin,
            schema,
            migration.ZIP_INDEX_NAME,
        )
        assert await _zip_index_is_valid(
            admin, schema, migration.ZIP_INDEX_NAME
        ) is False

        await _run_zip_index_upgrade(
            async_engine,
            migration,
            monkeypatch,
            schema,
        )

        assert await _zip_index_is_valid(
            admin, schema, migration.ZIP_INDEX_NAME
        ) is True
        index_definition = await admin.fetchval(
            "SELECT pg_get_indexdef(to_regclass($1))",
            f"{schema}.{migration.ZIP_INDEX_NAME}",
        )
        assert "(latitude, longitude, zip_code)" in index_definition
    finally:
        await async_engine.dispose()
        await admin.execute(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE")
        await admin.close()


@pytest.mark.asyncio
async def test_zip_index_skips_relation_without_required_columns(monkeypatch):
    dsn = os.getenv(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")

    admin = await asyncpg.connect(dsn)
    database_name = await admin.fetchval("SELECT current_database()")
    if TEST_DATABASE_PATTERN.search(str(database_name)) is None:
        await admin.close()
        pytest.fail(f"{POSTGRES_DSN_ENV} must target an explicit test database")
    schema = f"plan_pricing_columns_{uuid.uuid4().hex[:12]}"
    quoted_schema = f'"{schema}"'
    migration = _load_projection_migration(schema)
    async_engine = create_async_engine(_sqlalchemy_async_dsn(dsn))
    try:
        await admin.execute(f"CREATE SCHEMA {quoted_schema}")
        await admin.execute(
            f"CREATE TABLE {quoted_schema}.geo_zip_lookup "
            "(zip_code varchar(5) NOT NULL)"
        )

        await _run_zip_index_upgrade(
            async_engine,
            migration,
            monkeypatch,
            schema,
        )

        assert await _zip_index_is_valid(
            admin, schema, migration.ZIP_INDEX_NAME
        ) is None
    finally:
        await async_engine.dispose()
        await admin.execute(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE")
        await admin.close()
