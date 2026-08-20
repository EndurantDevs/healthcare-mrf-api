# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import importlib
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from tests.test_prescription_autocomplete_postgres import _AsyncpgDatabase
from tests.test_prescription_autocomplete_rollup_postgres import (
    ROLLUP_TABLE,
    _create_provider_table,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260820140000_prescription_autocomplete_rollup.py"
)
drug_claims = importlib.import_module("process.drug_claims")


class _TransactionalAsyncpgDatabase(_AsyncpgDatabase):
    @asynccontextmanager
    async def transaction(self):
        async with self.connection.transaction():
            yield


def _load_rollup_migration():
    specification = importlib.util.spec_from_file_location(
        "prescription_autocomplete_rollup_postgres_migration",
        MIGRATION_PATH,
    )
    assert specification and specification.loader
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    return migration


async def _run_rollup_migration(
    engine,
    migration,
    monkeypatch,
    schema,
    operation,
):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    async with engine.connect() as connection:

        def run(sync_connection):
            context = MigrationContext.configure(sync_connection)
            monkeypatch.setattr(migration, "op", Operations(context))
            with context.begin_transaction():
                getattr(migration, operation)()

        await connection.run_sync(run)


async def _create_migration_source(connection, schema):
    relation = f'"{schema}".pricing_provider_prescription'
    await connection.execute(f'CREATE SCHEMA "{schema}"')
    await _create_provider_table(connection, relation)
    await connection.execute(
        f"""
        INSERT INTO {relation} VALUES
            (1000000001, 2023, 'HP_RX_CODE', 'RX-A',
             'Aspirin', NULL, 'Brand A', 10, 100, 5),
            (1000000002, 2023, 'HP_RX_CODE', 'RX-A',
             'Different', NULL, 'Brand A', 20, 200, 6),
            (1000000003, 2023, 'HP_RX_CODE', 'RX-A',
             'Aspirin', NULL, 'Brand A', 30, 300, 7),
            (1000000004, 2022, 'HP_RX_CODE', 'RX-A',
             'Prior aspirin', NULL, NULL, 40, 400, 8),
            (1000000005, 2023, 'HP_RX_CODE', 'RX-B',
             NULL, 'Aspirin generic', NULL, 50, 500, 9)
        """
    )


async def _assert_migration_rollup(connection, schema):
    rollup_rows = await connection.fetch(
        f"""
        SELECT year, rx_code, variant_id, rx_name, generic_name,
               brand_name, total_claims, total_drug_cost, total_benes
        FROM "{schema}".{ROLLUP_TABLE}
        ORDER BY year, rx_code, variant_id
        """
    )
    assert [tuple(rollup_row.values()) for rollup_row in rollup_rows] == [
        (2022, "RX-A", 1, "Prior aspirin", None, None, 40.0, 400, 8.0),
        (2023, "RX-A", 1, "Aspirin", None, "Brand A", 40.0, 400, 12.0),
        (2023, "RX-A", 2, "Different", None, "Brand A", 20.0, 200, 6.0),
        (2023, "RX-B", 1, None, "Aspirin generic", None, 50.0, 500, 9.0),
    ]


async def _assert_provider_swap_invalidates_rollup(connection, schema):
    guarded_count_sql = (
        f'SELECT COUNT(*) FROM "{schema}".{ROLLUP_TABLE} '
        "WHERE source_relation_fingerprint = "
        f"to_regclass('\"{schema}\".\"pricing_provider_prescription\"')::oid::text"
    )
    assert await connection.fetchval(guarded_count_sql) == 4
    transaction = connection.transaction()
    await transaction.start()
    try:
        await connection.execute(
            f'CREATE TABLE "{schema}".provider_replacement '
            f'(LIKE "{schema}".pricing_provider_prescription)'
        )
        await connection.execute(
            f'ALTER TABLE "{schema}".pricing_provider_prescription '
            "RENAME TO provider_previous"
        )
        await connection.execute(
            f'ALTER TABLE "{schema}".provider_replacement '
            "RENAME TO pricing_provider_prescription"
        )
        assert await connection.fetchval(guarded_count_sql) == 0
    finally:
        await transaction.rollback()


async def _create_publication_fixture(connection, migration, schema, staged):
    await connection.execute(f'CREATE SCHEMA "{schema}"')
    await connection.execute(
        f'CREATE TABLE "{schema}".pricing_prescription '
        "(rx_code_system text, rx_code text)"
    )
    await _create_provider_table(
        connection,
        f'"{schema}".pricing_provider_prescription',
    )
    await connection.execute(migration._create_table_sql(schema))
    for model_name, live_table in (
        ("PricingPrescription", "pricing_prescription"),
        ("PricingProviderPrescription", "pricing_provider_prescription"),
        ("PricingProviderPrescriptionAutocomplete", ROLLUP_TABLE),
    ):
        stage_table = staged[model_name].__tablename__
        await connection.execute(
            f'CREATE TABLE "{schema}"."{stage_table}" '
            f'(LIKE "{schema}"."{live_table}" INCLUDING ALL)'
        )


async def _seed_and_index_publication_fixture(connection, schema, staged):
    provider_stage = staged["PricingProviderPrescription"].__tablename__
    prescription_stage = staged["PricingPrescription"].__tablename__
    await connection.execute(
        f"""
        INSERT INTO "{schema}"."{provider_stage}" VALUES
            (1000000001, 2023, 'HP_RX_CODE', 'RX-A',
             'Aspirin', 'Aspirin', 'Brand A', 10, 100, 5);
        CREATE UNIQUE INDEX "{prescription_stage}_idx_primary"
            ON "{schema}"."{prescription_stage}" (rx_code_system, rx_code);
        CREATE UNIQUE INDEX "{provider_stage}_idx_primary"
            ON "{schema}"."{provider_stage}"
            (npi, year, rx_code_system, rx_code)
        """
    )


async def _seed_live_publication_generation(connection, schema):
    await connection.execute(
        f"""
        INSERT INTO "{schema}".pricing_provider_prescription VALUES
            (1000000000, 2023, 'HP_RX_CODE', 'RX-A',
             'Prior name', 'Prior generic', 'Prior brand', 1, 10, 1);
        INSERT INTO "{schema}".{ROLLUP_TABLE} VALUES
            (2023, 'HP_RX_CODE', 'RX-A', 1,
             'Prior name', 'Prior generic', 'Prior brand', 1, 10, 1,
             to_regclass('"{schema}"."pricing_provider_prescription"')::oid::text)
        """
    )


async def _publication_generation(connection, schema):
    publication_row = await connection.fetchrow(
        f"""
        SELECT provider.rx_name, rollup.rx_name,
               rollup.source_relation_fingerprint =
                   to_regclass('"{schema}"."pricing_provider_prescription"')::oid::text
        FROM "{schema}".pricing_provider_prescription AS provider
        JOIN "{schema}".{ROLLUP_TABLE} AS rollup
          USING (year, rx_code_system, rx_code)
        """
    )
    return tuple(publication_row) if publication_row else None


async def _wait_for_publication_lock(connection, writer_pid):
    for _attempt in range(100):
        wait_type = await connection.fetchval(
            "SELECT wait_event_type FROM pg_stat_activity WHERE pid = $1",
            writer_pid,
        )
        if wait_type == "Lock":
            return
        await asyncio.sleep(0.01)
    raise AssertionError("publication did not wait for the active reader")


async def _assert_atomic_publication_reader(
    reader,
    writer,
    staged,
    schema,
):
    async with reader.transaction():
        assert await _publication_generation(reader, schema) == (
            "Prior name",
            "Prior name",
            True,
        )
        publication = asyncio.create_task(
            drug_claims._publish_by_table_rename(staged, schema)
        )
        await _wait_for_publication_lock(reader, writer.get_server_pid())
        assert not publication.done()
        assert await _publication_generation(reader, schema) == (
            "Prior name",
            "Prior name",
            True,
        )
    await asyncio.wait_for(publication, timeout=5)
    assert await _publication_generation(reader, schema) == (
        "Aspirin",
        "Aspirin",
        True,
    )


@pytest.mark.asyncio
async def test_rollup_migration_backfills_and_tracks_provider_swap(monkeypatch):
    """Bind migration rows to the exact live provider relation generation."""

    dsn = os.getenv("HLTHPRT_PRESCRIPTION_AUTOCOMPLETE_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")

    migration = _load_rollup_migration()
    schema = f"prescription_rollup_migration_{uuid.uuid4().hex[:12]}"
    connection = await asyncpg.connect(dsn)
    engine = create_async_engine(
        dsn.replace("postgresql://", "postgresql+asyncpg://", 1),
        pool_size=1,
        max_overflow=0,
    )
    try:
        await _create_migration_source(connection, schema)
        await _run_rollup_migration(
            engine, migration, monkeypatch, schema, "upgrade"
        )
        await _assert_migration_rollup(connection, schema)
        await _assert_provider_swap_invalidates_rollup(connection, schema)
        await _run_rollup_migration(
            engine, migration, monkeypatch, schema, "downgrade"
        )
        assert not await connection.fetchval(
            "SELECT to_regclass($1)", f"{schema}.{ROLLUP_TABLE}"
        )
        await _run_rollup_migration(
            engine, migration, monkeypatch, schema, "upgrade"
        )
        await _assert_migration_rollup(connection, schema)
    finally:
        await engine.dispose()
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()


@pytest.mark.asyncio
async def test_rollup_migration_supports_missing_provider_table(monkeypatch):
    dsn = os.getenv("HLTHPRT_PRESCRIPTION_AUTOCOMPLETE_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")

    migration = _load_rollup_migration()
    schema = f"prescription_rollup_fresh_{uuid.uuid4().hex[:12]}"
    connection = await asyncpg.connect(dsn)
    engine = create_async_engine(
        dsn.replace("postgresql://", "postgresql+asyncpg://", 1),
        pool_size=1,
        max_overflow=0,
    )
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await _run_rollup_migration(
            engine, migration, monkeypatch, schema, "upgrade"
        )
        assert await connection.fetchval(
            "SELECT to_regclass($1)", f"{schema}.{ROLLUP_TABLE}"
        )
        assert await connection.fetchval(
            f'SELECT COUNT(*) FROM "{schema}".{ROLLUP_TABLE}'
        ) == 0
    finally:
        await engine.dispose()
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()


@pytest.mark.asyncio
async def test_rollup_migration_rejects_incompatible_existing_table(
    monkeypatch,
):
    dsn = os.getenv("HLTHPRT_PRESCRIPTION_AUTOCOMPLETE_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")

    migration = _load_rollup_migration()
    schema = f"prescription_rollup_mismatch_{uuid.uuid4().hex[:12]}"
    connection = await asyncpg.connect(dsn)
    engine = create_async_engine(
        dsn.replace("postgresql://", "postgresql+asyncpg://", 1),
        pool_size=1,
        max_overflow=0,
    )
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await _create_provider_table(
            connection,
            f'"{schema}".pricing_provider_prescription',
        )
        await connection.execute(migration._create_table_sql(schema))
        await connection.execute(
            f"""
            ALTER TABLE "{schema}".{ROLLUP_TABLE}
                DROP CONSTRAINT {ROLLUP_TABLE}_pkey,
                ALTER COLUMN year TYPE text,
                ALTER COLUMN year DROP NOT NULL;
            INSERT INTO "{schema}".{ROLLUP_TABLE}
                (year, rx_code_system, rx_code, variant_id,
                 source_relation_fingerprint)
            VALUES ('sentinel', 'HP_RX_CODE', 'RX-1', 1, 'source')
            """
        )
        with pytest.raises(
            RuntimeError,
            match=f"existing_schema_table_mismatch:{schema}.{ROLLUP_TABLE}",
        ):
            await _run_rollup_migration(
                engine,
                migration,
                monkeypatch,
                schema,
                "upgrade",
            )
        assert await connection.fetchval(
            f'SELECT year FROM "{schema}".{ROLLUP_TABLE}'
        ) == "sentinel"
    finally:
        await engine.dispose()
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()


@pytest.mark.asyncio
async def test_staged_drug_claims_publication_keeps_current_rollup(monkeypatch):
    """Create and atomically rename the model-declared import rollup."""

    dsn = os.getenv("HLTHPRT_PRESCRIPTION_AUTOCOMPLETE_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")

    migration = _load_rollup_migration()
    schema = f"prescription_rollup_publish_{uuid.uuid4().hex[:12]}"
    connection = await asyncpg.connect(dsn)
    reader = await asyncpg.connect(dsn)
    staged = drug_claims._staging_classes(
        "abcdefghijkl_12345678",
        schema,
    )
    try:
        await _create_publication_fixture(connection, migration, schema, staged)
        await _seed_and_index_publication_fixture(connection, schema, staged)
        await _seed_live_publication_generation(connection, schema)
        monkeypatch.setattr(
            drug_claims,
            "db",
            _TransactionalAsyncpgDatabase(connection),
        )
        await drug_claims._materialize_prescription_autocomplete_rollup(
            schema,
            staged["PricingProviderPrescriptionAutocomplete"].__tablename__,
            staged["PricingProviderPrescription"].__tablename__,
        )
        await drug_claims._ensure_indexes(
            staged["PricingProviderPrescriptionAutocomplete"],
            schema,
        )
        await _assert_atomic_publication_reader(
            reader,
            connection,
            staged,
            schema,
        )

        assert await connection.fetchval(
            f'SELECT COUNT(*) FROM "{schema}".{ROLLUP_TABLE} '
            "WHERE source_relation_fingerprint = "
            f"to_regclass('\"{schema}\".\"pricing_provider_prescription\"')::oid::text"
        ) == 1
        assert await connection.fetchval(
            "SELECT to_regclass($1)",
            f"{schema}.{ROLLUP_TABLE}_idx_primary",
        )
    finally:
        await reader.close()
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()
