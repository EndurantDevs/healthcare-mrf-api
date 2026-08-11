# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
import uuid

import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from process.provider_directory_rooted_graph_registration import (
    register_provider_directory_rooted_graph_source,
)
from tests.formulary_fhir_twin_admission_pg_support import connect
from tests.formulary_fhir_twin_admission_pg_support import database_url
from tests.formulary_fhir_twin_admission_pg_support import drop_schema
from tests.formulary_fhir_twin_admission_pg_support import load_migration
from tests.formulary_fhir_twin_admission_pg_support import quoted
from tests.formulary_fhir_twin_admission_pg_support import run_migration
from tests.provider_directory_rooted_graph_pg_support import (
    configure_database,
    extend_publication_foundation,
)
from tests.test_provider_directory_rooted_graph_acquisition_postgres import (
    _load_legacy_migrations,
)
from tests.test_provider_directory_uhc_flex_practitioner_publication_postgres import (
    _prepare_publication_schema,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions"
    / "20260811020000_provider_directory_rooted_graph_acquisition.py"
)
ACQUISITION = "provider_directory_rooted_graph_acquisition"
IDENTITY_CHECK = "provider_directory_rooted_graph_acquisition_identity_check"


class _OwnedTableSeed:
    def __init__(self, migration, schema_name: str) -> None:
        self.migration = migration
        self.schema_name = schema_name
        self.op = None

    def upgrade(self) -> None:
        self.migration.op = self.op
        for create_tables in (
            self.migration._create_acquisition,
            self.migration._create_work,
            self.migration._create_resource,
            self.migration._create_edge,
            self.migration._create_twin_tables,
            self.migration._create_publication_tables,
        ):
            create_tables(self.schema_name)


@asynccontextmanager
async def _adoption_scope(monkeypatch, label: str):
    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    schema = quoted(schema_name)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = configure_database(monkeypatch, url)
    migration = load_migration(MIGRATION_PATH, f"rooted_adoption_{label}")
    legacy_migrations = _load_legacy_migrations(f"rooted_adoption_legacy_{label}")
    connection = None
    try:
        await _prepare_publication_schema(
            engine,
            url,
            schema_name,
            schema,
            legacy_migrations,
        )
        connection = await connect(url)
        await extend_publication_foundation(connection, schema_name)
        await database.connect()
        await register_provider_directory_rooted_graph_source(database=database)
        await run_migration(
            engine,
            _OwnedTableSeed(migration, schema_name),
            "upgrade",
        )
        yield SimpleNamespace(
            connection=connection,
            database=database,
            engine=engine,
            migration=migration,
            schema=schema,
            schema_name=schema_name,
        )
    finally:
        await database.disconnect()
        if connection is not None:
            await connection.close()
        await drop_schema(engine, schema_name)
        await engine.dispose()


async def _drop_identity_check(context) -> None:
    await context.connection.execute(
        f"ALTER TABLE {context.schema}.{ACQUISITION} "
        f"DROP CONSTRAINT {IDENTITY_CHECK}"
    )


async def _insert_valid_acquisition(context) -> str:
    migration = context.migration
    acquisition_id = "pdrga_" + "9" * 48
    root_dataset_id = await context.connection.fetchval(
        f"SELECT dataset_id FROM {context.schema}.provider_directory_endpoint_dataset "
        "ORDER BY dataset_id LIMIT 1"
    )
    assert root_dataset_id is not None
    await context.connection.execute(
        f"""
        INSERT INTO {context.schema}.{ACQUISITION} (
            acquisition_id, storage_contract_id, scope_id,
            root_source_id, root_endpoint_id,
            acquisition_source_id, acquisition_endpoint_id,
            source_authority_id, root_dataset_variant,
            root_publication_contract_id, endpoint_signature_sha256,
            root_dataset_id, root_dataset_hash, root_content_proof_sha256,
            root_cohort_id, root_resource_type, root_resource_count,
            connector_id, graph_contract_sha256, query_contract_sha256,
            acquisition_role, run_id, dataset_intent_id,
            max_work_items, max_resource_rows, max_edge_rows, max_payload_bytes,
            status, rooted_graph_complete,
            endpoint_collection_complete, endpoint_complete
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
            $12, $13, $14, $15, 'Practitioner', $16, $17, $18, $19,
            $20, $21, $22, $23, $24, $25, $26,
            'building', false, false, false
        )
        """,
        acquisition_id,
        migration._STORAGE_CONTRACT,
        "pdrgs_" + "8" * 48,
        migration._LEGACY_SOURCE_ID,
        migration._LEGACY_ENDPOINT_ID,
        migration._ROOTED_SOURCE_ID,
        migration._ROOTED_ENDPOINT_ID,
        migration._SOURCE_AUTHORITY,
        migration._LEGACY_VARIANT,
        migration._LEGACY_PUBLICATION_CONTRACT,
        migration._ROOTED_ENDPOINT_SIGNATURE,
        root_dataset_id,
        "a" * 64,
        "b" * 64,
        "synthetic-adoption-root",
        1,
        migration._CONNECTOR_ID,
        migration._GRAPH_CONTRACT_SHA256,
        migration._QUERY_CONTRACT_SHA256,
        "baseline",
        "pdrgr_" + "7" * 48,
        "pdrgi_" + "6" * 48,
        20,
        20,
        40,
        1_000_000,
    )
    return acquisition_id


@pytest.mark.asyncio
async def test_missing_check_on_empty_adopted_table_is_validated(monkeypatch) -> None:
    async with _adoption_scope(monkeypatch, "missing") as context:
        await _drop_identity_check(context)

        await run_migration(context.engine, context.migration, "upgrade")

        check_record = await context.connection.fetchrow(
            """
            SELECT constraint_row.convalidated,
                   pg_catalog.pg_get_constraintdef(constraint_row.oid, false)
              FROM pg_catalog.pg_constraint AS constraint_row
              JOIN pg_catalog.pg_class AS relation
                ON relation.oid = constraint_row.conrelid
              JOIN pg_catalog.pg_namespace AS namespace
                ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = $1
               AND relation.relname = $2
               AND constraint_row.conname = $3
            """,
            context.schema_name,
            ACQUISITION,
            IDENTITY_CHECK,
        )
        assert check_record is not None and check_record["convalidated"] is True
        assert context.migration._ROOTED_ENDPOINT_SIGNATURE in check_record[1]


@pytest.mark.asyncio
async def test_weaker_named_check_rejects_adoption_without_mutation(
    monkeypatch,
) -> None:
    async with _adoption_scope(monkeypatch, "weaker") as context:
        await _drop_identity_check(context)
        await context.connection.execute(
            f"ALTER TABLE {context.schema}.{ACQUISITION} "
            f"ADD CONSTRAINT {IDENTITY_CHECK} CHECK (acquisition_id IS NOT NULL)"
        )

        with pytest.raises(DBAPIError, match="rooted_graph_check_mismatch"):
            await run_migration(context.engine, context.migration, "upgrade")

        definition = await context.connection.fetchval(
            """
            SELECT pg_catalog.pg_get_constraintdef(constraint_row.oid, false)
              FROM pg_catalog.pg_constraint AS constraint_row
              JOIN pg_catalog.pg_class AS relation
                ON relation.oid = constraint_row.conrelid
              JOIN pg_catalog.pg_namespace AS namespace
                ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = $1
               AND relation.relname = $2
               AND constraint_row.conname = $3
            """,
            context.schema_name,
            ACQUISITION,
            IDENTITY_CHECK,
        )
        assert "acquisition_id IS NOT NULL" in definition


@pytest.mark.asyncio
async def test_nonempty_owned_table_rejects_adoption_before_guards(monkeypatch) -> None:
    async with _adoption_scope(monkeypatch, "nonempty") as context:
        acquisition_id = await _insert_valid_acquisition(context)

        with pytest.raises(DBAPIError, match="rooted_graph_adoption_nonempty"):
            await run_migration(context.engine, context.migration, "upgrade")

        assert (
            await context.connection.fetchval(
                f"SELECT count(*) FROM {context.schema}.{ACQUISITION} "
                "WHERE acquisition_id = $1",
                acquisition_id,
            )
            == 1
        )
        assert (
            await context.connection.fetchval(
                """
            SELECT count(*)
              FROM pg_catalog.pg_trigger AS trigger_row
              JOIN pg_catalog.pg_class AS relation
                ON relation.oid = trigger_row.tgrelid
              JOIN pg_catalog.pg_namespace AS namespace
                ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = $1
               AND relation.relname = $2
               AND trigger_row.tgname = $3
            """,
                context.schema_name,
                ACQUISITION,
                f"{ACQUISITION}_row_guard",
            )
            == 0
        )
