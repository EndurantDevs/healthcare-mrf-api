# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proof for persisted Provider Directory premise keys."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import importlib
import importlib.util
import os
from pathlib import Path
import uuid

import pytest

from db.connection import Database
directory = importlib.import_module("process.provider_directory_fhir")


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT / "alembic/versions/20260811130000_address_premise_grouping.py"
)
MATCHED_ADDRESS_KEY = "00000000-0000-0000-0000-000000000001"
NULL_PREMISE_ADDRESS_KEY = "00000000-0000-0000-0000-000000000002"
MERGED_ADDRESS_KEY = "00000000-0000-0000-0000-000000000003"
ARCHIVE_PREMISE_KEY = "10000000-0000-0000-0000-000000000001"
STALE_PREMISE_KEY = "20000000-0000-0000-0000-000000000001"
MERGED_PREMISE_KEY = "30000000-0000-0000-0000-000000000001"
MERGED_TARGET_KEY = "40000000-0000-0000-0000-000000000001"


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "address_premise_grouping_db_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


class _OperationsRecorder:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement) -> None:
        self.statements.append(str(statement))


def _migration_statements(operation: str, schema: str) -> list[str]:
    migration = _load_migration()
    recorder = _OperationsRecorder()
    migration.op = recorder
    migration._schema = lambda: schema
    getattr(migration, operation)()
    return recorder.statements


@asynccontextmanager
async def _premise_schema() -> AsyncIterator[tuple[Database, str]]:
    if "test" not in os.getenv("HLTHPRT_DB_DATABASE", "").lower():
        pytest.skip("overlay premise proof requires a disposable test database")
    database = Database()
    await database.connect()
    actual_database = str(await database.scalar("SELECT current_database();") or "")
    if "test" not in actual_database.lower():
        await database.disconnect()
        pytest.skip("overlay premise proof connected to a non-disposable database")
    schema = f"overlay_premise_{uuid.uuid4().hex[:12]}"
    await database.status(f'CREATE SCHEMA "{schema}";')
    try:
        yield database, schema
    finally:
        await database.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await database.disconnect()


async def _create_legacy_overlay(database: Database, schema: str) -> None:
    await database.status(
        f"""
        CREATE TABLE "{schema}".provider_directory_address_overlay (
            source_record_id varchar PRIMARY KEY,
            npi bigint NOT NULL,
            address_key uuid NOT NULL
        );
        """
    )


async def _apply_statements(
    database: Database,
    statements: list[str],
) -> None:
    for statement in statements:
        await database.status(statement)


async def _assert_upgrade_shape(database: Database, schema: str) -> None:
    column = await database.first(
        """
        SELECT udt_name, is_nullable
          FROM information_schema.columns
         WHERE table_schema = :schema
           AND table_name = 'provider_directory_address_overlay'
           AND column_name = 'premise_key';
        """,
        schema=schema,
    )
    index_definition = await database.scalar(
        """
        SELECT indexdef
          FROM pg_indexes
         WHERE schemaname = :schema
           AND indexname = 'provider_directory_address_overlay_npi_premise_key_idx';
        """,
        schema=schema,
    )
    assert column is not None
    assert (column.udt_name, column.is_nullable) == ("uuid", "YES")
    assert "(npi, premise_key)" in str(index_definition)
    assert "WHERE (premise_key IS NOT NULL)" in str(index_definition)


async def _seed_archive_and_overlay_rows(database: Database, schema: str) -> None:
    await database.status(
        f"""
        CREATE TABLE "{schema}".address_archive_v2 (
            address_key uuid PRIMARY KEY,
            premise_key uuid,
            merged_into uuid
        );
        """
    )
    await database.status(
        f"""
        INSERT INTO "{schema}".address_archive_v2
            (address_key, premise_key, merged_into)
        VALUES
            (CAST(:matched_address_key AS uuid), CAST(:archive_premise_key AS uuid), NULL),
            (CAST(:null_premise_address_key AS uuid), NULL, NULL),
            (CAST(:merged_address_key AS uuid), CAST(:merged_premise_key AS uuid), CAST(:merged_target_key AS uuid));
        """,
        matched_address_key=MATCHED_ADDRESS_KEY,
        null_premise_address_key=NULL_PREMISE_ADDRESS_KEY,
        merged_address_key=MERGED_ADDRESS_KEY,
        archive_premise_key=ARCHIVE_PREMISE_KEY,
        merged_premise_key=MERGED_PREMISE_KEY,
        merged_target_key=MERGED_TARGET_KEY,
    )
    await database.status(
        f"""
        INSERT INTO "{schema}".provider_directory_address_overlay
            (source_record_id, npi, address_key, premise_key)
        VALUES
            ('synthetic:matched', 1000000001, CAST(:matched_address_key AS uuid), CAST(:stale_premise_key AS uuid)),
            ('synthetic:null', 1000000001, CAST(:null_premise_address_key AS uuid), CAST(:stale_premise_key AS uuid)),
            ('synthetic:merged', 1000000001, CAST(:merged_address_key AS uuid), CAST(:stale_premise_key AS uuid));
        """,
        matched_address_key=MATCHED_ADDRESS_KEY,
        null_premise_address_key=NULL_PREMISE_ADDRESS_KEY,
        merged_address_key=MERGED_ADDRESS_KEY,
        stale_premise_key=STALE_PREMISE_KEY,
    )


async def _assert_archive_hydration(database: Database, schema: str, monkeypatch) -> None:
    monkeypatch.setattr(directory, "db", database)
    changed_rows = await directory._backfill_address_overlay_stage_premise_keys(
        schema,
        f'"{schema}"."provider_directory_address_overlay"',
    )
    premise_rows = await database.all(
        f"""
        SELECT source_record_id, premise_key::text AS premise_key
          FROM "{schema}".provider_directory_address_overlay
         ORDER BY source_record_id;
        """
    )
    assert changed_rows == 3
    assert [(row.source_record_id, row.premise_key) for row in premise_rows] == [
        ("synthetic:matched", ARCHIVE_PREMISE_KEY),
        ("synthetic:merged", None),
        ("synthetic:null", None),
    ]


async def _assert_downgrade(database: Database, schema: str) -> None:
    await _apply_statements(database, _migration_statements("downgrade", schema))
    column_count = await database.scalar(
        """
        SELECT count(*)
          FROM information_schema.columns
         WHERE table_schema = :schema
           AND table_name = 'provider_directory_address_overlay'
           AND column_name = 'premise_key';
        """,
        schema=schema,
    )
    index_relation = await database.scalar(
        "SELECT to_regclass(:index_name);",
        index_name=f'{schema}.provider_directory_address_overlay_npi_premise_key_idx',
    )
    assert column_count == 0
    assert index_relation is None


async def _seed_duplicate_source_rows(database: Database, schema: str) -> None:
    await database.status(
        f"""
        CREATE TABLE "{schema}".address_archive_v2 (
            address_key uuid PRIMARY KEY,
            premise_key uuid,
            merged_into uuid
        );
        """
    )
    await database.status(
        f"""
        CREATE TABLE "{schema}".overlay_stage (
            source_record_id varchar,
            address_key uuid NOT NULL,
            premise_key uuid
        );
        """
    )
    await database.status(
        f"""
        INSERT INTO "{schema}".address_archive_v2
            (address_key, premise_key, merged_into)
        VALUES
            (CAST(:first_key AS uuid), CAST(:first_premise AS uuid), NULL),
            (CAST(:second_key AS uuid), CAST(:second_premise AS uuid), NULL);
        """,
        first_key=MATCHED_ADDRESS_KEY,
        second_key=NULL_PREMISE_ADDRESS_KEY,
        first_premise=ARCHIVE_PREMISE_KEY,
        second_premise=MERGED_PREMISE_KEY,
    )
    await database.status(
        f"""
        INSERT INTO "{schema}".overlay_stage
            (source_record_id, address_key, premise_key)
        VALUES
            ('synthetic:duplicate', CAST(:first_key AS uuid), CAST(:stale AS uuid)),
            ('synthetic:duplicate', CAST(:second_key AS uuid), CAST(:stale AS uuid));
        """,
        first_key=MATCHED_ADDRESS_KEY,
        second_key=NULL_PREMISE_ADDRESS_KEY,
        stale=STALE_PREMISE_KEY,
    )


@pytest.mark.asyncio
async def test_premise_migration_and_archive_hydration_are_reversible(monkeypatch):
    """Prove idempotent upgrade, exact hydration, and reversible storage."""
    async with _premise_schema() as (database, schema):
        await _create_legacy_overlay(database, schema)
        upgrade_statements = _migration_statements("upgrade", schema)
        await _apply_statements(database, upgrade_statements)
        await _apply_statements(database, upgrade_statements)
        await _assert_upgrade_shape(database, schema)
        await _seed_archive_and_overlay_rows(database, schema)
        await _assert_archive_hydration(database, schema, monkeypatch)
        await _assert_downgrade(database, schema)


@pytest.mark.asyncio
async def test_premise_hydration_binds_duplicate_source_rows_by_stage_identity(
    monkeypatch,
):
    """Prove pre-dedupe duplicate source ids receive their own exact premise."""
    async with _premise_schema() as (database, schema):
        await _seed_duplicate_source_rows(database, schema)
        monkeypatch.setattr(directory, "db", database)

        changed_rows = await directory._backfill_address_overlay_stage_premise_keys(
            schema,
            f'"{schema}"."overlay_stage"',
        )
        rows = await database.all(
            f"""
            SELECT address_key::text AS address_key,
                   premise_key::text AS premise_key
              FROM "{schema}".overlay_stage
          ORDER BY address_key;
            """
        )

        assert changed_rows == 2
        assert [(row.address_key, row.premise_key) for row in rows] == [
            (MATCHED_ADDRESS_KEY, ARCHIVE_PREMISE_KEY),
            (NULL_PREMISE_ADDRESS_KEY, MERGED_PREMISE_KEY),
        ]
