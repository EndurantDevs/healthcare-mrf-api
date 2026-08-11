# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import importlib
import os
import uuid

import pytest

from db.connection import Database


directory = importlib.import_module("process.provider_directory_fhir")
unified = importlib.import_module("process.entity_address_unified")


def _require_disposable_postgres() -> None:
    if "test" not in os.getenv("HLTHPRT_DB_DATABASE", "").lower():
        pytest.skip("formatted-address serving tests require a disposable test database")


@asynccontextmanager
async def _temporary_schema() -> AsyncIterator[tuple[Database, str]]:
    _require_disposable_postgres()
    database = Database()
    await database.connect()
    schema_name = f"address_display_{uuid.uuid4().hex[:12]}"
    await database.status(f'CREATE SCHEMA "{schema_name}";')
    try:
        yield database, schema_name
    finally:
        await database.status(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE;')
        await database.disconnect()


@pytest.mark.asyncio
async def test_overlay_stage_hydration_copies_one_archive_label(monkeypatch):
    address_key = "00000000-0000-0000-0000-000000000123"
    async with _temporary_schema() as (database, schema_name):
        monkeypatch.setattr(directory, "db", database)
        await database.status(
            f"""
            CREATE TABLE "{schema_name}".address_archive_v2 (
                address_key uuid PRIMARY KEY,
                formatted_address varchar,
                formatted_address_version smallint,
                formatted_address_source varchar(32),
                merged_into uuid
            );
            """
        )
        await database.status(
            f"""
            CREATE TABLE "{schema_name}".provider_directory_address_overlay (
                address_key uuid NOT NULL,
                formatted_address varchar,
                formatted_address_version smallint,
                formatted_address_source varchar(32)
            );
            """
        )
        await database.status(
            f"""
            INSERT INTO "{schema_name}".address_archive_v2 VALUES
                ('{address_key}', '123 Main St, Example, NY 10001', 1, 'canonical_v1', NULL);
            """
        )
        await database.status(
            f"""
            INSERT INTO "{schema_name}".provider_directory_address_overlay VALUES
                ('{address_key}', 'stale label', NULL, NULL);
            """
        )

        changed_rows = await directory._backfill_address_overlay_stage_formatted_addresses(
            schema_name,
            f'"{schema_name}"."provider_directory_address_overlay"',
        )
        hydrated_overlay_record = await database.first(
            f'SELECT * FROM "{schema_name}".provider_directory_address_overlay;'
        )

        assert changed_rows == 1
        assert hydrated_overlay_record.formatted_address == "123 Main St, Example, NY 10001"
        assert hydrated_overlay_record.formatted_address_version == 1
        assert hydrated_overlay_record.formatted_address_source == "canonical_v1"


@pytest.mark.asyncio
async def test_archive_label_wins_as_one_coherent_aggregate():
    async with _temporary_schema() as (database, _schema_name):
        aggregate_record = await database.first(
            f"""
            WITH formatted_rows (
                formatted_address,
                formatted_address_version,
                formatted_address_source,
                source_priority,
                updated_at,
                source_record_id
            ) AS (
                VALUES
                    ('legacy label'::varchar, NULL::smallint, NULL::varchar,
                     1, '2026-08-11'::timestamp, 'legacy'::varchar),
                    ('canonical label'::varchar, 1::smallint, 'canonical_v1'::varchar,
                     9, '2026-01-01'::timestamp, 'archive'::varchar),
                    (NULL::varchar, NULL::smallint, NULL::varchar,
                     0, '2026-08-12'::timestamp, 'empty'::varchar)
            )
            SELECT
                {unified._formatted_address_aggregate("formatted_address")},
                {unified._formatted_address_aggregate("formatted_address_version", "smallint")},
                {unified._formatted_address_aggregate("formatted_address_source")}
              FROM formatted_rows;
            """
        )

        assert aggregate_record.formatted_address == "canonical label"
        assert aggregate_record.formatted_address_version == 1
        assert aggregate_record.formatted_address_source == "canonical_v1"
