# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

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
FOUNDATION_MIGRATION_PATH = (
    ROOT / "alembic/versions/20260611100000_address_canonical_foundation.py"
)
V2_MIGRATION_PATH = (
    ROOT
    / "alembic/versions/20260815010000_address_formatted_display_v2.py"
)


def _load_migration(path: Path, name: str):
    module_spec = importlib.util.spec_from_file_location(
        name,
        path,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


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


async def _install_renderer_functions(database: Database, schema_name: str) -> None:
    foundation = _load_migration(
        FOUNDATION_MIGRATION_PATH,
        "address_formatted_serving_foundation",
    )
    migration = _load_migration(
        V2_MIGRATION_PATH,
        "address_formatted_serving_v2_migration",
    )
    assert database.engine is not None
    async with database.engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: foundation._exec_sql_batch(
                sync_connection,
                foundation._create_functions_sql(schema_name),
            )
        )
    await database.status(migration._humanize_component_function_sql(schema_name))
    await database.status(migration._formatted_address_function_sql(schema_name))


async def _seed_scoped_overlay(database: Database, schema_name: str) -> None:
    await database.status(
        f"""
            CREATE TABLE "{schema_name}".provider_directory_address_overlay (
                address_key uuid NOT NULL,
                source_id varchar NOT NULL,
                first_line varchar,
                second_line varchar,
                city_name varchar,
                state_name varchar,
                postal_code varchar,
                country_code varchar,
                formatted_address varchar,
                formatted_address_version smallint,
                formatted_address_source varchar(32)
            );
        """
    )
    await database.status(
        f"""
            INSERT INTO "{schema_name}".provider_directory_address_overlay (
                address_key, source_id, first_line, second_line, city_name,
                state_name, postal_code, country_code, formatted_address,
                formatted_address_version, formatted_address_source
            )
            VALUES (
                '00000000-0000-0000-0000-000000000123', 'selected-source',
                '4007 Clarksville Pike Suite 301',
                'Ste 301', 'NASHVILLE', 'TN', '37218', 'US',
                '4007 CLARKSVILLE PIKE, 101, NASHVILLE, TN 37218',
                NULL, NULL
            ), (
                '00000000-0000-0000-0000-000000000124', 'copied-source',
                '4007 Clarksville Pike Suite 301',
                'Ste 301', 'NASHVILLE', 'TN', '37218', 'US',
                'COPIED LABEL', NULL, NULL
            );
        """
    )


@pytest.mark.asyncio
async def test_overlay_stage_renders_its_own_components(monkeypatch) -> None:
    """Render only the refreshed source instead of rescanning copied rows."""
    async with _temporary_schema() as (database, schema_name):
        monkeypatch.setattr(directory, "db", database)
        await _install_renderer_functions(database, schema_name)
        await _seed_scoped_overlay(database, schema_name)

        changed_rows = await directory._backfill_address_overlay_stage_formatted_addresses(
            schema_name,
            f'"{schema_name}"."provider_directory_address_overlay"',
            source_ids=["selected-source"],
        )
        overlay_records = await database.all(
            f'SELECT * FROM "{schema_name}".provider_directory_address_overlay '
            "ORDER BY source_id DESC;"
        )

        assert changed_rows == 1
        selected_record, copied_record = overlay_records
        assert selected_record.formatted_address == (
            "4007 Clarksville Pike, Suite 301, Nashville, TN 37218"
        )
        assert selected_record.formatted_address_version == 2
        assert selected_record.formatted_address_source == "canonical_v2"
        assert copied_record.formatted_address == "COPIED LABEL"
        assert copied_record.formatted_address_version is None
        assert copied_record.formatted_address_source is None
