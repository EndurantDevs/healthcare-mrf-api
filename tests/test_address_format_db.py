# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL parity proof for deterministic formatted-address rendering."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import importlib.util
import os
from pathlib import Path
import uuid

import pytest

from db.connection import Database
from process.ext.address_format import render_formatted_address_v2


address_formatted_address = importlib.import_module(
    "process.address_formatted_address"
)


ROOT = Path(__file__).resolve().parents[1]
FOUNDATION_MIGRATION_PATH = (
    ROOT / "alembic/versions/20260611100000_address_canonical_foundation.py"
)
MIGRATION_PATH = (
    ROOT / "alembic/versions/20260815010000_address_formatted_display_v2.py"
)


def _load_migration(
    path: Path = MIGRATION_PATH,
    name: str = "address_formatted_display_db_migration",
):
    module_spec = importlib.util.spec_from_file_location(
        name,
        path,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


@asynccontextmanager
async def _formatted_renderer_schema() -> AsyncIterator[tuple[Database, str]]:
    if "test" not in os.getenv("HLTHPRT_DB_DATABASE", "").lower():
        pytest.skip("formatted-address parity requires a disposable test database")
    database = Database()
    await database.connect()
    schema_name = f"address_format_{uuid.uuid4().hex[:12]}"
    await database.status(f'CREATE SCHEMA "{schema_name}";')
    try:
        foundation = _load_migration(
            FOUNDATION_MIGRATION_PATH,
            "address_formatted_display_db_foundation",
        )
        migration = _load_migration()
        assert database.engine is not None
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync_connection: foundation._exec_sql_batch(
                    sync_connection,
                    foundation._create_functions_sql(schema_name),
                )
            )
        await database.status(
            migration._humanize_component_function_sql(schema_name)
        )
        await database.status(
            migration._formatted_address_function_sql(schema_name)
        )
        yield database, schema_name
    finally:
        await database.status(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE;')
        await database.disconnect()


ADDRESS_ROWS = (
        (" 100\u00a0Main\tStreet ", " Suite   200 ", " Springfield ", " IL ", "627041234", " us "),
        ("100 Main St, Suite 200", "Suite 200", "Springfield", "IL", "62704", "US"),
        ("100 Main St, Suite 200", "Suite 201", "Springfield", "IL", "62704", "US"),
        ("3800 S WHITNEY AVE", "SUITE 301", "INDEPENDENCE", "MO", "64055", None),
        ("4007 Clarksville Pike Suite 301", "Ste 301", "NASHVILLE", "TN", "37218", "US"),
        ("123 NE 1ST ST NW", None, "O'FALLON", "MO", "63366", "USA"),
        ("P.O. BOX 42", None, "ST LOUIS", "MO", "63101", "U.S."),
        ("1 US HWY 101", None, "SAN JOSÉ", "CA", "95112", "United States"),
        ("SUITE 301", None, "NASHVILLE", "TN", "37218", "US"),
        ("123 UNIT RD", None, "CITY", "MO", "64055", "US"),
        ("1 BUILDING WAY", None, "CITY", "MO", "64055", "US"),
        ("123 STE GENEVIEVE DR", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST 2ND FLOOR", "FLOOR 2", "CITY", "MO", "64055", "US"),
        ("100 MAIN ST floor A", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST STE. 301", "STE 301", "CITY", "MO", "64055", "US"),
        ("100 MAIN ST STE#301", "SUITE #301", "CITY", "MO", "64055", "US"),
        ("100 MAIN ST APT 2ND", None, "EXAMPLE", "NY", "10001", "US"),
        ("100 MAIN ST STE MCDONALD", None, "EXAMPLE", "NY", "10001", "US"),
        ("1110 CALLE FLAMBOYAN", None, "SAN JUAN", "PR", "00901", "US"),
        ("123 STEWART", None, "CITY", "MO", "64055", "US"),
        ("STEWART A", None, "CITY", "MO", "64055", "US"),
        ("COMMANDING OFFICER", None, "CITY", "MO", "64055", "US"),
        ("123 OCEAN FRONT", None, "CITY", "MO", "64055", "US"),
        ("GME OFFICE", None, "CITY", "MO", "64055", "US"),
        ("MEDICAL OFFICE BUILDING", None, "CITY", "MO", "64055", "US"),
        ("UNIVERSITY DEPARTMENT PEDIATRICS", None, "CITY", "MO", "64055", "US"),
        ("EMERGENCY ROOM PHYSICIANS", None, "CITY", "MO", "64055", "US"),
        ("POST OFFICE BOX", None, "CITY", "MO", "64055", "US"),
        ("ST OFFICE", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST SUITE E", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST STE301", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST #LA", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST BLDG A STE 2", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST APT 2 OFFICE", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST", "BLDG A STE 2", "CITY", "MO", "64055", "US"),
        ("100 MAIN ST STE-301", "STE 301", "CITY", "MO", "64055", "US"),
        ("100 MAIN ST STE/301-A", None, "CITY", "MO", "64055", "US"),
        ("100 S. MAIN ST", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST N.", None, "CITY", "MO", "64055", "US"),
        ("100 N.E. MAIN ST", None, "CITY", "MO", "64055", "US"),
        ("100 N E MAIN ST", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST N.W.", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST", None, "FT. WORTH,", "TX,", "76102,", "US,"),
        ("100 S MAIN ST.", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST,", None, "CITY", "MO", "64055", "US"),
        ("POST OFFICE BOX. 42", None, "CITY", "MO", "64055", "US"),
        ("P.O. BOX#42", None, "CITY", "MO", "64055", "US"),
        ("P.O. BOX #42", None, "CITY", "MO", "64055", "US"),
        ("RR 2 BOX #42", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST OFC", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST OFFICE", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST OFFICE 200", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST N, OFFICE", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST DEPT 2", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST SPC 4", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST PH", None, "CITY", "MO", "64055", "US"),
        ("1 BUILDING-WAY", None, "CITY", "MO", "64055", "US"),
        ("100 BUILDING-WAY STE-2", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST STE A B", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST STE A-B", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST STE AB-301", None, "CITY", "MO", "64055", "US"),
        ("100 MAIN ST STE ABC-1D", None, "CITY", "MO", "64055", "US"),
        (
            "100 MAIN ST STE 1",
            "100 Main Street Suite 1",
            "CITY",
            "MO",
            "64055",
            "US",
        ),
        ("100 MAIN ST STE 301", "301", "CITY", "MO", "64055", "US"),
        ("1 ST", None, "北京", None, "100000", "中国"),
        ("STRASSE SUITE 1", "Straße Suite 1", "BERLIN", None, "10115", "DE"),
        ("STRASSE STE-301", None, "BERLIN", None, "10115", "DE"),
        ("İSTANBUL SUITE 1", "SUITE 1", "BERLIN", None, "10115", "DE"),
        ("Cafe\u0301 Road", None, "Montre\u0301al", "QC", "H2Y 1C6", "CA"),
        (None, None, "Example City", None, "12345-6789", None),
        (None, None, None, None, None, "GB"),
        ("\u00e9" * 1025, None, None, None, None, None),
)


@pytest.mark.asyncio
async def test_sql_renderer_matches_python_for_boundary_corpus() -> None:
    """Keep the installed SQL renderer byte-equal to Python."""
    async with _formatted_renderer_schema() as (database, schema_name):
        for address_row in ADDRESS_ROWS:
            sql_record = await database.first(
                f'SELECT "{schema_name}".addr_formatted_address_v2('
                ":first_line, :second_line, :city_name, :state_name, "
                ":postal_code, :country_code) AS rendered;",
                **dict(
                    zip(
                        (
                            "first_line",
                            "second_line",
                            "city_name",
                            "state_name",
                            "postal_code",
                            "country_code",
                        ),
                        address_row,
                        strict=True,
                    )
                ),
            )
            assert sql_record.rendered == render_formatted_address_v2(*address_row)


async def _seed_archive_display_rows(database: Database, schema_name: str) -> None:
    """Create three ordered archive rows for keyset refresh proof."""
    await database.status(
        f"""
        CREATE TABLE "{schema_name}".address_archive_v2 (
            address_key uuid PRIMARY KEY,
            first_line varchar,
            second_line varchar,
            city_name varchar,
            state_name varchar,
            postal_code varchar,
            country_code varchar,
            formatted_address varchar,
            formatted_address_version smallint,
            formatted_address_source varchar(32),
            merged_into uuid
        );
        """
    )
    await database.status(
        f"""
        INSERT INTO "{schema_name}".address_archive_v2 (
            address_key, first_line, city_name, state_name, postal_code,
            country_code, formatted_address, formatted_address_version,
            formatted_address_source
        ) VALUES
            ('00000000-0000-0000-0000-000000000001', '1 Main St',
             'Example', 'NY', '10001', 'US', 'external unit 101', 1,
             'canonical_v1'),
            ('00000000-0000-0000-0000-000000000002', '2 Main St',
             'Example', 'NY', '10001', 'US', NULL, NULL, NULL),
            ('00000000-0000-0000-0000-000000000003', '3 Main St',
             'Example', 'NY', '10001', 'US', NULL, NULL, NULL);
        """
    )


@pytest.mark.asyncio
async def test_archive_refresh_uses_uuid_keyset_and_is_idempotent(monkeypatch) -> None:
    """Prove UUID keyset refresh terminates and a second run changes nothing."""
    async with _formatted_renderer_schema() as (database, schema_name):
        await _seed_archive_display_rows(database, schema_name)
        monkeypatch.setattr(address_formatted_address, "db", database)

        first_refresh = (
            await address_formatted_address.refresh_address_archive_formatted_addresses(
                schema=schema_name,
                batch_size=2,
            )
        )
        second_refresh = (
            await address_formatted_address.refresh_address_archive_formatted_addresses(
                schema=schema_name,
                batch_size=2,
            )
        )
        archive_rows = await database.all(
            f"""
            SELECT formatted_address,
                   formatted_address_version,
                   formatted_address_source
            FROM "{schema_name}".address_archive_v2
            ORDER BY address_key;
            """
        )

        assert first_refresh.scanned == 3
        assert first_refresh.updated == 3
        assert first_refresh.batches == 2
        assert second_refresh.scanned == 3
        assert second_refresh.updated == 0
        assert second_refresh.batches == 2
        assert [archive_row.formatted_address for archive_row in archive_rows] == [
            "1 Main Street, Example, NY 10001",
            "2 Main Street, Example, NY 10001",
            "3 Main Street, Example, NY 10001",
        ]
        assert {
            (
                archive_row.formatted_address_version,
                archive_row.formatted_address_source,
            )
            for archive_row in archive_rows
        } == {(2, "canonical_v2")}
