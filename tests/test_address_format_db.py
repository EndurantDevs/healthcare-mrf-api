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
from process.ext.address_format import render_formatted_address_v1


address_formatted_address = importlib.import_module(
    "process.address_formatted_address"
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic/versions/20260811110000_address_formatted_display.py"


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "address_formatted_display_db_migration",
        MIGRATION_PATH,
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
        migration = _load_migration()
        await database.status(migration._formatted_address_function_sql(schema_name))
        yield database, schema_name
    finally:
        await database.status(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE;')
        await database.disconnect()


@pytest.mark.asyncio
async def test_sql_renderer_matches_python_for_boundary_corpus() -> None:
    address_rows = (
        (" 100\u00a0Main\tStreet ", " Suite   200 ", " Springfield ", " IL ", "627041234", " us "),
        ("100 Main St, Suite 200", "Suite 200", "Springfield", "IL", "62704", "US"),
        ("100 Main St, Suite 200", "Suite 201", "Springfield", "IL", "62704", "US"),
        ("Cafe\u0301 Road", None, "Montre\u0301al", "QC", "H2Y 1C6", "CA"),
        (None, None, "Example City", None, "12345-6789", None),
        (None, None, None, None, None, "GB"),
        ("\u00e9" * 1025, None, None, None, None, None),
    )
    async with _formatted_renderer_schema() as (database, schema_name):
        for address_row in address_rows:
            sql_record = await database.first(
                f'SELECT "{schema_name}".addr_formatted_address_v1('
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
            assert sql_record.rendered == render_formatted_address_v1(*address_row)


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
            country_code
        ) VALUES
            ('00000000-0000-0000-0000-000000000001', '1 Main St',
             'Example', 'NY', '10001', 'US'),
            ('00000000-0000-0000-0000-000000000002', '2 Main St',
             'Example', 'NY', '10001', 'US'),
            ('00000000-0000-0000-0000-000000000003', '3 Main St',
             'Example', 'NY', '10001', 'US');
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
            "1 Main St, Example, NY 10001",
            "2 Main St, Example, NY 10001",
            "3 Main St, Example, NY 10001",
        ]
        assert {
            (
                archive_row.formatted_address_version,
                archive_row.formatted_address_source,
            )
            for archive_row in archive_rows
        } == {(1, "canonical_v1")}
