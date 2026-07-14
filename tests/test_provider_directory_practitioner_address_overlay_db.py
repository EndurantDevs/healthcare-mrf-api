# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
import importlib
import importlib.util
import json
import os
from pathlib import Path
import uuid

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database

importer = importlib.import_module("process.provider_directory_fhir")


CANONICAL_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260611100000_address_canonical_foundation.py"
)


def _canonical_migration():
    module_name = f"address_canonical_foundation_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, CANONICAL_MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _require_disposable_database() -> None:
    database = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database.rsplit("/", 1)[-1].lower():
        pytest.skip(
            "Practitioner address overlay DB test requires HLTHPRT_DB_DATABASE "
            "to name a disposable test database"
        )


@asynccontextmanager
async def _temporary_schema(monkeypatch):
    _require_disposable_database()
    database = Database()
    schema = f"practitioner_address_overlay_{uuid.uuid4().hex[:12]}"
    schema_created = False
    try:
        try:
            await database.connect()
            actual_database = str(await database.scalar("SELECT current_database();") or "")
        except (OSError, OperationalError) as exc:
            pytest.skip(f"PostgreSQL is not available for practitioner address overlay test: {exc}")
        except Exception as exc:
            pytest.skip(f"PostgreSQL test connection is unavailable: {exc}")
        if "test" not in actual_database.lower():
            pytest.skip(
                "Practitioner address overlay DB test connected to a non-disposable database"
            )
        await database.status(f'CREATE SCHEMA "{schema}";')
        schema_created = True
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        yield database, schema
    finally:
        if schema_created:
            await database.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await database.disconnect()


async def _install_canonical_functions(database: Database, schema: str) -> None:
    migration = _canonical_migration()
    assert database.engine is not None
    async with database.engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: migration._exec_sql_batch(
                sync_connection,
                migration._create_functions_sql(schema),
            )
        )


async def _create_fixture_tables(database: Database, schema: str, stage_table: str) -> None:
    await database.status(
        f"""
        CREATE TABLE "{schema}"."provider_directory_practitioner" (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            npi bigint,
            active boolean,
            telecom jsonb,
            addresses jsonb,
            last_seen_run_id varchar(64),
            updated_at timestamp,
            PRIMARY KEY (source_id, resource_id)
        );
        """
    )
    await database.status(importer.provider_directory_address_overlay_table_sql(schema, stage_table))


async def _insert_practitioner_rows(database: Database, schema: str) -> None:
    """Insert practitioner rows used by the overlay fixture."""
    rows = [
        (
            "source-target",
            "practitioner-good",
            1234567890,
            True,
            "run-current",
            [
                {
                    "line": ["100 Main Street", "Suite 200"],
                    "city": "Austin",
                    "state": "Texas",
                    "postalCode": "78701",
                },
                {"line": ["Missing City"], "state": "TX", "postalCode": "78701"},
                {"city": "Austin", "state": "TX", "postalCode": "78701"},
                {"line": ["Missing ZIP"], "city": "Austin", "state": "TX"},
            ],
            [
                {"system": "phone", "value": "(312) 555-1212"},
                {"system": "fax", "value": "+1 (312) 555-0199"},
            ],
        ),
        (
            "source-target",
            "practitioner-old-run",
            1234567891,
            True,
            "run-old",
            [{"line": ["200 Old Run Road"], "city": "Austin", "state": "TX", "postalCode": "78702"}],
            [],
        ),
        (
            "source-other",
            "practitioner-other-source",
            1234567892,
            True,
            "run-current",
            [{"line": ["300 Other Source Road"], "city": "Austin", "state": "TX", "postalCode": "78703"}],
            [],
        ),
        (
            "source-target",
            "practitioner-invalid-npi",
            123456789,
            True,
            "run-current",
            [{"line": ["400 Invalid NPI Road"], "city": "Austin", "state": "TX", "postalCode": "78704"}],
            [],
        ),
        (
            "source-target",
            "practitioner-inactive",
            1234567893,
            False,
            "run-current",
            [{"line": ["500 Inactive Road"], "city": "Austin", "state": "TX", "postalCode": "78705"}],
            [],
        ),
    ]
    for source_id, resource_id, npi, active, run_id, addresses, telecom in rows:
        await database.status(
            f"""
            INSERT INTO "{schema}"."provider_directory_practitioner" (
                source_id, resource_id, npi, active, telecom, addresses,
                last_seen_run_id, updated_at
            ) VALUES (
                :source_id, :resource_id, :npi, :active,
                CAST(:telecom AS jsonb), CAST(:addresses AS jsonb),
                :run_id, TIMESTAMP '2026-07-14 12:00:00'
            );
            """,
            source_id=source_id,
            resource_id=resource_id,
            npi=npi,
            active=active,
            telecom=json.dumps(telecom),
            addresses=json.dumps(addresses),
            run_id=run_id,
        )


@pytest.mark.asyncio
async def test_practitioner_address_overlay_executes_scoped_sql_in_isolated_schema(
    monkeypatch,
):
    """Verify overlay SQL remains scoped to the isolated schema."""
    stage_table = "provider_directory_practitioner_address_stage"
    async with _temporary_schema(monkeypatch) as (database, schema):
        await _install_canonical_functions(database, schema)
        await _create_fixture_tables(database, schema, stage_table)
        await _insert_practitioner_rows(database, schema)

        assert await database.scalar(
            "SELECT to_regclass(:relation_name) IS NULL;",
            relation_name=f"{schema}.provider_directory_location",
        ) is True
        assert await database.scalar(
            "SELECT to_regclass(:relation_name) IS NULL;",
            relation_name=f"{schema}.provider_directory_practitioner_role",
        ) is True

        sql = importer._address_overlay_component_insert_sql(
            schema,
            stage_table,
            component="practitioner_address",
            run_id="run-current",
            source_ids=["source-target"],
        )
        inserted = await database.status(
            sql,
            run_id="run-current",
            source_ids=["source-target"],
        )

        assert inserted == 1
        row = await database.first(
            f"""
            SELECT source_record_id, source_id, last_seen_run_id, resource_type,
                   resource_id, npi, address_key::text, first_line, second_line,
                   city_name, state_name, state_code, postal_code, country_code,
                   telephone_number, fax_number, phone_number, fax_number_digits
              FROM "{schema}"."{stage_table}";
            """
        )
        assert row is not None
        values = row._mapping
        expected_address_key = await database.scalar(
            f"""
            SELECT "{schema}".addr_key_v1(
                '100 Main Street', 'Suite 200', 'Austin', 'Texas', '78701', 'US'
            )::text;
            """
        )
        assert values["source_record_id"] == (
            "provider_directory_fhir:practitioner_address:"
            "source-target:practitioner-good:1"
        )
        assert values["source_id"] == "source-target"
        assert values["last_seen_run_id"] == "run-current"
        assert values["resource_type"] == "Practitioner"
        assert values["resource_id"] == "practitioner-good"
        assert values["npi"] == 1234567890
        assert values["address_key"] == expected_address_key
        assert values["state_code"] == "TX"
        assert values["country_code"] == "US"
        assert values["telephone_number"] == "(312) 555-1212"
        assert values["fax_number"] == "+1 (312) 555-0199"
        assert values["phone_number"] == "3125551212"
        assert values["fax_number_digits"] == "3125550199"
