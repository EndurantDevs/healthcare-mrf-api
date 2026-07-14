# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
import importlib
import os
import uuid

import pytest

from db.connection import Database


entity_address_unified = importlib.import_module("process.entity_address_unified")

_TABLE_DEFINITION_SQL = (
    """
    CREATE TABLE {schema_name}.address_archive_v2 (
        address_key uuid PRIMARY KEY,
        identity_version smallint NOT NULL,
        precision text NOT NULL,
        line1_norm text,
        city_norm text,
        state_code text,
        zip5 text,
        country_code text NOT NULL,
        lat numeric,
        long numeric,
        merged_into uuid,
        provenance text NOT NULL
    );
    """,
    """
    CREATE TABLE {schema_name}.entity_address_unified_stage (
        case_name text PRIMARY KEY,
        location_key text NOT NULL UNIQUE,
        address_key uuid,
        address_precision text NOT NULL,
        archive_identity_version text NOT NULL,
        identity_marker text NOT NULL,
        lat numeric,
        long numeric
    );
    """,
    """
    CREATE TABLE {schema_name}.entity_address_unified (
        case_name text PRIMARY KEY,
        address_key uuid,
        lat numeric,
        long numeric
    );
    """,
)

_ARCHIVE_FIXTURES_SQL = """
INSERT INTO {schema_name}.address_archive_v2
    (address_key, identity_version, precision, line1_norm, city_norm,
     state_code, zip5, country_code, lat, long, merged_into, provenance)
VALUES
    ('a95430ba-8cbe-777f-c710-08e45e130b4e', 2, 'street', '5501smccollrd', 'edinburg', 'TX', '78539', 'US', NULL, NULL, NULL, 'current-v2'),
    ('d557f5ba-f302-66ab-3740-a8de5dd337e8', 1, 'street', '5501smccollrd', 'edinburg', 'TX', '78539', 'US', 26.252675, -98.205322, NULL, 'legacy-v1'),
    ('00000000-0000-0000-0000-000000000100', 2, 'street', '1mainst', 'austin', 'TX', '78701', 'US', 30.100000, -97.100000, NULL, 'current-valid'),
    ('00000000-0000-0000-0000-000000000101', 1, 'street', '1mainst', 'austin', 'TX', '78701', 'US', 31.100000, -96.100000, NULL, 'legacy-not-used'),
    ('00000000-0000-0000-0000-000000000200', 2, 'street', '2mainst', 'dallas', 'TX', '75201', 'US', NULL, NULL, NULL, 'duplicate-current'),
    ('00000000-0000-0000-0000-000000000201', 1, 'street', '2mainst', 'dallas', 'TX', '75201', 'US', 32.100000, -96.800000, NULL, 'duplicate-a'),
    ('00000000-0000-0000-0000-000000000202', 1, 'street', '2mainst', 'dallas', 'TX', '75201', 'US', 32.200000, -96.900000, NULL, 'duplicate-b'),
    ('00000000-0000-0000-0000-000000000300', 2, 'street', '3mainst', 'edinburg', 'TX', '78539', 'US', NULL, NULL, NULL, 'city-current'),
    ('00000000-0000-0000-0000-000000000301', 1, 'street', '3mainst', 'mcallen', 'TX', '78539', 'US', 26.300000, -98.200000, NULL, 'city-mismatch'),
    ('00000000-0000-0000-0000-000000000400', 2, 'street', '4mainst', 'edinburg', 'TX', '78539', 'US', NULL, NULL, NULL, 'zip-current'),
    ('00000000-0000-0000-0000-000000000401', 1, 'street', '4mainst', 'edinburg', 'TX', '78540', 'US', 26.400000, -98.200000, NULL, 'zip-mismatch'),
    ('00000000-0000-0000-0000-000000000500', 2, 'street', '5mainst', 'edinburg', 'TX', '78539', 'US', NULL, NULL, NULL, 'state-current'),
    ('00000000-0000-0000-0000-000000000501', 1, 'street', '5mainst', 'edinburg', 'NM', '78539', 'US', 26.500000, -98.200000, NULL, 'state-mismatch'),
    ('00000000-0000-0000-0000-000000000600', 2, 'street', '6mainst', 'toronto', 'ON', '12345', 'CA', NULL, NULL, NULL, 'non-us-current'),
    ('00000000-0000-0000-0000-000000000601', 1, 'street', '6mainst', 'toronto', 'ON', '12345', 'CA', 43.600000, -79.300000, NULL, 'non-us-legacy'),
    ('00000000-0000-0000-0000-000000000700', 2, 'city_zip', '7mainst', 'edinburg', 'TX', '78539', 'US', NULL, NULL, NULL, 'non-street-current'),
    ('00000000-0000-0000-0000-000000000701', 1, 'city_zip', '7mainst', 'edinburg', 'TX', '78539', 'US', 26.700000, -98.200000, NULL, 'non-street-legacy'),
    ('00000000-0000-0000-0000-000000000800', 2, 'street', '8mainst', 'edinburg', 'TX', '78539', 'US', NULL, NULL, NULL, 'fuzzy-current'),
    ('00000000-0000-0000-0000-000000000801', 1, 'street', '8mainstreet', 'edinburg', 'TX', '78539', 'US', 26.800000, -98.200000, NULL, 'fuzzy-only');
"""

_STAGE_FIXTURES_SQL = """
INSERT INTO {schema_name}.entity_address_unified_stage
    (case_name, location_key, address_key, address_precision, archive_identity_version,
     identity_marker, lat, long)
VALUES
    ('supplied', 'replacement-new', 'a95430ba-8cbe-777f-c710-08e45e130b4e', 'street', 'v2', 'keep-supplied', NULL, NULL),
    ('current-wins', 'affected-existing', '00000000-0000-0000-0000-000000000100', 'street', 'v2', 'keep-current', NULL, NULL),
    ('duplicate', 'duplicate', '00000000-0000-0000-0000-000000000200', 'street', 'v2', 'keep-duplicate', NULL, NULL),
    ('city-mismatch', 'city-mismatch', '00000000-0000-0000-0000-000000000300', 'street', 'v2', 'keep-city', NULL, NULL),
    ('zip-mismatch', 'zip-mismatch', '00000000-0000-0000-0000-000000000400', 'street', 'v2', 'keep-zip', NULL, NULL),
    ('state-mismatch', 'state-mismatch', '00000000-0000-0000-0000-000000000500', 'street', 'v2', 'keep-state', NULL, NULL),
    ('non-us', 'non-us', '00000000-0000-0000-0000-000000000600', 'street', 'v2', 'keep-country', NULL, NULL),
    ('non-street', 'non-street', '00000000-0000-0000-0000-000000000700', 'city_zip', 'v2', 'keep-precision', NULL, NULL),
    ('fuzzy-only', 'fuzzy-only', '00000000-0000-0000-0000-000000000800', 'street', 'v2', 'keep-fuzzy', NULL, NULL);
"""

_LIVE_FIXTURE_SQL = """
INSERT INTO {schema_name}.entity_address_unified
    (case_name, address_key, lat, long)
VALUES ('live-supplied', 'a95430ba-8cbe-777f-c710-08e45e130b4e', NULL, NULL);
"""

_SAME_PROVIDER_FIXTURES_SQL = (
    """
    CREATE TABLE {schema_name}.entity_address_unified_replacement (
        location_key text PRIMARY KEY,
        address_source text NOT NULL,
        entity_type text NOT NULL,
        entity_id text NOT NULL,
        npi bigint,
        inferred_npi bigint,
        address_key uuid,
        telephone_number text,
        phone_number text,
        phone_extension text,
        fax_number text,
        fax_number_digits text,
        fax_extension text,
        lat numeric,
        long numeric,
        source_count integer NOT NULL,
        updated_at timestamptz
    );
    """,
    """
    INSERT INTO {schema_name}.entity_address_unified_replacement
        (location_key, address_source, entity_type, entity_id, npi,
         address_key, telephone_number, phone_number, lat, long,
         source_count, updated_at)
    VALUES
        ('fhir-affected', 'provider_directory_fhir', 'npi', '1234567890',
         1234567890, '00000000-0000-0000-0000-000000000900',
         NULL, NULL, NULL, NULL, 1, '2026-07-14T12:00:00Z'),
        ('nppes-unchanged', 'npi', 'npi', '1234567890', 1234567890,
         '00000000-0000-0000-0000-000000000900', '+1 512-555-0199',
         '5125550199', 30.2672, -97.7431, 2, '2026-07-13T12:00:00Z'),
        ('fhir-unaffected', 'provider_directory_fhir', 'npi', '1234567890',
         1234567890, '00000000-0000-0000-0000-000000000900',
         NULL, NULL, NULL, NULL, 1, '2026-07-12T12:00:00Z');
    """,
    """
    CREATE TABLE {schema_name}.provider_directory_coordinate_scope (
        location_key text PRIMARY KEY
    );
    """,
    """
    INSERT INTO {schema_name}.provider_directory_coordinate_scope (location_key)
    VALUES ('fhir-affected');
    """,
)


def _require_disposable_postgres() -> None:
    if "test" not in os.getenv("HLTHPRT_DB_DATABASE", "").lower():
        pytest.skip("EAU archive-coordinate DB test requires a disposable test database")


@asynccontextmanager
async def _temporary_schema() -> AsyncIterator[tuple[Database, str]]:
    _require_disposable_postgres()
    database = Database()
    await database.connect()
    schema_name = f"eau_geo_{uuid.uuid4().hex[:12]}"
    await database.status(f"CREATE SCHEMA {schema_name};")
    try:
        yield database, schema_name
    finally:
        await database.status(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
        await database.disconnect()


async def _prepare_coordinate_fixtures(database: Database, schema_name: str) -> None:
    for table_definition_sql in _TABLE_DEFINITION_SQL:
        await database.status(table_definition_sql.format(schema_name=schema_name))
    await database.status(_ARCHIVE_FIXTURES_SQL.format(schema_name=schema_name))
    await database.status(_STAGE_FIXTURES_SQL.format(schema_name=schema_name))
    await database.status(_LIVE_FIXTURE_SQL.format(schema_name=schema_name))


async def _prepare_same_provider_fixtures(database: Database, schema_name: str) -> None:
    for fixture_sql in _SAME_PROVIDER_FIXTURES_SQL:
        await database.status(fixture_sql.format(schema_name=schema_name))


async def _archive_snapshot(database: Database, schema_name: str) -> list[tuple]:
    archive_records = await database.all(
        f"SELECT * FROM {schema_name}.address_archive_v2 ORDER BY address_key;"
    )
    return [tuple(archive_record) for archive_record in archive_records]


async def _run_coordinate_backfills(
    database: Database,
    schema_name: str,
) -> tuple[int, dict[str, int]]:
    same_key_rows = await database.status(
        entity_address_unified._backfill_archive_coordinates_sql(
            schema_name,
            "entity_address_unified_stage",
        )
    )
    metric_records = await database.all(
        entity_address_unified._inherit_archive_coordinates_sql(
            schema_name,
            "entity_address_unified_stage",
        )
    )
    metrics_by_name = dict(metric_records[0]._mapping)
    return int(same_key_rows or 0), metrics_by_name


async def _stage_rows_by_case(database: Database, schema_name: str) -> dict[str, tuple]:
    stage_records = await database.all(
        f"""
        SELECT case_name, address_key::text, archive_identity_version,
               identity_marker, lat, long
          FROM {schema_name}.entity_address_unified_stage
      ORDER BY case_name;
        """
    )
    return {stage_record[0]: tuple(stage_record[1:]) for stage_record in stage_records}


def _assert_stage_coordinate_outcomes(stage_by_case: dict[str, tuple]) -> None:
    assert tuple(map(float, stage_by_case["supplied"][-2:])) == (26.252675, -98.205322)
    assert tuple(map(float, stage_by_case["current-wins"][-2:])) == (30.1, -97.1)
    rejected_cases = (
        "duplicate",
        "city-mismatch",
        "zip-mismatch",
        "state-mismatch",
        "non-us",
        "non-street",
        "fuzzy-only",
    )
    for case_name in rejected_cases:
        assert stage_by_case[case_name][-2:] == (None, None)
    assert stage_by_case["supplied"][:3] == (
        "a95430ba-8cbe-777f-c710-08e45e130b4e",
        "v2",
        "keep-supplied",
    )


async def _assert_live_table_unchanged(database: Database, schema_name: str) -> None:
    live_records = await database.all(
        f"SELECT case_name, address_key::text, lat, long FROM {schema_name}.entity_address_unified;"
    )
    assert [tuple(live_record) for live_record in live_records] == [
        (
            "live-supplied",
            "a95430ba-8cbe-777f-c710-08e45e130b4e",
            None,
            None,
        )
    ]


@pytest.mark.asyncio
async def test_archive_coordinate_backfill_inherits_only_one_exact_legacy_identity():
    """Only an exact, unique older identity may enrich the private stage."""
    async with _temporary_schema() as (database, schema_name):
        await _prepare_coordinate_fixtures(database, schema_name)
        archive_before = await _archive_snapshot(database, schema_name)
        same_key_rows, metrics_by_name = await _run_coordinate_backfills(
            database,
            schema_name,
        )
        assert same_key_rows == 1
        assert metrics_by_name == {"inherited_rows": 1, "ambiguous_rows": 1}
        stage_by_case = await _stage_rows_by_case(database, schema_name)
        _assert_stage_coordinate_outcomes(stage_by_case)
        assert await _archive_snapshot(database, schema_name) == archive_before
        await _assert_live_table_unchanged(database, schema_name)


@pytest.mark.asyncio
async def test_partial_archive_coordinate_backfill_excludes_unchanged_live_groups():
    """Provider Directory replacement scope includes affected and new locations only."""
    async with _temporary_schema() as (database, schema_name):
        await _prepare_coordinate_fixtures(database, schema_name)
        await database.status(
            f"""
            INSERT INTO {schema_name}.entity_address_unified_stage
                (case_name, location_key, address_key, address_precision,
                 archive_identity_version, identity_marker, lat, long)
            VALUES
                ('unchanged-current', 'unchanged-existing',
                 '00000000-0000-0000-0000-000000000100', 'street', 'v2',
                 'keep-unchanged', NULL, NULL);
            """
        )
        await database.status(
            f"""
            CREATE UNLOGGED TABLE {schema_name}.provider_directory_coordinate_scope (
                location_key text PRIMARY KEY
            );
            """
        )
        await database.status(
            f"""
            INSERT INTO {schema_name}.provider_directory_coordinate_scope (location_key)
            VALUES ('affected-existing'), ('replacement-new');
            """
        )
        archive_before = await _archive_snapshot(database, schema_name)

        same_key_rows = await database.status(
            entity_address_unified._backfill_archive_coordinates_sql(
                schema_name,
                "entity_address_unified_stage",
                coordinate_scope_table="provider_directory_coordinate_scope",
            )
        )
        metric_records = await database.all(
            entity_address_unified._inherit_archive_coordinates_sql(
                schema_name,
                "entity_address_unified_stage",
                coordinate_scope_table="provider_directory_coordinate_scope",
            )
        )
        metrics_by_name = dict(metric_records[0]._mapping)
        stage_by_case = await _stage_rows_by_case(database, schema_name)

        assert same_key_rows == 1
        assert metrics_by_name == {"inherited_rows": 1, "ambiguous_rows": 0}
        assert tuple(map(float, stage_by_case["current-wins"][-2:])) == (30.1, -97.1)
        assert tuple(map(float, stage_by_case["supplied"][-2:])) == (26.252675, -98.205322)
        assert stage_by_case["unchanged-current"][-2:] == (None, None)
        assert await _archive_snapshot(database, schema_name) == archive_before
        await _assert_live_table_unchanged(database, schema_name)


@pytest.mark.asyncio
async def test_partial_same_provider_backfill_uses_unaffected_cross_source_donor():
    """An affected FHIR target may inherit from an unchanged non-FHIR stage row."""
    async with _temporary_schema() as (database, schema_name):
        await _prepare_same_provider_fixtures(database, schema_name)

        updated_rows = await database.status(
            entity_address_unified._backfill_same_provider_address_fields_sql(
                schema_name,
                "entity_address_unified_replacement",
                coordinate_scope_table="provider_directory_coordinate_scope",
            )
        )
        stage_records = await database.all(
            f"""
            SELECT location_key, telephone_number, phone_number, lat, long
              FROM {schema_name}.entity_address_unified_replacement
          ORDER BY location_key;
            """
        )
        rows_by_location = {
            stage_record[0]: tuple(stage_record[1:]) for stage_record in stage_records
        }

        assert int(updated_rows or 0) == 1
        assert rows_by_location["fhir-affected"][:2] == (
            "+1 512-555-0199",
            "5125550199",
        )
        assert tuple(map(float, rows_by_location["fhir-affected"][-2:])) == (
            30.2672,
            -97.7431,
        )
        assert rows_by_location["nppes-unchanged"][:2] == (
            "+1 512-555-0199",
            "5125550199",
        )
        assert rows_by_location["fhir-unaffected"] == (None, None, None, None)
