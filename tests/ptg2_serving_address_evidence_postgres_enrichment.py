# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Executable provider-enrichment address selection regressions."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from db.connection import Database
from tests.ptg2_serving_address_evidence_postgres_support import (
    _schema_sql,
    _temporary_schema,
)


async def _prepare_enrichment_tables(database: Database, schema: str) -> None:
    await database.status(
        f"""ALTER TABLE {schema}.npi_address
            ADD COLUMN first_line varchar,
            ADD COLUMN second_line varchar,
            ADD COLUMN city_name varchar,
            ADD COLUMN state_name varchar,
            ADD COLUMN postal_code varchar,
            ADD COLUMN country_code varchar,
            ADD COLUMN telephone_number varchar,
            ADD COLUMN fax_number varchar,
            ADD COLUMN phone_number varchar,
            ADD COLUMN phone_extension varchar,
            ADD COLUMN fax_number_digits varchar,
            ADD COLUMN fax_extension varchar,
            ADD COLUMN lat numeric,
            ADD COLUMN long numeric"""
    )
    await database.status(
        f"""CREATE TABLE {schema}.npi (
            npi bigint PRIMARY KEY,
            provider_organization_name varchar,
            provider_first_name varchar,
            provider_middle_name varchar,
            provider_last_name varchar,
            provider_sex_code varchar
        )"""
    )
    await database.status(
        f"""CREATE TABLE {schema}.npi_taxonomy (
            npi bigint,
            healthcare_provider_taxonomy_code varchar,
            healthcare_provider_primary_taxonomy_switch varchar,
            checksum bigint
        )"""
    )
    await database.status(
        f"""CREATE TABLE {schema}.nucc_taxonomy (
            code varchar PRIMARY KEY,
            display_name varchar,
            classification varchar,
            specialization varchar
        )"""
    )


async def _insert_enrichment_provider_rows(
    database: Database,
    schema: str,
) -> None:
    """Insert provider identities used by address-selection cases."""

    await database.status(
        f"""INSERT INTO {schema}.npi (npi, provider_organization_name)
        VALUES
            (1990000304, 'Synthetic Provider 304'),
            (1990000312, 'Synthetic Provider 312'),
            (1990000320, 'Synthetic Provider 320'),
            (1990000338, 'Synthetic Provider 338')"""
    )


async def _insert_enrichment_unified_rows(
    database: Database,
    schema: str,
) -> None:
    """Insert physical, postal, and blank unified candidates."""

    await database.status(
        f"""INSERT INTO {schema}.entity_address_unified (
            location_key, npi, address_key, premise_key,
            type, checksum, first_line, second_line, city_name, state_name,
            postal_code, country_code, telephone_number, lat, long
        ) VALUES
            ('physical-304', 1990000304,
             '00000000-0000-0000-0000-000000000401',
             '10000000-0000-0000-0000-000000000401',
             'practice', 401, '401 PHYSICAL WAY', NULL,
             'PHYSICAL CITY', 'PS', '00401', 'US', '401-555-0100',
             41.01, -81.01),
            ('postal-304', 1990000304,
             '00000000-0000-0000-0000-000000000402',
             '10000000-0000-0000-0000-000000000402',
             'primary', 402, 'P.O. Box 402', NULL,
             'POSTAL CITY', 'PS', '00402', 'US', '402-555-0100',
             42.02, -82.02),
            ('blank-304', 1990000304,
             '00000000-0000-0000-0000-000000000400',
             '10000000-0000-0000-0000-000000000400',
             'primary', 400, NULL, NULL,
             'BLANK CITY', 'PS', '00400', 'US', '400-555-0100',
             40.0, -80.0),
            ('postal-312', 1990000312,
             '00000000-0000-0000-0000-000000000411',
             '10000000-0000-0000-0000-000000000411',
             'primary', 411, 'SYNTHETIC CLINIC', 'P.O. Box 411',
             'UNIFIED CITY', 'US', '00411', 'US', '411-555-0100',
             41.11, -81.11),
            ('postal-320', 1990000320,
             '00000000-0000-0000-0000-000000000421',
             '10000000-0000-0000-0000-000000000421',
             'primary', 421, 'P.O. Box 421', NULL,
             'UNIFIED CITY', 'US', '00421', 'US', '421-555-0100',
             41.21, -81.21),
            ('blank-338', 1990000338,
             '00000000-0000-0000-0000-000000000431',
             '10000000-0000-0000-0000-000000000431',
             'primary', 431, NULL, NULL,
             'BLANK CITY', 'BS', '00431', 'US', '431-555-0100',
             41.31, -81.31)"""
    )


async def _insert_enrichment_legacy_rows(
    database: Database,
    schema: str,
) -> None:
    """Insert legacy candidates that test atomic fallback selection."""

    await database.status(
        f"""INSERT INTO {schema}.npi_address (
            npi, address_key, type, checksum, date_added,
            first_line, second_line, city_name, state_name, postal_code,
            country_code, telephone_number, lat, long
        ) VALUES
            (1990000312,
             '00000000-0000-0000-0000-000000000412',
             'practice', 412, '2026-08-01', '412 LEGACY WAY', NULL,
             'LEGACY CITY', 'LS', '00412', 'US', '412-555-0100',
             42.12, -82.12),
            (1990000320,
             '00000000-0000-0000-0000-000000000422',
             'practice', 422, '2026-08-01', 'P.O. Box 422', NULL,
             'LEGACY CITY', 'LS', '00422', 'US', '422-555-0100',
             42.22, -82.22),
            (1990000338,
             '00000000-0000-0000-0000-000000000432',
             'practice', 432, '2026-08-01', 'P.O. Box 432', NULL,
             'LEGACY CITY', 'LS', '00432', 'US', '432-555-0100',
             42.32, -82.32)"""
    )


async def _insert_enrichment_candidates(
    database: Database,
    schema: str,
) -> None:
    """Insert every candidate set required by the executable matrix."""

    await _insert_enrichment_provider_rows(database, schema)
    await _insert_enrichment_unified_rows(database, schema)
    await _insert_enrichment_legacy_rows(database, schema)


def _provider_rows_by_npi(provider_rows) -> dict[int, dict]:
    return {
        int(provider_row._mapping["npi"]): dict(provider_row._mapping)
        for provider_row in provider_rows
    }


def _assert_physical_address_selected(provider_by_npi: dict[int, dict]) -> None:
    physical_provider = provider_by_npi[1990000304]
    physical_address = json.loads(physical_provider["address_payload"])

    assert physical_address["first_line"] == "401 PHYSICAL WAY"
    assert physical_provider["location_source"] == "entity_address_unified"
    assert physical_provider["location_hash"] == (
        "entity_address_unified:physical-304"
    )


def _assert_legacy_physical_fallback(provider_by_npi: dict[int, dict]) -> None:
    legacy_provider = provider_by_npi[1990000312]
    legacy_address = json.loads(legacy_provider["address_payload"])

    assert legacy_provider["location_source"] == "npi_address"
    assert legacy_provider["location_hash"] == (
        "npi_address:1990000312:practice:412"
    )
    assert legacy_address == {
        "npi": 1990000312,
        "type": "practice",
        "checksum": 412,
        "first_line": "412 LEGACY WAY",
        "second_line": None,
        "city_name": "LEGACY CITY",
        "state_name": "LS",
        "city": "LEGACY CITY",
        "state": "LS",
        "postal_code": "00412",
        "country_code": "US",
        "telephone_number": "412-555-0100",
        "fax_number": None,
        "phone_number": None,
        "phone_extension": None,
        "fax_number_digits": None,
        "fax_extension": None,
        "address_key": "00000000-0000-0000-0000-000000000412",
        "address_site_key": None,
        "lat": 42.12,
        "long": -82.12,
    }


def _assert_postal_fallback_boundaries(provider_by_npi: dict[int, dict]) -> None:
    unified_provider = provider_by_npi[1990000320]
    unified_address = json.loads(unified_provider["address_payload"])
    assert unified_provider["location_source"] == "entity_address_unified"
    assert unified_address["first_line"] == "P.O. Box 421"
    assert unified_address["address_site_key"] == (
        "10000000-0000-0000-0000-000000000421"
    )

    fallback_provider = provider_by_npi[1990000338]
    fallback_address = json.loads(fallback_provider["address_payload"])
    assert fallback_provider["location_source"] == "npi_address"
    assert fallback_address["first_line"] == "P.O. Box 432"
    assert fallback_address["address_site_key"] is None


@pytest.mark.asyncio
async def test_provider_enrichment_selects_one_truthful_address_row(monkeypatch):
    """Execute the full physical, postal, blank, and legacy fallback matrix."""

    async with _temporary_schema() as (database, schema):
        await _prepare_enrichment_tables(database, schema)
        await _insert_enrichment_candidates(database, schema)
        monkeypatch.setattr(
            serving,
            "_ptg2_table_columns",
            AsyncMock(return_value=serving._PTG2_LEGACY_ADDRESS_COLUMNS),
        )
        statement = await serving._provider_enrichment_statement(
            object(),
            f"{serving.PTG2_SCHEMA}.npi",
            f"{serving.PTG2_SCHEMA}.entity_address_unified",
        )
        provider_rows = await database.all(
            _schema_sql(str(statement), schema),
            npis=[1990000304, 1990000312, 1990000320, 1990000338],
        )

        provider_by_npi = _provider_rows_by_npi(provider_rows)
        _assert_physical_address_selected(provider_by_npi)
        _assert_legacy_physical_fallback(provider_by_npi)
        _assert_postal_fallback_boundaries(provider_by_npi)
