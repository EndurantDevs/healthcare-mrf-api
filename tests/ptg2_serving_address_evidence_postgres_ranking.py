# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json

import pytest

from db.connection import Database
from tests.ptg2_serving_address_evidence_postgres_geo import (
    _fetch_optimized_locations,
)
from tests.ptg2_serving_address_evidence_postgres_support import _temporary_schema


async def _insert_tied_locations(database: Database, schema: str) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_npi_scope (snapshot_key, npi)
        VALUES (41, 1990000106)
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.entity_address_unified (
            location_key, npi, address_key, premise_key,
            address_source_mask, address_sources, source_count, source_mask,
            type, checksum, first_line, city_name, state_name, postal_code
        ) VALUES
            (
                'z-tied-location', 1990000106,
                '00000000-0000-0000-0000-000000000020',
                '10000000-0000-0000-0000-000000000020',
                1, ARRAY['nppes']::varchar[], 1, 1,
                'practice', 900, '9 TEST ST', 'DETROIT', 'MI', '48201'
            ),
            (
                'a-tied-location', 1990000106,
                '00000000-0000-0000-0000-000000000020',
                '10000000-0000-0000-0000-000000000020',
                1, ARRAY['nppes']::varchar[], 1, 1,
                'practice', 900, '9 TEST ST', 'DETROIT', 'MI', '48201'
            )
        """
    )


async def _insert_tied_location_source(database: Database, schema: str) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.npi_address (
            npi, address_key, type, checksum, date_added
        ) VALUES (
            1990000106,
            '00000000-0000-0000-0000-000000000020',
            'practice', 901, '2026-07-31'
        )
        """
    )


@pytest.mark.asyncio
async def test_optimized_membership_uses_location_key_as_final_tie_breaker():
    async with _temporary_schema() as (database, schema):
        await _insert_tied_locations(database, schema)
        await _insert_tied_location_source(database, schema)

        location_rows = await _fetch_optimized_locations(database, schema, 1)

        assert len(location_rows) == 1
        selected_location = location_rows[0]._mapping
        assert json.loads(selected_location["address_payload"])["location_key"] == (
            "a-tied-location"
        )


async def _insert_mrf_issuer_locations(database: Database, schema: str) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_npi_scope (snapshot_key, npi)
        VALUES (41, 1990000049)
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.entity_address_unified (
            location_key, npi, address_key, premise_key,
            address_source_mask, address_sources, source_count, source_mask,
            type, checksum, first_line, city_name, state_name, postal_code
        ) VALUES
            (
                'duplicate-issuer', 1990000049,
                '00000000-0000-0000-0000-000000000006',
                '10000000-0000-0000-0000-000000000006',
                2, ARRAY['mrf']::varchar[], 1, 2,
                'practice', 1, '1 TEST ST', 'DETROIT', 'MI', '48201'
            ),
            (
                'distinct-issuers', 1990000049,
                '00000000-0000-0000-0000-000000000007',
                '10000000-0000-0000-0000-000000000007',
                2, ARRAY['mrf']::varchar[], 1, 2,
                'practice', 2, '2 TEST ST', 'DETROIT', 'MI', '48201'
            )
        """
    )


async def _insert_mrf_issuer_sources(database: Database, schema: str) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.mrf_address (
            npi, address_key, type, checksum, date_added, source_issuer_names
        ) VALUES
            (
                1990000049,
                '00000000-0000-0000-0000-000000000006',
                'practice', 1, NOW(), ARRAY['Issuer A', ' issuer a ']::varchar[]
            ),
            (
                1990000049,
                '00000000-0000-0000-0000-000000000007',
                'practice', 2, NOW(), ARRAY['Issuer A', 'Issuer B']::varchar[]
            )
        """
    )


@pytest.mark.asyncio
async def test_optimized_membership_requires_distinct_normalized_mrf_issuers():
    async with _temporary_schema() as (database, schema):
        await _insert_mrf_issuer_locations(database, schema)
        await _insert_mrf_issuer_sources(database, schema)

        location_rows = await _fetch_optimized_locations(database, schema, 2)

        assert len(location_rows) == 1
        selected_location = location_rows[0]._mapping
        assert selected_location["_geo_evidence_level"] == (
            "multi_issuer_marketplace_address"
        )
        assert selected_location["_geo_evidence_source_id"] == 2
        assert json.loads(selected_location["address_payload"])["location_key"] == (
            "distinct-issuers"
        )
