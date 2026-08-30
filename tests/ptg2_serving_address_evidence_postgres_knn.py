# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace
import json

import pytest

from api import ptg2_serving as serving
from db.connection import Database
from tests.ptg2_serving_address_evidence_postgres_geo import (
    _insert_spatial_reference_rows,
    _knn_query,
)
from tests.ptg2_serving_address_evidence_postgres_support import (
    _schema_sql,
    _temporary_schema,
)


async def _fetch_hybrid_knn_locations(database: Database, schema: str):
    query = _knn_query(schema, zip5="00001", radius_miles="5", limit=1)
    sql = _schema_sql(
        serving._membership_location_sql(query, limit=1, offset=0),
        schema,
    )
    return query, await database.all(sql, **query.parameter_map)


async def _insert_exact_zip_fallback(database: Database, schema: str) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_npi_scope (snapshot_key, npi)
        VALUES (41, 1990000247)
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.entity_address_unified (
            location_key, npi, address_key, premise_key,
            address_source_mask, address_sources, source_count, source_mask,
            type, checksum, first_line, city_name, state_name, state_code,
            postal_code, zip5, country_code, address_precision, lat, long
        ) VALUES (
            'hybrid-exact-no-point', 1990000247,
            '00000000-0000-0000-0000-000000000247',
            '10000000-0000-0000-0000-000000000247',
            1, ARRAY['nppes']::varchar[], 1, 1,
            'practice', 247, '247 TEST STREET', 'TEST CITY', 'TS', 'TS',
            '00001', '00001', 'US', 'street', NULL, NULL
        )
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.npi_address (
            npi, address_key, type, checksum, date_added
        ) VALUES (
            1990000247,
            '00000000-0000-0000-0000-000000000247',
            'practice', 247, '2026-08-30'
        )
        """
    )


async def _insert_radius_flood(
    database: Database,
    schema: str,
    *,
    longitude_sql: str,
    count: int = 70,
) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_npi_scope (snapshot_key, npi)
        SELECT 41, 1980000000 + value
        FROM generate_series(1, {count}) value
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.entity_address_unified (
            location_key, npi, type, checksum, address_precision, lat, long
        )
        SELECT
            'hybrid-flood-' || value,
            1980000000 + value,
            'practice', value, 'street', 42.3314, {longitude_sql}
        FROM generate_series(1, {count}) value
        """
    )


async def _insert_assurance_rank_case(database: Database, schema: str) -> None:
    await _insert_spatial_reference_rows(database, schema)
    await database.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_npi_scope (snapshot_key, npi)
        VALUES (41, 1990000015)
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.entity_address_unified (
            location_key, npi, address_key, premise_key,
            address_source_mask, address_sources, source_count, source_mask,
            type, checksum, first_line, city_name, state_name, state_code,
            postal_code, zip5, country_code, address_precision, lat, long
        ) VALUES
            (
                'hybrid-near-unassured', 1990000015,
                '00000000-0000-0000-0000-000000000011',
                '10000000-0000-0000-0000-000000000011',
                1, ARRAY['nppes']::varchar[], 1, 1,
                'practice', 11, '11 TEST STREET', 'TEST CITY', 'TS', 'TS',
                '00001', '00001', 'US', 'street', 42.3314, -83.0458
            ),
            (
                'hybrid-far-assured', 1990000015,
                '00000000-0000-0000-0000-000000000012',
                '10000000-0000-0000-0000-000000000012',
                1, ARRAY['nppes']::varchar[], 1, 1,
                'practice', 12, '12 TEST STREET', 'TEST CITY', 'TS', 'TS',
                '00001', '00001', 'US', 'street', 42.3314, -83.0558
            )
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.npi_address (
            npi, address_key, type, checksum, date_added
        ) VALUES (
            1990000015,
            '00000000-0000-0000-0000-000000000012',
            'practice', 12, '2026-08-30'
        )
        """
    )


def _location_signature(locations):
    return [
        (
            location._mapping["npi"],
            json.loads(location._mapping["address_payload"])["location_key"],
            location._mapping["distance_miles"],
            location._mapping["_geo_evidence_level"],
        )
        for location in locations
    ]


@pytest.mark.asyncio
async def test_knn_radius_fence_preserves_spheroid_boundary():
    async with _temporary_schema() as (database, _):
        boundary = await database.first(
            """
            WITH sample AS (
                SELECT Geography(ST_MakePoint(-87.6, 41.9)) AS origin,
                       40230.123456789::double precision AS radius_meters
            ), candidate AS (
                SELECT origin, radius_meters,
                       ST_Project(origin, radius_meters, 3.0) AS point
                FROM sample
            )
            SELECT ST_DWithin(origin, point, radius_meters, true) AS direct,
                   (SELECT ST_DWithin(origin, point, radius_meters, true)
                    OFFSET 0) AS fenced,
                   ST_Distance(origin, point, true) <= radius_meters AS rounded
            FROM candidate
            """
        )

        assert dict(boundary._mapping) == {
            "direct": True,
            "fenced": True,
            "rounded": False,
        }


@pytest.mark.asyncio
async def test_knn_exact_zip_runs_after_out_of_radius_source_exhaustion():
    async with _temporary_schema() as (database, schema):
        await _insert_spatial_reference_rows(database, schema)
        await _insert_exact_zip_fallback(database, schema)
        await _insert_radius_flood(database, schema, longitude_sql="-84.0")

        _, locations = await _fetch_hybrid_knn_locations(database, schema)

        assert len(locations) == 1
        selected = locations[0]._mapping
        assert json.loads(selected["address_payload"])["location_key"] == (
            "hybrid-exact-no-point"
        )
        assert selected["distance_miles"] is None
        assert selected["_ptg_source_exhausted"] is True


@pytest.mark.asyncio
async def test_knn_broad_underfilled_radius_reaches_exact_zip():
    async with _temporary_schema() as (database, schema):
        await _insert_spatial_reference_rows(database, schema)
        await _insert_exact_zip_fallback(database, schema)
        await _insert_radius_flood(
            database,
            schema,
            longitude_sql="-84.0",
            count=serving._MEMBERSHIP_KNN_SPARSE_SCOPE_LIMIT + 1,
        )

        _, locations = await _fetch_hybrid_knn_locations(database, schema)

        assert len(locations) == 1
        selected = locations[0]._mapping
        assert json.loads(selected["address_payload"])["location_key"] == (
            "hybrid-exact-no-point"
        )
        assert selected["_ptg_source_exhausted"] is True


@pytest.mark.asyncio
async def test_knn_withholds_exact_zip_while_radius_source_is_capped():
    async with _temporary_schema() as (database, schema):
        await _insert_spatial_reference_rows(database, schema)
        await _insert_exact_zip_fallback(database, schema)
        await _insert_radius_flood(
            database,
            schema,
            longitude_sql="-83.0458 + value * 0.00001",
        )

        _, locations = await _fetch_hybrid_knn_locations(database, schema)

        assert len(locations) == 1
        empty_probe = locations[0]._mapping
        assert empty_probe["npi"] is None
        assert empty_probe["_ptg_probe_empty"] is True
        assert empty_probe["_ptg_source_exhausted"] is False


@pytest.mark.asyncio
async def test_knn_assurance_precedes_per_npi_distance_rank():
    async with _temporary_schema() as (database, schema):
        await _insert_assurance_rank_case(database, schema)
        query, bounded_locations = await _fetch_hybrid_knn_locations(database, schema)
        exhaustive_query = replace(query, knn_order_sql=None)
        exhaustive_sql = _schema_sql(
            serving._membership_location_sql(exhaustive_query, limit=1, offset=0),
            schema,
        )
        exhaustive_locations = await database.all(
            exhaustive_sql,
            **exhaustive_query.parameter_map,
        )

        assert len(bounded_locations) == 1
        selected = bounded_locations[0]._mapping
        assert json.loads(selected["address_payload"])["location_key"] == (
            "hybrid-far-assured"
        )
        assert selected["_ptg_source_exhausted"] is True
        assert _location_signature(bounded_locations) == _location_signature(
            exhaustive_locations
        )
