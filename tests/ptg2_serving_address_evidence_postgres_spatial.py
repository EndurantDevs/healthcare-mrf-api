# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from api import ptg2_serving as serving
from db.connection import Database
from tests.ptg2_serving_address_evidence_postgres_geo import (
    _insert_spatial_reference_rows,
)
from tests.ptg2_serving_address_evidence_postgres_support import (
    _schema_sql,
    _temporary_schema,
)


def _spatial_coherence_query(schema: str) -> serving._MembershipLocationQuery:
    request_arg_map = {
        "zip5": "00001",
        "lat": "42.0",
        "long": "-83.0",
        "radius_miles": "30",
    }
    query_parameter_map = {
        "limit": 10,
        "offset": 0,
        "shared_snapshot_key": 41,
    }
    filter_sql, distance_sql = serving._membership_filter_sql(
        request_arg_map,
        candidate_npis=None,
        uses_unified_addresses=True,
        address_zip5_sql=serving._ptg2_address_zip5_sql("addr", unified=True),
        parameter_map=query_parameter_map,
        literal_service_address_types=True,
    )
    return serving._MembershipLocationQuery(
        address_table=f"{schema}.entity_address_unified",
        npi_scope_table=f"{schema}.ptg2_v3_npi_scope",
        filter_sql=(
            "npi_scope.snapshot_key = :shared_snapshot_key "
            f"AND ({filter_sql})"
        ),
        parameter_map=query_parameter_map,
        distance_sql=distance_sql,
        knn_order_sql=None,
        address_assurance_sql=serving._membership_address_assurance_sql(
            request_arg_map,
            True,
        ),
    )


async def _insert_spatial_candidate_memberships(
    database: Database,
    schema: str,
) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_npi_scope (snapshot_key, npi)
        VALUES
            (41, 1990000205),
            (41, 1990000213),
            (41, 1990000221),
            (41, 1990000239)
        """
    )


async def _insert_spatial_unified_addresses(
    database: Database,
    schema: str,
) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.entity_address_unified (
            location_key, npi, address_key, premise_key,
            address_source_mask, address_sources, source_count, source_mask,
            type, checksum, first_line, city_name, state_name, state_code,
            postal_code, zip5, country_code, address_precision, lat, long
        ) VALUES
            (
                'validated-exact-no-point', 1990000205,
                '00000000-0000-0000-0000-000000000201',
                '10000000-0000-0000-0000-000000000201',
                1, ARRAY['nppes']::varchar[], 1, 1,
                'practice', 201, '201 SYNTHETIC WAY', 'TEST CITY', 'TS', 'TS',
                '00001', '00001', 'US', 'street', NULL, NULL
            ),
            (
                'validated-nearby-point', 1990000213,
                '00000000-0000-0000-0000-000000000202',
                '10000000-0000-0000-0000-000000000202',
                2, ARRAY['mrf']::varchar[], 1, 2,
                'practice', 202, '202 SYNTHETIC WAY', 'NEARBY CITY', 'TS', 'TS',
                '00002', '00002', 'US', 'street', 42.0, -83.18
            ),
            (
                'incoherent-nearby-point', 1990000221,
                '00000000-0000-0000-0000-000000000203',
                '10000000-0000-0000-0000-000000000203',
                1, ARRAY['nppes']::varchar[], 1, 1,
                'practice', 203, '203 SYNTHETIC WAY', 'OTHER CITY', 'OS', 'OS',
                '00003', '00003', 'US', 'street', 42.0, -83.05
            ),
            (
                'coherent-exact-point-outside-radius', 1990000239,
                '00000000-0000-0000-0000-000000000204',
                '10000000-0000-0000-0000-000000000204',
                1, ARRAY['nppes']::varchar[], 1, 1,
                'practice', 204, '204 SYNTHETIC WAY', 'TEST CITY', 'TS', 'TS',
                '00001', '00001', 'US', 'street', 42.65, -83.0
            )
        """
    )


async def _insert_spatial_source_addresses(
    database: Database,
    schema: str,
) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.npi_address (
            npi, address_key, type, checksum, date_added
        ) VALUES
            (
                1990000205,
                '00000000-0000-0000-0000-000000000201',
                'practice', 201, '2026-07-31'
            ),
            (
                1990000221,
                '00000000-0000-0000-0000-000000000203',
                'practice', 203, '2026-07-31'
            ),
            (
                1990000239,
                '00000000-0000-0000-0000-000000000204',
                'practice', 204, '2026-07-31'
            )
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.mrf_address (
            npi, address_key, type, checksum, date_added,
            source_import_ids, source_import_dates,
            source_issuer_names, source_urls
        ) VALUES (
            1990000213,
            '00000000-0000-0000-0000-000000000202',
            'practice', 202, '2026-07-31',
            ARRAY['synthetic-version']::varchar[],
            ARRAY['2026-07-31'::date],
            ARRAY['SYNTHETIC ISSUER A', 'SYNTHETIC ISSUER B']::varchar[],
            ARRAY['https://example.test/synthetic']::varchar[]
        )
        """
    )


async def _insert_spatial_candidate_rows(database: Database, schema: str) -> None:
    await _insert_spatial_candidate_memberships(database, schema)
    await _insert_spatial_unified_addresses(database, schema)
    await _insert_spatial_source_addresses(database, schema)


@pytest.mark.asyncio
async def test_radius_membership_rejects_incoherent_and_out_of_radius_points():
    async with _temporary_schema() as (database, schema):
        await _insert_spatial_reference_rows(database, schema)
        await _insert_spatial_candidate_rows(database, schema)
        location_query = _spatial_coherence_query(schema)
        location_sql = _schema_sql(
            serving._membership_location_sql(location_query, limit=10, offset=0),
            schema,
        )

        location_rows = await database.all(
            location_sql,
            **location_query.parameter_map,
        )

        actual_npis = sorted(
            location_record._mapping["npi"]
            for location_record in location_rows
        )
        assert actual_npis == [
            1990000205,
            1990000213,
        ], actual_npis

        await database.status(
            f"""
            UPDATE {schema}.entity_address_unified
               SET country_code = NULL
             WHERE location_key = 'validated-exact-no-point'
            """
        )
        rows_without_country = await database.all(
            location_sql,
            **location_query.parameter_map,
        )

        assert [
            location_record._mapping["npi"]
            for location_record in rows_without_country
        ] == [
            1990000213
        ]
