# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json

import pytest

from api import ptg2_serving as serving
from db.connection import Database
from tests.ptg2_serving_address_evidence_postgres_support import (
    _schema_sql,
    _temporary_schema,
)


def _optimized_query(schema: str, source_mask: int) -> serving._MembershipLocationQuery:
    return serving._MembershipLocationQuery(
        address_table=f"{schema}.entity_address_unified",
        npi_scope_table=f"{schema}.ptg2_v3_npi_scope",
        filter_sql=(
            "npi_scope.snapshot_key = :shared_snapshot_key "
            f"AND (addr.address_source_mask & {source_mask}) <> 0"
        ),
        parameter_map={"shared_snapshot_key": 41, "limit": 10, "offset": 0},
        distance_sql="NULL::double precision",
        knn_order_sql=None,
        address_assurance_sql=serving._ptg2_geo_assured_address_sql("addr"),
    )


async def _fetch_optimized_locations(
    database: Database,
    schema: str,
    source_mask: int,
):
    location_query = _optimized_query(schema, source_mask)
    location_sql = _schema_sql(
        serving._membership_location_sql(location_query, limit=10, offset=0),
        schema,
    )
    return await database.all(location_sql, **location_query.parameter_map)


def _knn_query(schema: str) -> serving._MembershipLocationQuery:
    request_arg_map = {
        "lat": "42.3314",
        "long": "-83.0458",
        "radius_miles": "30",
    }
    query_parameter_map = {
        "limit": 2,
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
        knn_order_sql=serving._membership_knn_order_sql(
            request_arg_map,
            candidate_npis=None,
            uses_unified_addresses=True,
            offset=0,
        ),
        address_assurance_sql=serving._membership_address_assurance_sql(
            request_arg_map,
            True,
        ),
    )


async def _insert_knn_location(database: Database, schema: str) -> None:
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
            type, checksum, first_line, city_name, state_name, postal_code,
            zip5, address_precision, lat, long
        ) VALUES (
            'knn-precedence', 1990000015,
            '00000000-0000-0000-0000-000000000033',
            '10000000-0000-0000-0000-000000000033',
            7, ARRAY['nppes', 'mrf', 'cms_doctors']::varchar[], 3, 7,
            'practice', 33, '33 TEST STREET', 'TEST CITY', 'MI', '48201',
            '48201', 'street', 42.3314, -83.0458
        )
        """
    )


async def _insert_knn_source_addresses(database: Database, schema: str) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.npi_address (
            npi, address_key, type, checksum, date_added
        ) VALUES (
            1990000015,
            '00000000-0000-0000-0000-000000000033',
            'practice', 33, '2026-07-29'
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
            1990000015,
            '00000000-0000-0000-0000-000000000033',
            'practice', 33, '2026-07-29',
            ARRAY['knn-test-version']::varchar[],
            ARRAY['2026-07-29'::date],
            ARRAY['TEST ISSUER A', 'TEST ISSUER B']::varchar[],
            ARRAY['https://example.test/knn']::varchar[]
        )
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.doctor_clinician_address (
            npi, address_key, address_checksum, updated_at
        ) VALUES (
            1990000015,
            '00000000-0000-0000-0000-000000000033',
            33, '2026-07-29 12:00:00'
        )
        """
    )


async def _fetch_knn_locations(database: Database, schema: str):
    location_query = _knn_query(schema)
    location_sql = _schema_sql(
        serving._membership_location_sql(location_query, limit=2, offset=0),
        schema,
    )
    location_rows = await database.all(
        location_sql,
        **location_query.parameter_map,
    )
    return location_query, location_sql, location_rows


async def _clear_knn_source_addresses(database: Database, schema: str) -> None:
    for source_table_name in (
        "doctor_clinician_address",
        "mrf_address",
        "npi_address",
    ):
        await database.status(f"DELETE FROM {schema}.{source_table_name}")
    await database.status(
        f"""
        UPDATE {schema}.entity_address_unified
           SET address_source_mask = 0,
               source_mask = 0,
               source_count = 0,
               address_sources = ARRAY[]::varchar[]
         WHERE location_key = 'knn-precedence'
        """
    )


@pytest.mark.asyncio
async def test_knn_template_executes_precedence_and_empty_probe_shape():
    async with _temporary_schema() as (database, schema):
        await _insert_knn_location(database, schema)
        await _insert_knn_source_addresses(database, schema)

        location_query, location_sql, location_rows = await _fetch_knn_locations(
            database, schema
        )

        assert len(location_rows) == 1
        selected_location = location_rows[0]._mapping
        assert selected_location["npi"] == 1990000015
        assert selected_location["_geo_evidence_level"] == "nppes_registry_address"
        assert selected_location["_geo_evidence_source_id"] == 1
        assert selected_location["_ptg_source_exhausted"] is True
        assert selected_location["_ptg_probe_empty"] is False
        assert json.loads(selected_location["address_payload"])["location_key"] == (
            "knn-precedence"
        )

        await _clear_knn_source_addresses(database, schema)
        empty_probe_rows = await database.all(
            location_sql,
            **location_query.parameter_map,
        )

        assert len(empty_probe_rows) == 1
        empty_probe = empty_probe_rows[0]._mapping
        assert empty_probe["npi"] is None
        assert empty_probe["_ptg_source_exhausted"] is True
        assert empty_probe["_ptg_probe_empty"] is True


async def _insert_cms_identity_locations(database: Database, schema: str) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_npi_scope (snapshot_key, npi)
        VALUES (41, 1990000031), (41, 1990000114)
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
                'cms-unanchored', 1990000031,
                '00000000-0000-0000-0000-000000000003',
                '10000000-0000-0000-0000-000000000003',
                4, ARRAY['cms_doctors']::varchar[], 1, 4,
                'practice', 1, '1 TEST ST', 'DETROIT', 'MI', '48201'
            ),
            (
                'cms-anchored', 1990000031,
                '00000000-0000-0000-0000-000000000004',
                '10000000-0000-0000-0000-000000000004',
                4, ARRAY['cms_doctors']::varchar[], 1, 4,
                'practice', 2, '2 TEST ST', 'DETROIT', 'MI', '48201'
            ),
            (
                'nppes-anchor', 1990000031,
                '00000000-0000-0000-0000-000000000005',
                '10000000-0000-0000-0000-000000000004',
                1, ARRAY['nppes']::varchar[], 1, 1,
                'practice', 3, '2 TEST ST', 'DETROIT', 'MI', '48201'
            )
        """
    )


async def _insert_cms_mask_only_locations(database: Database, schema: str) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.entity_address_unified (
            location_key, npi, address_key, premise_key,
            address_source_mask, address_sources, source_count, source_mask,
            type, checksum, first_line, city_name, state_name, postal_code
        ) VALUES
            (
                'cms-mask-only', 1990000114,
                '00000000-0000-0000-0000-000000000030',
                '10000000-0000-0000-0000-000000000030',
                4, ARRAY['cms_doctors']::varchar[], 1, 4,
                'practice', 4, '3 TEST ST', 'DETROIT', 'MI', '48201'
            ),
            (
                'nppes-mask-only-anchor', 1990000114,
                '00000000-0000-0000-0000-000000000031',
                '10000000-0000-0000-0000-000000000030',
                1, ARRAY['nppes']::varchar[], 1, 1,
                'practice', 5, '3 TEST ST', 'DETROIT', 'MI', '48201'
            )
        """
    )


async def _insert_cms_source_anchors(database: Database, schema: str) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.doctor_clinician_address (
            npi, address_key, address_checksum, updated_at
        ) VALUES
            (
                1990000031,
                '00000000-0000-0000-0000-000000000003',
                1, '2026-07-31 10:00:00'
            ),
            (
                1990000031,
                '00000000-0000-0000-0000-000000000004',
                2, '2026-07-31 10:00:00'
            ),
            (
                1990000114,
                '00000000-0000-0000-0000-000000000030',
                4, '2026-07-31 10:00:00'
            )
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.npi_address (
            npi, address_key, type, checksum, date_added
        ) VALUES (
            1990000031,
            '00000000-0000-0000-0000-000000000005',
            'practice', 3, '2026-07-31'
        )
        """
    )


@pytest.mark.asyncio
async def test_optimized_membership_rejects_npi_wide_cms_anchor():
    async with _temporary_schema() as (database, schema):
        await _insert_cms_identity_locations(database, schema)
        await _insert_cms_mask_only_locations(database, schema)
        await _insert_cms_source_anchors(database, schema)

        location_rows = await _fetch_optimized_locations(database, schema, 4)

        assert len(location_rows) == 1
        selected_location = location_rows[0]._mapping
        assert selected_location["_geo_evidence_level"] == (
            "cms_doctors_source_with_nppes_identity_anchor"
        )
        assert selected_location["_geo_evidence_source_id"] == 3
        assert json.loads(selected_location["address_payload"])["location_key"] == (
            "cms-anchored"
        )
