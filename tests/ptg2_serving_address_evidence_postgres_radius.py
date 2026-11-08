# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import math

import pytest

from api import ptg2_serving as serving
from api.endpoint import pricing
from db.connection import Database
from tests.ptg2_serving_address_evidence_postgres_geo import (
    _insert_spatial_reference_rows,
    _knn_query,
)
from tests.ptg2_serving_address_evidence_postgres_support import (
    _schema_sql,
    _temporary_schema,
)
from tests.test_ptg2_graph_runtime_boundaries import _local_distance_tables


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("origin", "point"),
    [((42.3314, -83.0458), (42.2, -83.0)), ((0.0, 179.99), (0.0, -179.99)), ((89.99, 0.0), (89.99, 90.0))],
)
async def test_radius_uses_exact_displayed_distance_boundary(origin, point):
    async with _temporary_schema() as (database, _):
        parameter_map = dict(geo_lat=origin[0], geo_long=origin[1], latitude=point[0], longitude=point[1])
        coordinates = (
            "(SELECT CAST(:latitude AS double precision) AS lat, CAST(:longitude AS double precision) AS long)"
        )
        distance_sql = serving._ptg2_geo_distance_miles_sql("addr.lat", "addr.long")
        distance_row = await database.first(
            f"SELECT {distance_sql} AS distance FROM {coordinates} addr", **parameter_map
        )
        distance = distance_row._mapping["distance"]
        predicate = serving._ptg2_geo_dwithin_sql("addr.lat", "addr.long")
        for radius, expected in (
            (math.nextafter(distance, -math.inf), False),
            (distance, True),
            (math.nextafter(distance, math.inf), True),
        ):
            radius_row = await database.first(
                f"SELECT {distance_sql} AS distance, {predicate} AS eligible FROM {coordinates} addr",
                **parameter_map,
                geo_radius_miles=radius,
            )
            assert radius_row._mapping["distance"] == distance
            assert radius_row._mapping["eligible"] is expected


@pytest.mark.asyncio
async def test_zero_radius_keeps_only_the_origin():
    async with _temporary_schema() as (database, _):
        predicate = serving._ptg2_geo_dwithin_sql("addr.lat", "addr.long")
        rows = await database.all(
            f"SELECT {predicate} AS eligible FROM (VALUES (0.0, 0.0), (0.0, 0.000000001)) addr(lat, long)",
            geo_lat=0.0,
            geo_long=0.0,
            geo_radius_miles=0.0,
        )
        assert [row._mapping["eligible"] for row in rows] == [True, False]


@pytest.mark.asyncio
@pytest.mark.parametrize(("bearing", "offset_meters", "eligible"), [(math.pi, -1.0, False), (math.pi / 2, 1.0, True)])
@pytest.mark.parametrize("allowed_amount", [False, True])
async def test_spherical_radius_rejects_spheroid_only_and_keeps_sphere_only(
    bearing, offset_meters, eligible, allowed_amount
):
    async with _temporary_schema() as (database, _):
        parameter_map = {}
        geo_sql = pricing._allowed_amount_geo_sql if allowed_amount else serving._membership_geo_sql
        distance_sql, filters = geo_sql(
            {"lat": "42.3314", "long": "-83.0458", "radius_miles": "10"},
            uses_unified_addresses=True,
            parameter_map=parameter_map,
        )
        radius_row = await database.first(
            f"""
            WITH sample AS (
                SELECT Geography(ST_MakePoint(-83.0458, 42.3314)) AS origin
            ), point AS (
                SELECT origin, ST_Project(origin, 10 * 1609.344 + :offset_meters, :bearing) AS point
                FROM sample
            ), addr AS (
                SELECT origin, point, ST_Y(point::geometry) AS lat, ST_X(point::geometry) AS long,
                       'street'::text AS address_precision FROM point
            )
            SELECT {distance_sql} AS distance, ({" AND ".join(filters)}) AS eligible,
                   ST_DWithin(origin, point, 10 * 1609.34, true) AS old_eligible
            FROM addr
            """,
            **parameter_map,
            bearing=bearing,
            offset_meters=offset_meters,
        )
        assert radius_row._mapping["eligible"] is eligible
        assert (radius_row._mapping["distance"] <= 10.0) is eligible
        assert radius_row._mapping["old_eligible"] is not eligible


async def _insert_radius_boundary_addresses(database: Database, schema: str, broad_scope: bool) -> None:
    await _insert_spatial_reference_rows(database, schema)
    await database.status(
        f"""
        INSERT INTO {schema}.entity_address_unified (
            location_key, npi, address_key, premise_key, address_source_mask, address_sources,
            source_count, source_mask, type, checksum, first_line, city_name, state_name, state_code,
            postal_code, zip5, country_code, address_precision, lat, long
        )
        SELECT 'radius-boundary-' || value, npi, md5('address-' || value)::uuid, md5('premise-' || value)::uuid,
               1, ARRAY['nppes']::varchar[], 1, 1, 'practice', value, 'SYNTHETIC CLINIC',
               'TEST CITY', 'TS', 'TS', '00001', '00001', 'US', 'street',
               ST_Y(point::geometry), ST_X(point::geometry)
        FROM (
            SELECT value, CASE value WHEN 1 THEN 1990000015 WHEN 2 THEN 1990000221 ELSE 1990000247 END AS npi,
                   ST_Project(Geography(ST_MakePoint(-83.0458, 42.3314)),
                              CASE value WHEN 1 THEN 5.0 WHEN 2 THEN 9.9 ELSE 9.999 END * 1609.344, pi()) AS point
            FROM generate_series(1, 72) value
        ) candidates
        """
    )
    await database.status(
        f"INSERT INTO {schema}.ptg2_v3_npi_scope (snapshot_key, npi) "
        f"SELECT DISTINCT 41, npi FROM {schema}.entity_address_unified"
    )
    await database.status(
        f"INSERT INTO {schema}.npi_address (npi, address_key, type, checksum, date_added) "
        f"SELECT npi, address_key, type, checksum, '2026-01-01' FROM {schema}.entity_address_unified"
    )
    if broad_scope:
        await database.status(
            f"INSERT INTO {schema}.ptg2_v3_npi_scope (snapshot_key, npi) "
            f"SELECT 41, value FROM generate_series(1, {serving._MEMBERSHIP_KNN_SPARSE_SCOPE_LIMIT}) value"
        )


async def _radius_locations(database: Database, schema: str, limit: int, offset: int = 0):
    query = _knn_query(schema, radius_miles="10", limit=limit)
    query.parameter_map["offset"] = offset
    sql = _schema_sql(serving._membership_location_sql(query, limit=limit, offset=offset), schema)
    return [dict(row._mapping) for row in await database.all(sql, **query.parameter_map)]


async def _assert_later_code_match(database, schema, monkeypatch, matching_npi):
    """Grow a real row-limited KNN prefix before declaring its code scope exhausted."""
    probe_limits = []

    async def location_rows(*_args, limit, **_kwargs):
        probe_limits.append(limit)
        return await _radius_locations(database, schema, limit)

    async def memberships(_session, _tables, npis, _request, _state):
        return {npi: (10 if npi == matching_npi else 20,) for npi in npis}

    async def code_sets(*_args, provider_set_keys, **_kwargs):
        return tuple(key for key in provider_set_keys if key == 10)

    monkeypatch.setattr(serving, "_membership_location_rows", location_rows)
    monkeypatch.setattr(serving, "_local_v4_memberships", memberships)
    monkeypatch.setattr(serving, "_scoped_rate_provider_set_keys", code_sets)
    tables = _local_distance_tables()
    request = serving._LocalDistanceGraphRequest(1, [{"code_key": 7}], serving._v4_geo_rate_forward_limits(tables))
    candidates = await serving._scan_local_distance_graph(object(), tables, {}, request, 1, 4)

    assert probe_limits == [1, 4]
    assert [location["npi"] for location in candidates.location_rows] == [matching_npi]
    assert candidates.provider_set_keys_by_npi == {matching_npi: {10}}


@pytest.mark.asyncio
@pytest.mark.parametrize("broad_scope", [False, True])
async def test_exact_radius_precedes_raw_probe_limit_and_page_exhaustion(broad_scope, monkeypatch):
    async with _temporary_schema() as (database, schema):
        await _insert_radius_boundary_addresses(database, schema, broad_scope)
        first_page = await _radius_locations(database, schema, 1)
        full_prefix = await _radius_locations(database, schema, 3)
        second_page = await _radius_locations(database, schema, 1, offset=1)
        empty_page = await _radius_locations(database, schema, 1, offset=2)

        assert [row["npi"] for row in full_prefix] == [1990000015, 1990000221]
        assert [row["npi"] for row in first_page + second_page] == [row["npi"] for row in full_prefix]
        assert all(row["distance_miles"] <= 10.0 for row in full_prefix)
        assert first_page[0]["_ptg_source_exhausted"] is False
        assert all(row["_ptg_source_exhausted"] is True for row in full_prefix)
        assert empty_page == []
        await _assert_later_code_match(database, schema, monkeypatch, full_prefix[1]["npi"])
