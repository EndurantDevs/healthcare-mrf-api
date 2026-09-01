# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real-PostgreSQL proof for the bounded exact-distance taxonomy path."""

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_address_evidence_postgres_coverage import (
    _invalid_fixture_npis,
)
from tests.ptg2_serving_address_evidence_postgres_geo import (
    _insert_spatial_reference_rows,
)
from tests.ptg2_serving_address_evidence_postgres_support import (
    _schema_sql,
    _temporary_schema,
)
from tests.ptg2_serving_coverage_paydown_support import strict_v3_tables
from tests.test_ptg2_graph_runtime_boundaries import _local_distance_tables


_CREATE_SQL = (
    """CREATE TABLE {schema}.npi (
        npi bigint PRIMARY KEY,
        entity_type_code smallint NOT NULL,
        search_taxonomy_codes varchar[] NOT NULL
    )""",
    """CREATE TABLE {schema}.npi_taxonomy (
        npi bigint NOT NULL,
        healthcare_provider_taxonomy_code varchar NOT NULL,
        healthcare_provider_primary_taxonomy_switch varchar
    )""",
)
_INSERT_NPI_SQL = """INSERT INTO {schema}.npi (
        npi, entity_type_code, search_taxonomy_codes
    )
    SELECT npi, 1, ARRAY['207R00000X']::varchar[]
      FROM unnest(CAST(:projection_negative_npis AS bigint[])) npi
    UNION ALL
    SELECT npi, 1, ARRAY['207Q00000X']::varchar[]
      FROM unnest(CAST(:projection_positive_npis AS bigint[])) npi"""
_INSERT_SCOPE_SQL = """INSERT INTO {schema}.ptg2_v3_npi_scope (snapshot_key, npi)
    SELECT 41, npi FROM {schema}.npi"""
_INSERT_TAXONOMY_SQL = """INSERT INTO {schema}.npi_taxonomy (
        npi, healthcare_provider_taxonomy_code,
        healthcare_provider_primary_taxonomy_switch
    )
    SELECT npi, '207Q00000X', 'Y'
      FROM unnest(CAST(:exact_npis AS bigint[])) npi"""
_INSERT_PROJECTION_NEGATIVE_SQL = """INSERT INTO {schema}.entity_address_unified (
        location_key, npi, address_key, premise_key,
        address_source_mask, address_sources, source_count, source_mask,
        type, checksum, first_line, city_name, state_name, state_code,
        postal_code, zip5, country_code, address_precision, lat, long
    )
    SELECT
        'projection-negative-' || ordinal, npi,
        md5('projection-negative-address-' || ordinal)::uuid,
        md5('projection-negative-premise-' || ordinal)::uuid,
        1, ARRAY['nppes']::varchar[], 1, 1, 'practice', ordinal,
        ordinal || ' SYNTHETIC WAY', 'TEST CITY', 'TS', 'TS',
        '00001', '00001', 'US', 'street', 42.0, -83.0 + ordinal * 0.000001
      FROM unnest(CAST(:projection_negative_npis AS bigint[]))
           WITH ORDINALITY fixture(npi, ordinal)"""
_INSERT_EXACT_NEGATIVE_SQL = """INSERT INTO {schema}.entity_address_unified (
        location_key, npi, address_key, premise_key,
        address_source_mask, address_sources, source_count, source_mask,
        type, checksum, first_line, city_name, state_name, state_code,
        postal_code, zip5, country_code, address_precision, lat, long
    )
    SELECT
        'exact-negative-' || ordinal, npi,
        md5('exact-negative-address-' || ordinal)::uuid,
        md5('exact-negative-premise-' || ordinal)::uuid,
        1, ARRAY['nppes']::varchar[], 1, 1, 'practice', 1000 + ordinal,
        ordinal || ' SYNTHETIC WAY', 'TEST CITY', 'TS', 'TS',
        '00001', '00001', 'US', 'street', 42.0, -83.0 + ordinal * 0.00001
      FROM unnest(CAST(:exact_negative_npis AS bigint[]))
           WITH ORDINALITY fixture(npi, ordinal)"""
_INSERT_EXACT_SQL = """INSERT INTO {schema}.entity_address_unified (
        location_key, npi, address_key, premise_key,
        address_source_mask, address_sources, source_count, source_mask,
        type, checksum, first_line, city_name, state_name, state_code,
        postal_code, zip5, country_code, address_precision, lat, long
    ) VALUES
        (
            'exact-geocoded', :exact_geo_npi,
            md5('exact-geocoded-address')::uuid,
            md5('exact-geocoded-premise')::uuid,
            1, ARRAY['nppes']::varchar[], 1, 1,
            'practice', 2001, 'FARTHER SYNTHETIC WAY',
            'TEST CITY', 'TS', 'TS', '00001', '00001', 'US',
            'street', 42.0, -83.1
        ),
        (
            'exact-zip', :exact_zip_npi,
            md5('exact-zip-address')::uuid,
            md5('exact-zip-premise')::uuid,
            1, ARRAY['nppes']::varchar[], 1, 1,
            'practice', 2002, 'EXACT ZIP SYNTHETIC WAY',
            'TEST CITY', 'TS', 'TS', '00001', '00001', 'US',
            'street', NULL, NULL
        )"""
_INSERT_ADDRESS_LINK_SQL = """INSERT INTO {schema}.npi_address (
        npi, address_key, type, checksum, date_added
    )
    SELECT npi, address_key, type, checksum, '2026-08-31'::date
      FROM {schema}.entity_address_unified"""


async def _prepare_rows(database, schema: str) -> tuple[int, int]:
    fixture_npis = _invalid_fixture_npis(137)
    projection_negative_npis = fixture_npis[:69]
    exact_negative_npis = fixture_npis[69:135]
    exact_npis = fixture_npis[135:]
    for statement in _CREATE_SQL:
        await database.status(statement.format(schema=schema))
    await database.status(
        _INSERT_NPI_SQL.format(schema=schema),
        projection_negative_npis=list(projection_negative_npis),
        projection_positive_npis=[*exact_negative_npis, *exact_npis],
    )
    await database.status(_INSERT_SCOPE_SQL.format(schema=schema))
    await database.status(
        _INSERT_TAXONOMY_SQL.format(schema=schema), exact_npis=list(exact_npis)
    )
    await database.status(
        _INSERT_PROJECTION_NEGATIVE_SQL.format(schema=schema),
        projection_negative_npis=list(projection_negative_npis),
    )
    await database.status(
        _INSERT_EXACT_NEGATIVE_SQL.format(schema=schema),
        exact_negative_npis=list(exact_negative_npis),
    )
    await database.status(
        _INSERT_EXACT_SQL.format(schema=schema),
        exact_geo_npi=exact_npis[0],
        exact_zip_npi=exact_npis[1],
    )
    await database.status(_INSERT_ADDRESS_LINK_SQL.format(schema=schema))
    return exact_npis


def _request_by_field() -> dict[str, object]:
    return {
        "taxonomy_codes": ["207Q00000X"],
        "zip5": "00001",
        "lat": 42.0,
        "long": -83.0,
        "radius_miles": 30,
        "order_by": "distance",
        "order": "asc",
    }


def _schema_location_executor(schema: str):
    execute_location_sql = serving._execute_membership_location_sql

    async def execute(session, query_context, location_sql, *, offset):
        return await execute_location_sql(
            session,
            query_context,
            _schema_sql(location_sql, schema),
            offset=offset,
        )

    return execute


def _recording_location_reader(location_probes: list[tuple[int, list[dict]]]):
    membership_location_rows = serving._membership_location_rows

    async def read(*args, limit, **kwargs):
        locations = await membership_location_rows(*args, limit=limit, **kwargs)
        location_probes.append((limit, locations))
        return locations

    return read


async def _exact_memberships(_session, _tables, npis, _request, _state):
    return {int(npi): (10,) for npi in npis}


async def _classify_exact_sets(_session, _tables, memberships, _request, state):
    if memberships:
        state.code_sets.add(10)


def _assert_scan_result(exact_npis, location_probes, candidates) -> None:
    assert [limit for limit, _locations in location_probes] == [1, 4]
    first_location = location_probes[0][1][0]
    assert first_location["npi"] is None
    assert first_location["_ptg_source_exhausted"] is False
    assert first_location["_ptg_probe_empty"] is True
    assert [location["npi"] for location in candidates.location_rows] == list(
        exact_npis
    )
    assert candidates.location_rows[0]["distance_miles"] is not None
    assert candidates.location_rows[1]["distance_miles"] is None
    assert all(
        location["_ptg_source_exhausted"] for location in candidates.location_rows
    )
    assert candidates.provider_set_keys_by_npi == {
        exact_npis[0]: {10},
        exact_npis[1]: {10},
    }
    assert candidates.taxonomy_filtered is True


def _configure_serving(monkeypatch, schema: str, location_probes) -> None:
    provenance_sql = _schema_sql(serving._ADDRESS_PROVENANCE_SQL, schema)
    monkeypatch.setenv("HLTHPRT_NPI_SEARCH_TAXONOMY_PROJECTION_ENABLED", "1")
    monkeypatch.setattr(serving, "PTG2_SCHEMA", schema)
    monkeypatch.setattr(serving, "_ADDRESS_PROVENANCE_SQL", provenance_sql)
    monkeypatch.setattr(serving, "_MEMBERSHIP_EXACT_NPI_SCOPE_LIMIT", 1)
    monkeypatch.setattr(
        serving,
        "_ptg2_npi_scope_table",
        lambda *_args, **_kwargs: f"{schema}.ptg2_v3_npi_scope",
    )
    monkeypatch.setattr(
        serving,
        "_membership_address_table_for_request",
        AsyncMock(return_value=f"{schema}.entity_address_unified"),
    )
    monkeypatch.setattr(
        serving,
        "_execute_membership_location_sql",
        _schema_location_executor(schema),
    )
    monkeypatch.setattr(
        serving,
        "_membership_location_rows",
        _recording_location_reader(location_probes),
    )
    monkeypatch.setattr(serving, "_local_v4_memberships", _exact_memberships)
    monkeypatch.setattr(
        serving,
        "_classify_local_code_sets",
        _classify_exact_sets,
    )


@pytest.mark.asyncio
async def test_coarse_taxonomy_knn_executes_prefix_and_zip_recovery(monkeypatch):
    async with _temporary_schema() as (database, schema):
        await _insert_spatial_reference_rows(database, schema)
        exact_npis = await _prepare_rows(database, schema)
        location_probes = []
        _configure_serving(monkeypatch, schema, location_probes)
        request_by_field = _request_by_field()
        tables = strict_v3_tables()
        async with database.transaction() as session:
            assert await serving._bounded_exact_distance_npis(
                session, tables, request_by_field, 20, 1
            ) == (None, True)
            candidates = await serving._scan_local_distance_graph(
                session,
                tables,
                request_by_field,
                serving._LocalDistanceGraphRequest(
                    2,
                    [{"code_key": 7}],
                    serving._v4_geo_rate_forward_limits(_local_distance_tables()),
                ),
                1,
                4,
                candidate_npis=None,
                coarse_taxonomy_knn=True,
            )

    assert candidates is not None
    _assert_scan_result(exact_npis, location_probes, candidates)
