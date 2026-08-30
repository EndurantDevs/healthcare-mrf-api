# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""NPI-only source-assured lookup for aggregate reverse geo selection."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_address_evidence_postgres_geo import (
    _insert_cms_identity_locations,
    _insert_cms_source_anchors,
    _insert_spatial_reference_rows,
)
from tests.ptg2_serving_address_evidence_postgres_spatial import (
    _insert_spatial_candidate_rows,
)
from tests.ptg2_serving_address_evidence_postgres_support import (
    _schema_sql,
    _temporary_schema,
)
from tests.ptg2_serving_coverage_paydown_support import strict_v3_tables


def _location_query(schema: str) -> serving._MembershipLocationQuery:
    request_arg_map = {
        "zip5": "00001",
        "lat": 42.0,
        "long": -83.0,
        "radius_miles": 30,
    }
    query_parameter_map: dict[str, object] = {
        "limit": 10,
        "offset": 0,
        "shared_snapshot_key": 41,
    }
    address_filter_sql, distance_sql = serving._membership_filter_sql(
        request_arg_map,
        candidate_npis=None,
        uses_unified_addresses=True,
        address_zip5_sql=serving._ptg2_address_zip5_sql("addr", unified=True),
        parameter_map=query_parameter_map,
        literal_service_address_types=True,
    )
    address_filter_sql = _schema_sql(address_filter_sql, schema)
    return serving._MembershipLocationQuery(
        address_table=f"{schema}.entity_address_unified",
        npi_scope_table=f"{schema}.ptg2_v3_npi_scope",
        filter_sql=(
            "npi_scope.snapshot_key = :shared_snapshot_key "
            f"AND ({address_filter_sql})"
        ),
        parameter_map=query_parameter_map,
        distance_sql=distance_sql,
        knn_order_sql=None,
        address_assurance_sql=serving._membership_address_assurance_sql(
            request_arg_map,
            True,
        ),
        address_filter_sql=address_filter_sql,
    )


async def _insert_cms_spatial_candidate(database, schema: str) -> None:
    await _insert_cms_identity_locations(database, schema)
    await _insert_cms_source_anchors(database, schema)
    await database.status(
        f"""
        UPDATE {schema}.entity_address_unified
           SET city_name = 'TEST CITY',
               state_name = 'TS',
               state_code = 'TS',
               postal_code = '00001',
               zip5 = '00001',
               country_code = 'US',
               address_precision = 'street',
               lat = 42.0,
               long = -83.0
         WHERE location_key = 'cms-anchored'
        """
    )


async def _insert_parity_edge_rows(database, schema: str) -> None:
    """Add one duplicate assured address and one incomplete issuer witness."""

    await database.status(
        f"INSERT INTO {schema}.ptg2_v3_npi_scope (snapshot_key, npi) "
        "VALUES (41, 1990000304)"
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
                'duplicate-nearer', 1990000205,
                '00000000-0000-0000-0000-000000000206',
                '10000000-0000-0000-0000-000000000206',
                1, ARRAY['nppes']::varchar[], 1, 1,
                'practice', 206, '206 SYNTHETIC WAY',
                'TEST CITY', 'TS', 'TS', '00001', '00001', 'US',
                'street', 42.0, -83.05
            ),
            (
                'single-issuer', 1990000304,
                '00000000-0000-0000-0000-000000000304',
                '10000000-0000-0000-0000-000000000304',
                2, ARRAY['mrf']::varchar[], 1, 2,
                'practice', 304, '304 SYNTHETIC WAY',
                'TEST CITY', 'TS', 'TS', '00001', '00001', 'US',
                'street', 42.0, -83.0
            )
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.npi_address (npi, address_key, type, checksum, date_added)
        VALUES (1990000205,
            '00000000-0000-0000-0000-000000000206',
            'practice', 206, '2026-07-31'
        )
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.mrf_address (
            npi, address_key, type, checksum, date_added,
            source_import_ids, source_import_dates, source_issuer_names, source_urls
        ) VALUES (
            1990000304,
            '00000000-0000-0000-0000-000000000304',
            'practice', 304, '2026-07-31',
            ARRAY['single-issuer-version']::varchar[],
            ARRAY['2026-07-31'::date],
            ARRAY['SYNTHETIC ISSUER A']::varchar[],
            ARRAY['https://example.test/single-issuer']::varchar[]
        )
        """
    )


def test_npi_only_sql_keeps_the_unique_scope_probe_correlated():
    query = _location_query("mrf")

    statement = serving._membership_npi_sql(query)

    assert "npi_scope.snapshot_key = :shared_snapshot_key" in statement
    assert "npi_scope.npi = addr.npi" in statement
    assert "OFFSET 0" in statement
    assert "PARTITION BY addr.npi" in statement
    assert "CASE addr.type WHEN 'practice' THEN 0" in statement
    assert "addr.checksum" in statement
    assert "addr.location_key" in statement
    assert "ORDER BY distance_miles ASC NULLS LAST, npi" in statement
    assert statement.index("ORDER BY distance_miles") < statement.index("LIMIT :limit")
    assert "jsonb_build_" not in statement
    assert "address_payload" not in statement


@pytest.mark.asyncio
async def test_reverse_geo_requests_only_npis(monkeypatch):
    npi_rows = AsyncMock(
        return_value=[
            {"npi": 202, "_ptg_source_exhausted": True},
            {"npi": 101},
            {"npi": 202},
        ]
    )
    monkeypatch.setattr(serving, "_membership_npi_rows", npi_rows)

    result = await serving._bounded_reverse_geo_npis(
        object(),
        strict_v3_tables(),
        {"zip5": "00001"},
        100_000,
    )

    assert result == ((101, 202), True)
    npi_rows.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query_updates",
    [
        {"knn_order_sql": "addr.location <-> :requested_location"},
        {"address_table": "mrf.npi_address"},
        {"address_assurance_sql": "TRUE"},
        {"address_filter_sql": None},
        {"taxonomy_index_sql": "addr.taxonomy_array && ARRAY[1]"},
    ],
)
async def test_npi_reader_falls_back_when_compact_lookup_is_unsafe(
    monkeypatch,
    query_updates,
):
    query = replace(_location_query("mrf"), **query_updates)
    full_reader = AsyncMock(return_value=[{"npi": 101}])
    monkeypatch.setattr(
        serving,
        "_membership_location_query",
        AsyncMock(return_value=query),
    )
    monkeypatch.setattr(serving, "_membership_location_rows", full_reader)

    result = await serving._membership_npi_rows(
        object(),
        strict_v3_tables(),
        {"lat": 42.0, "long": -83.0},
        candidate_npis=None,
        limit=10,
    )

    assert result == [{"npi": 101}]
    full_reader.assert_awaited_once()


@pytest.mark.asyncio
async def test_npi_reader_keeps_compact_taxonomy_lookup_for_bounded_npis(monkeypatch):
    query = replace(
        _location_query("mrf"),
        taxonomy_index_sql="addr.taxonomy_array && ARRAY[1]",
    )
    compact_reader = AsyncMock(return_value=[{"npi": 101}])
    full_reader = AsyncMock()
    monkeypatch.setattr(
        serving,
        "_membership_location_query",
        AsyncMock(return_value=query),
    )
    monkeypatch.setattr(serving, "_execute_membership_location_sql", compact_reader)
    monkeypatch.setattr(serving, "_is_relation_available", AsyncMock(return_value=True))
    monkeypatch.setattr(serving, "_membership_location_rows", full_reader)

    result = await serving._membership_npi_rows(
        object(),
        strict_v3_tables(),
        {"lat": 42.0, "long": -83.0},
        candidate_npis=(101,),
        limit=10,
    )

    assert result == [{"npi": 101}]
    compact_reader.assert_awaited_once()
    full_reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_npi_reader_short_circuits_empty_and_unavailable_queries(monkeypatch):
    query_builder = AsyncMock(return_value=None)
    monkeypatch.setattr(serving, "_membership_location_query", query_builder)

    assert await serving._membership_npi_rows(
        object(),
        strict_v3_tables(),
        {},
        candidate_npis=(),
        limit=10,
    ) == []
    query_builder.assert_not_awaited()

    assert await serving._membership_npi_rows(
        object(),
        strict_v3_tables(),
        {},
        candidate_npis=None,
        limit=10,
    ) is None
    query_builder.assert_awaited_once()

    query_builder.return_value = _location_query("mrf")
    query_executor = AsyncMock(return_value=[])
    evidence_probe = AsyncMock()
    monkeypatch.setattr(serving, "_execute_membership_location_sql", query_executor)
    monkeypatch.setattr(serving, "_is_relation_available", evidence_probe)

    assert await serving._membership_npi_rows(
        object(),
        strict_v3_tables(),
        {"zip5": "00001"},
        candidate_npis=None,
        limit=10,
    ) == []
    query_executor.assert_awaited_once()
    evidence_probe.assert_not_awaited()

    query_executor.return_value = [{"npi": 101}]
    evidence_probe.return_value = False
    assert await serving._membership_npi_rows(
        object(),
        strict_v3_tables(),
        {"zip5": "00001"},
        candidate_npis=(101,),
        limit=10,
    ) == []
    evidence_probe.assert_awaited_once()


async def _read_full_and_compact_rows(
    session,
    serving_tables,
    request_arg_map,
    *,
    limit=10,
):
    full_location_rows = await serving._membership_location_rows(
        session,
        serving_tables,
        request_arg_map,
        candidate_npis=None,
        limit=limit,
    )
    compact_npi_rows = await serving._membership_npi_rows(
        session,
        serving_tables,
        request_arg_map,
        candidate_npis=None,
        limit=limit,
    )
    return full_location_rows, compact_npi_rows


@pytest.mark.asyncio
async def test_npi_only_reader_preserves_assurance_exhaustion_and_fail_closed(
    monkeypatch,
):
    """Match full-reader assurance, ordering, exhaustion, and corruption behavior."""

    async with _temporary_schema() as (database, schema):
        await _insert_spatial_reference_rows(database, schema)
        await _insert_spatial_candidate_rows(database, schema)
        await _insert_cms_spatial_candidate(database, schema)
        await _insert_parity_edge_rows(database, schema)
        provenance_sql = _schema_sql(serving._ADDRESS_PROVENANCE_SQL, schema)
        monkeypatch.setattr(serving, "PTG2_SCHEMA", schema)
        monkeypatch.setattr(serving, "_ADDRESS_PROVENANCE_SQL", provenance_sql)
        monkeypatch.setattr(
            serving,
            "_membership_location_query",
            AsyncMock(side_effect=lambda *_args, **_kwargs: _location_query(schema)),
        )
        serving_tables = strict_v3_tables()
        request_arg_map = {
            "zip5": "00001", "lat": 42.0, "long": -83.0, "radius_miles": 30
        }

        async with database.transaction() as session:
            full_rows, npi_rows = await _read_full_and_compact_rows(
                session,
                serving_tables,
                request_arg_map,
            )

            expected_npis = (1990000031, 1990000205, 1990000213)
            full_npis = tuple(int(location_row["npi"]) for location_row in full_rows)
            compact_npis = tuple(int(npi_row["npi"]) for npi_row in npi_rows)
            assert full_npis == expected_npis
            assert compact_npis == expected_npis
            assert len(set(compact_npis)) == len(compact_npis)
            assert [npi_row["distance_miles"] for npi_row in npi_rows] == pytest.approx(
                [location_row["distance_miles"] for location_row in full_rows]
            )
            assert serving._is_graph_location_source_exhausted(full_rows, 10)
            assert serving._is_graph_location_source_exhausted(npi_rows, 10)

            await session.execute(
                serving.text(f"DROP TABLE {schema}.entity_address_evidence")
            )
            missing_full_rows, missing_npi_rows = await _read_full_and_compact_rows(
                session,
                serving_tables,
                request_arg_map,
                limit=3,
            )
            expected_missing_evidence_rows = [
                {"_ptg_probe_empty": True, "_ptg_source_exhausted": False}
            ]
            assert missing_full_rows == expected_missing_evidence_rows
            assert missing_npi_rows == expected_missing_evidence_rows
            assert not serving._is_graph_location_source_exhausted(
                missing_npi_rows,
                3,
            )
