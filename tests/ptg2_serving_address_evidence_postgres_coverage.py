# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Executable PostGIS coverage for rate-scoped aggregate geo witnesses."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from api import ptg2_serving as serving
from tests.ptg2_serving_address_evidence_postgres_support import (
    _schema_sql,
    _temporary_schema,
)
from tests.ptg2_serving_coverage_paydown_support import strict_v3_tables


def _is_valid_npi_check_digit(npi_text: str) -> bool:
    npi_digits = f"80840{npi_text}"
    checksum_total = 0
    for digit_index, digit_text in enumerate(npi_digits):
        digit = int(digit_text)
        if digit_index % 2 == len(npi_digits) % 2:
            digit *= 2
            if digit > 9:
                digit -= 9
        checksum_total += digit
    return checksum_total % 10 == 0


def _invalid_fixture_npis(count: int) -> tuple[int, ...]:
    invalid_npis: list[int] = []
    for ordinal in range(count):
        nine_digit_prefix = f"1992{ordinal:05d}"
        invalid_text = next(
            f"{nine_digit_prefix}{check_digit}"
            for check_digit in range(10)
            if not _is_valid_npi_check_digit(f"{nine_digit_prefix}{check_digit}")
        )
        invalid_npis.append(int(invalid_text))
    return tuple(invalid_npis)


async def _insert_spatial_reference_rows(database, schema: str) -> None:
    """Create one coherent synthetic ZIP/state/polygon reference record."""

    await database.status(
        f"""
        INSERT INTO {schema}.geo_zip_lookup (zip_code, state, state_name)
        VALUES ('48201', 'MI', 'MICHIGAN')
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.zip_state (zip, stusps)
        VALUES ('48201', 'MI')
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.zcta5 (zcta5ce, the_geom)
        VALUES (
            '48201',
            ST_GeomFromText(
                'POLYGON((-83.20 42.20,-82.90 42.20,-82.90 42.50,-83.20 42.50,-83.20 42.20))',
                4269
            )
        )
        """
    )


async def _insert_candidate_addresses(database, schema: str, npis: tuple[int, ...]):
    """Insert the bounded synthetic candidate and evidence population."""

    await _insert_spatial_reference_rows(database, schema)
    await database.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_npi_scope (snapshot_key, npi)
        SELECT 41, candidate.npi
          FROM UNNEST(CAST(:npis AS bigint[])) AS candidate(npi)
        """,
        npis=list(npis),
    )
    await database.status(
        f"""
        WITH candidates AS (
            SELECT candidate.npi, candidate.ordinality
              FROM UNNEST(CAST(:npis AS bigint[])) WITH ORDINALITY
                   AS candidate(npi, ordinality)
        )
        INSERT INTO {schema}.entity_address_unified (
            location_key, npi, address_key, premise_key,
            address_source_mask, address_sources, source_count, source_mask,
            type, checksum, first_line, city_name, state_name, state_code,
            postal_code, zip5, country_code, address_precision, lat, long
        )
        SELECT
            'geo-rate-' || candidates.ordinality,
            candidates.npi,
            md5(candidates.npi::text)::uuid,
            md5('premise-' || candidates.npi::text)::uuid,
            CASE WHEN candidates.ordinality IN (1, 2501, 5001) THEN 1 ELSE 0 END,
            CASE
                WHEN candidates.ordinality IN (1, 2501, 5001)
                THEN ARRAY['nppes']::varchar[]
                ELSE ARRAY[]::varchar[]
            END,
            CASE WHEN candidates.ordinality IN (1, 2501, 5001) THEN 1 ELSE 0 END,
            CASE WHEN candidates.ordinality IN (1, 2501, 5001) THEN 1 ELSE 0 END,
            'practice', candidates.ordinality,
            'TEST ADDRESS', 'TEST CITY', 'MI', 'MI', '48201', '48201', 'US',
            'street', 42.3314, -83.0458
          FROM candidates
        """,
        npis=list(npis),
    )
    await database.status(
        f"""
        INSERT INTO {schema}.npi_address (
            npi, address_key, type, checksum, date_added
        )
        SELECT candidate.npi,
               md5(candidate.npi::text)::uuid,
               'practice', candidate.ordinality, '2026-07-31'::date
          FROM UNNEST(CAST(:npis AS bigint[])) WITH ORDINALITY
               AS candidate(npi, ordinality)
         WHERE candidate.ordinality IN (1, 2501, 5001)
        """,
        npis=list(npis),
    )


async def _index_candidate_addresses(database, schema: str) -> None:
    for index_sql in (
        f"CREATE INDEX ON {schema}.ptg2_v3_npi_scope (snapshot_key, npi)",
        f"CREATE INDEX ON {schema}.entity_address_unified (npi, location_key)",
        f"CREATE INDEX ON {schema}.npi_address (npi, address_key)",
    ):
        await database.status(index_sql)
    for table_name in (
        "ptg2_v3_npi_scope",
        "entity_address_unified",
        "npi_address",
    ):
        await database.status(f"ANALYZE {schema}.{table_name}")


def _patch_spatial_policy_dependencies(monkeypatch, schema: str) -> None:
    """Route canonical capability and geometry reads to the test schema."""

    geo_capability = serving.is_provider_address_geo_capability_available

    async def schema_geo_capability(session, *, schema_name, **_kwargs):
        return await geo_capability(
            session,
            schema_name=schema_name,
            reference_schema=schema,
        )

    monkeypatch.setattr(
        serving,
        "is_provider_address_geo_capability_available",
        schema_geo_capability,
    )
    location_filter_sql = serving.provider_address_location_filter_sql

    def schema_location_filter_sql(*args, **kwargs):
        spatial_filter = location_filter_sql(*args, **kwargs)
        return (
            _schema_sql(spatial_filter, schema)
            if spatial_filter is not None
            else None
        )

    monkeypatch.setattr(
        serving,
        "provider_address_location_filter_sql",
        schema_location_filter_sql,
    )


def _patch_candidate_geo_dependencies(monkeypatch, schema: str):
    """Route the production geo read to isolated test tables."""

    provenance_sql = _schema_sql(serving._ADDRESS_PROVENANCE_SQL, schema)
    monkeypatch.setattr(serving, "PTG2_SCHEMA", schema)
    monkeypatch.setattr(serving, "_ADDRESS_PROVENANCE_SQL", provenance_sql)
    monkeypatch.setattr(
        serving,
        "_ptg2_address_serving_table",
        AsyncMock(return_value=f"{schema}.entity_address_unified"),
    )
    monkeypatch.setattr(
        serving,
        "_ptg2_npi_scope_table",
        lambda *_args, **_kwargs: f"{schema}.ptg2_v3_npi_scope",
    )
    _patch_spatial_policy_dependencies(monkeypatch, schema)
    provider_enrichment = AsyncMock()
    monkeypatch.setattr(
        serving,
        "_enriched_provider_rows_for_npis",
        provider_enrichment,
    )
    captured_location_rows: list[dict[str, object]] = []
    membership_rows = serving._membership_location_rows

    async def capture_membership_rows(*args, **kwargs):
        location_rows = await membership_rows(*args, **kwargs)
        if location_rows:
            captured_location_rows.extend(location_rows)
        return location_rows

    monkeypatch.setattr(
        serving,
        "_membership_location_rows",
        capture_membership_rows,
    )
    return captured_location_rows, provider_enrichment


@pytest.mark.asyncio
async def test_provider_set_geo_candidate_scope_executes_beyond_old_prefix(
    monkeypatch,
):
    """Prove 5,001 candidates remain bounded and source-assured in PostGIS."""

    npis = _invalid_fixture_npis(5001)
    assert len(npis) == len(set(npis))
    assert all(not _is_valid_npi_check_digit(str(npi)) for npi in npis)

    async with _temporary_schema() as (database, schema):
        await _insert_candidate_addresses(database, schema, npis)
        await _index_candidate_addresses(database, schema)
        captured_location_rows, provider_enrichment = _patch_candidate_geo_dependencies(
            monkeypatch, schema
        )
        npis_by_set = {
            "set-a": npis[:2500],
            "set-b": npis[2500:4999],
            "set-c": npis[4999:],
        }
        async with database.transaction() as session:
            await session.execute(text("SET LOCAL statement_timeout = '5s'"))
            covered_sets = await serving._geo_eligible_provider_sets(
                session,
                strict_v3_tables(),
                {
                    "lat": "42.3314",
                    "long": "-83.0458",
                    "radius_miles": "30",
                },
                npis_by_set,
            )

        assert covered_sets == {"set-a", "set-b", "set-c"}
        assert {
            location_record["npi"] for location_record in captured_location_rows
        } == {
            npis[0],
            npis[2500],
            npis[5000],
        }
        for location_record in captured_location_rows:
            address_payload = json.loads(str(location_record["address_payload"]))
            assert "address_provenance" not in address_payload
            assert "geo_evidence_level" not in address_payload
        provider_enrichment.assert_not_awaited()
