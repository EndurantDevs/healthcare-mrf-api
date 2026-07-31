# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import re
import uuid

import pytest

from api import ptg2_serving as serving
from db.connection import Database


def _is_valid_npi_check_digit(value: str) -> bool:
    payload = f"80840{value}"
    total = 0
    for index, digit_text in enumerate(payload):
        digit = int(digit_text)
        if index % 2 == len(payload) % 2:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return total % 10 == 0


def test_fixture_npis_are_deliberately_checksum_invalid():
    source_text = Path(__file__).read_text(encoding="utf-8")
    fixture_npis = set(re.findall(r"\b[1-9][0-9]{9}\b", source_text))

    assert fixture_npis
    assert all(not _is_valid_npi_check_digit(npi) for npi in fixture_npis)


def _require_disposable_postgres() -> None:
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database_name.lower():
        pytest.skip("address evidence integration tests require a disposable test database")


@asynccontextmanager
async def _temporary_schema():
    _require_disposable_postgres()
    database = Database()
    await database.connect()
    schema = f"ptg2_address_evidence_{uuid.uuid4().hex[:12]}"
    await database.status(f"CREATE SCHEMA {schema};")
    try:
        await _create_tables(database, schema)
        yield database, schema
    finally:
        await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()


async def _create_tables(database: Database, schema: str) -> None:
    statements = (
        "CREATE EXTENSION IF NOT EXISTS postgis",
        f"""
        CREATE TABLE {schema}.entity_address_unified (
            location_key varchar PRIMARY KEY,
            npi bigint,
            address_key uuid,
            premise_key uuid,
            address_source_mask bigint NOT NULL DEFAULT 0,
            address_sources varchar[] NOT NULL DEFAULT '{{}}',
            source_record_ids varchar[] NOT NULL DEFAULT '{{}}',
            source_count int NOT NULL DEFAULT 0,
            multi_source_confirmed boolean NOT NULL DEFAULT false,
            source_mask bigint NOT NULL DEFAULT 0,
            location_confidence_id smallint NOT NULL DEFAULT 0,
            address_precision varchar NOT NULL DEFAULT 'street',
            zip5 varchar,
            state_name varchar,
            city_name varchar,
            postal_code varchar,
            type varchar,
            checksum bigint,
            telephone_number varchar,
            fax_number varchar,
            phone_number varchar,
            phone_extension varchar,
            fax_number_digits varchar,
            fax_extension varchar,
            first_line varchar,
            second_line varchar,
            country_code varchar,
            lat numeric,
            long numeric,
            updated_at timestamptz,
            last_seen_at timestamptz
        )
        """,
        f"""
        CREATE INDEX entity_address_unified_test_geo_idx
            ON {schema}.entity_address_unified
         USING gist (
            Geography(
                ST_MakePoint(
                    (long)::double precision,
                    (lat)::double precision
                )
            )
         )
         WHERE type IN ('primary', 'secondary', 'practice', 'site')
           AND COALESCE(address_precision, '') <> 'city_zip'
           AND lat IS NOT NULL
           AND long IS NOT NULL
        """,
        f"""
        CREATE TABLE {schema}.entity_address_evidence (
            evidence_id bigint PRIMARY KEY,
            location_key varchar NOT NULL,
            address_key uuid,
            premise_key uuid,
            npi bigint,
            source_id smallint NOT NULL,
            source_record_key varchar,
            source_run_id varchar NOT NULL,
            source_snapshot_id varchar,
            observed_at timestamptz,
            last_seen_at timestamptz,
            retired_at timestamptz
        )
        """,
        f"""
        CREATE TABLE {schema}.mrf_address (
            npi bigint,
            address_key uuid,
            type varchar,
            checksum bigint,
            date_added date,
            source_import_ids varchar[],
            source_import_dates date[],
            source_issuer_names varchar[],
            source_urls varchar[]
        )
        """,
        f"""
        CREATE TABLE {schema}.npi_address (
            npi bigint,
            address_key uuid,
            type varchar,
            checksum bigint,
            date_added date
        )
        """,
        f"""
        CREATE TABLE {schema}.doctor_clinician_address (
            npi bigint,
            address_key uuid,
            address_checksum bigint,
            updated_at timestamp
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v3_npi_scope (
            snapshot_key bigint NOT NULL,
            npi bigint NOT NULL
        )
        """,
    )
    for statement in statements:
        await database.status(statement)


def _schema_sql(sql: str, schema: str) -> str:
    for table_name in (
        "doctor_clinician_address",
        "entity_address_evidence",
        "entity_address_unified",
        "mrf_address",
        "npi_address",
        "ptg2_v3_npi_scope",
    ):
        sql = sql.replace(
            f"{serving.PTG2_SCHEMA}.{table_name}",
            f"{schema}.{table_name}",
        )
    return sql


def _complete_lineage(entry: dict[str, object]) -> bool:
    return all(
        entry.get(field_name) not in (None, "", [])
        for field_name in (
            "dataset_id",
            "source_record_id",
            "record_version_id",
            "retrieved_at",
        )
    )


@pytest.mark.asyncio
async def test_admitted_mrf_recovers_specific_lineage_without_fabrication():
    async with _temporary_schema() as (database, schema):
        await database.status(
            f"""
            INSERT INTO {schema}.entity_address_unified (
                location_key, npi, address_key, premise_key,
                address_source_mask, address_sources, source_record_ids
            ) VALUES (
                'mrf-admitted', 1990000015,
                '00000000-0000-0000-0000-000000000001',
                '10000000-0000-0000-0000-000000000001',
                0, ARRAY[]::varchar[], ARRAY['nppes:foreign']::varchar[]
            )
            """
        )
        await database.status(
            f"""
            INSERT INTO {schema}.entity_address_evidence (
                evidence_id, location_key, address_key, premise_key, npi,
                source_id, source_record_key, source_run_id,
                observed_at, last_seen_at
            ) VALUES
                (
                    1, 'mrf-admitted',
                    '00000000-0000-0000-0000-000000000001',
                    '10000000-0000-0000-0000-000000000001',
                    1990000015, 0, 'mrf-admitted', '20260731', NULL, NULL
                ),
                (
                    2, 'mrf-admitted', NULL, NULL, NULL,
                    2, 'wrong:mrf-record', '20260731', NULL, NULL
                ),
                (
                    3, 'mrf-admitted',
                    '00000000-0000-0000-0000-000000000001',
                    '10000000-0000-0000-0000-000000000001',
                    1990000015, 1, 'nppes:stale', '20260731', NULL, NULL
                )
            """
        )
        await database.status(
            f"""
            INSERT INTO {schema}.mrf_address (
                npi, address_key, type, checksum, date_added,
                source_import_ids, source_import_dates,
                source_issuer_names, source_urls
            ) VALUES
                (
                    1990000015,
                    '00000000-0000-0000-0000-000000000001',
                    'practice', 11, NULL,
                    ARRAY['qualifying-v1']::varchar[],
                    ARRAY['2026-07-29'::date],
                    ARRAY['Issuer A', 'Issuer B']::varchar[],
                    ARRAY['https://example.test/qualifying']::varchar[]
                ),
                (
                    1990000015,
                    '00000000-0000-0000-0000-000000000001',
                    'practice', 10, NULL,
                    ARRAY[]::varchar[], ARRAY[]::date[],
                    ARRAY['Issuer A', 'Issuer B']::varchar[],
                    ARRAY['https://example.test/incomplete']::varchar[]
                ),
                (
                    1990000015,
                    '00000000-0000-0000-0000-000000000001',
                    'practice', 12, '2026-07-30',
                    ARRAY['newer-single-v2']::varchar[],
                    ARRAY['2026-07-30'::date],
                    ARRAY['Issuer A']::varchar[],
                    ARRAY['https://example.test/newer']::varchar[]
                )
            """
        )

        rows = await database.all(
            _schema_sql(serving._ADDRESS_PROVENANCE_SQL, schema),
            location_keys=["mrf-admitted"],
            admitted_source_ids=[2],
        )

        assert len(rows) == 1
        row = rows[0]._mapping
        assert row["source_id"] == 2
        assert row["source_record_key"] == "mrf:1990000015:practice:11"
        assert row["source_import_ids"] == ["qualifying-v1"]
        assert row["source_issuer_names"] == ["Issuer A", "Issuer B"]
        entry = serving._index_address_provenance(rows)["mrf-admitted"][0]
        assert _complete_lineage(entry)
        assert entry["record_version_id"] == "qualifying-v1"
        assert entry["retrieved_at"] == "2026-07-29"
        assert "mrf-admitted" not in str(entry["source_record_id"])
        assert "mrf-admitted" not in str(entry["record_version_id"])


@pytest.mark.asyncio
async def test_specific_evidence_excludes_retired_and_blank_rows():
    async with _temporary_schema() as (database, schema):
        await database.status(
            f"""
            INSERT INTO {schema}.entity_address_unified (
                location_key, npi, address_key, premise_key, address_sources
            ) VALUES (
                'active-lineage', 1990000023,
                '00000000-0000-0000-0000-000000000002',
                '10000000-0000-0000-0000-000000000002',
                ARRAY['provider_directory_fhir']::varchar[]
            )
            """
        )
        await database.status(
            f"""
            INSERT INTO {schema}.entity_address_evidence (
                evidence_id, location_key, source_id, source_record_key,
                source_run_id, observed_at, last_seen_at, retired_at
            ) VALUES
                (
                    10, 'active-lineage', 8,
                    'provider_directory_fhir:active', 'fhir-v1',
                    '2026-07-20T00:00:00Z', '2026-07-20T00:00:00Z', NULL
                ),
                (
                    11, 'active-lineage', 8,
                    'provider_directory_fhir:retired', 'fhir-v2',
                    '2026-07-30T00:00:00Z', '2026-07-30T00:00:00Z',
                    '2026-07-31T00:00:00Z'
                ),
                (
                    12, 'active-lineage', 8,
                    'provider_directory_fhir:blank-version', '   ',
                    '2026-07-31T00:00:00Z', '2026-07-31T00:00:00Z', NULL
                ),
                (
                    13, 'active-lineage', 8,
                    'providerXdirectoryYfhir:wildcard', 'fhir-v3',
                    '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z', NULL
                )
            """
        )

        rows = await database.all(
            _schema_sql(serving._ADDRESS_PROVENANCE_SQL, schema),
            location_keys=["active-lineage"],
            admitted_source_ids=[0],
        )

        assert len(rows) == 1
        row = rows[0]._mapping
        assert row["source_id"] == 8
        assert row["source_record_key"] == "provider_directory_fhir:active"
        assert row["source_run_id"] == "fhir-v1"


@pytest.mark.asyncio
async def test_stored_compact_run_date_is_complete_without_timestamps():
    async with _temporary_schema() as (database, schema):
        await database.status(
            f"""
            INSERT INTO {schema}.entity_address_unified (
                location_key, npi, address_key, premise_key, address_sources
            ) VALUES (
                'compact-run-lineage', 1990000122,
                '00000000-0000-0000-0000-000000000032',
                '10000000-0000-0000-0000-000000000032',
                ARRAY['provider_directory_fhir']::varchar[]
            )
            """
        )
        await database.status(
            f"""
            INSERT INTO {schema}.entity_address_evidence (
                evidence_id, location_key, source_id, source_record_key,
                source_run_id, observed_at, last_seen_at
            ) VALUES
                (
                    14, 'compact-run-lineage', 8,
                    'provider_directory_fhir:valid-run-only', '20260730',
                    NULL, NULL
                ),
                (
                    15, 'compact-run-lineage', 8,
                    'provider_directory_fhir:invalid-newer-run', '20261399',
                    NULL, NULL
                )
            """
        )

        rows = await database.all(
            _schema_sql(serving._ADDRESS_PROVENANCE_SQL, schema),
            location_keys=["compact-run-lineage"],
            admitted_source_ids=[0],
        )

        assert len(rows) == 1
        row = rows[0]._mapping
        assert row["source_record_key"] == (
            "provider_directory_fhir:valid-run-only"
        )
        assert row["source_run_id"] == "20260730"
        entry = serving._index_address_provenance(rows)["compact-run-lineage"][0]
        assert entry["record_version_id"] == "20260730"
        assert entry["retrieved_at"] == "2026-07-30"
        assert _complete_lineage(entry)


@pytest.mark.asyncio
async def test_incomplete_specific_and_source_zero_are_not_public_lineage():
    async with _temporary_schema() as (database, schema):
        await database.status(
            f"""
            INSERT INTO {schema}.entity_address_unified (
                location_key, npi, address_key, premise_key, address_sources
            ) VALUES (
                'tuple-guard', 1990000064,
                '00000000-0000-0000-0000-000000000016',
                '10000000-0000-0000-0000-000000000016',
                ARRAY['provider_directory_fhir']::varchar[]
            )
            """
        )
        await database.status(
            f"""
            INSERT INTO {schema}.entity_address_evidence (
                evidence_id, location_key, source_id, source_record_key,
                source_run_id, observed_at, last_seen_at
            ) VALUES
                (20, 'tuple-guard', 0, 'tuple-guard', '20260731', NULL, NULL),
                (
                    21, 'tuple-guard', 8,
                    'provider_directory_fhir:record-only', '   ',
                    NULL, '2026-07-30T00:00:00Z'
                ),
                (
                    22, 'tuple-guard', 8, '   ', 'fhir-v2',
                    '2026-07-31T00:00:00Z', NULL
                )
            """
        )

        rows = await database.all(
            _schema_sql(serving._ADDRESS_PROVENANCE_SQL, schema),
            location_keys=["tuple-guard"],
            admitted_source_ids=[0],
        )

        assert rows == []
        assert serving._index_address_provenance(rows) == {}


@pytest.mark.asyncio
async def test_live_nppes_and_cms_use_source_specific_versions_as_whole_rows():
    async with _temporary_schema() as (database, schema):
        await database.status(
            f"""
            INSERT INTO {schema}.entity_address_unified (
                location_key, npi, address_key, premise_key, address_sources
            ) VALUES
                (
                    'nppes-live', 1990000072,
                    '00000000-0000-0000-0000-000000000017',
                    '10000000-0000-0000-0000-000000000017',
                    ARRAY[]::varchar[]
                ),
                (
                    'cms-live', 1990000080,
                    '00000000-0000-0000-0000-000000000018',
                    '10000000-0000-0000-0000-000000000018',
                    ARRAY[]::varchar[]
                )
            """
        )
        await database.status(
            f"""
            INSERT INTO {schema}.entity_address_evidence (
                evidence_id, location_key, source_id, source_record_key,
                source_run_id, observed_at, last_seen_at
            ) VALUES
                (30, 'nppes-live', 0, 'nppes-live', 'unified-20260731', NULL, NULL),
                (
                    31, 'nppes-live', 1, 'nppes:stale', 'stale-nppes-version',
                    '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z'
                ),
                (32, 'cms-live', 0, 'cms-live', 'unified-20260731', NULL, NULL),
                (
                    33, 'cms-live', 3, 'cms_doctors:stale', 'stale-cms-version',
                    '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z'
                )
            """
        )
        await database.status(
            f"""
            INSERT INTO {schema}.npi_address (
                npi, address_key, type, checksum, date_added
            ) VALUES (
                1990000072,
                '00000000-0000-0000-0000-000000000017',
                'practice', 101, '2026-07-15'
            )
            """
        )
        await database.status(
            f"""
            INSERT INTO {schema}.doctor_clinician_address (
                npi, address_key, address_checksum, updated_at
            ) VALUES (
                1990000080,
                '00000000-0000-0000-0000-000000000018',
                202, '2026-07-16 12:34:56'
            )
            """
        )

        rows = await database.all(
            _schema_sql(serving._ADDRESS_PROVENANCE_SQL, schema),
            location_keys=["nppes-live", "cms-live"],
            admitted_source_ids=[1, 3],
        )

        assert len(rows) == 2
        rows_by_source_id = {row._mapping["source_id"]: row._mapping for row in rows}
        assert rows_by_source_id[1]["source_record_key"] == (
            "nppes:1990000072:practice:101"
        )
        assert rows_by_source_id[1]["source_run_id"] == "2026-07-15"
        assert rows_by_source_id[3]["source_record_key"] == (
            "cms_doctors:1990000080:202"
        )
        assert rows_by_source_id[3]["source_run_id"].startswith(
            "2026-07-16 12:34:56"
        )
        indexed = serving._index_address_provenance(rows)
        assert indexed["nppes-live"][0]["record_version_id"] == "2026-07-15"
        assert indexed["nppes-live"][0]["retrieved_at"].startswith("2026-07-15")
        assert indexed["cms-live"][0]["record_version_id"].startswith(
            "2026-07-16 12:34:56"
        )
        assert indexed["cms-live"][0]["retrieved_at"].startswith("2026-07-16")
        assert "unified-20260731" not in json.dumps(indexed, default=str)
        assert "stale-" not in json.dumps(indexed, default=str)


@pytest.mark.asyncio
async def test_admitted_source_never_falls_back_to_generic_materialization():
    async with _temporary_schema() as (database, schema):
        await database.status(
            f"""
            INSERT INTO {schema}.entity_address_unified (
                location_key, npi, address_key, premise_key, address_sources
            ) VALUES (
                'admitted-without-lineage', 1990000098,
                '00000000-0000-0000-0000-000000000019',
                '10000000-0000-0000-0000-000000000019',
                ARRAY[]::varchar[]
            )
            """
        )
        await database.status(
            f"""
            INSERT INTO {schema}.entity_address_evidence (
                evidence_id, location_key, source_id, source_record_key,
                source_run_id, observed_at, last_seen_at
            ) VALUES (
                40, 'admitted-without-lineage', 0,
                'admitted-without-lineage', '20260731', NULL, NULL
            )
            """
        )

        rows = await database.all(
            _schema_sql(serving._ADDRESS_PROVENANCE_SQL, schema),
            location_keys=["admitted-without-lineage"],
            admitted_source_ids=[1],
        )

        assert rows == []


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


async def _optimized_rows(database: Database, schema: str, source_mask: int):
    query = _optimized_query(schema, source_mask)
    sql = _schema_sql(serving._membership_location_sql(query, limit=10, offset=0), schema)
    return await database.all(sql, **query.parameter_map)


def _knn_query(schema: str) -> serving._MembershipLocationQuery:
    args = {
        "lat": "42.3314",
        "long": "-83.0458",
        "radius_miles": "30",
    }
    parameter_map = {
        "limit": 2,
        "offset": 0,
        "shared_snapshot_key": 41,
    }
    filter_sql, distance_sql = serving._membership_filter_sql(
        args,
        candidate_npis=None,
        uses_unified_addresses=True,
        address_zip5_sql=serving._ptg2_address_zip5_sql(
            "addr",
            unified=True,
        ),
        parameter_map=parameter_map,
        literal_service_address_types=True,
    )
    return serving._MembershipLocationQuery(
        address_table=f"{schema}.entity_address_unified",
        npi_scope_table=f"{schema}.ptg2_v3_npi_scope",
        filter_sql=(
            "npi_scope.snapshot_key = :shared_snapshot_key "
            f"AND ({filter_sql})"
        ),
        parameter_map=parameter_map,
        distance_sql=distance_sql,
        knn_order_sql=serving._membership_knn_order_sql(
            args,
            candidate_npis=None,
            uses_unified_addresses=True,
            offset=0,
        ),
        address_assurance_sql=serving._membership_address_assurance_sql(
            args,
            True,
        ),
    )


@pytest.mark.asyncio
async def test_knn_template_executes_precedence_and_empty_probe_shape():
    async with _temporary_schema() as (database, schema):
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

        query = _knn_query(schema)
        sql = _schema_sql(
            serving._membership_location_sql(query, limit=2, offset=0),
            schema,
        )
        rows = await database.all(sql, **query.parameter_map)

        assert len(rows) == 1
        row = rows[0]._mapping
        assert row["npi"] == 1990000015
        assert row["_geo_evidence_level"] == "nppes_registry_address"
        assert row["_geo_evidence_source_id"] == 1
        assert row["_ptg_source_exhausted"] is True
        assert row["_ptg_probe_empty"] is False
        assert json.loads(row["address_payload"])["location_key"] == (
            "knn-precedence"
        )

        for table_name in (
            "doctor_clinician_address",
            "mrf_address",
            "npi_address",
        ):
            await database.status(f"DELETE FROM {schema}.{table_name}")
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

        empty_rows = await database.all(sql, **query.parameter_map)

        assert len(empty_rows) == 1
        empty_row = empty_rows[0]._mapping
        assert empty_row["npi"] is None
        assert empty_row["_ptg_source_exhausted"] is True
        assert empty_row["_ptg_probe_empty"] is True


@pytest.mark.asyncio
async def test_optimized_membership_rejects_npi_wide_cms_anchor():
    async with _temporary_schema() as (database, schema):
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
                ),
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

        rows = await _optimized_rows(database, schema, 4)

        assert len(rows) == 1
        assert rows[0]._mapping["_geo_evidence_level"] == (
            "cms_doctors_source_with_nppes_identity_anchor"
        )
        assert rows[0]._mapping["_geo_evidence_source_id"] == 3
        assert json.loads(rows[0]._mapping["address_payload"])["location_key"] == (
            "cms-anchored"
        )


@pytest.mark.asyncio
async def test_optimized_membership_uses_location_key_as_final_tie_breaker():
    async with _temporary_schema() as (database, schema):
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

        rows = await _optimized_rows(database, schema, 1)

        assert len(rows) == 1
        assert json.loads(rows[0]._mapping["address_payload"])["location_key"] == (
            "a-tied-location"
        )


@pytest.mark.asyncio
async def test_optimized_membership_requires_distinct_normalized_mrf_issuers():
    async with _temporary_schema() as (database, schema):
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

        rows = await _optimized_rows(database, schema, 2)

        assert len(rows) == 1
        assert rows[0]._mapping["_geo_evidence_level"] == (
            "multi_issuer_marketplace_address"
        )
        assert rows[0]._mapping["_geo_evidence_source_id"] == 2
        assert json.loads(rows[0]._mapping["address_payload"])["location_key"] == (
            "distinct-issuers"
        )
