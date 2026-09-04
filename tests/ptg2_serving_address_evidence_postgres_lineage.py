# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json

import pytest

from api import ptg2_serving as serving
from db.connection import Database
from tests.ptg2_serving_address_evidence_postgres_support import (
    _has_complete_lineage,
    _schema_sql,
    _temporary_schema,
)


async def _fetch_provenance(
    database: Database,
    schema: str,
    location_keys: list[str],
    admitted_source_ids: list[int],
    *,
    use_stored_only: bool = False,
    strict_stored_identity: bool = False,
):
    return await database.all(
        _schema_sql(serving._ADDRESS_PROVENANCE_SQL, schema),
        location_keys=location_keys,
        admitted_source_ids=admitted_source_ids,
        stored_only=use_stored_only,
        strict_stored_identity=use_stored_only or strict_stored_identity,
    )


async def _insert_admitted_mrf_location(database: Database, schema: str) -> None:
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


async def _insert_admitted_mrf_evidence(database: Database, schema: str) -> None:
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


async def _insert_admitted_mrf_candidates(database: Database, schema: str) -> None:
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


@pytest.mark.asyncio
async def test_admitted_mrf_recovers_specific_lineage_without_fabrication():
    async with _temporary_schema() as (database, schema):
        await _insert_admitted_mrf_location(database, schema)
        await _insert_admitted_mrf_evidence(database, schema)
        await _insert_admitted_mrf_candidates(database, schema)

        provenance_rows = await _fetch_provenance(
            database, schema, ["mrf-admitted"], [2]
        )

        assert len(provenance_rows) == 1
        provenance_mapping = provenance_rows[0]._mapping
        assert provenance_mapping["source_id"] == 2
        assert provenance_mapping["source_record_key"] == "mrf:1990000015:practice:11"
        assert provenance_mapping["source_import_ids"] == ["qualifying-v1"]
        assert provenance_mapping["source_issuer_names"] == ["Issuer A", "Issuer B"]
        lineage_entry = serving._index_address_provenance(provenance_rows)[
            "mrf-admitted"
        ][0]
        assert _has_complete_lineage(lineage_entry)
        assert lineage_entry["record_version_id"] == "qualifying-v1"
        assert lineage_entry["retrieved_at"] == "2026-07-29"
        assert "mrf-admitted" not in str(lineage_entry["source_record_id"])
        assert "mrf-admitted" not in str(lineage_entry["record_version_id"])


@pytest.mark.asyncio
async def test_stored_only_lineage_does_not_consult_live_mrf_fallback():
    async with _temporary_schema() as (database, schema):
        await _insert_admitted_mrf_location(database, schema)
        await _insert_admitted_mrf_evidence(database, schema)
        await _insert_admitted_mrf_candidates(database, schema)

        provenance_rows = await _fetch_provenance(
            database,
            schema,
            ["mrf-admitted"],
            [2],
            use_stored_only=True,
        )

        assert provenance_rows == []


@pytest.mark.asyncio
async def test_live_fallback_rejects_mismatched_stored_identity_without_live_row():
    async with _temporary_schema() as (database, schema):
        await database.status(
            f"""
            INSERT INTO {schema}.entity_address_unified (
                location_key, npi, address_key, premise_key, address_sources
            ) VALUES (
                'stale-identity', 1990000015,
                '00000000-0000-0000-0000-000000000001',
                '10000000-0000-0000-0000-000000000001',
                ARRAY['nppes']::varchar[]
            )
            """
        )
        await database.status(
            f"""
            INSERT INTO {schema}.entity_address_evidence (
                evidence_id, location_key, address_key, premise_key, npi,
                source_id, source_record_key, source_run_id, observed_at
            ) VALUES (
                4, 'stale-identity',
                '00000000-0000-0000-0000-000000000002',
                '10000000-0000-0000-0000-000000000002',
                1990000023, 1, 'nppes:stale', '20260731',
                '2026-07-31T00:00:00Z'
            )
            """
        )

        provenance_rows = await _fetch_provenance(
            database,
            schema,
            ["stale-identity"],
            [1],
            strict_stored_identity=True,
        )

        assert provenance_rows == []


async def _insert_specific_evidence_candidates(database: Database, schema: str) -> None:
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


@pytest.mark.asyncio
async def test_specific_evidence_excludes_retired_and_blank_rows():
    async with _temporary_schema() as (database, schema):
        await _insert_specific_evidence_candidates(database, schema)

        provenance_rows = await _fetch_provenance(
            database, schema, ["active-lineage"], [0]
        )

        assert len(provenance_rows) == 1
        provenance_mapping = provenance_rows[0]._mapping
        assert provenance_mapping["source_id"] == 8
        assert provenance_mapping["source_record_key"] == (
            "provider_directory_fhir:active"
        )
        assert provenance_mapping["source_run_id"] == "fhir-v1"


async def _insert_compact_run_candidates(database: Database, schema: str) -> None:
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


@pytest.mark.asyncio
async def test_stored_compact_run_date_is_complete_without_timestamps():
    async with _temporary_schema() as (database, schema):
        await _insert_compact_run_candidates(database, schema)

        provenance_rows = await _fetch_provenance(
            database, schema, ["compact-run-lineage"], [0]
        )

        assert len(provenance_rows) == 1
        provenance_mapping = provenance_rows[0]._mapping
        assert provenance_mapping["source_record_key"] == (
            "provider_directory_fhir:valid-run-only"
        )
        assert provenance_mapping["source_run_id"] == "20260730"
        lineage_entry = serving._index_address_provenance(provenance_rows)[
            "compact-run-lineage"
        ][0]
        assert lineage_entry["record_version_id"] == "20260730"
        assert lineage_entry["retrieved_at"] == "2026-07-30"
        assert _has_complete_lineage(lineage_entry)


async def _insert_incomplete_lineage_candidates(database: Database, schema: str) -> None:
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


@pytest.mark.asyncio
async def test_incomplete_specific_and_source_zero_are_not_public_lineage():
    async with _temporary_schema() as (database, schema):
        await _insert_incomplete_lineage_candidates(database, schema)
        provenance_rows = await _fetch_provenance(
            database, schema, ["tuple-guard"], [0]
        )

        assert provenance_rows == []
        assert serving._index_address_provenance(provenance_rows) == {}


async def _insert_live_unified_addresses(database: Database, schema: str) -> None:
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


async def _insert_live_stored_evidence(database: Database, schema: str) -> None:
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


async def _insert_live_source_addresses(database: Database, schema: str) -> None:
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


@pytest.mark.asyncio
async def test_live_nppes_and_cms_use_source_specific_versions_as_whole_rows():
    async with _temporary_schema() as (database, schema):
        await _insert_live_unified_addresses(database, schema)
        await _insert_live_stored_evidence(database, schema)
        await _insert_live_source_addresses(database, schema)

        provenance_rows = await _fetch_provenance(
            database, schema, ["nppes-live", "cms-live"], [1, 3]
        )

        assert len(provenance_rows) == 2
        provenance_by_source_id = {
            provenance_row._mapping["source_id"]: provenance_row._mapping
            for provenance_row in provenance_rows
        }
        assert provenance_by_source_id[1]["source_record_key"] == (
            "nppes:1990000072:practice:101"
        )
        assert provenance_by_source_id[1]["source_run_id"] == "2026-07-15"
        assert provenance_by_source_id[3]["source_record_key"] == (
            "cms_doctors:1990000080:202"
        )
        assert provenance_by_source_id[3]["source_run_id"].startswith(
            "2026-07-16 12:34:56"
        )
        lineage_by_location = serving._index_address_provenance(provenance_rows)
        assert lineage_by_location["nppes-live"][0]["record_version_id"] == "2026-07-15"
        assert lineage_by_location["nppes-live"][0]["retrieved_at"].startswith("2026-07-15")
        assert lineage_by_location["cms-live"][0]["record_version_id"].startswith(
            "2026-07-16 12:34:56"
        )
        assert lineage_by_location["cms-live"][0]["retrieved_at"].startswith("2026-07-16")
        assert "unified-20260731" not in json.dumps(lineage_by_location, default=str)
        assert "stale-" not in json.dumps(lineage_by_location, default=str)


async def _insert_generic_materialization_only(database: Database, schema: str) -> None:
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


@pytest.mark.asyncio
async def test_admitted_source_never_falls_back_to_generic_materialization():
    async with _temporary_schema() as (database, schema):
        await _insert_generic_materialization_only(database, schema)

        provenance_rows = await _fetch_provenance(
            database, schema, ["admitted-without-lineage"], [1]
        )

        assert provenance_rows == []
