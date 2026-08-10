# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic PostgreSQL fixtures for the UHC Flex cohort seal."""

from __future__ import annotations

import json

import asyncpg
import pytest

from process.uhc_flex_official_cohort_contract import (
    UHCFlexOfficialNPICohort,
)
from process.uhc_flex_official_cohort_contract import (
    build_uhc_flex_official_cohort,
)
from process.uhc_provider_file_source_identity import (
    UHC_PROVIDER_FILE_SOURCE_ID,
)
from tests.formulary_fhir_twin_admission_pg_support import assert_sqlstate
from tests.formulary_fhir_twin_admission_pg_support import quoted


ENDPOINT_ID = "endpoint-official"
DATASET_ID = "dataset-2026"
ACQUISITION_ROOT_RUN_ID = "r" * 64
DATASET_HASH = "d" * 64
CONTENT_PROOF_SHA256 = "c" * 64
PRACTITIONER_NPIS = (1003821380, 1003821380, 1518379601)
MEMBER_NPIS = tuple(sorted(set(PRACTITIONER_NPIS)))
COHORT_TABLE = "provider_directory_uhc_flex_npi_cohort"
MEMBER_TABLE = "provider_directory_uhc_flex_npi_member"


def cohort_fixture() -> UHCFlexOfficialNPICohort:
    """Return the exact application identity independently sealed by SQL."""

    return build_uhc_flex_official_cohort(
        official_endpoint_id=ENDPOINT_ID,
        official_dataset_id=DATASET_ID,
        official_acquisition_root_run_id=ACQUISITION_ROOT_RUN_ID,
        official_dataset_hash=DATASET_HASH,
        official_content_proof_sha256=CONTENT_PROOF_SHA256,
        practitioner_resource_count=len(PRACTITIONER_NPIS),
        npi_count=len(MEMBER_NPIS),
    )


def _npi_function_sql(schema: str) -> str:
    return f"""
    CREATE FUNCTION {schema}.public_evidence_npi_valid(candidate_npi text)
    RETURNS boolean LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    SET search_path = pg_catalog AS $function$
        SELECT CASE WHEN candidate_npi ~ '^[0-9]{{10}}$' THEN
            CASE WHEN candidate_npi::bigint BETWEEN 1000000000 AND 2999999999
            THEN mod(24 + (
                SELECT sum(CASE
                    WHEN ordinal < 10 AND mod(ordinal, 2) = 1
                    THEN digit * 2 - CASE WHEN digit >= 5 THEN 9 ELSE 0 END
                    ELSE digit END)
                FROM unnest(string_to_array(candidate_npi, NULL))
                    WITH ORDINALITY AS item(value, ordinal)
                CROSS JOIN LATERAL (SELECT value::integer AS digit) AS parsed
            ), 10) = 0 ELSE false END
        ELSE false END;
    $function$;
    """


async def create_provider_foundation(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    """Create only the pre-existing relations referenced by the migration."""

    schema = quoted(schema_name)
    await connection.execute(
        f"""
        CREATE TABLE {schema}.provider_directory_api_endpoint (
            endpoint_id varchar(64) PRIMARY KEY
        );
        CREATE TABLE {schema}.provider_directory_source (
            source_id varchar(64) PRIMARY KEY,
            endpoint_id varchar(64) NOT NULL REFERENCES
                {schema}.provider_directory_api_endpoint(endpoint_id)
        );
        CREATE TABLE {schema}.provider_directory_endpoint_dataset (
            dataset_id varchar(96) PRIMARY KEY,
            endpoint_id varchar(64) NOT NULL REFERENCES
                {schema}.provider_directory_api_endpoint(endpoint_id),
            acquisition_root_run_id varchar(64),
            dataset_hash varchar(64),
            status varchar(32) NOT NULL,
            is_current boolean NOT NULL,
            resource_count bigint NOT NULL,
            publication_metadata_json jsonb
        );
        CREATE TABLE {schema}.provider_directory_dataset_resource (
            dataset_id varchar(96) NOT NULL REFERENCES
                {schema}.provider_directory_endpoint_dataset(dataset_id),
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            payload_hash varchar(64) NOT NULL,
            payload_json jsonb NOT NULL,
            PRIMARY KEY (dataset_id, resource_type, resource_id)
        );
        {_npi_function_sql(schema)}
        """
    )


def _practitioner_payload(npi: int) -> dict[str, object]:
    return {
        "npi": npi,
        "identifiers": [
            {
                "system": "http://hl7.org/fhir/sid/us-npi",
                "value": str(npi),
            }
        ],
    }


def _content_proof() -> dict[str, object]:
    return {
        "contract_id": "healthporta.uhc.canonical-content-proof.v1",
        "complete": True,
        "source_id": UHC_PROVIDER_FILE_SOURCE_ID,
        "dataset_id": DATASET_ID,
        "endpoint_id": ENDPOINT_ID,
        "acquisition_root_run_id": ACQUISITION_ROOT_RUN_ID,
        "dataset_hash": DATASET_HASH,
        "resource_count": 4,
        "resource_counts": {"Practitioner": 3, "Organization": 1},
        "proof_sha256": CONTENT_PROOF_SHA256,
    }


async def seed_official_dataset(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    """Seed three Practitioner rows with two distinct canonical NPIs."""

    schema = quoted(schema_name)
    await connection.execute(
        f"INSERT INTO {schema}.provider_directory_api_endpoint "
        "(endpoint_id) VALUES ($1)",
        ENDPOINT_ID,
    )
    await connection.execute(
        f"INSERT INTO {schema}.provider_directory_source "
        "(source_id, endpoint_id) VALUES ($1, $2)",
        UHC_PROVIDER_FILE_SOURCE_ID,
        ENDPOINT_ID,
    )
    await connection.execute(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset "
        "(dataset_id, endpoint_id, acquisition_root_run_id, dataset_hash, "
        "status, is_current, resource_count, publication_metadata_json) "
        "VALUES ($1, $2, $3, $4, 'published', true, 4, $5::jsonb)",
        DATASET_ID,
        ENDPOINT_ID,
        ACQUISITION_ROOT_RUN_ID,
        DATASET_HASH,
        json.dumps({"uhc_canonical_content_proof_v1": _content_proof()}),
    )
    resource_rows = tuple(
        (
            DATASET_ID,
            "Practitioner",
            f"practitioner-{index}",
            str(index) * 64,
            json.dumps(_practitioner_payload(npi)),
        )
        for index, npi in enumerate(PRACTITIONER_NPIS, start=1)
    ) + (
        (DATASET_ID, "Organization", "organization-1", "4" * 64, "{}"),
    )
    await connection.executemany(
        f"INSERT INTO {schema}.provider_directory_dataset_resource "
        "(dataset_id, resource_type, resource_id, payload_hash, payload_json) "
        "VALUES ($1, $2, $3, $4, $5::jsonb)",
        resource_rows,
    )


async def insert_members(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    """Insert the unique members before their deferred parent header."""

    fixture = cohort_fixture()
    await connection.executemany(
        f"INSERT INTO {quoted(schema_name)}.{MEMBER_TABLE} "
        "(cohort_id, npi) VALUES ($1, $2)",
        tuple((fixture.cohort_id, npi) for npi in MEMBER_NPIS),
    )


async def insert_header(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    """Insert the canonical header after every distinct member."""

    fixture = cohort_fixture()
    await connection.execute(
        f"INSERT INTO {quoted(schema_name)}.{COHORT_TABLE} "
        "(cohort_id, contract_id, authority_id, official_source_id, "
        "official_endpoint_id, official_dataset_id, "
        "official_acquisition_root_run_id, official_dataset_hash, "
        "official_content_proof_sha256, resource_type, "
        "practitioner_resource_count, npi_count, cohort_complete, "
        "endpoint_collection_complete, endpoint_complete) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, "
        "$13, $14, $15)",
        fixture.cohort_id,
        fixture.contract_id,
        fixture.authority_id,
        fixture.official_source_id,
        fixture.official_endpoint_id,
        fixture.official_dataset_id,
        fixture.official_acquisition_root_run_id,
        fixture.official_dataset_hash,
        fixture.official_content_proof_sha256,
        fixture.resource_type,
        fixture.practitioner_resource_count,
        fixture.npi_count,
        fixture.cohort_complete,
        fixture.endpoint_collection_complete,
        fixture.endpoint_complete,
    )


async def assert_header_rejected_after(
    connection: asyncpg.Connection,
    schema_name: str,
    mutation_sql: str,
) -> None:
    """Roll back one source mutation after its header is rejected."""

    transaction = connection.transaction()
    await transaction.start()
    try:
        await connection.execute(mutation_sql)
        await insert_members(connection, schema_name)
        with pytest.raises(asyncpg.PostgresError) as error:
            await insert_header(connection, schema_name)
        assert error.value.sqlstate == "23514"
    finally:
        await transaction.rollback()


async def assert_invalid_sources_block_header(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    """Reject malformed Practitioner NPIs and current-pointer drift."""

    schema = quoted(schema_name)
    payload_ref = "payload_json"
    mutations = (
        f"UPDATE {schema}.provider_directory_dataset_resource SET "
        f"{payload_ref} = {payload_ref} - 'npi' WHERE resource_id = "
        "'practitioner-3'",
        f"UPDATE {schema}.provider_directory_dataset_resource SET "
        f"{payload_ref} = jsonb_set({payload_ref}, '{{npi}}', "
        "'1003821381'::jsonb) WHERE resource_id = 'practitioner-3'",
        f"UPDATE {schema}.provider_directory_dataset_resource SET "
        f"{payload_ref} = jsonb_set({payload_ref}, '{{npi}}', "
        "'\"1518379601\"'::jsonb) WHERE resource_id = 'practitioner-3'",
        f"UPDATE {schema}.provider_directory_dataset_resource SET "
        f"{payload_ref} = {payload_ref} - 'identifiers' WHERE resource_id = "
        "'practitioner-3'",
        f"UPDATE {schema}.provider_directory_dataset_resource SET "
        f"{payload_ref} = jsonb_set({payload_ref}, '{{identifiers,0,value}}', "
        "'\"1003821380\"'::jsonb) WHERE resource_id = 'practitioner-3'",
        f"UPDATE {schema}.provider_directory_endpoint_dataset "
        "SET is_current = false WHERE dataset_id = 'dataset-2026'",
    )
    for mutation_sql in mutations:
        await assert_header_rejected_after(
            connection,
            schema_name,
            mutation_sql,
        )


async def insert_valid_cohort(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    """Commit the canonical child-first, header-last cohort."""

    async with connection.transaction():
        await insert_members(connection, schema_name)
        await insert_header(connection, schema_name)


async def assert_stored_cohort(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    """Prove numeric scalars pass and duplicate Practitioner NPIs collapse."""

    schema = quoted(schema_name)
    fixture = cohort_fixture()
    header = await connection.fetchrow(
        f"SELECT cohort_id, practitioner_resource_count, npi_count, "
        f"cohort_complete, endpoint_collection_complete, endpoint_complete "
        f"FROM {schema}.{COHORT_TABLE}"
    )
    assert dict(header) == {
        "cohort_id": fixture.cohort_id,
        "practitioner_resource_count": 3,
        "npi_count": 2,
        "cohort_complete": True,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
    }
    member_npis = await connection.fetch(
        f"SELECT npi FROM {schema}.{MEMBER_TABLE} ORDER BY npi"
    )
    assert tuple(
        member_row["npi"] for member_row in member_npis
    ) == MEMBER_NPIS
    payload_shape = await connection.fetchrow(
        f"SELECT count(*) AS practitioner_count, count(*) FILTER (WHERE "
        "jsonb_typeof(payload_json -> 'npi') = 'number') AS numeric_count, "
        "count(*) FILTER (WHERE EXISTS (SELECT 1 FROM "
        "jsonb_array_elements(payload_json -> 'identifiers') AS identifier "
        "WHERE jsonb_typeof(identifier -> 'value') = 'string' AND "
        "identifier ->> 'value' = payload_json ->> 'npi')) AS matched_count "
        f"FROM {schema}.provider_directory_dataset_resource "
        "WHERE resource_type = 'Practitioner'"
    )
    assert tuple(payload_shape) == (3, 3, 3)


async def assert_cohort_immutability(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    """Reject late members and every material header/member mutation."""

    schema = quoted(schema_name)
    cohort_id = cohort_fixture().cohort_id
    statements = (
        f"INSERT INTO {schema}.{MEMBER_TABLE} (cohort_id, npi) "
        f"VALUES ('{cohort_id}', 1234567893)",
        f"UPDATE {schema}.{COHORT_TABLE} SET npi_count = 3",
        f"DELETE FROM {schema}.{COHORT_TABLE}",
        f"TRUNCATE TABLE {schema}.{COHORT_TABLE}, {schema}.{MEMBER_TABLE}",
        f"UPDATE {schema}.{MEMBER_TABLE} SET npi = 1234567893",
        f"DELETE FROM {schema}.{MEMBER_TABLE}",
        f"TRUNCATE TABLE {schema}.{MEMBER_TABLE}",
    )
    for statement in statements:
        await assert_sqlstate(connection, "55000", statement)


__all__ = (
    "assert_cohort_immutability",
    "assert_invalid_sources_block_header",
    "assert_stored_cohort",
    "cohort_fixture",
    "create_provider_foundation",
    "insert_valid_cohort",
    "seed_official_dataset",
)
