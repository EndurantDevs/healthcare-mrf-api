# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

import pytest
from sqlalchemy import text

from api.endpoint import npi as npi_module
from db.models import db
from tests.test_npi_provider_directory_role_evidence_db import (
    _insert_dataset_rows,
    _insert_endpoint_rows,
    _insert_resource_rows,
    _insert_source_rows,
    _publish_dataset_rows,
)


LOCATION_STATUS_ENDPOINT_ID = "pd-location-status"
LOCATION_STATUS_SOURCE_ID = "pdfhir_location_status_test"
LOCATION_STATUS_DATASET_ID = "dataset-location-status"
LOCATION_STATUS_RUN_ID = "run-location-status"
LOCATION_STATUS_DATASET_ROWS = [
    (
        LOCATION_STATUS_DATASET_ID,
        LOCATION_STATUS_ENDPOINT_ID,
        LOCATION_STATUS_RUN_ID,
        {},
    )
]
LOCATION_STATUS_ROLE_RECORDS = (
    {
        "role_id": "role-current",
        "source_record_id": "provider_directory_fhir:practitioner_role:"
        f"{LOCATION_STATUS_SOURCE_ID}:role-current:location-current",
        "role_run_id": LOCATION_STATUS_RUN_ID,
        "period_start": "2999-01-01",
        "period_end": None,
        "payload": {"active": True},
        "npi": 1000000001,
        "address_key": "00000000-0000-0000-0000-000000000001",
    },
    {
        "role_id": "role-fallback",
        "source_record_id": "provider_directory_fhir:practitioner_role:"
        f"{LOCATION_STATUS_SOURCE_ID}:role-fallback:location-fallback",
        "role_run_id": "future-checkpoint-run",
        "period_start": None,
        "period_end": None,
        "payload": {"active": False},
        "npi": 1000000002,
        "address_key": "00000000-0000-0000-0000-000000000002",
    },
    {
        "role_id": "role-expired",
        "source_record_id": "provider_directory_fhir:practitioner_role:"
        f"{LOCATION_STATUS_SOURCE_ID}:role-expired:location-expired",
        "role_run_id": LOCATION_STATUS_RUN_ID,
        "period_start": None,
        "period_end": "2000-01-01",
        "payload": {"active": True},
        "npi": 1000000003,
        "address_key": "00000000-0000-0000-0000-000000000003",
    },
)


def _requires_test_database() -> None:
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database_name.lower():
        pytest.skip("geo evidence SQL test requires a disposable test database")


def _geo_membership_fixture() -> dict[str, object]:
    source_id = "pdfhir_geo_membership_test"
    return {
        "endpoint_id": "endpoint_geo_membership_test",
        "source_id": source_id,
        "dataset_id": "dataset_geo_membership_test",
        "run_id": "run_geo_membership_test",
        "npi": 1003968710,
        "address_key": "47ab23c8-74f7-41eb-8ffb-e8b9f6214594",
        "included_record_id": (
            "provider_directory_fhir:organization_address:"
            f"{source_id}:organization-included:0"
        ),
        "excluded_record_id": (
            "provider_directory_fhir:practitioner_role_address:"
            f"{source_id}:role-excluded:0"
        ),
    }


async def _insert_endpoint_and_source(session, schema: str, fixture: dict) -> None:
    await session.execute(
        text(
            f"INSERT INTO {schema}.provider_directory_api_endpoint "
            "(endpoint_id, canonical_api_base, credential_descriptor_hash, "
            "endpoint_signature_hash) VALUES (:endpoint_id, :api_base, "
            ":credential_hash, :signature_hash)"
        ),
        {
            "endpoint_id": fixture["endpoint_id"],
            "api_base": "https://geo-membership.test/fhir",
            "credential_hash": "geo-membership-credential",
            "signature_hash": "geo-membership-signature",
        },
    )
    await session.execute(
        text(
            f"INSERT INTO {schema}.provider_directory_source "
            "(source_id, org_name, canonical_api_base, endpoint_id, "
            "requires_registration, requires_api_key) VALUES "
            "(:source_id, 'Geo Membership Test', :api_base, :endpoint_id, "
            "false, false)"
        ),
        {
            "source_id": fixture["source_id"],
            "api_base": "https://geo-membership.test/fhir",
            "endpoint_id": fixture["endpoint_id"],
        },
    )


async def _insert_current_dataset(session, schema: str, fixture: dict) -> None:
    await session.execute(
        text(
            f"INSERT INTO {schema}.provider_directory_endpoint_dataset "
            "(dataset_id, endpoint_id, import_run_id, status, is_current, "
            "resource_count) VALUES "
            "(:dataset_id, :endpoint_id, :run_id, 'acquiring', false, 1)"
        ),
        fixture,
    )
    await session.execute(
        text(
            f"INSERT INTO {schema}.provider_directory_dataset_resource "
            "(dataset_id, resource_type, resource_id, payload_hash, payload_json) "
            "VALUES (:dataset_id, 'Organization', 'organization-included', "
            "'geo-membership-payload', CAST('{}' AS jsonb))"
        ),
        fixture,
    )
    await session.execute(
        text(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET status = 'validated', validated_at = transaction_timestamp() "
            "WHERE dataset_id = :dataset_id"
        ),
        fixture,
    )
    await session.execute(
        text(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET status = 'published', is_current = true, "
            "published_at = transaction_timestamp() "
            "WHERE dataset_id = :dataset_id"
        ),
        fixture,
    )


async def _insert_overlay_rows(session, schema: str, fixture: dict) -> None:
    overlay_sql = text(
        f"INSERT INTO {schema}.provider_directory_address_overlay "
        "(source_record_id, source_id, last_seen_run_id, resource_type, "
        "resource_id, npi, address_key, address_precision) VALUES "
        "(:source_record_id, :source_id, :run_id, :resource_type, "
        ":resource_id, :npi, CAST(:address_key AS uuid), 'street')"
    )
    for resource_type, resource_id, record_key in (
        ("Organization", "organization-included", "included_record_id"),
        ("PractitionerRole", "role-excluded", "excluded_record_id"),
    ):
        await session.execute(
            overlay_sql,
            {
                **fixture,
                "resource_type": resource_type,
                "resource_id": resource_id,
                "source_record_id": fixture[record_key],
            },
        )


async def _insert_location_status_fixture(session, schema: str) -> None:
    """Create current typed and immutable-fallback practitioner roles."""
    await _insert_endpoint_rows(
        session,
        schema,
        [(LOCATION_STATUS_ENDPOINT_ID, "https://location-status.test/fhir")],
    )
    await _insert_source_rows(
        session,
        schema,
        [(LOCATION_STATUS_SOURCE_ID, LOCATION_STATUS_ENDPOINT_ID)],
    )
    await _insert_dataset_rows(
        session,
        schema,
        LOCATION_STATUS_DATASET_ROWS,
    )
    await _insert_resource_rows(
        session,
        schema,
        LOCATION_STATUS_DATASET_ID,
        [
            ("PractitionerRole", role_record["role_id"], role_record["payload"])
            for role_record in LOCATION_STATUS_ROLE_RECORDS
        ],
    )
    await _publish_dataset_rows(
        session,
        schema,
        [(LOCATION_STATUS_DATASET_ID, len(LOCATION_STATUS_ROLE_RECORDS))],
    )
    fixture_params = [
        {
            **role_record,
            "source_id": LOCATION_STATUS_SOURCE_ID,
            "run_id": LOCATION_STATUS_RUN_ID,
        }
        for role_record in LOCATION_STATUS_ROLE_RECORDS
    ]
    await session.execute(
        text(
            f"INSERT INTO {schema}.provider_directory_practitioner_role "
            "(source_id, resource_id, active, period_start, period_end, "
            "last_seen_run_id) VALUES (:source_id, :role_id, true, "
            ":period_start, :period_end, :role_run_id)"
        ),
        fixture_params,
    )
    await session.execute(
        text(
            f"INSERT INTO {schema}.provider_directory_address_overlay "
            "(source_record_id, source_id, last_seen_run_id, resource_type, "
            "resource_id, npi, address_key) VALUES (:source_record_id, "
            ":source_id, :run_id, 'PractitionerRole', :role_id, :npi, "
            "CAST(:address_key AS uuid))"
        ),
        fixture_params,
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_geo_evidence_excludes_resource_absent_from_current_dataset():
    """Current-run overlays still require exact current dataset membership."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    fixture = _geo_membership_fixture()

    async with db.transaction() as session:
        fixture_savepoint = await session.begin_nested()
        try:
            await _insert_endpoint_and_source(session, schema, fixture)
            await _insert_current_dataset(session, schema, fixture)
            await _insert_overlay_rows(session, schema, fixture)
            result = await session.execute(
                text(npi_module._current_provider_directory_geo_evidence_sql()),
                {
                    "candidate_npis": [fixture["npi"]],
                    "candidate_address_keys": [fixture["address_key"]],
                },
            )
            evidence_rows = result.all()

            assert len(evidence_rows) == 1
            evidence = evidence_rows[0]._mapping
            assert evidence["source_record_ids"] == [fixture["included_record_id"]]
            assert evidence["provider_directory_source_count"] == 1
        finally:
            await fixture_savepoint.rollback()


@pytest.mark.asyncio(loop_scope="session")
async def test_location_status_query_uses_current_typed_row_and_dataset_fallback():
    """Use current typed rows and immutable payloads for mismatched checkpoints."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")

    async with db.transaction() as database_session:
        fixture_savepoint = await database_session.begin_nested()
        try:
            await _insert_location_status_fixture(database_session, schema)
            query_result = await database_session.execute(
                npi_module._location_status_query(
                    schema,
                    f"{schema}.provider_directory_address_overlay",
                ),
                {
                    "source_record_ids": [
                        role_record["source_record_id"]
                        for role_record in LOCATION_STATUS_ROLE_RECORDS
                    ]
                },
            )
            status_by_record_id = {
                status_row._mapping["source_record_id"]: status_row._mapping[
                    "location_status"
                ]
                for status_row in query_result.all()
            }

            assert status_by_record_id == {
                LOCATION_STATUS_ROLE_RECORDS[0]["source_record_id"]: "inactive",
                LOCATION_STATUS_ROLE_RECORDS[1]["source_record_id"]: "inactive",
                LOCATION_STATUS_ROLE_RECORDS[2]["source_record_id"]: "inactive",
            }
        finally:
            await fixture_savepoint.rollback()
