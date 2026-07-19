# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

import pytest
from sqlalchemy import text

from api.endpoint import npi as npi_module
from db.models import db


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
            "resource_count, published_at) VALUES "
            "(:dataset_id, :endpoint_id, :run_id, 'published', true, 1, now())"
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


async def _delete_geo_membership_fixture(session, schema: str, fixture: dict) -> None:
    await session.execute(
        text(
            f"DELETE FROM {schema}.provider_directory_address_overlay "
            "WHERE source_record_id = ANY(CAST(:record_ids AS varchar[]))"
        ),
        {
            "record_ids": [
                fixture["included_record_id"],
                fixture["excluded_record_id"],
            ]
        },
    )
    for table_name, key_name in (
        ("provider_directory_dataset_resource", "dataset_id"),
        ("provider_directory_endpoint_dataset", "dataset_id"),
        ("provider_directory_source", "source_id"),
        ("provider_directory_api_endpoint", "endpoint_id"),
    ):
        await session.execute(
            text(f"DELETE FROM {schema}.{table_name} WHERE {key_name} = :value"),
            {"value": fixture[key_name]},
        )


@pytest.mark.asyncio(loop_scope="session")
async def test_geo_evidence_excludes_resource_absent_from_current_dataset():
    """Current-run overlays still require exact current dataset membership."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    fixture = _geo_membership_fixture()

    async with db.transaction() as session:
        await _insert_endpoint_and_source(session, schema, fixture)
        await _insert_current_dataset(session, schema, fixture)
        await _insert_overlay_rows(session, schema, fixture)
        try:
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
            await _delete_geo_membership_fixture(session, schema, fixture)
