# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

import pytest
from sqlalchemy import text

from api.endpoint import npi as npi_module
from db.models import db


def _requires_test_database() -> None:
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database_name.lower():
        pytest.skip("phone evidence SQL test requires a disposable test database")


def _phone_membership_fixture() -> dict[str, object]:
    source_id = "pdfhir_phone_membership_test"
    return {
        "endpoint_id": "endpoint_phone_membership_test",
        "source_id": source_id,
        "dataset_id": "dataset_phone_membership_test",
        "run_id": "run_phone_membership_test",
        "phone_number": "4195550198",
        "member_npi": 1003968710,
        "member_address_key": "47ab23c8-74f7-41eb-8ffb-e8b9f6214594",
        "member_record_id": (
            "provider_directory_fhir:organization_address:"
            f"{source_id}:shared-resource:0"
        ),
        "nonmember_npi": 1003968728,
        "nonmember_address_key": "b231f8d5-9af7-40b6-81c5-95a69eb52311",
        "nonmember_record_id": (
            "provider_directory_fhir:practitioner_role_address:"
            f"{source_id}:shared-resource:0"
        ),
    }


def _phone_candidate_membership_sql() -> str:
    empty_address_relation = """(
        SELECT NULL::bigint AS npi, NULL::bigint AS inferred_npi,
               NULL::uuid AS address_key, NULL::integer AS source_count,
               NULL::varchar AS type
         WHERE false
    )"""
    candidate_rows = npi_module._PHONE_CANDIDATE_ROWS_CTE.format(
        address_table_sql=empty_address_relation,
        service_types="'primary'",
        direct_phone="false",
    )
    return f"""
        WITH {npi_module._CURRENT_PROVIDER_DIRECTORY_PHONE_CTES.strip()},
             {candidate_rows.strip()}
        SELECT provider_npi, address_key::text AS address_key, source_record_id
          FROM phone_candidate_rows
         WHERE provider_directory_matched
      ORDER BY provider_npi
    """


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
            "api_base": "https://phone-membership.test/fhir",
            "credential_hash": "phone-membership-credential",
            "signature_hash": "phone-membership-signature",
        },
    )
    await session.execute(
        text(
            f"INSERT INTO {schema}.provider_directory_source "
            "(source_id, org_name, canonical_api_base, endpoint_id, "
            "requires_registration, requires_api_key) VALUES "
            "(:source_id, 'Phone Membership Test', :api_base, :endpoint_id, "
            "false, false)"
        ),
        {
            "source_id": fixture["source_id"],
            "api_base": "https://phone-membership.test/fhir",
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
            "VALUES (:dataset_id, 'Organization', 'shared-resource', "
            "'phone-membership-payload', CAST('{}' AS jsonb))"
        ),
        fixture,
    )


async def _insert_overlay_rows(session, schema: str, fixture: dict) -> None:
    overlay_sql = text(
        f"INSERT INTO {schema}.provider_directory_address_overlay "
        "(source_record_id, source_id, last_seen_run_id, resource_type, "
        "resource_id, npi, address_key, phone_number, address_precision) VALUES "
        "(:source_record_id, :source_id, :run_id, :resource_type, "
        "'shared-resource', :npi, CAST(:address_key AS uuid), :phone_number, 'street')"
    )
    overlay_rows = (
        ("Organization", "member_npi", "member_address_key", "member_record_id"),
        (
            "PractitionerRole",
            "nonmember_npi",
            "nonmember_address_key",
            "nonmember_record_id",
        ),
    )
    for resource_type, npi_key, address_key, record_key in overlay_rows:
        await session.execute(
            overlay_sql,
            {
                **fixture,
                "resource_type": resource_type,
                "npi": fixture[npi_key],
                "address_key": fixture[address_key],
                "source_record_id": fixture[record_key],
            },
        )


async def _delete_phone_membership_fixture(session, schema: str, fixture: dict) -> None:
    await session.execute(
        text(
            f"DELETE FROM {schema}.provider_directory_address_overlay "
            "WHERE source_record_id = ANY(CAST(:record_ids AS varchar[]))"
        ),
        {
            "record_ids": [
                fixture["member_record_id"],
                fixture["nonmember_record_id"],
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
            text(f"DELETE FROM {schema}.{table_name} WHERE {key_name} = :key_value"),
            {"key_value": fixture[key_name]},
        )


@pytest.mark.asyncio(loop_scope="session")
async def test_phone_candidates_require_exact_current_dataset_membership():
    """A matching current run cannot admit a nonmember resource overlay."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    fixture = _phone_membership_fixture()

    async with db.transaction() as session:
        await _insert_endpoint_and_source(session, schema, fixture)
        await _insert_current_dataset(session, schema, fixture)
        await _insert_overlay_rows(session, schema, fixture)
        try:
            candidate_result = await session.execute(
                text(_phone_candidate_membership_sql()),
                {"phone_digits": fixture["phone_number"]},
            )
            candidate_rows = [dict(candidate._mapping) for candidate in candidate_result.all()]

            assert candidate_rows == [
                {
                    "provider_npi": fixture["member_npi"],
                    "address_key": fixture["member_address_key"],
                    "source_record_id": fixture["member_record_id"],
                }
            ]
        finally:
            await _delete_phone_membership_fixture(session, schema, fixture)
