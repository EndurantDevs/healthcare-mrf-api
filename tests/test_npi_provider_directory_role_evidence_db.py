# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import os

import pytest
from sqlalchemy import text

from api.endpoint import npi as npi_module
from db.models import db


def _requires_test_database():
    database = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database.lower():
        pytest.skip("DB-backed role-evidence SQL test requires a disposable test database")


async def _insert_dataset_resource(
    session,
    schema: str,
    dataset_id: str,
    resource_type: str,
    resource_id: str,
    resource_payload: dict,
) -> None:
    payload_hash = f"hash-{dataset_id}-{resource_type}-{resource_id}"
    payload_json = json.dumps(resource_payload)
    await session.execute(
        text(
            f"INSERT INTO {schema}.provider_directory_dataset_resource "
            "(dataset_id, resource_type, resource_id, payload_hash, payload_json) "
            "VALUES (:dataset_id, :resource_type, :resource_id, :payload_hash, "
            "CAST(:payload_json AS jsonb))"
        ),
        {
            "dataset_id": dataset_id,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "payload_hash": payload_hash,
            "payload_json": payload_json,
        },
    )
    if resource_type == "InsurancePlan":
        await session.execute(
            text(
                f"INSERT INTO {schema}.provider_directory_dataset_insurance_plan "
                "(dataset_id, resource_id, payload_hash, payload_json) "
                "VALUES (:dataset_id, :resource_id, :payload_hash, "
                "CAST(:payload_json AS jsonb))"
            ),
            {
                "dataset_id": dataset_id,
                "resource_id": resource_id,
                "payload_hash": payload_hash,
                "payload_json": payload_json,
            },
        )


async def _insert_endpoint_rows(session, schema: str, endpoint_rows: list[tuple[str, str]]) -> None:
    for endpoint_id, api_base in endpoint_rows:
        await session.execute(
            text(
                f"INSERT INTO {schema}.provider_directory_api_endpoint "
                "(endpoint_id, canonical_api_base, credential_descriptor_hash, "
                "endpoint_signature_hash) VALUES (:endpoint_id, :api_base, "
                ":credential_hash, :signature_hash)"
            ),
            {
                "endpoint_id": endpoint_id,
                "api_base": api_base,
                "credential_hash": f"credential-{endpoint_id}",
                "signature_hash": f"signature-{endpoint_id}",
            },
        )


async def _insert_source_rows(session, schema: str, source_rows: list[tuple[str, str]]) -> None:
    for source_id, endpoint_id in source_rows:
        await session.execute(
            text(
                f"INSERT INTO {schema}.provider_directory_source "
                "(source_id, org_name, canonical_api_base, endpoint_id, "
                "requires_registration, requires_api_key) "
                "VALUES (:source_id, :org_name, :api_base, :endpoint_id, false, false)"
            ),
            {
                "source_id": source_id,
                "org_name": source_id,
                "api_base": f"https://{source_id}.test/fhir",
                "endpoint_id": endpoint_id,
            },
        )


async def _insert_dataset_rows(
    session,
    schema: str,
    dataset_rows: list[tuple[str, str, str, dict]],
) -> None:
    for dataset_id, endpoint_id, run_id, publication_metadata in dataset_rows:
        await session.execute(
            text(
                f"INSERT INTO {schema}.provider_directory_endpoint_dataset "
                "(dataset_id, endpoint_id, import_run_id, status, is_current, "
                "resource_count, published_at, publication_metadata_json) "
                "VALUES (:dataset_id, :endpoint_id, :run_id, 'published', true, "
                "0, now(), CAST(:metadata AS jsonb))"
            ),
            {
                "dataset_id": dataset_id,
                "endpoint_id": endpoint_id,
                "run_id": run_id,
                "metadata": json.dumps(publication_metadata),
            },
        )


async def _insert_resource_rows(
    session,
    schema: str,
    dataset_id: str,
    resource_rows: list[tuple[str, str, dict]],
) -> None:
    for resource_type, resource_id, resource_payload in resource_rows:
        await _insert_dataset_resource(
            session,
            schema,
            dataset_id,
            resource_type,
            resource_id,
            resource_payload,
        )


async def _insert_catalog_rows(
    session,
    schema: str,
    catalog_rows: list[tuple[str, str, str]],
) -> None:
    for source_id, network_id, network_name in catalog_rows:
        await session.execute(
            text(
                f"INSERT INTO {schema}.provider_directory_network_catalog "
                "(source_id, network_resource_id, provider_directory_network_name, "
                "provider_directory_network_key) VALUES (:source_id, :network_id, "
                ":network_name, :network_key)"
            ),
            {
                "source_id": source_id,
                "network_id": network_id,
                "network_name": network_name,
                "network_key": network_name.lower().replace(" ", "-"),
            },
        )


def _complete_resource_rows() -> list[tuple[str, str, dict]]:
    return [
        ("Organization", "network-edge", {"active": True, "name": "Edge Network"}),
        ("Organization", "network-zero", {"active": True, "name": "Zero Network"}),
        (
            "InsurancePlan",
            "plan-edge",
            {
                "status": "active",
                "name": "Edge Plan",
                "plan_identifier": "edge-plan",
                "network_refs": ["Organization/network-edge"],
            },
        ),
        (
            "InsurancePlan",
            "plan-zero",
            {
                "status": "active",
                "name": "Zero Plan",
                "plan_identifier": "zero-plan",
                "network_refs": ["Organization/network-zero"],
            },
        ),
        (
            "PractitionerRole",
            "role-edge",
            {
                "active": True,
                "network_refs": ["Organization/network-edge"],
                "insurance_plan_refs": [],
            },
        ),
        (
            "PractitionerRole",
            "role-zero",
            {
                "active": True,
                "network_refs": ["Organization/network-zero"],
                "insurance_plan_refs": [],
            },
        ),
        (
            "PractitionerRole",
            "role-direct",
            {
                "active": True,
                "network_refs": [],
                "insurance_plan_refs": ["InsurancePlan/plan-edge"],
            },
        ),
    ]


def _fallback_resource_rows() -> list[tuple[str, str, dict]]:
    return [
        ("Organization", "network-fallback", {"active": True, "name": "Fallback Network"}),
        (
            "InsurancePlan",
            "plan-fallback",
            {
                "status": "active",
                "name": "Fallback Plan",
                "plan_identifier": "fallback-plan",
                "network_refs": ["Organization/network-fallback"],
            },
        ),
        (
            "PractitionerRole",
            "role-fallback",
            {
                "active": True,
                "network_refs": ["Organization/network-fallback"],
                "insurance_plan_refs": [],
            },
        ),
    ]


def _relation_fixture_ids() -> dict[str, list[str]]:
    return {
        "endpoint_ids": ["pd-relation-complete", "pd-relation-fallback"],
        "source_ids": [
            "pdfhir_relation_edge_test",
            "pdfhir_relation_zero_test",
            "pdfhir_relation_direct_test",
            "pdfhir_relation_fallback_test",
        ],
        "dataset_ids": ["dataset-relation-complete", "dataset-relation-fallback"],
    }


async def _insert_fixture_ownership_rows(
    session,
    schema: str,
    fixture_ids: dict[str, list[str]],
) -> None:
    endpoint_ids = fixture_ids["endpoint_ids"]
    source_ids = fixture_ids["source_ids"]
    dataset_ids = fixture_ids["dataset_ids"]
    await _insert_endpoint_rows(
        session,
        schema,
        [
            (endpoint_ids[0], "https://relation-complete.test/fhir"),
            (endpoint_ids[1], "https://relation-fallback.test/fhir"),
        ],
    )
    await _insert_source_rows(
        session,
        schema,
        [
            (source_ids[0], endpoint_ids[0]),
            (source_ids[1], endpoint_ids[0]),
            (source_ids[2], endpoint_ids[0]),
            (source_ids[3], endpoint_ids[1]),
        ],
    )

    completion_proof_by_relation = {
        "dataset_network_plan": {
            "complete": True,
            "version": "1",
            "dataset_id": dataset_ids[0],
        },
        "dataset_affiliation_organization": {
            "complete": True,
            "version": "1",
            "dataset_id": dataset_ids[0],
        },
    }
    await _insert_dataset_rows(
        session,
        schema,
        [
            (
                dataset_ids[0],
                endpoint_ids[0],
                "run-relation-complete",
                completion_proof_by_relation,
            ),
            (dataset_ids[1], endpoint_ids[1], "run-relation-fallback", {}),
        ],
    )


async def _insert_fixture_relation_rows(
    session,
    schema: str,
    fixture_ids: dict[str, list[str]],
) -> None:
    source_ids = fixture_ids["source_ids"]
    dataset_ids = fixture_ids["dataset_ids"]
    await session.execute(
        text(
            f"INSERT INTO {schema}.provider_directory_dataset_network_plan "
            "(dataset_id, network_resource_id, insurance_plan_resource_id) "
            "VALUES (:dataset_id, 'network-edge', 'plan-edge')"
        ),
        {"dataset_id": dataset_ids[0]},
    )
    await _insert_catalog_rows(
        session,
        schema,
        [
            (source_ids[0], "network-edge", "Edge Network"),
            (source_ids[1], "network-zero", "Zero Network"),
            (source_ids[2], "network-edge", "Edge Network"),
            (source_ids[3], "network-fallback", "Fallback Network"),
        ],
    )


async def _insert_relation_fixture(session, schema: str) -> dict[str, list[str]]:
    """Insert mixed relation-ready and legacy-fallback datasets for execution tests."""
    fixture_ids = _relation_fixture_ids()
    dataset_ids = fixture_ids["dataset_ids"]
    await _insert_fixture_ownership_rows(session, schema, fixture_ids)
    await _insert_resource_rows(
        session,
        schema,
        dataset_ids[0],
        _complete_resource_rows(),
    )
    await _insert_resource_rows(
        session,
        schema,
        dataset_ids[1],
        _fallback_resource_rows(),
    )
    await _insert_fixture_relation_rows(session, schema, fixture_ids)
    return fixture_ids


async def _delete_relation_fixture(session, schema: str, fixture: dict[str, list[str]]) -> None:
    await session.execute(
        text(
            f"DELETE FROM {schema}.provider_directory_dataset_network_plan "
            "WHERE dataset_id = ANY(CAST(:dataset_ids AS varchar[]))"
        ),
        {"dataset_ids": fixture["dataset_ids"]},
    )
    await session.execute(
        text(
            f"DELETE FROM {schema}.provider_directory_dataset_resource "
            "WHERE dataset_id = ANY(CAST(:dataset_ids AS varchar[]))"
        ),
        {"dataset_ids": fixture["dataset_ids"]},
    )
    await session.execute(
        text(
            f"DELETE FROM {schema}.provider_directory_endpoint_dataset "
            "WHERE dataset_id = ANY(CAST(:dataset_ids AS varchar[]))"
        ),
        {"dataset_ids": fixture["dataset_ids"]},
    )
    await session.execute(
        text(
            f"DELETE FROM {schema}.provider_directory_network_catalog "
            "WHERE source_id = ANY(CAST(:source_ids AS varchar[]))"
        ),
        {"source_ids": fixture["source_ids"]},
    )
    await session.execute(
        text(
            f"DELETE FROM {schema}.provider_directory_source "
            "WHERE source_id = ANY(CAST(:source_ids AS varchar[]))"
        ),
        {"source_ids": fixture["source_ids"]},
    )
    await session.execute(
        text(
            f"DELETE FROM {schema}.provider_directory_api_endpoint "
            "WHERE endpoint_id = ANY(CAST(:endpoint_ids AS varchar[]))"
        ),
        {"endpoint_ids": fixture["endpoint_ids"]},
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_provider_directory_role_evidence_sql_explains_with_array_binds():
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    sql = npi_module._provider_directory_role_evidence_sql(
        schema,
        has_catalog=True,
        has_dataset_network_plan=True,
        has_dataset_affiliation_organization=True,
        has_dataset_insurance_plan=True,
    )

    plan = await db.all(
        f"EXPLAIN {sql}",
        source_ids=["pdfhir_execution_test"],
        role_ids=["practitioner-role-execution-test"],
    )

    assert plan


@pytest.mark.asyncio(loop_scope="module")
async def test_provider_directory_affiliation_evidence_sql_explains_with_dataset_relation():
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    sql = npi_module._provider_directory_affiliation_evidence_sql(
        schema,
        has_catalog=True,
        has_dataset_network_plan=True,
        has_dataset_insurance_plan=True,
    )

    plan = await db.all(
        f"EXPLAIN {sql}",
        source_ids=["pdfhir_execution_test"],
        affiliation_ids=["organization-affiliation-execution-test"],
    )

    assert plan


@pytest.mark.asyncio(loop_scope="module")
async def test_role_evidence_executes_relation_fallback_and_zero_edge_contracts():
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    sql = npi_module._provider_directory_role_evidence_sql(
        schema,
        has_catalog=True,
        has_dataset_network_plan=True,
        has_dataset_affiliation_organization=True,
        has_dataset_insurance_plan=True,
    )

    async with db.transaction() as session:
        fixture = await _insert_relation_fixture(session, schema)
        try:
            evidence_rows = await db.all(
                sql,
                source_ids=fixture["source_ids"],
                role_ids=["role-edge", "role-zero", "role-direct", "role-fallback"],
            )
            evidence_key_set = {
                (
                    evidence_row._mapping["source_id"],
                    evidence_row._mapping["role_id"],
                    evidence_row._mapping["evidence_type"],
                    evidence_row._mapping["resource_id"],
                )
                for evidence_row in evidence_rows
            }

            assert (
                "pdfhir_relation_edge_test",
                "role-edge",
                "insurance_plan",
                "plan-edge",
            ) in evidence_key_set
            assert (
                "pdfhir_relation_fallback_test",
                "role-fallback",
                "insurance_plan",
                "plan-fallback",
            ) in evidence_key_set
            assert not any(
                source_id == "pdfhir_relation_zero_test"
                and evidence_type == "insurance_plan"
                for source_id, _role_id, evidence_type, _resource_id in evidence_key_set
            )
            assert (
                "pdfhir_relation_direct_test",
                "role-direct",
                "network",
                "network-edge",
            ) in evidence_key_set
        finally:
            await _delete_relation_fixture(session, schema, fixture)
