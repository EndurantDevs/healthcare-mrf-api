# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
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
    payload_json = json.dumps(resource_payload)
    payload_hash = hashlib.sha256(payload_json.encode()).hexdigest()
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
                "resource_count, publication_metadata_json) "
                "VALUES (:dataset_id, :endpoint_id, :run_id, 'acquiring', false, "
                "0, CAST(:metadata AS jsonb))"
            ),
            {
                "dataset_id": dataset_id,
                "endpoint_id": endpoint_id,
                "run_id": run_id,
                "metadata": json.dumps(publication_metadata),
            },
        )


async def _publish_dataset_rows(
    session,
    schema: str,
    dataset_rows: list[tuple[str, int]],
) -> None:
    for dataset_id, resource_count in dataset_rows:
        await session.execute(
            text(
                f"UPDATE {schema}.provider_directory_endpoint_dataset "
                "SET status = 'validated', resource_count = :resource_count, "
                "validated_at = transaction_timestamp() "
                "WHERE dataset_id = :dataset_id"
            ),
            {
                "dataset_id": dataset_id,
                "resource_count": resource_count,
            },
        )
        await session.execute(
            text(
                f"UPDATE {schema}.provider_directory_endpoint_dataset "
                "SET status = 'published', is_current = true, "
                "published_at = transaction_timestamp() "
                "WHERE dataset_id = :dataset_id"
            ),
            {"dataset_id": dataset_id},
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


def _affiliation_resource_row(suffix: str) -> tuple[str, str, dict]:
    return (
        "OrganizationAffiliation",
        f"affiliation-{suffix}",
        {"active": True, "network_refs": [f"Organization/network-{suffix}"]},
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
        _affiliation_resource_row("edge"),
        _affiliation_resource_row("zero"),
    ]


def _fallback_resource_rows() -> list[tuple[str, str, dict]]:
    return [
        (
            "Organization",
            "network-fallback",
            {"active": True, "name": "Fallback Network"},
        ),
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
        _affiliation_resource_row("fallback"),
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


async def _insert_relation_fixture(session, schema: str, *, should_publish: bool = True) -> dict[str, list[str]]:
    """Load mixed datasets, deferring publication when more fixture rows are needed."""
    fixture_ids = _relation_fixture_ids()
    dataset_ids = fixture_ids["dataset_ids"]
    complete_resource_rows = _complete_resource_rows()
    fallback_resource_rows = _fallback_resource_rows()
    await _insert_fixture_ownership_rows(session, schema, fixture_ids)
    await _insert_resource_rows(
        session,
        schema,
        dataset_ids[0],
        complete_resource_rows,
    )
    await _insert_resource_rows(
        session,
        schema,
        dataset_ids[1],
        fallback_resource_rows,
    )
    await _insert_fixture_relation_rows(session, schema, fixture_ids)
    if should_publish:
        await _publish_dataset_rows(
            session,
            schema,
            [
                (dataset_ids[0], len(complete_resource_rows)),
                (dataset_ids[1], len(fallback_resource_rows)),
            ],
        )
    return fixture_ids


@pytest.mark.asyncio(loop_scope="session")
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


@pytest.mark.asyncio(loop_scope="session")
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


@pytest.mark.asyncio(loop_scope="session")
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
        fixture_savepoint = await session.begin_nested()
        try:
            fixture = await _insert_relation_fixture(session, schema)
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
                source_id == "pdfhir_relation_zero_test" and evidence_type == "insurance_plan"
                for source_id, _role_id, evidence_type, _resource_id in evidence_key_set
            )
            assert (
                "pdfhir_relation_direct_test",
                "role-direct",
                "network",
                "network-edge",
            ) in evidence_key_set
        finally:
            await fixture_savepoint.rollback()


@pytest.mark.asyncio(loop_scope="session")
async def test_affiliation_evidence_executes_relation_fallback_and_zero_edge_contracts():
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    sql = npi_module._provider_directory_affiliation_evidence_sql(
        schema,
        has_catalog=True,
        has_dataset_network_plan=True,
        has_dataset_insurance_plan=True,
    )

    async with db.transaction() as session:
        fixture_savepoint = await session.begin_nested()
        try:
            fixture = await _insert_relation_fixture(session, schema)
            edge, zero, _unrequested_alias, fallback = fixture["source_ids"]
            evidence_rows = await db.all(
                sql,
                source_ids=[edge, zero, fallback],
                affiliation_ids=["affiliation-edge", "affiliation-zero", "affiliation-fallback"],
            )
            mappings = [entry._mapping for entry in evidence_rows]
            assert [
                (
                    entry["source_id"],
                    entry["affiliation_id"],
                    entry["evidence_type"],
                    entry["resource_id"],
                )
                for entry in mappings
            ] == [
                (edge, "affiliation-edge", "affiliation", "affiliation-edge"),
                (fallback, "affiliation-fallback", "affiliation", "affiliation-fallback"),
                (zero, "affiliation-zero", "affiliation", "affiliation-zero"),
                (edge, "affiliation-edge", "insurance_plan", "plan-edge"),
                (edge, "affiliation-edge", "network", "network-edge"),
                (fallback, "affiliation-fallback", "insurance_plan", "plan-fallback"),
                (fallback, "affiliation-fallback", "network", "network-fallback"),
                (zero, "affiliation-zero", "network", "network-zero"),
            ]
            affiliations = [entry for entry in mappings if entry["evidence_type"] == "affiliation"]
            assert [entry["plan_returned"] for entry in affiliations] == [1, 1, 0]
            assert [entry["plan_total"] for entry in affiliations] == [1, 1, 0]
            assert all(entry["plan_truncated"] is False for entry in affiliations)
            assert all(entry["catalog_complete"] is True for entry in affiliations)
            assert all(entry["evidence_row_total"] == 8 for entry in mappings)
        finally:
            await fixture_savepoint.rollback()


async def _insert_endpoint_subset_fixture(session, schema: str) -> str:
    fixture = await _insert_relation_fixture(session, schema, should_publish=False)
    local_dataset, foreign_dataset = fixture["dataset_ids"]
    local_payload_map = {
        "active": True,
        "network_refs": ["https://relation-fallback.test/fhir/Organization/network-edge"],
    }
    foreign_payload_map = {
        "active": True,
        "network_refs": ["https://relation-complete.test/fhir/Organization/network-edge"],
    }
    local_resources = [
        ("PractitionerRole", "role-subset", local_payload_map),
        ("OrganizationAffiliation", "affiliation-subset", local_payload_map),
    ]
    foreign_resources = [
        ("Organization", "network-edge", {"active": True, "name": "Foreign Network"}),
        (
            "InsurancePlan",
            "plan-edge",
            {
                "status": "active",
                "name": "Foreign Plan",
                "plan_identifier": "foreign-plan",
                "network_refs": foreign_payload_map["network_refs"],
            },
        ),
        ("PractitionerRole", "role-subset", foreign_payload_map),
        ("OrganizationAffiliation", "affiliation-subset", foreign_payload_map),
        ("PractitionerRole", "role-foreign-only", foreign_payload_map),
        ("OrganizationAffiliation", "affiliation-foreign-only", foreign_payload_map),
    ]
    await _insert_resource_rows(session, schema, local_dataset, local_resources)
    await _insert_resource_rows(session, schema, foreign_dataset, foreign_resources)
    await _insert_catalog_rows(session, schema, [(fixture["source_ids"][3], "network-edge", "Foreign Network")])
    await _publish_dataset_rows(
        session,
        schema,
        [
            (local_dataset, len(_complete_resource_rows()) + len(local_resources)),
            (foreign_dataset, len(_fallback_resource_rows()) + len(foreign_resources)),
        ],
    )
    return fixture["source_ids"][0]


@pytest.mark.parametrize("evidence_kind", ["role", "affiliation"])
@pytest.mark.asyncio(loop_scope="session")
async def test_evidence_subset_does_not_cross_endpoint_datasets(evidence_kind):
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    builder = (
        npi_module._provider_directory_role_evidence_sql
        if evidence_kind == "role"
        else npi_module._provider_directory_affiliation_evidence_sql
    )
    sql = builder(schema, has_catalog=True, has_dataset_network_plan=True, has_dataset_insurance_plan=True)

    async with db.transaction() as session:
        fixture_savepoint = await session.begin_nested()
        try:
            local_source = await _insert_endpoint_subset_fixture(session, schema)
            evidence_rows = await db.all(
                sql,
                source_ids=[local_source],
                **{f"{evidence_kind}_ids": [f"{evidence_kind}-subset", f"{evidence_kind}-foreign-only"]},
            )
            mappings = [entry._mapping for entry in evidence_rows]
            assert [(entry["evidence_type"], entry["resource_id"], entry["name"]) for entry in mappings] == [
                (evidence_kind, f"{evidence_kind}-subset", None),
                ("insurance_plan", "plan-edge", None),
                ("network", "network-edge", "Edge Network"),
            ]
            plan = next(entry for entry in mappings if entry["evidence_type"] == "insurance_plan")
            assert plan["identifier"] == "edge-plan"
            plan_payload = plan["plan_payload_json"]
            if isinstance(plan_payload, str):
                plan_payload = json.loads(plan_payload)
            assert plan_payload == {"status": "active", "name": "Edge Plan"}
            assert all(entry["source_id"] == local_source for entry in mappings)
            assert all(entry[f"{evidence_kind}_id"] == f"{evidence_kind}-subset" for entry in mappings)
            assert all(entry["evidence_row_total"] == 3 for entry in mappings)
        finally:
            await fixture_savepoint.rollback()
