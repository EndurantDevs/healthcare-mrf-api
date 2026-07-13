# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

import pytest
from sqlalchemy import text

from api.endpoint import npi as npi_module
from db.connection import Database
from tests.test_npi_provider_directory_role_evidence_db import (
    _delete_relation_fixture,
    _insert_catalog_rows,
    _insert_dataset_rows,
    _insert_endpoint_rows,
    _insert_resource_rows,
    _insert_source_rows,
    _requires_test_database,
)


ENDPOINT_ID = "pd-plan-cap"
SOURCE_ID = "pdfhir_plan_cap_test"
DATASET_ID = "dataset-plan-cap"
ROLE_ID = "role-plan-cap"
DERIVED_PLAN_IDS = tuple(f"plan-derived-{index:03d}" for index in range(130))
DIRECT_PLAN_IDS = tuple(f"plan-direct-{index:03d}" for index in range(5))


def _plan_resource_rows() -> list[tuple[str, str, dict]]:
    """Build a mixed direct and network-derived plan fixture."""
    return [
        ("Organization", "network-cap", {"active": True, "name": "Cap Network"}),
        (
            "PractitionerRole",
            ROLE_ID,
            {
                "active": True,
                "network_refs": ["Organization/network-cap"],
                "insurance_plan_refs": [
                    f"InsurancePlan/{plan_id}" for plan_id in DIRECT_PLAN_IDS
                ],
            },
        ),
        *[
            (
                "InsurancePlan",
                plan_id,
                {
                    "status": (
                        "inactive" if plan_id == DERIVED_PLAN_IDS[0] else "active"
                    ),
                    "name": f"Derived {plan_id}",
                    "plan_identifier": plan_id,
                    "network_refs": ["Organization/network-cap"],
                },
            )
            for plan_id in DERIVED_PLAN_IDS
        ],
        *[
            (
                "InsurancePlan",
                plan_id,
                {
                    "status": "active",
                    "name": f"Direct {plan_id}",
                    "plan_identifier": plan_id,
                    "network_refs": [],
                },
            )
            for plan_id in DIRECT_PLAN_IDS
        ],
    ]


async def _insert_plan_cap_ownership(session, schema: str) -> None:
    """Create one current endpoint dataset with complete serving relations."""
    await _insert_endpoint_rows(
        session,
        schema,
        [(ENDPOINT_ID, "https://plan-cap.test/fhir")],
    )
    await _insert_source_rows(session, schema, [(SOURCE_ID, ENDPOINT_ID)])
    relation_proof_by_name = {
        relation_name: {
            "complete": True,
            "version": "1",
            "dataset_id": DATASET_ID,
        }
        for relation_name in (
            "dataset_network_plan",
            "dataset_affiliation_organization",
        )
    }
    await _insert_dataset_rows(
        session,
        schema,
        [(DATASET_ID, ENDPOINT_ID, "run-plan-cap", relation_proof_by_name)],
    )


async def _insert_plan_cap_fixture(session, schema: str) -> None:
    """Materialize the ownership, resources, edges, and source catalog."""
    await _insert_plan_cap_ownership(session, schema)
    await _insert_resource_rows(session, schema, DATASET_ID, _plan_resource_rows())
    await session.execute(
        text(
            f"INSERT INTO {schema}.provider_directory_dataset_network_plan "
            "(dataset_id, network_resource_id, insurance_plan_resource_id) "
            "VALUES (:dataset_id, 'network-cap', :plan_id)"
        ),
        [
            {"dataset_id": DATASET_ID, "plan_id": plan_id}
            for plan_id in DERIVED_PLAN_IDS
        ],
    )
    await _insert_catalog_rows(
        session,
        schema,
        [(SOURCE_ID, "network-cap", "Cap Network")],
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_role_evidence_caps_payloads_but_keeps_exact_plan_total():
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    evidence_sql = npi_module._provider_directory_role_evidence_sql(
        schema,
        has_catalog=True,
        has_dataset_network_plan=True,
        has_dataset_affiliation_organization=True,
        has_dataset_insurance_plan=True,
        has_dataset_insurance_plan_scalars=True,
    )
    cleanup_scope_by_field = {
        "endpoint_ids": [ENDPOINT_ID],
        "source_ids": [SOURCE_ID],
        "dataset_ids": [DATASET_ID],
    }

    database = Database()
    await database.connect()
    try:
        async with database.transaction() as session:
            await _insert_plan_cap_fixture(session, schema)
            evidence_result = await session.execute(
                text(evidence_sql),
                {
                    "source_ids": [SOURCE_ID],
                    "role_ids": [ROLE_ID],
                },
            )
            evidence_rows = evidence_result.all()
            role_evidence_by_field = next(
                evidence_row._mapping
                for evidence_row in evidence_rows
                if evidence_row._mapping["evidence_type"] == "role"
            )
            returned_plan_id_set = {
                evidence_row._mapping["resource_id"]
                for evidence_row in evidence_rows
                if evidence_row._mapping["evidence_type"] == "insurance_plan"
            }

            assert role_evidence_by_field["plan_returned"] == 100
            assert role_evidence_by_field["plan_total"] == 134
            assert role_evidence_by_field["plan_truncated"] is True
            assert len(returned_plan_id_set) == 100
            assert set(DIRECT_PLAN_IDS) <= returned_plan_id_set
            assert returned_plan_id_set & set(DERIVED_PLAN_IDS) == set(
                DERIVED_PLAN_IDS[1:96]
            )
            await _delete_relation_fixture(
                session,
                schema,
                cleanup_scope_by_field,
            )
    finally:
        await database.disconnect()
