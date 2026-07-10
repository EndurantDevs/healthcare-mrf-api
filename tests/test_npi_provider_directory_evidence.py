# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi as npi_module


@pytest.fixture
def provider_directory_plan_network_fixture():
    source_id, role_id, plan_id = "pdfhir_example", "role-100", "plan-200"
    network_id, location_id = "network-300", "location-400"
    return {
        "role_key": (source_id, role_id),
        "address": {
            "type": "practice",
            "address_key": "00000000-0000-0000-0000-000000000111",
            "address_precision": "street",
            "first_line": "100 Main St",
            "city_name": "Chicago",
            "state_name": "IL",
            "postal_code": "60601",
            "country_code": "US",
            "lat": 41.8818,
            "long": -87.6232,
            "checksum": 100,
            "address_sources": ["provider_directory_fhir"],
            "source_record_ids": [
                f"provider_directory_fhir:practitioner_role:{source_id}:{role_id}:{location_id}"
            ],
            "aca_plan_array": ["H1234-001"],
            "aca_network_array": ["Example Choice Network"],
        },
        "source_detail_map": {
            source_id: {
                "source": "provider_directory_fhir",
                "source_id": source_id,
                "endpoint_id": "pd_endpoint_example",
                "canonical_api_base": "https://example.test/fhir",
                "org_name": "Example Health",
                "plan_name": "Example Directory",
            }
        },
        "role_evidence_map": {
            (source_id, role_id): {
                "insurance_plans": [
                    {
                        "resource_type": "InsurancePlan",
                        "resource_id": plan_id,
                        "identifier": "H1234-001",
                    }
                ],
                "networks": [
                    {
                        "resource_type": "Organization",
                        "resource_id": network_id,
                        "name": "Example Choice Network",
                        "reference": f"Organization/{network_id}",
                        "provenance": "provider_directory_network_catalog",
                    }
                ],
            }
        },
    }


def test_provider_directory_role_evidence_sql_is_keyed_and_bounded():
    sql = npi_module._provider_directory_role_evidence_sql("mrf", has_catalog=True)

    assert "unnest(CAST(:source_ids AS varchar[]), CAST(:role_ids AS varchar[]))" in sql
    assert "role.source_id = requested.source_id AND role.resource_id = requested.role_id" in sql
    assert "'role'::varchar AS evidence_type" in sql
    assert "FROM roles AS role" in sql
    assert "insurance_plan.source_id = role.source_id" in sql
    assert "insurance_plan.resource_id = NULLIF(BTRIM(CASE" in sql
    assert "COALESCE(insurance_plan.network_refs::jsonb, '[]'::jsonb)" in sql
    assert "role_network_organization.source_id = role_network.source_id" in sql
    assert "role_network_organization.resource_id = role_network.resource_id" in sql
    assert "insurance_plan.source_id = requested_source.source_id" in sql
    assert "plan_network.resource_id = role_network.resource_id" in sql
    assert "FROM direct_plans AS direct_plan" in sql
    assert "'network-derived'::varchar AS provenance" in sql
    assert "network_catalog.network_resource_id = network.resource_id" in sql
    assert "network_organization.resource_id = network.resource_id" in sql
    assert f"LIMIT {npi_module.MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_ROWS}" in sql
    assert "jsonb_set" not in sql
    assert "UPDATE " not in sql


def test_network_matched_plan_is_labeled_as_network_derived():
    evidence_map = npi_module._map_provider_directory_role_evidence(
        [
            {
                "source_id": "pdfhir_example",
                "role_id": "role-100",
                "evidence_type": "insurance_plan",
                "resource_id": "plan-via-network",
                "identifier": "NETWORK-PLAN",
                "name": None,
                "reference": None,
                "provenance": "network-derived",
            }
        ]
    )

    assert evidence_map[("pdfhir_example", "role-100")]["insurance_plans"] == [
        {
            "resource_type": "InsurancePlan",
            "resource_id": "plan-via-network",
            "identifier": "NETWORK-PLAN",
            "provenance": "network-derived",
        }
    ]


def test_unmatched_role_network_does_not_add_plan_evidence():
    evidence_map = npi_module._map_provider_directory_role_evidence(
        [
            {
                "source_id": "pdfhir_example",
                "role_id": "role-100",
                "evidence_type": "role",
                "resource_id": "role-100",
                "identifier": None,
                "name": None,
                "reference": None,
                "provenance": "provider_directory_practitioner_role",
            },
            {
                "source_id": "pdfhir_example",
                "role_id": "role-100",
                "evidence_type": "network",
                "resource_id": "unmatched-network",
                "identifier": None,
                "name": "Unmatched Network",
                "reference": "Organization/unmatched-network",
                "provenance": "provider_directory_organization",
            },
        ]
    )

    role_evidence = evidence_map[("pdfhir_example", "role-100")]
    assert role_evidence["insurance_plans"] == []
    assert role_evidence["networks"][0]["resource_id"] == "unmatched-network"


def test_direct_plan_reference_keeps_legacy_plan_evidence_shape():
    evidence_map = npi_module._map_provider_directory_role_evidence(
        [
            {
                "source_id": "pdfhir_example",
                "role_id": "role-100",
                "evidence_type": "insurance_plan",
                "resource_id": "direct-plan",
                "identifier": "DIRECT-PLAN",
                "name": None,
                "reference": None,
                "provenance": "provider_directory_insurance_plan",
            }
        ]
    )

    assert evidence_map[("pdfhir_example", "role-100")]["insurance_plans"] == [
        {
            "resource_type": "InsurancePlan",
            "resource_id": "direct-plan",
            "identifier": "DIRECT-PLAN",
        }
    ]


def test_provider_directory_role_marker_retains_roles_without_plan_or_network_refs():
    evidence_map = npi_module._map_provider_directory_role_evidence(
        [
            {
                "source_id": "pdfhir_example",
                "role_id": "role-100",
                "evidence_type": "role",
                "resource_id": "role-100",
                "identifier": None,
                "name": None,
                "reference": None,
                "provenance": "provider_directory_practitioner_role",
            }
        ]
    )

    assert evidence_map == {
        ("pdfhir_example", "role-100"): {"insurance_plans": [], "networks": []}
    }


def test_provider_directory_role_evidence_keys_do_not_require_existing_aca_arrays():
    addresses = [
        {
            "source_record_ids": [
                "provider_directory_fhir:practitioner_role:pdfhir_example:role-100:location-400"
            ],
            "aca_plan_array": [],
            "aca_network_array": [],
        }
    ]

    assert npi_module._provider_directory_role_keys_from_addresses(addresses) == [
        ("pdfhir_example", "role-100")
    ]


@pytest.mark.asyncio
async def test_role_evidence_disables_jit_once_per_request_session(monkeypatch):
    class FakeResult:
        def all(self):
            return []

    class FakeSession:
        def __init__(self):
            self.statements = []

        async def execute(self, statement, _params=None):
            self.statements.append(str(statement))
            return FakeResult()

    session = FakeSession()
    monkeypatch.setattr(npi_module, "_table_exists", AsyncMock(return_value=True))

    await npi_module._fetch_provider_directory_role_evidence_map(
        [("pdfhir_example", "role-100")],
        session=session,
    )
    await npi_module._fetch_provider_directory_role_evidence_map(
        [("pdfhir_example", "role-100")],
        session=session,
    )

    assert session.statements.count("SET LOCAL jit = off") == 1
    assert sum("requested_roles AS" in statement for statement in session.statements) == 2


@pytest.mark.asyncio
async def test_get_npi_exposes_resolved_provider_directory_plan_network_evidence(
    monkeypatch,
    provider_directory_plan_network_fixture,
):
    fixture = provider_directory_plan_network_fixture
    role_key = fixture["role_key"]

    async def fake_build(npi, **_kwargs):
        return {
            "npi": npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "address_list": [dict(fixture["address"])],
            "do_business_as": [],
        }

    fetch_role_evidence = AsyncMock(return_value=fixture["role_evidence_map"])
    patch_map = {
        "_build_npi_details": fake_build,
        "_fetch_provider_directory_address_overlay": AsyncMock(return_value=[]),
        "_fetch_provider_directory_source_detail_map": AsyncMock(
            return_value=fixture["source_detail_map"]
        ),
        "_fetch_provider_directory_role_evidence_map": fetch_role_evidence,
        "_fetch_other_names": AsyncMock(return_value=[]),
        "_fetch_provider_enrichment_detail": AsyncMock(return_value=None),
    }
    for function_name, replacement in patch_map.items():
        monkeypatch.setattr(npi_module, function_name, replacement)

    app = types.SimpleNamespace(
        config={"NPI_API_UPDATE_GEOCODE": False},
        add_task=lambda _coro: None,
    )
    request = types.SimpleNamespace(
        args={"include_sources": "true", "include_evidence": "true"},
        app=app,
    )
    response = await npi_module.get_npi(request, "1518379601")
    address = json.loads(response.body)["address_list"][0]
    evidence = address["provider_directory_sources"][0]

    fetch_role_evidence.assert_awaited_once_with([role_key], session=None)
    assert address["aca_plan_array"] == ["H1234-001"]
    assert address["aca_network_array"] == ["Example Choice Network"]
    assert evidence["source_ids"] == [role_key[0]]
    assert evidence["practitioner_role_ids"] == [role_key[1]]
    assert evidence["insurance_plans"][0]["identifier"] == "H1234-001"
    assert evidence["networks"][0] == fixture["role_evidence_map"][role_key]["networks"][0]
