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


def _provider_directory_evidence_row(
    evidence_type,
    resource_id,
    provenance,
    identifier=None,
    name=None,
    reference=None,
):
    return {
        "source_id": "pdfhir_aetna",
        "role_id": "role-100",
        "evidence_type": evidence_type,
        "resource_id": resource_id,
        "identifier": identifier,
        "name": name,
        "reference": reference,
        "provenance": provenance,
    }


def test_provider_directory_role_evidence_sql_is_keyed_and_bounded():
    sql = npi_module._provider_directory_role_evidence_sql("mrf", has_catalog=True)

    assert "unnest(CAST(:source_ids AS varchar[]), CAST(:role_ids AS varchar[]))" in sql
    assert "role.source_id = requested.source_id AND role.resource_id = requested.role_id" in sql
    assert "role.active IS DISTINCT FROM false" in sql
    assert "role_organization.source_id = role.source_id" in sql
    assert "role_organization.resource_id = NULLIF(BTRIM(CASE" in sql
    assert "affiliation.source_id = role_organization.source_id" in sql
    assert "affiliation.participating_organization_ref" in sql
    assert "= role_organization.organization_resource_id" in sql
    assert "affiliation.active IS DISTINCT FROM false" in sql
    assert "COALESCE(affiliation.network_refs::jsonb, '[]'::jsonb)" in sql
    assert "'role'::varchar AS evidence_type" in sql
    assert "FROM roles AS role" in sql
    assert "insurance_plan.source_id = role.source_id" in sql
    assert "insurance_plan.resource_id = NULLIF(BTRIM(CASE" in sql
    assert "COALESCE(insurance_plan.network_refs::jsonb, '[]'::jsonb)" in sql
    assert sql.count(
        "COALESCE(NULLIF(LOWER(BTRIM(insurance_plan.status)), ''), 'active') = 'active'"
    ) == 2
    assert "role_network_organization.source_id = role_network.source_id" in sql
    assert "role_network_organization.resource_id = role_network.resource_id" in sql
    assert "FROM (SELECT DISTINCT source_id FROM valid_role_networks) AS requested_source" in sql
    assert "insurance_plan.source_id = requested_source.source_id" in sql
    assert "plan_network.resource_id = role_network.resource_id" in sql
    assert "BOOL_OR(role_network.plan_provenance = 'network-derived')" in sql
    assert "FROM direct_plans AS direct_plan" in sql
    assert "'organization-affiliation-network-derived'::varchar" in sql
    assert "'provider_directory_organization_affiliation'::varchar AS evidence_provenance" in sql
    assert "network_catalog.network_resource_id = network.resource_id" in sql
    assert "network_organization.resource_id = network.resource_id" in sql
    assert f"LIMIT {npi_module.MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_ROWS}" in sql
    assert "jsonb_set" not in sql
    assert "UPDATE " not in sql
    assert "owned_by" not in sql.lower()
    assert "administered_by" not in sql.lower()
    assert "affiliation.organization_ref" not in sql


def test_affiliation_mapping_has_provenance():
    evidence_map = npi_module._map_provider_directory_role_evidence(
        [
            _provider_directory_evidence_row(
                "insurance_plan", "plan-via-role-network", "network-derived", "AETNA-DIRECT"
            ),
            _provider_directory_evidence_row(
                "insurance_plan",
                "plan-via-affiliation",
                "organization-affiliation-network-derived",
                "AETNA-PLAN",
            ),
            _provider_directory_evidence_row(
                "network",
                "network-via-affiliation",
                "provider_directory_organization_affiliation",
                name="Aetna Choice POS II",
                reference="Organization/network-via-affiliation",
            ),
        ]
    )

    role_evidence = evidence_map[("pdfhir_aetna", "role-100")]
    assert role_evidence["insurance_plans"] == [
        {
            "resource_type": "InsurancePlan",
            "resource_id": "plan-via-role-network",
            "identifier": "AETNA-DIRECT",
            "provenance": "network-derived",
        },
        {
            "resource_type": "InsurancePlan",
            "resource_id": "plan-via-affiliation",
            "identifier": "AETNA-PLAN",
            "provenance": "organization-affiliation-network-derived",
        }
    ]
    assert role_evidence["networks"] == [
        {
            "resource_type": "Organization",
            "resource_id": "network-via-affiliation",
            "name": "Aetna Choice POS II",
            "reference": "Organization/network-via-affiliation",
            "provenance": "provider_directory_organization_affiliation",
        }
    ]


def test_affiliation_sql_requires_active_resources():
    sql = npi_module._provider_directory_role_evidence_sql(
        "mrf",
        has_catalog=False,
        has_affiliations=True,
    )

    assert "role_organization.active IS DISTINCT FROM false" in sql
    assert "affiliation.active IS DISTINCT FROM false" in sql
    assert "role_network_organization.active IS DISTINCT FROM false" in sql
    assert "role_network_organization.resource_id = role_network.resource_id" in sql
    assert "plan_network.resource_id = role_network.resource_id" in sql
    assert "plan_network.resource_id = affiliation.resource_id" not in sql
    assert "insurance_plan.organization_ref" not in sql


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
async def test_missing_affiliation_table_falls_back(monkeypatch):
    class FakeResult:
        def all(self):
            return []

    class FakeSession:
        def __init__(self):
            self.statements = []

        async def execute(self, statement, _params=None):
            self.statements.append(str(statement))
            return FakeResult()

    checked_tables = []

    async def has_table(table_name, *, session=None):
        assert session is evidence_session
        checked_tables.append(table_name)
        return table_name not in {
            "provider_directory_organization_affiliation",
            "provider_directory_network_catalog",
        }

    evidence_session = FakeSession()
    monkeypatch.setattr(npi_module, "_table_exists", has_table)

    evidence_map = await npi_module._fetch_provider_directory_role_evidence_map(
        [("pdfhir_example", "role-100")],
        session=evidence_session,
    )

    query_sql = next(
        statement for statement in evidence_session.statements if "requested_roles AS" in statement
    )
    assert evidence_map == {}
    assert "provider_directory_organization_affiliation AS affiliation" not in query_sql
    assert "direct_role_networks AS MATERIALIZED" in query_sql
    assert "FROM direct_plans AS direct_plan" in query_sql
    assert checked_tables == [
        "provider_directory_practitioner_role",
        "provider_directory_insurance_plan",
        "provider_directory_organization",
        "provider_directory_organization_affiliation",
        "provider_directory_network_catalog",
    ]


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
