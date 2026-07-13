# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi as npi_module


@pytest.fixture
def provider_directory_fixture():
    source_id, role_id = "pdfhir_example", "role-100"
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
                f"provider_directory_fhir:practitioner_role:{source_id}:{role_id}:location-400"
            ],
        },
        "source_detail_map": {
            source_id: {
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
                        "resource_id": "plan-200",
                        "identifier": "H1234-001",
                    }
                ],
                "networks": [
                    {
                        "resource_type": "Organization",
                        "resource_id": "network-300",
                        "name": "Example Choice Network",
                        "reference": "Organization/network-300",
                        "provenance": "provider_directory_network_catalog",
                    }
                ],
                "insurance_plan_metadata": {
                    "returned": 1,
                    "total": 1,
                    "truncated": False,
                    "catalog_complete": True,
                },
            }
        },
    }


def _evidence_row(evidence_type, resource_id, provenance, **fields):
    evidence_row_map = {
        "source_id": "pdfhir_aetna",
        "role_id": "role-100",
        "evidence_type": evidence_type,
        "resource_id": resource_id,
        "identifier": None,
        "name": None,
        "reference": None,
        "provenance": provenance,
    }
    evidence_row_map.update(fields)
    return evidence_row_map


class _EvidenceResult:
    def __init__(self, evidence_rows=()):
        self.evidence_rows = list(evidence_rows)

    def all(self):
        return self.evidence_rows


class _EvidenceSession:
    def __init__(self, unavailable_table_names=()):
        self.statements = []
        self.unavailable_table_names = set(unavailable_table_names)
        self.requested_table_names = []

    async def execute(self, statement, params=None):
        self.statements.append(str(statement))
        if "to_regclass" in str(statement) and params:
            self.requested_table_names = list(params["table_names"])
            return _EvidenceResult(
                [
                    {"table_name": table_name, "is_available": table_name not in self.unavailable_table_names}
                    for table_name in self.requested_table_names
                ]
            )
        return _EvidenceResult()


def test_role_evidence_sql_uses_indexed_catalog_lookup_and_active_resources():
    sql = npi_module._provider_directory_role_evidence_sql("mrf", has_catalog=True)

    assert "dataset.is_current IS TRUE" in sql
    assert "dataset.status = 'published'" in sql
    assert "dataset.published_at IS NOT NULL" in sql
    assert "resource.dataset_id = dataset.dataset_id" in sql
    assert "current_resources AS NOT MATERIALIZED" in sql
    assert "resource.payload_json::jsonb AS payload_json" in sql
    assert "current_roles AS NOT MATERIALIZED" in sql
    assert "resource.resource_type = 'PractitionerRole'" in sql
    assert "unnest(CAST(:source_ids AS varchar[]), CAST(:role_ids AS varchar[]))" in sql
    assert "role.source_id = requested.source_id AND role.resource_id = requested.role_id" in sql
    assert "JOIN current_roles AS role" in sql
    assert "provider_directory_practitioner_role AS role" not in sql
    assert "current_insurance_plans AS NOT MATERIALIZED" in sql
    assert "JOIN current_insurance_plans AS insurance_plan" in sql
    assert "insurance_plan.last_seen_run_id" not in sql
    assert "role.active IS DISTINCT FROM false" in sql
    assert "role_organization.active IS DISTINCT FROM false" in sql
    assert "affiliation.active IS DISTINCT FROM false" in sql
    assert "role_network_organization.active IS DISTINCT FROM false" in sql
    assert "(role_organization.organization_ref::varchar)" in sql
    assert "(role_organization.organization_resource_id::varchar)" in sql
    assert "'Organization/' || role_organization.organization_resource_id" in sql
    assert "affiliation_locator.source_id = organization_candidate.source_id" in sql
    assert "affiliation_locator.participating_organization_ref = organization_candidate.reference" in sql
    assert "JOIN current_affiliations AS affiliation" in sql
    assert "regexp_replace(affiliation.participating_organization_ref" not in sql
    assert "network_catalog.refs" not in sql
    assert "jsonb_array_elements(network_catalog.refs" not in sql
    assert "catalog_plan_candidates" not in sql
    assert "COALESCE(insurance_plan.network_refs::jsonb, '[]'::jsonb)" in sql
    assert "insurance_plan.source_id = role_network.source_id" in sql
    assert "plan_network_ref.value = role_network.reference" in sql
    assert "role_network.resource_id" in sql
    assert "FROM (SELECT DISTINCT source_id FROM valid_role_networks)" not in sql
    active_plan_sql = "COALESCE(NULLIF(LOWER(BTRIM(insurance_plan.status)), ''), 'active') = 'active'"
    assert sql.count(active_plan_sql) == 2
    assert "network_catalog.network_resource_id = role_network.resource_id" in sql
    assert "network_catalog.provider_directory_network_name" in sql
    assert "catalog_status.catalog_complete" in sql
    assert "FROM direct_plans AS direct_plan" in sql
    assert "direct_plan.resource_id = insurance_plan.resource_id" in sql
    assert "BOOL_OR(role_network.plan_provenance = 'network-derived')" in sql
def test_role_plan_cap_prioritizes_direct_plans_and_reports_both_bounds():
    sql = npi_module._provider_directory_role_evidence_sql("mrf", has_catalog=True)
    cap = npi_module.MAX_PROVIDER_DIRECTORY_PLANS_PER_ROLE

    direct_first = (
        "PARTITION BY source_id, role_id\n"
        "                   ORDER BY\n"
        "                       CASE WHEN provenance = "
        "'provider_directory_insurance_plan' THEN 0 ELSE 1 END,\n"
        "                       resource_id, identifier NULLS LAST, provenance"
    )
    assert direct_first in sql
    assert f"WHERE plan_rank <= {cap}" in sql
    assert "plan_counts_by_role AS MATERIALIZED" in sql and f"LEAST(COALESCE(plan_count.plan_total, 0), {cap})" in sql
    assert "CASE WHEN catalog_status.catalog_complete" in sql
    assert "evidence_count AS MATERIALIZED" in sql
    assert "CASE WHEN evidence_type = 'role' THEN 0 ELSE 1 END" in sql
    assert npi_module.MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_ROWS == 8192
    assert f"LIMIT {npi_module.MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_ROWS}" in sql


def test_incomplete_catalog_keeps_direct_evidence_with_unknown_total():
    evidence_rows = [
        _evidence_row(
            "role",
            "role-100",
            "provider_directory_practitioner_role",
            plan_returned=1,
            plan_total=None,
            plan_truncated=None,
            catalog_complete=False,
        ),
        _evidence_row(
            "insurance_plan",
            "direct-plan",
            "provider_directory_insurance_plan",
            identifier="DIRECT",
        ),
        _evidence_row(
            "network",
            "network-1",
            "provider_directory_organization",
            name="Direct Network",
            reference="Organization/network-1",
        ),
    ]

    role_evidence = npi_module._map_provider_directory_role_evidence(evidence_rows)[
        ("pdfhir_aetna", "role-100")
    ]
    assert [entry["resource_id"] for entry in role_evidence["insurance_plans"]] == [
        "direct-plan"
    ]
    assert "provenance" not in role_evidence["insurance_plans"][0]
    assert [entry["resource_id"] for entry in role_evidence["networks"]] == ["network-1"]
    assert role_evidence["insurance_plan_metadata"] == {
        "returned": 1,
        "total": None,
        "truncated": None,
        "catalog_complete": False,
    }


def test_mapper_preserves_cap_and_global_truncation_metadata():
    evidence_rows = [
        _evidence_row(
            "role",
            "role-100",
            "provider_directory_practitioner_role",
            plan_returned=1,
            plan_total=1000,
            plan_truncated=True,
            catalog_complete=True,
            evidence_row_total=9000,
        ),
        _evidence_row(
            "insurance_plan",
            "plan-1",
            "network-derived",
            identifier="PLAN-1",
            evidence_row_total=9000,
        ),
    ]

    role_evidence = npi_module._map_provider_directory_role_evidence(evidence_rows)[
        ("pdfhir_aetna", "role-100")
    ]
    assert role_evidence["insurance_plan_metadata"] == {
        "returned": 1,
        "total": 1000,
        "truncated": True,
        "catalog_complete": True,
    }
    assert role_evidence["insurance_plans"] == [
        {
            "resource_type": "InsurancePlan",
            "resource_id": "plan-1",
            "identifier": "PLAN-1",
            "provenance": "network-derived",
        }
    ]
    assert role_evidence["evidence_metadata"] == {
        "returned": 2,
        "total": 9000,
        "truncated": True,
    }


def test_high_cardinality_mapping_is_linear_and_stable(monkeypatch):
    evidence_rows = []
    for plan_number in range(3000):
        plan_row = _evidence_row(
            "insurance_plan",
            f"plan-{plan_number:04d}",
            "network-derived",
            identifier=f"PLAN-{plan_number:04d}",
        )
        evidence_rows.extend((plan_row, dict(plan_row)))
    monkeypatch.setattr(
        npi_module.json,
        "dumps",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("JSON dedup used")),
    )

    plans = npi_module._map_provider_directory_role_evidence(evidence_rows)[
        ("pdfhir_aetna", "role-100")
    ]["insurance_plans"]
    assert len(plans) == 3000
    assert (plans[0]["resource_id"], plans[-1]["resource_id"]) == (
        "plan-0000",
        "plan-2999",
    )


@pytest.mark.asyncio
async def test_role_evidence_disables_jit_once_per_request_session():
    evidence_session = _EvidenceSession()
    for _attempt in range(2):
        await npi_module._fetch_provider_directory_role_evidence_map(
            [("pdfhir_example", "role-100")],
            session=evidence_session,
        )
    assert evidence_session.statements.count("SET LOCAL jit = off") == 1
    assert sum("requested_roles AS" in statement for statement in evidence_session.statements) == 2


@pytest.mark.asyncio
async def test_missing_affiliation_and_catalog_tables_keep_direct_path():
    evidence_session = _EvidenceSession({
        "provider_directory_organization_affiliation",
        "provider_directory_network_catalog",
        "provider_directory_dataset_network_plan",
        "provider_directory_dataset_affiliation_organization",
    })

    evidence_map = await npi_module._fetch_provider_directory_role_evidence_map(
        [("pdfhir_example", "role-100")],
        session=evidence_session,
    )

    query_sql = next(
        statement for statement in evidence_session.statements if "requested_roles AS" in statement
    )
    assert evidence_map == {}
    assert "provider_directory_organization_affiliation AS affiliation" not in query_sql
    assert "provider_directory_network_catalog" not in query_sql
    assert "FROM direct_plans AS direct_plan" in query_sql
    assert evidence_session.requested_table_names == [
        "provider_directory_source",
        "provider_directory_endpoint_dataset",
        "provider_directory_dataset_resource",
        "provider_directory_practitioner_role",
        "provider_directory_insurance_plan",
        "provider_directory_organization",
        "provider_directory_organization_affiliation",
        "provider_directory_network_catalog",
        "provider_directory_dataset_network_plan",
        "provider_directory_dataset_affiliation_organization",
        "provider_directory_dataset_insurance_plan",
    ]


@pytest.mark.asyncio
async def test_same_source_roles_are_isolated_to_exact_addresses(monkeypatch):
    source_id = "pdfhir_aetna"
    role_keys = [(source_id, "role-a"), (source_id, "role-b")]
    addresses = [
        {
            "source_record_ids": [
                f"provider_directory_fhir:practitioner_role:{source_id}:{role_id}:location-1"
            ]
        }
        for _source_id, role_id in role_keys
    ]
    detail_by_id = {
        source_id: {
            "source_id": source_id,
            "endpoint_id": "pd_endpoint_aetna",
            "canonical_api_base": "https://example.test/fhir",
        }
    }
    role_evidence_map = {
        role_key: {
            "insurance_plans": [
                {
                    "resource_type": "InsurancePlan",
                    "resource_id": f"plan-{role_key[1]}",
                    "identifier": role_key[1].upper(),
                }
            ],
            "networks": [],
        }
        for role_key in role_keys
    }
    fetch_roles = AsyncMock(return_value=role_evidence_map)
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_directory_source_detail_map",
        AsyncMock(return_value=detail_by_id),
    )
    monkeypatch.setattr(npi_module, "_fetch_provider_directory_role_evidence_map", fetch_roles)

    await npi_module._attach_provider_directory_source_details(
        addresses,
        include_role_evidence=True,
    )

    fetch_roles.assert_awaited_once_with(role_keys, session=None)
    for address, role_key in zip(addresses, role_keys):
        evidence = address["provider_directory_sources"][0]
        assert evidence["practitioner_role_ids"] == [role_key[1]]
        assert [entry["resource_id"] for entry in evidence["insurance_plans"]] == [
            f"plan-{role_key[1]}"
        ]


def test_overlapping_multi_role_plans_keep_role_keyed_metadata():
    source_id = "pdfhir_aetna"
    role_keys = [(source_id, "role-a"), (source_id, "role-b")]
    detail_by_id = {source_id: {"source_id": source_id, "endpoint_id": "pd_endpoint_aetna"}}
    shared_plan_map = {
        "resource_type": "InsurancePlan",
        "resource_id": "shared-plan",
        "identifier": "SHARED",
    }
    role_evidence_map = {
        role_key: {
            "insurance_plans": [dict(shared_plan_map)],
            "networks": [],
            "insurance_plan_metadata": {
                "returned": 1,
                "total": 1,
                "truncated": False,
                "catalog_complete": True,
            },
        }
        for role_key in role_keys
    }

    evidence = npi_module._provider_directory_endpoint_provenance(
        [source_id], detail_by_id, role_evidence_map, role_keys
    )[0]

    assert evidence["insurance_plans"] == [shared_plan_map]
    assert "insurance_plan_metadata" not in evidence
    assert [
        (entry["practitioner_role_id"], entry["total"])
        for entry in evidence["insurance_plan_metadata_by_role"]
    ] == [("role-a", 1), ("role-b", 1)]


def _patch_detail_endpoint(monkeypatch, fixture, role_evidence_mock):
    async def fake_build(npi, **_kwargs):
        return {
            "npi": npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "address_list": [dict(fixture["address"])],
            "do_business_as": [],
        }

    replacement_map = {
        "_build_npi_details": fake_build,
        "_fetch_provider_directory_address_overlay": AsyncMock(return_value=[]),
        "_fetch_provider_directory_source_detail_map": AsyncMock(
            return_value=fixture["source_detail_map"]
        ),
        "_fetch_provider_directory_role_evidence_map": role_evidence_mock,
        "_fetch_other_names": AsyncMock(return_value=[]),
        "_fetch_provider_enrichment_detail": AsyncMock(return_value=None),
    }
    for function_name, replacement in replacement_map.items():
        monkeypatch.setattr(npi_module, function_name, replacement)


def _detail_request(**args):
    app = types.SimpleNamespace(
        config={"NPI_API_UPDATE_GEOCODE": False},
        add_task=lambda _coro: None,
    )
    return types.SimpleNamespace(args=args, app=app)


@pytest.mark.asyncio
async def test_include_evidence_runs_full_role_resolution(monkeypatch, provider_directory_fixture):
    fixture = provider_directory_fixture
    fetch_roles = AsyncMock(return_value=fixture["role_evidence_map"])
    _patch_detail_endpoint(monkeypatch, fixture, fetch_roles)

    response = await npi_module.get_npi(
        _detail_request(include_sources="true", include_evidence="true"),
        "1518379601",
    )
    evidence = json.loads(response.body)["address_list"][0]["provider_directory_sources"][0]

    fetch_roles.assert_awaited_once_with([fixture["role_key"]], session=None)
    assert evidence["source_ids"] == [fixture["role_key"][0]]
    assert evidence["practitioner_role_ids"] == [fixture["role_key"][1]]
    assert evidence["insurance_plans"][0]["identifier"] == "H1234-001"
    assert evidence["insurance_plan_metadata"]["catalog_complete"] is True


@pytest.mark.asyncio
async def test_include_sources_is_compact_and_skips_roles(monkeypatch, provider_directory_fixture):
    fixture = provider_directory_fixture
    fetch_roles = AsyncMock(side_effect=AssertionError("full role evidence requested"))
    _patch_detail_endpoint(monkeypatch, fixture, fetch_roles)

    response = await npi_module.get_npi(
        _detail_request(include_sources="true"),
        "1518379601",
    )
    evidence = json.loads(response.body)["address_list"][0]["provider_directory_sources"][0]

    fetch_roles.assert_not_awaited()
    assert evidence["source_ids"] == [fixture["role_key"][0]]
    assert [entry["source_id"] for entry in evidence["catalog_aliases"]] == [
        fixture["role_key"][0]
    ]
    assert "insurance_plans" not in evidence
    assert "networks" not in evidence
    assert "practitioner_role_ids" not in evidence
