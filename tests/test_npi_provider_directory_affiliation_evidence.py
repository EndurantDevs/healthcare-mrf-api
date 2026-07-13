# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi as npi_module


SOURCE_ID = "pdfhir_contra_costa"
AFFILIATION_ID = "affiliation-21438"
AFFILIATION_KEY = (SOURCE_ID, AFFILIATION_ID)


def _affiliation_evidence_row(evidence_type, resource_id, provenance, **fields):
    evidence_row_map = {
        "source_id": SOURCE_ID,
        "affiliation_id": AFFILIATION_ID,
        "evidence_type": evidence_type,
        "resource_id": resource_id,
        "identifier": None,
        "name": None,
        "reference": None,
        "provenance": provenance,
    }
    evidence_row_map.update(fields)
    return evidence_row_map


def _affiliation_only_contract():
    address_maps = [
        {
            "source_record_ids": [
                (
                    "provider_directory_fhir:organization_affiliation:"
                    f"{SOURCE_ID}:{AFFILIATION_ID}:location-496899"
                )
            ]
        }
    ]
    source_details_by_id = {
        SOURCE_ID: {
            "source_id": SOURCE_ID,
            "endpoint_id": "pd_endpoint_contra_costa",
            "canonical_api_base": "https://directory.contracosta.test/fhir",
        }
    }
    affiliation_evidence_by_key = {
        AFFILIATION_KEY: {
            "insurance_plans": [
                {
                    "resource_type": "InsurancePlan",
                    "resource_id": "plan-contra-costa",
                    "identifier": "CCHP-2026",
                    "provenance": "organization-affiliation-network-derived",
                }
            ],
            "networks": [
                {
                    "resource_type": "Organization",
                    "resource_id": "network-contra-costa",
                    "name": "Contra Costa Health Plan Network",
                    "reference": "Organization/network-contra-costa",
                    "provenance": "provider_directory_organization_affiliation",
                }
            ],
        }
    }
    return address_maps, source_details_by_id, affiliation_evidence_by_key


def test_affiliation_evidence_sql_fences_current_networks_and_resolved_plans():
    sql = npi_module._provider_directory_affiliation_evidence_sql(
        "mrf",
        has_catalog=True,
    )

    assert "dataset.is_current IS TRUE" in sql
    assert "dataset.status = 'published'" in sql
    assert "dataset.published_at IS NOT NULL" in sql
    assert "dataset.superseded_at IS NULL" in sql
    assert "COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id)" in sql
    assert "CAST(:affiliation_ids AS varchar[])" in sql
    assert "visible_affiliation_resource.resource_type = 'OrganizationAffiliation'" in sql
    assert "affiliation.last_seen_run_id = visible_affiliation_resource.run_id" in sql
    assert "network_organization.last_seen_run_id = current_network_organization_resource.run_id" in sql
    assert "insurance_plan.last_seen_run_id = current_insurance_plan.run_id" in sql
    assert "affiliation.active IS DISTINCT FROM false" in sql
    assert "network_organization.active IS DISTINCT FROM false" in sql
    assert "network_catalog.refs" not in sql
    assert "jsonb_array_elements(network_catalog.refs" not in sql
    assert "catalog_plan_candidates" not in sql
    assert "COALESCE(insurance_plan.network_refs::jsonb, '[]'::jsonb)" in sql
    assert "insurance_plan.source_id = affiliation_network.source_id" in sql
    assert "plan_network_ref.value = affiliation_network.reference" in sql
    assert "affiliation_network.resource_id" in sql
    assert "affiliation_catalog_status" not in sql
    assert "network_catalog.network_resource_id = network.resource_id" in sql
    assert "network_catalog.provider_directory_network_name" in sql
    assert "'organization-affiliation-network-derived'::varchar AS provenance" in sql
    assert "'provider_directory_organization_affiliation'::varchar AS provenance" in sql


def test_affiliation_evidence_mapper_returns_current_network_and_plan_context():
    evidence_rows = [
        _affiliation_evidence_row(
            "affiliation",
            AFFILIATION_ID,
            "provider_directory_organization_affiliation",
            plan_returned=1,
            plan_total=1,
            plan_truncated=False,
            catalog_complete=True,
        ),
        _affiliation_evidence_row(
            "insurance_plan",
            "plan-contra-costa",
            "organization-affiliation-network-derived",
            identifier="CCHP-2026",
        ),
        _affiliation_evidence_row(
            "network",
            "network-contra-costa",
            "provider_directory_organization_affiliation",
            name="Contra Costa Health Plan Network",
            reference="Organization/network-contra-costa",
        ),
    ]

    evidence_map = npi_module._map_provider_directory_affiliation_evidence(
        evidence_rows
    )[AFFILIATION_KEY]

    assert evidence_map["insurance_plans"][0] == {
        "resource_type": "InsurancePlan",
        "resource_id": "plan-contra-costa",
        "identifier": "CCHP-2026",
        "provenance": "organization-affiliation-network-derived",
    }
    assert evidence_map["networks"][0] == {
        "resource_type": "Organization",
        "resource_id": "network-contra-costa",
        "name": "Contra Costa Health Plan Network",
        "reference": "Organization/network-contra-costa",
        "provenance": "provider_directory_organization_affiliation",
    }
    assert evidence_map["insurance_plan_metadata"]["catalog_complete"] is True


def test_source_record_ids_parse_roles_and_affiliations_separately():
    record_ids = [
        "provider_directory_fhir:practitioner_role:pdfhir_example:role-1:location-1",
        (
            "provider_directory_fhir:organization_affiliation:pdfhir_example:"
            "affiliation-1:location-1"
        ),
        "provider_directory_fhir:organization_address:pdfhir_example:org-1:1",
    ]

    assert npi_module._directory_role_keys_from_records(record_ids) == [
        ("pdfhir_example", "role-1")
    ]
    assert npi_module._directory_affiliation_keys_from_records(record_ids) == [
        ("pdfhir_example", "affiliation-1")
    ]


@pytest.mark.asyncio
async def test_affiliation_only_evidence_is_scoped_to_exact_current_id(monkeypatch):
    """Expose current affiliation evidence without inventing PractitionerRole IDs."""
    address_maps, source_details_by_id, affiliation_evidence_by_key = (
        _affiliation_only_contract()
    )
    fetch_roles = AsyncMock(return_value={})
    fetch_affiliations = AsyncMock(return_value=affiliation_evidence_by_key)
    replacement_map = {
        "_fetch_provider_directory_source_detail_map": AsyncMock(
            return_value=source_details_by_id
        ),
        "_fetch_provider_directory_role_evidence_map": fetch_roles,
        "_fetch_provider_directory_affiliation_evidence_map": fetch_affiliations,
    }
    for function_name, replacement in replacement_map.items():
        monkeypatch.setattr(npi_module, function_name, replacement)

    await npi_module._attach_provider_directory_source_details(
        address_maps,
        include_role_evidence=True,
    )

    fetch_roles.assert_awaited_once_with([], session=None)
    fetch_affiliations.assert_awaited_once_with([AFFILIATION_KEY], session=None)
    evidence_map = address_maps[0]["provider_directory_sources"][0]
    assert evidence_map["organization_affiliation_ids"] == [AFFILIATION_ID]
    assert "practitioner_role_ids" not in evidence_map
    assert [network["resource_id"] for network in evidence_map["networks"]] == [
        "network-contra-costa"
    ]
    assert [plan["resource_id"] for plan in evidence_map["insurance_plans"]] == [
        "plan-contra-costa"
    ]
