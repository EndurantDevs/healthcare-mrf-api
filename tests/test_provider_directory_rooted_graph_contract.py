# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure tests for the source-neutral rooted graph contract."""

from dataclasses import replace

import pytest

from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_TYPES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_EXACT_SEARCHES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_SELECTION,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES,
    ProviderDirectoryRootedGraphContractError,
    provider_directory_rooted_graph_contract_payload,
)


def test_contract_closes_the_seven_family_rooted_graph():
    contract = PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT

    assert contract.contract_id == PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID
    assert contract.connector_id == PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID
    assert contract.resource_types == (
        "PractitionerRole",
        "OrganizationAffiliation",
        "Organization",
        "Location",
        "HealthcareService",
        "InsurancePlan",
        "Endpoint",
    )
    assert contract.direct_read_types == (
        "Organization",
        "Location",
        "HealthcareService",
        "Endpoint",
    )
    assert contract.insurance_plan_selection == (
        "full-finite-census-local-network-intersection"
    )
    assert contract.insurance_plan_network_query == "forbidden"
    assert contract.rooted_graph_complete is True
    assert contract.endpoint_collection_complete is False
    assert contract.endpoint_complete is False


def test_exact_search_contracts_are_one_value_reference_queries():
    documents = [
        search.document() for search in PROVIDER_DIRECTORY_ROOTED_GRAPH_EXACT_SEARCHES
    ]

    assert documents == [
        {
            "expansion": "once-per-root-practitioner",
            "pagination": "same-origin-source-issued-until-terminal",
            "page_size": 100,
            "query_values_per_request": 1,
            "reference_type": "Practitioner",
            "resource_type": "PractitionerRole",
            "search_parameter": "practitioner",
        },
        {
            "expansion": "reachable-participating-organization-fixed-point",
            "pagination": "same-origin-source-issued-until-terminal",
            "page_size": 100,
            "query_values_per_request": 1,
            "reference_type": "Organization",
            "resource_type": "OrganizationAffiliation",
            "search_parameter": "participating-organization",
        },
    ]


def test_contract_payload_separates_rooted_and_endpoint_completion():
    payload = provider_directory_rooted_graph_contract_payload()

    assert payload["completion"] == {
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
        "rooted_graph_complete": True,
        "scope": "rooted-reference-closure",
    }
    assert payload["insurance_plan"] == {
        "admission": "database-proven-root-reference-fixed-point",
        "network_query": "forbidden",
        "page_size": 100,
        "pagination": "same-origin-source-issued-until-terminal",
        "selection": PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_SELECTION,
    }
    assert payload["direct_reads"]["expansion"] == ("deduplicated-reference-closure")
    assert payload["direct_reads"]["missing_http_statuses"] == [404, 410]
    assert payload["persistence"] == {
        "derived_registration": "same-transaction-as-terminal-witness",
        "root_initialization": "set-wise-sql-canonical-query-identity",
    }
    assert "canonical_api_base" not in payload
    assert "source_id" not in payload
    assert "authority_id" not in payload


def test_endpoint_signature_is_fresh_and_source_neutral():
    first = PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT.endpoint_signature()
    second = PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT.endpoint_signature()

    assert first == second
    first["connector_acquisition_contract"]["connector_id"] = "forged"
    assert second["connector_acquisition_contract"]["connector_id"] == (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID
    )
    assert second["connector_acquisition_contract"]["graph_contract_sha256"] == (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_SHA256
    )


@pytest.mark.parametrize(
    "change",
    [
        {"connector_id": "pdrgc_" + "0" * 48},
        {"resource_types": ("PractitionerRole",)},
        {"direct_read_types": ("Organization",)},
        {"insurance_plan_selection": "server-filter"},
        {"insurance_plan_network_query": "allowed"},
        {"completion_scope": "endpoint-census"},
        {"rooted_graph_complete": False},
        {"endpoint_collection_complete": True},
        {"endpoint_complete": True},
    ],
)
def test_contract_rejects_identity_or_completion_drift(change):
    with pytest.raises(
        ProviderDirectoryRootedGraphContractError,
        match="rooted_graph_contract_inconsistent",
    ):
        replace(PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT, **change)


@pytest.mark.parametrize(
    "change",
    [
        {"search_parameter": "subject"},
        {"reference_type": "Organization"},
        {"expansion": "once"},
        {"query_values_per_request": 2},
        {"page_size": 1},
        {"pagination": "forbidden"},
    ],
)
def test_exact_search_contract_rejects_broadened_semantics(change):
    with pytest.raises(
        ProviderDirectoryRootedGraphContractError,
        match="exact_search_invalid",
    ):
        replace(PROVIDER_DIRECTORY_ROOTED_GRAPH_EXACT_SEARCHES[0], **change)
