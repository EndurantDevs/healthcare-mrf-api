# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure tests for rooted graph query construction."""

from dataclasses import replace
import urllib.parse

import pytest

from process.provider_directory_rooted_graph_identity import (
    build_provider_directory_rooted_graph_scope,
)
from process.provider_directory_rooted_graph_query import (
    ROOTED_GRAPH_QUERY_DIRECT_READ,
    ROOTED_GRAPH_QUERY_EXACT_SEARCH,
    ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS,
    ProviderDirectoryRootedGraphQueryError,
    build_insurance_plan_census_query,
    build_provider_directory_organization_affiliation_query,
    build_provider_directory_practitioner_role_query,
    build_rooted_graph_direct_read,
    build_rooted_graph_search_query,
    canonical_provider_directory_api_base,
)


API_BASE = "https://directory.example.test/fhir/R4"


def _scope_id() -> str:
    return build_provider_directory_rooted_graph_scope(
        root_dataset_variant="uhc_flex_practitioner",
        root_publication_contract_id=(
            "healthporta.provider-directory.uhc-flex-practitioner-"
            "dataset-publication.v1"
        ),
        root_source_id="synthetic-root-source",
        root_endpoint_id="f" * 64,
        acquisition_source_id="synthetic-acquisition-source",
        acquisition_endpoint_id="e" * 64,
        source_authority_id="synthetic-reviewed-authority",
        root_dataset_id="dataset-synthetic-a",
        root_dataset_hash="d" * 64,
        root_content_proof_sha256="c" * 64,
        root_resource_count=3,
    ).scope_id


def test_practitioner_role_query_is_one_exact_reference_value():
    query = build_provider_directory_practitioner_role_query(
        API_BASE,
        "practitioner-a",
    )
    parsed = urllib.parse.urlsplit(query.url)

    assert query.kind == ROOTED_GRAPH_QUERY_EXACT_SEARCH
    assert query.resource_type == "PractitionerRole"
    assert query.search_parameter == "practitioner"
    assert query.reference == "Practitioner/practitioner-a"
    assert query.page_size == 100
    assert urllib.parse.parse_qsl(parsed.query) == [
        ("practitioner", "Practitioner/practitioner-a"),
        ("_count", "100"),
    ]
    assert parsed.path == "/fhir/R4/PractitionerRole"
    assert "," not in parsed.query


def test_affiliation_query_is_exact_participating_organization():
    query = build_provider_directory_organization_affiliation_query(
        API_BASE,
        "network-a",
    )

    assert query.url == (
        "https://directory.example.test/fhir/R4/OrganizationAffiliation?"
        "participating-organization=Organization%2Fnetwork-a&_count=100"
    )
    assert query.identity_document() == {
        "kind": "exact_reference_search",
        "page_size": 100,
        "pagination": "same-origin-source-issued-until-terminal",
        "reference": "Organization/network-a",
        "resource_type": "OrganizationAffiliation",
        "search_parameter": "participating-organization",
    }


@pytest.mark.parametrize(
    "resource_type",
    ["Organization", "Location", "HealthcareService", "Endpoint"],
)
def test_direct_reads_are_limited_to_the_four_referenced_families(resource_type):
    query = build_rooted_graph_direct_read(
        api_base=API_BASE,
        resource_type=resource_type,
        resource_id="synthetic-a",
    )

    assert query.kind == ROOTED_GRAPH_QUERY_DIRECT_READ
    assert query.reference == f"{resource_type}/synthetic-a"
    assert query.url == f"{API_BASE}/{resource_type}/synthetic-a"
    assert query.page_size is None
    assert query.pagination == "forbidden"


@pytest.mark.parametrize(
    "resource_type",
    ["Practitioner", "PractitionerRole", "OrganizationAffiliation", "InsurancePlan"],
)
def test_direct_reads_reject_non_direct_families(resource_type):
    with pytest.raises(
        ProviderDirectoryRootedGraphQueryError,
        match="direct read is forbidden",
    ) as error_info:
        build_rooted_graph_direct_read(
            api_base=API_BASE,
            resource_type=resource_type,
            resource_id="synthetic-a",
        )

    assert error_info.value.code == "direct_read_forbidden"


def test_insurance_plan_query_is_unfiltered_full_census_start():
    query = build_insurance_plan_census_query(API_BASE)
    parsed = urllib.parse.urlsplit(query.url)

    assert query.kind == ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS
    assert query.resource_type == "InsurancePlan"
    assert query.search_parameter is None
    assert query.reference is None
    assert urllib.parse.parse_qsl(parsed.query) == [("_count", "100")]
    assert "network" not in parsed.query
    assert query.pagination == "same-origin-source-issued-until-terminal"


def test_insurance_plan_network_query_is_explicitly_forbidden():
    with pytest.raises(
        ProviderDirectoryRootedGraphQueryError,
        match="network query is forbidden",
    ) as error_info:
        build_rooted_graph_search_query(
            api_base=API_BASE,
            resource_type="InsurancePlan",
            search_parameter="network",
            referenced_resource_id="network-a",
        )

    assert error_info.value.code == "insurance_plan_network_query_forbidden"


@pytest.mark.parametrize(
    ("resource_type", "search_parameter"),
    [
        ("PractitionerRole", "subject"),
        ("OrganizationAffiliation", "organization"),
        ("Organization", "identifier"),
    ],
)
def test_other_search_shapes_are_forbidden(resource_type, search_parameter):
    with pytest.raises(
        ProviderDirectoryRootedGraphQueryError,
        match="search is forbidden",
    ):
        build_rooted_graph_search_query(
            api_base=API_BASE,
            resource_type=resource_type,
            search_parameter=search_parameter,
            referenced_resource_id="synthetic-a",
        )


@pytest.mark.parametrize(
    "candidate",
    [
        "http://directory.example.test/fhir/R4",
        "https://user:pass@directory.example.test/fhir/R4",
        "https://directory.example.test:bad/fhir/R4",
        "https://directory.example.test",
        "https://directory.example.test/fhir/R4/",
        "https://directory.example.test/fhir/R4?token=secret",
        "https://directory.example.test/fhir/R4#fragment",
        " padded ",
        None,
    ],
)
def test_api_base_rejects_noncanonical_or_credentialed_values(candidate):
    with pytest.raises(
        ProviderDirectoryRootedGraphQueryError,
        match="API base is invalid",
    ):
        canonical_provider_directory_api_base(candidate)


def test_query_rejects_url_or_contract_field_drift():
    query = build_provider_directory_practitioner_role_query(
        API_BASE,
        "practitioner-a",
    )

    for change in (
        {"url": query.url + "&subject=other"},
        {"reference": "Practitioner/other"},
        {"page_size": 1},
        {"pagination": "forbidden"},
        {"kind": "unknown"},
    ):
        with pytest.raises(
            ProviderDirectoryRootedGraphQueryError,
            match="query is invalid",
        ):
            replace(query, **change)


def test_query_id_binds_query_to_scope_without_repr_disclosure():
    query = build_provider_directory_practitioner_role_query(
        API_BASE,
        "practitioner-a",
    )

    assert query.query_id(_scope_id()).startswith("pdrgq_")
    assert repr(query) == (
        "<provider-directory-rooted-graph-query "
        "kind='exact_reference_search' resource_type='PractitionerRole'>"
    )
    assert API_BASE not in repr(query)
    assert "practitioner-a" not in repr(query)


@pytest.mark.parametrize(
    "resource_id",
    ["", "invalid/id", " padded", "a" * 65, None],
)
def test_query_builders_reject_invalid_resource_ids(resource_id):
    with pytest.raises(ValueError, match="resource_id_invalid"):
        build_provider_directory_practitioner_role_query(API_BASE, resource_id)
