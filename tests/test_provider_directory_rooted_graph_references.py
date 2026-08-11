# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure tests for rooted graph references and local plan selection."""

from dataclasses import replace

import pytest

from process import provider_directory_rooted_graph_references as references
from process.provider_directory_rooted_graph_references import (
    ProviderDirectoryFHIRReference,
    ProviderDirectoryRootedGraphReferenceError,
    build_provider_directory_insurance_plan_census,
    canonical_provider_directory_fhir_reference,
    intersect_provider_directory_insurance_plan_census,
    provider_directory_rooted_graph_resource_references,
)


def _reference(resource_type: str, resource_id: str) -> dict:
    return {"reference": f"{resource_type}/{resource_id}"}


def _plan(resource_id: str, *network_ids: str, **extra_fields) -> dict:
    return {
        "resourceType": "InsurancePlan",
        "id": resource_id,
        "network": [
            _reference("Organization", network_id) for network_id in network_ids
        ],
        **extra_fields,
    }


def _census():
    return build_provider_directory_insurance_plan_census(
        [
            _plan("plan-c"),
            _plan("plan-a", "network-a", "network-b", status="active"),
            _plan("plan-b", "network-b"),
        ],
        advertised_total=3,
        terminal_page_count=2,
    )


def test_local_reference_parser_accepts_only_typed_relative_references():
    reference = canonical_provider_directory_fhir_reference(
        "Organization/network-a",
        expected_resource_type="Organization",
    )

    assert reference == ProviderDirectoryFHIRReference(
        "Organization",
        "network-a",
    )
    assert reference.canonical == "Organization/network-a"


@pytest.mark.parametrize(
    "candidate",
    [
        "https://directory.example.test/fhir/R4/Organization/network-a",
        "#contained",
        "Organization/network-a/_history/1",
        "Organization/network-a?secret=value",
        "Organization/under_score",
        " Organization/network-a",
        None,
    ],
)
def test_local_reference_parser_rejects_nonlocal_or_noncanonical_values(candidate):
    with pytest.raises(
        ProviderDirectoryRootedGraphReferenceError,
        match="reference is invalid",
    ):
        canonical_provider_directory_fhir_reference(candidate)


def test_reference_parser_rejects_a_wrong_expected_type():
    with pytest.raises(ProviderDirectoryRootedGraphReferenceError):
        canonical_provider_directory_fhir_reference(
            "Location/location-a",
            expected_resource_type="Organization",
        )


def test_reference_value_object_rejects_a_family_outside_the_contract():
    with pytest.raises(
        ProviderDirectoryRootedGraphReferenceError,
        match="reference is invalid",
    ):
        ProviderDirectoryFHIRReference("Patient", "synthetic-a")


def test_practitioner_role_references_cover_each_direct_read_family():
    role_by_field = {
        "resourceType": "PractitionerRole",
        "id": "role-a",
        "practitioner": _reference("Practitioner", "practitioner-a"),
        "organization": _reference("Organization", "organization-a"),
        "network": [_reference("Organization", "network-a")],
        "location": [_reference("Location", "location-a")],
        "healthcareService": [_reference("HealthcareService", "service-a")],
        "endpoint": [_reference("Endpoint", "endpoint-a")],
    }

    assert provider_directory_rooted_graph_resource_references(role_by_field) == (
        ProviderDirectoryFHIRReference("Endpoint", "endpoint-a"),
        ProviderDirectoryFHIRReference("HealthcareService", "service-a"),
        ProviderDirectoryFHIRReference("Location", "location-a"),
        ProviderDirectoryFHIRReference("Organization", "network-a"),
        ProviderDirectoryFHIRReference("Organization", "organization-a"),
        ProviderDirectoryFHIRReference("Practitioner", "practitioner-a"),
    )


def test_affiliation_references_include_participant_and_network_edges():
    affiliation_by_field = {
        "resourceType": "OrganizationAffiliation",
        "id": "affiliation-a",
        "organization": _reference("Organization", "owner-a"),
        "participatingOrganization": _reference(
            "Organization",
            "participant-a",
        ),
        "network": [
            _reference("Organization", "network-a"),
            _reference("Organization", "network-a"),
        ],
        "location": [_reference("Location", "location-a")],
        "healthcareService": [_reference("HealthcareService", "service-a")],
        "endpoint": [_reference("Endpoint", "endpoint-a")],
    }

    references = provider_directory_rooted_graph_resource_references(
        affiliation_by_field
    )

    assert (
        ProviderDirectoryFHIRReference(
            "Organization",
            "participant-a",
        )
        in references
    )
    assert ProviderDirectoryFHIRReference("Organization", "network-a") in references
    assert len(references) == 6


@pytest.mark.parametrize(
    "resource",
    [
        {
            "resourceType": "Organization",
            "id": "organization-a",
            "partOf": _reference("Organization", "organization-parent"),
            "endpoint": [_reference("Endpoint", "endpoint-a")],
        },
        {
            "resourceType": "Location",
            "id": "location-a",
            "managingOrganization": _reference(
                "Organization",
                "organization-a",
            ),
            "partOf": _reference("Location", "location-parent"),
            "endpoint": [_reference("Endpoint", "endpoint-a")],
        },
        {
            "resourceType": "HealthcareService",
            "id": "service-a",
            "providedBy": _reference("Organization", "organization-a"),
            "location": [_reference("Location", "location-a")],
            "coverageArea": [_reference("Location", "area-a")],
            "endpoint": [_reference("Endpoint", "endpoint-a")],
        },
        {
            "resourceType": "InsurancePlan",
            "id": "plan-a",
            "ownedBy": _reference("Organization", "owner-a"),
            "administeredBy": _reference("Organization", "administrator-a"),
            "coverageArea": [_reference("Location", "area-a")],
            "network": [_reference("Organization", "network-a")],
        },
        {
            "resourceType": "Endpoint",
            "id": "endpoint-a",
            "managingOrganization": _reference(
                "Organization",
                "organization-a",
            ),
        },
    ],
)
def test_all_direct_and_plan_families_have_closed_reference_fields(resource):
    references = provider_directory_rooted_graph_resource_references(resource)

    assert references
    assert len(references) == len(set(references))


def test_identifier_only_reference_is_valid_but_not_traversable():
    resource_by_field = {
        "resourceType": "Organization",
        "id": "organization-a",
        "partOf": {"identifier": {"value": "synthetic"}},
    }

    assert provider_directory_rooted_graph_resource_references(resource_by_field) == ()


@pytest.mark.parametrize(
    "resource",
    [
        None,
        {"resourceType": "Practitioner", "id": "practitioner-a"},
        {"resourceType": "Organization", "id": "invalid/id"},
        {"resourceType": "Organization", "id": "organization-a", "endpoint": {}},
        {"resourceType": "Organization", "id": "organization-a", "partOf": []},
        {
            "resourceType": "Organization",
            "id": "organization-a",
            "endpoint": [None],
        },
    ],
)
def test_reference_extraction_fails_closed_on_unknown_or_malformed_shapes(resource):
    with pytest.raises(ProviderDirectoryRootedGraphReferenceError):
        provider_directory_rooted_graph_resource_references(resource)


def test_complete_plan_census_is_terminal_deterministic_and_returns_copies():
    census = _census()

    assert census.advertised_total == 3
    assert census.resource_count == 3
    assert census.terminal_page_count == 2
    assert census.census_complete is True
    assert census.pagination_terminal is True
    assert len(census.census_sha256) == 64
    assert repr(census) == (
        "<provider-directory-insurance-plan-census resources=3 pages=2>"
    )
    assert [resource["id"] for resource in census.resources()] == [
        "plan-a",
        "plan-b",
        "plan-c",
    ]
    returned = census.resources()
    returned[0]["status"] = "retired"
    assert census.resources()[0]["status"] == "active"


def test_census_hash_is_independent_of_input_order_but_binds_page_count():
    forward = _census()
    reverse = build_provider_directory_insurance_plan_census(
        list(reversed(forward.resources())),
        advertised_total=3,
        terminal_page_count=2,
    )
    other_pages = build_provider_directory_insurance_plan_census(
        forward.resources(),
        advertised_total=3,
        terminal_page_count=3,
    )

    assert reverse.census_sha256 == forward.census_sha256
    assert other_pages.census_sha256 != forward.census_sha256


def test_local_plan_intersection_uses_only_reachable_organization_networks():
    census = _census()

    selected_a = intersect_provider_directory_insurance_plan_census(
        census,
        ["Organization/network-a"],
    )
    selected_b = intersect_provider_directory_insurance_plan_census(
        census,
        {
            ProviderDirectoryFHIRReference(
                "Organization",
                "network-b",
            )
        },
    )

    assert [resource["id"] for resource in selected_a] == ["plan-a"]
    assert [resource["id"] for resource in selected_b] == [
        "plan-a",
        "plan-b",
    ]
    assert intersect_provider_directory_insurance_plan_census(census, []) == ()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"advertised_total": 2, "terminal_page_count": 2},
        {"advertised_total": 3, "terminal_page_count": 0},
        {"advertised_total": True, "terminal_page_count": 2},
    ],
)
def test_plan_census_rejects_nonterminal_or_unreconciled_counts(kwargs):
    with pytest.raises(
        ProviderDirectoryRootedGraphReferenceError,
        match="census is incomplete",
    ):
        build_provider_directory_insurance_plan_census(
            [_plan("plan-a"), _plan("plan-b"), _plan("plan-c")],
            **kwargs,
        )


@pytest.mark.parametrize(
    "resources",
    [
        [_plan("plan-a"), _plan("plan-a")],
        [{"resourceType": "Organization", "id": "organization-a"}],
        [_plan("invalid/id")],
        [_plan("plan-a", "invalid/id")],
        "not-a-resource-vector",
    ],
)
def test_plan_census_rejects_invalid_or_duplicate_resources(resources):
    with pytest.raises(ProviderDirectoryRootedGraphReferenceError):
        build_provider_directory_insurance_plan_census(
            resources,
            advertised_total=2,
            terminal_page_count=1,
        )


def test_plan_census_rejects_unserializable_or_oversized_resources(monkeypatch):
    with pytest.raises(
        ProviderDirectoryRootedGraphReferenceError,
        match="resource is invalid",
    ):
        build_provider_directory_insurance_plan_census(
            [_plan("plan-a", invalid=object())],
            advertised_total=1,
            terminal_page_count=1,
        )

    monkeypatch.setattr(
        references,
        "PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_JSON_BYTES",
        1,
    )
    with pytest.raises(
        ProviderDirectoryRootedGraphReferenceError,
        match="resource is invalid",
    ):
        build_provider_directory_insurance_plan_census(
            [_plan("plan-a")],
            advertised_total=1,
            terminal_page_count=1,
        )


def test_plan_census_rejects_later_integrity_or_completion_drift():
    census = _census()

    for change in (
        {"census_sha256": "0" * 64},
        {"advertised_total": 2},
        {"census_complete": False},
        {"pagination_terminal": False},
        {"selection": "server-filter"},
    ):
        with pytest.raises(
            ProviderDirectoryRootedGraphReferenceError,
            match="census is invalid",
        ):
            replace(census, **change)


def test_plan_census_rejects_corrupted_stored_json():
    with pytest.raises(
        ProviderDirectoryRootedGraphReferenceError,
        match="census is invalid",
    ):
        replace(
            _census(),
            _resource_json_rows=(("plan-a", "{"),),
        )


@pytest.mark.parametrize(
    "networks",
    [
        "Organization/network-a",
        ["Location/location-a"],
        ["Organization/invalid/id"],
        [None],
    ],
)
def test_local_plan_intersection_rejects_nonorganization_network_sets(networks):
    with pytest.raises(
        ProviderDirectoryRootedGraphReferenceError,
        match="network reference is invalid",
    ):
        intersect_provider_directory_insurance_plan_census(
            _census(),
            networks,
        )


def test_local_plan_intersection_requires_a_sealed_census():
    with pytest.raises(
        ProviderDirectoryRootedGraphReferenceError,
        match="census is incomplete",
    ):
        intersect_provider_directory_insurance_plan_census(
            object(),
            [],
        )
