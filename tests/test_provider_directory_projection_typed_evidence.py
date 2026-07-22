# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from copy import deepcopy

import pytest

from process.provider_directory_projection_contribution import (
    reduce_semantic_outcomes,
)
from process.provider_directory_projection_typed_evidence import (
    SEMANTIC_GEOCODE_EVIDENCE_KEY,
    SEMANTIC_RELATIONSHIP_EVIDENCE_KEY,
    normalized_semantic_evidence,
)
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
)
from tests.provider_directory_projection_semantic_support import (
    semantic_pair,
    semantic_rows_and_contributions,
)


def _resource_of_type(resources, resource_type):
    return next(
        candidate
        for candidate in resources
        if candidate["resource_type"] == resource_type
    )


def _mutate_geocode_evidence(mutation, resources):
    replacement_by_mutation = {
        "coordinate_bool": ("latitude_microdegrees", True),
        "longitude_bool": ("longitude_microdegrees", True),
        "latitude_overflow": ("latitude_microdegrees", 90_000_001),
        "latitude_underflow": ("latitude_microdegrees", -90_000_001),
        "longitude_overflow": ("longitude_microdegrees", 180_000_001),
        "longitude_underflow": ("longitude_microdegrees", -180_000_001),
        "coordinate_float": ("latitude_microdegrees", 40.5),
    }
    location_address = _resource_of_type(resources, "Location")[
        "profile_evidence_json"
    ]["addresses"][0]
    geocode = location_address[SEMANTIC_GEOCODE_EVIDENCE_KEY]
    replacement = replacement_by_mutation.get(mutation)
    if replacement is not None:
        field_name, invalid_value = replacement
        geocode[field_name] = invalid_value
        return
    if mutation == "geocode_missing_field":
        del geocode["longitude_microdegrees"]
        return
    if mutation == "geocode_non_mapping":
        location_address[SEMANTIC_GEOCODE_EVIDENCE_KEY] = []
        return
    if mutation == "geocode_without_address_fields":
        location_address.clear()
        location_address[SEMANTIC_GEOCODE_EVIDENCE_KEY] = geocode
        return
    if mutation == "geocode_unknown_address_field":
        location_address["unbound_coordinate_source"] = "caller"
        return
    if mutation == "geocode_extra_field":
        geocode["accuracy"] = 1
        return
    practitioner_address = _resource_of_type(resources, "Practitioner")[
        "profile_evidence_json"
    ]["addresses"][0]
    practitioner_address[SEMANTIC_GEOCODE_EVIDENCE_KEY] = deepcopy(geocode)


def _mutate_relationship_evidence(mutation, resources):
    replacement_by_mutation = {
        "relationship_unknown_kind": ("kind", "provider"),
        "relationship_wrong_target_type": (
            "target_resource_type",
            "Practitioner",
        ),
    }
    plan_references = _resource_of_type(resources, "InsurancePlan")[
        "profile_evidence_json"
    ]["references"]
    relationship = plan_references[0][SEMANTIC_RELATIONSHIP_EVIDENCE_KEY]
    replacement = replacement_by_mutation.get(mutation)
    if replacement is not None:
        field_name, invalid_value = replacement
        relationship[field_name] = invalid_value
        return
    if mutation == "relationship_duplicate":
        plan_references.append(deepcopy(plan_references[0]))
        return
    if mutation == "relationship_extra_field":
        plan_references[0]["display"] = "Network"
        return
    if mutation == "relationship_missing_field":
        del relationship["target_resource_id"]
        return
    if mutation == "relationship_non_mapping":
        plan_references[0][SEMANTIC_RELATIONSHIP_EVIDENCE_KEY] = []
        return
    relationship["target_resource_id"] = "x" * 257


@pytest.mark.parametrize(
    "resource_type, evidence_name, replacement_value",
    (
        ("Location", "addresses", 40_713_001),
        ("InsurancePlan", "references", "network-replacement"),
    ),
)
def test_candidate_typed_evidence_binds_physical_and_profile_hashes(
    resource_type,
    evidence_name,
    replacement_value,
):
    resources, contributions = semantic_rows_and_contributions()
    baseline_proof = reduce_semantic_outcomes(resources, contributions)
    resource = _resource_of_type(resources, resource_type)
    contribution = _resource_of_type(contributions, resource_type)
    raw_resource_evidence = resource["profile_evidence_json"][evidence_name][0]
    raw_profile_evidence = contribution[f"{evidence_name}_json"][0]
    if resource_type == "Location":
        raw_resource_evidence[SEMANTIC_GEOCODE_EVIDENCE_KEY][
            "latitude_microdegrees"
        ] = replacement_value
        raw_profile_evidence[SEMANTIC_GEOCODE_EVIDENCE_KEY][
            "latitude_microdegrees"
        ] = replacement_value
    else:
        raw_resource_evidence[SEMANTIC_RELATIONSHIP_EVIDENCE_KEY][
            "target_resource_id"
        ] = replacement_value
        raw_profile_evidence[SEMANTIC_RELATIONSHIP_EVIDENCE_KEY][
            "target_resource_id"
        ] = replacement_value

    changed_proof = reduce_semantic_outcomes(resources, contributions)

    assert changed_proof.outcome_counts == baseline_proof.outcome_counts
    assert changed_proof.canonical_row_sha256 != baseline_proof.canonical_row_sha256
    assert (
        changed_proof.profile_contribution_sha256
        != baseline_proof.profile_contribution_sha256
    )
    assert changed_proof.proof_sha256 != baseline_proof.proof_sha256


@pytest.mark.parametrize(
    ("resource_type", "evidence_name", "field_name", "replacement_value"),
    (
        (
            "Location",
            "addresses_json",
            "latitude_microdegrees",
            40_713_001,
        ),
        (
            "InsurancePlan",
            "references_json",
            "target_resource_id",
            "network-cross-wire",
        ),
        (
            "OrganizationAffiliation",
            "references_json",
            "target_resource_id",
            "affiliation-cross-wire",
        ),
    ),
)
def test_profile_contribution_rejects_valid_but_cross_wired_typed_evidence(
    resource_type,
    evidence_name,
    field_name,
    replacement_value,
):
    resources, contributions = semantic_rows_and_contributions()
    contribution = _resource_of_type(contributions, resource_type)
    evidence_entry = contribution[evidence_name][0]
    envelope_key = (
        SEMANTIC_GEOCODE_EVIDENCE_KEY
        if resource_type == "Location"
        else SEMANTIC_RELATIONSHIP_EVIDENCE_KEY
    )
    evidence_entry[envelope_key][field_name] = replacement_value

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="semantic_pair_evidence_mismatch",
    ):
        reduce_semantic_outcomes(resources, contributions)


@pytest.mark.parametrize(
    ("latitude", "longitude"),
    (
        (-90_000_000, -180_000_000),
        (90_000_000, 180_000_000),
    ),
)
def test_geocode_evidence_accepts_exact_microdegree_boundaries(
    latitude,
    longitude,
):
    resource, contribution = semantic_pair(
        "Location",
        "boundary-location",
        evidence_by_name={"addresses": [{"line": ["1 Boundary Rd"]}]},
        geocode=(latitude, longitude),
    )

    proof = reduce_semantic_outcomes((resource,), (contribution,))

    assert proof.outcome_counts["addressed_locations"] == 1
    assert proof.outcome_counts["geocoded_locations"] == 1


@pytest.mark.parametrize(
    ("kind", "target_resource_type"),
    (
        ("organization", "Organization"),
        ("participating_organization", "Organization"),
        ("network", "Organization"),
        ("location", "Location"),
        ("healthcare_service", "HealthcareService"),
        ("endpoint", "Endpoint"),
    ),
)
def test_affiliation_relationship_accepts_each_exact_kind_target_pair(
    kind,
    target_resource_type,
):
    resource, contribution = semantic_pair(
        "OrganizationAffiliation",
        f"affiliation-{kind}",
        affiliation_relationships=((kind, target_resource_type, "x" * 256),),
    )

    proof = reduce_semantic_outcomes((resource,), (contribution,))

    assert proof.outcome_counts["organization_affiliation_links"] == 1


@pytest.mark.parametrize(
    ("kind", "wrong_target_resource_type"),
    (
        ("organization", "Location"),
        ("participating_organization", "Practitioner"),
        ("network", "Endpoint"),
        ("location", "Organization"),
        ("healthcare_service", "Endpoint"),
        ("endpoint", "HealthcareService"),
    ),
)
def test_affiliation_relationship_rejects_cross_wired_kind_target_pair(
    kind,
    wrong_target_resource_type,
):
    resource, contribution = semantic_pair(
        "OrganizationAffiliation",
        f"bad-affiliation-{kind}",
        affiliation_relationships=((kind, wrong_target_resource_type, "target-1"),),
    )

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="relationship_evidence_invalid",
    ):
        reduce_semantic_outcomes((resource,), (contribution,))


@pytest.mark.parametrize(
    "resource_type, summary_field, replacement_value",
    (
        ("Location", "summary_geocoded_location", False),
        ("InsurancePlan", "summary_network_link_count", 2),
        (
            "OrganizationAffiliation",
            "summary_affiliation_link_count",
            1,
        ),
    ),
)
def test_semantic_summaries_must_equal_typed_evidence(
    resource_type,
    summary_field,
    replacement_value,
):
    resources, contributions = semantic_rows_and_contributions()
    resource = _resource_of_type(resources, resource_type)
    resource[summary_field] = replacement_value

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="semantic_contribution_inconsistent",
    ):
        reduce_semantic_outcomes(resources, contributions)


@pytest.mark.parametrize(
    "mutation, expected_error",
    (
        ("coordinate_bool", "geocode_evidence_invalid"),
        ("longitude_bool", "geocode_evidence_invalid"),
        ("coordinate_float", "geocode_evidence_invalid"),
        ("latitude_overflow", "geocode_evidence_invalid"),
        ("latitude_underflow", "geocode_evidence_invalid"),
        ("longitude_overflow", "geocode_evidence_invalid"),
        ("longitude_underflow", "geocode_evidence_invalid"),
        ("geocode_missing_field", "geocode_evidence_invalid"),
        ("geocode_non_mapping", "geocode_evidence_invalid"),
        ("geocode_without_address_fields", "geocode_evidence_invalid"),
        ("geocode_unknown_address_field", "geocode_evidence_invalid"),
        ("geocode_extra_field", "geocode_evidence_invalid"),
        ("geocode_wrong_resource", "geocode_evidence_invalid"),
        ("relationship_unknown_kind", "relationship_evidence_invalid"),
        ("relationship_wrong_target_type", "relationship_evidence_invalid"),
        ("relationship_duplicate", "relationship_evidence_invalid"),
        ("relationship_extra_field", "relationship_evidence_invalid"),
        ("relationship_missing_field", "relationship_evidence_invalid"),
        ("relationship_non_mapping", "relationship_evidence_invalid"),
        (
            "relationship_oversized_target",
            "relationship_target_resource_id_invalid",
        ),
    ),
)
def test_candidate_reducer_rejects_malformed_typed_evidence(
    mutation,
    expected_error,
):
    """Reject malformed coordinates and relationship target evidence."""

    resources, contributions = semantic_rows_and_contributions()
    if mutation.startswith(("coordinate", "geocode", "latitude", "longitude")):
        _mutate_geocode_evidence(mutation, resources)
    else:
        _mutate_relationship_evidence(mutation, resources)

    with pytest.raises(ProviderDirectoryProjectionError, match=expected_error):
        reduce_semantic_outcomes(resources, contributions)


def test_candidate_evidence_normalizer_requires_exact_profile_fields():
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="semantic_evidence_fields_invalid",
    ):
        normalized_semantic_evidence("Location", {"addresses": []})


@pytest.mark.parametrize(
    "evidence_name",
    ("names", "specialties", "contacts", "addresses", "references"),
)
@pytest.mark.parametrize("invalid_entry", (None, "scalar", 7, []))
def test_each_counted_profile_family_rejects_non_mapping_entries(
    evidence_name,
    invalid_entry,
):
    resource, contribution = semantic_pair(
        "Practitioner",
        f"bad-{evidence_name}",
        evidence_by_name={evidence_name: [invalid_entry]},
    )

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match=f"{evidence_name}_evidence_invalid",
    ):
        reduce_semantic_outcomes((resource,), (contribution,))


@pytest.mark.parametrize(
    ("evidence_name", "meaningless_entry"),
    (
        ("names", {"use": "official"}),
        ("specialties", {"system": "taxonomy"}),
        ("contacts", {"system": "phone"}),
        ("addresses", {"use": "work"}),
        ("references", {"display": "Unbound target"}),
    ),
)
def test_each_counted_profile_family_requires_meaningful_content(
    evidence_name,
    meaningless_entry,
):
    resource, contribution = semantic_pair(
        "Practitioner",
        f"empty-{evidence_name}",
        evidence_by_name={evidence_name: [meaningless_entry]},
    )

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match=f"{evidence_name}_evidence_invalid",
    ):
        reduce_semantic_outcomes((resource,), (contribution,))


@pytest.mark.parametrize(
    ("evidence_name", "malformed_entry"),
    (
        ("names", {"given": "Pat"}),
        ("names", {"prefix": ["Dr"]}),
        ("specialties", {"coding": [{"system": "taxonomy"}]}),
        ("contacts", {"value": "555-0100", "rank": True}),
        ("addresses", {"line": "1 Main St"}),
        ("addresses", {"line": ["1 Main St"], "period": None}),
        ("addresses", {"line": ["1 Main St"], "unknown": "caller"}),
        ("references", {"type": "Organization"}),
        ("references", {"id": "orphan-id"}),
        ("references", {"identifier": {"value": None}}),
    ),
)
def test_counted_profile_families_reject_malformed_fhir_shapes(
    evidence_name,
    malformed_entry,
):
    resource, contribution = semantic_pair(
        "Practitioner",
        f"malformed-{evidence_name}",
        evidence_by_name={evidence_name: [malformed_entry]},
    )

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match=f"{evidence_name}_evidence_invalid",
    ):
        reduce_semantic_outcomes((resource,), (contribution,))


def test_profile_side_cannot_pair_malformed_counted_evidence_with_valid_winner():
    resources, contributions = semantic_rows_and_contributions()
    contributions[0]["names_json"] = [None]

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="names_evidence_invalid",
    ):
        reduce_semantic_outcomes(resources, contributions)
