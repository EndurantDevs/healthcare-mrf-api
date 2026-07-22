# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-neutral FHIR-to-Profile semantic extraction regressions."""

from __future__ import annotations

from dataclasses import replace
import hashlib

import pytest

from process.provider_directory_projection_transform import projection_resource_row
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
)
from tests.provider_directory_projection_materializer_context import (
    synthetic_projection_context,
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _projection_row(payload, *, partition_ordinal: int = 0, input_ordinal: int = 0):
    context = synthetic_projection_context("ndjson")
    shard = replace(context.claim.shard, partition_ordinal=partition_ordinal)
    claim = replace(context.claim, shard=shard)
    return projection_resource_row(
        payload,
        claim=claim,
        input_ordinal=input_ordinal,
        payload_hash=_digest(f"payload:{payload['resourceType']}:{payload['id']}"),
    )


def _profile_entries(projection_row, evidence_name: str):
    evidence_by_name = projection_row["profile_evidence_json"] or {}
    return evidence_by_name.get(evidence_name, [])


def _specialty_codes(projection_row) -> set[str]:
    return {
        coding["code"]
        for concept in _profile_entries(projection_row, "specialties")
        for coding in concept.get("coding", ())
        if "code" in coding
    }


@pytest.mark.parametrize(
    "payload",
    (
        {
            "resourceType": "Endpoint",
            "id": "endpoint-no-specialty",
            "status": "active",
            "connectionType": {"code": "specialty-looking-code"},
            "payloadType": [{"coding": [{"code": "207Q00000X"}]}],
        },
        {
            "resourceType": "InsurancePlan",
            "id": "plan-no-specialty",
            "status": "active",
            "type": [{"coding": [{"code": "207Q00000X"}]}],
        },
        {
            "resourceType": "Organization",
            "id": "organization-no-specialty",
            "active": True,
            "type": [{"coding": [{"code": "207Q00000X"}]}],
        },
        {
            "resourceType": "Location",
            "id": "location-no-specialty",
            "status": "active",
            "type": [{"coding": [{"code": "207Q00000X"}]}],
        },
        {
            "resourceType": "HealthcareService",
            "id": "service-no-specialty",
            "active": True,
            "category": [{"coding": [{"code": "207Q00000X"}]}],
        },
        {
            "resourceType": "Practitioner",
            "id": "practitioner-no-specialty",
            "active": True,
            "qualification": [
                {
                    "code": {
                        "coding": [
                            {"system": "urn:license", "code": "MD"}
                        ]
                    }
                }
            ],
        },
    ),
)
def test_unrelated_codeable_concepts_do_not_inflate_specialties(payload) -> None:
    """Only explicit specialty fields or taxonomy qualifications contribute."""

    assert _specialty_codes(_projection_row(payload)) == set()


@pytest.mark.parametrize(
    ("resource_type", "specialty_field"),
    (
        ("HealthcareService", "specialty"),
        ("OrganizationAffiliation", "specialty"),
        ("PractitionerRole", "specialty"),
    ),
)
def test_explicit_plan_net_specialty_fields_are_preserved(
    resource_type,
    specialty_field,
) -> None:
    """The three explicit Plan-Net specialty paths feed Profile evidence."""

    specialty_payload_map = {
        "resourceType": resource_type,
        "id": f"{resource_type.lower()}-specialty",
        "active": True,
        specialty_field: [
            {
                "coding": [
                    {
                        "system": "http://nucc.org/provider-taxonomy",
                        "code": "207Q00000X",
                    }
                ]
            }
        ],
    }

    assert _specialty_codes(_projection_row(specialty_payload_map)) == {
        "207Q00000X"
    }


def test_practitioner_taxonomy_qualification_is_a_specialty() -> None:
    """A taxonomy-like practitioner qualification remains usable evidence."""

    taxonomy_payload_map = {
        "resourceType": "Practitioner",
        "id": "practitioner-taxonomy",
        "active": True,
        "qualification": [
            {
                "code": {
                    "text": "Family Medicine",
                    "coding": [
                        {"system": "urn:vendor", "code": "207Q00000X"}
                    ],
                }
            }
        ],
    }

    assert _specialty_codes(_projection_row(taxonomy_payload_map)) == {
        "207Q00000X"
    }


def test_exact_npi_system_wins_over_fuzzy_descriptor() -> None:
    """The official NPI system has deterministic priority over fuzzy labels."""

    payload = {
        "resourceType": "Practitioner",
        "id": "practitioner-npi-priority",
        "active": True,
        "identifier": [
            {"system": "urn:vendor:npi", "value": "1999999999"},
            {
                "system": "http://hl7.org/fhir/sid/us-npi",
                "value": "1234567893",
            },
        ],
    }

    assert _projection_row(payload)["summary_npi"] == 1234567893


def test_fuzzy_npi_descriptor_is_a_bounded_fallback() -> None:
    """A vendor NPI descriptor is accepted only for an in-range ten-digit value."""

    payload = {
        "resourceType": "HealthcareService",
        "id": "service-fuzzy-npi",
        "active": True,
        "identifier": [
            {
                "type": {"text": "National Provider NPI"},
                "value": "1-987-654-321",
            }
        ],
    }

    assert _projection_row(payload)["summary_npi"] == 1987654321


@pytest.mark.parametrize(
    ("resource_type", "resource_id", "expected_npi"),
    (
        ("Practitioner", "1000000000", 1000000000),
        ("Organization", "2999999999", 2999999999),
        ("HealthcareService", "1234567893", None),
        ("PractitionerRole", "1234567893", None),
        ("Practitioner", "0123456789", None),
        ("Practitioner", "0001234567893", None),
        ("Organization", "9999999999", None),
    ),
)
def test_npi_id_fallback_is_resource_scoped_and_range_bounded(
    resource_type,
    resource_id,
    expected_npi,
) -> None:
    """Only practitioner and organization IDs can supply a bounded NPI."""

    payload = {
        "resourceType": resource_type,
        "id": resource_id,
        "active": True,
    }

    assert _projection_row(payload)["summary_npi"] == expected_npi


def test_reference_normalization_accepts_all_supported_fhir_forms() -> None:
    """Bare, relative, absolute, versioned, query, and fragment targets resolve."""

    target_references = (
        "network-bare",
        "Organization/network-relative",
        "https://directory.example/fhir/Organization/network-absolute",
        "Organization/network-versioned/_history/7",
        "Organization/network-query?active=true",
        "Organization/network-fragment#contained",
    )
    reference_payload_map = {
        "resourceType": "InsurancePlan",
        "id": "plan-reference-forms",
        "status": "active",
        "network": [
            {"reference": target_reference}
            for target_reference in target_references
        ],
    }

    projection_row = _projection_row(reference_payload_map)
    assert projection_row["summary_network_link_count"] == len(target_references)


def test_plan_net_network_extensions_recurse_normalize_and_deduplicate() -> None:
    """Known URL variants, versions, and nested extensions share one target set."""

    network_reference_url = (
        "http://hl7.org/fhir/us/davinci-pdex-plan-net/"
        "StructureDefinition/network-reference"
    )
    participating_network_url = (
        "http://hl7.org/fhir/us/davinci-pdex-plan-net/"
        "StructureDefinition/plannet-ParticipatingNetwork-extension"
    )
    plan_payload_map = {
        "resourceType": "InsurancePlan",
        "id": "plan-network-extensions",
        "status": "active",
        "network": [{"reference": "Organization/network-one"}],
        "extension": [
            {
                "url": network_reference_url,
                "valueReference": {"reference": "Organization/network-one"},
            },
            {
                "url": network_reference_url.replace("http://", "https://")
                + "|1.2.0",
                "valueReference": {"reference": "Organization/network-two"},
            },
            {
                "url": "urn:synthetic:nesting",
                "extension": [
                    {
                        "url": participating_network_url,
                        "valueReference": {
                            "reference": "Organization/network-three"
                        },
                    }
                ],
            },
        ],
    }

    projection_row = _projection_row(plan_payload_map)
    assert projection_row["summary_network_link_count"] == 3


def test_plan_and_coverage_backbones_contribute_networks_once() -> None:
    """InsurancePlan backbone networks combine with direct network references."""

    payload = {
        "resourceType": "InsurancePlan",
        "id": "plan-backbone-networks",
        "status": "active",
        "network": [{"reference": "Organization/network-one"}],
        "plan": [
            {"network": [{"reference": "Organization/network-two"}]}
        ],
        "coverage": [
            {"network": [{"reference": "Organization/network-one"}]}
        ],
    }

    assert _projection_row(payload)["summary_network_link_count"] == 2


@pytest.mark.parametrize(
    ("payload", "evidence_name", "expected_reference"),
    (
        (
            {
                "resourceType": "PractitionerRole",
                "id": "role-plan",
                "active": True,
                "insurancePlan": [{"reference": "InsurancePlan/plan-one"}],
            },
            "references",
            "InsurancePlan/plan-one",
        ),
        (
            {
                "resourceType": "Location",
                "id": "location-parent",
                "status": "active",
                "partOf": {"reference": "Location/location-root"},
            },
            "references",
            "Location/location-root",
        ),
        (
            {
                "resourceType": "Endpoint",
                "id": "endpoint-contact",
                "status": "active",
                "contact": [{"system": "phone", "value": "2025550100"}],
            },
            "contacts",
            "2025550100",
        ),
    ),
)
def test_supported_reference_and_contact_paths_are_retained(
    payload,
    evidence_name,
    expected_reference,
) -> None:
    """Less common supported paths remain visible in Profile evidence."""

    evidence_entries = _profile_entries(_projection_row(payload), evidence_name)
    flattened_values = {
        str(evidence_value)
        for evidence_entry in evidence_entries
        for evidence_value in evidence_entry.values()
    }
    assert expected_reference in flattened_values


@pytest.mark.parametrize(
    ("resource_type", "status", "expected_active"),
    (
        ("Location", "active", True),
        ("Location", "suspended", False),
        ("Location", "inactive", False),
        ("InsurancePlan", "active", True),
        ("InsurancePlan", "draft", False),
        ("InsurancePlan", "retired", False),
        ("InsurancePlan", "unknown", False),
        ("Endpoint", "active", True),
        ("Endpoint", "suspended", False),
        ("Endpoint", "error", False),
        ("Endpoint", "off", False),
        ("Endpoint", "entered-in-error", False),
        ("Endpoint", "test", False),
    ),
)
def test_enumerated_resource_status_maps_to_exact_activity(
    resource_type,
    status,
    expected_active,
) -> None:
    """Enumerated FHIR statuses map deterministically to active truth."""

    payload = {
        "resourceType": resource_type,
        "id": f"{resource_type.lower()}-{status}",
        "status": status,
    }
    assert _projection_row(payload)["active"] is expected_active


@pytest.mark.parametrize(
    "resource_type",
    (
        "HealthcareService",
        "Organization",
        "OrganizationAffiliation",
        "Practitioner",
        "PractitionerRole",
    ),
)
@pytest.mark.parametrize("active_flag", (True, False))
def test_boolean_resource_activity_is_preserved(resource_type, active_flag) -> None:
    """FHIR boolean activity values survive without truthiness coercion."""

    payload = {
        "resourceType": resource_type,
        "id": f"{resource_type.lower()}-{str(active_flag).lower()}",
        "active": active_flag,
    }
    assert _projection_row(payload)["active"] is active_flag


@pytest.mark.parametrize(
    "invalid_fields",
    (
        {"telecom": {"value": "2025550100"}},
        {"identifier": {"system": "urn:npi", "value": "1234567893"}},
        {"alias": "scalar-alias"},
        {"qualification": {"code": {"text": "MD"}}},
        {"period": "2026"},
        {"meta": ["not", "an", "object"]},
    ),
)
def test_malformed_present_cardinality_and_objects_fail_closed(
    invalid_fields,
) -> None:
    """Present malformed arrays and structured values cannot be ignored."""

    payload = {
        "resourceType": "Practitioner",
        "id": "practitioner-malformed",
        "active": True,
        **invalid_fields,
    }
    with pytest.raises(ProviderDirectoryProjectionError):
        _projection_row(payload)


def test_source_rank_is_deterministic_for_competing_resource_versions() -> None:
    """Competing type/id versions retain deterministic partition/hash/ordinal rank."""

    payload = {
        "resourceType": "Organization",
        "id": "organization-competing-version",
        "active": True,
    }
    first_row = _projection_row(payload, partition_ordinal=3, input_ordinal=11)
    second_row = _projection_row(payload, partition_ordinal=3, input_ordinal=12)

    assert first_row["resource_id"] == second_row["resource_id"]
    assert first_row["source_rank"].startswith("00000000000000000003:")
    assert first_row["source_rank"].endswith(":00000000000000000011")
    assert second_row["source_rank"].endswith(":00000000000000000012")
    assert first_row["source_rank"] < second_row["source_rank"]
