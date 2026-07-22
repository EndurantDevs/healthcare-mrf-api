# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared 55-case semantic corpus for Python and native-Rust parity."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ProjectionSemanticCase:
    """One named FHIR resource and whether strict projection accepts it."""

    name: str
    resource: dict[str, Any]
    accepted: bool = True


def _case(name: str, resource: dict[str, Any]) -> ProjectionSemanticCase:
    return ProjectionSemanticCase(name, resource)


def _unrelated_concept_cases() -> list[ProjectionSemanticCase]:
    resources = (
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
                {"code": {"coding": [{"system": "urn:license", "code": "MD"}]}}
            ],
        },
    )
    return [_case(f"unrelated-concept-{index}", resource) for index, resource in enumerate(resources)]


def _specialty_and_npi_cases() -> list[ProjectionSemanticCase]:
    """Return explicit specialty, taxonomy, and NPI precedence cases."""

    specialty_cases = []
    for resource_type in (
        "HealthcareService",
        "OrganizationAffiliation",
        "PractitionerRole",
    ):
        specialty_cases.append(
            _case(
                f"explicit-specialty-{resource_type.lower()}",
                {
                    "resourceType": resource_type,
                    "id": f"{resource_type.lower()}-specialty",
                    "active": True,
                    "specialty": [{"coding": [{"system": "http://nucc.org/provider-taxonomy", "code": "207Q00000X"}]}],
                },
            )
        )
    specialty_cases.append(
        _case(
            "practitioner-taxonomy",
            {
                "resourceType": "Practitioner",
                "id": "practitioner-taxonomy",
                "active": True,
                "qualification": [{"code": {"text": "Family Medicine", "coding": [{"system": "urn:vendor", "code": "207Q00000X"}]}}],
            },
        )
    )
    return [*specialty_cases, *_npi_cases()]


def _npi_cases() -> list[ProjectionSemanticCase]:
    npi_cases = [
            _case(
                "exact-npi-priority",
                {
                    "resourceType": "Practitioner",
                    "id": "practitioner-npi-priority",
                    "active": True,
                    "identifier": [
                        {"system": "urn:vendor:npi", "value": "1999999999"},
                        {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567893"},
                    ],
                },
            ),
            _case(
                "fuzzy-npi-fallback",
                {
                    "resourceType": "HealthcareService",
                    "id": "service-fuzzy-npi",
                    "active": True,
                    "identifier": [{"type": {"text": "National Provider NPI"}, "value": "1-987-654-321"}],
                },
            ),
    ]
    for resource_type, resource_id in (
        ("Practitioner", "1000000000"),
        ("Organization", "2999999999"),
        ("HealthcareService", "1234567893"),
        ("PractitionerRole", "1234567893"),
        ("Practitioner", "0123456789"),
        ("Practitioner", "0001234567893"),
        ("Organization", "9999999999"),
    ):
        npi_cases.append(
            _case(
                f"id-npi-{resource_type.lower()}-{resource_id}",
                {"resourceType": resource_type, "id": resource_id, "active": True},
            )
        )
    return npi_cases


def _reference_cases() -> list[ProjectionSemanticCase]:
    network_reference_url = "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/network-reference"
    participating_network_url = "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/plannet-ParticipatingNetwork-extension"
    return [
        _case(
            "reference-forms",
            {
                "resourceType": "InsurancePlan",
                "id": "plan-reference-forms",
                "status": "active",
                "network": [{"reference": reference_text} for reference_text in (
                    "network-bare",
                    "Organization/network-relative",
                    "https://directory.example/fhir/Organization/network-absolute",
                    "Organization/network-versioned/_history/7",
                    "Organization/network-query?active=true",
                    "Organization/network-fragment#contained",
                )],
            },
        ),
        _case(
            "network-extensions",
            {
                "resourceType": "InsurancePlan",
                "id": "plan-network-extensions",
                "status": "active",
                "network": [{"reference": "Organization/network-one"}],
                "extension": [
                    {"url": network_reference_url, "valueReference": {"reference": "Organization/network-one"}},
                    {"url": network_reference_url.replace("http://", "https://") + "|1.2.0", "valueReference": {"reference": "Organization/network-two"}},
                    {"url": "urn:synthetic:nesting", "extension": [{"url": participating_network_url, "valueReference": {"reference": "Organization/network-three"}}]},
                ],
            },
        ),
        _case(
            "plan-coverage-networks",
            {
                "resourceType": "InsurancePlan",
                "id": "plan-backbone-networks",
                "status": "active",
                "network": [{"reference": "Organization/network-one"}],
                "plan": [{"network": [{"reference": "Organization/network-two"}]}],
                "coverage": [{"network": [{"reference": "Organization/network-one"}]}],
            },
        ),
        _case("role-insurance-plan", {"resourceType": "PractitionerRole", "id": "role-plan", "active": True, "insurancePlan": [{"reference": "InsurancePlan/plan-one"}]}),
        _case("location-part-of", {"resourceType": "Location", "id": "location-parent", "status": "active", "partOf": {"reference": "Location/location-root"}}),
        _case("endpoint-contact", {"resourceType": "Endpoint", "id": "endpoint-contact", "status": "active", "contact": [{"system": "phone", "value": "2025550100"}]}),
    ]


def _status_cases() -> list[ProjectionSemanticCase]:
    statuses_by_resource_type = {
        "Location": ("active", "suspended", "inactive"),
        "InsurancePlan": ("active", "draft", "retired", "unknown"),
        "Endpoint": ("active", "suspended", "error", "off", "entered-in-error", "test"),
    }
    status_cases = [
        _case(
            f"status-{resource_type.lower()}-{status}",
            {"resourceType": resource_type, "id": f"{resource_type.lower()}-{status}", "status": status},
        )
        for resource_type, statuses in statuses_by_resource_type.items()
        for status in statuses
    ]
    for resource_type in (
        "HealthcareService",
        "Organization",
        "OrganizationAffiliation",
        "Practitioner",
        "PractitionerRole",
    ):
        for active_flag in (True, False):
            status_cases.append(
                _case(
                    f"boolean-{resource_type.lower()}-{str(active_flag).lower()}",
                    {"resourceType": resource_type, "id": f"{resource_type.lower()}-{str(active_flag).lower()}", "active": active_flag},
                )
            )
    return status_cases


def _malformed_cases() -> list[ProjectionSemanticCase]:
    invalid_fields = (
        {"telecom": {"value": "2025550100"}},
        {"identifier": {"system": "urn:npi", "value": "1234567893"}},
        {"alias": "scalar-alias"},
        {"qualification": {"code": {"text": "MD"}}},
        {"period": "2026"},
        {"meta": ["not", "an", "object"]},
    )
    return [
        ProjectionSemanticCase(
            f"malformed-{invalid_ordinal}",
            {
                "resourceType": "Practitioner",
                "id": f"practitioner-malformed-{invalid_ordinal}",
                "active": True,
                **fields,
            },
            accepted=False,
        )
        for invalid_ordinal, fields in enumerate(invalid_fields)
    ]


def semantic_projection_cases() -> tuple[ProjectionSemanticCase, ...]:
    """Return the frozen semantic matrix shared by both implementations."""

    cases = [
        *_unrelated_concept_cases(),
        *_specialty_and_npi_cases(),
        *_reference_cases(),
        *_status_cases(),
        *_malformed_cases(),
        _case(
            "deterministic-source-rank",
            {
                "resourceType": "Organization",
                "id": "organization-competing-version",
                "active": True,
            },
        ),
    ]
    if len(cases) != 55:
        raise AssertionError(f"semantic corpus must contain 55 cases, got {len(cases)}")
    return tuple(cases)


__all__ = ("ProjectionSemanticCase", "semantic_projection_cases")
