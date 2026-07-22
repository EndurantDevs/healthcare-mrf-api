# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic, source-free fixtures for native Provider Directory projection."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class SyntheticFHIRFixture:
    """One deterministic local FHIR input with no source identity or URL."""

    framing: str
    resources: tuple[dict[str, Any], ...]
    encoded: bytes


@dataclass(frozen=True)
class _ResourceCoordinates:
    sequence: int
    suffix: str
    organization_id: str
    network_id: str
    location_id: str
    practitioner_id: str

    @classmethod
    def from_sequence(cls, sequence: int) -> "_ResourceCoordinates":
        suffix = f"{sequence:06d}"
        return cls(
            sequence=sequence,
            suffix=suffix,
            organization_id=f"organization-{suffix}",
            network_id=f"network-{suffix}",
            location_id=f"location-{suffix}",
            practitioner_id=f"practitioner-{suffix}",
        )


def _endpoint(coordinates: _ResourceCoordinates) -> dict[str, Any]:
    return {
        "resourceType": "Endpoint",
        "id": f"endpoint-{coordinates.suffix}",
        "status": "active",
        "connectionType": {
            "system": "urn:synthetic:connection",
            "code": "test",
        },
        "payloadType": [{"text": "Synthetic fixture"}],
        "address": "urn:synthetic:offline",
    }


def _healthcare_service(coordinates: _ResourceCoordinates) -> dict[str, Any]:
    return {
        "resourceType": "HealthcareService",
        "id": f"service-{coordinates.suffix}",
        "active": True,
        "providedBy": {
            "reference": f"Organization/{coordinates.organization_id}"
        },
        "location": [{"reference": f"Location/{coordinates.location_id}"}],
        "name": f"Synthetic service {coordinates.sequence}",
    }


def _insurance_plan(coordinates: _ResourceCoordinates) -> dict[str, Any]:
    return {
        "resourceType": "InsurancePlan",
        "id": f"plan-{coordinates.suffix}",
        "status": "active",
        "name": f"Synthetic plan {coordinates.sequence}",
        "network": [
            {"reference": f"Organization/{coordinates.network_id}"}
        ],
    }


def _postal_address(coordinates: _ResourceCoordinates) -> dict[str, Any]:
    return {
        "line": [f"{coordinates.sequence + 1} Fixture Avenue"],
        "city": "Example",
        "state": "IA",
        "postalCode": f"{50000 + coordinates.sequence % 1000:05d}",
        "country": "US",
    }


def _location(coordinates: _ResourceCoordinates) -> dict[str, Any]:
    return {
        "resourceType": "Location",
        "id": coordinates.location_id,
        "status": "active",
        "name": f"Synthetic location {coordinates.sequence}",
        "address": _postal_address(coordinates),
        "position": {"latitude": 41.6, "longitude": -93.6},
    }


def _organization(coordinates: _ResourceCoordinates) -> dict[str, Any]:
    return {
        "resourceType": "Organization",
        "id": coordinates.organization_id,
        "active": True,
        "name": f"Synthetic organization {coordinates.sequence}",
        "address": [_postal_address(coordinates)],
    }


def _organization_affiliation(
    coordinates: _ResourceCoordinates,
) -> dict[str, Any]:
    return {
        "resourceType": "OrganizationAffiliation",
        "id": f"affiliation-{coordinates.suffix}",
        "active": True,
        "organization": {
            "reference": f"Organization/{coordinates.organization_id}"
        },
        "participatingOrganization": {
            "reference": f"Organization/{coordinates.network_id}"
        },
        "network": [
            {"reference": f"Organization/{coordinates.network_id}"}
        ],
        "location": [{"reference": f"Location/{coordinates.location_id}"}],
    }


def _practitioner(coordinates: _ResourceCoordinates) -> dict[str, Any]:
    return {
        "resourceType": "Practitioner",
        "id": coordinates.practitioner_id,
        "active": True,
        "identifier": [
            {
                "system": "http://hl7.org/fhir/sid/us-npi",
                "value": "1234567893",
            }
        ],
        "name": [
            {
                "family": f"Fixture{coordinates.sequence}",
                "given": ["Alex"],
            }
        ],
        "telecom": [{"system": "phone", "value": "2025550100"}],
    }


def _practitioner_role(coordinates: _ResourceCoordinates) -> dict[str, Any]:
    return {
        "resourceType": "PractitionerRole",
        "id": f"role-{coordinates.suffix}",
        "active": True,
        "practitioner": {
            "reference": f"Practitioner/{coordinates.practitioner_id}"
        },
        "organization": {
            "reference": f"Organization/{coordinates.organization_id}"
        },
        "location": [{"reference": f"Location/{coordinates.location_id}"}],
        "specialty": [
            {
                "coding": [
                    {
                        "system": "urn:synthetic:specialty",
                        "code": "fixture",
                    }
                ]
            }
        ],
    }


def _resource_set(sequence: int) -> tuple[dict[str, Any], ...]:
    """Return all eight supported resource families for one synthetic group."""

    coordinates = _ResourceCoordinates.from_sequence(sequence)
    return (
        _endpoint(coordinates),
        _healthcare_service(coordinates),
        _insurance_plan(coordinates),
        _location(coordinates),
        _organization(coordinates),
        _organization_affiliation(coordinates),
        _practitioner(coordinates),
        _practitioner_role(coordinates),
    )


def synthetic_fhir_resources(groups: int = 1) -> tuple[dict[str, Any], ...]:
    """Return deterministic resources suitable for correctness or throughput."""

    if groups < 1:
        raise ValueError("groups must be positive")
    return tuple(
        resource
        for sequence in range(groups)
        for resource in _resource_set(sequence)
    )


def canonical_json_bytes(json_content: Any) -> bytes:
    return json.dumps(
        json_content,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def fhir_fixture(
    framing: str,
    resources: Sequence[Mapping[str, Any]] | None = None,
    *,
    groups: int = 1,
) -> SyntheticFHIRFixture:
    """Encode one synthetic FHIR fixture as NDJSON or a searchset Bundle."""

    normalized_resources = tuple(
        dict(resource)
        for resource in (resources or synthetic_fhir_resources(groups))
    )
    if framing == "ndjson":
        encoded = b"".join(
            canonical_json_bytes(resource) + b"\n"
            for resource in normalized_resources
        )
    elif framing == "bundle":
        encoded = canonical_json_bytes(
            {
                "resourceType": "Bundle",
                "type": "searchset",
                "entry": [
                    {"resource": resource}
                    for resource in normalized_resources
                ],
            }
        )
    else:
        raise ValueError("framing must be ndjson or bundle")
    return SyntheticFHIRFixture(framing, normalized_resources, encoded)


def resource_counts(resources: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    """Return sorted FHIR resource-family counts for assertions."""

    resource_count_by_type: dict[str, int] = {}
    for resource in resources:
        resource_type = str(resource["resourceType"])
        resource_count_by_type[resource_type] = (
            resource_count_by_type.get(resource_type, 0) + 1
        )
    return dict(sorted(resource_count_by_type.items()))


__all__ = (
    "SyntheticFHIRFixture",
    "canonical_json_bytes",
    "fhir_fixture",
    "resource_counts",
    "synthetic_fhir_resources",
)
