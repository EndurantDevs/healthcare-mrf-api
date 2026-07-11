"""Validation contracts for maintained Provider Directory support metadata."""

from __future__ import annotations

import datetime as dt
from typing import Any

ACCESS_REQUIREMENTS = {
    "none",
    "oauth2-client-credentials",
    "private-connector",
    "user-token",
    "unknown",
}
SUPPORT_LEVELS = {
    "supported",
    "externally-supported",
    "current-connector",
    "acquisition-configured",
    "probe-only",
    "blocked",
    "not-supported",
}
RESOURCE_TYPES = {
    "InsurancePlan",
    "PractitionerRole",
    "Practitioner",
    "Organization",
    "Location",
    "HealthcareService",
    "OrganizationAffiliation",
    "Endpoint",
}
BLOCKER_OPERATIONAL_STATUSES = {"unreachable", "auth-gated", "not-published"}
BLOCKER_ACQUISITION_METHODS = {"not-importable"}
BLOCKER_REQUIRED_FIELDS = {
    "id",
    "display_name",
    "plan_name",
    "acquisition_method",
    "documented_resources",
    "canonical_base",
    "access_requirement",
    "requires_registration",
    "operational_status",
    "live_verification",
    "reviewed_at",
    "source_detail",
    "source_url",
    "reason",
    "note",
}


class SupportDocumentationError(ValueError):
    """Raised when generated support documentation cannot be trusted."""


def validate_access_review_metadata(entry_id: str, support: dict[str, Any]) -> None:
    """Validate registration and review claims for one configured endpoint."""
    requires_registration = support["requires_registration"]
    access_requirement = support["access_requirement"]
    if not isinstance(requires_registration, bool):
        raise SupportDocumentationError(
            f"{entry_id}: requires_registration must be boolean"
        )
    if access_requirement == "none" and requires_registration:
        raise SupportDocumentationError(
            f"{entry_id}: public access cannot require registration"
        )
    gated_access_requirements = {
        "oauth2-client-credentials",
        "private-connector",
        "user-token",
    }
    if access_requirement in gated_access_requirements and not requires_registration:
        raise SupportDocumentationError(
            f"{entry_id}: configured access requires registration or a private arrangement"
        )
    try:
        dt.date.fromisoformat(str(support["reviewed_at"] or ""))
    except ValueError as exc:
        raise SupportDocumentationError(
            f"{entry_id}: reviewed_at must be an ISO date"
        ) from exc


def _validate_blocker_acquisition(entry_id: str, entry: dict[str, Any]) -> None:
    acquisition_method = entry.get("acquisition_method")
    if (
        not isinstance(acquisition_method, str)
        or acquisition_method not in BLOCKER_ACQUISITION_METHODS
    ):
        raise SupportDocumentationError(f"{entry_id}: invalid acquisition method")
    documented_resources = entry.get("documented_resources")
    if (
        not isinstance(documented_resources, list)
        or not all(isinstance(resource, str) for resource in documented_resources)
        or len(documented_resources) != len(set(documented_resources))
        or not all(resource in RESOURCE_TYPES for resource in documented_resources)
    ):
        raise SupportDocumentationError(
            f"{entry_id}: documented_resources must contain unique known resource types"
        )
    if entry.get("canonical_base") is not None:
        raise SupportDocumentationError(
            f"{entry_id}: blocked canonical_base must be null until confirmed"
        )
    if entry.get("live_verification") != {
        "status": "not_recorded",
        "checked_at": None,
    }:
        raise SupportDocumentationError(
            f"{entry_id}: blocked live_verification must remain not recorded"
        )


def _validate_blocker_evidence(entry_id: str, entry: dict[str, Any]) -> None:
    text_fields = BLOCKER_REQUIRED_FIELDS - {
        "canonical_base",
        "documented_resources",
        "live_verification",
        "requires_registration",
    }
    for field_name in text_fields:
        if not isinstance(entry.get(field_name), str) or not entry[field_name].strip():
            raise SupportDocumentationError(
                f"{entry_id}: {field_name} must be non-empty text"
            )
    if not str(entry["source_url"]).startswith("https://"):
        raise SupportDocumentationError(f"{entry_id}: source_url must use HTTPS")


def _validate_blocker_entry(entry: Any, seen_ids: set[str]) -> dict[str, Any]:
    if not isinstance(entry, dict) or set(entry) != BLOCKER_REQUIRED_FIELDS:
        raise SupportDocumentationError(
            f"blocker entries must contain {sorted(BLOCKER_REQUIRED_FIELDS)}"
        )
    entry_id = str(entry.get("id") or "")
    if not entry_id or entry_id in seen_ids:
        raise SupportDocumentationError(
            "blocker entries must have unique non-empty ids"
        )
    seen_ids.add(entry_id)
    access_requirement = entry.get("access_requirement")
    if (
        not isinstance(access_requirement, str)
        or access_requirement not in ACCESS_REQUIREMENTS
    ):
        raise SupportDocumentationError(f"{entry_id}: invalid access requirement")
    operational_status = entry.get("operational_status")
    if (
        not isinstance(operational_status, str)
        or operational_status not in BLOCKER_OPERATIONAL_STATUSES
    ):
        raise SupportDocumentationError(f"{entry_id}: invalid operational status")
    validate_access_review_metadata(entry_id, entry)
    _validate_blocker_acquisition(entry_id, entry)
    _validate_blocker_evidence(entry_id, entry)
    return entry


def validate_blocker_registry(registry: dict[str, Any]) -> list[dict[str, Any]]:
    """Validate blocked sources that cannot be acquisition manifest entries."""
    if not isinstance(registry, dict) or registry.get("schema_version") != 2:
        raise SupportDocumentationError("blocker registry schema_version must be 2")
    entries = registry.get("entries")
    if not isinstance(entries, list) or not entries:
        raise SupportDocumentationError(
            "blocker registry entries must be a non-empty list"
        )
    seen_ids: set[str] = set()
    return [_validate_blocker_entry(entry, seen_ids) for entry in entries]
