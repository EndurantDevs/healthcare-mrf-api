"""Validation contracts for maintained Provider Directory support metadata."""

from __future__ import annotations

import datetime as dt
import re
import urllib.parse
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
SOURCE_ID_PATTERN = re.compile(r"pdfhir_[0-9a-f]{24}")
FRESHNESS_POLICY_FIELDS = {
    "catalog_confirmation_max_age_days",
    "source_review_max_age_days",
    "terminal_verification_max_age_days",
}
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


def _validate_endpoint_identity(entry_id: str, entry: dict[str, Any]) -> None:
    """Validate one configured endpoint's display, source, and URL identity."""
    display_name = entry.get("display_name")
    source_ids = entry.get("source_ids")
    canonical_base = entry.get("canonical_base")
    if not isinstance(display_name, str) or not display_name.strip():
        raise SupportDocumentationError(f"{entry_id}: display_name must be non-empty text")
    if (
        not isinstance(source_ids, list)
        or not source_ids
        or len(source_ids) != len(set(source_ids))
        or not all(
            isinstance(source_id, str) and SOURCE_ID_PATTERN.fullmatch(source_id)
            for source_id in source_ids
        )
    ):
        raise SupportDocumentationError(
            f"{entry_id}: source_ids must contain unique full pdfhir IDs"
        )
    if not isinstance(canonical_base, str):
        raise SupportDocumentationError(
            f"{entry_id}: canonical_base must be a credential-free HTTPS URL"
        )
    parsed_base = urllib.parse.urlsplit(canonical_base)
    if (
        parsed_base.scheme != "https"
        or not parsed_base.netloc
        or parsed_base.username
        or parsed_base.password
        or parsed_base.fragment
    ):
        raise SupportDocumentationError(
            f"{entry_id}: canonical_base must be a credential-free HTTPS URL"
        )


def _validate_endpoint_resources(entry_id: str, entry: dict[str, Any]) -> None:
    """Validate a configured endpoint's explicit resource collection."""
    resources = entry.get("resources")
    if (
        not isinstance(resources, list)
        or len(resources) != len(set(resources))
        or not all(isinstance(resource, str) and resource in RESOURCE_TYPES for resource in resources)
    ):
        raise SupportDocumentationError(
            f"{entry_id}: resources must contain unique known resource types"
        )


def _validate_endpoint_classification(
    entry_id: str,
    entry: dict[str, Any],
    support: dict[str, Any],
) -> None:
    """Keep support labels and methods aligned with executable classification."""
    classification = entry.get("classification")
    resources = entry.get("resources")
    expected_support = {
        "acquisition": ("acquisition-configured", "rest", True),
        "bulk_acquisition": ("acquisition-configured", "bulk", True),
        "probe_only": ("probe-only", "probe", False),
        "external": ("externally-supported", "graphql", False),
    }.get(classification)
    if expected_support is None:
        raise SupportDocumentationError(f"{entry_id}: invalid classification")
    support_level, method, expects_resources = expected_support
    if support.get("support_level") != support_level or support.get("method") != method:
        raise SupportDocumentationError(
            f"{entry_id}: {classification} classification requires "
            f"{support_level} support and {method} method"
        )
    if bool(resources) != expects_resources:
        requirement = "non-empty" if expects_resources else "empty"
        raise SupportDocumentationError(
            f"{entry_id}: {classification} resources must be {requirement}"
        )
    documented_resources = support.get("documented_resources")
    if classification == "external" and not documented_resources:
        raise SupportDocumentationError(
            f"{entry_id}: external support requires documented_resources"
        )
    if classification != "external" and documented_resources is not None:
        raise SupportDocumentationError(
            f"{entry_id}: documented_resources is reserved for external support"
        )


def validate_configured_endpoint(entry: dict[str, Any], support: dict[str, Any]) -> None:
    """Validate the endpoint identity and its documented acquisition contract."""
    entry_id = str(entry.get("entry_id") or "")
    _validate_endpoint_identity(entry_id, entry)
    _validate_endpoint_resources(entry_id, entry)
    _validate_endpoint_classification(entry_id, entry, support)


def validate_freshness_policy(manifest: dict[str, Any]) -> dict[str, int]:
    """Return the controlled positive-age documentation policy."""
    freshness_policy = manifest.get("support_documentation", {}).get(
        "freshness_policy"
    )
    if (
        not isinstance(freshness_policy, dict)
        or set(freshness_policy) != FRESHNESS_POLICY_FIELDS
    ):
        raise SupportDocumentationError(
            "support_documentation.freshness_policy must contain "
            f"{sorted(FRESHNESS_POLICY_FIELDS)}"
        )
    if not all(
        isinstance(age_days, int) and not isinstance(age_days, bool) and age_days > 0
        for age_days in freshness_policy.values()
    ):
        raise SupportDocumentationError("freshness policy ages must be positive integer days")
    return freshness_policy


def parse_review_date(date_value: Any, label: str) -> dt.date:
    """Parse an ISO review date with a documentation-specific error."""
    try:
        return dt.date.fromisoformat(str(date_value or ""))
    except ValueError as exc:
        raise SupportDocumentationError(f"{label} must be an ISO date") from exc


def parse_timestamp_date(timestamp_value: Any, label: str) -> dt.date:
    """Parse a timezone-qualified timestamp and return its calendar date."""
    if not isinstance(timestamp_value, str) or not timestamp_value.strip():
        raise SupportDocumentationError(f"{label} must be ISO-8601")
    try:
        parsed_timestamp = dt.datetime.fromisoformat(
            timestamp_value.replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise SupportDocumentationError(f"{label} must be ISO-8601") from exc
    if parsed_timestamp.tzinfo is None:
        raise SupportDocumentationError(f"{label} must include a timezone")
    return parsed_timestamp.date()


def review_valid_through(reviewed_on: dt.date, maximum_age_days: int) -> str:
    """Return the inclusive final date for maintained evidence."""
    return (reviewed_on + dt.timedelta(days=maximum_age_days)).isoformat()


def _review_expiration_messages(
    manifest: dict[str, Any],
    blocker_entries: list[dict[str, Any]],
    evaluation_date: dt.date,
) -> list[str]:
    """Return expiration messages for catalog and source reviews."""
    freshness_policy = validate_freshness_policy(manifest)
    catalog_confirmation = manifest["catalog_confirmation"]
    catalog_date = parse_timestamp_date(
        catalog_confirmation["checked_at"], "catalog_confirmation.checked_at"
    )
    catalog_due = catalog_date + dt.timedelta(
        days=freshness_policy["catalog_confirmation_max_age_days"]
    )
    expiration_messages = []
    if evaluation_date > catalog_due:
        expiration_messages.append(
            f"catalog confirmation expired {catalog_due.isoformat()}"
        )
    support_records_by_entry = manifest["support_documentation"]["entry_support"]
    source_reviews = [
        (entry["entry_id"], support_records_by_entry[entry["entry_id"]]["reviewed_at"])
        for entry in manifest["entries"]
    ] + [
        (blocker_entry["id"], blocker_entry["reviewed_at"])
        for blocker_entry in blocker_entries
    ]
    for entry_id, reviewed_at in source_reviews:
        reviewed_on = parse_review_date(reviewed_at, f"{entry_id}: reviewed_at")
        review_due = reviewed_on + dt.timedelta(
            days=freshness_policy["source_review_max_age_days"]
        )
        if evaluation_date > review_due:
            expiration_messages.append(
                f"{entry_id} review expired {review_due.isoformat()}"
            )
    return expiration_messages


def _verification_expiration_messages(
    manifest: dict[str, Any],
    verification_snapshot: dict[str, Any],
    evaluation_date: dt.date,
) -> list[str]:
    """Return expiration messages for current terminal proof."""
    maximum_age_days = validate_freshness_policy(manifest)[
        "terminal_verification_max_age_days"
    ]
    expiration_messages = []
    verification_records_by_entry = verification_snapshot.get("entries", {})
    for entry_id, verification_record in verification_records_by_entry.items():
        if (
            not isinstance(verification_record, dict)
            or verification_record.get("proof_state") == "superseded"
            or verification_record.get("terminal_status") is None
            or verification_record.get("checked_at") is None
        ):
            continue
        checked_on = parse_timestamp_date(
            verification_record["checked_at"], f"{entry_id}: checked_at"
        )
        proof_due = checked_on + dt.timedelta(days=maximum_age_days)
        if evaluation_date > proof_due:
            expiration_messages.append(
                f"{entry_id} terminal proof expired {proof_due.isoformat()}"
            )
    return expiration_messages


def validate_support_freshness(
    manifest: dict[str, Any],
    blocker_entries: list[dict[str, Any]],
    verification_snapshot: dict[str, Any],
    evaluation_date: dt.date,
) -> None:
    """Fail when maintained catalog, source, or current proof review has expired."""
    expiration_messages = _review_expiration_messages(
        manifest, blocker_entries, evaluation_date
    )
    expiration_messages.extend(
        _verification_expiration_messages(
            manifest, verification_snapshot, evaluation_date
        )
    )
    if expiration_messages:
        raise SupportDocumentationError(
            "stale Provider Directory support evidence: "
            + "; ".join(expiration_messages)
        )


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
