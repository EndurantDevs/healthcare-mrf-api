# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""FHIR response checks shared by collection imports and access probes."""

from typing import Any


def has_operation_outcome_error(outcome: dict[str, Any]) -> bool:
    """Treat missing, fatal, error, and unknown issue severities as failures."""
    issues = [issue for issue in outcome.get("issue") or [] if isinstance(issue, dict)]
    if not issues:
        return True
    return any(str(issue.get("severity") or "").strip().lower() not in {"warning", "information"} for issue in issues)


def is_access_denied_outcome(resource_payload: dict[str, Any] | None) -> bool:
    """Identify permission failures encoded in an OperationOutcome."""
    if not resource_payload or resource_payload.get("resourceType") != "OperationOutcome":
        return False
    issue_text = " ".join(
        str(issue.get("diagnostics") or issue.get("details", {}).get("text") or "")
        for issue in resource_payload.get("issue") or []
        if isinstance(issue, dict)
    ).lower()
    return any(marker in issue_text for marker in ("access denied", "not authorized", "not authorised", "forbidden"))
