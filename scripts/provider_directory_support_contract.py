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
