# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact attempt-fence identity and audit-shape rules."""

from __future__ import annotations

import datetime as dt
import re
import uuid
from typing import Any, Mapping

from process.ptg_parts.ptg2_v4_stale_metadata_json import (
    canonical_json_digest,
    database_json_digest,
    json_mapping,
    named_json_value_digest,
)
from process.ptg_parts.ptg2_v4_stale_metadata_types import (
    PTG2V4StaleMetadataContext,
    PTG2_V4_STALE_METADATA_CONTRACT,
    PTG2_V4_STALE_METADATA_ZERO_EFFECTS,
)


_CANONICAL_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$"
)
_HEX_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_MARKER_FIELDS = frozenset(
    {
        "contract",
        "status",
        "target_digest",
        "plan_digest",
        "reconciled_at",
        "server_stale_after_seconds",
        "observed_stale_age_seconds",
        "metadata_updates",
        "external_effects",
        "audit_safe",
    }
)


def utc_naive(value: Any) -> dt.datetime | None:
    """Normalize a database timestamp to naive UTC."""

    if not isinstance(value, dt.datetime):
        return None
    if value.tzinfo is not None:
        return value.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return value


def timestamp_text(value: Any) -> str | None:
    """Return one UTC database timestamp in canonical audit form."""

    normalized = utc_naive(value)
    if normalized is None:
        return None
    return normalized.isoformat(timespec="microseconds") + "Z"


def fence_nonce_text(value: Any) -> str | None:
    """Return one canonical immutable fence identity."""

    try:
        return str(uuid.UUID(str(value)))
    except (AttributeError, TypeError, ValueError):
        return None


def is_exact_marker(
    marker_by_field: Mapping[str, Any],
    *,
    target_digest: str,
) -> bool:
    """Validate the complete persisted reconciliation marker contract."""

    if set(marker_by_field) != _MARKER_FIELDS:
        return False
    stale_after_seconds = marker_by_field.get(
        "server_stale_after_seconds"
    )
    stale_age_seconds = marker_by_field.get("observed_stale_age_seconds")
    return (
        marker_by_field.get("contract") == PTG2_V4_STALE_METADATA_CONTRACT
        and marker_by_field.get("target_digest") == target_digest
        and _HEX_DIGEST_RE.fullmatch(
            str(marker_by_field.get("plan_digest") or "")
        )
        is not None
        and marker_by_field.get("status") == "reconciled"
        and _CANONICAL_TIMESTAMP_RE.fullmatch(
            str(marker_by_field.get("reconciled_at") or "")
        )
        is not None
        and type(stale_after_seconds) is int
        and stale_after_seconds > 0
        and type(stale_age_seconds) is int
        and stale_age_seconds >= stale_after_seconds
        and marker_by_field.get("metadata_updates")
        == {
            "snapshot_rows": 1,
            "internal_run_rows": 1,
            "attempt_fence_rows": 1,
        }
        and marker_by_field.get("external_effects")
        == PTG2_V4_STALE_METADATA_ZERO_EFFECTS
        and marker_by_field.get("audit_safe") is True
    )


def has_authorized_audit_marker(
    context: PTG2V4StaleMetadataContext,
    marker_by_field: Mapping[str, Any],
    *,
    target_digest: str,
) -> bool:
    """Require one complete immutable authority row for an exact retry."""

    fence_by_field = context.attempt_fence_by_field or {}
    snapshot_by_field = context.snapshot_by_field or {}
    internal_run_by_field = context.internal_run_by_field or {}
    fence_marker = json_mapping(fence_by_field.get("marker"))
    return (
        fence_by_field.get("state") == "reconciled"
        and fence_by_field.get("snapshot_id")
        == snapshot_by_field.get("snapshot_id")
        and fence_by_field.get("internal_run_id")
        == internal_run_by_field.get("import_run_id")
        and fence_nonce_text(fence_by_field.get("fence_nonce")) is not None
        and timestamp_text(fence_by_field.get("created_at")) is not None
        and fence_by_field.get("target_digest") == target_digest
        and fence_by_field.get("plan_digest")
        == marker_by_field.get("plan_digest")
        and fence_by_field.get("marker_digest")
        == canonical_json_digest(marker_by_field)
        and fence_by_field.get("marker_is_sql_null") is False
        and fence_marker == dict(marker_by_field)
        and timestamp_text(fence_by_field.get("reconciled_at"))
        == marker_by_field.get("reconciled_at")
    )


def is_pristine_active_fence(
    context: PTG2V4StaleMetadataContext,
) -> bool:
    """Require the exact unsealed authority row reviewed by the plan."""

    fence_by_field = context.attempt_fence_by_field or {}
    snapshot_by_field = context.snapshot_by_field or {}
    internal_run_by_field = context.internal_run_by_field or {}
    return (
        fence_by_field.get("state") == "active"
        and fence_by_field.get("snapshot_id")
        == snapshot_by_field.get("snapshot_id")
        and fence_by_field.get("internal_run_id")
        == internal_run_by_field.get("import_run_id")
        and fence_nonce_text(fence_by_field.get("fence_nonce")) is not None
        and timestamp_text(fence_by_field.get("created_at")) is not None
        and fence_by_field.get("target_digest") is None
        and fence_by_field.get("plan_digest") is None
        and fence_by_field.get("marker_digest") is None
        and fence_by_field.get("marker") is None
        and fence_by_field.get("marker_is_sql_null") is True
        and fence_by_field.get("reconciled_at") is None
    )


def stable_fence_payload(
    context: PTG2V4StaleMetadataContext,
) -> dict[str, Any]:
    """Bind every authority identity and audit field without disclosure."""

    fence_by_field = context.attempt_fence_by_field or {}
    nonce_text = fence_nonce_text(fence_by_field.get("fence_nonce"))
    return {
        "exists": context.attempt_fence_by_field is not None,
        "snapshot_id_digest": named_json_value_digest(
            "snapshot_id",
            fence_by_field.get("snapshot_id"),
        ),
        "internal_run_id_digest": named_json_value_digest(
            "internal_run_id",
            fence_by_field.get("internal_run_id"),
        ),
        "fence_nonce_digest": named_json_value_digest(
            "fence_nonce",
            nonce_text,
        ),
        "state": fence_by_field.get("state"),
        "target_digest": fence_by_field.get("target_digest"),
        "plan_digest": fence_by_field.get("plan_digest"),
        "stored_marker_digest": fence_by_field.get("marker_digest"),
        "marker_value_digest": database_json_digest(
            fence_by_field.get("marker")
        ),
        "marker_is_sql_null": fence_by_field.get("marker_is_sql_null"),
        "created_at": timestamp_text(fence_by_field.get("created_at")),
        "reconciled_at": timestamp_text(
            fence_by_field.get("reconciled_at")
        ),
    }


__all__ = [
    "fence_nonce_text",
    "has_authorized_audit_marker",
    "is_exact_marker",
    "is_pristine_active_fence",
    "stable_fence_payload",
    "timestamp_text",
    "utc_naive",
]
