# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Audit-safe reports for PTG V4 stale metadata reconciliation."""

from __future__ import annotations

from typing import Any, Mapping

from process.ptg_parts.ptg2_v4_stale_metadata_types import (
    PTG2_V4_STALE_METADATA_CONTRACT,
    PTG2_V4_STALE_METADATA_ZERO_EFFECTS,
)


def build_stale_marker(
    *,
    plan_by_field: Mapping[str, Any],
    reconciled_at_text: str,
) -> dict[str, Any]:
    """Build the identical marker persisted on both metadata rows."""

    return {
        "contract": PTG2_V4_STALE_METADATA_CONTRACT,
        "status": "reconciled",
        "target_digest": plan_by_field["target_digest"],
        "plan_digest": plan_by_field["plan_digest"],
        "reconciled_at": reconciled_at_text,
        "server_stale_after_seconds": plan_by_field["server_policy"][
            "stale_after_seconds"
        ],
        "observed_stale_age_seconds": plan_by_field["observations"][
            "stale_age_seconds"
        ],
        "metadata_updates": {
            "snapshot_rows": 1,
            "internal_run_rows": 1,
            "attempt_fence_rows": 1,
        },
        "external_effects": dict(PTG2_V4_STALE_METADATA_ZERO_EFFECTS),
        "audit_safe": True,
    }


def build_stale_report(
    *,
    marker_by_field: Mapping[str, Any],
    is_idempotent: bool,
) -> dict[str, Any]:
    """Return the redacted execute result with explicit zero side effects."""

    metadata_updates = (
        {
            "snapshot_rows": 0,
            "internal_run_rows": 0,
            "attempt_fence_rows": 0,
        }
        if is_idempotent
        else dict(marker_by_field["metadata_updates"])
    )
    return {
        "contract": PTG2_V4_STALE_METADATA_CONTRACT,
        "action": "metadata_only_stale_build_reconcile",
        "status": (
            "already_reconciled" if is_idempotent else "reconciled"
        ),
        "target_digest": marker_by_field["target_digest"],
        "plan_digest": marker_by_field["plan_digest"],
        "reconciled_at": marker_by_field["reconciled_at"],
        "idempotent": is_idempotent,
        "metadata_updates": metadata_updates,
        "external_effects": dict(PTG2_V4_STALE_METADATA_ZERO_EFFECTS),
        "audit_safe": True,
    }


__all__ = ["build_stale_marker", "build_stale_report"]
