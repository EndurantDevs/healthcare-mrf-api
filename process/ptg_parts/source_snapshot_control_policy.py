# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admission and reference policy for targeted source snapshot control."""

from __future__ import annotations

import json
from typing import Any

from process.ptg_parts.snapshot_cleanup import (
    _is_ptg2_snapshot_in_flight,
    is_shared_snapshot_control_manifest,
)


SUPPORTED_SHARED_SNAPSHOT_CONTROL_MESSAGE = (
    "only postgres_binary_v3/shared_blocks_v3 or "
    "postgres_binary_v3/shared_blocks_v4 snapshots"
)


def manifest_dict(value: Any) -> dict[str, Any]:
    """Decode a stored manifest while treating malformed values as empty."""

    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _is_remove_blocked(snapshot_status: Any) -> bool:
    """Allow explicit cleanup of validated candidates while builds stay protected."""

    normalized_status = str(snapshot_status or "").strip().lower()
    return (
        _is_ptg2_snapshot_in_flight(normalized_status)
        and normalized_status != "validated"
    )


def snapshot_remove_reasons(
    *,
    source_key: str | None,
    manifest_source_key: str | None,
    snapshot_status: str,
    references: dict[str, list[str]],
) -> list[str]:
    """Describe every current or rollback reference that prevents removal."""

    reasons: list[str] = []
    if source_key and manifest_source_key and source_key != manifest_source_key:
        reasons.append("snapshot source_key does not match requested source_key")
    if _is_remove_blocked(snapshot_status):
        reasons.append(f"snapshot is in-flight (status: {snapshot_status})")
    label_by_reference_name = {
        "global_slots": "current global",
        "source_keys": "current source",
        "plan_source_keys": "current plan",
        "previous_global_slots": "previous global",
        "previous_source_keys": "previous source",
        "previous_plan_source_keys": "previous plan",
        "plan_release_pins": "plan release pin",
    }
    reasons.extend(
        f"snapshot is referenced by {label} pointer"
        for reference_name, label in label_by_reference_name.items()
        if references.get(reference_name)
    )
    return reasons


def retirement_manifest_source_key(
    snapshot: dict[str, Any],
    requested_source_key: str | None,
) -> str | None:
    """Validate a snapshot's immutable identity before retiring its pointers."""

    if not snapshot:
        return None
    manifest_by_field = manifest_dict(snapshot.get("manifest"))
    serving_index_by_field = (
        manifest_by_field.get("serving_index")
        if isinstance(manifest_by_field.get("serving_index"), dict)
        else {}
    )
    manifest_source_key = (
        str(serving_index_by_field.get("source_key") or "").strip() or None
    )
    if not is_shared_snapshot_control_manifest(serving_index_by_field):
        raise ValueError(
            f"{SUPPORTED_SHARED_SNAPSHOT_CONTROL_MESSAGE} can be retired"
        )
    snapshot_status = str(snapshot.get("status") or "").strip().lower()
    if _is_ptg2_snapshot_in_flight(snapshot_status):
        raise ValueError(f"snapshot is in-flight (status: {snapshot_status})")
    if (
        requested_source_key
        and manifest_source_key
        and requested_source_key != manifest_source_key
    ):
        raise ValueError("snapshot source_key does not match requested source_key")
    return manifest_source_key
