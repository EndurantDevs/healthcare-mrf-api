# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure result builders for source-snapshot control operations."""

from __future__ import annotations

from typing import Any


def missing_snapshot_remove_plan(
    snapshot_id: str,
    source_key: str | None,
) -> dict[str, Any]:
    """Build the idempotent no-op plan for an absent source snapshot."""

    return {
        "snapshot_id": snapshot_id,
        "source_key": source_key,
        "exists": False,
        "removable": True,
        "metadata_only": True,
        "tables": [],
        "artifact_manifest_ids": [],
        "current_references": {},
    }


def executed_empty_remove_plan(plan: dict[str, Any]) -> dict[str, Any]:
    """Mark an absent-snapshot removal plan as successfully executed."""

    return {
        **plan,
        "executed": True,
        "deleted_tables": 0,
        "deleted_v3_snapshot_scopes": 0,
        "deleted_v3_snapshot_bindings": 0,
        "deleted_artifact_manifests": 0,
        "deleted_snapshots": 0,
        "released_shared_layouts": 0,
    }
