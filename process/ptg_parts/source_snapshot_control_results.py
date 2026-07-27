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
        "deleted_artifact_chunks": 0,
        "deleted_artifact_manifests": 0,
        "deleted_snapshots": 0,
        "released_shared_layouts": 0,
        "queued_shared_block_candidates": 0,
        "queued_shared_block_bytes": 0,
        "physical_cleanup": "not_applicable",
    }


def unsupported_snapshot_remove_plan(
    *,
    snapshot_id: str,
    source_key: str | None,
    snapshot: dict[str, Any],
    storage_generation: str,
    reason: str,
) -> dict[str, Any]:
    """Build a fail-closed plan for an unsupported storage manifest."""

    return {
        "snapshot_id": snapshot_id,
        "source_key": source_key,
        "exists": True,
        "removable": False,
        "reason": reason,
        "metadata_only": True,
        "tables": [],
        "artifact_manifest_ids": [],
        "current_references": {},
        "storage_generation": storage_generation or None,
        "status": snapshot.get("status"),
        "import_month": str(snapshot.get("import_month") or ""),
    }


def supported_snapshot_remove_plan(
    *,
    snapshot_id: str,
    source_key: str | None,
    snapshot: dict[str, Any],
    serving_index: dict[str, Any],
    storage_generation: str,
    references: dict[str, Any],
    artifact_ids: list[str],
    reasons: list[str],
) -> dict[str, Any]:
    """Build a generation-aware removal plan for a supported manifest."""

    return {
        "snapshot_id": snapshot_id,
        "source_key": source_key,
        "exists": True,
        "removable": not reasons,
        "reason": "; ".join(reasons) if reasons else None,
        "metadata_only": False,
        "tables": [],
        "artifact_manifest_ids": artifact_ids,
        "current_references": references,
        "storage_generation": storage_generation,
        "shared_snapshot_key": serving_index.get("shared_snapshot_key"),
        "status": snapshot.get("status"),
        "import_month": str(snapshot.get("import_month") or ""),
    }


def executed_snapshot_remove_plan(
    *,
    plan: dict[str, Any],
    deletion_counts: dict[str, int],
    layout_keys: tuple[int, ...],
    shared_layout_release: Any | None,
) -> dict[str, Any]:
    """Attach transactional deletion and shared-layout release outcomes."""

    released_layouts = int(
        getattr(shared_layout_release, "logical_layout_count", 0) or 0
    )
    queued_candidates = int(
        getattr(shared_layout_release, "candidate_hash_count", 0) or 0
    )
    queued_bytes = int(getattr(shared_layout_release, "stored_bytes", 0) or 0)
    return {
        **plan,
        "executed": True,
        "deleted_tables": 0,
        **deletion_counts,
        "released_shared_layouts": released_layouts,
        "queued_shared_block_candidates": queued_candidates,
        "queued_shared_block_bytes": queued_bytes,
        "physical_cleanup": (
            "released"
            if released_layouts
            else ("deferred" if layout_keys else "not_applicable")
        ),
    }
