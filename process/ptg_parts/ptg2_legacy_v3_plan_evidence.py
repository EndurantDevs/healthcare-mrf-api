# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Digest-bound redacted evidence for one legacy V3 repair plan."""

from __future__ import annotations

from typing import Any, Mapping

from process.ptg_parts.ptg_source_attempt_guard import canonical_digest


def legacy_v3_plan_evidence(
    observation: Mapping[str, Any],
    operational_evidence: Mapping[str, Any],
    *,
    target_digest: str,
    stale_after_seconds: int,
) -> dict[str, Any]:
    """Return all mutable gates covered by the executable plan digest."""

    return {
        "target_digest": target_digest,
        "snapshot": observation.get("snapshot"),
        "internal_run": observation.get("internal_run"),
        "run_snapshots": observation.get("run_snapshots"),
        "source_internal_runs": observation.get("source_internal_runs"),
        "source_snapshots": observation.get("source_snapshots"),
        "outer_runs_digest": canonical_digest(
            observation.get("outer_runs") or []
        ),
        "control_mirrors_digest": canonical_digest(
            observation.get("control_run_mirrors") or []
        ),
        "source_import_digest": canonical_digest(
            observation.get("source_import_rows") or []
        ),
        "placement_digest": canonical_digest(
            observation.get("placement_rows") or []
        ),
        "event_high_water_mark": observation.get("event_high_water_mark"),
        "event_digest": observation.get("event_digest"),
        "attachment_counts": observation.get("attachment_counts"),
        "attachment_digest": observation.get("attachment_digest"),
        "catalog_digest": observation.get("catalog_digest"),
        "dynamic_relations": observation.get("dynamic_relations"),
        "operational_digest": canonical_digest(operational_evidence),
        "stale_after_seconds": stale_after_seconds,
    }


__all__ = ["legacy_v3_plan_evidence"]
