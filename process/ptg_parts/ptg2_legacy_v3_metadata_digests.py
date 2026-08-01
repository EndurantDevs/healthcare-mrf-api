# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserved-state digests for the legacy V3 metadata-only repair."""

from __future__ import annotations

from typing import Any, Mapping

from process.ptg_parts.ptg_source_attempt_guard import canonical_digest


def _payload(row_envelope: Any) -> Mapping[str, Any]:
    if not isinstance(row_envelope, Mapping):
        return {}
    payload = row_envelope.get("payload")
    return payload if isinstance(payload, Mapping) else {}


def legacy_v3_retained_state_digest(
    observation: Mapping[str, Any],
) -> str:
    """Digest every row/catalog fact that the repair must not change."""

    source_internal_runs = observation.get("source_internal_runs")
    source_internal_ids = sorted(
        str(_payload(run_envelope).get("import_run_id") or "")
        for run_envelope in (
            source_internal_runs
            if isinstance(source_internal_runs, list)
            else []
        )
    )
    source_snapshots = observation.get("source_snapshots")
    source_snapshot_pairs = sorted(
        (
            str(snapshot_by_field.get("snapshot_id") or ""),
            str(snapshot_by_field.get("import_run_id") or ""),
        )
        for snapshot_by_field in (
            source_snapshots if isinstance(source_snapshots, list) else []
        )
        if isinstance(snapshot_by_field, Mapping)
    )
    return canonical_digest(
        {
            "outer_runs": observation.get("outer_runs") or [],
            "control_run_mirrors": observation.get(
                "control_run_mirrors"
            )
            or [],
            "source_import_rows": observation.get("source_import_rows")
            or [],
            "placement_rows": observation.get("placement_rows") or [],
            "source_internal_run_ids": source_internal_ids,
            "source_snapshot_pairs": source_snapshot_pairs,
            "event_high_water_mark": observation.get(
                "event_high_water_mark"
            ),
            "event_digest": observation.get("event_digest"),
            "attachment_counts": observation.get("attachment_counts"),
            "attachment_digest": observation.get("attachment_digest"),
            "catalog_digest": observation.get("catalog_digest"),
            "dynamic_relations": observation.get("dynamic_relations"),
        }
    )


def legacy_v3_preserved_row_digest(
    observation: Mapping[str, Any],
) -> str:
    """Digest the snapshot/run fields that the two-row CAS preserves."""

    snapshot_by_field = dict(_payload(observation.get("snapshot")))
    internal_run_by_field = dict(_payload(observation.get("internal_run")))
    snapshot_by_field.pop("status", None)
    for field_name in ("status", "finished_at", "heartbeat_at"):
        internal_run_by_field.pop(field_name, None)
    return canonical_digest(
        {
            "snapshot": snapshot_by_field,
            "internal_run": internal_run_by_field,
        }
    )


__all__ = [
    "legacy_v3_preserved_row_digest",
    "legacy_v3_retained_state_digest",
]
