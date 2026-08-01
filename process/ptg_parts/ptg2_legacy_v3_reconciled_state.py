# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Idempotent-state validation for a completed legacy V3 repair."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from db.migration_ptg2_legacy_v3_metadata_reconcile import (
    LEGACY_V3_RECONCILE_CONTRACT,
)
from process.ptg_parts.ptg_source_attempt_guard import canonical_digest


@dataclass(frozen=True)
class ReconciledStateReview:
    """Current evidence and precomputed immutable digest expectations."""

    observation: Mapping[str, Any]
    operational_evidence: Mapping[str, Any]
    snapshot_id: str
    internal_run_id: str
    outer_run_id: str
    target_digest: str
    capabilities_ready: bool
    retained_state_digest: str
    preserved_row_digest: str
    lineage_reasons: tuple[str, ...]
    attachment_reasons: tuple[str, ...]
    source_pair_reasons: tuple[str, ...]


def _payload(row_envelope: Any) -> Mapping[str, Any]:
    if not isinstance(row_envelope, Mapping):
        return {}
    payload_by_field = row_envelope.get("payload")
    return (
        payload_by_field
        if isinstance(payload_by_field, Mapping)
        else {}
    )


def _source_file_import_id(internal_run: Mapping[str, Any]) -> str:
    options = internal_run.get("options")
    options_by_name = options if isinstance(options, Mapping) else {}
    raw_source_id = options_by_name.get("source_file_import_id")
    return raw_source_id if isinstance(raw_source_id, str) else ""


def _integer_or_default(value: Any, default: int) -> int:
    return default if value is None else int(value)


def _audit_reasons(
    review: ReconciledStateReview,
    *,
    audit_payload: Mapping[str, Any],
    marker_by_name: Mapping[str, Any],
    source_file_import_id: str,
) -> list[str]:
    has_conflict = (
        audit_payload.get("contract") != LEGACY_V3_RECONCILE_CONTRACT
        or audit_payload.get("snapshot_id") != review.snapshot_id
        or audit_payload.get("internal_run_id") != review.internal_run_id
        or audit_payload.get("outer_run_id") != review.outer_run_id
        or audit_payload.get("target_digest") != review.target_digest
        or audit_payload.get("source_file_import_id")
        != source_file_import_id
        or marker_by_name.get("plan_digest")
        != audit_payload.get("plan_digest")
        or audit_payload.get("reconciliation_id")
        != canonical_digest(marker_by_name)
    )
    return ["reconciliation_audit_conflict"] if has_conflict else []


def _terminal_row_reasons(
    review: ReconciledStateReview,
    *,
    snapshot: Mapping[str, Any],
    internal_run: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if (
        snapshot.get("snapshot_id") != review.snapshot_id
        or snapshot.get("import_run_id") != review.internal_run_id
        or snapshot.get("status") != "failed"
        or snapshot.get("validated_at") is not None
        or snapshot.get("published_at") is not None
        or snapshot.get("manifest") not in (None, {})
    ):
        reasons.append("reconciled_snapshot_changed")
    options = internal_run.get("options")
    options_by_name = options if isinstance(options, Mapping) else {}
    if (
        internal_run.get("import_run_id") != review.internal_run_id
        or internal_run.get("status") != "failed"
        or internal_run.get("finished_at") is None
        or options_by_name.get("storage_generation") != "shared_blocks_v3"
        or options_by_name.get("snapshot_arch") != "postgres_binary_v3"
    ):
        reasons.append("reconciled_internal_run_changed")
    snapshot_rows = review.observation.get("run_snapshots")
    if (
        not isinstance(snapshot_rows, list)
        or len(snapshot_rows) != 1
        or snapshot_rows[0].get("snapshot_id") != review.snapshot_id
        or snapshot_rows[0].get("import_run_id") != review.internal_run_id
    ):
        reasons.append("reconciled_pair_cardinality_changed")
    return reasons


def _evidence_reasons(
    review: ReconciledStateReview,
    *,
    audit_payload: Mapping[str, Any],
    marker_by_name: Mapping[str, Any],
) -> list[str]:
    observation = review.observation
    has_changed = (
        audit_payload.get("attachment_digest")
        != observation.get("attachment_digest")
        or marker_by_name.get("attachment_digest")
        != observation.get("attachment_digest")
        or audit_payload.get("catalog_digest")
        != observation.get("catalog_digest")
        or marker_by_name.get("catalog_digest")
        != observation.get("catalog_digest")
        or _integer_or_default(
            audit_payload.get("event_high_water_mark"),
            -1,
        )
        != _integer_or_default(observation.get("event_high_water_mark"), 0)
        or _integer_or_default(
            marker_by_name.get("event_high_water_mark"),
            -1,
        )
        != _integer_or_default(observation.get("event_high_water_mark"), 0)
        or marker_by_name.get("retained_state_digest")
        != review.retained_state_digest
        or marker_by_name.get("preserved_row_digest")
        != review.preserved_row_digest
    )
    return ["reconciled_evidence_changed"] if has_changed else []


def reconciled_state_reasons(review: ReconciledStateReview) -> list[str]:
    """Return all reasons a completed audit is no longer idempotent."""

    audit_payload = _payload(review.observation.get("audit"))
    marker = audit_payload.get("marker")
    marker_by_name = marker if isinstance(marker, Mapping) else {}
    snapshot = _payload(review.observation.get("snapshot"))
    internal_run = _payload(review.observation.get("internal_run"))
    reasons = _audit_reasons(
        review,
        audit_payload=audit_payload,
        marker_by_name=marker_by_name,
        source_file_import_id=_source_file_import_id(internal_run),
    )
    reasons.extend(
        _terminal_row_reasons(
            review,
            snapshot=snapshot,
            internal_run=internal_run,
        )
    )
    reasons.extend(review.lineage_reasons)
    reasons.extend(review.source_pair_reasons)
    reasons.extend(review.attachment_reasons)
    reasons.extend(
        _evidence_reasons(
            review,
            audit_payload=audit_payload,
            marker_by_name=marker_by_name,
        )
    )
    if not review.capabilities_ready:
        reasons.append("shared_attempt_guard_capability_missing")
    if not review.operational_evidence.get("exact_external_absence"):
        reasons.append("external_attempt_identity_present")
    return sorted(set(reasons))


__all__ = ["ReconciledStateReview", "reconciled_state_reasons"]
