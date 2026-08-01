# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure eligibility and digest contract for legacy PTG V3 repair."""

from __future__ import annotations

import datetime as dt
from typing import Any, Mapping

from db.migration_ptg2_legacy_v3_metadata_reconcile import (
    LEGACY_V3_RECONCILE_CONTRACT,
)
from process.ptg_parts.ptg2_legacy_v3_metadata_store import (
    ALLOWED_ATTACHMENT_NAMES,
)
from process.ptg_parts.ptg2_legacy_v3_metadata_lineage import (
    legacy_v3_outer_lineage_reasons,
)
from process.ptg_parts.ptg2_legacy_v3_metadata_digests import (
    legacy_v3_preserved_row_digest,
    legacy_v3_retained_state_digest,
)
from process.ptg_parts.ptg2_legacy_v3_plan_evidence import (
    legacy_v3_plan_evidence,
)
from process.ptg_parts.ptg2_legacy_v3_reconciled_state import (
    ReconciledStateReview,
    reconciled_state_reasons,
)
from process.ptg_parts.ptg_source_attempt_guard import canonical_digest


LEGACY_V3_STALE_AFTER_SECONDS = 21_600
_ACTIVE_INTERNAL_STATUSES = frozenset(
    {"queued", "starting", "running", "finalizing"}
)


def _payload(row_envelope: Any) -> Mapping[str, Any]:
    if not isinstance(row_envelope, Mapping):
        return {}
    payload = row_envelope.get("payload")
    return payload if isinstance(payload, Mapping) else {}


def _timestamp(value: Any) -> dt.datetime | None:
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        candidate = str(value or "").strip()
        if not candidate:
            return None
        try:
            parsed = dt.datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def _stale_age_seconds(
    observation: Mapping[str, Any],
    observed_at: dt.datetime,
) -> int | None:
    internal_run = _payload(observation.get("internal_run"))
    reference = _timestamp(internal_run.get("heartbeat_at"))
    if reference is None:
        return None
    return max(0, int((observed_at - reference).total_seconds()))


def legacy_v3_target_digest(
    *,
    snapshot_id: str,
    internal_run_id: str,
    outer_run_id: str,
) -> str:
    """Bind one reviewed repair to all three live coordinates."""

    return canonical_digest(
        {
            "contract": LEGACY_V3_RECONCILE_CONTRACT,
            "snapshot_id": snapshot_id,
            "internal_run_id": internal_run_id,
            "outer_run_id": outer_run_id,
        }
    )


def _attachment_reasons(observation: Mapping[str, Any]) -> list[str]:
    raw_counts = observation.get("attachment_counts")
    counts_by_name = raw_counts if isinstance(raw_counts, Mapping) else {}
    reasons: list[str] = []
    present_names = {
        str(name)
        for name, count in counts_by_name.items()
        if int(count) > 0
    }
    missing_relations = sorted(
        str(name)
        for name, count in counts_by_name.items()
        if int(count) < 0
    )
    if missing_relations:
        reasons.append("attachment_catalog_incomplete")
    if present_names != ALLOWED_ATTACHMENT_NAMES:
        reasons.append("attachment_set_not_exact")
    retained_count_by_name = {
        name: int(counts_by_name.get(name) or 0)
        for name in ALLOWED_ATTACHMENT_NAMES
    }
    if any(count == 0 for count in retained_count_by_name.values()):
        reasons.append("retained_attachment_missing")
    if any(count != 1 for count in retained_count_by_name.values()):
        reasons.append("retained_attachment_cardinality_changed")
    raw_dynamic = observation.get("dynamic_relations")
    dynamic_by_name = raw_dynamic if isinstance(raw_dynamic, Mapping) else {}
    if not dynamic_by_name.get("suffix_valid"):
        reasons.append("legacy_suffix_unproved")
    if int(dynamic_by_name.get("relation_count") or 0) != 0:
        reasons.append("legacy_dynamic_relation_present")
    return reasons


def _state_reasons(
    observation: Mapping[str, Any],
    *,
    snapshot_id: str,
    internal_run_id: str,
) -> tuple[list[str], str]:
    reasons: list[str] = []
    snapshot = _payload(observation.get("snapshot"))
    internal_run = _payload(observation.get("internal_run"))
    if snapshot.get("snapshot_id") != snapshot_id:
        reasons.append("snapshot_missing")
    if snapshot.get("import_run_id") != internal_run_id:
        reasons.append("snapshot_run_pair_changed")
    if snapshot.get("status") != "building":
        reasons.append("snapshot_not_building")
    if snapshot.get("validated_at") is not None:
        reasons.append("snapshot_validated")
    if snapshot.get("published_at") is not None:
        reasons.append("snapshot_published")
    if snapshot.get("manifest") not in (None, {}):
        reasons.append("snapshot_manifest_present")
    if internal_run.get("import_run_id") != internal_run_id:
        reasons.append("internal_run_missing")
    if str(internal_run.get("status") or "") not in _ACTIVE_INTERNAL_STATUSES:
        reasons.append("internal_run_not_active")
    if internal_run.get("finished_at") is not None:
        reasons.append("internal_run_already_finished")
    options = internal_run.get("options")
    options_by_name = options if isinstance(options, Mapping) else {}
    if options_by_name.get("storage_generation") != "shared_blocks_v3":
        reasons.append("internal_run_not_shared_blocks_v3")
    if options_by_name.get("snapshot_arch") != "postgres_binary_v3":
        reasons.append("internal_run_not_postgres_binary_v3")
    raw_source_file_import_id = options_by_name.get(
        "source_file_import_id"
    )
    source_file_import_id = (
        raw_source_file_import_id
        if isinstance(raw_source_file_import_id, str)
        else ""
    )
    if (
        not source_file_import_id
        or source_file_import_id != source_file_import_id.strip()
        or len(source_file_import_id) > 64
    ):
        reasons.append("source_file_import_id_missing")
    run_snapshots = observation.get("run_snapshots")
    snapshot_rows = run_snapshots if isinstance(run_snapshots, list) else []
    if len(snapshot_rows) != 1:
        reasons.append("internal_run_snapshot_cardinality_changed")
    elif snapshot_rows[0].get("snapshot_id") != snapshot_id:
        reasons.append("internal_run_reverse_pair_changed")
    return reasons, source_file_import_id


def _source_pair_reasons(
    observation: Mapping[str, Any],
    *,
    snapshot_id: str,
    internal_run_id: str,
) -> list[str]:
    source_internal_runs = observation.get("source_internal_runs")
    internal_rows = (
        source_internal_runs if isinstance(source_internal_runs, list) else []
    )
    internal_ids = {
        str(_payload(run_envelope).get("import_run_id") or "")
        for run_envelope in internal_rows
    }
    source_snapshots = observation.get("source_snapshots")
    snapshot_rows = (
        source_snapshots if isinstance(source_snapshots, list) else []
    )
    snapshot_pairs = {
        (
            str(snapshot_by_field.get("snapshot_id") or ""),
            str(snapshot_by_field.get("import_run_id") or ""),
        )
        for snapshot_by_field in snapshot_rows
        if isinstance(snapshot_by_field, Mapping)
    }
    reasons: list[str] = []
    if len(internal_rows) != 1 or internal_ids != {internal_run_id}:
        reasons.append("source_internal_run_cardinality_changed")
    if len(snapshot_rows) != 1 or snapshot_pairs != {
        (snapshot_id, internal_run_id)
    }:
        reasons.append("source_snapshot_cardinality_changed")
    return reasons


def _eligibility_reasons(
    observation: Mapping[str, Any],
    operational_evidence: Mapping[str, Any],
    *,
    snapshot_id: str,
    internal_run_id: str,
    outer_run_id: str,
    observed_at: dt.datetime,
    capabilities_ready: bool,
) -> tuple[list[str], str, int | None]:
    reasons, source_file_import_id = _state_reasons(
        observation,
        snapshot_id=snapshot_id,
        internal_run_id=internal_run_id,
    )
    reasons.extend(
        legacy_v3_outer_lineage_reasons(
            observation,
            outer_run_id=outer_run_id,
            source_file_import_id=source_file_import_id,
            snapshot_id=snapshot_id,
        )
    )
    reasons.extend(
        _source_pair_reasons(
            observation,
            snapshot_id=snapshot_id,
            internal_run_id=internal_run_id,
        )
    )
    reasons.extend(_attachment_reasons(observation))
    stale_age_seconds = _stale_age_seconds(observation, observed_at)
    if stale_age_seconds is None:
        reasons.append("stale_reference_missing")
    elif stale_age_seconds < LEGACY_V3_STALE_AFTER_SECONDS:
        reasons.append("internal_run_not_stale")
    if not capabilities_ready:
        reasons.append("shared_attempt_guard_capability_missing")
    if not operational_evidence.get("exact_external_absence"):
        reasons.append("external_attempt_identity_present")
    return sorted(set(reasons)), source_file_import_id, stale_age_seconds


def _planned_effects(is_eligible: bool) -> dict[str, int]:
    row_count = 1 if is_eligible else 0
    return {
        "snapshot_rows_updated": row_count,
        "internal_run_rows_updated": row_count,
        "audit_rows_inserted": row_count,
        "attachment_rows_changed": 0,
        "external_effects": 0,
    }


def _new_reconcile_plan(
    observation: Mapping[str, Any],
    operational_evidence: Mapping[str, Any],
    *,
    target_digest: str,
    reason_codes: list[str],
    source_file_import_id: str,
    stale_age_seconds: int | None,
) -> dict[str, Any]:
    is_eligible = not reason_codes
    plan_digest = canonical_digest(
        legacy_v3_plan_evidence(
            observation,
            operational_evidence,
            target_digest=target_digest,
            stale_after_seconds=LEGACY_V3_STALE_AFTER_SECONDS,
        )
    )
    return {
        "contract": LEGACY_V3_RECONCILE_CONTRACT,
        "status": "ready" if is_eligible else "ineligible",
        "eligible": is_eligible,
        "idempotent": False,
        "target_digest": target_digest,
        "plan_digest": plan_digest if is_eligible else None,
        "source_attempt_digest": canonical_digest(
            {"source_file_import_id": source_file_import_id}
        ),
        "reason_codes": reason_codes,
        "stale_policy_seconds": LEGACY_V3_STALE_AFTER_SECONDS,
        "stale_age_seconds": stale_age_seconds,
        "event_high_water_mark": observation.get("event_high_water_mark"),
        "attachment_counts": observation.get("attachment_counts"),
        "attachment_digest": observation.get("attachment_digest"),
        "catalog_digest": observation.get("catalog_digest"),
        "retained_state_digest": legacy_v3_retained_state_digest(
            observation
        ),
        "preserved_row_digest": legacy_v3_preserved_row_digest(observation),
        "planned_effects": _planned_effects(is_eligible),
    }


def _reconciled_review(
    observation: Mapping[str, Any],
    operational_evidence: Mapping[str, Any],
    *,
    snapshot_id: str,
    internal_run_id: str,
    outer_run_id: str,
    target_digest: str,
    capabilities_ready: bool,
) -> ReconciledStateReview:
    source_file_import_id = str(
        observation.get("source_file_import_id") or ""
    )
    return ReconciledStateReview(
        observation=observation,
        operational_evidence=operational_evidence,
        snapshot_id=snapshot_id,
        internal_run_id=internal_run_id,
        outer_run_id=outer_run_id,
        target_digest=target_digest,
        capabilities_ready=capabilities_ready,
        retained_state_digest=legacy_v3_retained_state_digest(observation),
        preserved_row_digest=legacy_v3_preserved_row_digest(observation),
        lineage_reasons=tuple(
            legacy_v3_outer_lineage_reasons(
                observation,
                outer_run_id=outer_run_id,
                source_file_import_id=source_file_import_id,
                snapshot_id=snapshot_id,
            )
        ),
        attachment_reasons=tuple(_attachment_reasons(observation)),
        source_pair_reasons=tuple(
            _source_pair_reasons(
                observation,
                snapshot_id=snapshot_id,
                internal_run_id=internal_run_id,
            )
        ),
    )


def _existing_audit_plan(
    observation: Mapping[str, Any],
    operational_evidence: Mapping[str, Any],
    *,
    snapshot_id: str,
    internal_run_id: str,
    outer_run_id: str,
    target_digest: str,
    capabilities_ready: bool,
) -> dict[str, Any] | None:
    existing_audit = observation.get("audit")
    if not isinstance(existing_audit, Mapping):
        return None
    audit_reasons = reconciled_state_reasons(
        _reconciled_review(
            observation,
            operational_evidence,
            snapshot_id=snapshot_id,
            internal_run_id=internal_run_id,
            outer_run_id=outer_run_id,
            target_digest=target_digest,
            capabilities_ready=capabilities_ready,
        )
    )
    if audit_reasons:
        return {
            "contract": LEGACY_V3_RECONCILE_CONTRACT,
            "status": "ineligible",
            "eligible": False,
            "idempotent": False,
            "target_digest": target_digest,
            "plan_digest": None,
            "reason_codes": audit_reasons,
        }
    audit_payload = _payload(existing_audit)
    return {
        "contract": LEGACY_V3_RECONCILE_CONTRACT,
        "status": "already_reconciled",
        "eligible": True,
        "idempotent": True,
        "target_digest": target_digest,
        "plan_digest": audit_payload.get("plan_digest"),
        "reason_codes": [],
        "planned_effects": _planned_effects(False),
    }


def build_legacy_v3_reconcile_plan(
    observation: Mapping[str, Any],
    operational_evidence: Mapping[str, Any],
    *,
    snapshot_id: str,
    internal_run_id: str,
    outer_run_id: str,
    observed_at: dt.datetime,
    capabilities_ready: bool,
) -> dict[str, Any]:
    """Return the deterministic redacted plan for one exact target."""

    target_digest = legacy_v3_target_digest(
        snapshot_id=snapshot_id,
        internal_run_id=internal_run_id,
        outer_run_id=outer_run_id,
    )
    existing_plan = _existing_audit_plan(
        observation,
        operational_evidence,
        snapshot_id=snapshot_id,
        internal_run_id=internal_run_id,
        outer_run_id=outer_run_id,
        target_digest=target_digest,
        capabilities_ready=capabilities_ready,
    )
    if existing_plan is not None:
        return existing_plan
    reason_codes, source_file_import_id, stale_age_seconds = (
        _eligibility_reasons(
            observation,
            operational_evidence,
            snapshot_id=snapshot_id,
            internal_run_id=internal_run_id,
            outer_run_id=outer_run_id,
            observed_at=observed_at,
            capabilities_ready=capabilities_ready,
        )
    )
    return _new_reconcile_plan(
        observation,
        operational_evidence,
        target_digest=target_digest,
        reason_codes=reason_codes,
        source_file_import_id=source_file_import_id,
        stale_age_seconds=stale_age_seconds,
    )


__all__ = [
    "LEGACY_V3_STALE_AFTER_SECONDS",
    "build_legacy_v3_reconcile_plan",
    "legacy_v3_retained_state_digest",
    "legacy_v3_preserved_row_digest",
    "legacy_v3_target_digest",
]
