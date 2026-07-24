# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure planning and redaction rules for stale PTG V4 metadata."""

from __future__ import annotations

import datetime as dt
import hashlib
from typing import Any, Mapping

from process.ptg_parts.ptg2_v4_snapshot_maps import PTG2_V4_SHARED_GENERATION
from process.ptg_parts.ptg2_v4_stale_metadata_authority import (
    has_authorized_audit_marker,
    is_exact_marker,
    is_pristine_active_fence,
    stable_fence_payload,
    timestamp_text,
    utc_naive,
)
from process.ptg_parts.ptg2_v4_stale_metadata_json import (
    canonical_json_digest,
    database_json_digest,
    is_non_object_json_envelope,
    json_mapping,
    named_json_value_digest,
)
from process.ptg_parts.ptg2_v4_stale_metadata_types import (
    PTG2V4StaleMetadataContext,
    PTG2_V4_STALE_METADATA_CONTRACT,
    PTG2_V4_STALE_METADATA_MARKER,
    PTG2_V4_STALE_METADATA_ZERO_EFFECTS,
)


_ACTIVE_INTERNAL_RUN_STATUSES = frozenset({"pending", "running", "building"})
_PHYSICAL_LAYOUT_REPORT_PREFIXES = ("shared_", "physical_")


def stale_target_digest(snapshot_id: str, internal_run_id: str) -> str:
    """Hash the length-delimited exact coordinates without disclosing them."""

    digest = hashlib.sha256()
    digest.update(b"ptg2-v4-stale-metadata-target-v1\0")
    for coordinate in (snapshot_id, internal_run_id):
        encoded = coordinate.encode("utf-8")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _marker_from(value: Any) -> dict[str, Any]:
    envelope_by_field = json_mapping(value)
    return json_mapping(envelope_by_field.get(PTG2_V4_STALE_METADATA_MARKER))


def _physical_layout_report_keys(value: Any) -> tuple[str, ...]:
    """Return report fields proving physical-layout or recovery activity."""

    report_by_field = json_mapping(value)
    return tuple(
        sorted(
            field_name
            for field_name in report_by_field
            if isinstance(field_name, str)
            and (
                field_name.lower().startswith(
                    _PHYSICAL_LAYOUT_REPORT_PREFIXES
                )
                or "layout" in field_name.lower()
                or "recovery" in field_name.lower()
            )
        )
    )


def _stale_reference_at(
    context: PTG2V4StaleMetadataContext,
) -> dt.datetime | None:
    internal_run = context.internal_run_by_field or {}
    snapshot = context.snapshot_by_field or {}
    return (
        utc_naive(internal_run.get("heartbeat_at"))
        or utc_naive(internal_run.get("started_at"))
        or utc_naive(snapshot.get("created_at"))
    )


def _stale_age_seconds(
    context: PTG2V4StaleMetadataContext,
) -> int | None:
    stale_reference_at = _stale_reference_at(context)
    observed_at = utc_naive(context.observed_at)
    if stale_reference_at is None or observed_at is None:
        return None
    return int((observed_at - stale_reference_at).total_seconds())


def _stale_cutoff_decision(
    context: PTG2V4StaleMetadataContext,
    stale_after_seconds: int,
) -> tuple[bool | None, str | None]:
    stale_age_seconds = _stale_age_seconds(context)
    if stale_age_seconds is None:
        return None, "stale_reference_missing"
    if stale_age_seconds < stale_after_seconds:
        return False, "internal_run_not_stale"
    return True, None


def _attachment_total(context: PTG2V4StaleMetadataContext) -> int:
    return sum(int(value) for value in context.attachment_count_by_name.values())


def exact_stale_marker(
    context: PTG2V4StaleMetadataContext,
    *,
    target_digest: str,
) -> dict[str, Any] | None:
    """Return the matching dual-row marker for a write-free retry."""

    snapshot = context.snapshot_by_field or {}
    internal_run = context.internal_run_by_field or {}
    if snapshot.get("import_run_id") != internal_run.get("import_run_id"):
        return None
    snapshot_marker = _marker_from(snapshot.get("manifest"))
    internal_run_marker = _marker_from(internal_run.get("report"))
    if snapshot_marker != internal_run_marker:
        return None
    if _attachment_total(context):
        return None
    if not is_exact_marker(snapshot_marker, target_digest=target_digest):
        return None
    if not has_authorized_audit_marker(
        context,
        snapshot_marker,
        target_digest=target_digest,
    ):
        return None
    if snapshot.get("status") != "failed" or internal_run.get("status") != "failed":
        return None
    return snapshot_marker


def _append_attempt_state_reasons(
    reason_codes: list[str],
    context: PTG2V4StaleMetadataContext,
    *,
    internal_run_id: str,
) -> None:
    """Append eligibility failures from the present snapshot/run pair."""

    snapshot = context.snapshot_by_field
    internal_run = context.internal_run_by_field
    assert snapshot is not None
    assert internal_run is not None
    if snapshot.get("import_run_id") != internal_run_id:
        reason_codes.append("snapshot_run_pair_mismatch")
    if snapshot.get("status") != "building":
        reason_codes.append("snapshot_not_building")
    if snapshot.get("validated_at") is not None:
        reason_codes.append("snapshot_validation_evidence_present")
    if snapshot.get("published_at") is not None:
        reason_codes.append("snapshot_publication_evidence_present")
    if internal_run.get("status") not in _ACTIVE_INTERNAL_RUN_STATUSES:
        reason_codes.append("internal_run_not_active")
    if internal_run.get("finished_at") is not None:
        reason_codes.append("internal_run_finished_at_present")
    if internal_run.get("error") not in (None, ""):
        reason_codes.append("internal_run_error_present")
    options_by_field = json_mapping(internal_run.get("options"))
    if options_by_field.get("storage_generation") != PTG2_V4_SHARED_GENERATION:
        reason_codes.append("internal_run_not_v4")
    fence_by_field = context.attempt_fence_by_field
    if fence_by_field is None:
        reason_codes.append("attempt_fence_missing")
    elif fence_by_field.get("state") != "active":
        reason_codes.append("attempt_fence_not_active")
    elif not is_pristine_active_fence(context):
        reason_codes.append("attempt_fence_audit_not_pristine")
    if json_mapping(snapshot.get("manifest")):
        reason_codes.append("snapshot_manifest_not_empty")
    if is_non_object_json_envelope(snapshot, "manifest"):
        reason_codes.append("snapshot_manifest_not_object")
    if is_non_object_json_envelope(internal_run, "report"):
        reason_codes.append("internal_run_report_not_object")
    if _marker_from(snapshot.get("manifest")) or _marker_from(
        internal_run.get("report")
    ):
        reason_codes.append("inconsistent_reconciliation_marker")
    if _physical_layout_report_keys(internal_run.get("report")):
        reason_codes.append("physical_layout_or_recovery_report_present")


def _reason_codes(
    context: PTG2V4StaleMetadataContext,
    *,
    internal_run_id: str,
    stale_after_seconds: int,
    target_digest: str,
) -> tuple[str, ...]:
    """Return every fail-closed eligibility reason for one reviewed pair."""

    if exact_stale_marker(context, target_digest=target_digest) is not None:
        return ()
    reason_codes: list[str] = []
    if context.snapshot_by_field is None:
        reason_codes.append("snapshot_not_found")
    if context.internal_run_by_field is None:
        reason_codes.append("internal_run_not_found")
    if reason_codes:
        return tuple(reason_codes)
    _append_attempt_state_reasons(
        reason_codes,
        context,
        internal_run_id=internal_run_id,
    )
    _stale_cutoff_reached, stale_reason_code = _stale_cutoff_decision(
        context, stale_after_seconds
    )
    if stale_reason_code is not None:
        reason_codes.append(stale_reason_code)
    if _attachment_total(context):
        reason_codes.append("attached_state_present")
    return tuple(reason_codes)


def _stable_plan_payload(
    context: PTG2V4StaleMetadataContext,
    *, internal_run_id: str, stale_after_seconds: int, target_digest: str,
) -> dict[str, Any]:
    """Bind all reviewed eligibility inputs into a non-identifying digest."""

    snapshot = context.snapshot_by_field or {}
    internal_run = context.internal_run_by_field or {}
    options_by_field = json_mapping(internal_run.get("options"))
    source_key = str(options_by_field.get("source_key") or "").strip()
    snapshot_manifest = snapshot.get("manifest")
    run_report = internal_run.get("report")
    physical_layout_report_keys = _physical_layout_report_keys(run_report)
    stale_cutoff_reached, stale_reason_code = _stale_cutoff_decision(context, stale_after_seconds)
    return {
        "contract": PTG2_V4_STALE_METADATA_CONTRACT,
        "action": "metadata_only_stale_build_reconcile",
        "target_digest": target_digest,
        "server_stale_after_seconds": stale_after_seconds,
        "stale_policy_decision": {
            "cutoff_reached": stale_cutoff_reached,
            "reason_code": stale_reason_code,
        },
        "snapshot": {
            "exists": context.snapshot_by_field is not None,
            "pair_matches": snapshot.get("import_run_id") == internal_run_id,
            "status": snapshot.get("status"),
            "created_at": timestamp_text(snapshot.get("created_at")),
            "validated_at": timestamp_text(snapshot.get("validated_at")),
            "published_at": timestamp_text(snapshot.get("published_at")),
            "previous_snapshot_id_digest": named_json_value_digest("previous_snapshot_id", snapshot.get("previous_snapshot_id")),
            "manifest_digest": database_json_digest(snapshot_manifest),
            "manifest_is_sql_null": snapshot.get("manifest_is_sql_null"),
            "marker_digest": database_json_digest(_marker_from(snapshot_manifest)),
        },
        "internal_run": {
            "exists": context.internal_run_by_field is not None,
            "status": internal_run.get("status"),
            "started_at": timestamp_text(internal_run.get("started_at")),
            "finished_at": timestamp_text(internal_run.get("finished_at")),
            "heartbeat_at": timestamp_text(internal_run.get("heartbeat_at")),
            "error_digest": named_json_value_digest("error", internal_run.get("error")),
            "storage_generation": options_by_field.get("storage_generation"),
            "source_key_digest": canonical_json_digest({"source_key": source_key}),
            "report_digest": database_json_digest(run_report),
            "report_is_sql_null": internal_run.get("report_is_sql_null"),
            "physical_layout_report_keys": list(physical_layout_report_keys),
            "marker_digest": database_json_digest(_marker_from(run_report)),
        },
        "attempt_fence": stable_fence_payload(context),
        "attachment_counts": dict(sorted(context.attachment_count_by_name.items())),
    }


def _plan_decision(
    context: PTG2V4StaleMetadataContext,
    *,
    internal_run_id: str,
    stale_after_seconds: int,
    target_digest: str,
) -> dict[str, Any]:
    stable_payload = _stable_plan_payload(
        context,
        internal_run_id=internal_run_id,
        stale_after_seconds=stale_after_seconds,
        target_digest=target_digest,
    )
    plan_digest = canonical_json_digest(stable_payload)
    idempotent_marker = exact_stale_marker(
        context,
        target_digest=target_digest,
    )
    reason_codes = _reason_codes(
        context,
        internal_run_id=internal_run_id,
        stale_after_seconds=stale_after_seconds,
        target_digest=target_digest,
    )
    status = (
        "already_reconciled"
        if idempotent_marker is not None
        else ("ready" if not reason_codes else "ineligible")
    )
    return {
        "status": status,
        "plan_digest": (
            str(idempotent_marker["plan_digest"])
            if idempotent_marker is not None
            else plan_digest
        ),
        "idempotent": idempotent_marker is not None,
        "reason_codes": list(reason_codes),
    }


def _plan_report(
    context: PTG2V4StaleMetadataContext,
    *,
    stale_after_seconds: int,
    target_digest: str,
    decision_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    is_idempotent = bool(decision_by_field["idempotent"])
    planned_row_count = (
        1 if decision_by_field["status"] == "ready" else 0
    )
    return {
        "contract": PTG2_V4_STALE_METADATA_CONTRACT,
        "action": "metadata_only_stale_build_reconcile",
        "status": decision_by_field["status"],
        "target_digest": target_digest,
        "plan_digest": decision_by_field["plan_digest"],
        "idempotent": is_idempotent,
        "reason_codes": list(decision_by_field["reason_codes"]),
        "server_policy": {
            "stale_after_seconds": stale_after_seconds,
            "request_override_supported": False,
        },
        "observations": {
            "stale_age_seconds": _stale_age_seconds(context),
            "internal_run_report_digest": database_json_digest(
                (context.internal_run_by_field or {}).get("report")
            ),
            "physical_layout_report_keys": list(
                _physical_layout_report_keys(
                    (context.internal_run_by_field or {}).get("report")
                )
            ),
            "attachment_counts": dict(
                sorted(context.attachment_count_by_name.items())
            ),
            "attachment_total": _attachment_total(context),
        },
        "planned_metadata_updates": {
            "snapshot_rows": planned_row_count,
            "internal_run_rows": planned_row_count,
            "attempt_fence_rows": planned_row_count,
        },
        "external_effects": dict(PTG2_V4_STALE_METADATA_ZERO_EFFECTS),
        "audit_safe": True,
    }


def build_stale_plan(
    context: PTG2V4StaleMetadataContext,
    *,
    internal_run_id: str,
    stale_after_seconds: int,
    target_digest: str,
) -> dict[str, Any]:
    """Build a redacted review plan from stable stored state."""

    decision_by_field = _plan_decision(
        context,
        internal_run_id=internal_run_id,
        stale_after_seconds=stale_after_seconds,
        target_digest=target_digest,
    )
    return _plan_report(
        context,
        stale_after_seconds=stale_after_seconds,
        target_digest=target_digest,
        decision_by_field=decision_by_field,
    )


__all__ = [
    "build_stale_plan",
    "exact_stale_marker",
    "stale_target_digest",
    "timestamp_text",
]
