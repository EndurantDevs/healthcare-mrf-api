"""Immutable failure receipts and read-only recovery selection."""

from __future__ import annotations

from typing import Any, Iterable

from process.ptg_wave_failure_kubernetes import (
    _verify_preclaim_kubernetes_failure,
)
from process.ptg_wave_failure_types import (
    CLAIMED_PRESTART_FAILURE_REASON,
    CLAIMED_PRESTART_FAILURE_SCHEMA,
    FAILURE_REASONS,
    FAILURE_SCHEMA,
    PTGWaveFailureConflict,
    PTGWaveReadOnlyRecovery,
    _claimed_ordinals_digest,
    _digest,
    _require_mapping,
    _unclaimed_ordinals_digest,
    is_claimed_prestart_failure_receipt,
)
from process.ptg_wave_failure_validation import _verify_failure_redis
from process.ptg_wave_state import canonical_json, sha256_digest


def read_only_recovery_plan(wave: Any) -> PTGWaveReadOnlyRecovery | None:
    """Return the sole GET-only action for an unresolved mutation ticket."""

    confirmed_failure_reason = _confirmed_failure_reason(wave)
    if (
        getattr(wave, "kubernetes_delete_ticket", None) is not None
        and getattr(wave, "kubernetes_delete_evidence_digest", None) is None
    ):
        return PTGWaveReadOnlyRecovery(
            "kubernetes_delete",
            wave.kubernetes_delete_ticket,
            "get_exact_job_and_labeled_pods_absence",
            False,
        )
    if (
        getattr(wave, "redis_cleanup_ticket", None) is not None
        and getattr(wave, "redis_cleanup_evidence_digest", None) is None
    ):
        return PTGWaveReadOnlyRecovery(
            "redis_cleanup",
            wave.redis_cleanup_ticket,
            "get_exact_wave_key_absence",
            False,
        )
    if (
        getattr(wave, "redis_release_ticket", None) is not None
        and getattr(wave, "redis_release_attestation_digest", None) is None
        and confirmed_failure_reason != "redis_release_absent"
    ):
        return PTGWaveReadOnlyRecovery(
            "redis_release",
            wave.redis_release_ticket,
            "get_exact_release_receipt",
            False,
        )
    if (
        getattr(wave, "k8s_post_ticket", None) is not None
        and getattr(wave, "kubernetes_job_receipt_digest", None) is None
        and confirmed_failure_reason != "kubernetes_post_absent"
    ):
        return PTGWaveReadOnlyRecovery(
            "kubernetes_post",
            wave.k8s_post_ticket,
            "get_exact_job_by_persisted_name_and_manifest_identity",
            False,
        )
    return None


def _confirmed_failure_reason(wave: Any) -> str | None:
    if getattr(wave, "failure_receipt_digest", None) is None:
        return None
    failure_receipt = _require_failure_receipt(
        wave,
        getattr(wave, "failure_receipt", None),
        require_origin_state=False,
    )
    if (
        sha256_digest(canonical_json(failure_receipt))
        != wave.failure_receipt_digest
    ):
        raise PTGWaveFailureConflict(
            "persisted failure receipt digest is corrupt"
        )
    return failure_receipt["reason"]


def _require_unclaimed_failure_receipt(
    wave: Any,
    receipt: object,
    *,
    require_origin_state: bool,
) -> dict[str, Any]:
    """Validate an immutable receipt for a wave with no worker claim."""

    receipt_map, evidence_map = _validate_unclaimed_receipt_envelope(
        wave, receipt, require_origin_state=require_origin_state
    )
    reason = receipt_map["reason"]
    if reason == "kubernetes_post_absent":
        _validate_kubernetes_post_absence(wave, receipt_map, evidence_map)
    elif reason == "redis_release_absent":
        _validate_redis_release_absence(wave, receipt_map, evidence_map)
    else:
        _validate_preclaim_failure(wave, receipt_map, evidence_map)
    return receipt_map


def _validate_unclaimed_receipt_envelope(
    wave: Any,
    receipt: object,
    *,
    require_origin_state: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    receipt_map = _require_mapping(receipt, "unclaimed failure receipt")
    expected_fields = {
        "schema_version", "wave_id", "wave_digest", "origin_state", "reason",
        "operation", "operation_ticket", "evidence", "evidence_digest",
        "unclaimed_ordinals_digest",
    }
    if (
        set(receipt_map) != expected_fields
        or receipt_map["schema_version"] != FAILURE_SCHEMA
    ):
        raise PTGWaveFailureConflict(
            "unclaimed failure receipt fields are not exact"
        )
    if (
        receipt_map["wave_id"] != wave.wave_id
        or receipt_map["wave_digest"] != wave.wave_digest
        or receipt_map["reason"] not in FAILURE_REASONS
        or receipt_map["unclaimed_ordinals_digest"]
        != _unclaimed_ordinals_digest(wave)
    ):
        raise PTGWaveFailureConflict(
            "unclaimed failure receipt does not bind the exact wave"
        )
    if require_origin_state and receipt_map["origin_state"] != wave.state:
        raise PTGWaveFailureConflict(
            "unclaimed failure receipt origin state has changed"
        )
    evidence_map = _require_mapping(
        receipt_map["evidence"], "unclaimed failure evidence"
    )
    _digest(receipt_map["evidence_digest"], "unclaimed failure evidence digest")
    if receipt_map["evidence_digest"] != sha256_digest(
        canonical_json(evidence_map)
    ):
        raise PTGWaveFailureConflict(
            "unclaimed failure evidence digest is invalid"
        )
    return receipt_map, evidence_map


def _validate_kubernetes_post_absence(
    wave: Any,
    receipt: dict[str, Any],
    evidence: dict[str, Any],
) -> None:
    expected_evidence_map = {
        "wave_digest": wave.wave_digest,
        "manifest_identity": wave.kubernetes_manifest_identity,
        "job_name": (
            wave.kubernetes_manifest.get("metadata", {}).get("name")
            if isinstance(wave.kubernetes_manifest, dict)
            else None
        ),
        "job_absent": True,
        "pod_count": 0,
        "pods_absent": True,
    }
    if (
        receipt["origin_state"] != "slots_waiting"
        or receipt["operation"] != "kubernetes_post"
        or receipt["operation_ticket"] != wave.k8s_post_ticket
        or evidence != expected_evidence_map
        or wave.kubernetes_job_uid is not None
        or wave.kubernetes_job_receipt_digest is not None
    ):
        raise PTGWaveFailureConflict(
            "Kubernetes POST absence evidence is not exact"
        )


def _validate_redis_release_absence(
    wave: Any,
    receipt: dict[str, Any],
    evidence: dict[str, Any],
) -> None:
    if (
        receipt["origin_state"] != "redis_releasing"
        or receipt["operation"] != "redis_release"
        or receipt["operation_ticket"] != wave.redis_release_ticket
        or wave.redis_release_attestation_digest is not None
    ):
        raise PTGWaveFailureConflict(
            "Redis release absence evidence is not exact"
        )
    _verify_failure_redis(
        wave, receipt, evidence, require_release_absent=True
    )


def _validate_preclaim_failure(
    wave: Any,
    receipt: dict[str, Any],
    evidence: dict[str, Any],
) -> None:
    if (
        receipt["origin_state"] not in {"slots_waiting", "released", "executing"}
        or receipt["operation"] != "worker_start"
        or receipt["operation_ticket"] is not None
    ):
        raise PTGWaveFailureConflict(
            "pre-claim Job failure evidence is not exact"
        )
    _verify_preclaim_kubernetes_failure(wave, evidence)


def _require_claimed_prestart_failure_receipt(
    wave: Any,
    receipt: object,
    *,
    require_origin_state: bool,
) -> dict[str, Any]:
    """Validate the immutable receipt for a claimed-before-start crash."""

    receipt_map = _validate_claimed_receipt_envelope(
        wave, receipt, require_origin_state=require_origin_state
    )
    claimed_ordinals = _validate_claimed_ordinals(wave, receipt_map)
    if (claimed_ordinals and receipt_map["origin_state"] != "executing") or (
        not claimed_ordinals and receipt_map["origin_state"] != "released"
    ):
        raise PTGWaveFailureConflict(
            "claimed-prestart failure receipt conflicts with wave execution state"
        )
    kubernetes_evidence, redis_evidence = _validated_claimed_evidence(
        receipt_map
    )
    _verify_preclaim_kubernetes_failure(wave, kubernetes_evidence)
    _verify_failure_redis(wave, receipt_map, redis_evidence)
    return receipt_map


def _validate_claimed_receipt_envelope(
    wave: Any,
    receipt: object,
    *,
    require_origin_state: bool,
) -> dict[str, Any]:
    receipt_map = _require_mapping(receipt, "claimed-prestart failure receipt")
    expected_fields = {
        "schema_version", "wave_id", "wave_digest", "origin_state", "reason",
        "operation", "operation_ticket", "claimed_ordinals",
        "claimed_ordinals_digest", "kubernetes_evidence",
        "kubernetes_evidence_digest", "redis_evidence",
        "redis_evidence_digest",
    }
    if (
        set(receipt_map) != expected_fields
        or receipt_map["schema_version"] != CLAIMED_PRESTART_FAILURE_SCHEMA
        or receipt_map["wave_id"] != wave.wave_id
        or receipt_map["wave_digest"] != wave.wave_digest
        or receipt_map["reason"] != CLAIMED_PRESTART_FAILURE_REASON
        or receipt_map["origin_state"] not in {"released", "executing"}
        or receipt_map["operation"] != "worker_start"
        or receipt_map["operation_ticket"] is not None
    ):
        raise PTGWaveFailureConflict(
            "claimed-prestart failure receipt does not bind the exact wave"
        )
    if require_origin_state and receipt_map["origin_state"] != wave.state:
        raise PTGWaveFailureConflict(
            "claimed-prestart failure receipt origin state has changed"
        )
    return receipt_map


def _validate_claimed_ordinals(
    wave: Any, receipt: dict[str, Any]
) -> list[int]:
    claimed_ordinals = receipt["claimed_ordinals"]
    if (
        not isinstance(claimed_ordinals, list)
        or any(
            not isinstance(ordinal, int) or isinstance(ordinal, bool)
            for ordinal in claimed_ordinals
        )
        or claimed_ordinals != sorted(set(claimed_ordinals))
        or any(ordinal not in range(wave.intent_count) for ordinal in claimed_ordinals)
        or receipt["claimed_ordinals_digest"]
        != _claimed_ordinals_digest(wave, claimed_ordinals)
    ):
        raise PTGWaveFailureConflict(
            "claimed-prestart failure receipt has invalid claimed ordinals"
        )
    return claimed_ordinals


def _validated_claimed_evidence(
    receipt: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    kubernetes_evidence = _require_mapping(
        receipt["kubernetes_evidence"],
        "claimed-prestart Kubernetes evidence",
    )
    redis_evidence = _require_mapping(
        receipt["redis_evidence"], "claimed-prestart Redis evidence"
    )
    _digest(
        receipt["kubernetes_evidence_digest"],
        "claimed-prestart Kubernetes evidence digest",
    )
    _digest(
        receipt["redis_evidence_digest"],
        "claimed-prestart Redis evidence digest",
    )
    if (
        receipt["kubernetes_evidence_digest"]
        != sha256_digest(canonical_json(kubernetes_evidence))
        or receipt["redis_evidence_digest"]
        != sha256_digest(canonical_json(redis_evidence))
    ):
        raise PTGWaveFailureConflict(
            "claimed-prestart failure evidence digest is invalid"
        )
    return kubernetes_evidence, redis_evidence


def _require_failure_receipt(
    wave: Any,
    receipt: object,
    *,
    require_origin_state: bool,
) -> dict[str, Any]:
    if is_claimed_prestart_failure_receipt(receipt):
        return _require_claimed_prestart_failure_receipt(
            wave, receipt, require_origin_state=require_origin_state
        )
    return _require_unclaimed_failure_receipt(
        wave, receipt, require_origin_state=require_origin_state
    )


def _claimed_prestart_failure_receipt(
    wave: Any,
    *,
    claimed_ordinals: Iterable[int],
    kubernetes_evidence: object,
    redis_evidence: object,
) -> dict[str, Any]:
    claimed_ordinal_list = list(claimed_ordinals)
    failure_receipt_map = {
        "schema_version": CLAIMED_PRESTART_FAILURE_SCHEMA,
        "wave_id": wave.wave_id,
        "wave_digest": wave.wave_digest,
        "origin_state": wave.state,
        "reason": CLAIMED_PRESTART_FAILURE_REASON,
        "operation": "worker_start",
        "operation_ticket": None,
        "claimed_ordinals": claimed_ordinal_list,
        "claimed_ordinals_digest": _claimed_ordinals_digest(
            wave, claimed_ordinal_list
        ),
        "kubernetes_evidence": kubernetes_evidence,
        "kubernetes_evidence_digest": sha256_digest(
            canonical_json(kubernetes_evidence)
        ),
        "redis_evidence": redis_evidence,
        "redis_evidence_digest": sha256_digest(
            canonical_json(redis_evidence)
        ),
    }
    return _require_claimed_prestart_failure_receipt(
        wave, failure_receipt_map, require_origin_state=True
    )
