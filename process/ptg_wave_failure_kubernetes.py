"""Pure Kubernetes validation for exact-wave failure evidence."""

from __future__ import annotations

from typing import Any

from process.ptg_wave_failure_types import (
    PTGWaveFailureConflict,
    _require_mapping,
    is_claimed_prestart_failure_receipt,
)
from process.ptg_wave_state import canonical_json, sha256_digest


def _verify_failure_kubernetes(
    wave: Any, failure: dict[str, Any], receipt: object
) -> dict[str, Any]:
    receipt_map = _require_mapping(
        receipt, "failure Kubernetes terminal receipt"
    )
    if is_claimed_prestart_failure_receipt(failure):
        if receipt_map != failure["kubernetes_evidence"]:
            raise PTGWaveFailureConflict(
                "claimed-prestart Kubernetes receipt differs from its first attestation"
            )
        _verify_preclaim_kubernetes_failure(wave, receipt_map)
        return receipt_map
    if failure["reason"] == "kubernetes_post_absent":
        if receipt_map != dict(failure["evidence"]):
            raise PTGWaveFailureConflict(
                "failure Kubernetes absence does not match its GET receipt"
            )
        return receipt_map
    if failure["reason"] == "pre_claim_failure":
        if receipt_map != failure["evidence"]:
            raise PTGWaveFailureConflict(
                "failure Kubernetes receipt differs from its first attestation"
            )
        _verify_preclaim_kubernetes_failure(wave, receipt_map)
        return receipt_map
    if receipt_map != wave.kubernetes_delete_evidence:
        raise PTGWaveFailureConflict(
            "failure Kubernetes absence differs from its persisted receipt"
        )
    _verify_kubernetes_absence(wave, receipt_map)
    return receipt_map


def _verify_preclaim_kubernetes_failure(
    wave: Any, evidence: object
) -> dict[str, Any]:
    evidence_map = _require_mapping(evidence, "pre-claim Kubernetes failure")
    expected_slots = [
        {
            "slot": slot["slot"],
            "pod_uid": slot["pod_uid"],
            "phase": "Failed",
            "runtime_image_identity": wave.runtime_image_identity,
        }
        for slot in (wave.kubernetes_ready_attestation or {}).get("slots", [])
    ]
    expected_evidence_map = _expected_preclaim_kubernetes_evidence(
        wave, expected_slots
    )
    unsigned_evidence_map = {
        name: field_value
        for name, field_value in evidence_map.items()
        if name != "attestation_digest"
    }
    if (
        set(evidence_map) != set(expected_evidence_map) | {"attestation_digest"}
        or unsigned_evidence_map != expected_evidence_map
        or len(expected_slots) != 12
        or evidence_map["attestation_digest"]
        != sha256_digest(canonical_json(unsigned_evidence_map))
    ):
        raise PTGWaveFailureConflict(
            "pre-claim Job failure evidence is not exact"
        )
    return evidence_map


def _expected_preclaim_kubernetes_evidence(
    wave: Any, expected_slots: list[dict[str, Any]]
) -> dict[str, Any]:
    metadata = (
        wave.kubernetes_manifest.get("metadata")
        if isinstance(wave.kubernetes_manifest, dict)
        else None
    )
    return {
        "schema_version": "healthporta.ptg-wave.kubernetes-preclaim-failure.v1",
        "wave_digest": wave.wave_digest,
        "queue": wave.release_queue,
        "manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest,
        "job_count": wave.intent_count,
        "config_identity": wave.kubernetes_config_identity,
        "manifest_identity": wave.kubernetes_manifest_identity,
        "image_identity": wave.pinned_image_reference,
        "runtime_image_identity": wave.runtime_image_identity,
        "job_name": (
            metadata.get("name") if isinstance(metadata, dict) else None
        ),
        "job_uid": wave.kubernetes_job_uid,
        "backoff_limit": 0,
        "job_active": 0,
        "job_failed": 12,
        "job_succeeded": 0,
        "job_failure_condition": {"type": "Failed", "status": "True"},
        "failed_slots": expected_slots,
    }


def _verify_kubernetes_absence(
    wave: Any, evidence: object
) -> dict[str, Any]:
    evidence_map = _require_mapping(evidence, "failure Kubernetes absence")
    expected_evidence_map = _expected_kubernetes_absence(wave)
    unsigned_evidence_map = {
        name: field_value
        for name, field_value in evidence_map.items()
        if name != "observation_digest"
    }
    if (
        set(evidence_map) != set(expected_evidence_map) | {"observation_digest"}
        or unsigned_evidence_map != expected_evidence_map
        or evidence_map["observation_digest"]
        != sha256_digest(canonical_json(unsigned_evidence_map))
    ):
        raise PTGWaveFailureConflict(
            "failure Kubernetes absence is not exact"
        )
    return evidence_map


def _expected_kubernetes_absence(wave: Any) -> dict[str, Any]:
    metadata = (
        wave.kubernetes_manifest.get("metadata")
        if isinstance(wave.kubernetes_manifest, dict)
        else None
    )
    return {
        "schema_version": "healthporta.ptg-wave.kubernetes-absence.v1",
        "operation_ticket": wave.kubernetes_delete_ticket,
        "wave_digest": wave.wave_digest,
        "job_name": (
            metadata.get("name") if isinstance(metadata, dict) else None
        ),
        "job_uid": wave.kubernetes_job_uid,
        "manifest_identity": wave.kubernetes_manifest_identity,
        "delete_permitted": wave.kubernetes_job_uid is not None,
        "job_absent": True,
        "pod_count": 0,
        "pods_absent": True,
    }
