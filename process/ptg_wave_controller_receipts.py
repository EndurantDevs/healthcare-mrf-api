"""Pure receipt projections for the exact-wave controller."""

from __future__ import annotations

import uuid
from typing import Any, Iterable

from api.ptg_wave_kubernetes import (
    PTGWaveContractError,
    validate_ptg_wave_job_manifest,
)
from api.ptg_wave_kubernetes_attestation import (
    PTGWaveKubernetesAttestation,
    attest_existing_ptg_wave_job,
)
from db.models import PTGImportWave
from process._ptg_wave_redis_models import PTGSmallWaveReceipt
from process.ptg_wave_state import canonical_json, sha256_digest


def kubernetes_job_receipt(
    manifest: dict[str, Any], actual_job: dict[str, Any]
) -> dict[str, Any]:
    """Project one attested Kubernetes Job creation receipt."""

    attested = attest_existing_ptg_wave_job(manifest, actual_job)
    image_digest = attested.image_identity.rsplit("@sha256:", 1)[1]
    return {
        "wave_digest": attested.wave_digest,
        "job_uid": attested.job_uid,
        "manifest_identity": attested.manifest_identity,
        "config_identity": attested.config_identity,
        "pinned_image_reference": attested.image_identity,
        "pinned_image_digest": image_digest,
        "runtime_image_identity": attested.runtime_image_identity,
    }


def kubernetes_ready_receipt(
    manifest: dict[str, Any],
    attested: PTGWaveKubernetesAttestation,
) -> dict[str, Any]:
    """Project one exact 12-slot Kubernetes readiness receipt."""

    contract = validate_ptg_wave_job_manifest(manifest)
    return {
        "wave_digest": attested.wave_digest,
        "job_uid": attested.job_uid,
        "manifest_identity": attested.manifest_identity,
        "config_identity": attested.config_identity,
        "pinned_image_reference": contract.image,
        "pinned_image_digest": contract.image.rsplit("@sha256:", 1)[1],
        "runtime_image_identity": attested.runtime_image_identity,
        "slots": [
            {
                "slot": slot,
                "pod_uid": attested.pod_uid_by_slot[slot],
                "runtime_image_identity": attested.runtime_image_identity,
            }
            for slot in range(12)
        ],
    }


def assert_slot_membership(
    kubernetes: PTGWaveKubernetesAttestation,
    redis_slots: Iterable[Any],
) -> None:
    """Require Redis and Kubernetes to attest the identical 12 Pods."""

    redis_pod_map = {
        ready_slot.slot: ready_slot.pod_uid for ready_slot in redis_slots
    }
    if redis_pod_map != kubernetes.pod_uid_by_slot:
        raise PTGWaveContractError(
            "Redis ready slots differ from Kubernetes Pod membership"
        )


def redis_release_receipt(receipt: PTGSmallWaveReceipt) -> dict[str, Any]:
    """Project the immutable Redis release receipt."""

    return {
        "wave_digest": receipt.wave_id,
        "release_queue": receipt.queue_name,
        "redis_manifest_digest": receipt.manifest_digest,
        "jobs_digest": receipt.jobs_digest,
        "job_count": receipt.job_count,
        "protocol_identity": receipt.protocol_identity,
        "serializer_identity": receipt.serializer_identity,
        "manifest_identity": receipt.kubernetes_manifest_identity,
        "config_identity": receipt.config_identity,
        "pinned_image_reference": receipt.image_identity,
        "pinned_image_digest": receipt.image_identity.rsplit("@sha256:", 1)[1],
        "runtime_image_identity": receipt.runtime_image_identity,
        "runtime_identity_digest": receipt.runtime_identity_digest,
        "ready_slots": [slot.as_mapping() for slot in receipt.ready_slots],
        "ready_slots_digest": receipt.ready_slots_digest,
        "release_digest": receipt.release_digest,
    }


def initial_kubernetes_attestation(
    wave: PTGImportWave,
) -> PTGWaveKubernetesAttestation:
    """Restore the persisted initial Kubernetes identity."""

    contract = validate_ptg_wave_job_manifest(wave.kubernetes_manifest)
    ready_slots = wave.kubernetes_ready_attestation["slots"]
    job_name = wave.kubernetes_manifest["metadata"]["name"]
    return PTGWaveKubernetesAttestation(
        wave_digest=wave.wave_digest,
        queue=wave.release_queue,
        manifest_digest=wave.manifest_digest,
        jobs_digest=wave.jobs_digest,
        job_count=wave.intent_count,
        config_identity=wave.kubernetes_config_identity,
        manifest_identity=wave.kubernetes_manifest_identity,
        job_name=job_name,
        job_uid=wave.kubernetes_job_uid,
        runtime_image_identity=contract.runtime_image_identity,
        pod_uid_by_slot={
            ready_slot["slot"]: ready_slot["pod_uid"]
            for ready_slot in ready_slots
        },
    )


def kubernetes_terminal_receipt(
    wave: PTGImportWave, terminal: Any
) -> dict[str, Any]:
    """Project a terminal Kubernetes attestation."""

    del wave
    return terminal.as_mapping()


def redis_terminal_receipt(attestation: Any) -> dict[str, Any]:
    """Project a pre-cleanup Redis attestation."""

    return attestation.as_mapping()


def redis_post_cleanup_receipt(attestation: Any) -> dict[str, Any]:
    """Project a post-cleanup Redis attestation."""

    return attestation.as_mapping()


def redis_cleanup_receipt(
    wave: PTGImportWave,
    operation: dict[str, Any],
    cleanup_receipt: Any,
    post_cleanup: Any,
) -> dict[str, Any]:
    """Project executed or get-only Redis cleanup evidence."""

    terminal_summary = (
        wave.terminal_summary if isinstance(wave.terminal_summary, dict) else {}
    )
    operation_receipt = (
        cleanup_receipt.as_mapping() if cleanup_receipt is not None else None
    )
    return {
        "schema_version": "healthporta.ptg-wave.redis-cleanup.v1",
        "operation_ticket": (
            operation.get("operation_ticket") or wave.redis_cleanup_ticket
        ),
        "mode": (
            "executed" if operation.get("owner") else "get_only_reconciled"
        ),
        "pre_cleanup": terminal_summary.get("redis_pre_cleanup"),
        "operation_receipt": operation_receipt,
        "post_cleanup": redis_post_cleanup_receipt(post_cleanup),
    }


def kubernetes_absence_receipt(
    wave: PTGImportWave,
    observation: dict[str, Any],
    *,
    operation_ticket: str | None = None,
) -> dict[str, Any]:
    """Project a digest-bound Kubernetes Job and Pod absence receipt."""

    metadata = (
        wave.kubernetes_manifest.get("metadata")
        if isinstance(wave.kubernetes_manifest, dict)
        else None
    )
    unsigned_absence_evidence_map = {
        "schema_version": "healthporta.ptg-wave.kubernetes-absence.v1",
        "operation_ticket": operation_ticket or wave.kubernetes_delete_ticket,
        "wave_digest": wave.wave_digest,
        "job_name": (
            metadata.get("name") if isinstance(metadata, dict) else None
        ),
        "job_uid": wave.kubernetes_job_uid,
        "manifest_identity": wave.kubernetes_manifest_identity,
        "delete_permitted": wave.kubernetes_job_uid is not None,
        "job_absent": observation.get("job_absent"),
        "pod_count": observation.get("pod_count"),
        "pods_absent": observation.get("pods_absent"),
    }
    return {
        **unsigned_absence_evidence_map,
        "observation_digest": sha256_digest(
            canonical_json(unsigned_absence_evidence_map)
        ),
    }


def operation_ticket(operation: str) -> str:
    """Create one unique external-operation ticket."""

    return f"{operation}:{uuid.uuid4().hex}"


def unclaimed_failure_receipt(
    wave: PTGImportWave,
    *,
    origin_state: str,
    reason: str,
    operation: str,
    operation_ticket: str | None,
    evidence: dict[str, Any],
) -> dict[str, Any]:
    """Project the immutable all-unclaimed failure receipt."""

    evidence_digest = sha256_digest(canonical_json(evidence))
    unclaimed_ordinals_digest = sha256_digest(
        canonical_json(
            {
                "schema_version": 1,
                "wave_id": wave.wave_id,
                "ordinals": list(range(wave.intent_count)),
            }
        )
    )
    return {
        "schema_version": "healthporta.ptg-wave.unclaimed-failure.v1",
        "wave_id": wave.wave_id,
        "wave_digest": wave.wave_digest,
        "origin_state": origin_state,
        "reason": reason,
        "operation": operation,
        "operation_ticket": operation_ticket,
        "evidence": evidence,
        "evidence_digest": evidence_digest,
        "unclaimed_ordinals_digest": unclaimed_ordinals_digest,
    }
