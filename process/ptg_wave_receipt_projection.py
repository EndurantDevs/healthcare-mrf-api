"""Pure GET projection for durable exact-wave controller receipts."""

from __future__ import annotations

from typing import Any

from process.ptg_parts.ptg_wave_admission_fence import PTG_WAVE_TERMINAL_STATES


def wave_receipt_mapping(
    wave: Any,
    *,
    retired: bool = False,
) -> dict[str, Any]:
    """Project one durable wave without causing reconciliation or I/O."""

    return {
        "wave_id": wave.wave_id, "wave_digest": wave.wave_digest, "state": wave.state,
        "state_version": wave.state_version,
        "capacity_owning": (
            wave.state not in PTG_WAVE_TERMINAL_STATES and not retired
        ),
        "intent_count": wave.intent_count,
        "physical_coordinate_count": wave.physical_coordinate_count,
        "imported_coordinate_count": wave.imported_coordinate_count,
        "reused_coordinate_count": wave.reused_coordinate_count,
        "physical_coordinate_digest": wave.physical_coordinate_digest,
        "imported_coordinate_digest": wave.imported_coordinate_digest,
        "reused_coordinate_digest": wave.reused_coordinate_digest,
        "partition_digest": wave.partition_digest,
        "jobs_digest": wave.jobs_digest,
        "manifest_digest": wave.manifest_digest,
        "worker_limit": wave.worker_limit,
        "worker_class": wave.worker_class,
        "resource_class": wave.resource_class,
        "release_queue": wave.release_queue,
        "k8s_post_started": wave.k8s_post_started_at is not None,
        "k8s_post_ticket": wave.k8s_post_ticket,
        "kubernetes_job_uid": wave.kubernetes_job_uid,
        "kubernetes_job_receipt": wave.kubernetes_job_receipt,
        "kubernetes_ready_attestation_digest": wave.kubernetes_ready_attestation_digest,
        "kubernetes_ready_attestation": wave.kubernetes_ready_attestation,
        "redis_release_started": wave.redis_release_started_at is not None,
        "redis_release_ticket": wave.redis_release_ticket,
        "redis_release_attestation_digest": wave.redis_release_attestation_digest,
        "redis_release_attestation": wave.redis_release_attestation,
        "outcomes_digest": wave.outcomes_digest,
        "failure_receipt_digest": wave.failure_receipt_digest,
        "failure_receipt": wave.failure_receipt,
        "linkage_ack_digest": wave.linkage_ack_digest,
        "linkage_ack": wave.linkage_ack,
        "terminal_evidence_digest": wave.terminal_evidence_digest,
        "terminal_summary": wave.terminal_summary,
        "redis_cleanup_ticket": wave.redis_cleanup_ticket,
        "redis_cleanup_evidence_digest": wave.redis_cleanup_evidence_digest,
        "redis_cleanup_evidence": wave.redis_cleanup_evidence,
        "kubernetes_delete_ticket": wave.kubernetes_delete_ticket,
        "kubernetes_delete_evidence_digest": wave.kubernetes_delete_evidence_digest,
        "kubernetes_delete_evidence": wave.kubernetes_delete_evidence,
        "cleanup_evidence_digest": wave.cleanup_evidence_digest,
        "cleanup_summary": wave.cleanup_summary,
        "resolved_at": (
            wave.resolved_at.isoformat() if wave.resolved_at is not None else None
        ),
    }
