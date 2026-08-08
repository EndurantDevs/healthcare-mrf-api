"""Stable projections for durable exact-wave admission records."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select

from api.control_import_wave_attestation import _identifier
from api.control_import_wave_materialized_preclaim import (
    is_materialized_preclaim_retired,
)
from db.models import PTGImportWave, db
from process.ptg_parts.ptg_wave_admission_fence import (
    PTG_WAVE_CAPACITY_OWNING_STATES,
    PTG_WAVE_TERMINAL_STATES,
)


def wave_response(
    wave: PTGImportWave,
    *,
    retired: bool = False,
) -> dict[str, Any]:
    """Project one durable wave without changing its persisted state."""

    return {
        "wave_id": wave.wave_id, "request_digest": wave.request_digest,
        "cohort_attestation_digest": wave.cohort_attestation_digest,
        "physical_coordinate_count": wave.physical_coordinate_count,
        "physical_coordinate_digest": wave.physical_coordinate_digest,
        "imported_coordinate_count": wave.imported_coordinate_count,
        "imported_coordinate_digest": wave.imported_coordinate_digest,
        "reused_coordinate_count": wave.reused_coordinate_count,
        "reused_coordinate_digest": wave.reused_coordinate_digest,
        "partition_digest": wave.partition_digest, "intent_count": wave.intent_count,
        "jobs_digest": wave.jobs_digest, "manifest_digest": wave.manifest_digest,
        "wave_digest": wave.wave_digest, "enqueue_time_ms": wave.enqueue_time_ms,
        "state": wave.state, "state_version": wave.state_version,
        "capacity_owning": (
            wave.state in PTG_WAVE_CAPACITY_OWNING_STATES and not retired
        ),
        "terminal": wave.state in PTG_WAVE_TERMINAL_STATES, "queue": wave.queue,
        "release_queue": wave.release_queue, "worker_class": wave.worker_class,
        "resource_class": wave.resource_class, "worker_limit": wave.worker_limit,
        "protocol_identity": wave.protocol_identity, "serializer_identity": wave.serializer_identity,
        "kubernetes_job_uid": wave.kubernetes_job_uid,
        "kubernetes_job_receipt_digest": wave.kubernetes_job_receipt_digest,
        "kubernetes_ready_attestation_digest": wave.kubernetes_ready_attestation_digest,
        "redis_release_attestation_digest": wave.redis_release_attestation_digest,
        "outcomes_digest": wave.outcomes_digest,
        "linkage_ack_digest": wave.linkage_ack_digest,
        "terminal_evidence_digest": wave.terminal_evidence_digest,
        "redis_cleanup_evidence_digest": wave.redis_cleanup_evidence_digest,
        "kubernetes_delete_evidence_digest": wave.kubernetes_delete_evidence_digest,
        "cleanup_evidence_digest": wave.cleanup_evidence_digest,
        "resolved_at": wave.resolved_at,
    }


async def get_import_wave(wave_id: str) -> dict[str, Any] | None:
    """Return one durable wave projection without mutating its state."""

    result = await db.execute(select(PTGImportWave).where(
        PTGImportWave.wave_id == _identifier(wave_id, "wave_id", 64)))
    wave = result.scalar_one_or_none()
    if wave is None:
        return None
    retired = await is_materialized_preclaim_retired(db, wave.wave_id)
    return wave_response(wave, retired=retired)


__all__ = ["get_import_wave", "wave_response"]
