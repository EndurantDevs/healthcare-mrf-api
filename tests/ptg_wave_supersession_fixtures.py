"""Synthetic exact-wave supersession proof fixtures for admission tests."""

from __future__ import annotations

from process.ptg_wave_state import canonical_json, sha256_digest
from process._ptg_wave_redis_encoding import (
    PTG_SMALL_WAVE_PROTOCOL_IDENTITY,
    wave_queue_name,
)
from process.ptg_wave_admission_rollback_supersession import (
    DATABASE_FIELDS,
    build_admission_rollback_supersession_proof,
)
from api.ptg_wave_kubernetes import _job_name


def supersession_proof(*, successor_wave_id: str, intent_count: int) -> dict:
    """Build a canonical successor-bound proof with no coordinate work."""

    unsigned_proof_map = {
        "schema_version": "healthporta.ptg-wave.logical-preclaim-supersession.v1",
        "recovery_basis": "logical_preclaim_failure",
        "predecessor": {
            "wave_id": "retired-wave-unit",
            "wave_digest": "1" * 64,
            "manifest_digest": "2" * 64,
            "jobs_digest": "3" * 64,
            "intent_count": intent_count,
        },
        "successor_wave_id": successor_wave_id,
        "database": {
            "pristine_run_count": intent_count,
            "claim_count": 0,
            "outcome_count": 0,
            "worker_start_event_count": 0,
        },
        "kubernetes": {
            "job_name": "hpw-ptg-wave-" + "1" * 40,
            "job_uid": "synthetic-job-uid",
            "completion_mode": "Indexed",
            "completions": 12,
            "parallelism": 12,
            "backoff_limit": 0,
            "failed": 12,
            "active": 0,
            "succeeded": 0,
            "ready": 0,
            "terminating": 0,
            "failed_condition": True,
            "complete_condition": False,
        },
        "redis": {
            "unclaimed_attestation_digest": "4" * 64,
            "ready_slot_count": 0,
            "release_present": False,
            "queued_ordinal_count": 0,
            "job_ordinal_count": 0,
            "result_ordinal_count": 0,
            "retry_ordinal_count": 0,
            "in_progress_ordinal_count": 0,
            "health_check_present": False,
        },
    }
    return {
        **unsigned_proof_map,
        "proof_digest": sha256_digest(canonical_json(unsigned_proof_map)),
    }


def admission_rollback_proof(
    *,
    successor_wave_id: str,
    intent_count: int,
) -> dict:
    """Build a canonical absent-admission predecessor proof."""

    request_digest = "5" * 64
    wave_digest = sha256_digest(
        (
            PTG_SMALL_WAVE_PROTOCOL_IDENTITY
            + "\0"
            + request_digest
        ).encode("utf-8")
    )
    predecessor_map = {
        "wave_id": "retired-request-unit",
        "idempotency_key": "retired-request-unit",
        "request_digest": request_digest,
        "wave_digest": wave_digest,
        "release_queue": wave_queue_name(wave_digest),
        "intent_count": intent_count,
    }
    return build_admission_rollback_supersession_proof(
        predecessor_map,
        successor_wave_id,
        database={name: 0 for name in DATABASE_FIELDS},
        kubernetes={
            "job_name": _job_name(wave_digest),
            "job_present": False,
            "pod_count": 0,
        },
        redis={
            "queue_name": predecessor_map["release_queue"],
            "queued_entry_count": 0,
            "ready_slot_count": 0,
            "release_present": False,
            "health_check_present": False,
        },
    )


def recovery_proofs(
    *,
    schema_version: str,
    successor_wave_id: str,
    intent_count: int,
) -> dict:
    """Build only the recovery proofs required by one attestation version."""

    recovery_proofs_map = {}
    if schema_version in {
        "healthporta.ptg-import-wave-attestation.v3",
        "healthporta.ptg-import-wave-attestation.v4",
    }:
        recovery_proofs_map["supersession"] = supersession_proof(
            successor_wave_id=successor_wave_id,
            intent_count=intent_count,
        )
    if schema_version == "healthporta.ptg-import-wave-attestation.v4":
        recovery_proofs_map[
            "admission_rollback_supersession"
        ] = admission_rollback_proof(
            successor_wave_id=successor_wave_id,
            intent_count=intent_count,
        )
    return recovery_proofs_map


__all__ = [
    "admission_rollback_proof",
    "recovery_proofs",
    "supersession_proof",
]
