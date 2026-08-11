"""Pure witness for a ticketed Job that failed before any worker claim."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
    build_materialized_preclaim_supersession_proof,
)
from process.ptg_wave_preclaim_supersession import (
    _attest_empty_unclaimed_redis,
    _attest_terminal_preclaim_job,
    _require_exact_intents_and_pristine_runs,
)
from process.ptg_wave_state import canonical_json, sha256_digest


@dataclass(frozen=True)
class PTGWaveMaterializedPreclaimObservation:
    """One database snapshot plus GET-only Kubernetes and Redis evidence."""

    predecessor_wave: Any
    intents: Sequence[Any]
    runs: Sequence[Any]
    claims: Sequence[Any]
    outcomes: Sequence[Any]
    worker_start_event_ordinals: Sequence[Any]
    logical_supersession: Any
    admission_rollback: Any
    actual_job: Mapping[str, Any]
    redis_unclaimed_attestation: Mapping[str, Any]


def attest_materialized_preclaim_supersession(
    observation: PTGWaveMaterializedPreclaimObservation,
    successor_wave_id: str,
) -> dict[str, Any]:
    """Prove a materialized predecessor failed with no admitted work."""

    wave = observation.predecessor_wave
    _require_boundary(wave)
    _require_sequences_and_zero_work(observation)
    _require_exact_intents_and_pristine_runs(
        wave, observation.intents, observation.runs
    )
    prior_recovery = _prior_recovery(observation)
    job_name, job_uid, redis_digest = _attest_external_boundary(
        observation
    )
    if job_uid != wave.kubernetes_job_uid:
        raise PTGWaveMaterializedPreclaimConflict(
            "terminal Job UID differs from the durable creation receipt"
        )
    predecessor = _predecessor_mapping(wave)
    return build_materialized_preclaim_supersession_proof(
        predecessor=predecessor,
        successor_wave_id=successor_wave_id,
        prior_recovery=prior_recovery,
        kubernetes=_kubernetes_proof_map(wave, job_name, job_uid),
        redis=_redis_proof_map(redis_digest),
    )


def _attest_external_boundary(
    observation: PTGWaveMaterializedPreclaimObservation,
) -> tuple[str, str, str]:
    wave = observation.predecessor_wave
    try:
        job_name, job_uid = _attest_terminal_preclaim_job(
            wave,
            observation.actual_job,
        )
        redis_digest = _attest_empty_unclaimed_redis(
            wave,
            observation.redis_unclaimed_attestation,
        )
    except PTGWaveMaterializedPreclaimConflict:
        raise
    except Exception as exc:
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized preclaim external evidence is invalid"
        ) from exc
    return job_name, job_uid, redis_digest


def _kubernetes_proof_map(
    wave: Any,
    job_name: str,
    job_uid: str,
) -> dict[str, Any]:
    return {
        "job_name": job_name,
        "job_uid": job_uid,
        "job_receipt_digest": wave.kubernetes_job_receipt_digest,
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
    }


def _redis_proof_map(redis_digest: str) -> dict[str, Any]:
    return {
        "unclaimed_attestation_digest": redis_digest,
        "ready_slot_count": 0,
        "release_present": False,
        "queued_ordinal_count": 0,
        "job_ordinal_count": 0,
        "result_ordinal_count": 0,
        "retry_ordinal_count": 0,
        "in_progress_ordinal_count": 0,
        "health_check_present": False,
    }


def _require_boundary(wave: Any) -> None:
    """Require the exact durable pre-release boundary for retirement."""

    _require_materialized_state(wave)
    _require_durable_job_receipt(wave)
    _require_no_post_materialization_progress(wave)


def _require_materialized_state(wave: Any) -> None:
    if (
        getattr(wave, "state", None) != "slots_waiting"
        or getattr(wave, "uncertainty_resume_state", None) is not None
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "predecessor must be the exact materialized slots_waiting wave"
        )


def _require_durable_job_receipt(wave: Any) -> None:
    required_values = (
        getattr(wave, "k8s_post_ticket", None),
        getattr(wave, "k8s_post_started_at", None),
        getattr(wave, "kubernetes_job_uid", None),
        getattr(wave, "kubernetes_job_receipt", None),
        getattr(wave, "kubernetes_job_receipt_digest", None),
    )
    if any(
        required_field_value is None
        for required_field_value in required_values
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "predecessor lacks its durable materialization receipt"
        )
    receipt = wave.kubernetes_job_receipt
    if not isinstance(receipt, Mapping):
        raise PTGWaveMaterializedPreclaimConflict(
            "predecessor Job receipt is invalid"
        )
    expected_receipt_map = {
        "wave_digest": wave.wave_digest,
        "job_uid": wave.kubernetes_job_uid,
        "manifest_identity": wave.kubernetes_manifest_identity,
        "config_identity": wave.kubernetes_config_identity,
        "pinned_image_reference": wave.pinned_image_reference,
        "pinned_image_digest": wave.pinned_image_digest,
        "runtime_image_identity": wave.runtime_image_identity,
    }
    if (
        dict(receipt) != expected_receipt_map
        or wave.kubernetes_job_receipt_digest
        != sha256_digest(canonical_json(expected_receipt_map))
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "predecessor Job receipt is corrupt"
        )


def _require_no_post_materialization_progress(wave: Any) -> None:
    absent_fields = (
        "kubernetes_ready_attestation",
        "kubernetes_ready_attestation_digest",
        "redis_release_ticket",
        "redis_release_started_at",
        "redis_release_attestation",
        "redis_release_attestation_digest",
        "failure_receipt",
        "failure_receipt_digest",
        "outcomes_digest",
        "linkage_ack",
        "linkage_ack_digest",
        "linkage_receipt",
        "linkage_receipt_payload_digest",
        "linkage_receipt_issued_at",
        "terminal_evidence_digest",
        "terminal_summary",
        "redis_cleanup_ticket",
        "redis_cleanup_started_at",
        "redis_cleanup_evidence",
        "redis_cleanup_evidence_digest",
        "kubernetes_delete_ticket",
        "kubernetes_delete_started_at",
        "kubernetes_delete_evidence",
        "kubernetes_delete_evidence_digest",
        "cleanup_evidence_digest",
        "cleanup_summary",
        "resolved_at",
    )
    if any(
        getattr(wave, field_name, None) is not None
        for field_name in absent_fields
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "predecessor contains post-materialization progress"
        )


def _require_sequences_and_zero_work(
    observation: PTGWaveMaterializedPreclaimObservation,
) -> None:
    for collection_name in (
        "intents",
        "runs",
        "claims",
        "outcomes",
        "worker_start_event_ordinals",
    ):
        collection_value = getattr(observation, collection_name)
        if not isinstance(collection_value, Sequence) or isinstance(
            collection_value, (str, bytes, bytearray)
        ):
            raise PTGWaveMaterializedPreclaimConflict(
                "materialized predecessor "
                f"{collection_name} must be a sequence"
            )
    if observation.claims:
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized predecessor must have zero claims"
        )
    if observation.outcomes:
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized predecessor must have zero outcomes"
        )
    if observation.worker_start_event_ordinals:
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized predecessor must have zero worker start events"
        )


def _prior_recovery(
    observation: PTGWaveMaterializedPreclaimObservation,
) -> dict[str, str]:
    wave = observation.predecessor_wave
    attestation = getattr(wave, "cohort_attestation", None)
    logical = observation.logical_supersession
    rollback = observation.admission_rollback
    if (
        not isinstance(attestation, Mapping)
        or attestation.get("schema_version")
        != "healthporta.ptg-import-wave-attestation.v4"
        or attestation.get("wave_id") != wave.wave_id
        or logical is None
        or rollback is None
        or logical.successor_wave_id != wave.wave_id
        or rollback.successor_wave_id != wave.wave_id
        or logical.recovery_basis != "logical_preclaim_failure"
        or rollback.recovery_basis != "admission_rollback_absent"
        or logical.recovery_evidence != attestation.get("supersession")
        or rollback.recovery_evidence
        != attestation.get("admission_rollback_supersession")
        or logical.recovery_evidence_sha256
        != logical.recovery_evidence.get("proof_digest")
        or rollback.recovery_evidence_sha256
        != rollback.recovery_evidence.get("proof_digest")
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized predecessor prior recovery is invalid"
        )
    return {
        "logical_preclaim_predecessor_wave_id": logical.predecessor_wave_id,
        "logical_preclaim_proof_digest": logical.recovery_evidence_sha256,
        "admission_rollback_predecessor_wave_id": (
            rollback.predecessor_wave_id
        ),
        "admission_rollback_proof_digest": (
            rollback.recovery_evidence_sha256
        ),
    }


def _predecessor_mapping(wave: Any) -> dict[str, Any]:
    return {
        "wave_id": wave.wave_id,
        "idempotency_key": wave.idempotency_key,
        "request_digest": wave.request_digest,
        "cohort_attestation_digest": wave.cohort_attestation_digest,
        "wave_digest": wave.wave_digest,
        "release_queue": wave.release_queue,
        "manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest,
        "intent_count": wave.intent_count,
        "worker_limit": wave.worker_limit,
        "kubernetes_manifest_identity": wave.kubernetes_manifest_identity,
        "kubernetes_config_identity": wave.kubernetes_config_identity,
        "pinned_image_reference": wave.pinned_image_reference,
        "pinned_image_digest": wave.pinned_image_digest,
        "runtime_image_identity": wave.runtime_image_identity,
        "kubernetes_job_uid": wave.kubernetes_job_uid,
        "kubernetes_job_receipt_digest": (
            wave.kubernetes_job_receipt_digest
        ),
    }


__all__ = [
    "PTGWaveMaterializedPreclaimObservation",
    "attest_materialized_preclaim_supersession",
]
