"""Synthetic exact-wave supersession proof fixtures for admission tests."""

from __future__ import annotations

from process.ptg_wave_state import canonical_json, sha256_digest


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


__all__ = ["supersession_proof"]
