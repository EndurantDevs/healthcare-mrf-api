"""Neutral V13 proof fixtures shared by migration enforcement tests."""

from __future__ import annotations

from pathlib import Path

from process._ptg_wave_redis_encoding import (
    encode_job_count,
    runtime_identity_digest,
)
from process.ptg_wave_receipt_contract import ordinary_cutover_id
from process.ptg_wave_state import canonical_json, sha256_digest
from process.ptg_wave_v13_post_ready_abandonment import (
    V13_ABANDONMENT_PROOF_SCHEMA,
    V13_QUARANTINE_REASON,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / (
    "202608170001_ptg_v13_post_ready_failure_guard.py"
)
JSON_NULL_GUARD_MIGRATION_PATH = ROOT / "alembic" / "versions" / (
    "202608200001_ptg_v13_json_null_guard.py"
)


async def add_v13_head_prerequisites(connection, quoted_schema: str) -> None:
    """Bring the focused V12 fixture to the healthcare V13 schema head."""

    await connection.execute(
        f"ALTER TABLE {quoted_schema}.ptg_import_wave "
        "ADD COLUMN kubernetes_manifest json, "
        "ADD COLUMN kubernetes_manifest_bytes bytea, "
        "ADD COLUMN kubernetes_manifest_sha256 text"
    )


def v13_proof(admission: dict, job_receipt_by_field: dict) -> dict:
    """Build neutral signed-proof input matching the closed SQL shape."""

    queue = "arq:PTGSmall:wave:" + admission["wave_digest"]
    count = admission["intent_count"]
    ready_slots = _ready_slots(admission, job_receipt_by_field, queue, count)
    failure_by_field = _failure_by_field(
        admission,
        job_receipt_by_field,
        queue,
        count,
    )
    redis_by_field = _redis_by_field(admission, queue, count, ready_slots)
    unsigned_proof_by_field = _unsigned_proof_by_field(
        admission,
        job_receipt_by_field,
        failure_by_field,
        redis_by_field,
        count,
    )
    return {
        **unsigned_proof_by_field,
        "proof_digest": sha256_digest(
            V13_ABANDONMENT_PROOF_SCHEMA.encode("ascii")
            + b"\0"
            + canonical_json(unsigned_proof_by_field)
        ),
    }


def _ready_slots(
    admission: dict,
    job_receipt_by_field: dict,
    queue: str,
    count: int,
) -> list[dict]:
    runtime_digest = runtime_identity_digest(
        job_receipt_by_field["config_identity"],
        job_receipt_by_field["manifest_identity"],
        job_receipt_by_field["pinned_image_reference"],
        job_receipt_by_field["runtime_image_identity"],
    )
    return [
        {
            "config_identity": job_receipt_by_field["config_identity"],
            "kubernetes_manifest_identity": job_receipt_by_field["manifest_identity"],
            "image_identity": job_receipt_by_field["pinned_image_reference"],
            "runtime_image_identity": job_receipt_by_field["runtime_image_identity"],
            "runtime_identity_digest": runtime_digest,
            "manifest_digest": admission["manifest_digest"],
            "pod_uid": f"retained-pod-{slot}",
            "queue_name": queue,
            "slot": slot,
            "wave_id": admission["wave_digest"],
            "jobs_digest": admission["jobs_digest"],
            "job_count": encode_job_count(count),
            "protocol_identity": "healthporta.ptg-small.exact-wave.v1",
            "serializer_identity": "arq-0.28.process-msgpack.v1",
        }
        for slot in range(12)
    ]


def _failure_by_field(
    admission: dict,
    job_receipt_by_field: dict,
    queue: str,
    count: int,
) -> dict:
    failure_by_field = {
        "schema_version": "healthporta.ptg-wave.kubernetes-retained-preclaim-failure.v1",
        "wave_digest": admission["wave_digest"],
        "queue": queue,
        "manifest_digest": admission["manifest_digest"],
        "jobs_digest": admission["jobs_digest"],
        "job_count": count,
        "config_identity": job_receipt_by_field["config_identity"],
        "manifest_identity": job_receipt_by_field["manifest_identity"],
        "image_identity": job_receipt_by_field["pinned_image_reference"],
        "runtime_image_identity": job_receipt_by_field["runtime_image_identity"],
        "job_name": "hpw-ptg-wave-" + admission["wave_digest"][:40],
        "job_uid": job_receipt_by_field["job_uid"],
        "backoff_limit": 0,
        "job_active": None,
        "job_failed": 12,
        "job_succeeded": None,
        "job_ready": 0,
        "job_terminating": 0,
        "completed_indexes": None,
        "failed_indexes": None,
        "completion_time": None,
        "start_time": "2026-08-17T00:06:01Z",
        "uncounted_terminated_pods": {},
        "job_conditions": _conditions(),
        "retained_failed_slots": _retained_slots(job_receipt_by_field),
    }
    failure_by_field["attestation_digest"] = sha256_digest(
        canonical_json(failure_by_field)
    )
    return failure_by_field


def _conditions() -> list[dict]:
    return [
        {
            "type": "Failed",
            "status": "True",
            "reason": "BackoffLimitExceeded",
            "message": "Job has reached the specified backoff limit",
            "last_probe_time": "2026-08-17T00:06:13Z",
            "last_transition_time": "2026-08-17T00:06:13Z",
        },
        {
            "type": "FailureTarget",
            "status": "True",
            "reason": "BackoffLimitExceeded",
            "message": "Job has reached the specified backoff limit",
            "last_probe_time": "2026-08-17T00:06:11Z",
            "last_transition_time": "2026-08-17T00:06:11Z",
        },
    ]


def _retained_slots(job_receipt_by_field: dict) -> list[dict]:
    return [
        {
            "slot": slot,
            "pod_uid": f"retained-pod-{slot}",
            "phase": "Failed",
            "runtime_image_identity": job_receipt_by_field["runtime_image_identity"],
            "termination": {
                "container_id": f"containerd://retained-{slot}",
                "reason": "Error",
                "exit_code": 1,
                "started_at": "2026-08-17T00:06:03Z",
                "finished_at": "2026-08-17T00:06:09Z",
            },
        }
        for slot in (0, 2)
    ]


def _redis_by_field(
    admission: dict,
    queue: str,
    count: int,
    ready_slots: list[dict],
) -> dict:
    redis_by_field = {
        "schema_version": "healthporta.ptg-wave.redis-unclaimed-failure.v1",
        "wave_id": admission["wave_digest"],
        "queue_name": queue,
        "manifest_digest": admission["manifest_digest"],
        "jobs_digest": admission["jobs_digest"],
        "job_count": count,
        "target_key_count": 4 + (4 * count),
        "ready_slots": ready_slots,
        "ready_slots_digest": sha256_digest(canonical_json(ready_slots)),
        "release_present": False,
        "release_digest": None,
        "release_receipt": None,
        "queued_ordinals": [],
        "job_ordinals": [],
        "result_ordinals": [],
        "retry_ordinals": [],
        "in_progress_ordinals": [],
        "health_check_present": False,
    }
    redis_by_field["attestation_digest"] = sha256_digest(canonical_json(redis_by_field))
    return redis_by_field


def _unsigned_proof_by_field(
    admission: dict,
    job_receipt_by_field: dict,
    failure_by_field: dict,
    redis_by_field: dict,
    count: int,
) -> dict:
    return {
        "schema_version": V13_ABANDONMENT_PROOF_SCHEMA,
        "recovery_basis": V13_QUARANTINE_REASON,
        "operation_id": admission["wave_id"],
        "cutover_id": ordinary_cutover_id(admission["wave_id"]),
        "admission": admission,
        "database": {
            "state": "slots_waiting",
            "intent_count": count,
            "run_count": count,
            "pristine_run_count": count,
            "unassigned_run_count": count,
            "claim_count": 0,
            "outcome_count": 0,
            "worker_start_event_count": 0,
            "member_rows_digest": "a" * 64,
            "intent_rows_digest": "b" * 64,
            "run_rows_digest": "c" * 64,
        },
        "kubernetes": {
            "job_receipt": job_receipt_by_field,
            "job_receipt_digest": sha256_digest(canonical_json(job_receipt_by_field)),
            "ready_attestation": None,
            "ready_attestation_digest": None,
            "failure": failure_by_field,
        },
        "redis": redis_by_field,
    }
