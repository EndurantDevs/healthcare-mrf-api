"""Canonical proof for one materialized, all-unclaimed wave failure."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from api.ptg_wave_kubernetes import PTG_WAVE_SLOT_COUNT, _job_name
from process._ptg_wave_redis_encoding import (
    PTG_SMALL_WAVE_PROTOCOL_IDENTITY,
    wave_queue_name,
)
from process.ptg_wave_state import (
    PTGWaveStateConflict,
    canonical_json,
    sha256_digest,
)
from process.ptg_wave_quarantine_basis import (
    MATERIALIZED_PRECLAIM_FAILURE_BASIS,
)


SCHEMA_VERSION = (
    "healthporta.ptg-wave.materialized-preclaim-supersession.v1"
)
RECOVERY_BASIS = MATERIALIZED_PRECLAIM_FAILURE_BASIS


class PTGWaveMaterializedPreclaimConflict(PTGWaveStateConflict):
    """A materialized predecessor is not safe to supersede."""


def build_materialized_preclaim_supersession_proof(
    *,
    predecessor: Mapping[str, Any],
    successor_wave_id: str,
    prior_recovery: Mapping[str, Any],
    kubernetes: Mapping[str, Any],
    redis: Mapping[str, Any],
) -> dict[str, Any]:
    """Build and validate one canonical successor-bound proof."""

    intent_count = predecessor.get("intent_count")
    unsigned_proof_map = {
        "schema_version": SCHEMA_VERSION,
        "recovery_basis": RECOVERY_BASIS,
        "predecessor": dict(predecessor),
        "successor_wave_id": successor_wave_id,
        "prior_recovery": dict(prior_recovery),
        "database": {
            "state": "slots_waiting",
            "pristine_run_count": intent_count,
            "claim_count": 0,
            "outcome_count": 0,
            "worker_start_event_count": 0,
        },
        "kubernetes": deepcopy(dict(kubernetes)),
        "redis": deepcopy(dict(redis)),
    }
    return validate_materialized_preclaim_supersession_proof({
        **unsigned_proof_map,
        "proof_digest": sha256_digest(canonical_json(unsigned_proof_map)),
    })


def validate_materialized_preclaim_supersession_proof(
    proof: Any,
    *,
    predecessor_wave_id: str | None = None,
    successor_wave_id: str | None = None,
) -> dict[str, Any]:
    """Validate exact database, Kubernetes, Redis, and lineage evidence."""

    proof_map = _exact_mapping(
        proof,
        {
            "schema_version",
            "recovery_basis",
            "predecessor",
            "successor_wave_id",
            "prior_recovery",
            "database",
            "kubernetes",
            "redis",
            "proof_digest",
        },
        "materialized preclaim proof",
    )
    if (
        proof_map["schema_version"] != SCHEMA_VERSION
        or proof_map["recovery_basis"] != RECOVERY_BASIS
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized preclaim proof version or basis is unsupported"
        )
    predecessor_map = _validate_predecessor(
        proof_map["predecessor"], predecessor_wave_id
    )
    successor = _wave_id(proof_map["successor_wave_id"], "successor wave ID")
    if successor == predecessor_map["wave_id"]:
        raise PTGWaveMaterializedPreclaimConflict(
            "successor wave ID must differ from the predecessor"
        )
    if successor_wave_id is not None and successor != _wave_id(
        successor_wave_id, "expected successor wave ID"
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized preclaim proof identifies another successor"
        )
    _validate_prior_recovery(proof_map["prior_recovery"])
    _validate_database(proof_map["database"], predecessor_map["intent_count"])
    _validate_kubernetes(proof_map["kubernetes"], predecessor_map)
    _validate_redis(proof_map["redis"])
    _validate_proof_digest(proof_map)
    return deepcopy(proof_map)


def _validate_proof_digest(proof_map: Mapping[str, Any]) -> None:
    _digest(proof_map["proof_digest"], "materialized preclaim proof digest")
    unsigned_proof_map = {
        field_name: proof_field_value
        for field_name, proof_field_value in proof_map.items()
        if field_name != "proof_digest"
    }
    if proof_map["proof_digest"] != sha256_digest(
        canonical_json(unsigned_proof_map)
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized preclaim proof digest is invalid"
        )


def _validate_predecessor(
    predecessor_value: Any,
    expected_wave_id: str | None,
) -> dict[str, Any]:
    """Validate the exact durable predecessor identity and receipt."""

    expected_fields = {
        "wave_id",
        "idempotency_key",
        "request_digest",
        "cohort_attestation_digest",
        "wave_digest",
        "release_queue",
        "manifest_digest",
        "jobs_digest",
        "intent_count",
        "worker_limit",
        "kubernetes_manifest_identity",
        "kubernetes_config_identity",
        "pinned_image_reference",
        "pinned_image_digest",
        "runtime_image_identity",
        "kubernetes_job_uid",
        "kubernetes_job_receipt_digest",
    }
    predecessor_map = _exact_mapping(
        predecessor_value,
        expected_fields,
        "materialized predecessor",
    )
    _validate_predecessor_identity(predecessor_map, expected_wave_id)
    _validate_predecessor_digests(predecessor_map)
    _validate_predecessor_runtime(predecessor_map)
    return predecessor_map


def _validate_predecessor_identity(
    predecessor_map: Mapping[str, Any],
    expected_wave_id: str | None,
) -> None:
    wave_id = _wave_id(predecessor_map["wave_id"], "predecessor wave ID")
    if expected_wave_id is not None and wave_id != _wave_id(
        expected_wave_id, "expected predecessor wave ID"
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized preclaim proof identifies another predecessor"
        )
    if predecessor_map["idempotency_key"] != wave_id:
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized predecessor idempotency key is invalid"
        )


def _validate_predecessor_digests(
    predecessor_map: Mapping[str, Any],
) -> None:
    for field_name in (
        "request_digest",
        "cohort_attestation_digest",
        "wave_digest",
        "manifest_digest",
        "jobs_digest",
        "kubernetes_manifest_identity",
        "kubernetes_config_identity",
        "pinned_image_digest",
        "kubernetes_job_receipt_digest",
    ):
        _digest(
            predecessor_map[field_name],
            f"predecessor {field_name}",
        )
    expected_wave_digest = sha256_digest(
        (
            PTG_SMALL_WAVE_PROTOCOL_IDENTITY
            + "\0"
            + predecessor_map["request_digest"]
        ).encode("utf-8")
    )
    if (
        predecessor_map["wave_digest"] != expected_wave_digest
        or predecessor_map["release_queue"]
        != wave_queue_name(predecessor_map["wave_digest"])
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized predecessor engine identity is invalid"
        )


def _validate_predecessor_runtime(
    predecessor_map: Mapping[str, Any],
) -> None:
    intent_count = _exact_int(
        predecessor_map["intent_count"],
        "intent count",
    )
    if not 1 <= intent_count <= 4096:
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized predecessor intent count is invalid"
        )
    if _exact_int(predecessor_map["worker_limit"], "worker limit") != 12:
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized predecessor worker limit is invalid"
        )
    pinned_reference = _text(
        predecessor_map["pinned_image_reference"],
        "pinned image reference",
        512,
    )
    if not pinned_reference.endswith(
        "@sha256:" + predecessor_map["pinned_image_digest"]
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "pinned image reference and digest differ"
        )
    runtime = _text(
        predecessor_map["runtime_image_identity"],
        "runtime image identity",
        72,
    )
    if not runtime.startswith("sha256:"):
        raise PTGWaveMaterializedPreclaimConflict(
            "runtime image identity is invalid"
        )
    _digest(runtime.removeprefix("sha256:"), "runtime image identity")
    _text(
        predecessor_map["kubernetes_job_uid"],
        "Kubernetes Job UID",
        160,
    )


def _validate_prior_recovery(recovery_value: Any) -> None:
    recovery_map = _exact_mapping(
        recovery_value,
        {
            "logical_preclaim_predecessor_wave_id",
            "logical_preclaim_proof_digest",
            "admission_rollback_predecessor_wave_id",
            "admission_rollback_proof_digest",
        },
        "prior recovery",
    )
    _wave_id(
        recovery_map["logical_preclaim_predecessor_wave_id"],
        "logical preclaim predecessor wave ID",
    )
    _wave_id(
        recovery_map["admission_rollback_predecessor_wave_id"],
        "admission rollback predecessor wave ID",
    )
    _digest(
        recovery_map["logical_preclaim_proof_digest"],
        "logical proof digest",
    )
    _digest(
        recovery_map["admission_rollback_proof_digest"],
        "rollback proof digest",
    )


def _validate_database(database_value: Any, intent_count: int) -> None:
    database_map = _exact_mapping(
        database_value,
        {
            "state",
            "pristine_run_count",
            "claim_count",
            "outcome_count",
            "worker_start_event_count",
        },
        "materialized preclaim database proof",
    )
    expected_database_map = {
        "state": "slots_waiting",
        "pristine_run_count": intent_count,
        "claim_count": 0,
        "outcome_count": 0,
        "worker_start_event_count": 0,
    }
    for field_name in expected_database_map:
        if field_name != "state":
            _exact_int(
                database_map[field_name],
                f"database {field_name}",
            )
    if database_map != expected_database_map:
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized preclaim database proof is not pristine"
        )


def _validate_kubernetes(
    kubernetes_value: Any,
    predecessor_map: Mapping[str, Any],
) -> None:
    """Validate one exact durable Job-only terminal witness."""

    expected_fields = {
        "job_name",
        "job_uid",
        "job_receipt_digest",
        "completion_mode",
        "completions",
        "parallelism",
        "backoff_limit",
        "failed",
        "active",
        "succeeded",
        "ready",
        "terminating",
        "failed_condition",
        "complete_condition",
    }
    kubernetes_map = _exact_mapping(
        kubernetes_value,
        expected_fields,
        "materialized Kubernetes proof",
    )
    expected_kubernetes_map = _expected_kubernetes_map(predecessor_map)
    for field_name in (
        "completions",
        "parallelism",
        "backoff_limit",
        "failed",
        "active",
        "succeeded",
        "ready",
        "terminating",
    ):
        _exact_int(
            kubernetes_map[field_name],
            f"Kubernetes {field_name}",
        )
    for field_name in ("failed_condition", "complete_condition"):
        if type(kubernetes_map[field_name]) is not bool:
            raise PTGWaveMaterializedPreclaimConflict(
                f"Kubernetes {field_name} must be a boolean"
            )
    if kubernetes_map != expected_kubernetes_map:
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized Kubernetes proof does not bind the predecessor"
        )


def _expected_kubernetes_map(
    predecessor_map: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "job_name": _job_name(predecessor_map["wave_digest"]),
        "job_uid": predecessor_map["kubernetes_job_uid"],
        "job_receipt_digest": predecessor_map[
            "kubernetes_job_receipt_digest"
        ],
        "completion_mode": "Indexed",
        "completions": PTG_WAVE_SLOT_COUNT,
        "parallelism": PTG_WAVE_SLOT_COUNT,
        "backoff_limit": 0,
        "failed": PTG_WAVE_SLOT_COUNT,
        "active": 0,
        "succeeded": 0,
        "ready": 0,
        "terminating": 0,
        "failed_condition": True,
        "complete_condition": False,
    }


def _validate_redis(redis_value: Any) -> None:
    expected_fields = {
        "unclaimed_attestation_digest",
        "ready_slot_count",
        "release_present",
        "queued_ordinal_count",
        "job_ordinal_count",
        "result_ordinal_count",
        "retry_ordinal_count",
        "in_progress_ordinal_count",
        "health_check_present",
    }
    redis_map = _exact_mapping(
        redis_value,
        expected_fields,
        "materialized Redis proof",
    )
    expected_redis_map = {
        "unclaimed_attestation_digest": redis_map[
            "unclaimed_attestation_digest"
        ],
        "ready_slot_count": 0,
        "release_present": False,
        "queued_ordinal_count": 0,
        "job_ordinal_count": 0,
        "result_ordinal_count": 0,
        "retry_ordinal_count": 0,
        "in_progress_ordinal_count": 0,
        "health_check_present": False,
    }
    _digest(
        redis_map["unclaimed_attestation_digest"],
        "Redis unclaimed attestation digest",
    )
    for field_name in (
        "ready_slot_count",
        "queued_ordinal_count",
        "job_ordinal_count",
        "result_ordinal_count",
        "retry_ordinal_count",
        "in_progress_ordinal_count",
    ):
        _exact_int(redis_map[field_name], f"Redis {field_name}")
    for field_name in ("release_present", "health_check_present"):
        if type(redis_map[field_name]) is not bool:
            raise PTGWaveMaterializedPreclaimConflict(
                f"Redis {field_name} must be a boolean"
            )
    if redis_map != expected_redis_map:
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized Redis proof is not exact and empty"
        )


def _exact_mapping(value: Any, fields: set[str], name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise PTGWaveMaterializedPreclaimConflict(f"{name} fields are not exact")
    return dict(value)


def _wave_id(value: Any, name: str) -> str:
    return _text(value, name, 64)


def _text(value: Any, name: str, limit: int) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > limit
    ):
        raise PTGWaveMaterializedPreclaimConflict(f"{name} is invalid")
    return value


def _digest(value: Any, name: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            f"{name} must be a lowercase SHA-256 digest"
        )
    return value


def _exact_int(value: Any, name: str) -> int:
    if type(value) is not int:
        raise PTGWaveMaterializedPreclaimConflict(f"{name} must be an integer")
    return value


__all__ = [
    "PTGWaveMaterializedPreclaimConflict",
    "RECOVERY_BASIS",
    "SCHEMA_VERSION",
    "build_materialized_preclaim_supersession_proof",
    "validate_materialized_preclaim_supersession_proof",
]
