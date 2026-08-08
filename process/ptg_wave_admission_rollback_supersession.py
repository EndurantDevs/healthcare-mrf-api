"""Canonical proof that an ambiguous admission left no engine wave.

The proof binds one fully derived predecessor identity to one fresh successor.
It is deliberately limited to state addressable from that predecessor
descriptor: engine rows, the deterministic Kubernetes wave identity, and the
four deterministic wave-scoped Redis keys.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from api.ptg_wave_kubernetes import _job_name
from process._ptg_wave_redis_encoding import (
    PTG_SMALL_WAVE_MAX_JOB_COUNT,
    PTG_SMALL_WAVE_PROTOCOL_IDENTITY,
    wave_queue_name,
)
from process.ptg_wave_state import (
    PTGWaveStateConflict,
    canonical_json,
    sha256_digest,
)


SCHEMA_VERSION = (
    "healthporta.ptg-wave.admission-rollback-supersession.v1"
)
RECOVERY_BASIS = "admission_rollback_absent"
DATABASE_FIELDS = frozenset({
    "wave_id_count",
    "idempotency_key_count",
    "request_digest_count",
    "wave_digest_count",
    "intent_count",
    "claim_count",
    "outcome_count",
    "wave_tagged_import_run_count",
    "wave_tagged_worker_start_event_count",
    "supersession_predecessor_count",
    "supersession_successor_count",
    "retirement_count",
})
PREDECESSOR_FIELDS = frozenset({
    "wave_id",
    "idempotency_key",
    "request_digest",
    "wave_digest",
    "release_queue",
    "intent_count",
})
PROOF_FIELDS = frozenset({
    "schema_version",
    "recovery_basis",
    "predecessor",
    "successor_wave_id",
    "database",
    "kubernetes",
    "redis",
    "proof_digest",
})


class PTGWaveAdmissionRollbackConflict(PTGWaveStateConflict):
    """An absent predecessor cannot be retired by this successor."""


def validate_admission_rollback_predecessor(
    predecessor: Any,
) -> dict[str, Any]:
    """Validate the deterministic identity of an unrecorded engine request."""

    predecessor_map = _exact_mapping(
        predecessor,
        set(PREDECESSOR_FIELDS),
        "admission rollback predecessor",
    )
    wave_id = _wave_id(predecessor_map["wave_id"], "predecessor wave ID")
    idempotency_key = _text(
        predecessor_map["idempotency_key"],
        "predecessor idempotency key",
        160,
    )
    if idempotency_key != wave_id:
        raise PTGWaveAdmissionRollbackConflict(
            "predecessor wave and idempotency identities differ"
        )
    request_digest = _digest(
        predecessor_map["request_digest"],
        "predecessor request digest",
    )
    wave_digest = _digest(
        predecessor_map["wave_digest"],
        "predecessor wave digest",
    )
    expected_wave_digest = _derived_wave_digest(request_digest)
    if wave_digest != expected_wave_digest:
        raise PTGWaveAdmissionRollbackConflict(
            "predecessor wave digest does not derive from its request"
        )
    release_queue = _text(
        predecessor_map["release_queue"],
        "predecessor release queue",
        160,
    )
    if release_queue != wave_queue_name(wave_digest):
        raise PTGWaveAdmissionRollbackConflict(
            "predecessor release queue does not derive from its wave digest"
        )
    intent_count = _exact_int(
        predecessor_map["intent_count"],
        "predecessor intent count",
    )
    if not 1 <= intent_count <= PTG_SMALL_WAVE_MAX_JOB_COUNT:
        raise PTGWaveAdmissionRollbackConflict(
            "predecessor intent count is outside the exact-wave boundary"
        )
    return {
        "wave_id": wave_id,
        "idempotency_key": idempotency_key,
        "request_digest": request_digest,
        "wave_digest": wave_digest,
        "release_queue": release_queue,
        "intent_count": intent_count,
    }


def validate_admission_rollback_successor(
    predecessor_wave_id: Any,
    successor_wave_id: Any,
) -> str:
    """Validate one fresh successor identity before any observation."""

    predecessor_id = _wave_id(
        predecessor_wave_id,
        "predecessor wave ID",
    )
    return _bound_successor(successor_wave_id, predecessor_id)


def build_admission_rollback_supersession_proof(
    predecessor: Any,
    successor_wave_id: str,
    *,
    database: Any,
    kubernetes: Any,
    redis: Any,
) -> dict[str, Any]:
    """Build and validate one successor-bound all-absence proof."""

    predecessor_map = validate_admission_rollback_predecessor(predecessor)
    successor_id = _bound_successor(
        successor_wave_id,
        predecessor_map["wave_id"],
    )
    database_map = _validate_database_absence(database)
    kubernetes_map = _validate_kubernetes_absence(
        kubernetes,
        predecessor_map["wave_digest"],
    )
    redis_map = _validate_redis_absence(
        redis,
        predecessor_map["release_queue"],
    )
    unsigned_proof_map = {
        "schema_version": SCHEMA_VERSION,
        "recovery_basis": RECOVERY_BASIS,
        "predecessor": predecessor_map,
        "successor_wave_id": successor_id,
        "database": database_map,
        "kubernetes": kubernetes_map,
        "redis": redis_map,
    }
    return {
        **unsigned_proof_map,
        "proof_digest": sha256_digest(canonical_json(unsigned_proof_map)),
    }


def validate_admission_rollback_supersession_proof(
    proof: Any,
    *,
    predecessor: Any | None = None,
    predecessor_wave_id: str | None = None,
    successor_wave_id: str | None = None,
) -> dict[str, Any]:
    """Validate one exact canonical admission-rollback proof."""

    proof_map = _exact_mapping(
        proof,
        set(PROOF_FIELDS),
        "admission rollback proof",
    )
    if (
        proof_map["schema_version"] != SCHEMA_VERSION
        or proof_map["recovery_basis"] != RECOVERY_BASIS
    ):
        raise PTGWaveAdmissionRollbackConflict(
            "admission rollback proof version or basis is unsupported"
        )
    actual_predecessor = _validate_proof_identities(
        proof_map,
        predecessor=predecessor,
        predecessor_wave_id=predecessor_wave_id,
        successor_wave_id=successor_wave_id,
    )
    _validate_database_absence(proof_map["database"])
    _validate_kubernetes_absence(
        proof_map["kubernetes"],
        actual_predecessor["wave_digest"],
    )
    _validate_redis_absence(
        proof_map["redis"],
        actual_predecessor["release_queue"],
    )
    proof_digest = _digest(
        proof_map["proof_digest"],
        "admission rollback proof digest",
    )
    unsigned_proof_map = {
        name: proof_field_value
        for name, proof_field_value in proof_map.items()
        if name != "proof_digest"
    }
    if proof_digest != sha256_digest(canonical_json(unsigned_proof_map)):
        raise PTGWaveAdmissionRollbackConflict(
            "admission rollback proof digest is invalid"
        )
    return deepcopy(proof_map)


def _validate_proof_identities(
    proof_map: Mapping[str, Any],
    *,
    predecessor: Any | None,
    predecessor_wave_id: str | None,
    successor_wave_id: str | None,
) -> dict[str, Any]:
    """Validate the optional predecessor and successor proof bindings."""

    actual_predecessor = validate_admission_rollback_predecessor(
        proof_map["predecessor"]
    )
    if predecessor is not None:
        expected_predecessor_map = validate_admission_rollback_predecessor(
            predecessor
        )
        if actual_predecessor != expected_predecessor_map:
            raise PTGWaveAdmissionRollbackConflict(
                "admission rollback proof identifies another predecessor"
            )
    if predecessor_wave_id is not None and actual_predecessor["wave_id"] != (
        _wave_id(predecessor_wave_id, "expected predecessor wave ID")
    ):
        raise PTGWaveAdmissionRollbackConflict(
            "admission rollback proof identifies another predecessor"
        )
    actual_successor = _bound_successor(
        proof_map["successor_wave_id"],
        actual_predecessor["wave_id"],
    )
    if successor_wave_id is not None and actual_successor != _wave_id(
        successor_wave_id,
        "expected successor wave ID",
    ):
        raise PTGWaveAdmissionRollbackConflict(
            "admission rollback proof identifies another successor"
        )
    return actual_predecessor


def _validate_database_absence(database: Any) -> dict[str, int]:
    database_map = _exact_mapping(
        database,
        set(DATABASE_FIELDS),
        "admission rollback database proof",
    )
    for name in DATABASE_FIELDS:
        if _exact_int(database_map[name], f"database {name}") != 0:
            raise PTGWaveAdmissionRollbackConflict(
                "admission rollback database proof is not empty"
            )
    return dict(database_map)


def _validate_kubernetes_absence(
    kubernetes: Any,
    wave_digest: str,
) -> dict[str, Any]:
    kubernetes_map = _exact_mapping(
        kubernetes,
        {"job_name", "job_present", "pod_count"},
        "admission rollback Kubernetes proof",
    )
    expected_map = {
        "job_name": _job_name(wave_digest),
        "job_present": False,
        "pod_count": 0,
    }
    _text(kubernetes_map["job_name"], "Kubernetes Job name", 253)
    if type(kubernetes_map["job_present"]) is not bool:
        raise PTGWaveAdmissionRollbackConflict(
            "Kubernetes job_present must be a boolean"
        )
    _exact_int(kubernetes_map["pod_count"], "Kubernetes Pod count")
    if kubernetes_map != expected_map:
        raise PTGWaveAdmissionRollbackConflict(
            "admission rollback Kubernetes proof is not empty"
        )
    return expected_map


def _validate_redis_absence(
    redis: Any,
    release_queue: str,
) -> dict[str, Any]:
    redis_map = _exact_mapping(
        redis,
        {
            "queue_name",
            "queued_entry_count",
            "ready_slot_count",
            "release_present",
            "health_check_present",
        },
        "admission rollback Redis proof",
    )
    expected_map = {
        "queue_name": release_queue,
        "queued_entry_count": 0,
        "ready_slot_count": 0,
        "release_present": False,
        "health_check_present": False,
    }
    for name in ("queued_entry_count", "ready_slot_count"):
        _exact_int(redis_map[name], f"Redis {name}")
    for name in ("release_present", "health_check_present"):
        if type(redis_map[name]) is not bool:
            raise PTGWaveAdmissionRollbackConflict(
                f"Redis {name} must be a boolean"
            )
    if redis_map != expected_map:
        raise PTGWaveAdmissionRollbackConflict(
            "admission rollback Redis proof is not empty"
        )
    return expected_map


def _bound_successor(successor: Any, predecessor: str) -> str:
    successor_id = _wave_id(successor, "successor wave ID")
    if successor_id == predecessor:
        raise PTGWaveAdmissionRollbackConflict(
            "successor wave ID must differ from the predecessor"
        )
    return successor_id


def _derived_wave_digest(request_digest: str) -> str:
    return sha256_digest(
        f"{PTG_SMALL_WAVE_PROTOCOL_IDENTITY}\0{request_digest}".encode("utf-8")
    )


def _exact_mapping(
    candidate: Any,
    fields: set[str],
    name: str,
) -> dict[str, Any]:
    if not isinstance(candidate, Mapping) or set(candidate) != fields:
        raise PTGWaveAdmissionRollbackConflict(
            f"{name} fields are not exact"
        )
    return dict(candidate)


def _text(candidate: Any, name: str, maximum: int) -> str:
    if (
        type(candidate) is not str
        or not candidate
        or candidate != candidate.strip()
        or len(candidate) > maximum
    ):
        raise PTGWaveAdmissionRollbackConflict(
            f"{name} must be exact bounded text"
        )
    return candidate


def _wave_id(candidate: Any, name: str) -> str:
    return _text(candidate, name, 64)


def _digest(candidate: Any, name: str) -> str:
    if (
        type(candidate) is not str
        or len(candidate) != 64
        or any(character not in "0123456789abcdef" for character in candidate)
    ):
        raise PTGWaveAdmissionRollbackConflict(
            f"{name} must be a lowercase SHA-256 digest"
        )
    return candidate


def _exact_int(candidate: Any, name: str) -> int:
    if type(candidate) is not int or candidate < 0:
        raise PTGWaveAdmissionRollbackConflict(
            f"{name} must be a non-negative integer"
        )
    return candidate


__all__ = [
    "DATABASE_FIELDS",
    "PTGWaveAdmissionRollbackConflict",
    "RECOVERY_BASIS",
    "SCHEMA_VERSION",
    "build_admission_rollback_supersession_proof",
    "validate_admission_rollback_predecessor",
    "validate_admission_rollback_successor",
    "validate_admission_rollback_supersession_proof",
]
