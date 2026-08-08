"""Canonical contract for one logical pre-claim supersession proof."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from api.ptg_wave_kubernetes import PTG_WAVE_SLOT_COUNT, _job_name
from process.ptg_wave_state import (
    PTGWaveStateConflict,
    canonical_json,
    sha256_digest,
)


SCHEMA_VERSION = "healthporta.ptg-wave.logical-preclaim-supersession.v1"
RECOVERY_BASIS = "logical_preclaim_failure"


class PTGWavePreclaimSupersessionConflict(PTGWaveStateConflict):
    """A predecessor cannot safely support logical pre-claim supersession."""


@dataclass(frozen=True)
class PTGWaveLogicalPreclaimSupersessionWitness:
    """Canonical proof that binds one failed predecessor to its successor."""

    predecessor_wave_id: str
    predecessor_wave_digest: str
    successor_wave_id: str
    manifest_digest: str
    jobs_digest: str
    intent_count: int
    job_name: str
    job_uid: str
    redis_attestation_digest: str
    proof_digest: str

    def evidence_mapping(self) -> dict[str, Any]:
        """Return stable unsigned proof data without volatile Pod identity."""

        return {
            "schema_version": SCHEMA_VERSION,
            "recovery_basis": RECOVERY_BASIS,
            "predecessor": {
                "wave_id": self.predecessor_wave_id,
                "wave_digest": self.predecessor_wave_digest,
                "manifest_digest": self.manifest_digest,
                "jobs_digest": self.jobs_digest,
                "intent_count": self.intent_count,
            },
            "successor_wave_id": self.successor_wave_id,
            "database": {
                "pristine_run_count": self.intent_count,
                "claim_count": 0,
                "outcome_count": 0,
                "worker_start_event_count": 0,
            },
            "kubernetes": {
                "job_name": self.job_name,
                "job_uid": self.job_uid,
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
            },
            "redis": {
                "unclaimed_attestation_digest": self.redis_attestation_digest,
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

    def as_mapping(self) -> dict[str, Any]:
        """Return canonical proof data together with its SHA-256 digest."""

        return {**self.evidence_mapping(), "proof_digest": self.proof_digest}


def validate_logical_preclaim_supersession_proof(
    proof: Any,
    *,
    predecessor_wave_id: str | None = None,
    successor_wave_id: str | None = None,
) -> dict[str, Any]:
    """Validate one exact canonical logical pre-claim proof mapping."""

    proof_map = _validate_proof_envelope(proof)
    predecessor_map, intent_count = _validate_wave_binding(
        proof_map,
        predecessor_wave_id=predecessor_wave_id,
        successor_wave_id=successor_wave_id,
    )
    _validate_database_proof(proof_map["database"], intent_count)
    _validate_kubernetes_proof(
        proof_map["kubernetes"],
        predecessor_map["wave_digest"],
    )
    _validate_redis_proof(proof_map["redis"])
    _verify_proof_digest(proof_map)
    return deepcopy(proof_map)


def _validate_proof_envelope(proof: Any) -> dict[str, Any]:
    top_fields = {
        "schema_version",
        "recovery_basis",
        "predecessor",
        "successor_wave_id",
        "database",
        "kubernetes",
        "redis",
        "proof_digest",
    }
    if not isinstance(proof, Mapping) or set(proof) != top_fields:
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim proof fields are not exact"
        )
    proof_map = dict(proof)
    if (
        _require_exact_text(
            proof_map["schema_version"],
            "logical pre-claim proof schema version",
        )
        != SCHEMA_VERSION
        or _require_exact_text(
            proof_map["recovery_basis"],
            "logical pre-claim proof basis",
        )
        != RECOVERY_BASIS
    ):
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim proof version or basis is unsupported"
        )
    return proof_map


def _validate_wave_binding(
    proof_map: dict[str, Any],
    *,
    predecessor_wave_id: str | None,
    successor_wave_id: str | None,
) -> tuple[dict[str, Any], int]:
    predecessor_map = _exact_mapping(
        proof_map["predecessor"],
        {
            "wave_id",
            "wave_digest",
            "manifest_digest",
            "jobs_digest",
            "intent_count",
        },
        "logical pre-claim predecessor",
    )
    predecessor_id = _validated_wave_id(
        predecessor_map["wave_id"],
        "predecessor wave ID",
    )
    successor_id = _validated_wave_id(
        proof_map["successor_wave_id"],
        "successor wave ID",
    )
    if predecessor_id == successor_id:
        raise PTGWavePreclaimSupersessionConflict(
            "successor wave ID must differ from the predecessor"
        )
    _require_expected_wave_id(
        predecessor_id,
        predecessor_wave_id,
        role="predecessor",
    )
    _require_expected_wave_id(
        successor_id,
        successor_wave_id,
        role="successor",
    )
    for name in ("wave_digest", "manifest_digest", "jobs_digest"):
        _require_digest(predecessor_map[name], f"predecessor {name}")
    intent_count = _require_exact_int(
        predecessor_map["intent_count"],
        "logical pre-claim proof intent count",
    )
    if not 1 <= intent_count <= 4096:
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim proof intent count is invalid"
        )
    return predecessor_map, intent_count


def _require_expected_wave_id(
    actual_wave_id: str,
    expected_wave_id: str | None,
    *,
    role: str,
) -> None:
    if expected_wave_id is None:
        return
    normalized_expected_id = _validated_wave_id(
        expected_wave_id,
        f"expected {role} wave ID",
    )
    if actual_wave_id != normalized_expected_id:
        raise PTGWavePreclaimSupersessionConflict(
            f"logical pre-claim proof identifies another {role}"
        )


def _validate_database_proof(database: Any, intent_count: int) -> None:
    database_map = _exact_mapping(
        database,
        {
            "pristine_run_count",
            "claim_count",
            "outcome_count",
            "worker_start_event_count",
        },
        "logical pre-claim database proof",
    )
    _require_exact_int(
        database_map["pristine_run_count"],
        "logical pre-claim database pristine run count",
    )
    for name in ("claim_count", "outcome_count", "worker_start_event_count"):
        _require_exact_int(
            database_map[name],
            f"logical pre-claim database {name}",
        )
    expected_database_map = {
        "pristine_run_count": intent_count,
        "claim_count": 0,
        "outcome_count": 0,
        "worker_start_event_count": 0,
    }
    if database_map != expected_database_map:
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim database proof is not empty and pristine"
        )


def _expected_kubernetes_proof(
    kubernetes_map: dict[str, Any],
    predecessor_wave_digest: str,
) -> dict[str, Any]:
    return {
        "job_name": _job_name(predecessor_wave_digest),
        "job_uid": kubernetes_map["job_uid"],
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


def _validate_kubernetes_proof(
    kubernetes: Any,
    predecessor_wave_digest: str,
) -> None:
    kubernetes_map = _exact_mapping(
        kubernetes,
        {
            "job_name",
            "job_uid",
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
        },
        "logical pre-claim Kubernetes proof",
    )
    _require_text(kubernetes_map["job_name"], "Kubernetes Job name")
    _require_text(kubernetes_map["job_uid"], "Kubernetes Job UID")
    _require_exact_text(
        kubernetes_map["completion_mode"],
        "Kubernetes completion mode",
    )
    for name in (
        "completions",
        "parallelism",
        "backoff_limit",
        "failed",
        "active",
        "succeeded",
        "ready",
        "terminating",
    ):
        _require_exact_int(kubernetes_map[name], f"Kubernetes {name}")
    for name in ("failed_condition", "complete_condition"):
        _is_exact_bool(kubernetes_map[name], f"Kubernetes {name}")
    expected_kubernetes_map = _expected_kubernetes_proof(
        kubernetes_map,
        predecessor_wave_digest,
    )
    if kubernetes_map != expected_kubernetes_map:
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim Kubernetes proof is not exact"
        )


def _validate_redis_proof(redis_proof: Any) -> None:
    redis_map = _exact_mapping(
        redis_proof,
        {
            "unclaimed_attestation_digest",
            "ready_slot_count",
            "release_present",
            "queued_ordinal_count",
            "job_ordinal_count",
            "result_ordinal_count",
            "retry_ordinal_count",
            "in_progress_ordinal_count",
            "health_check_present",
        },
        "logical pre-claim Redis proof",
    )
    _require_digest(
        redis_map["unclaimed_attestation_digest"],
        "Redis unclaimed attestation digest",
    )
    for name in (
        "ready_slot_count",
        "queued_ordinal_count",
        "job_ordinal_count",
        "result_ordinal_count",
        "retry_ordinal_count",
        "in_progress_ordinal_count",
    ):
        _require_exact_int(redis_map[name], f"Redis {name}")
    for name in ("release_present", "health_check_present"):
        _is_exact_bool(redis_map[name], f"Redis {name}")
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
    if redis_map != expected_redis_map:
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim Redis proof is not empty"
        )


def _verify_proof_digest(proof_map: dict[str, Any]) -> None:
    proof_digest = proof_map["proof_digest"]
    _require_digest(proof_digest, "logical pre-claim proof digest")
    unsigned_proof_map = {
        name: proof_map[name]
        for name in proof_map
        if name != "proof_digest"
    }
    if proof_digest != sha256_digest(canonical_json(unsigned_proof_map)):
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim proof digest is invalid"
        )


def _exact_mapping(
    candidate: Any,
    fields: set[str],
    name: str,
) -> dict[str, Any]:
    if not isinstance(candidate, Mapping) or set(candidate) != fields:
        raise PTGWavePreclaimSupersessionConflict(
            f"{name} fields are not exact"
        )
    return dict(candidate)


def _require_digest(candidate: Any, name: str) -> None:
    if (
        type(candidate) is not str
        or len(candidate) != 64
        or any(
            character not in "0123456789abcdef"
            for character in candidate
        )
    ):
        raise PTGWavePreclaimSupersessionConflict(
            f"{name} must be a lowercase SHA-256 digest"
        )


def _require_text(candidate: Any, name: str) -> None:
    if (
        type(candidate) is not str
        or not candidate
        or candidate != candidate.strip()
        or len(candidate) > 160
    ):
        raise PTGWavePreclaimSupersessionConflict(
            f"{name} must be exact non-empty text"
        )


def _require_wave_id(candidate: Any, name: str) -> None:
    if (
        type(candidate) is not str
        or not candidate
        or candidate != candidate.strip()
        or len(candidate) > 64
    ):
        raise PTGWavePreclaimSupersessionConflict(f"{name} is invalid")


def _validated_wave_id(candidate: Any, name: str) -> str:
    _require_wave_id(candidate, name)
    return candidate


def _require_exact_int(candidate: Any, name: str) -> int:
    if type(candidate) is not int:
        raise PTGWavePreclaimSupersessionConflict(f"{name} must be an integer")
    return candidate


def _is_exact_bool(candidate: Any, name: str) -> bool:
    if type(candidate) is not bool:
        raise PTGWavePreclaimSupersessionConflict(f"{name} must be a boolean")
    return candidate


def _require_exact_text(candidate: Any, name: str) -> str:
    if type(candidate) is not str:
        raise PTGWavePreclaimSupersessionConflict(f"{name} must be text")
    return candidate


__all__ = [
    "PTGWaveLogicalPreclaimSupersessionWitness",
    "PTGWavePreclaimSupersessionConflict",
    "RECOVERY_BASIS",
    "SCHEMA_VERSION",
    "validate_logical_preclaim_supersession_proof",
]
