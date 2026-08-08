"""Pure witness for superseding an unclaimed pre-claim wave failure.

This module deliberately consumes only already-observed mappings and durable
row snapshots.  It neither reads nor writes PostgreSQL, Redis, or Kubernetes.
The witness is narrow: it proves a POST-ticketed predecessor could not have
released or started work, while its exact Indexed Job reached all-slot failure.
It does not infer, store, or depend on historical Pod membership.
"""

from __future__ import annotations

import json
from copy import deepcopy
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from api.ptg_wave_kubernetes import (
    PTGWaveContractError,
    PTG_WAVE_SLOT_COUNT,
    _job_name,
)
from api.ptg_wave_kubernetes_attestation import attest_existing_ptg_wave_job
from process.ptg_wave_failure_snapshots import _is_prestart_run_pristine
from process.ptg_wave_state import PTGWaveStateConflict, canonical_json, sha256_digest


_SCHEMA_VERSION = "healthporta.ptg-wave.logical-preclaim-supersession.v1"
_RECOVERY_BASIS = "logical_preclaim_failure"
_REDIS_ATTESTATION_FIELDS = frozenset(
    {
        "schema_version",
        "wave_id",
        "queue_name",
        "manifest_digest",
        "jobs_digest",
        "job_count",
        "target_key_count",
        "ready_slots",
        "ready_slots_digest",
        "release_present",
        "release_digest",
        "release_receipt",
        "queued_ordinals",
        "job_ordinals",
        "result_ordinals",
        "retry_ordinals",
        "in_progress_ordinals",
        "health_check_present",
        "attestation_digest",
    }
)


class PTGWavePreclaimSupersessionConflict(PTGWaveStateConflict):
    """A predecessor cannot safely support logical pre-claim supersession."""


def validate_logical_preclaim_supersession_proof(
    proof: Any,
    *,
    predecessor_wave_id: str | None = None,
    successor_wave_id: str | None = None,
) -> dict[str, Any]:
    """Validate one exact canonical logical pre-claim proof mapping."""

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
    if (
        _require_exact_text(proof["schema_version"], "logical pre-claim proof schema version")
        != _SCHEMA_VERSION
        or _require_exact_text(proof["recovery_basis"], "logical pre-claim proof basis")
        != _RECOVERY_BASIS
    ):
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim proof version or basis is unsupported"
        )
    predecessor = _exact_mapping(
        proof["predecessor"],
        {"wave_id", "wave_digest", "manifest_digest", "jobs_digest", "intent_count"},
        "logical pre-claim predecessor",
    )
    predecessor_id = _validated_wave_id(predecessor["wave_id"], "predecessor wave ID")
    successor_id = _validated_wave_id(proof["successor_wave_id"], "successor wave ID")
    if predecessor_id == successor_id:
        raise PTGWavePreclaimSupersessionConflict(
            "successor wave ID must differ from the predecessor"
        )
    if predecessor_wave_id is not None:
        expected_predecessor_id = _validated_wave_id(
            predecessor_wave_id, "expected predecessor wave ID"
        )
        if predecessor_id != expected_predecessor_id:
            raise PTGWavePreclaimSupersessionConflict(
                "logical pre-claim proof identifies another predecessor"
            )
    if successor_wave_id is not None:
        expected_successor_id = _validated_wave_id(
            successor_wave_id, "expected successor wave ID"
        )
        if successor_id != expected_successor_id:
            raise PTGWavePreclaimSupersessionConflict(
                "logical pre-claim proof identifies another successor"
            )
    for name in ("wave_digest", "manifest_digest", "jobs_digest"):
        _require_digest(predecessor[name], f"predecessor {name}")
    intent_count = _require_exact_int(
        predecessor["intent_count"], "logical pre-claim proof intent count"
    )
    if not 1 <= intent_count <= 4096:
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim proof intent count is invalid"
        )
    database = _exact_mapping(
        proof["database"],
        {"pristine_run_count", "claim_count", "outcome_count", "worker_start_event_count"},
        "logical pre-claim database proof",
    )
    _require_exact_int(
        database["pristine_run_count"], "logical pre-claim database pristine run count"
    )
    for name in ("claim_count", "outcome_count", "worker_start_event_count"):
        _require_exact_int(database[name], f"logical pre-claim database {name}")
    if database != {
        "pristine_run_count": intent_count,
        "claim_count": 0,
        "outcome_count": 0,
        "worker_start_event_count": 0,
    }:
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim database proof is not empty and pristine"
        )
    kubernetes = _exact_mapping(
        proof["kubernetes"],
        {
            "job_name", "job_uid", "completion_mode", "completions",
            "parallelism", "backoff_limit", "failed", "active", "succeeded",
            "ready", "terminating", "failed_condition", "complete_condition",
        },
        "logical pre-claim Kubernetes proof",
    )
    _require_text(kubernetes["job_name"], "Kubernetes Job name")
    _require_text(kubernetes["job_uid"], "Kubernetes Job UID")
    _require_exact_text(kubernetes["completion_mode"], "Kubernetes completion mode")
    for name in (
        "completions", "parallelism", "backoff_limit", "failed", "active",
        "succeeded", "ready", "terminating",
    ):
        _require_exact_int(kubernetes[name], f"Kubernetes {name}")
    for name in ("failed_condition", "complete_condition"):
        _require_exact_bool(kubernetes[name], f"Kubernetes {name}")
    expected_kubernetes = {
        "job_name": _job_name(predecessor["wave_digest"]),
        "job_uid": kubernetes["job_uid"],
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
    if kubernetes != expected_kubernetes:
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim Kubernetes proof is not exact"
        )
    redis = _exact_mapping(
        proof["redis"],
        {
            "unclaimed_attestation_digest", "ready_slot_count", "release_present",
            "queued_ordinal_count", "job_ordinal_count", "result_ordinal_count",
            "retry_ordinal_count", "in_progress_ordinal_count", "health_check_present",
        },
        "logical pre-claim Redis proof",
    )
    _require_digest(
        redis["unclaimed_attestation_digest"],
        "Redis unclaimed attestation digest",
    )
    for name in (
        "ready_slot_count", "queued_ordinal_count", "job_ordinal_count",
        "result_ordinal_count", "retry_ordinal_count", "in_progress_ordinal_count",
    ):
        _require_exact_int(redis[name], f"Redis {name}")
    for name in ("release_present", "health_check_present"):
        _require_exact_bool(redis[name], f"Redis {name}")
    if redis != {
        "unclaimed_attestation_digest": redis["unclaimed_attestation_digest"],
        "ready_slot_count": 0,
        "release_present": False,
        "queued_ordinal_count": 0,
        "job_ordinal_count": 0,
        "result_ordinal_count": 0,
        "retry_ordinal_count": 0,
        "in_progress_ordinal_count": 0,
        "health_check_present": False,
    }:
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim Redis proof is not empty"
        )
    proof_digest = proof["proof_digest"]
    _require_digest(proof_digest, "logical pre-claim proof digest")
    unsigned = {name: proof[name] for name in proof if name != "proof_digest"}
    if proof_digest != sha256_digest(canonical_json(unsigned)):
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim proof digest is invalid"
        )
    return deepcopy(dict(proof))


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
        """Return the stable, unsigned proof payload.

        This is intentionally a logical witness: it contains neither Pod
        identity nor volatile Kubernetes metadata such as timestamps,
        resourceVersions, or event observations.
        """

        return {
            "schema_version": _SCHEMA_VERSION,
            "recovery_basis": _RECOVERY_BASIS,
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


def attest_logical_preclaim_supersession(
    predecessor_wave: Any,
    intents: Sequence[Any],
    runs: Sequence[Any],
    claims: Sequence[Any],
    outcomes: Sequence[Any],
    worker_start_event_ordinals: Sequence[Any],
    actual_job: Mapping[str, Any],
    redis_unclaimed_attestation: Mapping[str, Any],
    successor_wave_id: str,
) -> PTGWaveLogicalPreclaimSupersessionWitness:
    """Return a witness only for an exact, fully unclaimed predecessor.

    A caller supplies a single atomic snapshot of predecessor rows and the
    external observations.  Every mismatch blocks supersession.  This helper
    does not interpret an all-failed Job as evidence of any particular Pods.
    """

    _require_sequence(intents, "intents")
    _require_sequence(runs, "runs")
    _require_sequence(claims, "claims")
    _require_sequence(outcomes, "outcomes")
    _require_sequence(worker_start_event_ordinals, "worker start event ordinals")
    _require_predecessor_preclaim_boundary(predecessor_wave)
    _require_wave_id(successor_wave_id, "successor wave ID")
    predecessor_wave_id = _text_attr(predecessor_wave, "wave_id")
    if successor_wave_id == predecessor_wave_id:
        raise PTGWavePreclaimSupersessionConflict(
            "successor wave ID must differ from the predecessor"
        )
    _require_exact_intents_and_pristine_runs(predecessor_wave, intents, runs)
    if claims:
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim supersession requires no claims"
        )
    if outcomes:
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim supersession requires no outcomes"
        )
    if worker_start_event_ordinals:
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim supersession requires no worker start events"
        )
    job_name, job_uid = _attest_terminal_preclaim_job(predecessor_wave, actual_job)
    redis_digest = _attest_empty_unclaimed_redis(
        predecessor_wave, redis_unclaimed_attestation
    )
    values = {
        "predecessor_wave_id": predecessor_wave_id,
        "predecessor_wave_digest": _text_attr(predecessor_wave, "wave_digest"),
        "successor_wave_id": successor_wave_id,
        "manifest_digest": _text_attr(predecessor_wave, "manifest_digest"),
        "jobs_digest": _text_attr(predecessor_wave, "jobs_digest"),
        "intent_count": _int_attr(predecessor_wave, "intent_count"),
        "job_name": job_name,
        "job_uid": job_uid,
        "redis_attestation_digest": redis_digest,
    }
    unsigned = PTGWaveLogicalPreclaimSupersessionWitness(
        **values, proof_digest=""
    ).evidence_mapping()
    return PTGWaveLogicalPreclaimSupersessionWitness(
        **values, proof_digest=sha256_digest(canonical_json(unsigned))
    )


def _require_predecessor_preclaim_boundary(wave: Any) -> None:
    if (
        getattr(wave, "state", None) != "uncertain"
        or getattr(wave, "uncertainty_resume_state", None) != "slots_waiting"
    ):
        raise PTGWavePreclaimSupersessionConflict(
            "predecessor must be uncertain with slots_waiting resume state"
        )
    _require_text(getattr(wave, "k8s_post_ticket", None), "predecessor POST ticket")
    if getattr(wave, "k8s_post_started_at", None) is None:
        raise PTGWavePreclaimSupersessionConflict(
            "predecessor POST ticket lacks its start receipt"
        )
    _require_text_attr(wave, "wave_id")
    _require_text_attr(wave, "wave_digest")
    _require_text_attr(wave, "manifest_digest")
    _require_text_attr(wave, "jobs_digest")
    if _int_attr(wave, "intent_count") < 1 or getattr(wave, "worker_limit", None) != PTG_WAVE_SLOT_COUNT:
        raise PTGWavePreclaimSupersessionConflict(
            "predecessor must retain a positive exact intent count and twelve workers"
        )
    _require_no_receipts(wave)


def _require_no_receipts(wave: Any) -> None:
    absent_fields = (
        "kubernetes_job_uid",
        "kubernetes_job_receipt",
        "kubernetes_job_receipt_digest",
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
    if any(getattr(wave, name, None) is not None for name in absent_fields):
        raise PTGWavePreclaimSupersessionConflict(
            "predecessor contains a receipt or lifecycle marker after POST ticketing"
        )


def _require_exact_intents_and_pristine_runs(
    wave: Any, intents: Sequence[Any], runs: Sequence[Any]
) -> None:
    intent_count = _int_attr(wave, "intent_count")
    if len(intents) != intent_count or len(runs) != intent_count:
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim supersession requires every admitted intent and run"
        )
    ordinal_intents = [(_ordinal_attr(item), item) for item in intents]
    ordered_intents = [
        item for _, item in sorted(ordinal_intents, key=lambda item: item[0])
    ]
    if [ordinal for ordinal, _ in sorted(ordinal_intents)] != list(range(intent_count)):
        raise PTGWavePreclaimSupersessionConflict(
            "predecessor intents are not complete contiguous ordinals"
        )
    intents_by_run_id: dict[str, Any] = {}
    for intent in ordered_intents:
        if getattr(intent, "wave_id", None) != wave.wave_id:
            raise PTGWavePreclaimSupersessionConflict(
                "predecessor intent belongs to another wave"
            )
        run_id = getattr(intent, "run_id", None)
        job_id = getattr(intent, "job_id", None)
        if not isinstance(run_id, str) or not run_id or not isinstance(job_id, str) or not job_id or run_id in intents_by_run_id:
            raise PTGWavePreclaimSupersessionConflict(
                "predecessor intent identities are not exact"
            )
        intents_by_run_id[run_id] = intent
    runs_by_id: dict[str, Any] = {}
    for run in runs:
        run_id = getattr(run, "run_id", None)
        if not isinstance(run_id, str) or run_id in runs_by_id:
            raise PTGWavePreclaimSupersessionConflict(
                "predecessor ImportRun identities are not exact"
            )
        runs_by_id[run_id] = run
    if set(runs_by_id) != set(intents_by_run_id):
        raise PTGWavePreclaimSupersessionConflict(
            "predecessor ImportRuns do not exactly match admitted intents"
        )
    if any(
        not _is_prestart_run_pristine(wave, intent, runs_by_id[intent.run_id])
        for intent in ordered_intents
    ):
        raise PTGWavePreclaimSupersessionConflict(
            "predecessor ImportRuns are not pristine queued admissions"
        )


def _attest_terminal_preclaim_job(
    wave: Any, actual_job: Mapping[str, Any]
) -> tuple[str, str]:
    manifest = getattr(wave, "kubernetes_manifest", None)
    if not isinstance(manifest, Mapping):
        raise PTGWavePreclaimSupersessionConflict(
            "predecessor lacks its exact desired Kubernetes manifest"
        )
    _require_persisted_manifest_integrity(wave, manifest)
    try:
        attested = attest_existing_ptg_wave_job(manifest, actual_job)
    except (PTGWaveContractError, AttributeError, TypeError) as exc:
        raise PTGWavePreclaimSupersessionConflict(
            "actual Job does not exactly attest the predecessor manifest"
        ) from exc
    expected = (
        ("wave_digest", attested.wave_digest),
        ("release_queue", attested.queue),
        ("manifest_digest", attested.manifest_digest),
        ("jobs_digest", attested.jobs_digest),
        ("intent_count", attested.job_count),
        ("kubernetes_config_identity", attested.config_identity),
        ("kubernetes_manifest_identity", attested.manifest_identity),
        ("pinned_image_reference", attested.image_identity),
        ("pinned_image_digest", attested.image_identity.rsplit("@sha256:", 1)[1]),
        ("runtime_image_identity", attested.runtime_image_identity),
    )
    if any(getattr(wave, name, None) != value for name, value in expected):
        raise PTGWavePreclaimSupersessionConflict(
            "actual Job manifest attestation does not bind durable predecessor identity"
        )
    _require_terminal_status(actual_job)
    _require_text(attested.job_name, "attested Kubernetes Job name")
    _require_text(attested.job_uid, "attested Kubernetes Job UID")
    return attested.job_name, attested.job_uid


def _require_persisted_manifest_integrity(
    wave: Any, manifest: Mapping[str, Any]
) -> None:
    manifest_bytes = getattr(wave, "kubernetes_manifest_bytes", None)
    manifest_digest = getattr(wave, "kubernetes_manifest_sha256", None)
    if not isinstance(manifest_bytes, bytes) or not isinstance(manifest_digest, str):
        raise PTGWavePreclaimSupersessionConflict(
            "predecessor lacks its persisted desired Kubernetes manifest bytes"
        )
    if sha256_digest(manifest_bytes) != manifest_digest:
        raise PTGWavePreclaimSupersessionConflict(
            "predecessor desired Kubernetes manifest bytes are corrupt"
        )
    try:
        decoded_manifest = json.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PTGWavePreclaimSupersessionConflict(
            "predecessor desired Kubernetes manifest bytes are not JSON"
        ) from exc
    if decoded_manifest != manifest:
        raise PTGWavePreclaimSupersessionConflict(
            "predecessor desired Kubernetes manifest bytes differ from its mapping"
        )


def _require_terminal_status(actual_job: Mapping[str, Any]) -> None:
    status = actual_job.get("status")
    if not isinstance(status, Mapping):
        raise PTGWavePreclaimSupersessionConflict("actual Job terminal status is missing")
    for name in ("active", "succeeded", "ready", "terminating"):
        value = status.get(name, 0)
        if type(value) is not int or value != 0:
            raise PTGWavePreclaimSupersessionConflict(
                f"actual Job {name} must be zero or absent"
            )
    failed = status.get("failed")
    if type(failed) is not int or failed != PTG_WAVE_SLOT_COUNT:
        raise PTGWavePreclaimSupersessionConflict(
            "actual Job failed count must equal twelve"
        )
    if status.get("completedIndexes") is not None:
        raise PTGWavePreclaimSupersessionConflict(
            "actual Job must not report completed indexes"
        )
    conditions = status.get("conditions")
    if not isinstance(conditions, list):
        raise PTGWavePreclaimSupersessionConflict("actual Job conditions are missing")
    condition_by_type: dict[str, str] = {}
    for condition in conditions:
        if not isinstance(condition, Mapping):
            raise PTGWavePreclaimSupersessionConflict("actual Job condition is invalid")
        condition_type = condition.get("type")
        condition_status = condition.get("status")
        if (
            type(condition_type) is not str
            or not condition_type
            or type(condition_status) is not str
            or condition_status not in {"True", "False", "Unknown"}
            or condition_type in condition_by_type
        ):
            raise PTGWavePreclaimSupersessionConflict("actual Job condition is invalid")
        condition_by_type[condition_type] = condition_status
    if condition_by_type.get("Failed") != "True":
        raise PTGWavePreclaimSupersessionConflict(
            "actual Job must have a true Failed condition"
        )
    if condition_by_type.get("Complete") == "True":
        raise PTGWavePreclaimSupersessionConflict(
            "actual Job must not have a true Complete condition"
        )


def _attest_empty_unclaimed_redis(wave: Any, receipt: Mapping[str, Any]) -> str:
    if not isinstance(receipt, Mapping) or set(receipt) != _REDIS_ATTESTATION_FIELDS:
        raise PTGWavePreclaimSupersessionConflict(
            "Redis unclaimed attestation fields are not exact"
        )
    intent_count = _int_attr(wave, "intent_count")
    _require_text_attr(wave, "wave_digest")
    _require_text_attr(wave, "release_queue")
    _require_text_attr(wave, "manifest_digest")
    _require_text_attr(wave, "jobs_digest")
    for name in ("job_count", "target_key_count"):
        _require_exact_int(receipt[name], f"Redis unclaimed attestation {name}")
    for name in ("release_present", "health_check_present"):
        _require_exact_bool(receipt[name], f"Redis unclaimed attestation {name}")
    expected = {
        "schema_version": "healthporta.ptg-wave.redis-unclaimed-failure.v1",
        "wave_id": wave.wave_digest,
        "queue_name": wave.release_queue,
        "manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest,
        "job_count": intent_count,
        "target_key_count": 4 + (4 * intent_count),
        "ready_slots": [],
        "ready_slots_digest": sha256_digest(canonical_json([])),
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
    if any(receipt.get(name) != value for name, value in expected.items()):
        raise PTGWavePreclaimSupersessionConflict(
            "Redis unclaimed attestation is not the empty pre-release state"
        )
    if receipt["release_present"] is not False or receipt["health_check_present"] is not False:
        raise PTGWavePreclaimSupersessionConflict(
            "Redis unclaimed attestation booleans are not exact"
        )
    digest = receipt["attestation_digest"]
    if type(digest) is not str or digest != sha256_digest(
        canonical_json({name: receipt[name] for name in receipt if name != "attestation_digest"})
    ):
        raise PTGWavePreclaimSupersessionConflict(
            "Redis unclaimed attestation digest is invalid"
        )
    return digest


def _require_sequence(value: Any, name: str) -> None:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise PTGWavePreclaimSupersessionConflict(f"{name} must be a sequence")


def _exact_mapping(value: Any, fields: set[str], name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise PTGWavePreclaimSupersessionConflict(f"{name} fields are not exact")
    return dict(value)


def _require_digest(value: Any, name: str) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise PTGWavePreclaimSupersessionConflict(
            f"{name} must be a lowercase SHA-256 digest"
        )


def _require_text(value: Any, name: str) -> None:
    if type(value) is not str or not value or value != value.strip() or len(value) > 160:
        raise PTGWavePreclaimSupersessionConflict(f"{name} must be a non-empty bounded string")


def _require_wave_id(value: Any, name: str) -> None:
    if type(value) is not str or not value or value != value.strip() or len(value) > 64:
        raise PTGWavePreclaimSupersessionConflict(f"{name} must be a non-empty bounded string")


def _validated_wave_id(value: Any, name: str) -> str:
    _require_wave_id(value, name)
    return value


def _text_attr(value: Any, name: str) -> str:
    result = getattr(value, name, None)
    _require_text(result, name)
    return result


def _require_text_attr(value: Any, name: str) -> None:
    _text_attr(value, name)


def _int_attr(value: Any, name: str) -> int:
    result = getattr(value, name, None)
    if type(result) is not int:
        raise PTGWavePreclaimSupersessionConflict(f"{name} must be an integer")
    return result


def _ordinal_attr(value: Any) -> int:
    ordinal = getattr(value, "ordinal", None)
    if type(ordinal) is not int or ordinal < 0:
        raise PTGWavePreclaimSupersessionConflict(
            "predecessor intent ordinal is invalid"
        )
    return ordinal


def _require_exact_int(value: Any, name: str) -> int:
    if type(value) is not int:
        raise PTGWavePreclaimSupersessionConflict(f"{name} must be an exact integer")
    return value


def _require_exact_bool(value: Any, name: str) -> bool:
    if type(value) is not bool:
        raise PTGWavePreclaimSupersessionConflict(f"{name} must be an exact boolean")
    return value


def _require_exact_text(value: Any, name: str) -> str:
    if type(value) is not str:
        raise PTGWavePreclaimSupersessionConflict(f"{name} must be an exact string")
    return value


__all__ = [
    "PTGWaveLogicalPreclaimSupersessionWitness",
    "PTGWavePreclaimSupersessionConflict",
    "attest_logical_preclaim_supersession",
    "validate_logical_preclaim_supersession_proof",
]
