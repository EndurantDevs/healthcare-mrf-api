"""Pure witness for superseding an unclaimed pre-claim wave failure.

This module deliberately consumes only already-observed mappings and durable
row snapshots.  It neither reads nor writes PostgreSQL, Redis, or Kubernetes.
The witness is narrow: it proves a POST-ticketed predecessor could not have
released or started work, while its exact Indexed Job reached all-slot failure.
It does not infer, store, or depend on historical Pod membership.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from api.ptg_wave_kubernetes import (
    PTGWaveContractError,
    PTG_WAVE_SLOT_COUNT,
)
from api.ptg_wave_kubernetes_attestation import attest_existing_ptg_wave_job
from process.ptg_wave_failure_snapshots import _is_prestart_run_pristine
from process.ptg_wave_state import canonical_json, sha256_digest
from process.ptg_wave_preclaim_supersession_contract import (
    PTGWaveLogicalPreclaimSupersessionWitness,
    PTGWavePreclaimSupersessionConflict,
    _is_exact_bool,
    _require_exact_int,
    _require_text,
    _require_wave_id,
    validate_logical_preclaim_supersession_proof,
)


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


@dataclass(frozen=True)
class PTGWavePreclaimObservation:
    """One atomic predecessor snapshot with both external observations."""

    predecessor_wave: Any
    intents: Sequence[Any]
    runs: Sequence[Any]
    claims: Sequence[Any]
    outcomes: Sequence[Any]
    worker_start_event_ordinals: Sequence[Any]
    actual_job: Mapping[str, Any]
    redis_unclaimed_attestation: Mapping[str, Any]


def attest_logical_preclaim_supersession(
    observation: PTGWavePreclaimObservation,
    successor_wave_id: str,
) -> PTGWaveLogicalPreclaimSupersessionWitness:
    """Return a witness only for an exact, fully unclaimed predecessor.

    A caller supplies a single atomic snapshot of predecessor rows and the
    external observations.  Every mismatch blocks supersession.  This helper
    does not interpret an all-failed Job as evidence of any particular Pods.
    """

    predecessor_wave_id = _validate_preclaim_observation(
        observation,
        successor_wave_id,
    )
    job_name, job_uid = _attest_terminal_preclaim_job(
        observation.predecessor_wave,
        observation.actual_job,
    )
    redis_digest = _attest_empty_unclaimed_redis(
        observation.predecessor_wave,
        observation.redis_unclaimed_attestation,
    )
    return _build_preclaim_witness(
        observation.predecessor_wave,
        predecessor_wave_id=predecessor_wave_id,
        successor_wave_id=successor_wave_id,
        job_name=job_name,
        job_uid=job_uid,
        redis_digest=redis_digest,
    )


def _validate_preclaim_observation(
    observation: PTGWavePreclaimObservation,
    successor_wave_id: str,
) -> str:
    _require_sequence(observation.intents, "intents")
    _require_sequence(observation.runs, "runs")
    _require_sequence(observation.claims, "claims")
    _require_sequence(observation.outcomes, "outcomes")
    _require_sequence(
        observation.worker_start_event_ordinals,
        "worker start event ordinals",
    )
    _require_predecessor_preclaim_boundary(observation.predecessor_wave)
    _require_wave_id(successor_wave_id, "successor wave ID")
    predecessor_wave_id = _text_attr(observation.predecessor_wave, "wave_id")
    if successor_wave_id == predecessor_wave_id:
        raise PTGWavePreclaimSupersessionConflict(
            "successor wave ID must differ from the predecessor"
        )
    _require_exact_intents_and_pristine_runs(
        observation.predecessor_wave,
        observation.intents,
        observation.runs,
    )
    if observation.claims:
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim supersession requires no claims"
        )
    if observation.outcomes:
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim supersession requires no outcomes"
        )
    if observation.worker_start_event_ordinals:
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim supersession requires no worker start events"
        )
    return predecessor_wave_id


def _build_preclaim_witness(
    predecessor_wave: Any,
    *,
    predecessor_wave_id: str,
    successor_wave_id: str,
    job_name: str,
    job_uid: str,
    redis_digest: str,
) -> PTGWaveLogicalPreclaimSupersessionWitness:
    witness_field_map = {
        "predecessor_wave_id": predecessor_wave_id,
        "predecessor_wave_digest": _text_attr(
            predecessor_wave,
            "wave_digest",
        ),
        "successor_wave_id": successor_wave_id,
        "manifest_digest": _text_attr(predecessor_wave, "manifest_digest"),
        "jobs_digest": _text_attr(predecessor_wave, "jobs_digest"),
        "intent_count": _int_attr(predecessor_wave, "intent_count"),
        "job_name": job_name,
        "job_uid": job_uid,
        "redis_attestation_digest": redis_digest,
    }
    unsigned_evidence_map = PTGWaveLogicalPreclaimSupersessionWitness(
        **witness_field_map, proof_digest=""
    ).evidence_mapping()
    return PTGWaveLogicalPreclaimSupersessionWitness(
        **witness_field_map,
        proof_digest=sha256_digest(canonical_json(unsigned_evidence_map)),
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
    ordinal_intents = [(_ordinal_attr(intent), intent) for intent in intents]
    ordered_intents = [
        intent
        for _, intent in sorted(
            ordinal_intents,
            key=lambda ordinal_intent: ordinal_intent[0],
        )
    ]
    ordered_ordinals = [
        ordinal for ordinal, _ in sorted(ordinal_intents, key=lambda entry: entry[0])
    ]
    if ordered_ordinals != list(range(intent_count)):
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
    expected_attributes = (
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
    if any(
        getattr(wave, name, None) != expected_attribute_value
        for name, expected_attribute_value in expected_attributes
    ):
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
        status_count = status.get(name, 0)
        if type(status_count) is not int or status_count != 0:
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
        _is_exact_bool(receipt[name], f"Redis unclaimed attestation {name}")
    expected_redis_receipt_map = {
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
    if any(
        receipt.get(name) != expected_receipt_value
        for name, expected_receipt_value in expected_redis_receipt_map.items()
    ):
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


__all__ = [
    "PTGWaveLogicalPreclaimSupersessionWitness",
    "PTGWavePreclaimObservation",
    "PTGWavePreclaimSupersessionConflict",
    "attest_logical_preclaim_supersession",
    "validate_logical_preclaim_supersession_proof",
]
