"""Pure V13 evidence for an unreleased failed PTG wave after Pod GC.

The Job creation receipt is durable, but Kubernetes may garbage-collect most
failed Pods before this recovery is observed.  V13 therefore binds the
terminal all-twelve Job fact to all twelve GET-only Redis registrations and
to every failed Pod Kubernetes still retains; it never fabricates missing Pod
objects or a readiness receipt that was never persisted.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from api.ptg_wave_kubernetes_retained_failure_attestation import (
    attest_retained_preclaim_failure_kubernetes_objects,
)
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
)
from process.ptg_wave_preclaim_supersession import (
    _require_exact_intents_and_pristine_runs,
)
from process.ptg_wave_receipt_contract import (
    PTGWaveReceiptContractError,
    ordinary_cutover_id,
    validate_receipt_admission,
)
from process.ptg_wave_v12_database_evidence import exact_pristine_database_proof
from process.ptg_wave_v13_post_ready_abandonment_boundary import (
    _require_post_ready_boundary,
)
from process.ptg_wave_v13_post_ready_abandonment_contract import (
    ABANDONMENT_PAYLOAD_FIELDS,
    V13_ABANDONMENT_PROOF_SCHEMA,
    V13_ABANDONMENT_REQUEST_SCHEMA,
    V13_QUARANTINE_REASON,
    _PROOF_FIELDS,
    _proof_digest,
    _require_digest,
)
from process.ptg_wave_v13_post_ready_abandonment_validation import (
    _require_zero_work,
    _validated_redis_proof,
    _validate_database,
    _validate_kubernetes,
    _validate_redis,
)


@dataclass(frozen=True)
class PTGWaveV13PostReadyObservation:
    """A locked DB boundary and GET-only external observations."""

    predecessor_wave: Any
    intents: Sequence[Any]
    runs: Sequence[Any]
    claims: Sequence[Any]
    outcomes: Sequence[Any]
    worker_start_event_ordinals: Sequence[Any]
    logical_supersession: Any
    admission_rollback: Any
    actual_job: Mapping[str, Any]
    actual_pods: Sequence[Mapping[str, Any]]
    redis_unclaimed_attestation: Mapping[str, Any]


def validate_v13_abandonment_request(
    request: object,
    *,
    wave: Any,
    admission: Mapping[str, Any],
) -> dict[str, Any]:
    """Require caller material to equal the frozen stored admission."""

    if not isinstance(request, Mapping) or set(request) != {
        "schema",
        "key_id",
        "operation_id",
        "cutover_id",
        "admission",
    }:
        raise PTGWaveReceiptContractError("V13 abandonment request fields are invalid")
    admission_map = validate_receipt_admission(admission)
    operation_id = _require_digest(request.get("operation_id"), "operation ID")
    cutover_id = _require_digest(request.get("cutover_id"), "cutover ID")
    if (
        request.get("schema") != V13_ABANDONMENT_REQUEST_SCHEMA
        or operation_id != getattr(wave, "wave_id", None)
        or operation_id != admission_map["wave_id"]
        or cutover_id != ordinary_cutover_id(operation_id)
        or request.get("key_id") != admission_map["receipt_key_id"]
        or request.get("admission") != admission_map
    ):
        raise PTGWaveReceiptContractError(
            "V13 abandonment request conflicts with stored admission"
        )
    return dict(request)


def attest_v13_post_ready_abandonment(
    observation: PTGWaveV13PostReadyObservation,
    *,
    cutover_id: str,
    admission: Mapping[str, Any],
) -> dict[str, Any]:
    """Build a V13 proof without mutation or synthetic Pod identity."""

    wave = observation.predecessor_wave
    _require_post_ready_boundary(wave)
    _require_zero_work(observation)
    if (
        observation.logical_supersession is not None
        or observation.admission_rollback is not None
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 abandonment cannot depend on legacy recovery rows"
        )
    database, failure = _attested_v13_evidence(wave, observation)
    kubernetes_evidence_by_field = {
        "job_receipt": dict(wave.kubernetes_job_receipt),
        "job_receipt_digest": wave.kubernetes_job_receipt_digest,
        "ready_attestation": None,
        "ready_attestation_digest": None,
        "failure": failure,
    }
    redis = _validated_redis_proof(
        observation.redis_unclaimed_attestation,
        wave=wave,
        failure=failure,
    )
    unsigned_proof_by_field = {
        "schema_version": V13_ABANDONMENT_PROOF_SCHEMA,
        "recovery_basis": V13_QUARANTINE_REASON,
        "operation_id": wave.wave_id,
        "cutover_id": cutover_id,
        "admission": dict(admission),
        "database": database,
        "kubernetes": kubernetes_evidence_by_field,
        "redis": redis,
    }
    return validate_v13_abandonment_proof(
        {
            **unsigned_proof_by_field,
            "proof_digest": _proof_digest(unsigned_proof_by_field),
        },
        operation_id=wave.wave_id,
        cutover_id=cutover_id,
        admission=admission,
    )


def _attested_v13_evidence(
    wave: Any,
    observation: PTGWaveV13PostReadyObservation,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build exact database and retained-Kubernetes evidence from observation."""

    try:
        _require_exact_intents_and_pristine_runs(wave, observation.intents, observation.runs)
        database = exact_pristine_database_proof(
            wave,
            observation.intents,
            observation.runs,
            observation.claims,
            observation.outcomes,
            observation.worker_start_event_ordinals,
        )
        failure = attest_retained_preclaim_failure_kubernetes_objects(
            wave.kubernetes_manifest,
            observation.actual_job,
            observation.actual_pods,
        ).as_mapping()
    except PTGWaveMaterializedPreclaimConflict:
        raise
    except Exception as exc:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 post-ready evidence is invalid"
        ) from exc
    return database, failure


def validate_v13_abandonment_proof(
    proof: object,
    *,
    operation_id: str | None = None,
    cutover_id: str | None = None,
    admission: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate the closed V13 family and full cross-family evidence binding."""

    if not isinstance(proof, Mapping) or set(proof) != _PROOF_FIELDS:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 abandonment proof fields are invalid"
        )
    proof_by_field = dict(proof)
    if (
        proof_by_field["schema_version"] != V13_ABANDONMENT_PROOF_SCHEMA
        or proof_by_field["recovery_basis"] != V13_QUARANTINE_REASON
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 abandonment proof family is unsupported"
        )
    operation = _require_digest(proof_by_field["operation_id"], "operation ID")
    cutover = _require_digest(proof_by_field["cutover_id"], "cutover ID")
    if cutover != ordinary_cutover_id(operation):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 abandonment cutover identity is invalid"
        )
    if operation_id is not None and operation != operation_id:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 abandonment proof identifies another operation"
        )
    if cutover_id is not None and cutover != cutover_id:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 abandonment proof identifies another cutover"
        )
    try:
        admission_value = validate_receipt_admission(proof_by_field["admission"])
    except PTGWaveReceiptContractError as exc:
        raise PTGWaveMaterializedPreclaimConflict(str(exc)) from exc
    if (
        admission_value["wave_id"] != operation
        or (admission is not None and admission_value != dict(admission))
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 abandonment admission binding is invalid"
        )
    _validate_database(proof_by_field["database"], admission_value["intent_count"])
    failure = _validate_kubernetes(proof_by_field["kubernetes"], admission_value)
    _validate_redis(proof_by_field["redis"], admission_value, failure)
    unsigned_proof_by_field = {
        name: field_value
        for name, field_value in proof_by_field.items()
        if name != "proof_digest"
    }
    if proof_by_field["proof_digest"] != _proof_digest(unsigned_proof_by_field):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 abandonment proof digest is invalid"
        )
    return proof_by_field


def abandonment_receipt_payload(proof: object) -> dict[str, Any]:
    """Project the unchanged v2 signed-envelope payload from one V13 proof."""

    validated = validate_v13_abandonment_proof(proof)
    admission = validated["admission"]
    payload = {
        "operation_id": validated["operation_id"],
        "cutover_id": validated["cutover_id"],
        "wave_id": admission["wave_id"],
        "wave_digest": admission["wave_digest"],
        "state": "abandoned",
        "quarantine_reason": V13_QUARANTINE_REASON,
        "recovery_schema": validated["schema_version"],
        "recovery_evidence_sha256": validated["proof_digest"],
        "admission": admission,
        "database": validated["database"],
        "kubernetes": validated["kubernetes"],
        "redis": validated["redis"],
    }
    if set(payload) != ABANDONMENT_PAYLOAD_FIELDS:
        raise AssertionError("V13 abandonment receipt payload fields changed")
    return payload


__all__ = [
    "ABANDONMENT_PAYLOAD_FIELDS",
    "PTGWaveV13PostReadyObservation",
    "V13_ABANDONMENT_PROOF_SCHEMA",
    "V13_ABANDONMENT_REQUEST_SCHEMA",
    "V13_QUARANTINE_REASON",
    "abandonment_receipt_payload",
    "attest_v13_post_ready_abandonment",
    "validate_v13_abandonment_request",
    "validate_v13_abandonment_proof",
]
