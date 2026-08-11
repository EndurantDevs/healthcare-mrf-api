"""Fresh-V12 proof for a materialized wave that never started work."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from process.ptg_wave_materialized_preclaim_supersession import (
    PTGWaveMaterializedPreclaimObservation,
    _attest_external_boundary,
    _kubernetes_proof_map,
    _redis_proof_map,
    _require_boundary,
    _require_sequences_and_zero_work,
)
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
)
from process.ptg_wave_preclaim_supersession import (
    _require_exact_intents_and_pristine_runs,
)
from process.ptg_wave_receipt_contract import (
    ABANDONMENT_PROOF_SCHEMA,
    PTGWaveReceiptContractError,
    V12_QUARANTINE_REASON,
    ordinary_cutover_id,
    validate_receipt_admission,
)
from process.ptg_wave_state import canonical_json, sha256_digest
from process.ptg_wave_v12_database_evidence import (
    INTENT_ROWS_DIGEST_DOMAIN,
    MEMBER_ROWS_DIGEST_DOMAIN,
    RUN_ROWS_DIGEST_DOMAIN,
    exact_pristine_database_proof as _exact_database_proof,
    expected_pristine_run_values as _expected_run_values,
)


_PROOF_FIELDS = frozenset(
    {
        "schema_version",
        "recovery_basis",
        "operation_id",
        "cutover_id",
        "admission",
        "database",
        "kubernetes",
        "redis",
        "proof_digest",
    }
)
_DATABASE_FIELDS = frozenset(
    {
        "state",
        "intent_count",
        "run_count",
        "pristine_run_count",
        "unassigned_run_count",
        "claim_count",
        "outcome_count",
        "worker_start_event_count",
        "member_rows_digest",
        "intent_rows_digest",
        "run_rows_digest",
    }
)
ABANDONMENT_PAYLOAD_FIELDS = frozenset(
    {
        "operation_id",
        "cutover_id",
        "wave_id",
        "wave_digest",
        "state",
        "quarantine_reason",
        "recovery_schema",
        "recovery_evidence_sha256",
        "admission",
        "database",
        "kubernetes",
        "redis",
    }
)


def attest_v12_pristine_materialized_abandonment(
    observation: PTGWaveMaterializedPreclaimObservation,
    *,
    cutover_id: str,
    admission: Mapping[str, Any],
) -> dict[str, Any]:
    """Build one proof without any legacy predecessor or rollback lineage."""

    wave = observation.predecessor_wave
    _require_boundary(wave)
    _require_sequences_and_zero_work(observation)
    if (
        observation.logical_supersession is not None
        or observation.admission_rollback is not None
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 abandonment cannot depend on legacy recovery rows"
        )
    _require_exact_intents_and_pristine_runs(
        wave,
        observation.intents,
        observation.runs,
    )
    database = _exact_database_proof(
        wave,
        observation.intents,
        observation.runs,
        observation.claims,
        observation.outcomes,
        observation.worker_start_event_ordinals,
    )
    job_name, job_uid, redis_digest = _attest_external_boundary(observation)
    if job_uid != wave.kubernetes_job_uid:
        raise PTGWaveMaterializedPreclaimConflict(
            "terminal Job UID differs from the durable creation receipt"
        )
    unsigned_proof_by_field = {
        "schema_version": ABANDONMENT_PROOF_SCHEMA,
        "recovery_basis": V12_QUARANTINE_REASON,
        "operation_id": wave.wave_id,
        "cutover_id": cutover_id,
        "admission": dict(admission),
        "database": database,
        "kubernetes": _kubernetes_proof_map(wave, job_name, job_uid),
        "redis": _redis_proof_map(redis_digest),
    }
    return validate_v12_pristine_abandonment_proof(
        {
            **unsigned_proof_by_field,
            "proof_digest": _proof_digest(unsigned_proof_by_field),
        },
        operation_id=wave.wave_id,
        cutover_id=cutover_id,
        admission=admission,
    )


def validate_v12_pristine_abandonment_proof(
    proof: object,
    *,
    operation_id: str | None = None,
    cutover_id: str | None = None,
    admission: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate the exact fresh family and its domain-separated evidence."""

    if not isinstance(proof, Mapping) or set(proof) != _PROOF_FIELDS:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 abandonment proof fields are invalid"
        )
    proof_by_field = dict(proof)
    if (
        proof_by_field["schema_version"] != ABANDONMENT_PROOF_SCHEMA
        or proof_by_field["recovery_basis"] != V12_QUARANTINE_REASON
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 abandonment proof family is unsupported"
        )
    operation_value = _require_digest(proof_by_field["operation_id"], "operation ID")
    cutover_value = _require_digest(proof_by_field["cutover_id"], "cutover ID")
    if cutover_value != ordinary_cutover_id(operation_value):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 abandonment cutover identity is invalid"
        )
    if operation_id is not None and operation_value != operation_id:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 abandonment proof identifies another operation"
        )
    if cutover_id is not None and cutover_value != cutover_id:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 abandonment proof identifies another cutover"
        )
    try:
        admission_value = validate_receipt_admission(proof_by_field["admission"])
    except PTGWaveReceiptContractError as exc:
        raise PTGWaveMaterializedPreclaimConflict(str(exc)) from exc
    if (
        (admission is not None and admission_value != dict(admission))
        or admission_value["wave_id"] != operation_value
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 abandonment admission binding is invalid"
        )
    _validate_database(proof_by_field["database"], admission_value.get("intent_count"))
    _validate_kubernetes(proof_by_field["kubernetes"])
    _validate_redis(proof_by_field["redis"])
    unsigned_proof_by_field = {
        field_name: field_value
        for field_name, field_value in proof_by_field.items()
        if field_name != "proof_digest"
    }
    if proof_by_field["proof_digest"] != _proof_digest(unsigned_proof_by_field):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 abandonment proof digest is invalid"
        )
    return proof_by_field


def proof_signing_bytes(proof: Mapping[str, Any]) -> bytes:
    """Return the exact domain-separated bytes hashed as recovery evidence."""

    unsigned_proof_by_field = {
        field_name: field_value
        for field_name, field_value in proof.items()
        if field_name != "proof_digest"
    }
    return ABANDONMENT_PROOF_SCHEMA.encode("ascii") + b"\0" + canonical_json(
        unsigned_proof_by_field
    )


def abandonment_receipt_payload(proof: object) -> dict[str, Any]:
    """Project the exact signed payload from one validated fresh proof."""

    validated = validate_v12_pristine_abandonment_proof(proof)
    admission = validated["admission"]
    payload = {
        "operation_id": validated["operation_id"],
        "cutover_id": validated["cutover_id"],
        "wave_id": admission["wave_id"],
        "wave_digest": admission["wave_digest"],
        "state": "abandoned",
        "quarantine_reason": V12_QUARANTINE_REASON,
        "recovery_schema": validated["schema_version"],
        "recovery_evidence_sha256": validated["proof_digest"],
        "admission": admission,
        "database": validated["database"],
        "kubernetes": validated["kubernetes"],
        "redis": validated["redis"],
    }
    if set(payload) != ABANDONMENT_PAYLOAD_FIELDS:
        raise AssertionError("V12 abandonment receipt payload fields changed")
    return payload


def _proof_digest(unsigned: Mapping[str, Any]) -> str:
    return sha256_digest(
        ABANDONMENT_PROOF_SCHEMA.encode("ascii")
        + b"\0"
        + canonical_json(dict(unsigned))
    )


def _validate_database(database_by_field: object, intent_count: object) -> None:
    if not isinstance(database_by_field, Mapping) or set(database_by_field) != _DATABASE_FIELDS:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 database proof fields are invalid"
        )
    if type(intent_count) is not int or intent_count < 1:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 admission intent count is invalid"
        )
    expected_count_by_field = {
        "intent_count": intent_count,
        "run_count": intent_count,
        "pristine_run_count": intent_count,
        "unassigned_run_count": intent_count,
        "claim_count": 0,
        "outcome_count": 0,
        "worker_start_event_count": 0,
    }
    if database_by_field.get("state") != "slots_waiting" or any(
        type(database_by_field.get(field_name)) is not int
        or database_by_field.get(field_name) != count
        for field_name, count in expected_count_by_field.items()
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 database proof is not pristine"
        )
    for field_name in (
        "member_rows_digest",
        "intent_rows_digest",
        "run_rows_digest",
    ):
        _require_digest(database_by_field.get(field_name), field_name)


def _validate_kubernetes(kubernetes_by_field: object) -> None:
    expected_value_by_field = {
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
    fields = set(expected_value_by_field) | {"job_name", "job_uid", "job_receipt_digest"}
    if not isinstance(kubernetes_by_field, Mapping) or set(kubernetes_by_field) != fields:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 Kubernetes proof fields are invalid"
        )
    if any(
        kubernetes_by_field.get(name) != expected_value
        for name, expected_value in expected_value_by_field.items()
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 Kubernetes proof is not terminal preclaim failure"
        )
    if not all(
        isinstance(kubernetes_by_field.get(name), str) and kubernetes_by_field.get(name)
        for name in ("job_name", "job_uid")
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 Kubernetes identity is invalid"
        )
    _require_digest(kubernetes_by_field.get("job_receipt_digest"), "job receipt digest")


def _validate_redis(value: object) -> None:
    expected_value_by_field = {
        "ready_slot_count": 0,
        "release_present": False,
        "queued_ordinal_count": 0,
        "job_ordinal_count": 0,
        "result_ordinal_count": 0,
        "retry_ordinal_count": 0,
        "in_progress_ordinal_count": 0,
        "health_check_present": False,
    }
    fields = set(expected_value_by_field) | {"unclaimed_attestation_digest"}
    if not isinstance(value, Mapping) or set(value) != fields:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 Redis proof fields are invalid"
        )
    if any(
        value.get(name) != expected_value
        for name, expected_value in expected_value_by_field.items()
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 Redis proof is not empty"
        )
    _require_digest(
        value.get("unclaimed_attestation_digest"),
        "Redis attestation digest",
    )


def _require_digest(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise PTGWaveMaterializedPreclaimConflict(f"{name} is invalid")
    return value


__all__ = [
    "ABANDONMENT_PAYLOAD_FIELDS",
    "INTENT_ROWS_DIGEST_DOMAIN",
    "MEMBER_ROWS_DIGEST_DOMAIN",
    "RUN_ROWS_DIGEST_DOMAIN",
    "attest_v12_pristine_materialized_abandonment",
    "abandonment_receipt_payload",
    "proof_signing_bytes",
    "validate_v12_pristine_abandonment_proof",
]
