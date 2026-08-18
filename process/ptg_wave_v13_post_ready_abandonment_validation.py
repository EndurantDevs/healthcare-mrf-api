"""Pure field-level validation for the closed V13 abandonment proof."""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from typing import Any

from api.ptg_wave_kubernetes import PTG_WAVE_SLOT_COUNT
from api.ptg_wave_kubernetes_retained_failure_attestation import (
    RETAINED_FAILURE_SCHEMA,
)
from process._ptg_wave_redis_encoding import (
    PTG_SMALL_WAVE_PROTOCOL_IDENTITY,
    PTG_SMALL_WAVE_SERIALIZER_IDENTITY,
    encode_job_count,
    runtime_identity_digest,
)
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
)
from process.ptg_wave_state import canonical_json, sha256_digest
from process.ptg_wave_v13_post_ready_abandonment_contract import (
    _CONDITION_FIELDS,
    _DATABASE_FIELDS,
    _FAILURE_FIELDS,
    _JOB_RECEIPT_FIELDS,
    _KUBERNETES_FIELDS,
    _REDIS_FIELDS,
    _REDIS_SLOT_FIELDS,
    _RETAINED_SLOT_FIELDS,
    _TERMINATION_FIELDS,
    _TIME,
    _require_digest,
)


def _require_zero_work(observation: Any) -> None:
    for name in (
        "intents",
        "runs",
        "claims",
        "outcomes",
        "worker_start_event_ordinals",
        "actual_pods",
    ):
        value = getattr(observation, name)
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
            raise PTGWaveMaterializedPreclaimConflict(f"fresh V13 {name} must be a sequence")
    if observation.claims or observation.outcomes or observation.worker_start_event_ordinals:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 predecessor must have zero claims, outcomes, and worker starts"
        )


def _validated_redis_proof(
    value: object,
    *,
    wave: Any,
    failure: Mapping[str, Any],
) -> dict[str, Any]:
    _validate_redis_against_wave(value, wave, failure)
    return dict(value)


def _validate_database(value: object, intent_count: object) -> None:
    if not isinstance(value, Mapping) or set(value) != _DATABASE_FIELDS:
        raise PTGWaveMaterializedPreclaimConflict("fresh V13 database proof fields are invalid")
    if type(intent_count) is not int or intent_count < 1:
        raise PTGWaveMaterializedPreclaimConflict("fresh V13 admission intent count is invalid")
    expected_by_field = {
        "intent_count": intent_count,
        "run_count": intent_count,
        "pristine_run_count": intent_count,
        "unassigned_run_count": intent_count,
        "claim_count": 0,
        "outcome_count": 0,
        "worker_start_event_count": 0,
    }
    if value.get("state") != "slots_waiting" or any(
        type(value.get(name)) is not int or value[name] != expected_value
        for name, expected_value in expected_by_field.items()
    ):
        raise PTGWaveMaterializedPreclaimConflict("fresh V13 database proof is not pristine")
    for name in ("member_rows_digest", "intent_rows_digest", "run_rows_digest"):
        _require_digest(value.get(name), name)


def _validate_kubernetes(
    kubernetes_by_field: object,
    admission: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Validate the retained Job/Pod evidence against one stored admission."""

    if (
        not isinstance(kubernetes_by_field, Mapping)
        or set(kubernetes_by_field) != _KUBERNETES_FIELDS
    ):
        raise PTGWaveMaterializedPreclaimConflict("fresh V13 Kubernetes proof fields are invalid")
    job_receipt_by_field = _validated_job_receipt(kubernetes_by_field, admission)
    return _validated_failure_attestation(
        kubernetes_by_field,
        job_receipt_by_field,
        admission,
    )


def _validated_job_receipt(
    kubernetes_by_field: Mapping[str, Any],
    admission: Mapping[str, Any],
) -> Mapping[str, Any]:
    job_receipt_by_field = kubernetes_by_field["job_receipt"]
    if (
        not isinstance(job_receipt_by_field, Mapping)
        or set(job_receipt_by_field) != _JOB_RECEIPT_FIELDS
    ):
        raise PTGWaveMaterializedPreclaimConflict("fresh V13 Job receipt fields are invalid")
    if (
        kubernetes_by_field["ready_attestation"] is not None
        or kubernetes_by_field["ready_attestation_digest"] is not None
        or kubernetes_by_field["job_receipt_digest"]
        != sha256_digest(canonical_json(dict(job_receipt_by_field)))
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 Kubernetes durable receipt binding is invalid"
        )
    required_text_fields = (
        "job_uid",
        "manifest_identity",
        "config_identity",
        "pinned_image_reference",
        "pinned_image_digest",
        "runtime_image_identity",
    )
    if (
        job_receipt_by_field.get("wave_digest") != admission["wave_digest"]
        or any(
            not isinstance(job_receipt_by_field.get(name), str)
            or not job_receipt_by_field[name]
            for name in required_text_fields
        )
        or job_receipt_by_field["pinned_image_digest"]
        != job_receipt_by_field["pinned_image_reference"].rsplit("@sha256:", 1)[-1]
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 Job receipt admission binding is invalid"
        )
    return job_receipt_by_field


def _validated_failure_attestation(
    kubernetes_by_field: Mapping[str, Any],
    job_receipt_by_field: Mapping[str, Any],
    admission: Mapping[str, Any],
) -> Mapping[str, Any]:
    failure_by_field = kubernetes_by_field["failure"]
    if (
        not isinstance(failure_by_field, Mapping)
        or set(failure_by_field) != _FAILURE_FIELDS
    ):
        raise PTGWaveMaterializedPreclaimConflict("fresh V13 failure attestation fields are invalid")
    expected_by_field = _expected_failure_by_field(job_receipt_by_field, admission)
    if _is_failure_attestation_invalid(failure_by_field, expected_by_field, admission):
        raise PTGWaveMaterializedPreclaimConflict("fresh V13 failure attestation is invalid")
    _retained_slot_map(
        failure_by_field["retained_failed_slots"],
        failure_by_field["runtime_image_identity"],
    )
    return failure_by_field


def _expected_failure_by_field(
    job_receipt_by_field: Mapping[str, Any],
    admission: Mapping[str, Any],
) -> dict[str, Any]:
    expected_by_field = {
        "schema_version": RETAINED_FAILURE_SCHEMA,
        "wave_digest": job_receipt_by_field["wave_digest"],
        "manifest_digest": admission["manifest_digest"],
        "jobs_digest": admission["jobs_digest"],
        "job_count": admission["intent_count"],
        "config_identity": job_receipt_by_field["config_identity"],
        "manifest_identity": job_receipt_by_field["manifest_identity"],
        "image_identity": job_receipt_by_field["pinned_image_reference"],
        "runtime_image_identity": job_receipt_by_field["runtime_image_identity"],
        "job_uid": job_receipt_by_field["job_uid"],
        "backoff_limit": 0,
        "job_active": None,
        "job_failed": PTG_WAVE_SLOT_COUNT,
        "job_succeeded": None,
        "job_ready": 0,
        "job_terminating": 0,
        "completed_indexes": None,
        "failed_indexes": None,
        "completion_time": None,
        "uncounted_terminated_pods": {},
    }
    return expected_by_field


def _is_failure_attestation_invalid(
    failure_by_field: Mapping[str, Any],
    expected_by_field: Mapping[str, Any],
    admission: Mapping[str, Any],
) -> bool:
    if (
        any(
            not _is_exact_json_value(failure_by_field.get(name), expected_value)
            for name, expected_value in expected_by_field.items()
        )
        or failure_by_field.get("queue")
        != "arq:PTGSmall:wave:" + admission["wave_digest"]
        or failure_by_field.get("job_name")
        != "hpw-ptg-wave-" + admission["wave_digest"][:40]
        or not _is_timestamp(failure_by_field.get("start_time"))
        or not _has_exact_conditions(failure_by_field.get("job_conditions"))
        or failure_by_field.get("attestation_digest")
        != sha256_digest(
            canonical_json(
                {
                    name: field_value
                    for name, field_value in failure_by_field.items()
                    if name != "attestation_digest"
                }
            )
        )
    ):
        return True
    return False


def _validate_redis(
    value: object,
    admission: Mapping[str, Any],
    failure: Mapping[str, Any],
) -> None:
    wave_like = type("WaveLike", (), {
        "wave_digest": admission["wave_digest"],
        "release_queue": failure["queue"],
        "manifest_digest": admission["manifest_digest"],
        "jobs_digest": admission["jobs_digest"],
        "intent_count": admission["intent_count"],
        "kubernetes_config_identity": failure["config_identity"],
        "kubernetes_manifest_identity": failure["manifest_identity"],
        "pinned_image_reference": failure["image_identity"],
        "runtime_image_identity": failure["runtime_image_identity"],
        "protocol_identity": PTG_SMALL_WAVE_PROTOCOL_IDENTITY,
        "serializer_identity": PTG_SMALL_WAVE_SERIALIZER_IDENTITY,
    })()
    _validate_redis_against_wave(value, wave_like, failure)


def _validate_redis_against_wave(
    redis_by_field: object,
    wave: Any,
    failure: Mapping[str, Any],
) -> None:
    """Require the exact empty-release Redis state and retained Pod bindings."""

    _validate_redis_fields(redis_by_field, wave)
    ready_slots = _ready_slots(redis_by_field)
    expected_runtime_digest = _expected_runtime_identity_digest(wave)
    ready_pod_uid_by_slot = _ready_pod_uid_by_slot(
        ready_slots,
        wave,
        expected_runtime_digest,
    )
    if set(ready_pod_uid_by_slot) != set(range(PTG_WAVE_SLOT_COUNT)):
        raise PTGWaveMaterializedPreclaimConflict("fresh V13 Redis proof omits a ready slot")
    _require_retained_pod_bindings(ready_pod_uid_by_slot, failure)
    _validate_redis_attestation_digests(redis_by_field, ready_slots)


def _validate_redis_fields(redis_by_field: object, wave: Any) -> None:
    if not isinstance(redis_by_field, Mapping) or set(redis_by_field) != _REDIS_FIELDS:
        raise PTGWaveMaterializedPreclaimConflict("fresh V13 Redis proof fields are invalid")
    expected_by_field = {
        "schema_version": "healthporta.ptg-wave.redis-unclaimed-failure.v1",
        "wave_id": wave.wave_digest,
        "queue_name": wave.release_queue,
        "manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest,
        "job_count": wave.intent_count,
        "target_key_count": 4 + (4 * wave.intent_count),
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
        not _is_exact_json_value(redis_by_field.get(name), expected_value)
        for name, expected_value in expected_by_field.items()
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 Redis proof is not the exact unreleased ready state"
        )


def _ready_slots(redis_by_field: object) -> list[Any]:
    ready_slots = redis_by_field["ready_slots"]
    if not isinstance(ready_slots, list) or len(ready_slots) != PTG_WAVE_SLOT_COUNT:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 Redis proof must bind exactly twelve ready slots"
        )
    return ready_slots


def _expected_runtime_identity_digest(wave: Any) -> str:
    try:
        return runtime_identity_digest(
            wave.kubernetes_config_identity,
            wave.kubernetes_manifest_identity,
            wave.pinned_image_reference,
            wave.runtime_image_identity,
        )
    except Exception as exc:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 Redis runtime identity is invalid"
        ) from exc


def _ready_pod_uid_by_slot(
    ready_slots: list[Any],
    wave: Any,
    expected_runtime_digest: str,
) -> dict[int, str]:
    ready_pod_uid_by_slot: dict[int, str] = {}
    for ready_slot in ready_slots:
        if not isinstance(ready_slot, Mapping) or set(ready_slot) != _REDIS_SLOT_FIELDS:
            raise PTGWaveMaterializedPreclaimConflict("fresh V13 Redis ready-slot fields are invalid")
        ordinal = ready_slot.get("slot")
        if (
            type(ordinal) is not int
            or ordinal not in range(PTG_WAVE_SLOT_COUNT)
            or ordinal in ready_pod_uid_by_slot
        ):
            raise PTGWaveMaterializedPreclaimConflict("fresh V13 Redis slot identities are not exact")
        expected_slot_by_field = {
            "config_identity": wave.kubernetes_config_identity,
            "kubernetes_manifest_identity": wave.kubernetes_manifest_identity,
            "image_identity": wave.pinned_image_reference,
            "runtime_image_identity": wave.runtime_image_identity,
            "runtime_identity_digest": expected_runtime_digest,
            "manifest_digest": wave.manifest_digest,
            "pod_uid": ready_slot.get("pod_uid"),
            "queue_name": wave.release_queue,
            "slot": ordinal,
            "wave_id": wave.wave_digest,
            "jobs_digest": wave.jobs_digest,
            "job_count": encode_job_count(wave.intent_count),
            "protocol_identity": wave.protocol_identity,
            "serializer_identity": wave.serializer_identity,
        }
        if (
            not isinstance(ready_slot.get("pod_uid"), str)
            or not ready_slot["pod_uid"]
            or dict(ready_slot) != expected_slot_by_field
        ):
            raise PTGWaveMaterializedPreclaimConflict("fresh V13 Redis ready-slot binding is invalid")
        ready_pod_uid_by_slot[ordinal] = ready_slot["pod_uid"]
    return ready_pod_uid_by_slot


def _require_retained_pod_bindings(
    ready_pod_uid_by_slot: Mapping[int, str],
    failure: Mapping[str, Any],
) -> None:
    for ordinal, pod_uid in _retained_slot_map(
        failure["retained_failed_slots"],
        failure["runtime_image_identity"],
    ).items():
        if ready_pod_uid_by_slot[ordinal] != pod_uid:
            raise PTGWaveMaterializedPreclaimConflict(
                "fresh V13 retained Pods differ from Redis ready-slot identities"
            )


def _validate_redis_attestation_digests(
    redis_by_field: Mapping[str, Any],
    ready_slots: list[Any],
) -> None:
    if (
        redis_by_field["ready_slots_digest"]
        != sha256_digest(canonical_json(ready_slots))
        or redis_by_field["attestation_digest"]
        != sha256_digest(
            canonical_json(
                {
                    name: field_value
                    for name, field_value in redis_by_field.items()
                    if name != "attestation_digest"
                }
            )
        )
    ):
        raise PTGWaveMaterializedPreclaimConflict("fresh V13 Redis attestation digest is invalid")


def _retained_slot_map(
    retained_slots: object,
    runtime_image_identity: object,
) -> dict[int, str]:
    if not isinstance(retained_slots, list) or not retained_slots:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 failure proof requires a nonempty retained Pod subset"
        )
    pod_uid_by_slot: dict[int, str] = {}
    for retained_slot in retained_slots:
        if (
            not isinstance(retained_slot, Mapping)
            or set(retained_slot) != _RETAINED_SLOT_FIELDS
        ):
            raise PTGWaveMaterializedPreclaimConflict("fresh V13 retained Pod fields are invalid")
        slot, pod_uid = retained_slot.get("slot"), retained_slot.get("pod_uid")
        termination = retained_slot.get("termination")
        if (
            type(slot) is not int
            or slot not in range(PTG_WAVE_SLOT_COUNT)
            or slot in pod_uid_by_slot
            or not isinstance(pod_uid, str)
            or not pod_uid
            or retained_slot.get("phase") != "Failed"
            or retained_slot.get("runtime_image_identity") != runtime_image_identity
            or not isinstance(termination, Mapping)
            or set(termination) != _TERMINATION_FIELDS
            or not isinstance(termination.get("container_id"), str)
            or not termination["container_id"]
            or termination.get("reason") != "Error"
            or type(termination.get("exit_code")) is not int
            or termination.get("exit_code") != 1
            or not _is_timestamp(termination.get("started_at"))
            or not _is_timestamp(termination.get("finished_at"))
        ):
            raise PTGWaveMaterializedPreclaimConflict("fresh V13 retained Pod identity is invalid")
        pod_uid_by_slot[slot] = pod_uid
    return pod_uid_by_slot


def _has_exact_conditions(value: object) -> bool:
    if not isinstance(value, list) or len(value) != 2:
        return False
    if [condition.get("type") if isinstance(condition, Mapping) else None for condition in value] != [
        "Failed",
        "FailureTarget",
    ]:
        return False
    for condition in value:
        if (
            not isinstance(condition, Mapping)
            or set(condition) != _CONDITION_FIELDS
            or condition.get("status") != "True"
            or condition.get("reason") != "BackoffLimitExceeded"
            or condition.get("message") != "Job has reached the specified backoff limit"
            or not _is_timestamp(condition.get("last_probe_time"))
            or not _is_timestamp(condition.get("last_transition_time"))
        ):
            return False
    return True


def _is_timestamp(value: object) -> bool:
    if not isinstance(value, str) or _TIME.fullmatch(value) is None:
        return False
    try:
        dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return False
    return True


def _is_exact_json_value(value: object, expected: object) -> bool:
    """Compare JSON scalars without Python's boolean/integer coercion."""

    if expected is None:
        return value is None
    if type(expected) in {bool, int}:
        return type(value) is type(expected) and value == expected
    return value == expected
