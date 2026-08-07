"""Pure Kubernetes, Redis, and linkage validation for wave failures."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from process._ptg_wave_redis_encoding import runtime_identity_digest
from process.ptg_wave_failure_types import (
    PTGWaveFailureConflict,
    _require_mapping,
    is_claimed_prestart_failure_receipt,
)
from process.ptg_wave_outcomes import _validate_linkage_ack
from process.ptg_wave_state import (
    PTGWaveStateConflict,
    canonical_json,
    sha256_digest,
)


@dataclass(frozen=True)
class FailureRedisOrdinals:
    """Canonical Redis lifecycle ordinal sets from one failure witness."""

    queued: set[int]
    jobs: set[int]
    results: set[int]
    retries: set[int]
    in_progress: set[int]


def _verify_linkage(
    wave: Any,
    outcomes: list[Any],
    *,
    key: str | bytes | None,
) -> None:
    ack = wave.linkage_ack
    if not isinstance(ack, dict) or wave.linkage_ack_digest is None:
        raise PTGWaveFailureConflict(
            "failure terminal proof requires linkage acknowledgement"
        )
    try:
        _, digest = _validate_linkage_ack(wave, outcomes, ack, key)
    except PTGWaveStateConflict as exc:
        raise PTGWaveFailureConflict(
            "failure linkage acknowledgement does not cover exact outcomes"
        ) from exc
    if digest != wave.linkage_ack_digest:
        raise PTGWaveFailureConflict(
            "failure linkage acknowledgement does not cover exact outcomes"
        )




def _verify_failure_redis(
    wave: Any,
    failure: dict[str, Any],
    receipt: object,
    *,
    require_release_absent: bool = False,
) -> dict[str, Any]:
    """Verify the first exact Redis failure witness against durable state."""

    receipt_map, is_claimed_prestart = _validate_redis_receipt_envelope(
        wave, failure, receipt
    )
    expected_ready_slots = _validate_redis_ready_membership(
        wave, failure, receipt_map, is_claimed_prestart
    )
    lifecycle = _failure_redis_lifecycle(wave, receipt_map)
    _validate_redis_release(
        wave,
        failure,
        receipt_map,
        expected_ready_slots,
        lifecycle,
        require_release_absent=require_release_absent,
    )
    if is_claimed_prestart:
        _validate_claimed_prestart_redis(wave, receipt_map, lifecycle)
    return receipt_map


def _validate_redis_receipt_envelope(
    wave: Any, failure: dict[str, Any], receipt: object
) -> tuple[dict[str, Any], bool]:
    receipt_map = _require_mapping(receipt, "failure Redis terminal receipt")
    is_claimed_prestart = is_claimed_prestart_failure_receipt(failure)
    if is_claimed_prestart and receipt_map != failure.get("redis_evidence"):
        raise PTGWaveFailureConflict(
            "claimed-prestart Redis receipt differs from its first attestation"
        )
    expected_fields = {
        "schema_version", "wave_id", "queue_name", "manifest_digest",
        "jobs_digest", "job_count", "target_key_count", "ready_slots",
        "ready_slots_digest", "release_present", "release_digest",
        "release_receipt", "queued_ordinals", "job_ordinals",
        "result_ordinals", "retry_ordinals", "in_progress_ordinals",
        "health_check_present", "attestation_digest",
    }
    if set(receipt_map) != expected_fields:
        raise PTGWaveFailureConflict(
            "failure Redis terminal receipt fields are not exact"
        )
    if not _is_redis_receipt_bound_to_wave(wave, receipt_map):
        raise PTGWaveFailureConflict(
            "failure Redis terminal receipt does not bind the wave"
        )
    if not isinstance(receipt_map["health_check_present"], bool):
        raise PTGWaveFailureConflict(
            "failure Redis health-check observation is invalid"
        )
    unsigned_evidence_map = {
        name: field_value
        for name, field_value in receipt_map.items()
        if name != "attestation_digest"
    }
    if receipt_map["attestation_digest"] != sha256_digest(
        canonical_json(unsigned_evidence_map)
    ):
        raise PTGWaveFailureConflict(
            "failure Redis attestation digest is invalid"
        )
    return receipt_map, is_claimed_prestart


def _is_redis_receipt_bound_to_wave(
    wave: Any, receipt: dict[str, Any]
) -> bool:
    return (
        receipt["schema_version"]
        == "healthporta.ptg-wave.redis-unclaimed-failure.v1"
        and receipt["wave_id"] == wave.wave_digest
        and receipt["queue_name"] == wave.release_queue
        and receipt["manifest_digest"] == wave.manifest_digest
        and receipt["jobs_digest"] == wave.jobs_digest
        and receipt["job_count"] == wave.intent_count
        and receipt["target_key_count"] == 4 + (4 * wave.intent_count)
    )


def _validate_redis_ready_membership(
    wave: Any,
    failure: dict[str, Any],
    receipt: dict[str, Any],
    is_claimed_prestart: bool,
) -> list[dict[str, Any]]:
    expected_ready_slots = _expected_redis_ready_slots(wave)
    observed_ready_slots = receipt["ready_slots"]
    is_partial_unreleased_preclaim = (
        not is_claimed_prestart
        and failure["reason"] == "pre_claim_failure"
        and failure["origin_state"] == "slots_waiting"
        and wave.redis_release_attestation is None
        and wave.redis_release_attestation_digest is None
    )
    if is_partial_unreleased_preclaim:
        _validate_partial_ready_membership(
            expected_ready_slots, observed_ready_slots
        )
    elif observed_ready_slots != expected_ready_slots:
        raise PTGWaveFailureConflict(
            "failure Redis ready membership differs from Kubernetes"
        )
    if receipt["ready_slots_digest"] != sha256_digest(
        canonical_json(observed_ready_slots)
    ):
        raise PTGWaveFailureConflict(
            "failure Redis ready membership digest is invalid"
        )
    return expected_ready_slots


def _validate_partial_ready_membership(
    expected_ready_slots: list[dict[str, Any]], observed_ready_slots: object
) -> None:
    if not isinstance(observed_ready_slots, list):
        raise PTGWaveFailureConflict(
            "failure Redis ready membership is invalid"
        )
    observed_slots = [
        ready_slot.get("slot") if isinstance(ready_slot, dict) else None
        for ready_slot in observed_ready_slots
    ]
    is_canonical_subset = (
        all(
            isinstance(slot, int) and not isinstance(slot, bool)
            for slot in observed_slots
        )
        and observed_slots == sorted(set(observed_slots))
        and observed_ready_slots
        == [
            expected_ready_slots[slot]
            for slot in observed_slots
            if slot in range(len(expected_ready_slots))
        ]
    )
    if not is_canonical_subset:
        raise PTGWaveFailureConflict(
            "unreleased pre-claim Redis ready membership is not a canonical Kubernetes subset"
        )


def _failure_redis_lifecycle(
    wave: Any, receipt: dict[str, Any]
) -> FailureRedisOrdinals:
    lifecycle = FailureRedisOrdinals(
        queued=_ordinal_set(receipt["queued_ordinals"], wave.intent_count, "queued"),
        jobs=_ordinal_set(receipt["job_ordinals"], wave.intent_count, "job"),
        results=_ordinal_set(
            receipt["result_ordinals"], wave.intent_count, "result"
        ),
        retries=_ordinal_set(
            receipt["retry_ordinals"], wave.intent_count, "retry"
        ),
        in_progress=_ordinal_set(
            receipt["in_progress_ordinals"], wave.intent_count, "in-progress"
        ),
    )
    if lifecycle.retries or lifecycle.in_progress:
        raise PTGWaveFailureConflict(
            "failure Redis receipt still contains active lifecycle state"
        )
    return lifecycle


def _validate_redis_release(
    wave: Any,
    failure: dict[str, Any],
    receipt: dict[str, Any],
    expected_ready_slots: list[dict[str, Any]],
    lifecycle: FailureRedisOrdinals,
    *,
    require_release_absent: bool,
) -> None:
    release_attestation = wave.redis_release_attestation or {}
    is_release_expected = wave.redis_release_attestation_digest is not None
    if require_release_absent or failure["reason"] in {
        "kubernetes_post_absent", "redis_release_absent"
    }:
        is_release_expected = False
    if receipt["release_present"] is not is_release_expected:
        raise PTGWaveFailureConflict(
            "failure Redis release presence conflicts with durable state"
        )
    if not is_release_expected:
        if (
            receipt["release_digest"] is not None
            or receipt["release_receipt"] is not None
            or lifecycle.queued
            or lifecycle.jobs
            or lifecycle.results
        ):
            raise PTGWaveFailureConflict(
                "unreleased Redis failure receipt is not absent"
            )
        return
    release_mapping = _expected_redis_release_mapping(
        wave, expected_ready_slots
    )
    if (
        receipt["release_digest"] != release_attestation.get("release_digest")
        or receipt["release_receipt"] != release_mapping
        or receipt["release_digest"]
        != sha256_digest(canonical_json(release_mapping))
        or lifecycle.jobs != lifecycle.queued
        or lifecycle.queued & lifecycle.results
        or lifecycle.queued | lifecycle.results
        != set(range(wave.intent_count))
    ):
        raise PTGWaveFailureConflict(
            "released Redis failure receipt differs from the first release"
        )


def _validate_claimed_prestart_redis(
    wave: Any,
    receipt: dict[str, Any],
    lifecycle: FailureRedisOrdinals,
) -> None:
    exact_ordinals = set(range(wave.intent_count))
    if (
        lifecycle.results
        or lifecycle.retries
        or lifecycle.in_progress
        or receipt["health_check_present"] is not False
        or lifecycle.queued != exact_ordinals
        or lifecycle.jobs != exact_ordinals
    ):
        raise PTGWaveFailureConflict(
            "claimed-prestart Redis receipt contains progress or active lifecycle state"
        )


def _ordinal_set(value: object, count: int, label: str) -> set[int]:
    if (
        not isinstance(value, list)
        or any(
            not isinstance(item, int) or isinstance(item, bool)
            for item in value
        )
        or value != sorted(set(value))
        or any(item not in range(count) for item in value)
    ):
        raise PTGWaveFailureConflict(
            f"failure Redis {label} ordinals are invalid"
        )
    return set(value)


def _expected_redis_ready_slots(wave: Any) -> list[dict[str, Any]]:
    ready_slots = (wave.kubernetes_ready_attestation or {}).get("slots")
    if ready_slots is None:
        return []
    if not isinstance(ready_slots, list) or len(ready_slots) != 12:
        raise PTGWaveFailureConflict(
            "failure wave lacks exact Kubernetes readiness"
        )
    runtime_digest = runtime_identity_digest(
        wave.kubernetes_config_identity,
        wave.kubernetes_manifest_identity,
        wave.pinned_image_reference,
        wave.runtime_image_identity,
    )
    return [
        {
            "config_identity": wave.kubernetes_config_identity,
            "kubernetes_manifest_identity": wave.kubernetes_manifest_identity,
            "image_identity": wave.pinned_image_reference,
            "runtime_image_identity": wave.runtime_image_identity,
            "runtime_identity_digest": runtime_digest,
            "manifest_digest": wave.manifest_digest,
            "pod_uid": ready_slot["pod_uid"],
            "queue_name": wave.release_queue,
            "slot": ready_slot["slot"],
            "wave_id": wave.wave_digest,
            "jobs_digest": wave.jobs_digest,
            "job_count": str(wave.intent_count),
            "protocol_identity": wave.protocol_identity,
            "serializer_identity": wave.serializer_identity,
        }
        for ready_slot in ready_slots
    ]


def _expected_redis_release_mapping(
    wave: Any,
    ready_slots: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "wave_id": wave.wave_digest,
        "queue_name": wave.release_queue,
        "manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest,
        "job_count": str(wave.intent_count),
        "protocol_identity": wave.protocol_identity,
        "serializer_identity": wave.serializer_identity,
        "config_identity": wave.kubernetes_config_identity,
        "kubernetes_manifest_identity": wave.kubernetes_manifest_identity,
        "image_identity": wave.pinned_image_reference,
        "runtime_image_identity": wave.runtime_image_identity,
        "runtime_identity_digest": runtime_identity_digest(
            wave.kubernetes_config_identity,
            wave.kubernetes_manifest_identity,
            wave.pinned_image_reference,
            wave.runtime_image_identity,
        ),
        "ready_slots": ready_slots,
        "ready_slots_digest": sha256_digest(canonical_json(ready_slots)),
    }
