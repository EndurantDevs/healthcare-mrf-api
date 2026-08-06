"""Pure validation for exact Redis release receipts."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from process._ptg_wave_redis_encoding import runtime_identity_digest


@dataclass(frozen=True)
class _ReleaseContext:
    ready_slots: list[dict[str, Any]]
    kubernetes_slots: list[dict[str, Any]]
    runtime_digest: str


def validate_release_receipt(
    wave: Any,
    receipt: object,
    *,
    conflict_type: type[Exception],
    canonical_json: Callable[[Any], bytes],
    sha256_digest: Callable[[bytes], str],
    digest_validator: Callable[[object, str], str],
) -> dict[str, Any]:
    """Validate a release against the persisted exact execution identity."""

    receipt_map = _exact_receipt_map(receipt, conflict_type)
    context = _release_context(wave, receipt_map, conflict_type)
    _validate_ready_slots(wave, context, conflict_type)
    _validate_release_digests(
        wave,
        receipt_map,
        context,
        conflict_type=conflict_type,
        canonical_json=canonical_json,
        sha256_digest=sha256_digest,
        digest_validator=digest_validator,
    )
    return receipt_map


def _exact_receipt_map(
    receipt: object,
    conflict_type: type[Exception],
) -> dict[str, Any]:
    if not isinstance(receipt, dict):
        raise conflict_type("Redis release receipt must be an object")
    expected_receipt_fields = {
        "wave_digest", "release_queue", "redis_manifest_digest", "jobs_digest",
        "job_count", "protocol_identity", "serializer_identity",
        "manifest_identity", "config_identity", "pinned_image_reference",
        "pinned_image_digest", "runtime_image_identity", "runtime_identity_digest",
        "ready_slots", "ready_slots_digest", "release_digest",
    }
    if set(receipt) != expected_receipt_fields:
        raise conflict_type("Redis release receipt fields are not exact")
    return receipt


def _release_context(
    wave: Any,
    receipt: dict[str, Any],
    conflict_type: type[Exception],
) -> _ReleaseContext:
    expected_receipt_field_map = {
        "wave_digest": wave.wave_digest,
        "release_queue": wave.release_queue,
        "redis_manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest,
        "job_count": wave.intent_count,
        "protocol_identity": wave.protocol_identity,
        "serializer_identity": wave.serializer_identity,
        "manifest_identity": wave.kubernetes_manifest_identity,
        "config_identity": wave.kubernetes_config_identity,
        "pinned_image_reference": wave.pinned_image_reference,
        "pinned_image_digest": wave.pinned_image_digest,
        "runtime_image_identity": wave.runtime_image_identity,
    }
    if any(
        receipt[name] != expected_value
        for name, expected_value in expected_receipt_field_map.items()
    ):
        raise conflict_type(
            "Redis release receipt does not bind the admitted execution"
        )
    expected_runtime_digest = runtime_identity_digest(
        wave.kubernetes_config_identity,
        wave.kubernetes_manifest_identity,
        wave.pinned_image_reference,
        wave.runtime_image_identity,
    )
    if receipt["runtime_identity_digest"] != expected_runtime_digest:
        raise conflict_type(
            "Redis release runtime identity digest does not bind the wave"
        )
    ready_slots = receipt["ready_slots"]
    kubernetes_slots = (wave.kubernetes_ready_attestation or {}).get("slots")
    if not isinstance(ready_slots, list) or len(ready_slots) != 12:
        raise conflict_type("Redis release must contain exactly 12 ready slots")
    if not isinstance(kubernetes_slots, list) or len(kubernetes_slots) != 12:
        raise conflict_type("Redis release lacks persisted Kubernetes readiness")
    return _ReleaseContext(ready_slots, kubernetes_slots, expected_runtime_digest)


def _validate_ready_slots(
    wave: Any,
    context: _ReleaseContext,
    conflict_type: type[Exception],
) -> None:
    expected_slot_fields = {
        "config_identity", "kubernetes_manifest_identity", "image_identity",
        "runtime_image_identity", "runtime_identity_digest", "manifest_digest",
        "pod_uid", "queue_name", "slot", "wave_id", "jobs_digest", "job_count",
        "protocol_identity", "serializer_identity",
    }
    expected_pod_uid_by_slot = {
        ready_slot_entry["slot"]: ready_slot_entry["pod_uid"]
        for ready_slot_entry in context.kubernetes_slots
    }
    seen_slots: set[int] = set()
    for slot_receipt in context.ready_slots:
        if not isinstance(slot_receipt, dict) or set(slot_receipt) != expected_slot_fields:
            raise conflict_type("Redis ready-slot receipt fields are not exact")
        slot = slot_receipt["slot"]
        expected_slot_field_map = _expected_slot_map(
            wave,
            context.runtime_digest,
        )
        if (
            not isinstance(slot, int) or isinstance(slot, bool) or slot not in range(12)
            or slot in seen_slots
            or slot_receipt["pod_uid"] != expected_pod_uid_by_slot.get(slot)
            or any(
                slot_receipt[name] != expected_value
                for name, expected_value in expected_slot_field_map.items()
            )
        ):
            raise conflict_type(
                "Redis ready slots differ from Kubernetes readiness"
            )
        seen_slots.add(slot)
    if seen_slots != set(range(12)):
        raise conflict_type("Redis ready slots must cover indexes 0 through 11")


def _expected_slot_map(wave: Any, runtime_digest: str) -> dict[str, Any]:
    return {
        "config_identity": wave.kubernetes_config_identity,
        "kubernetes_manifest_identity": wave.kubernetes_manifest_identity,
        "image_identity": wave.pinned_image_reference,
        "runtime_image_identity": wave.runtime_image_identity,
        "runtime_identity_digest": runtime_digest,
        "manifest_digest": wave.manifest_digest,
        "queue_name": wave.release_queue,
        "wave_id": wave.wave_digest,
        "jobs_digest": wave.jobs_digest,
        "job_count": str(wave.intent_count),
        "protocol_identity": wave.protocol_identity,
        "serializer_identity": wave.serializer_identity,
    }


def _validate_release_digests(
    wave: Any,
    receipt: dict[str, Any],
    context: _ReleaseContext,
    *,
    conflict_type: type[Exception],
    canonical_json: Callable[[Any], bytes],
    sha256_digest: Callable[[bytes], str],
    digest_validator: Callable[[object, str], str],
) -> None:
    ready_slots_digest = sha256_digest(canonical_json(context.ready_slots))
    if receipt["ready_slots_digest"] != ready_slots_digest:
        raise conflict_type("Redis ready-slot digest is invalid")
    release_mapping = {
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
        "runtime_identity_digest": context.runtime_digest,
        "ready_slots": context.ready_slots,
        "ready_slots_digest": ready_slots_digest,
    }
    digest_validator(receipt["release_digest"], "Redis release digest")
    if receipt["release_digest"] != sha256_digest(canonical_json(release_mapping)):
        raise conflict_type("Redis release digest is invalid")
