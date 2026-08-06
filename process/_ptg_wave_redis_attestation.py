# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed-size worker identities and canonical release attestation."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from process._ptg_wave_redis_manifest import validate_ptg_small_wave_manifest
from process._ptg_wave_redis_reference import validate_ptg_small_wave_reference
from process._ptg_wave_redis_models import (
    PTG_SMALL_WAVE_SLOTS,
    WAVE_SCHEMA_VERSION,
    PTGSmallWaveAttestationError,
    PTGSmallWaveBarrierReceipt,
    PTGSmallWaveManifest,
    PTGSmallWaveReceipt,
    PTGSmallWaveReference,
    PTGSmallWaveSlotIdentity,
    PTGSmallWaveValidationError,
    as_optional_bytes,
    as_text,
    canonical_json_bytes,
    decode_job_count,
    encode_job_count,
    require_digest,
    require_identity,
    require_pinned_image_identity,
    require_protocol_identity,
    require_runtime_image_identity,
    require_wave_id,
    sha256_hex,
)


def parse_ptg_small_wave_ready_slots(
    reference: PTGSmallWaveReference,
    redis_entries: Any,
    *,
    exact: bool,
) -> tuple[PTGSmallWaveSlotIdentity, ...]:
    """Parse and validate the bounded ready hash for one worker reference."""

    if not isinstance(redis_entries, Mapping):
        raise PTGSmallWaveAttestationError(
            "ready slot registry has an invalid Redis shape"
        )
    entries_by_slot = {
        as_text(redis_field): ready_bytes
        for redis_field, ready_bytes in redis_entries.items()
    }
    expected_fields = {str(slot) for slot in PTG_SMALL_WAVE_SLOTS}
    actual_fields = set(entries_by_slot)
    if not actual_fields.issubset(expected_fields):
        raise PTGSmallWaveAttestationError(
            "ready slot registry has an unexpected slot"
        )
    if exact and actual_fields != expected_fields:
        raise PTGSmallWaveAttestationError(
            "ready slot registry is not exactly slots 0 through 11"
        )
    identities = tuple(
        _slot_identity_from_entry(slot_field, entries_by_slot[slot_field])
        for slot_field in sorted(actual_fields, key=int)
    )
    _validate_ready_identities(reference, identities)
    return identities


def _slot_identity_from_entry(
    slot_field: str,
    identity_bytes: Any,
) -> PTGSmallWaveSlotIdentity:
    identity_mapping = _parse_canonical_json_mapping(
        identity_bytes,
        "ready slot identity",
    )
    identity = _slot_identity_from_mapping(identity_mapping)
    if identity.slot != int(slot_field):
        raise PTGSmallWaveAttestationError(
            "ready slot field does not match its identity"
        )
    return identity


def _validate_ready_identities(
    reference: PTGSmallWaveReference,
    identities: tuple[PTGSmallWaveSlotIdentity, ...],
) -> None:
    if any(not _is_identity_for_reference(reference, identity) for identity in identities):
        raise PTGSmallWaveAttestationError(
            "ready slot identity belongs to another wave"
        )
    pod_uids = [identity.pod_uid for identity in identities]
    if len(set(pod_uids)) != len(pod_uids):
        raise PTGSmallWaveAttestationError(
            "ready slot registry repeats a pod_uid"
        )


def _slot_identity_from_mapping(
    identity_mapping: Mapping[str, Any],
) -> PTGSmallWaveSlotIdentity:
    expected_keys = {
        "config_identity",
        "image_identity",
        "kubernetes_manifest_identity",
        "runtime_image_identity",
        "job_count",
        "jobs_digest",
        "manifest_digest",
        "pod_uid",
        "protocol_identity",
        "queue_name",
        "slot",
        "serializer_identity",
        "runtime_identity_digest",
        "wave_id",
    }
    if set(identity_mapping) != expected_keys:
        raise PTGSmallWaveAttestationError(
            "ready slot identity fields are not exact"
        )
    slot = identity_mapping["slot"]
    if not isinstance(slot, int) or isinstance(slot, bool) or slot not in PTG_SMALL_WAVE_SLOTS:
        raise PTGSmallWaveAttestationError(
            "ready slot identity has an invalid slot"
        )
    try:
        return _validated_slot_identity(slot, identity_mapping)
    except PTGSmallWaveValidationError as exc:
        raise PTGSmallWaveAttestationError(
            "ready slot identity is invalid"
        ) from exc


def _validated_slot_identity(
    slot: int,
    identity_mapping: Mapping[str, Any],
) -> PTGSmallWaveSlotIdentity:
    return PTGSmallWaveSlotIdentity(
        slot=slot,
        pod_uid=require_identity("pod_uid", identity_mapping["pod_uid"]),
        config_identity=require_digest(
            "config_identity",
            identity_mapping["config_identity"],
        ),
        kubernetes_manifest_identity=require_digest(
            "kubernetes_manifest_identity",
            identity_mapping["kubernetes_manifest_identity"],
        ),
        image_identity=require_pinned_image_identity(
            identity_mapping["image_identity"]
        ),
        runtime_image_identity=require_runtime_image_identity(
            identity_mapping["runtime_image_identity"]
        ),
        runtime_identity_digest=require_digest(
            "runtime_identity_digest",
            identity_mapping["runtime_identity_digest"],
        ),
        wave_id=require_wave_id(identity_mapping["wave_id"]),
        manifest_digest=require_digest(
            "manifest_digest",
            identity_mapping["manifest_digest"],
        ),
        queue_name=require_identity("queue_name", identity_mapping["queue_name"]),
        jobs_digest=require_digest(
            "jobs_digest",
            identity_mapping["jobs_digest"],
        ),
        job_count=decode_job_count(identity_mapping["job_count"]),
        protocol_identity=require_protocol_identity(
            "protocol_identity",
            identity_mapping["protocol_identity"],
        ),
        serializer_identity=require_protocol_identity(
            "serializer_identity",
            identity_mapping["serializer_identity"],
        ),
    )


def build_ptg_small_wave_receipt(
    manifest: PTGSmallWaveManifest,
    ready_slots: tuple[PTGSmallWaveSlotIdentity, ...],
) -> PTGSmallWaveReceipt:
    """Build the canonical O(1)-size release for one exact manifest."""

    reference = manifest.reference
    _validate_release_ready_slots(reference, ready_slots)
    ready_slots_digest = sha256_hex(
        canonical_json_bytes([identity.as_mapping() for identity in ready_slots])
    )
    release_mapping = {
        "schema_version": WAVE_SCHEMA_VERSION,
        "wave_id": manifest.wave_id,
        "queue_name": manifest.queue_name,
        "manifest_digest": manifest.manifest_digest,
        "jobs_digest": manifest.jobs_digest,
        "job_count": encode_job_count(len(manifest.jobs)),
        "protocol_identity": manifest.protocol_identity,
        "serializer_identity": manifest.serializer_identity,
        "config_identity": manifest.config_identity,
        "kubernetes_manifest_identity": manifest.kubernetes_manifest_identity,
        "image_identity": manifest.image_identity,
        "runtime_image_identity": manifest.runtime_image_identity,
        "runtime_identity_digest": manifest.runtime_identity_digest,
        "ready_slots": [identity.as_mapping() for identity in ready_slots],
        "ready_slots_digest": ready_slots_digest,
    }
    release_payload = canonical_json_bytes(release_mapping)
    return PTGSmallWaveReceipt(
        wave_id=manifest.wave_id,
        queue_name=manifest.queue_name,
        manifest_digest=manifest.manifest_digest,
        jobs_digest=manifest.jobs_digest,
        job_count=len(manifest.jobs),
        protocol_identity=manifest.protocol_identity,
        serializer_identity=manifest.serializer_identity,
        config_identity=reference.config_identity,
        kubernetes_manifest_identity=reference.kubernetes_manifest_identity,
        image_identity=reference.image_identity,
        runtime_image_identity=reference.runtime_image_identity,
        runtime_identity_digest=reference.runtime_identity_digest,
        ready_slots=ready_slots,
        ready_slots_digest=ready_slots_digest,
        release_digest=sha256_hex(release_payload),
        release_payload=release_payload,
    )


def validate_ptg_small_wave_barrier_release(
    reference: PTGSmallWaveReference,
    registration: PTGSmallWaveSlotIdentity,
    release_scalar: Any,
) -> PTGSmallWaveBarrierReceipt:
    """Validate a canonical release using only fixed-size worker state."""

    validate_ptg_small_wave_reference(reference)
    if not isinstance(registration, PTGSmallWaveSlotIdentity):
        raise PTGSmallWaveAttestationError(
            "slot registration is not a PTGSmallWaveSlotIdentity"
        )
    if not _is_identity_for_reference(reference, registration):
        raise PTGSmallWaveAttestationError(
            "slot registration does not match the worker reference"
        )
    release_mapping, ready_slots = _release_components(reference, release_scalar)
    if registration not in ready_slots:
        raise PTGSmallWaveAttestationError(
            "release receipt excludes this slot identity"
        )
    return PTGSmallWaveBarrierReceipt(
        wave_id=reference.wave_id,
        queue_name=reference.queue_name,
        manifest_digest=reference.manifest_digest,
        jobs_digest=reference.jobs_digest,
        job_count=reference.job_count,
        protocol_identity=reference.protocol_identity,
        serializer_identity=reference.serializer_identity,
        config_identity=reference.config_identity,
        kubernetes_manifest_identity=reference.kubernetes_manifest_identity,
        image_identity=reference.image_identity,
        runtime_image_identity=reference.runtime_image_identity,
        runtime_identity_digest=reference.runtime_identity_digest,
        ready_slots_digest=release_mapping["ready_slots_digest"],
        release_digest=sha256_hex(as_optional_bytes(release_scalar) or b""),
    )


def parse_ptg_small_wave_controller_release(
    manifest: PTGSmallWaveManifest,
    release_scalar: Any,
) -> PTGSmallWaveReceipt:
    """Validate one GET result against the exact controller manifest."""

    validate_ptg_small_wave_manifest(manifest)
    _, ready_slots = _release_components(manifest.reference, release_scalar)
    receipt = build_ptg_small_wave_receipt(manifest, ready_slots)
    if as_optional_bytes(release_scalar) != receipt.release_payload:
        raise PTGSmallWaveAttestationError(
            "release receipt is not canonical or is tampered"
        )
    return receipt


def validate_ptg_small_wave_release_scalar(
    reference: PTGSmallWaveReference,
    release_scalar: Any,
) -> str:
    """Validate a release scalar and return its exact digest."""

    _release_components(reference, release_scalar)
    return sha256_hex(as_optional_bytes(release_scalar) or b"")


def _release_components(
    reference: PTGSmallWaveReference,
    release_scalar: Any,
) -> tuple[Mapping[str, Any], tuple[PTGSmallWaveSlotIdentity, ...]]:
    release_mapping = _parse_canonical_json_mapping(
        release_scalar,
        "release receipt",
    )
    _validate_release_mapping(reference, release_mapping)
    ready_sequence = release_mapping["ready_slots"]
    if not isinstance(ready_sequence, list):
        raise PTGSmallWaveAttestationError(
            "release receipt ready slots are invalid"
        )
    ready_slots = tuple(
        _slot_identity_from_mapping(identity_mapping)
        for identity_mapping in ready_sequence
        if isinstance(identity_mapping, Mapping)
    )
    if len(ready_slots) != len(ready_sequence):
        raise PTGSmallWaveAttestationError(
            "release receipt ready slots are invalid"
        )
    _validate_release_ready_slots(reference, ready_slots)
    expected_digest = sha256_hex(
        canonical_json_bytes([identity.as_mapping() for identity in ready_slots])
    )
    if release_mapping["ready_slots_digest"] != expected_digest:
        raise PTGSmallWaveAttestationError(
            "release receipt ready slot digest is invalid"
        )
    return release_mapping, ready_slots


def _validate_release_mapping(
    reference: PTGSmallWaveReference,
    release_mapping: Mapping[str, Any],
) -> None:
    expected_keys = {
        "schema_version",
        "wave_id",
        "queue_name",
        "manifest_digest",
        "jobs_digest",
        "job_count",
        "protocol_identity",
        "serializer_identity",
        "config_identity",
        "kubernetes_manifest_identity",
        "image_identity",
        "runtime_image_identity",
        "runtime_identity_digest",
        "ready_slots",
        "ready_slots_digest",
    }
    if set(release_mapping) != expected_keys:
        raise PTGSmallWaveAttestationError(
            "release receipt fields are not exact"
        )
    if release_mapping["schema_version"] != WAVE_SCHEMA_VERSION:
        raise PTGSmallWaveAttestationError(
            "release receipt schema version is invalid"
        )
    is_reference_match = (
        release_mapping["wave_id"] == reference.wave_id
        and release_mapping["queue_name"] == reference.queue_name
        and release_mapping["manifest_digest"] == reference.manifest_digest
        and release_mapping["jobs_digest"] == reference.jobs_digest
        and decode_job_count(release_mapping["job_count"]) == reference.job_count
        and release_mapping["protocol_identity"] == reference.protocol_identity
        and release_mapping["serializer_identity"] == reference.serializer_identity
        and release_mapping["config_identity"] == reference.config_identity
        and release_mapping["kubernetes_manifest_identity"]
        == reference.kubernetes_manifest_identity
        and release_mapping["image_identity"] == reference.image_identity
        and release_mapping["runtime_image_identity"]
        == reference.runtime_image_identity
        and release_mapping["runtime_identity_digest"]
        == reference.runtime_identity_digest
    )
    if not is_reference_match:
        raise PTGSmallWaveAttestationError(
            "release receipt does not match this wave reference"
        )


def _validate_release_ready_slots(
    reference: PTGSmallWaveReference,
    ready_slots: tuple[PTGSmallWaveSlotIdentity, ...],
) -> None:
    if tuple(identity.slot for identity in ready_slots) != PTG_SMALL_WAVE_SLOTS:
        raise PTGSmallWaveAttestationError(
            "release requires exactly ready slots 0 through 11"
        )
    if any(not _is_identity_for_reference(reference, identity) for identity in ready_slots):
        raise PTGSmallWaveAttestationError(
            "release ready slots do not bind this wave reference"
        )
    if len({identity.pod_uid for identity in ready_slots}) != len(ready_slots):
        raise PTGSmallWaveAttestationError(
            "release ready slots repeat a pod_uid"
        )
    if len({identity.config_identity for identity in ready_slots}) != 1:
        raise PTGSmallWaveAttestationError(
            "release ready slots do not share one config_identity"
        )
    if len({identity.image_identity for identity in ready_slots}) != 1:
        raise PTGSmallWaveAttestationError(
            "release ready slots do not share one image_identity"
        )
    if len({identity.runtime_image_identity for identity in ready_slots}) != 1:
        raise PTGSmallWaveAttestationError(
            "release ready slots do not share one runtime_image_identity"
        )


def _is_identity_for_reference(
    reference: PTGSmallWaveReference,
    identity: PTGSmallWaveSlotIdentity,
) -> bool:
    return (
        identity.wave_id == reference.wave_id
        and identity.manifest_digest == reference.manifest_digest
        and identity.queue_name == reference.queue_name
        and identity.jobs_digest == reference.jobs_digest
        and identity.job_count == reference.job_count
        and identity.protocol_identity == reference.protocol_identity
        and identity.serializer_identity == reference.serializer_identity
        and identity.config_identity == reference.config_identity
        and identity.kubernetes_manifest_identity
        == reference.kubernetes_manifest_identity
        and identity.image_identity == reference.image_identity
        and identity.runtime_image_identity == reference.runtime_image_identity
        and identity.runtime_identity_digest == reference.runtime_identity_digest
    )


def _parse_canonical_json_mapping(
    redis_scalar: Any,
    label: str,
) -> Mapping[str, Any]:
    raw_bytes = as_optional_bytes(redis_scalar)
    if raw_bytes is None:
        raise PTGSmallWaveAttestationError(f"{label} is missing")
    try:
        parsed_mapping = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PTGSmallWaveAttestationError(
            f"{label} is not valid JSON"
        ) from exc
    if not isinstance(parsed_mapping, Mapping):
        raise PTGSmallWaveAttestationError(f"{label} is not canonical")
    if canonical_json_bytes(parsed_mapping) != raw_bytes:
        raise PTGSmallWaveAttestationError(f"{label} is not canonical")
    return parsed_mapping
