"""Pure exactness tests for the twelve-slot Redis release receipt."""

from __future__ import annotations

import copy
import types

import pytest

from process._ptg_wave_redis_encoding import runtime_identity_digest
from process.ptg_wave_release_validation import validate_release_receipt
from process.ptg_wave_state import (
    PTGWaveStateConflict,
    canonical_json,
    sha256_digest,
)


_WAVE = "1" * 64
_MANIFEST = "2" * 64
_JOBS = "3" * 64
_CONFIG = "4" * 64
_IMAGE_DIGEST = "5" * 64
_IMAGE = "registry.example/engine@sha256:" + _IMAGE_DIGEST
_RUNTIME = "sha256:" + "6" * 64
_PROTOCOL = "healthporta.ptg-wave.v1"
_SERIALIZER = "arq-pickle-v1"


def _wave(**overrides):
    fields_by_field = {
        "wave_digest": _WAVE,
        "release_queue": f"arq:PTGSmall:wave:{_WAVE}",
        "manifest_digest": _MANIFEST,
        "jobs_digest": _JOBS,
        "intent_count": 17,
        "protocol_identity": _PROTOCOL,
        "serializer_identity": _SERIALIZER,
        "kubernetes_manifest_identity": "7" * 64,
        "kubernetes_config_identity": _CONFIG,
        "pinned_image_reference": _IMAGE,
        "pinned_image_digest": _IMAGE_DIGEST,
        "runtime_image_identity": _RUNTIME,
        "kubernetes_ready_attestation": {
            "slots": [
                {"slot": slot, "pod_uid": f"pod-{slot}"}
                for slot in range(12)
            ],
        },
    }
    fields_by_field.update(overrides)
    return types.SimpleNamespace(**fields_by_field)


def _ready_slots(wave):
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
            "pod_uid": f"pod-{slot}",
            "queue_name": wave.release_queue,
            "slot": slot,
            "wave_id": wave.wave_digest,
            "jobs_digest": wave.jobs_digest,
            "job_count": str(wave.intent_count),
            "protocol_identity": wave.protocol_identity,
            "serializer_identity": wave.serializer_identity,
        }
        for slot in range(12)
    ]


def _receipt(wave):
    ready_slots = _ready_slots(wave)
    runtime_digest = ready_slots[0]["runtime_identity_digest"]
    ready_slots_digest = sha256_digest(canonical_json(ready_slots))
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
        "runtime_identity_digest": runtime_digest,
        "ready_slots": ready_slots,
        "ready_slots_digest": ready_slots_digest,
    }
    return {
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
        "runtime_identity_digest": runtime_digest,
        "ready_slots": ready_slots,
        "ready_slots_digest": ready_slots_digest,
        "release_digest": sha256_digest(canonical_json(release_mapping)),
    }


def _digest(value, name):
    if not isinstance(value, str) or len(value) != 64:
        raise PTGWaveStateConflict(f"{name} must be a digest")
    return value


def _validate(wave, receipt):
    return validate_release_receipt(
        wave,
        receipt,
        conflict_type=PTGWaveStateConflict,
        canonical_json=canonical_json,
        sha256_digest=sha256_digest,
        digest_validator=_digest,
    )


def test_valid_release_binds_every_persisted_identity_and_slot():
    wave = _wave()
    receipt = _receipt(wave)
    assert _validate(wave, receipt) == receipt


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda receipt: receipt.pop("jobs_digest"), "fields are not exact"),
        (lambda receipt: receipt.__setitem__("jobs_digest", "8" * 64), "admitted execution"),
        (lambda receipt: receipt.__setitem__("runtime_identity_digest", "8" * 64), "runtime identity"),
        (lambda receipt: receipt.__setitem__("ready_slots", {}), "exactly 12"),
        (lambda receipt: receipt["ready_slots"][0].pop("pod_uid"), "fields are not exact"),
        (lambda receipt: receipt["ready_slots"][0].__setitem__("slot", True), "Kubernetes readiness"),
        (lambda receipt: receipt["ready_slots"][1].__setitem__("slot", 0), "Kubernetes readiness"),
        (lambda receipt: receipt["ready_slots"][0].__setitem__("pod_uid", "other"), "Kubernetes readiness"),
        (lambda receipt: receipt["ready_slots"][0].__setitem__("queue_name", "other"), "Kubernetes readiness"),
        (lambda receipt: receipt.__setitem__("ready_slots_digest", "8" * 64), "ready-slot digest"),
        (lambda receipt: receipt.__setitem__("release_digest", "short"), "must be a digest"),
        (lambda receipt: receipt.__setitem__("release_digest", "8" * 64), "release digest is invalid"),
    ],
)
def test_release_rejects_identity_shape_slot_and_digest_drift(mutate, message):
    wave = _wave()
    receipt = copy.deepcopy(_receipt(wave))
    mutate(receipt)
    with pytest.raises(PTGWaveStateConflict, match=message):
        _validate(wave, receipt)


def test_release_rejects_nonobjects_and_missing_kubernetes_readiness():
    wave = _wave()
    with pytest.raises(PTGWaveStateConflict, match="must be an object"):
        _validate(wave, [])

    wave.kubernetes_ready_attestation = None
    with pytest.raises(PTGWaveStateConflict, match="persisted Kubernetes readiness"):
        _validate(wave, _receipt(wave))


def test_release_requires_iteration_to_cover_all_twelve_slots():
    class ShortIterationList(list):
        def __iter__(self):
            return iter(list.__getitem__(self, slice(0, 11)))

    wave = _wave()
    receipt = _receipt(wave)
    receipt["ready_slots"] = ShortIterationList(receipt["ready_slots"])
    with pytest.raises(PTGWaveStateConflict, match="cover indexes"):
        _validate(wave, receipt)
