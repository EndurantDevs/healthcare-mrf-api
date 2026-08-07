# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic edge contracts for exact PTG Redis wave protocol state."""


from __future__ import annotations


import json


import math


from dataclasses import replace


from types import SimpleNamespace


from unittest.mock import AsyncMock


import pytest


from arq.constants import in_progress_key_prefix, job_key_prefix, result_key_prefix


from redis.exceptions import ResponseError


from process import _ptg_wave_redis_attestation as attestation


from process import _ptg_wave_redis_barrier as barrier


from process import _ptg_wave_redis_cleanup as cleanup_module


from process import _ptg_wave_redis_encoding as encoding


from process import _ptg_wave_redis_manifest as manifest_module


from process import _ptg_wave_redis_models as redis_models


from process import _ptg_wave_redis_reference as reference_module


from process import _ptg_wave_redis_restore as restore_module


from process import _ptg_wave_redis_unclaimed as unclaimed


from process import _ptg_wave_redis_unclaimed_models as unclaimed_models


from process import ptg_wave_redis as redis_module


from process import ptg_wave_redis_adapter as adapter_module


from process import ptg_wave_worker_claim_adapter as claim_adapter


from process._ptg_wave_redis_cleanup_plan import (
    plan_ptg_small_wave_terminal_cleanup,
)


from process._ptg_wave_redis_models import (
    PTGSmallWaveAttestationError,
    PTGSmallWaveBarrierTimeout,
    PTGSmallWaveCleanupActiveError,
    PTGSmallWaveConflictError,
    PTGSmallWaveValidationError,
    canonical_json_bytes,
    sha256_hex,
)


from process._ptg_wave_redis_reference import (
    create_ptg_small_wave_slot_identity,
)


from process._ptg_wave_redis_unclaimed_validation import (
    present_ordinals,
    queued_ordinals,
    scalar_sequence,
    validate_released_partition,
    verified_job_ordinals,
)


from process.ptg_wave_redis import (
    PTGSmallWaveBarrierReceipt,
    publish_ptg_small_wave,
    register_ptg_small_wave_slot,
)


from tests.ptg_wave_redis_test_support import (
    FakeRedis,
    RUNTIME_IDENTITY,
    manifest as make_manifest,
    register_all,
)


def _slot_identity(wave_manifest, *, slot: int, pod_uid: str):
    return create_ptg_small_wave_slot_identity(
        wave_manifest.reference,
        slot=slot,
        pod_uid=pod_uid,
    )


def _slot_bytes(wave_manifest, *, slot: int, pod_uid: str) -> bytes:
    return canonical_json_bytes(
        _slot_identity(
            wave_manifest,
            slot=slot,
            pod_uid=pod_uid,
        ).as_mapping()
    )


async def _published_wave(count: int = 2):
    redis = FakeRedis()
    wave_manifest = make_manifest(count)
    await register_all(redis, wave_manifest)
    receipt = await publish_ptg_small_wave(redis, wave_manifest)
    return redis, wave_manifest, receipt


def _release_mapping(receipt) -> dict:
    return json.loads(receipt.release_payload)


def _reencoded_release(receipt, mutate) -> bytes:
    payload = _release_mapping(receipt)
    mutate(payload)
    return canonical_json_bytes(payload)


def _reorder_ready_slots(payload: dict) -> None:
    payload["ready_slots"] = list(reversed(payload["ready_slots"]))
    payload["ready_slots_digest"] = sha256_hex(
        canonical_json_bytes(payload["ready_slots"])
    )


def _adapter_identity(wave_manifest, **overrides):
    reference = wave_manifest.reference
    values_by_field = {
        "wave_digest": reference.wave_id,
        "queue": reference.queue_name,
        "worker_class": "process.PTGSmall",
        "slot_index": 0,
        "pod_uid": "pod-00",
        "manifest_digest": reference.manifest_digest,
        "jobs_digest": reference.jobs_digest,
        "job_count": reference.job_count,
        "config_identity": reference.config_identity,
        "manifest_identity": reference.kubernetes_manifest_identity,
        "image_identity": reference.image_identity,
        "runtime_image_identity": reference.runtime_image_identity,
    }
    values_by_field.update(overrides)
    return SimpleNamespace(**values_by_field)
