# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace

import pytest

from process.ptg_wave_redis import (
    PTG_SMALL_WAVE_SLOTS,
    PTGSmallWaveValidationError,
    publish_ptg_small_wave,
    register_ptg_small_wave_slot,
)
from process.ptg_wave_redis_adapter import create_ptg_wave_redis_barrier
from tests.ptg_wave_redis_test_support import FakeRedis, manifest as make_manifest


def _worker_identity(wave_manifest, *, slot: int = 0, pod_uid: str = "pod-00"):
    reference = wave_manifest.reference
    return SimpleNamespace(
        wave_digest=reference.wave_id,
        queue=reference.queue_name,
        worker_class="process.PTGSmall",
        slot_index=slot,
        pod_uid=pod_uid,
        manifest_digest=reference.manifest_digest,
        jobs_digest=reference.jobs_digest,
        job_count=reference.job_count,
        config_identity=reference.config_identity,
        manifest_identity=reference.kubernetes_manifest_identity,
        image_identity=reference.image_identity,
        runtime_image_identity=reference.runtime_image_identity,
    )


async def _barrier_for(redis: FakeRedis, identity: SimpleNamespace):
    async def pool_factory(_settings):
        return redis

    return await create_ptg_wave_redis_barrier(
        identity,
        pool_factory=pool_factory,
        settings_factory=object,
    )


@pytest.mark.asyncio
async def test_adapter_returns_exact_kubernetes_release_and_closes_pool() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(13)
    identity = _worker_identity(wave_manifest)
    for slot in PTG_SMALL_WAVE_SLOTS[1:]:
        await register_ptg_small_wave_slot(
            redis,
            wave_manifest.reference,
            slot=slot,
            pod_uid=f"pod-{slot:02d}",
        )

    barrier = await _barrier_for(redis, identity)
    registered = await barrier.register_ready(identity)
    published = await publish_ptg_small_wave(redis, wave_manifest)
    released = await barrier.wait_for_release(identity)

    assert registered["slot"] == 0
    assert released == {
        "released": True,
        "wave_digest": published.wave_id,
        "queue": published.queue_name,
        "worker_class": "process.PTGSmall",
        "manifest_digest": published.manifest_digest,
        "jobs_digest": published.jobs_digest,
        "job_count": published.job_count,
        "config_identity": published.config_identity,
        "manifest_identity": published.kubernetes_manifest_identity,
        "image_identity": published.image_identity,
        "runtime_image_identity": published.runtime_image_identity,
    }
    assert released["image_identity"].rsplit(":", 1)[-1] != released[
        "runtime_image_identity"
    ].split(":", 1)[-1]
    assert redis.aclose_calls == 1


@pytest.mark.asyncio
async def test_adapter_closes_pool_when_identity_changes_after_factory() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(1)
    identity = _worker_identity(wave_manifest)
    barrier = await _barrier_for(redis, identity)
    changed_identity = SimpleNamespace(**vars(identity))
    changed_identity.config_identity = "f" * 64

    with pytest.raises(PTGSmallWaveValidationError, match="differs"):
        await barrier.register_ready(changed_identity)

    assert redis.aclose_calls == 1
    assert barrier.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "attribute,replacement,error_pattern",
    [
        ("image_identity", "sha256:" + "f" * 64, "pinned"),
        (
            "runtime_image_identity",
            "registry.example/ptg-worker@sha256:" + "f" * 64,
            "canonical sha256",
        ),
    ],
)
async def test_adapter_keeps_pinned_and_runtime_image_formats_distinct(
    attribute: str,
    replacement: str,
    error_pattern: str,
) -> None:
    wave_manifest = make_manifest(1)
    identity = _worker_identity(wave_manifest)
    setattr(identity, attribute, replacement)

    with pytest.raises(PTGSmallWaveValidationError, match=error_pattern):
        await create_ptg_wave_redis_barrier(identity)
