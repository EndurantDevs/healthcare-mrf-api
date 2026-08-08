# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from redis.exceptions import AuthenticationError, AuthorizationError, ResponseError

from process.ptg_wave_redis import (
    PTG_SMALL_WAVE_SLOTS,
    PTGSmallWaveValidationError,
    publish_ptg_small_wave,
    register_ptg_small_wave_slot,
)
from process.ptg_wave_redis_adapter import (
    _slot_stagger_seconds,
    create_ptg_wave_redis_barrier,
    create_ptg_wave_redis_pool,
)
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
        settings_factory=lambda: SimpleNamespace(
            conn_retries=10,
            conn_retry_delay=1,
        ),
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


def test_wave_redis_startup_stagger_is_exact_for_twelve_slots() -> None:
    assert [_slot_stagger_seconds(slot) for slot in PTG_SMALL_WAVE_SLOTS] == [
        slot / 4 for slot in PTG_SMALL_WAVE_SLOTS
    ]


@pytest.mark.asyncio
async def test_wave_redis_startup_recovers_with_bounded_single_attempts() -> None:
    identity = _worker_identity(make_manifest(1), slot=3)
    connection_attempts: list[None] = []
    sleep_delays: list[float] = []
    attempt_settings: list[SimpleNamespace] = []
    redis = FakeRedis()

    async def pool_factory(settings):
        connection_attempts.append(None)
        attempt_settings.append(settings)
        if len(connection_attempts) < 5:
            raise ConnectionRefusedError("synthetic startup refusal")
        return redis

    async def record_sleep(delay_seconds: float) -> None:
        sleep_delays.append(delay_seconds)

    opened = await create_ptg_wave_redis_pool(
        identity,
        pool_factory=pool_factory,
        settings_factory=lambda: SimpleNamespace(
            conn_retries=10,
            conn_retry_delay=1,
        ),
        sleep=record_sleep,
    )

    assert opened is redis
    assert len(connection_attempts) == 5
    assert sleep_delays == [0.75, 3.0, 3.0, 3.0, 3.0]
    assert all(settings.conn_retries == 0 for settings in attempt_settings)
    assert all(settings.conn_retry_delay == 0 for settings in attempt_settings)


@pytest.mark.asyncio
async def test_wave_redis_startup_exhaustion_is_terminal_and_bounded() -> None:
    identity = _worker_identity(make_manifest(1), slot=11)
    connection_attempts: list[None] = []
    sleep_delays: list[float] = []

    async def refuse(_settings):
        connection_attempts.append(None)
        raise ConnectionRefusedError("synthetic terminal refusal")

    async def record_sleep(delay_seconds: float) -> None:
        sleep_delays.append(delay_seconds)

    with pytest.raises(ConnectionRefusedError, match="terminal refusal"):
        await create_ptg_wave_redis_pool(
            identity,
            pool_factory=refuse,
            settings_factory=lambda: SimpleNamespace(
                conn_retries=10,
                conn_retry_delay=1,
            ),
            sleep=record_sleep,
        )

    assert len(connection_attempts) == 5
    assert sleep_delays == [2.75, 3.0, 3.0, 3.0, 3.0]


@pytest.mark.asyncio
async def test_wave_redis_startup_does_not_retry_programming_failures() -> None:
    identity = _worker_identity(make_manifest(1))
    connection_attempts: list[None] = []
    sleep_delays: list[float] = []

    def invalid_factory(_settings):
        connection_attempts.append(None)
        raise ValueError("synthetic invalid configuration")

    async def record_sleep(delay_seconds: float) -> None:
        sleep_delays.append(delay_seconds)

    with pytest.raises(ValueError, match="invalid configuration"):
        await create_ptg_wave_redis_pool(
            identity,
            pool_factory=invalid_factory,
            settings_factory=lambda: SimpleNamespace(
                conn_retries=10,
                conn_retry_delay=1,
            ),
            sleep=record_sleep,
        )

    assert len(connection_attempts) == 1
    assert sleep_delays == [0.0]


@pytest.mark.asyncio
async def test_wave_redis_startup_does_not_retry_permission_failures() -> None:
    identity = _worker_identity(make_manifest(1))
    connection_attempts: list[None] = []

    def deny_open(_settings):
        connection_attempts.append(None)
        raise PermissionError("synthetic permission failure")

    with pytest.raises(PermissionError, match="permission failure"):
        await create_ptg_wave_redis_pool(
            identity,
            pool_factory=deny_open,
            settings_factory=lambda: SimpleNamespace(
                conn_retries=10,
                conn_retry_delay=1,
            ),
            sleep=lambda _delay_seconds: None,
        )

    assert len(connection_attempts) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        AuthenticationError("synthetic authentication failure"),
        AuthorizationError("synthetic authorization failure"),
        ResponseError("synthetic response failure"),
    ],
)
async def test_wave_redis_startup_does_not_retry_permanent_redis_failures(
    failure: Exception,
) -> None:
    identity = _worker_identity(make_manifest(1))
    connection_attempts: list[None] = []
    sleep_delays: list[float] = []

    async def fail_once(_settings):
        connection_attempts.append(None)
        raise failure

    async def record_sleep(delay_seconds: float) -> None:
        sleep_delays.append(delay_seconds)

    with pytest.raises(type(failure), match="synthetic"):
        await create_ptg_wave_redis_pool(
            identity,
            pool_factory=fail_once,
            settings_factory=lambda: SimpleNamespace(
                conn_retries=10,
                conn_retry_delay=1,
            ),
            sleep=record_sleep,
        )

    assert len(connection_attempts) == 1
    assert sleep_delays == [0.0]


@pytest.mark.asyncio
async def test_wave_redis_startup_cancellation_during_stagger_skips_pool() -> None:
    identity = _worker_identity(make_manifest(1), slot=4)
    pool_factory_calls: list[None] = []

    async def cancel_sleep(_delay_seconds: float) -> None:
        raise asyncio.CancelledError

    def pool_factory(_settings):
        pool_factory_calls.append(None)
        return FakeRedis()

    with pytest.raises(asyncio.CancelledError):
        await create_ptg_wave_redis_pool(
            identity,
            pool_factory=pool_factory,
            settings_factory=lambda: SimpleNamespace(
                conn_retries=10,
                conn_retry_delay=1,
            ),
            sleep=cancel_sleep,
        )

    assert pool_factory_calls == []


@pytest.mark.asyncio
async def test_wave_redis_startup_cancellation_during_backoff_stops_retry() -> None:
    identity = _worker_identity(make_manifest(1))
    connection_attempts: list[None] = []
    sleep_delays: list[float] = []

    async def refuse(_settings):
        connection_attempts.append(None)
        raise ConnectionRefusedError("synthetic startup refusal")

    async def cancel_backoff(delay_seconds: float) -> None:
        sleep_delays.append(delay_seconds)
        if delay_seconds:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await create_ptg_wave_redis_pool(
            identity,
            pool_factory=refuse,
            settings_factory=lambda: SimpleNamespace(
                conn_retries=10,
                conn_retry_delay=1,
            ),
            sleep=cancel_backoff,
        )

    assert len(connection_attempts) == 1
    assert sleep_delays == [0.0, 3.0]


@pytest.mark.asyncio
async def test_wave_redis_startup_cancellation_stops_inflight_pool_open() -> None:
    identity = _worker_identity(make_manifest(1))
    connection_attempts: list[None] = []
    factory_started = asyncio.Event()
    factory_cancelled = asyncio.Event()

    async def block_until_cancelled(_settings):
        connection_attempts.append(None)
        factory_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            factory_cancelled.set()
            raise

    async def no_sleep(_delay_seconds: float) -> None:
        return None

    open_task = asyncio.create_task(
        create_ptg_wave_redis_pool(
            identity,
            pool_factory=block_until_cancelled,
            settings_factory=lambda: SimpleNamespace(
                conn_retries=10,
                conn_retry_delay=1,
            ),
            sleep=no_sleep,
        )
    )
    await factory_started.wait()
    open_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await open_task

    assert len(connection_attempts) == 1
    assert factory_cancelled.is_set()
