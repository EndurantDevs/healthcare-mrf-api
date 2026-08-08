# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Duck-typed Redis barrier factory for the PTG Kubernetes wave worker."""

from __future__ import annotations

import asyncio
import copy
import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from arq import create_pool
from redis.exceptions import (
    AuthenticationError,
    AuthorizationError,
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
)

from process._ptg_wave_redis_barrier import (
    register_ptg_small_wave_slot,
    wait_for_ptg_small_wave_release,
)
from process._ptg_wave_redis_models import (
    PTG_SMALL_WAVE_PROTOCOL_IDENTITY,
    PTG_SMALL_WAVE_SERIALIZER_IDENTITY,
    PTG_SMALL_WAVE_WORKER_CLASS,
    PTGSmallWaveBarrierReceipt,
    PTGSmallWaveReference,
    PTGSmallWaveSlotIdentity,
    PTGSmallWaveValidationError,
    runtime_identity_digest,
)
from process._ptg_wave_redis_reference import validate_ptg_small_wave_reference
from process.redis_config import build_redis_settings


_REDIS_STARTUP_ATTEMPTS = 5
_REDIS_STARTUP_ATTEMPT_TIMEOUT_SECONDS = 3.0
_REDIS_STARTUP_RETRY_DELAY_SECONDS = 3.0
_REDIS_STARTUP_SLOT_STAGGER_SECONDS = 0.25


@dataclass
class PTGSmallWaveRedisBarrier:
    """One worker slot's short-lived Redis registration/release client."""

    redis_pool: Any
    reference: PTGSmallWaveReference
    slot: int
    pod_uid: str
    worker_class: str
    registration: PTGSmallWaveSlotIdentity | None = None
    is_closed: bool = False

    async def register_ready(self, identity: Any) -> Mapping[str, Any]:
        """Register this exact duck-typed Kubernetes worker identity."""

        try:
            _validate_adapter_identity(self, identity)
            if self.registration is None:
                self.registration = await register_ptg_small_wave_slot(
                    self.redis_pool,
                    self.reference,
                    slot=self.slot,
                    pod_uid=self.pod_uid,
                )
            return self.registration.as_mapping()
        except BaseException:
            await self.aclose()
            raise

    async def wait_for_release(self, identity: Any) -> Mapping[str, Any]:
        """Wait for the exact release and return the Kubernetes mapping."""

        try:
            _validate_adapter_identity(self, identity)
            if self.registration is None:
                raise PTGSmallWaveValidationError(
                    "wave slot must register before waiting for release"
                )
            receipt = await wait_for_ptg_small_wave_release(
                self.redis_pool,
                self.reference,
                self.registration,
            )
            return _kubernetes_release_mapping(receipt, self.worker_class)
        finally:
            await self.aclose()

    async def aclose(self) -> None:
        """Close the owned Redis pool exactly once."""

        if self.is_closed:
            return
        self.is_closed = True
        close_pool = getattr(self.redis_pool, "aclose", None)
        if close_pool is None:
            close_pool = getattr(self.redis_pool, "close", None)
        if close_pool is None:
            return
        close_result = close_pool()
        if inspect.isawaitable(close_result):
            await close_result


async def create_ptg_wave_redis_barrier(
    identity: Any,
    *,
    pool_factory: Callable[..., Any] = create_pool,
    settings_factory: Callable[[], Any] = build_redis_settings,
    sleep: Callable[[float], Any] = asyncio.sleep,
) -> PTGSmallWaveRedisBarrier:
    """Create the environment-configured Redis barrier for one worker slot."""

    reference = _reference_from_identity(identity)
    slot = _identity_attribute(identity, "slot_index")
    pod_uid = _identity_attribute(identity, "pod_uid")
    worker_class = _identity_attribute(identity, "worker_class")
    redis_pool = await create_ptg_wave_redis_pool(
        identity,
        pool_factory=pool_factory,
        settings_factory=settings_factory,
        sleep=sleep,
    )
    return PTGSmallWaveRedisBarrier(
        redis_pool=redis_pool,
        reference=reference,
        slot=slot,
        pod_uid=pod_uid,
        worker_class=worker_class,
    )


async def create_ptg_wave_redis_pool(
    identity: Any,
    *,
    pool_factory: Callable[..., Any] = create_pool,
    settings_factory: Callable[[], Any] = build_redis_settings,
    sleep: Callable[[float], Any] = asyncio.sleep,
    pool_options: Mapping[str, Any] | None = None,
) -> Any:
    """Open one wave-local Redis pool with bounded deterministic startup retry."""

    slot = _identity_attribute(identity, "slot_index")
    await _sleep(sleep, _slot_stagger_seconds(slot))
    settings = _single_attempt_settings(settings_factory())
    options = {} if pool_options is None else dict(pool_options)
    for attempt_index in range(_REDIS_STARTUP_ATTEMPTS):
        try:
            return await _open_pool_once(pool_factory, settings, options)
        except (
            ConnectionError,
            TimeoutError,
            RedisConnectionError,
            RedisTimeoutError,
        ) as error:
            if isinstance(error, (AuthenticationError, AuthorizationError)):
                raise
            if attempt_index == _REDIS_STARTUP_ATTEMPTS - 1:
                raise
            await _sleep(sleep, _REDIS_STARTUP_RETRY_DELAY_SECONDS)
    raise AssertionError("bounded Redis startup loop did not terminate")


def _single_attempt_settings(settings: Any) -> Any:
    """Copy Redis settings so generic worker retry policy stays untouched."""

    wave_settings = copy.copy(settings)
    if hasattr(wave_settings, "conn_retries"):
        wave_settings.conn_retries = 0
    if hasattr(wave_settings, "conn_retry_delay"):
        wave_settings.conn_retry_delay = 0
    return wave_settings


async def _open_pool_once(
    pool_factory: Callable[..., Any],
    settings: Any,
    options: Mapping[str, Any],
) -> Any:
    pool = pool_factory(settings, **options)
    if not inspect.isawaitable(pool):
        return pool
    return await asyncio.wait_for(
        pool,
        timeout=_REDIS_STARTUP_ATTEMPT_TIMEOUT_SECONDS,
    )


async def _sleep(sleep: Callable[[float], Any], delay_seconds: float) -> None:
    result = sleep(delay_seconds)
    if inspect.isawaitable(result):
        await result


def _slot_stagger_seconds(slot: Any) -> float:
    if isinstance(slot, bool) or not isinstance(slot, int) or not 0 <= slot < 12:
        raise PTGSmallWaveValidationError("wave slot must be between zero and eleven")
    return slot * _REDIS_STARTUP_SLOT_STAGGER_SECONDS


def _reference_from_identity(identity: Any) -> PTGSmallWaveReference:
    config_identity = _identity_attribute(identity, "config_identity")
    manifest_identity = _identity_attribute(identity, "manifest_identity")
    image_identity = _identity_attribute(identity, "image_identity")
    runtime_image_identity = _identity_attribute(
        identity,
        "runtime_image_identity",
    )
    reference = PTGSmallWaveReference(
        wave_id=_identity_attribute(identity, "wave_digest"),
        queue_name=_identity_attribute(identity, "queue"),
        manifest_digest=_identity_attribute(identity, "manifest_digest"),
        jobs_digest=_identity_attribute(identity, "jobs_digest"),
        job_count=_identity_attribute(identity, "job_count"),
        protocol_identity=PTG_SMALL_WAVE_PROTOCOL_IDENTITY,
        serializer_identity=PTG_SMALL_WAVE_SERIALIZER_IDENTITY,
        config_identity=config_identity,
        kubernetes_manifest_identity=manifest_identity,
        image_identity=image_identity,
        runtime_image_identity=runtime_image_identity,
        runtime_identity_digest=runtime_identity_digest(
            config_identity,
            manifest_identity,
            image_identity,
            runtime_image_identity,
        ),
    )
    validate_ptg_small_wave_reference(reference)
    if _identity_attribute(identity, "worker_class") != PTG_SMALL_WAVE_WORKER_CLASS:
        raise PTGSmallWaveValidationError(
            "wave worker class must be process.PTGSmall"
        )
    return reference


def _validate_adapter_identity(
    barrier: PTGSmallWaveRedisBarrier,
    identity: Any,
) -> None:
    candidate_reference = _reference_from_identity(identity)
    if (
        candidate_reference != barrier.reference
        or _identity_attribute(identity, "slot_index") != barrier.slot
        or _identity_attribute(identity, "pod_uid") != barrier.pod_uid
        or _identity_attribute(identity, "worker_class") != barrier.worker_class
    ):
        raise PTGSmallWaveValidationError(
            "worker identity differs from the barrier factory identity"
        )
    if barrier.is_closed:
        raise PTGSmallWaveValidationError("wave Redis barrier is already closed")


def _identity_attribute(identity: Any, name: str) -> Any:
    try:
        return getattr(identity, name)
    except AttributeError as exc:
        raise PTGSmallWaveValidationError(
            f"wave worker identity is missing {name}"
        ) from exc


def _kubernetes_release_mapping(
    receipt: PTGSmallWaveBarrierReceipt,
    worker_class: str,
) -> dict[str, Any]:
    return {
        "released": True,
        "wave_digest": receipt.wave_id,
        "queue": receipt.queue_name,
        "worker_class": worker_class,
        "manifest_digest": receipt.manifest_digest,
        "jobs_digest": receipt.jobs_digest,
        "job_count": receipt.job_count,
        "config_identity": receipt.config_identity,
        "manifest_identity": receipt.kubernetes_manifest_identity,
        "image_identity": receipt.image_identity,
        "runtime_image_identity": receipt.runtime_image_identity,
    }


__all__ = [
    "PTGSmallWaveRedisBarrier",
    "create_ptg_wave_redis_pool",
    "create_ptg_wave_redis_barrier",
]
