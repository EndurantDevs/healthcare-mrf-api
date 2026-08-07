# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Worker registration and Pub/Sub release waiting for exact PTG waves."""

from __future__ import annotations

import asyncio
import math
import time
from typing import Any

from redis.exceptions import WatchError

from process._ptg_wave_redis_attestation import (
    parse_ptg_small_wave_ready_slots,
    validate_ptg_small_wave_barrier_release,
)
from process._ptg_wave_redis_models import (
    PTG_SMALL_WAVE_SLOT_COUNT,
    PTGSmallWaveAttestationError,
    PTGSmallWaveBarrierReceipt,
    PTGSmallWaveBarrierTimeout,
    PTGSmallWaveConflictError,
    PTGSmallWaveReference,
    PTGSmallWaveSlotIdentity,
    PTGSmallWaveValidationError,
    canonical_json_bytes,
)
from process._ptg_wave_redis_reference import (
    create_ptg_small_wave_slot_identity,
    validate_ptg_small_wave_reference,
)


_SLOT_REGISTRATION_MAX_ATTEMPTS = PTG_SMALL_WAVE_SLOT_COUNT + 4


async def register_ptg_small_wave_slot(
    redis: Any,
    reference: PTGSmallWaveReference,
    *,
    slot: int,
    pod_uid: str,
) -> PTGSmallWaveSlotIdentity:
    """Register one explicit slot without discovering platform state."""

    validate_ptg_small_wave_reference(reference)
    registration = create_ptg_small_wave_slot_identity(
        reference,
        slot=slot,
        pod_uid=pod_uid,
    )
    attempt = 0
    while True:
        try:
            return await _register_slot_once(redis, reference, registration)
        except WatchError as exc:
            if (attempt := attempt + 1) == _SLOT_REGISTRATION_MAX_ATTEMPTS:
                raise PTGSmallWaveConflictError(
                    "Redis kept changing while registering a wave slot"
                ) from exc
            await asyncio.sleep(0)


async def _register_slot_once(
    redis: Any,
    reference: PTGSmallWaveReference,
    registration: PTGSmallWaveSlotIdentity,
) -> PTGSmallWaveSlotIdentity:
    async with redis.pipeline(transaction=True) as pipe:
        await pipe.watch(reference.ready_key, reference.release_key)
        ready_entries = await pipe.hgetall(reference.ready_key)
        release_scalar = await pipe.get(reference.release_key)
        existing_slots = parse_ptg_small_wave_ready_slots(
            reference,
            ready_entries,
            exact=False,
        )
        if _is_registration_complete(
            reference,
            registration,
            existing_slots,
            release_scalar,
        ):
            return registration
        pipe.multi()
        pipe.hset(
            reference.ready_key,
            str(registration.slot),
            canonical_json_bytes(registration.as_mapping()),
        )
        await pipe.execute()
        return registration


def _is_registration_complete(
    reference: PTGSmallWaveReference,
    registration: PTGSmallWaveSlotIdentity,
    existing_slots: tuple[PTGSmallWaveSlotIdentity, ...],
    release_scalar: Any,
) -> bool:
    existing_by_slot = {
        identity.slot: identity
        for identity in existing_slots
    }
    current_registration = existing_by_slot.get(registration.slot)
    if release_scalar is not None:
        validate_ptg_small_wave_barrier_release(
            reference,
            registration,
            release_scalar,
        )
        if current_registration != registration:
            raise PTGSmallWaveValidationError(
                "release exists without this exact slot registration"
            )
        return True
    if current_registration is not None:
        if current_registration != registration:
            raise PTGSmallWaveValidationError(
                f"slot {registration.slot} is already registered by a different identity"
            )
        return True
    if any(
        identity.pod_uid == registration.pod_uid
        for identity in existing_slots
    ):
        raise PTGSmallWaveValidationError(
            "pod_uid may register exactly one wave slot"
        )
    return False


async def await_ptg_small_wave_slot_release(
    redis: Any,
    reference: PTGSmallWaveReference,
    *,
    slot: int,
    pod_uid: str,
    timeout_seconds: float | None = None,
) -> PTGSmallWaveBarrierReceipt:
    """Register one slot and wait only for its matching release channel."""

    _validate_timeout(timeout_seconds)
    registration = await register_ptg_small_wave_slot(
        redis,
        reference,
        slot=slot,
        pod_uid=pod_uid,
    )
    return await wait_for_ptg_small_wave_release(
        redis,
        reference,
        registration,
        timeout_seconds=timeout_seconds,
    )


async def wait_for_ptg_small_wave_release(
    redis: Any,
    reference: PTGSmallWaveReference,
    registration: PTGSmallWaveSlotIdentity,
    *,
    timeout_seconds: float | None = None,
) -> PTGSmallWaveBarrierReceipt:
    """Wait for release after an exact slot registration already exists."""

    _validate_timeout(timeout_seconds)
    pubsub = redis.pubsub()
    try:
        await pubsub.subscribe(reference.release_channel)
        release_scalar = await redis.get(reference.release_key)
        if release_scalar is not None:
            return validate_ptg_small_wave_barrier_release(
                reference,
                registration,
                release_scalar,
            )
        return await _wait_for_release_notification(
            redis,
            pubsub,
            reference,
            registration,
            timeout_seconds,
        )
    finally:
        await pubsub.unsubscribe(reference.release_channel)
        await pubsub.aclose()


def _validate_timeout(timeout_seconds: float | None) -> None:
    if timeout_seconds is None:
        return
    if (
        not isinstance(timeout_seconds, (int, float))
        or isinstance(timeout_seconds, bool)
    ):
        raise PTGSmallWaveValidationError(
            "timeout_seconds must be a number or None"
        )
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise PTGSmallWaveValidationError(
            "timeout_seconds must be positive and finite"
        )


async def _wait_for_release_notification(
    redis: Any,
    pubsub: Any,
    reference: PTGSmallWaveReference,
    registration: PTGSmallWaveSlotIdentity,
    timeout_seconds: float | None,
) -> PTGSmallWaveBarrierReceipt:
    deadline = None if timeout_seconds is None else time.monotonic() + timeout_seconds
    while True:
        remaining = None if deadline is None else max(deadline - time.monotonic(), 0.0)
        if remaining == 0.0:
            raise PTGSmallWaveBarrierTimeout(
                "matching wave release did not arrive before timeout"
            )
        message = await pubsub.get_message(
            ignore_subscribe_messages=True,
            timeout=remaining,
        )
        if message is None:
            raise PTGSmallWaveBarrierTimeout(
                "matching wave release did not arrive before timeout"
            )
        if message.get("type") != "message":
            continue
        return await _validate_notified_release(
            redis,
            reference,
            registration,
            message.get("data"),
        )


async def _validate_notified_release(
    redis: Any,
    reference: PTGSmallWaveReference,
    registration: PTGSmallWaveSlotIdentity,
    release_scalar: Any,
) -> PTGSmallWaveBarrierReceipt:
    receipt = validate_ptg_small_wave_barrier_release(
        reference,
        registration,
        release_scalar,
    )
    stored_release = await redis.get(reference.release_key)
    stored_receipt = validate_ptg_small_wave_barrier_release(
        reference,
        registration,
        stored_release,
    )
    if stored_receipt.release_digest != receipt.release_digest:
        raise PTGSmallWaveAttestationError(
            "release notification and release key differ"
        )
    return receipt


register_ptg_small_wave_slot_and_wait = await_ptg_small_wave_slot_release
