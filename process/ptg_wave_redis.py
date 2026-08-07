# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact Redis/ARQ publication and worker-barrier primitives for PTG waves.

Controllers hold the immutable ordered job manifest. Worker startup receives
only a fixed-size wave reference. Every queued value uses the public ARQ
serializer and key constants used by ``ArqRedis.enqueue_job``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from arq.constants import (
    in_progress_key_prefix,
    job_key_prefix,
    result_key_prefix,
    retry_key_prefix,
)
from redis.exceptions import WatchError

from process._ptg_wave_redis_attestation import (
    build_ptg_small_wave_receipt,
    parse_ptg_small_wave_controller_release,
    parse_ptg_small_wave_ready_slots,
    validate_ptg_small_wave_barrier_release,
    validate_ptg_small_wave_release_scalar,
)
from process._ptg_wave_redis_barrier import (
    register_ptg_small_wave_slot,
    register_ptg_small_wave_slot_and_wait,
    wait_for_ptg_small_wave_release,
)
from process._ptg_wave_redis_cleanup import (
    attest_ptg_wave_post_cleanup,
    attest_ptg_wave_pre_cleanup,
    attest_ptg_small_wave_unclaimed_failure_redis,
    attest_ptg_small_wave_unclaimed_failure_redis_post_cleanup,
    cleanup_ptg_small_wave_unclaimed_failure_redis,
    cleanup_ptg_small_wave_terminal_state,
    plan_ptg_small_wave_terminal_cleanup,
)
from process._ptg_wave_redis_manifest import (
    attest_arq_job_bytes,
    bind_ptg_small_wave_runtime_identity,
    build_ptg_small_wave_manifest,
    validate_ptg_small_wave_manifest,
)
from process._ptg_wave_redis_models import (
    PTG_SMALL_WAVE_FUNCTION,
    PTG_SMALL_WAVE_MAX_JOB_COUNT,
    PTG_SMALL_WAVE_PROTOCOL_IDENTITY,
    PTG_SMALL_WAVE_QUEUE_PREFIX,
    PTG_SMALL_WAVE_SERIALIZER_IDENTITY,
    PTG_SMALL_WAVE_SLOT_COUNT,
    PTG_SMALL_WAVE_SLOTS,
    PTG_SMALL_WAVE_WORKER_CLASS,
    PTGSmallWaveAttestationError,
    PTGSmallWaveBarrierReceipt,
    PTGSmallWaveBarrierTimeout,
    PTGSmallWaveCleanupActiveError,
    PTGSmallWaveCleanupPlan,
    PTGSmallWaveCleanupReceipt,
    PTGSmallWaveConflictError,
    PTGSmallWaveError,
    PTGSmallWaveJob,
    PTGSmallWaveManifest,
    PTGSmallWavePostCleanupAttestation,
    PTGSmallWavePreCleanupAttestation,
    PTGSmallWaveReadiness,
    PTGSmallWaveReceipt,
    PTGSmallWaveReference,
    PTGSmallWaveRuntimeIdentity,
    PTGSmallWaveSlotIdentity,
    PTGSmallWaveUnclaimedFailureRedisAttestation,
    PTGSmallWaveUnclaimedFailureRedisCleanupReceipt,
    PTGSmallWaveUnclaimedFailureRedisPostCleanupAttestation,
    PTGSmallWaveValidationError,
    as_optional_bytes,
    as_text,
    sha256_hex,
    wave_queue_name,
)
from process._ptg_wave_redis_reference import validate_ptg_small_wave_reference
from process._ptg_wave_redis_restore import restore_ptg_small_wave_manifest


@dataclass(frozen=True)
class _WaveRedisKeys:
    """Exact ARQ lifecycle keys derived from one immutable manifest."""

    job_keys: tuple[str, ...]
    result_keys: tuple[str, ...]
    retry_keys: tuple[str, ...]
    in_progress_keys: tuple[str, ...]


@dataclass(frozen=True)
class _PublicationGuard:
    """Watched state read before entering the publication transaction."""

    ready_entries: Mapping[Any, Any]
    release_scalar: Any
    has_existing_jobs: bool
    has_existing_results: bool
    has_existing_retries: bool
    has_existing_in_progress: bool
    queue_entries: Sequence[Any]


@dataclass(frozen=True)
class _AttestationSnapshot:
    """One atomic read of every pre-consumption wave key."""

    ready_entries: Any
    release_scalar: Any
    job_scalars: Any
    result_scalars: Any
    retry_scalars: Any
    in_progress_scalars: Any
    queue_entries: Any


async def publish_ptg_small_wave(
    redis: Any,
    manifest: PTGSmallWaveManifest,
) -> PTGSmallWaveReceipt:
    """Atomically publish every manifest job and its O(1) release, or none."""

    validate_ptg_small_wave_manifest(manifest)
    redis_keys = _wave_redis_keys(manifest)
    try:
        async with redis.pipeline(transaction=True) as pipe:
            await pipe.watch(*_publication_watch_keys(manifest, redis_keys))
            guard = await _read_publication_guard(pipe, manifest, redis_keys)
            _reject_existing_publication_state(guard)
            ready_slots = parse_ptg_small_wave_ready_slots(
                manifest.reference,
                guard.ready_entries,
                exact=True,
            )
            receipt = build_ptg_small_wave_receipt(manifest, ready_slots)
            pipe.multi()
            _queue_atomic_publication(pipe, manifest, receipt)
            await pipe.execute()
            return receipt
    except WatchError as exc:
        raise PTGSmallWaveConflictError(
            "Redis changed while publishing the exact wave; no retry was attempted"
        ) from exc


def _wave_redis_keys(manifest: PTGSmallWaveManifest) -> _WaveRedisKeys:
    return _WaveRedisKeys(
        job_keys=tuple(job_key_prefix + job.job_id for job in manifest.jobs),
        result_keys=tuple(result_key_prefix + job.job_id for job in manifest.jobs),
        retry_keys=tuple(retry_key_prefix + job.job_id for job in manifest.jobs),
        in_progress_keys=tuple(
            in_progress_key_prefix + job.job_id for job in manifest.jobs
        ),
    )


def _publication_watch_keys(
    manifest: PTGSmallWaveManifest,
    redis_keys: _WaveRedisKeys,
) -> tuple[str, ...]:
    return (
        manifest.ready_key,
        manifest.release_key,
        manifest.queue_name,
        *redis_keys.job_keys,
        *redis_keys.result_keys,
        *redis_keys.retry_keys,
        *redis_keys.in_progress_keys,
    )


async def _read_publication_guard(
    pipe: Any,
    manifest: PTGSmallWaveManifest,
    redis_keys: _WaveRedisKeys,
) -> _PublicationGuard:
    ready_entries = await pipe.hgetall(manifest.ready_key)
    release_scalar = await pipe.get(manifest.release_key)
    existing_jobs = await pipe.exists(*redis_keys.job_keys)
    existing_results = await pipe.exists(*redis_keys.result_keys)
    existing_retries = await pipe.exists(*redis_keys.retry_keys)
    existing_in_progress = await pipe.exists(*redis_keys.in_progress_keys)
    queue_entries = await pipe.zrange(
        manifest.queue_name,
        0,
        -1,
        withscores=True,
    )
    return _PublicationGuard(
        ready_entries=ready_entries,
        release_scalar=release_scalar,
        has_existing_jobs=bool(existing_jobs),
        has_existing_results=bool(existing_results),
        has_existing_retries=bool(existing_retries),
        has_existing_in_progress=bool(existing_in_progress),
        queue_entries=queue_entries,
    )


def _reject_existing_publication_state(guard: _PublicationGuard) -> None:
    if guard.release_scalar is not None:
        raise PTGSmallWaveValidationError("wave release key already exists")
    if guard.has_existing_jobs:
        raise PTGSmallWaveValidationError(
            "one or more deterministic job keys already exist"
        )
    if guard.has_existing_results:
        raise PTGSmallWaveValidationError(
            "one or more deterministic result keys already exist"
        )
    if guard.has_existing_retries:
        raise PTGSmallWaveValidationError(
            "one or more deterministic retry keys already exist"
        )
    if guard.has_existing_in_progress:
        raise PTGSmallWaveValidationError(
            "one or more deterministic in-progress keys already exist"
        )
    if guard.queue_entries:
        raise PTGSmallWaveValidationError(
            "dedicated wave queue already contains entries"
        )


def _queue_atomic_publication(
    pipe: Any,
    manifest: PTGSmallWaveManifest,
    receipt: PTGSmallWaveReceipt,
) -> None:
    for job in manifest.jobs:
        pipe.set(job_key_prefix + job.job_id, job.serialized_job)
    pipe.zadd(
        manifest.queue_name,
        {job.job_id: job.score_ms for job in manifest.jobs},
    )
    pipe.set(manifest.release_key, receipt.release_payload)
    pipe.publish(manifest.release_channel, receipt.release_payload)


async def attest_ptg_small_wave(
    redis: Any,
    manifest: PTGSmallWaveManifest,
) -> PTGSmallWaveReceipt:
    """Atomically attest the complete published, pre-consumption wave state."""

    validate_ptg_small_wave_manifest(manifest)
    redis_keys = _wave_redis_keys(manifest)
    snapshot = await _read_attestation_snapshot(redis, manifest, redis_keys)
    ready_slots = parse_ptg_small_wave_ready_slots(
        manifest.reference,
        snapshot.ready_entries,
        exact=True,
    )
    receipt = build_ptg_small_wave_receipt(manifest, ready_slots)
    if as_optional_bytes(snapshot.release_scalar) != receipt.release_payload:
        raise PTGSmallWaveAttestationError(
            "release receipt is missing or tampered"
        )
    _attest_job_lifecycle(manifest, snapshot)
    _attest_queue_membership(manifest, snapshot.queue_entries)
    return receipt


async def _read_attestation_snapshot(
    redis: Any,
    manifest: PTGSmallWaveManifest,
    redis_keys: _WaveRedisKeys,
) -> _AttestationSnapshot:
    async with redis.pipeline(transaction=True) as pipe:
        pipe.hgetall(manifest.ready_key)
        pipe.get(manifest.release_key)
        pipe.mget(redis_keys.job_keys)
        pipe.mget(redis_keys.result_keys)
        pipe.mget(redis_keys.retry_keys)
        pipe.mget(redis_keys.in_progress_keys)
        pipe.zrange(manifest.queue_name, 0, -1, withscores=True)
        snapshot_parts = await pipe.execute()
    return _AttestationSnapshot(*snapshot_parts)


def _attest_job_lifecycle(
    manifest: PTGSmallWaveManifest,
    snapshot: _AttestationSnapshot,
) -> None:
    lifecycle_sequences = (
        ("job", snapshot.job_scalars),
        ("result", snapshot.result_scalars),
        ("retry", snapshot.retry_scalars),
        ("in-progress", snapshot.in_progress_scalars),
    )
    for label, scalars in lifecycle_sequences:
        if not isinstance(scalars, Sequence) or len(scalars) != len(manifest.jobs):
            raise PTGSmallWaveAttestationError(
                f"ARQ {label} key read was incomplete"
            )
    for ordinal, job in enumerate(manifest.jobs):
        _attest_one_job_lifecycle(job, ordinal, snapshot)


def _attest_one_job_lifecycle(
    job: PTGSmallWaveJob,
    ordinal: int,
    snapshot: _AttestationSnapshot,
) -> None:
    for label, scalars in (
        ("result", snapshot.result_scalars),
        ("retry", snapshot.retry_scalars),
        ("in-progress", snapshot.in_progress_scalars),
    ):
        if scalars[ordinal] is not None:
            raise PTGSmallWaveAttestationError(
                f"unexpected ARQ {label} exists for job ordinal {job.ordinal}"
            )
    stored_job = as_optional_bytes(snapshot.job_scalars[ordinal])
    if stored_job is None:
        raise PTGSmallWaveAttestationError(
            f"ARQ job payload is missing or tampered for ordinal {job.ordinal}"
        )
    attest_arq_job_bytes(job, stored_job)


def _attest_queue_membership(
    manifest: PTGSmallWaveManifest,
    queue_entries: Any,
) -> None:
    if not isinstance(queue_entries, Sequence):
        raise PTGSmallWaveAttestationError(
            "dedicated queue read has an invalid shape"
        )
    actual_score_by_job_id = {
        as_text(job_id): int(score)
        for job_id, score in queue_entries
    }
    expected_score_by_job_id = {
        job.job_id: job.score_ms
        for job in manifest.jobs
    }
    if (
        len(actual_score_by_job_id) != len(queue_entries)
        or actual_score_by_job_id != expected_score_by_job_id
    ):
        raise PTGSmallWaveAttestationError(
            "dedicated queue membership is missing, extra, or tampered"
        )


async def read_ptg_small_wave_release(
    redis: Any,
    manifest: PTGSmallWaveManifest,
) -> PTGSmallWaveReceipt:
    """Reconcile an ambiguous EXEC using exactly one release-key GET."""

    validate_ptg_small_wave_manifest(manifest)
    release_scalar = await redis.get(manifest.release_key)
    return parse_ptg_small_wave_controller_release(manifest, release_scalar)


async def inspect_ptg_small_wave_readiness(
    redis: Any,
    reference: PTGSmallWaveReference,
) -> PTGSmallWaveReadiness:
    """Inspect only this wave's ready hash and release key without mutation."""

    validate_ptg_small_wave_reference(reference)
    async with redis.pipeline(transaction=True) as pipe:
        pipe.hgetall(reference.ready_key)
        pipe.get(reference.release_key)
        ready_entries, release_scalar = await pipe.execute()
    registered_slots = parse_ptg_small_wave_ready_slots(
        reference,
        ready_entries,
        exact=False,
    )
    registered_numbers = {identity.slot for identity in registered_slots}
    missing_slots = tuple(
        slot for slot in PTG_SMALL_WAVE_SLOTS if slot not in registered_numbers
    )
    config_identity = _uniform_identity(registered_slots, "config_identity")
    kubernetes_manifest_identity = _uniform_identity(
        registered_slots,
        "kubernetes_manifest_identity",
    )
    image_identity = _uniform_identity(registered_slots, "image_identity")
    runtime_image_identity = _uniform_identity(
        registered_slots,
        "runtime_image_identity",
    )
    is_ready = not missing_slots and all(
        identity is not None
        for identity in (
            config_identity,
            kubernetes_manifest_identity,
            image_identity,
            runtime_image_identity,
        )
    )
    is_released = release_scalar is not None
    release_digest = None
    if is_released:
        release_digest = validate_ptg_small_wave_release_scalar(
            reference,
            release_scalar,
        )
    return PTGSmallWaveReadiness(
        reference=reference,
        registered_slots=registered_slots,
        missing_slots=missing_slots,
        config_identity=config_identity,
        kubernetes_manifest_identity=kubernetes_manifest_identity,
        image_identity=image_identity,
        runtime_image_identity=runtime_image_identity,
        ready=is_ready,
        released=is_released,
        release_digest=release_digest,
    )


def _uniform_identity(
    registered_slots: tuple[PTGSmallWaveSlotIdentity, ...],
    attribute: str,
) -> str | None:
    identities = {getattr(identity, attribute) for identity in registered_slots}
    return next(iter(identities)) if len(identities) == 1 else None


__all__ = [
    "PTG_SMALL_WAVE_FUNCTION",
    "PTG_SMALL_WAVE_MAX_JOB_COUNT",
    "PTG_SMALL_WAVE_PROTOCOL_IDENTITY",
    "PTG_SMALL_WAVE_QUEUE_PREFIX",
    "PTG_SMALL_WAVE_SERIALIZER_IDENTITY",
    "PTG_SMALL_WAVE_SLOT_COUNT",
    "PTG_SMALL_WAVE_SLOTS",
    "PTG_SMALL_WAVE_WORKER_CLASS",
    "PTGSmallWaveAttestationError",
    "PTGSmallWaveBarrierReceipt",
    "PTGSmallWaveBarrierTimeout",
    "PTGSmallWaveCleanupActiveError",
    "PTGSmallWaveCleanupPlan",
    "PTGSmallWaveCleanupReceipt",
    "PTGSmallWaveConflictError",
    "PTGSmallWaveError",
    "PTGSmallWaveJob",
    "PTGSmallWaveManifest",
    "PTGSmallWavePostCleanupAttestation",
    "PTGSmallWavePreCleanupAttestation",
    "PTGSmallWaveReadiness",
    "PTGSmallWaveReceipt",
    "PTGSmallWaveReference",
    "PTGSmallWaveRuntimeIdentity",
    "PTGSmallWaveSlotIdentity",
    "PTGSmallWaveUnclaimedFailureRedisAttestation",
    "PTGSmallWaveUnclaimedFailureRedisCleanupReceipt",
    "PTGSmallWaveUnclaimedFailureRedisPostCleanupAttestation",
    "PTGSmallWaveValidationError",
    "attest_ptg_small_wave",
    "attest_ptg_wave_post_cleanup",
    "attest_ptg_wave_pre_cleanup",
    "attest_ptg_small_wave_unclaimed_failure_redis",
    "attest_ptg_small_wave_unclaimed_failure_redis_post_cleanup",
    "bind_ptg_small_wave_runtime_identity",
    "build_ptg_small_wave_manifest",
    "cleanup_ptg_small_wave_terminal_state",
    "cleanup_ptg_small_wave_unclaimed_failure_redis",
    "inspect_ptg_small_wave_readiness",
    "plan_ptg_small_wave_terminal_cleanup",
    "publish_ptg_small_wave",
    "read_ptg_small_wave_release",
    "register_ptg_small_wave_slot",
    "register_ptg_small_wave_slot_and_wait",
    "restore_ptg_small_wave_manifest",
    "validate_ptg_small_wave_barrier_release",
    "validate_ptg_small_wave_reference",
    "wait_for_ptg_small_wave_release",
    "wave_queue_name",
]
