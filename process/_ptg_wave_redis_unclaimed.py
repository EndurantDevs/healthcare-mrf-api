"""Exact Redis evidence and cleanup for waves with zero durable claims."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from redis.exceptions import ResponseError, WatchError

from process._ptg_wave_redis_attestation import (
    parse_ptg_small_wave_controller_release,
    parse_ptg_small_wave_ready_slots,
)
from process._ptg_wave_redis_cleanup_plan import (
    canonical_mapping_digest,
    plan_ptg_small_wave_terminal_cleanup,
)
from process._ptg_wave_redis_models import (
    PTGSmallWaveAttestationError,
    PTGSmallWaveCleanupActiveError,
    PTGSmallWaveCleanupPlan,
    PTGSmallWaveConflictError,
    PTGSmallWaveManifest,
    PTGSmallWaveReceipt,
    PTGSmallWaveSlotIdentity,
    PTGSmallWaveUnclaimedFailureRedisAttestation,
    PTGSmallWaveUnclaimedFailureRedisCleanupReceipt,
    PTGSmallWaveUnclaimedFailureRedisPostCleanupAttestation,
    as_optional_bytes,
    canonical_json_bytes,
    require_digest,
    sha256_hex,
)
from process._ptg_wave_redis_unclaimed_validation import (
    present_ordinals,
    queued_ordinals,
    scalar_sequence,
    validate_released_partition,
    verified_job_ordinals,
)


@dataclass(frozen=True)
class _UnclaimedFailureSnapshot:
    ready_entries: Any
    release_scalar: Any
    queue_entries: Any
    job_scalars: tuple[Any, ...]
    result_scalars: tuple[Any, ...]
    retry_scalars: tuple[Any, ...]
    in_progress_scalars: tuple[Any, ...]
    health_scalar: Any


@dataclass(frozen=True)
class _ReleaseEvidence:
    is_present: bool
    ready_slots: tuple[PTGSmallWaveSlotIdentity, ...]
    receipt: PTGSmallWaveReceipt | None
    digest: str | None


@dataclass(frozen=True)
class _LifecycleEvidence:
    queued_ordinals: tuple[int, ...]
    job_ordinals: tuple[int, ...]
    result_ordinals: tuple[int, ...]
    retry_ordinals: tuple[int, ...]
    in_progress_ordinals: tuple[int, ...]
    is_health_check_present: bool


async def attest_unclaimed_wave_redis(
    redis: Any,
    manifest: PTGSmallWaveManifest,
) -> PTGSmallWaveUnclaimedFailureRedisAttestation:
    """Prove all Redis state is safe for a no-claim failure transition."""

    plan = plan_ptg_small_wave_terminal_cleanup(manifest)
    try:
        snapshot = await _read_unclaimed_failure_snapshot(redis, plan)
    except ResponseError as exc:
        raise PTGSmallWaveAttestationError(
            "all-unclaimed Redis observation encountered an invalid key type"
        ) from exc
    return _attest_unclaimed_failure_snapshot(manifest, plan, snapshot)


async def cleanup_unclaimed_wave_redis(
    redis: Any,
    manifest: PTGSmallWaveManifest,
    *,
    expected_attestation_digest: str,
) -> PTGSmallWaveUnclaimedFailureRedisCleanupReceipt:
    """Delete exact keys after WATCH revalidates one durable witness."""

    expected_attestation_digest = require_digest(
        "expected_attestation_digest",
        expected_attestation_digest,
    )
    plan = plan_ptg_small_wave_terminal_cleanup(manifest)
    try:
        async with redis.pipeline(transaction=True) as pipe:
            await pipe.watch(*plan.target_keys)
            snapshot = await _read_watched_unclaimed_failure_snapshot(pipe, plan)
            attestation = _attest_unclaimed_failure_snapshot(
                manifest,
                plan,
                snapshot,
            )
            if attestation.attestation_digest != expected_attestation_digest:
                raise PTGSmallWaveAttestationError(
                    "all-unclaimed Redis state no longer matches its expected witness"
                )
            pipe.multi()
            pipe.delete(*plan.target_keys)
            deleted_key_count = int((await pipe.execute())[0])
    except WatchError as exc:
        raise PTGSmallWaveConflictError(
            "Redis changed during all-unclaimed cleanup; no retry was attempted"
        ) from exc
    except ResponseError as exc:
        raise PTGSmallWaveAttestationError(
            "all-unclaimed Redis cleanup encountered an invalid key type"
        ) from exc
    return PTGSmallWaveUnclaimedFailureRedisCleanupReceipt(
        wave_id=plan.wave_id,
        manifest_digest=plan.manifest_digest,
        target_key_count=len(plan.target_keys),
        deleted_key_count=deleted_key_count,
        expected_attestation_digest=expected_attestation_digest,
        attestation=attestation,
    )


async def attest_unclaimed_wave_redis_cleanup(
    redis: Any,
    manifest: PTGSmallWaveManifest,
    *,
    expected_attestation_digest: str,
) -> PTGSmallWaveUnclaimedFailureRedisPostCleanupAttestation:
    """Use GET only to prove exact all-unclaimed targets are absent."""

    expected_attestation_digest = require_digest(
        "expected_attestation_digest",
        expected_attestation_digest,
    )
    plan = plan_ptg_small_wave_terminal_cleanup(manifest)
    async with redis.pipeline(transaction=True) as pipe:
        for key in plan.target_keys:
            pipe.get(key)
        target_scalars = await pipe.execute(raise_on_error=False)
    # With `raise_on_error=False`, redis-py returns command errors as values in
    # this sequence instead of raising ResponseError from pipeline execution.
    # The exact-shape and scalar validators below reject those returned errors.
    # Keep that single protocol path as the fail-closed post-cleanup proof.
    # ResponseError handling remains on the watched mutation paths above.
    target_scalars = scalar_sequence(
        target_scalars,
        expected_count=len(plan.target_keys),
        label="post-cleanup",
    )
    if any(as_optional_bytes(scalar) is not None for scalar in target_scalars):
        raise PTGSmallWaveAttestationError(
            "all-unclaimed post-cleanup GET found an owned target key"
        )
    evidence_by_name = {
        "schema_version": "healthporta.ptg-wave.redis-unclaimed-post-cleanup.v1",
        "wave_id": manifest.wave_id,
        "manifest_digest": manifest.manifest_digest,
        "target_key_count": len(plan.target_keys),
        "absent_target_count": len(plan.target_keys),
        "expected_attestation_digest": expected_attestation_digest,
    }
    return PTGSmallWaveUnclaimedFailureRedisPostCleanupAttestation(
        wave_id=manifest.wave_id,
        manifest_digest=manifest.manifest_digest,
        target_key_count=len(plan.target_keys),
        absent_target_count=len(plan.target_keys),
        expected_attestation_digest=expected_attestation_digest,
        attestation_digest=canonical_mapping_digest(evidence_by_name),
    )


async def _read_unclaimed_failure_snapshot(
    redis: Any,
    plan: PTGSmallWaveCleanupPlan,
) -> _UnclaimedFailureSnapshot:
    async with redis.pipeline(transaction=True) as pipe:
        pipe.hgetall(plan.ready_key)
        pipe.get(plan.release_key)
        pipe.zrange(plan.queue_name, 0, -1, withscores=True)
        for key in plan.job_keys:
            pipe.get(key)
        for key in plan.result_keys:
            pipe.get(key)
        for key in plan.retry_keys:
            pipe.get(key)
        for key in plan.in_progress_keys:
            pipe.get(key)
        pipe.get(plan.health_check_key)
        redis_values = await pipe.execute(raise_on_error=False)
    return _unclaimed_failure_snapshot_from_values(plan, redis_values)


async def _read_watched_unclaimed_failure_snapshot(
    pipe: Any,
    plan: PTGSmallWaveCleanupPlan,
) -> _UnclaimedFailureSnapshot:
    return _UnclaimedFailureSnapshot(
        ready_entries=await pipe.hgetall(plan.ready_key),
        release_scalar=await pipe.get(plan.release_key),
        queue_entries=await pipe.zrange(
            plan.queue_name,
            0,
            -1,
            withscores=True,
        ),
        job_scalars=tuple([await pipe.get(key) for key in plan.job_keys]),
        result_scalars=tuple([await pipe.get(key) for key in plan.result_keys]),
        retry_scalars=tuple([await pipe.get(key) for key in plan.retry_keys]),
        in_progress_scalars=tuple(
            [await pipe.get(key) for key in plan.in_progress_keys]
        ),
        health_scalar=await pipe.get(plan.health_check_key),
    )


def _unclaimed_failure_snapshot_from_values(
    plan: PTGSmallWaveCleanupPlan,
    redis_values: Any,
) -> _UnclaimedFailureSnapshot:
    expected_count = 4 + (4 * len(plan.job_keys))
    if (
        not isinstance(redis_values, Sequence)
        or isinstance(redis_values, (str, bytes, bytearray))
        or len(redis_values) != expected_count
    ):
        raise PTGSmallWaveAttestationError(
            "all-unclaimed Redis observation returned an invalid shape"
        )
    redis_values = tuple(redis_values)
    size = len(plan.job_keys)
    return _UnclaimedFailureSnapshot(
        ready_entries=redis_values[0],
        release_scalar=redis_values[1],
        queue_entries=redis_values[2],
        job_scalars=redis_values[3 : 3 + size],
        result_scalars=redis_values[3 + size : 3 + (2 * size)],
        retry_scalars=redis_values[3 + (2 * size) : 3 + (3 * size)],
        in_progress_scalars=redis_values[3 + (3 * size) : 3 + (4 * size)],
        health_scalar=redis_values[-1],
    )


def _attest_unclaimed_failure_snapshot(
    manifest: PTGSmallWaveManifest,
    plan: PTGSmallWaveCleanupPlan,
    snapshot: _UnclaimedFailureSnapshot,
) -> PTGSmallWaveUnclaimedFailureRedisAttestation:
    release = _release_evidence(manifest, snapshot)
    lifecycle = _lifecycle_evidence(manifest, snapshot)
    _validate_stable_state(manifest, release, lifecycle)
    ready_slots_digest = sha256_hex(
        canonical_json_bytes([slot.as_mapping() for slot in release.ready_slots])
    )
    evidence_by_name = _unclaimed_evidence_map(
        manifest,
        plan,
        release,
        lifecycle,
        ready_slots_digest,
    )
    return PTGSmallWaveUnclaimedFailureRedisAttestation(
        wave_id=manifest.wave_id,
        queue_name=manifest.queue_name,
        manifest_digest=manifest.manifest_digest,
        jobs_digest=manifest.jobs_digest,
        job_count=len(manifest.jobs),
        target_key_count=len(plan.target_keys),
        ready_slots=release.ready_slots,
        ready_slots_digest=ready_slots_digest,
        release_present=release.is_present,
        release_digest=release.digest,
        release_receipt=release.receipt,
        queued_ordinals=lifecycle.queued_ordinals,
        job_ordinals=lifecycle.job_ordinals,
        result_ordinals=lifecycle.result_ordinals,
        retry_ordinals=lifecycle.retry_ordinals,
        in_progress_ordinals=lifecycle.in_progress_ordinals,
        health_check_present=lifecycle.is_health_check_present,
        attestation_digest=canonical_mapping_digest(evidence_by_name),
    )


def _release_evidence(
    manifest: PTGSmallWaveManifest,
    snapshot: _UnclaimedFailureSnapshot,
) -> _ReleaseEvidence:
    release_scalar = as_optional_bytes(snapshot.release_scalar)
    is_present = release_scalar is not None
    ready_slots = parse_ptg_small_wave_ready_slots(
        manifest.reference,
        snapshot.ready_entries,
        exact=is_present,
    )
    if not is_present:
        return _ReleaseEvidence(False, ready_slots, None, None)
    receipt = parse_ptg_small_wave_controller_release(manifest, release_scalar)
    if ready_slots != receipt.ready_slots:
        raise PTGSmallWaveAttestationError(
            "live ready identities do not match the exact release receipt"
        )
    return _ReleaseEvidence(True, ready_slots, receipt, receipt.release_digest)


def _lifecycle_evidence(
    manifest: PTGSmallWaveManifest,
    snapshot: _UnclaimedFailureSnapshot,
) -> _LifecycleEvidence:
    lifecycle = _LifecycleEvidence(
        queued_ordinals=queued_ordinals(manifest, snapshot.queue_entries),
        job_ordinals=verified_job_ordinals(manifest, snapshot.job_scalars),
        result_ordinals=present_ordinals(
            snapshot.result_scalars,
            expected_count=len(manifest.jobs),
            label="result",
        ),
        retry_ordinals=present_ordinals(
            snapshot.retry_scalars,
            expected_count=len(manifest.jobs),
            label="retry",
        ),
        in_progress_ordinals=present_ordinals(
            snapshot.in_progress_scalars,
            expected_count=len(manifest.jobs),
            label="in-progress",
        ),
        is_health_check_present=(
            as_optional_bytes(snapshot.health_scalar) is not None
        ),
    )
    if lifecycle.retry_ordinals or lifecycle.in_progress_ordinals:
        raise PTGSmallWaveCleanupActiveError(
            "all-unclaimed cleanup refused while retry or in-progress state exists"
        )
    return lifecycle


def _validate_stable_state(
    manifest: PTGSmallWaveManifest,
    release: _ReleaseEvidence,
    lifecycle: _LifecycleEvidence,
) -> None:
    if not release.is_present:
        if (
            lifecycle.queued_ordinals
            or lifecycle.job_ordinals
            or lifecycle.result_ordinals
        ):
            raise PTGSmallWaveAttestationError(
                "unreleased all-unclaimed state must have no queue, job, or result keys"
            )
        return
    validate_released_partition(
        manifest,
        lifecycle.queued_ordinals,
        lifecycle.job_ordinals,
        lifecycle.result_ordinals,
    )


def _unclaimed_evidence_map(
    manifest: PTGSmallWaveManifest,
    plan: PTGSmallWaveCleanupPlan,
    release: _ReleaseEvidence,
    lifecycle: _LifecycleEvidence,
    ready_slots_digest: str,
) -> dict[str, Any]:
    return {
        "schema_version": "healthporta.ptg-wave.redis-unclaimed-failure.v1",
        "wave_id": manifest.wave_id,
        "queue_name": manifest.queue_name,
        "manifest_digest": manifest.manifest_digest,
        "jobs_digest": manifest.jobs_digest,
        "job_count": len(manifest.jobs),
        "target_key_count": len(plan.target_keys),
        "ready_slots": [slot.as_mapping() for slot in release.ready_slots],
        "ready_slots_digest": ready_slots_digest,
        "release_present": release.is_present,
        "release_digest": release.digest,
        "release_receipt": (
            release.receipt.as_mapping() if release.receipt is not None else None
        ),
        "queued_ordinals": list(lifecycle.queued_ordinals),
        "job_ordinals": list(lifecycle.job_ordinals),
        "result_ordinals": list(lifecycle.result_ordinals),
        "retry_ordinals": list(lifecycle.retry_ordinals),
        "in_progress_ordinals": list(lifecycle.in_progress_ordinals),
        "health_check_present": lifecycle.is_health_check_present,
    }


attest_ptg_small_wave_unclaimed_failure_redis = attest_unclaimed_wave_redis
cleanup_ptg_small_wave_unclaimed_failure_redis = cleanup_unclaimed_wave_redis
attest_ptg_small_wave_unclaimed_failure_redis_post_cleanup = (
    attest_unclaimed_wave_redis_cleanup
)
