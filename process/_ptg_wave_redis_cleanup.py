# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact terminal Redis cleanup for one durable PTG wave manifest."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from redis.exceptions import WatchError

from process._ptg_wave_redis_attestation import (
    parse_ptg_small_wave_controller_release,
)
from process._ptg_wave_redis_cleanup_plan import (
    canonical_mapping_digest as _canonical_mapping_digest,
    plan_ptg_small_wave_terminal_cleanup,
)
from process._ptg_wave_redis_models import (
    WAVE_SCHEMA_VERSION,
    PTGSmallWaveAttestationError,
    PTGSmallWaveCleanupActiveError,
    PTGSmallWaveCleanupPlan,
    PTGSmallWaveCleanupReceipt,
    PTGSmallWaveConflictError,
    PTGSmallWaveManifest,
    PTGSmallWavePostCleanupAttestation,
    PTGSmallWavePreCleanupAttestation,
    as_optional_bytes,
    canonical_json_bytes,
    sha256_hex,
)
from process._ptg_wave_redis_unclaimed import (
    attest_ptg_small_wave_unclaimed_failure_redis,
    attest_ptg_small_wave_unclaimed_failure_redis_post_cleanup,
    attest_unclaimed_wave_redis,
    attest_unclaimed_wave_redis_cleanup,
    cleanup_ptg_small_wave_unclaimed_failure_redis,
    cleanup_unclaimed_wave_redis,
)


@dataclass(frozen=True)
class _PreCleanupSnapshot:
    queue_entry_count: Any
    job_payload_count: Any
    result_scalars: Any
    result_count: Any
    retry_count: Any
    in_progress_count: Any
    health_check_count: Any
    health_and_release_scalars: Any


async def cleanup_ptg_small_wave_terminal_state(
    redis: Any,
    manifest: PTGSmallWaveManifest,
) -> PTGSmallWaveCleanupReceipt:
    """Delete exact terminal keys while refusing any active lifecycle state."""

    plan = plan_ptg_small_wave_terminal_cleanup(manifest)
    try:
        async with redis.pipeline(transaction=True) as pipe:
            await pipe.watch(*plan.target_keys)
            pre_cleanup = _attest_pre_cleanup_snapshot(
                manifest,
                plan,
                await _read_watched_pre_cleanup_snapshot(pipe, plan),
            )
            pipe.multi()
            pipe.delete(*plan.target_keys)
            deleted_key_count = int((await pipe.execute())[0])
    except WatchError as exc:
        raise PTGSmallWaveConflictError(
            "Redis changed during terminal cleanup; no retry was attempted"
        ) from exc
    return PTGSmallWaveCleanupReceipt(
        wave_id=plan.wave_id,
        manifest_digest=plan.manifest_digest,
        target_key_count=len(plan.target_keys),
        deleted_key_count=deleted_key_count,
        pre_cleanup_attestation_digest=pre_cleanup.attestation_digest,
        pre_cleanup_attestation=pre_cleanup,
    )


async def attest_ptg_wave_pre_cleanup(
    redis: Any,
    manifest: PTGSmallWaveManifest,
) -> PTGSmallWavePreCleanupAttestation:
    """Read only the exact keys and prove terminal cleanup-safe state."""

    plan = plan_ptg_small_wave_terminal_cleanup(manifest)
    async with redis.pipeline(transaction=True) as pipe:
        pipe.zcard(plan.queue_name)
        pipe.exists(*plan.job_keys)
        pipe.mget(plan.result_keys)
        pipe.exists(*plan.result_keys)
        pipe.exists(*plan.retry_keys)
        pipe.exists(*plan.in_progress_keys)
        pipe.exists(plan.health_check_key)
        pipe.mget((plan.health_check_key, plan.release_key))
        snapshot = _PreCleanupSnapshot(*(await pipe.execute()))
    return _attest_pre_cleanup_snapshot(manifest, plan, snapshot)


async def _read_watched_pre_cleanup_snapshot(
    pipe: Any,
    plan: PTGSmallWaveCleanupPlan,
) -> _PreCleanupSnapshot:
    return _PreCleanupSnapshot(
        queue_entry_count=await pipe.zcard(plan.queue_name),
        job_payload_count=await pipe.exists(*plan.job_keys),
        result_scalars=await pipe.mget(plan.result_keys),
        result_count=await pipe.exists(*plan.result_keys),
        retry_count=await pipe.exists(*plan.retry_keys),
        in_progress_count=await pipe.exists(*plan.in_progress_keys),
        health_check_count=await pipe.exists(plan.health_check_key),
        health_and_release_scalars=await pipe.mget(
            (plan.health_check_key, plan.release_key)
        ),
    )


def _attest_pre_cleanup_snapshot(
    manifest: PTGSmallWaveManifest,
    plan: PTGSmallWaveCleanupPlan,
    snapshot: _PreCleanupSnapshot,
) -> PTGSmallWavePreCleanupAttestation:
    """Reduce one exact Redis snapshot into cleanup-safe evidence."""

    counts_by_name = _validated_pre_cleanup_counts(snapshot)
    result_presence = _presence_flags(
        snapshot.result_scalars,
        expected_count=len(plan.result_keys),
        label="result",
    )
    health_and_release = _redis_scalar_sequence(
        snapshot.health_and_release_scalars,
        expected_count=2,
        label="health/release",
    )
    is_health_present = as_optional_bytes(health_and_release[0]) is not None
    if (
        counts_by_name["result_count"] != sum(result_presence)
        or counts_by_name["health_check_count"] != int(is_health_present)
    ):
        raise PTGSmallWaveAttestationError(
            "terminal result or health-check key has an invalid Redis type"
        )
    release = parse_ptg_small_wave_controller_release(
        manifest,
        health_and_release[1],
    )
    result_presence_digest = sha256_hex(
        canonical_json_bytes(
            {
                "schema_version": WAVE_SCHEMA_VERSION,
                "manifest_digest": manifest.manifest_digest,
                "result_presence": list(result_presence),
            }
        )
    )
    return _build_pre_cleanup_attestation(
        manifest,
        plan,
        release.release_digest,
        result_presence_digest,
        counts_by_name,
    )


def _validated_pre_cleanup_counts(
    snapshot: _PreCleanupSnapshot,
) -> dict[str, int]:
    counts_by_name = {
        "queue_entry_count": _redis_count(snapshot.queue_entry_count, "queue"),
        "job_payload_count": _redis_count(snapshot.job_payload_count, "job payload"),
        "result_count": _redis_count(snapshot.result_count, "result"),
        "retry_count": _redis_count(snapshot.retry_count, "retry"),
        "in_progress_count": _redis_count(snapshot.in_progress_count, "in-progress"),
        "health_check_count": _redis_count(snapshot.health_check_count, "health-check"),
    }
    active_names = ("queue_entry_count", "job_payload_count", "retry_count", "in_progress_count")
    if any(counts_by_name[field_name] for field_name in active_names):
        raise PTGSmallWaveCleanupActiveError(
            "terminal cleanup refused while queue/job/retry/in-progress state is active"
        )
    return counts_by_name


def _build_pre_cleanup_attestation(
    manifest: PTGSmallWaveManifest,
    plan: PTGSmallWaveCleanupPlan,
    release_digest: str,
    result_presence_digest: str,
    counts_by_name: Mapping[str, int],
) -> PTGSmallWavePreCleanupAttestation:
    """Build canonical fixed-size evidence from validated terminal counts."""

    evidence_by_name = {
        "schema_version": WAVE_SCHEMA_VERSION,
        "wave_id": manifest.wave_id,
        "queue_name": manifest.queue_name,
        "manifest_digest": manifest.manifest_digest,
        "jobs_digest": manifest.jobs_digest,
        "job_count": len(manifest.jobs),
        "image_identity": manifest.reference.image_identity,
        "release_digest": release_digest,
        "target_key_count": len(plan.target_keys),
        **counts_by_name,
        "result_presence_digest": result_presence_digest,
    }
    return PTGSmallWavePreCleanupAttestation(
        **{
            field_name: field_value
            for field_name, field_value in evidence_by_name.items()
            if field_name != "schema_version"
        },
        attestation_digest=_canonical_mapping_digest(evidence_by_name),
    )


async def attest_ptg_wave_post_cleanup(
    redis: Any,
    manifest: PTGSmallWaveManifest,
) -> PTGSmallWavePostCleanupAttestation:
    """Use only exact-key GETs to prove every cleanup target is absent."""

    plan = plan_ptg_small_wave_terminal_cleanup(manifest)
    async with redis.pipeline(transaction=True) as pipe:
        for target_key in plan.target_keys:
            pipe.get(target_key)
        target_scalars = await pipe.execute(raise_on_error=False)
    if not isinstance(target_scalars, Sequence) or isinstance(
        target_scalars,
        (str, bytes, bytearray),
    ):
        raise PTGSmallWaveAttestationError(
            "post-clean GET attestation returned an invalid Redis shape"
        )
    remaining_count = sum(scalar is not None for scalar in target_scalars)
    if len(target_scalars) != len(plan.target_keys) or remaining_count:
        raise PTGSmallWaveAttestationError(
            "post-clean GET attestation found an owned target key"
        )
    evidence_by_name = {
        "schema_version": WAVE_SCHEMA_VERSION,
        "wave_id": manifest.wave_id,
        "manifest_digest": manifest.manifest_digest,
        "target_key_count": len(plan.target_keys),
        "absent_target_count": len(plan.target_keys),
    }
    return PTGSmallWavePostCleanupAttestation(
        wave_id=manifest.wave_id,
        manifest_digest=manifest.manifest_digest,
        target_key_count=len(plan.target_keys),
        absent_target_count=len(plan.target_keys),
        attestation_digest=_canonical_mapping_digest(evidence_by_name),
    )


def _presence_flags(
    redis_scalars: Any,
    *,
    expected_count: int,
    label: str,
) -> tuple[bool, ...]:
    scalars = _redis_scalar_sequence(
        redis_scalars,
        expected_count=expected_count,
        label=label,
    )
    return tuple(as_optional_bytes(scalar) is not None for scalar in scalars)


def _redis_scalar_sequence(
    redis_scalars: Any,
    *,
    expected_count: int,
    label: str,
) -> Sequence[Any]:
    if (
        not isinstance(redis_scalars, Sequence)
        or isinstance(redis_scalars, (str, bytes, bytearray))
        or len(redis_scalars) != expected_count
    ):
        raise PTGSmallWaveAttestationError(
            f"terminal {label} read returned an invalid Redis shape"
        )
    return redis_scalars


def _redis_count(candidate: Any, label: str) -> int:
    if not isinstance(candidate, int) or isinstance(candidate, bool) or candidate < 0:
        raise PTGSmallWaveAttestationError(
            f"terminal {label} count returned an invalid Redis value"
        )
    return candidate
