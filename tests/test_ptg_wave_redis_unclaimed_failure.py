# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact Redis evidence and cleanup tests for all-unclaimed PTG failures."""

from __future__ import annotations

import hashlib
import json

import pytest
from arq.constants import (
    in_progress_key_prefix,
    job_key_prefix,
    result_key_prefix,
    retry_key_prefix,
)

from process.ptg_wave_redis import (
    PTGSmallWaveAttestationError,
    PTGSmallWaveCleanupActiveError,
    PTGSmallWaveConflictError,
    attest_ptg_small_wave_unclaimed_failure_redis,
    attest_ptg_small_wave_unclaimed_failure_redis_post_cleanup,
    cleanup_ptg_small_wave_unclaimed_failure_redis,
    plan_ptg_small_wave_terminal_cleanup,
    publish_ptg_small_wave,
)
from tests.ptg_wave_redis_test_support import (
    CONFIG_IDENTITY,
    FakeRedis,
    manifest as make_manifest,
    register_all,
)


@pytest.mark.asyncio
async def test_unreleased_empty_wave_has_full_attestation_and_exact_cleanup() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(3)
    plan = plan_ptg_small_wave_terminal_cleanup(wave_manifest)
    redis.values["arq:ProviderDirectoryFHIR"] = b"preserve-fhir"
    redis.values["arq:job:unrelated"] = b"preserve-foreign"

    attestation = await attest_ptg_small_wave_unclaimed_failure_redis(
        redis,
        wave_manifest,
    )

    assert attestation.release_present is False
    assert attestation.release_receipt is None
    assert attestation.ready_slots == ()
    assert attestation.queued_ordinals == ()
    assert attestation.job_ordinals == ()
    assert attestation.result_ordinals == ()
    assert attestation.retry_ordinals == ()
    assert attestation.in_progress_ordinals == ()
    assert attestation.target_key_count == len(plan.target_keys)
    assert attestation.attestation_digest == _mapping_digest(
        attestation.evidence_mapping()
    )

    cleanup = await cleanup_ptg_small_wave_unclaimed_failure_redis(
        redis,
        wave_manifest,
        expected_attestation_digest=attestation.attestation_digest,
    )

    assert cleanup.deleted_key_count == 0
    assert cleanup.expected_attestation_digest == attestation.attestation_digest
    assert cleanup.attestation == attestation
    assert cleanup.as_mapping()["attestation"]["job_ordinals"] == []
    assert [command for command, _arguments in redis.transactions[-1]] == ["delete"]
    assert redis.transactions[-1][0][1] == plan.target_keys
    assert redis.values["arq:ProviderDirectoryFHIR"] == b"preserve-fhir"
    assert redis.values["arq:job:unrelated"] == b"preserve-foreign"

    redis.get_reads.clear()
    post_cleanup = await attest_ptg_small_wave_unclaimed_failure_redis_post_cleanup(
        redis,
        wave_manifest,
        expected_attestation_digest=attestation.attestation_digest,
    )

    assert tuple(redis.get_reads) == plan.target_keys
    assert post_cleanup.expected_attestation_digest == attestation.attestation_digest
    assert post_cleanup.absent_target_count == len(plan.target_keys)
    assert post_cleanup.attestation_digest == _mapping_digest(
        post_cleanup.evidence_mapping()
    )


@pytest.mark.asyncio
async def test_released_unclaimed_accepts_pending_and_result_subsets() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(3)
    await register_all(redis, wave_manifest)
    released = await publish_ptg_small_wave(redis, wave_manifest)
    first_job = wave_manifest.jobs[0]
    redis.zsets[wave_manifest.queue_name].pop(first_job.job_id)
    redis.bump(wave_manifest.queue_name)
    redis.values.pop(job_key_prefix + first_job.job_id)
    redis.bump(job_key_prefix + first_job.job_id)
    redis.values[result_key_prefix + first_job.job_id] = b"pre-claim-arq-result"
    redis.bump(result_key_prefix + first_job.job_id)
    plan = plan_ptg_small_wave_terminal_cleanup(wave_manifest)
    redis.values[plan.health_check_key] = b"worker-health"
    redis.bump(plan.health_check_key)

    attestation = await attest_ptg_small_wave_unclaimed_failure_redis(
        redis,
        wave_manifest,
    )

    assert attestation.release_present is True
    assert attestation.release_digest == released.release_digest
    assert attestation.release_receipt == released
    assert tuple(slot.slot for slot in attestation.ready_slots) == tuple(range(12))
    assert attestation.queued_ordinals == (1, 2)
    assert attestation.job_ordinals == (1, 2)
    assert attestation.result_ordinals == (0,)
    assert attestation.health_check_present is True

    cleanup = await cleanup_ptg_small_wave_unclaimed_failure_redis(
        redis,
        wave_manifest,
        expected_attestation_digest=attestation.attestation_digest,
    )
    assert cleanup.deleted_key_count == 7
    assert cleanup.as_mapping()["attestation"]["job_ordinals"] == [1, 2]
    assert all(not redis.has_key(key) for key in plan.target_keys)


@pytest.mark.asyncio
async def test_unclaimed_failure_rejects_foreign_queue_and_tampered_payloads() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(2)
    await register_all(redis, wave_manifest)
    await publish_ptg_small_wave(redis, wave_manifest)
    redis.zsets[wave_manifest.queue_name]["foreign-job"] = wave_manifest.enqueue_time_ms
    redis.bump(wave_manifest.queue_name)

    with pytest.raises(PTGSmallWaveAttestationError, match="foreign or repeated"):
        await attest_ptg_small_wave_unclaimed_failure_redis(redis, wave_manifest)

    redis.zsets[wave_manifest.queue_name].pop("foreign-job")
    redis.bump(wave_manifest.queue_name)
    redis.values[job_key_prefix + wave_manifest.jobs[1].job_id] = b"tampered"
    redis.bump(job_key_prefix + wave_manifest.jobs[1].job_id)

    with pytest.raises(PTGSmallWaveAttestationError, match="missing or tampered"):
        await attest_ptg_small_wave_unclaimed_failure_redis(redis, wave_manifest)


@pytest.mark.asyncio
async def test_unclaimed_failure_rejects_released_job_without_its_queue_member() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(2)
    await register_all(redis, wave_manifest)
    await publish_ptg_small_wave(redis, wave_manifest)
    job = wave_manifest.jobs[0]
    redis.zsets[wave_manifest.queue_name].pop(job.job_id)
    redis.bump(wave_manifest.queue_name)

    with pytest.raises(
        PTGSmallWaveAttestationError,
        match="queue and job payload subsets differ",
    ):
        await attest_ptg_small_wave_unclaimed_failure_redis(redis, wave_manifest)


@pytest.mark.asyncio
@pytest.mark.parametrize("prefix", [retry_key_prefix, in_progress_key_prefix])
async def test_unclaimed_failure_rejects_retry_or_in_progress_state(
    prefix: str,
) -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(2)
    await register_all(redis, wave_manifest)
    await publish_ptg_small_wave(redis, wave_manifest)
    key = prefix + wave_manifest.jobs[0].job_id
    redis.values[key] = b"active"
    redis.bump(key)

    with pytest.raises(PTGSmallWaveCleanupActiveError, match="retry or in-progress"):
        await attest_ptg_small_wave_unclaimed_failure_redis(redis, wave_manifest)


@pytest.mark.asyncio
async def test_unreleased_wave_rejects_any_queue_job_or_result_state() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(1)
    job = wave_manifest.jobs[0]
    redis.zsets[wave_manifest.queue_name][job.job_id] = job.score_ms
    redis.values[job_key_prefix + job.job_id] = job.serialized_job
    redis.values[result_key_prefix + job.job_id] = b"unexpected"

    with pytest.raises(PTGSmallWaveAttestationError, match="unreleased all-unclaimed"):
        await attest_ptg_small_wave_unclaimed_failure_redis(redis, wave_manifest)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["release", "ready"])
async def test_unclaimed_failure_rejects_tampered_release_or_ready_identity(
    kind: str,
) -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(1)
    await register_all(redis, wave_manifest)
    await publish_ptg_small_wave(redis, wave_manifest)
    if kind == "release":
        redis.values[wave_manifest.release_key] = b"{}"
        redis.bump(wave_manifest.release_key)
        pattern = "release receipt"
    else:
        original = redis.hashes[wave_manifest.ready_key]["0"]
        redis.hashes[wave_manifest.ready_key]["0"] = original.replace(
            CONFIG_IDENTITY.encode(),
            ("f" * 64).encode(),
        )
        redis.bump(wave_manifest.ready_key)
        pattern = "belongs to another wave"

    with pytest.raises(PTGSmallWaveAttestationError, match=pattern):
        await attest_ptg_small_wave_unclaimed_failure_redis(redis, wave_manifest)


@pytest.mark.asyncio
async def test_cleanup_requires_matching_watched_attestation_and_never_retries_conflict(
) -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(1)
    attestation = await attest_ptg_small_wave_unclaimed_failure_redis(
        redis,
        wave_manifest,
    )

    with pytest.raises(PTGSmallWaveAttestationError, match="expected witness"):
        await cleanup_ptg_small_wave_unclaimed_failure_redis(
            redis,
            wave_manifest,
            expected_attestation_digest="f" * 64,
        )
    assert redis.transactions == []

    redis.watch_failures_remaining = 1
    with pytest.raises(PTGSmallWaveConflictError, match="no retry"):
        await cleanup_ptg_small_wave_unclaimed_failure_redis(
            redis,
            wave_manifest,
            expected_attestation_digest=attestation.attestation_digest,
        )
    assert redis.transactions == []


def _mapping_digest(mapping: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(
            mapping,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    ).hexdigest()
