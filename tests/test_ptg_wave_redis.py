# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import json

import pytest
from arq.constants import in_progress_key_prefix, job_key_prefix, result_key_prefix

from process.ptg_wave_redis import (
    PTG_SMALL_WAVE_SLOTS,
    PTGSmallWaveAttestationError,
    PTGSmallWaveBarrierReceipt,
    PTGSmallWaveBarrierTimeout,
    PTGSmallWaveCleanupActiveError,
    PTGSmallWaveManifest,
    PTGSmallWaveReference,
    PTGSmallWaveValidationError,
    attest_ptg_small_wave,
    attest_ptg_wave_post_cleanup,
    attest_ptg_wave_pre_cleanup,
    cleanup_ptg_small_wave_terminal_state,
    inspect_ptg_small_wave_readiness,
    plan_ptg_small_wave_terminal_cleanup,
    publish_ptg_small_wave,
    read_ptg_small_wave_release,
    register_ptg_small_wave_slot,
    register_ptg_small_wave_slot_and_wait,
)
from tests.ptg_wave_redis_test_support import (
    CONFIG_IDENTITY,
    FakeRedis,
    IMAGE_IDENTITY,
    KUBERNETES_MANIFEST_IDENTITY,
    RUNTIME_IMAGE_IDENTITY,
    manifest as make_manifest,
    register_all,
)


@pytest.mark.asyncio
async def test_publish_writes_one_transaction_then_fully_attests() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(
        3,
        ordered_job_ids=("db-job-z", "db-job-a", "db-job-m"),
    )
    await register_all(redis, wave_manifest)

    receipt = await publish_ptg_small_wave(redis, wave_manifest)
    attested = await attest_ptg_small_wave(redis, wave_manifest)

    assert attested == receipt
    publication = redis.transactions[-1]
    assert [name for name, _arguments in publication] == [
        "set",
        "set",
        "set",
        "zadd",
        "set",
        "publish",
    ]
    assert set(redis.zsets[wave_manifest.queue_name]) == set(wave_manifest.job_ids)
    assert redis.values[wave_manifest.release_key] == receipt.release_payload
    assert redis.zrange_reads == [wave_manifest.queue_name, wave_manifest.queue_name]


@pytest.mark.asyncio
async def test_readiness_inspection_is_read_only_and_reports_release() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(1)

    empty = await inspect_ptg_small_wave_readiness(redis, wave_manifest.reference)
    assert empty.registered_slots == ()
    assert empty.missing_slots == PTG_SMALL_WAVE_SLOTS
    assert not empty.ready and not empty.released
    assert redis.transactions == []

    await register_all(redis, wave_manifest)
    ready = await inspect_ptg_small_wave_readiness(redis, wave_manifest.reference)
    assert tuple(identity.slot for identity in ready.registered_slots) == PTG_SMALL_WAVE_SLOTS
    assert ready.missing_slots == ()
    assert ready.config_identity == CONFIG_IDENTITY
    assert ready.kubernetes_manifest_identity == KUBERNETES_MANIFEST_IDENTITY
    assert ready.image_identity == IMAGE_IDENTITY
    assert ready.runtime_image_identity == RUNTIME_IMAGE_IDENTITY
    assert ready.ready and not ready.released

    receipt = await publish_ptg_small_wave(redis, wave_manifest)
    released = await inspect_ptg_small_wave_readiness(redis, wave_manifest.reference)
    assert released.ready and released.released
    assert released.release_digest == receipt.release_digest


@pytest.mark.asyncio
async def test_release_payload_size_is_independent_of_job_count() -> None:
    redis_one = FakeRedis()
    manifest_one = make_manifest(1, execution_digest="1" * 64)
    await register_all(redis_one, manifest_one)
    release_one = await publish_ptg_small_wave(redis_one, manifest_one)

    redis_many = FakeRedis()
    manifest_many = make_manifest(3_586, execution_digest="2" * 64)
    await register_all(redis_many, manifest_many)
    release_many = await publish_ptg_small_wave(redis_many, manifest_many)

    assert len(release_one.release_payload) == len(release_many.release_payload)
    assert b"job_ids" not in release_one.release_payload
    assert b"job_ids" not in release_many.release_payload
    assert json.loads(release_one.release_payload)["job_count"] == "0001"
    assert json.loads(release_many.release_payload)["job_count"] == "3586"


@pytest.mark.asyncio
async def test_ambiguous_publish_is_not_retried_and_get_reconciliation_succeeds() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(25)
    await register_all(redis, wave_manifest)
    redis.raise_after_execute = True

    with pytest.raises(TimeoutError, match="ambiguous EXEC"):
        await publish_ptg_small_wave(redis, wave_manifest)

    publication_transactions = [
        transaction
        for transaction in redis.transactions
        if any(
            command == "set" and arguments[0] == wave_manifest.release_key
            for command, arguments in transaction
        )
    ]
    assert len(publication_transactions) == 1
    redis.zrange_reads.clear()
    reconciled = await read_ptg_small_wave_release(redis, wave_manifest)
    assert reconciled.release_payload == redis.values[wave_manifest.release_key]
    assert redis.zrange_reads == []


@pytest.mark.asyncio
async def test_cleanup_refuses_active_state_then_deletes_only_exact_keys() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(3)
    await register_all(redis, wave_manifest)
    await publish_ptg_small_wave(redis, wave_manifest)
    plan = plan_ptg_small_wave_terminal_cleanup(wave_manifest)

    assert plan.job_keys == tuple(
        job_key_prefix + job_id for job_id in wave_manifest.job_ids
    )
    assert plan.health_check_key == wave_manifest.queue_name + ":health-check"
    assert wave_manifest.release_channel not in plan.target_keys
    with pytest.raises(PTGSmallWaveCleanupActiveError, match="state is active"):
        await attest_ptg_wave_pre_cleanup(redis, wave_manifest)
    with pytest.raises(PTGSmallWaveCleanupActiveError, match="state is active"):
        await cleanup_ptg_small_wave_terminal_state(redis, wave_manifest)

    _discard_queued_wave_state(redis, wave_manifest)
    active_key = in_progress_key_prefix + wave_manifest.job_ids[0]
    redis.values[active_key] = b"1"
    redis.bump(active_key)
    with pytest.raises(PTGSmallWaveCleanupActiveError, match="in-progress"):
        await cleanup_ptg_small_wave_terminal_state(redis, wave_manifest)


@pytest.mark.asyncio
async def test_cleanup_attests_results_health_and_get_only_absence() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(3)
    await register_all(redis, wave_manifest)
    published = await publish_ptg_small_wave(redis, wave_manifest)
    plan = plan_ptg_small_wave_terminal_cleanup(wave_manifest)
    foreign_key = "arq:job:foreign-durable-job"
    redis.values[foreign_key] = b"foreign"
    redis.bump(foreign_key)
    _discard_queued_wave_state(redis, wave_manifest)
    for job_id in wave_manifest.job_ids:
        terminal_key = result_key_prefix + job_id
        redis.values[terminal_key] = b"terminal-result"
        redis.bump(terminal_key)
    redis.values[plan.health_check_key] = b"worker-health"
    redis.bump(plan.health_check_key)
    transaction_count = len(redis.transactions)
    pre_cleanup = await attest_ptg_wave_pre_cleanup(redis, wave_manifest)
    assert pre_cleanup.release_digest == published.release_digest
    assert pre_cleanup.queue_entry_count == 0
    assert pre_cleanup.job_payload_count == 0
    assert pre_cleanup.result_count == len(wave_manifest.jobs)
    assert pre_cleanup.retry_count == 0
    assert pre_cleanup.in_progress_count == 0
    assert pre_cleanup.health_check_count == 1
    assert len(pre_cleanup.result_presence_digest) == 64
    assert pre_cleanup.attestation_digest == _mapping_digest(
        pre_cleanup.evidence_mapping()
    )
    assert len(redis.transactions) == transaction_count
    cleaned = await cleanup_ptg_small_wave_terminal_state(redis, wave_manifest)
    assert cleaned.target_key_count == len(plan.target_keys)
    assert cleaned.deleted_key_count == 6
    assert cleaned.pre_cleanup_attestation_digest == pre_cleanup.attestation_digest
    assert all(not redis.has_key(key) for key in plan.target_keys)
    assert redis.values[foreign_key] == b"foreign"
    redis.get_reads.clear()
    post_cleanup = await attest_ptg_wave_post_cleanup(
        redis,
        wave_manifest,
    )
    assert post_cleanup.target_key_count == len(plan.target_keys)
    assert post_cleanup.absent_target_count == len(plan.target_keys)
    assert post_cleanup.attestation_digest == _mapping_digest(
        post_cleanup.evidence_mapping()
    )
    assert tuple(redis.get_reads) == plan.target_keys

    redis.hashes[plan.ready_key]["0"] = b"wrong-type-remnant"
    redis.bump(plan.ready_key)
    with pytest.raises(PTGSmallWaveAttestationError, match="owned target"):
        await attest_ptg_wave_post_cleanup(redis, wave_manifest)


def _discard_queued_wave_state(
    redis: FakeRedis,
    wave_manifest: PTGSmallWaveManifest,
) -> None:
    redis.zsets[wave_manifest.queue_name].clear()
    redis.bump(wave_manifest.queue_name)
    for job_id in wave_manifest.job_ids:
        redis.values.pop(job_key_prefix + job_id)
        redis.bump(job_key_prefix + job_id)


@pytest.mark.asyncio
async def test_publish_rejects_incomplete_slots_without_partial_wave() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest()
    for slot in PTG_SMALL_WAVE_SLOTS[:-1]:
        await register_ptg_small_wave_slot(
            redis,
            wave_manifest.reference,
            slot=slot,
            pod_uid=f"pod-{slot:02d}",
        )

    with pytest.raises(PTGSmallWaveAttestationError, match="exactly slots"):
        await publish_ptg_small_wave(redis, wave_manifest)
    assert redis.zsets[wave_manifest.queue_name] == {}
    assert wave_manifest.release_key not in redis.values


@pytest.mark.asyncio
async def test_slot_registration_retries_only_unexecuted_watch_conflict() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(1)
    redis.watch_failures_remaining = 1

    registered = await register_ptg_small_wave_slot(
        redis,
        wave_manifest.reference,
        slot=0,
        pod_uid="pod-00",
    )
    assert registered.slot == 0
    assert redis.hashes[wave_manifest.ready_key]["0"]
    assert redis.watch_failures_remaining == 0


@pytest.mark.asyncio
async def test_publish_rejects_foreign_queue_entry() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest()
    await register_all(redis, wave_manifest)
    redis.zsets[wave_manifest.queue_name]["not-a-wave-job"] = wave_manifest.enqueue_time_ms
    redis.bump(wave_manifest.queue_name)

    with pytest.raises(PTGSmallWaveValidationError, match="already contains entries"):
        await publish_ptg_small_wave(redis, wave_manifest)
    assert wave_manifest.release_key not in redis.values


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected,replacement",
    [
        (
            IMAGE_IDENTITY,
            "registry.example/ptg-worker@sha256:" + "f" * 64,
        ),
        (RUNTIME_IMAGE_IDENTITY, "sha256:" + "f" * 64),
    ],
)
async def test_publish_rejects_worker_image_drift_from_controller_reference(
    expected: str,
    replacement: str,
) -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(13)
    await register_all(redis, wave_manifest)
    original = redis.hashes[wave_manifest.ready_key]["11"]
    redis.hashes[wave_manifest.ready_key]["11"] = original.replace(
        expected.encode(),
        replacement.encode(),
    )
    redis.bump(wave_manifest.ready_key)

    with pytest.raises(PTGSmallWaveAttestationError, match="another wave"):
        await publish_ptg_small_wave(redis, wave_manifest)


@pytest.mark.asyncio
@pytest.mark.parametrize("active_kind", ["tampered-job", "in-progress"])
async def test_attestation_fails_after_consumption_starts(active_kind: str) -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest()
    await register_all(redis, wave_manifest)
    await publish_ptg_small_wave(redis, wave_manifest)
    job_id = wave_manifest.jobs[3].job_id
    if active_kind == "tampered-job":
        active_key = job_key_prefix + job_id
        redis.values[active_key] = b"tampered"
        error_pattern = "missing or tampered"
    else:
        active_key = in_progress_key_prefix + job_id
        redis.values[active_key] = b"1"
        error_pattern = "unexpected ARQ in-progress"
    redis.bump(active_key)

    with pytest.raises(PTGSmallWaveAttestationError, match=error_pattern):
        await attest_ptg_small_wave(redis, wave_manifest)


@pytest.mark.asyncio
async def test_barrier_returns_matching_existing_release() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest()
    await register_all(redis, wave_manifest)
    published = await publish_ptg_small_wave(redis, wave_manifest)
    redis.zrange_reads.clear()

    received = await register_ptg_small_wave_slot_and_wait(
        redis,
        wave_manifest.reference,
        slot=0,
        pod_uid="pod-00",
        timeout_seconds=1,
    )
    assert isinstance(received, PTGSmallWaveBarrierReceipt)
    assert received.wave_id == published.wave_id
    assert received.jobs_digest == published.jobs_digest
    assert redis.zrange_reads == []
    assert redis.pubsubs[-1].closed


@pytest.mark.asyncio
async def test_worker_validates_3586_release_with_fixed_reference() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(3_586)
    reference = PTGSmallWaveReference(
        wave_id=wave_manifest.wave_id,
        queue_name=wave_manifest.queue_name,
        manifest_digest=wave_manifest.manifest_digest,
        jobs_digest=wave_manifest.jobs_digest,
        job_count=3_586,
        protocol_identity=wave_manifest.protocol_identity,
        serializer_identity=wave_manifest.serializer_identity,
        config_identity=wave_manifest.config_identity,
        kubernetes_manifest_identity=wave_manifest.kubernetes_manifest_identity,
        image_identity=wave_manifest.image_identity,
        runtime_image_identity=wave_manifest.runtime_image_identity,
        runtime_identity_digest=wave_manifest.runtime_identity_digest,
    )
    await register_all(redis, wave_manifest)
    await publish_ptg_small_wave(redis, wave_manifest)
    redis.zrange_reads.clear()

    received = await register_ptg_small_wave_slot_and_wait(
        redis,
        reference,
        slot=0,
        pod_uid="pod-00",
        timeout_seconds=1,
    )
    assert set(vars(reference)) == {
        "wave_id",
        "queue_name",
        "manifest_digest",
        "jobs_digest",
        "job_count",
        "protocol_identity",
        "serializer_identity",
        "config_identity",
        "kubernetes_manifest_identity",
        "image_identity",
        "runtime_image_identity",
        "runtime_identity_digest",
    }
    assert not hasattr(reference, "job_ids")
    assert received.job_count == 3_586
    assert not hasattr(received, "job_ids")
    assert redis.zrange_reads == []


@pytest.mark.asyncio
async def test_release_read_does_not_read_queue_or_jobs() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest(25)
    await register_all(redis, wave_manifest)
    published = await publish_ptg_small_wave(redis, wave_manifest)
    redis.zrange_reads.clear()
    assert await read_ptg_small_wave_release(redis, wave_manifest) == published
    assert redis.zrange_reads == []


@pytest.mark.asyncio
async def test_barrier_timeout_does_not_scan_or_claim_queue() -> None:
    redis = FakeRedis()
    wave_manifest = make_manifest()
    with pytest.raises(PTGSmallWaveBarrierTimeout):
        await register_ptg_small_wave_slot_and_wait(
            redis,
            wave_manifest.reference,
            slot=0,
            pod_uid="pod-00",
            timeout_seconds=0.001,
        )
    assert redis.zrange_reads == []
    assert wave_manifest.queue_name not in redis.zsets


def _mapping_digest(mapping: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(
            mapping,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    ).hexdigest()
