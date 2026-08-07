# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partitioned exact PTG Redis protocol edge contracts."""

from __future__ import annotations

from tests.test_ptg_wave_redis_protocol_edges import (
    AsyncMock,
    FakeRedis,
    PTGSmallWaveAttestationError,
    PTGSmallWaveConflictError,
    PTGSmallWaveValidationError,
    ResponseError,
    SimpleNamespace,
    _adapter_identity,
    _published_wave,
    _slot_bytes,
    _slot_identity,
    adapter_module,
    cleanup_module,
    encoding,
    in_progress_key_prefix,
    job_key_prefix,
    make_manifest,
    plan_ptg_small_wave_terminal_cleanup,
    publish_ptg_small_wave,
    pytest,
    redis_module,
    register_all,
    replace,
    result_key_prefix,
    unclaimed,
)


def _occupy_owned_key(redis, wave_manifest, occupied):
    first_job = wave_manifest.jobs[0]
    target_by_name = {
        "release": (redis.values, wave_manifest.release_key, b"occupied"),
        "job": (redis.values, job_key_prefix + first_job.job_id, b"occupied"),
        "result": (redis.values, result_key_prefix + first_job.job_id, b"occupied"),
        "retry": (redis.values, "arq:retry:" + first_job.job_id, b"occupied"),
        "in_progress": (
            redis.values,
            in_progress_key_prefix + first_job.job_id,
            b"occupied",
        ),
        "queue": (
            redis.zsets[wave_manifest.queue_name],
            first_job.job_id,
            first_job.score_ms,
        ),
    }
    target_values, target_key, target_value = target_by_name[occupied]
    target_values[target_key] = target_value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("occupied", "message"),
    [
        ("release", "release key"),
        ("job", "job keys"),
        ("result", "result keys"),
        ("retry", "retry keys"),
        ("in_progress", "in-progress keys"),
        ("queue", "queue already"),
    ],
)
async def test_publication_refuses_every_preexisting_owned_key(occupied, message):
    redis = FakeRedis()
    wave_manifest = make_manifest(2)
    await register_all(redis, wave_manifest)
    _occupy_owned_key(redis, wave_manifest, occupied)

    with pytest.raises(PTGSmallWaveValidationError, match=message):
        await publish_ptg_small_wave(redis, wave_manifest)

@pytest.mark.asyncio
async def test_publish_and_attest_residual_error_boundaries():
    redis = FakeRedis()
    wave_manifest = make_manifest(2)
    await register_all(redis, wave_manifest)
    redis.watch_failures_remaining = 1
    with pytest.raises(PTGSmallWaveConflictError, match="no retry"):
        await publish_ptg_small_wave(redis, wave_manifest)

    redis, wave_manifest, _receipt = await _published_wave()
    redis.values[wave_manifest.release_key] = b"tampered"
    with pytest.raises(PTGSmallWaveAttestationError, match="missing or tampered"):
        await redis_module.attest_ptg_small_wave(redis, wave_manifest)

    empty_snapshot = redis_module._AttestationSnapshot(
        ready_entries={},
        release_scalar=None,
        job_scalars=[],
        result_scalars=(),
        retry_scalars=(),
        in_progress_scalars=(),
        queue_entries=(),
    )
    with pytest.raises(PTGSmallWaveAttestationError, match="incomplete"):
        redis_module._attest_job_lifecycle(wave_manifest, empty_snapshot)

    active_snapshot = replace(
        empty_snapshot,
        job_scalars=[job.serialized_job for job in wave_manifest.jobs],
        result_scalars=[b"result", None],
        retry_scalars=[None, None],
        in_progress_scalars=[None, None],
    )
    with pytest.raises(PTGSmallWaveAttestationError, match="unexpected ARQ result"):
        redis_module._attest_job_lifecycle(wave_manifest, active_snapshot)

    missing_job_snapshot = replace(
        empty_snapshot,
        job_scalars=[None, wave_manifest.jobs[1].serialized_job],
        result_scalars=[None, None],
        retry_scalars=[None, None],
        in_progress_scalars=[None, None],
    )
    with pytest.raises(PTGSmallWaveAttestationError, match="payload is missing"):
        redis_module._attest_job_lifecycle(wave_manifest, missing_job_snapshot)

    with pytest.raises(PTGSmallWaveAttestationError, match="invalid shape"):
        redis_module._attest_queue_membership(wave_manifest, {})
    with pytest.raises(PTGSmallWaveAttestationError, match="missing, extra, or tampered"):
        redis_module._attest_queue_membership(
            wave_manifest,
            [(wave_manifest.jobs[0].job_id.encode(), wave_manifest.jobs[0].score_ms)],
        )

@pytest.mark.parametrize(
    ("function", "argument", "message"),
    [
        (encoding.require_digest, ("digest", "not-a-digest"), "digest must"),
        (encoding.require_job_count, (True,), "job_count"),
        (encoding.decode_job_count, ("12",), "fixed-width"),
        (encoding.decode_job_count, ("0000",), "outside"),
        (encoding.require_job_id, (" value ",), "job_id"),
        (encoding.require_protocol_identity, ("protocol", " value "), "protocol"),
        (encoding.require_identity, ("pod_uid", " value "), "pod_uid"),
        (encoding.as_optional_bytes, (object(),), "non-string"),
        (encoding.as_text, (None,), "missing queue"),
        (encoding.as_text, (b"\xff",), "non-UTF"),
    ],
)
def test_redis_encoding_rejects_noncanonical_values(function, argument, message):
    with pytest.raises(
        (PTGSmallWaveValidationError, PTGSmallWaveAttestationError),
        match=message,
    ):
        function(*argument)

@pytest.mark.asyncio
async def test_redis_adapter_requires_registration_before_release_wait():
    wave_manifest = make_manifest(2)
    identity = _adapter_identity(wave_manifest)
    redis = FakeRedis()
    barrier_client = adapter_module.PTGSmallWaveRedisBarrier(
        redis_pool=redis,
        reference=wave_manifest.reference,
        slot=0,
        pod_uid="pod-00",
        worker_class="process.PTGSmall",
    )
    with pytest.raises(PTGSmallWaveValidationError, match="must register"):
        await barrier_client.wait_for_release(identity)
    await barrier_client.aclose()


@pytest.mark.asyncio
async def test_redis_adapter_closes_pool_without_aclose_method():
    wave_manifest = make_manifest(2)
    no_close = adapter_module.PTGSmallWaveRedisBarrier(
        redis_pool=object(),
        reference=wave_manifest.reference,
        slot=0,
        pod_uid="pod-00",
        worker_class="process.PTGSmall",
    )
    await no_close.aclose()


@pytest.mark.asyncio
async def test_redis_adapter_rejects_invalid_identity_and_closed_client():
    wave_manifest = make_manifest(2)
    identity = _adapter_identity(wave_manifest)
    with pytest.raises(PTGSmallWaveValidationError, match="worker class"):
        await adapter_module.create_ptg_wave_redis_barrier(
            _adapter_identity(wave_manifest, worker_class="other"),
            pool_factory=lambda _settings: FakeRedis(),
            settings_factory=object,
        )

    barrier_client = adapter_module.PTGSmallWaveRedisBarrier(
        redis_pool=FakeRedis(),
        reference=wave_manifest.reference,
        slot=0,
        pod_uid="pod-00",
        worker_class="process.PTGSmall",
        is_closed=True,
    )
    with pytest.raises(PTGSmallWaveValidationError, match="already closed"):
        adapter_module._validate_adapter_identity(barrier_client, identity)
    with pytest.raises(PTGSmallWaveValidationError, match="missing absent"):
        adapter_module._identity_attribute(object(), "absent")


@pytest.mark.asyncio
async def test_redis_adapter_registers_ready_identity():
    wave_manifest = make_manifest(2)
    identity = _adapter_identity(wave_manifest)
    registered = adapter_module.PTGSmallWaveRedisBarrier(
        redis_pool=FakeRedis(),
        reference=wave_manifest.reference,
        slot=0,
        pod_uid="pod-00",
        worker_class="process.PTGSmall",
        registration=_slot_identity(wave_manifest, slot=0, pod_uid="pod-00"),
    )
    assert (await registered.register_ready(identity))["slot"] == 0


@pytest.mark.asyncio
async def test_redis_adapter_closes_sync_pool_and_accepts_sync_factory():
    wave_manifest = make_manifest(2)
    identity = _adapter_identity(wave_manifest)

    class SyncClosePool:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    sync_pool = SyncClosePool()
    sync_close = adapter_module.PTGSmallWaveRedisBarrier(
        redis_pool=sync_pool,
        reference=wave_manifest.reference,
        slot=0,
        pod_uid="pod-00",
        worker_class="process.PTGSmall",
    )
    await sync_close.aclose()
    assert sync_pool.closed is True

    synchronous_factory = await adapter_module.create_ptg_wave_redis_barrier(
        identity,
        pool_factory=lambda _settings: FakeRedis(),
        settings_factory=object,
    )
    assert isinstance(synchronous_factory.redis_pool, FakeRedis)
    await synchronous_factory.aclose()

@pytest.mark.asyncio
async def test_unclaimed_redis_translates_read_errors_and_rejects_live_keys(
    monkeypatch: pytest.MonkeyPatch,
):
    wave_manifest = make_manifest(2)
    monkeypatch.setattr(
        unclaimed,
        "_read_unclaimed_failure_snapshot",
        AsyncMock(side_effect=ResponseError("wrong type")),
    )
    with pytest.raises(PTGSmallWaveAttestationError, match="invalid key type"):
        await unclaimed.attest_unclaimed_wave_redis(FakeRedis(), wave_manifest)

    monkeypatch.setattr(
        unclaimed,
        "_read_watched_unclaimed_failure_snapshot",
        AsyncMock(side_effect=ResponseError("wrong type")),
    )
    with pytest.raises(PTGSmallWaveAttestationError, match="invalid key type"):
        await unclaimed.cleanup_unclaimed_wave_redis(
            FakeRedis(),
            wave_manifest,
            expected_attestation_digest="a" * 64,
        )

    redis = FakeRedis()
    plan = plan_ptg_small_wave_terminal_cleanup(wave_manifest)
    redis.values[plan.release_key] = b"owned"
    with pytest.raises(PTGSmallWaveAttestationError, match="owned target"):
        await unclaimed.attest_unclaimed_wave_redis_cleanup(
            redis,
            wave_manifest,
            expected_attestation_digest="a" * 64,
        )

    redis, wave_manifest, receipt = await _published_wave()
    ready_entries_by_field = dict(redis.hashes[wave_manifest.ready_key])
    ready_entries_by_field["0"] = _slot_bytes(
        wave_manifest,
        slot=0,
        pod_uid="pod-replaced",
    )
    snapshot = unclaimed._UnclaimedFailureSnapshot(
        ready_entries=ready_entries_by_field,
        release_scalar=receipt.release_payload,
        queue_entries=[
            (job.job_id.encode(), job.score_ms)
            for job in wave_manifest.jobs
        ],
        job_scalars=[
            redis.values[job_key_prefix + job.job_id]
            for job in wave_manifest.jobs
        ],
        result_scalars=[None, None],
        retry_scalars=[None, None],
        in_progress_scalars=[None, None],
        health_scalar=None,
    )
    with pytest.raises(PTGSmallWaveAttestationError, match="do not match"):
        unclaimed._release_evidence(wave_manifest, snapshot)

@pytest.mark.asyncio
async def test_terminal_cleanup_fails_after_exhausted_watch_retry(
    monkeypatch: pytest.MonkeyPatch,
):
    redis = FakeRedis()
    wave_manifest = make_manifest(1)
    redis.watch_failures_remaining = 1
    attest_pre_cleanup = cleanup_module._attest_pre_cleanup_snapshot
    monkeypatch.setattr(
        cleanup_module,
        "_attest_pre_cleanup_snapshot",
        lambda *_args: SimpleNamespace(attestation_digest="a" * 64),
    )
    with pytest.raises(PTGSmallWaveConflictError, match="no retry"):
        await cleanup_module.cleanup_ptg_small_wave_terminal_state(
            redis,
            wave_manifest,
        )
    monkeypatch.setattr(
        cleanup_module,
        "_attest_pre_cleanup_snapshot",
        attest_pre_cleanup,
    )


def test_terminal_cleanup_rejects_inconsistent_precleanup_snapshot():
    wave_manifest = make_manifest(1)
    plan = plan_ptg_small_wave_terminal_cleanup(wave_manifest)
    inconsistent_health = cleanup_module._PreCleanupSnapshot(
        queue_entry_count=0,
        job_payload_count=0,
        result_scalars=[None],
        result_count=0,
        retry_count=0,
        in_progress_count=0,
        health_check_count=0,
        health_and_release_scalars=[b"present", None],
    )
    with pytest.raises(PTGSmallWaveAttestationError, match="invalid Redis type"):
        cleanup_module._attest_pre_cleanup_snapshot(
            wave_manifest,
            plan,
            inconsistent_health,
        )
    with pytest.raises(PTGSmallWaveAttestationError, match="invalid Redis shape"):
        cleanup_module._redis_scalar_sequence(
            [None],
            expected_count=2,
            label="result",
        )
    with pytest.raises(PTGSmallWaveAttestationError, match="invalid Redis value"):
        cleanup_module._redis_count(True, "queue")


@pytest.mark.asyncio
async def test_post_cleanup_attestation_rejects_malformed_pipeline_shape():
    wave_manifest = make_manifest(1)

    class MalformedPipeline:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def get(self, _key):
            return self

        async def execute(self, *, raise_on_error):
            assert raise_on_error is False
            return {}

    class MalformedRedis:
        def pipeline(self, *, transaction):
            assert transaction is True
            return MalformedPipeline()

    with pytest.raises(PTGSmallWaveAttestationError, match="invalid Redis shape"):
        await cleanup_module.attest_ptg_wave_post_cleanup(
            MalformedRedis(), wave_manifest
        )
