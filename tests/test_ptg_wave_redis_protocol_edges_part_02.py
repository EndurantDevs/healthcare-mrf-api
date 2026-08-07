# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partitioned exact PTG Redis protocol edge contracts."""

from __future__ import annotations

from tests.test_ptg_wave_redis_protocol_edges import (
    FakeRedis,
    PTGSmallWaveAttestationError,
    PTGSmallWaveBarrierTimeout,
    PTGSmallWaveCleanupActiveError,
    PTGSmallWaveConflictError,
    PTGSmallWaveValidationError,
    SimpleNamespace,
    _published_wave,
    _release_mapping,
    _slot_identity,
    attestation,
    barrier,
    canonical_json_bytes,
    in_progress_key_prefix,
    job_key_prefix,
    make_manifest,
    manifest_module,
    plan_ptg_small_wave_terminal_cleanup,
    pytest,
    register_ptg_small_wave_slot,
    replace,
    result_key_prefix,
    sha256_hex,
    unclaimed,
)


async def _unclaimed_failure_snapshot_fixture():
    redis, wave_manifest, receipt = await _published_wave()
    plan = plan_ptg_small_wave_terminal_cleanup(wave_manifest)
    snapshot = unclaimed._UnclaimedFailureSnapshot(
        ready_entries=dict(redis.hashes[wave_manifest.ready_key]),
        release_scalar=receipt.release_payload,
        queue_entries=[
            (job.job_id.encode(), job.score_ms)
            for job in wave_manifest.jobs
        ],
        job_scalars=[
            redis.values[job_key_prefix + job.job_id]
            for job in wave_manifest.jobs
        ],
        result_scalars=[None] * len(wave_manifest.jobs),
        retry_scalars=[None] * len(wave_manifest.jobs),
        in_progress_scalars=[None] * len(wave_manifest.jobs),
        health_scalar=None,
    )
    return wave_manifest, plan, snapshot


@pytest.mark.asyncio
async def test_unclaimed_snapshot_attests_and_rejects_unreleased_state():
    wave_manifest, plan, baseline = await _unclaimed_failure_snapshot_fixture()
    attested = unclaimed._attest_unclaimed_failure_snapshot(
        wave_manifest,
        plan,
        baseline,
    )
    assert attested.release_present
    assert attested.queued_ordinals == (0, 1)

    with pytest.raises(PTGSmallWaveAttestationError, match="unreleased"):
        unclaimed._attest_unclaimed_failure_snapshot(
            wave_manifest,
            plan,
            replace(baseline, release_scalar=None),
        )


@pytest.mark.asyncio
async def test_unclaimed_snapshot_rejects_active_partial_and_invalid_shapes():
    wave_manifest, plan, baseline = await _unclaimed_failure_snapshot_fixture()
    with pytest.raises(PTGSmallWaveCleanupActiveError, match="retry"):
        unclaimed._attest_unclaimed_failure_snapshot(
            wave_manifest,
            plan,
            replace(
                baseline,
                retry_scalars=[
                    b"retry",
                    None,
                ],
            ),
        )
    with pytest.raises(PTGSmallWaveAttestationError, match="complete stable partition"):
        unclaimed._attest_unclaimed_failure_snapshot(
            wave_manifest,
            plan,
            replace(
                baseline,
                result_scalars=[
                    b"result",
                    None,
                ],
            ),
        )

    with pytest.raises(PTGSmallWaveAttestationError, match="invalid shape"):
        unclaimed._unclaimed_failure_snapshot_from_values(plan, ())
    assert in_progress_key_prefix
    assert result_key_prefix

@pytest.mark.asyncio
async def test_ready_slot_attestation_rejects_malformed_identities():
    wave_manifest = make_manifest(2)
    reference = wave_manifest.reference
    registration = _slot_identity(wave_manifest, slot=0, pod_uid="pod-00")

    missing_field = registration.as_mapping()
    missing_field.pop("pod_uid")
    with pytest.raises(PTGSmallWaveAttestationError, match="fields are not exact"):
        attestation.parse_ptg_small_wave_ready_slots(
            reference,
            {"0": canonical_json_bytes(missing_field)},
            exact=False,
        )

    boolean_slot = registration.as_mapping()
    boolean_slot["slot"] = True
    with pytest.raises(PTGSmallWaveAttestationError, match="invalid slot"):
        attestation.parse_ptg_small_wave_ready_slots(
            reference,
            {"0": canonical_json_bytes(boolean_slot)},
            exact=False,
        )

    invalid_identity = registration.as_mapping()
    invalid_identity["image_identity"] = "not-pinned"
    with pytest.raises(PTGSmallWaveAttestationError, match="identity is invalid"):
        attestation.parse_ptg_small_wave_ready_slots(
            reference,
            {"0": canonical_json_bytes(invalid_identity)},
            exact=False,
        )

    with pytest.raises(
        PTGSmallWaveAttestationError,
        match="does not match the worker reference",
    ):
        attestation.validate_ptg_small_wave_barrier_release(
            reference,
            replace(registration, jobs_digest="f" * 64),
            b"unused",
        )


@pytest.mark.asyncio
async def test_canonical_mapping_and_release_parser_reject_malformed_scalars():
    for scalar, message in (
        (None, "is missing"),
        (b"\xff", "not valid JSON"),
        (b"[]", "not canonical"),
    ):
        with pytest.raises(PTGSmallWaveAttestationError, match=message):
            attestation._parse_canonical_json_mapping(scalar, "synthetic")

    _redis, wave_manifest, receipt = await _published_wave()
    slot_payload_by_field = _release_mapping(receipt)
    slot_payload_by_field["ready_slots"] = {}
    with pytest.raises(PTGSmallWaveAttestationError, match="ready slots are invalid"):
        attestation.parse_ptg_small_wave_controller_release(
            wave_manifest,
            canonical_json_bytes(slot_payload_by_field),
        )

    slot_payload_by_field = _release_mapping(receipt)
    slot_payload_by_field["ready_slots"] = ["not-a-mapping"]
    with pytest.raises(PTGSmallWaveAttestationError, match="ready slots are invalid"):
        attestation.parse_ptg_small_wave_controller_release(
            wave_manifest,
            canonical_json_bytes(slot_payload_by_field),
        )

    slot_payload_by_field = _release_mapping(receipt)
    slot_payload_by_field["ready_slots"][0]["config_identity"] = "f" * 64
    slot_payload_by_field["ready_slots_digest"] = sha256_hex(
        canonical_json_bytes(slot_payload_by_field["ready_slots"])
    )
    with pytest.raises(PTGSmallWaveAttestationError, match="do not bind"):
        attestation.parse_ptg_small_wave_controller_release(
            wave_manifest,
            canonical_json_bytes(slot_payload_by_field),
        )

    slot_payload_by_field = _release_mapping(receipt)
    slot_payload_by_field["ready_slots"][1]["pod_uid"] = slot_payload_by_field["ready_slots"][0]["pod_uid"]
    slot_payload_by_field["ready_slots_digest"] = sha256_hex(
        canonical_json_bytes(slot_payload_by_field["ready_slots"])
    )
    with pytest.raises(PTGSmallWaveAttestationError, match="repeat a pod_uid"):
        attestation.parse_ptg_small_wave_controller_release(
            wave_manifest,
            canonical_json_bytes(slot_payload_by_field),
        )


@pytest.mark.asyncio
async def test_release_parser_rejects_tampered_rebuilt_receipt(
    monkeypatch: pytest.MonkeyPatch,
):
    _redis, wave_manifest, receipt = await _published_wave()
    monkeypatch.setattr(
        attestation,
        "build_ptg_small_wave_receipt",
        lambda *_args: replace(receipt, release_payload=b"different"),
    )
    with pytest.raises(PTGSmallWaveAttestationError, match="tampered"):
        attestation.parse_ptg_small_wave_controller_release(
            wave_manifest,
            receipt.release_payload,
        )

def test_manifest_builder_rejects_empty_and_unbound_inputs():
    with pytest.raises(PTGSmallWaveValidationError, match="task payload count"):
        manifest_module.build_ptg_small_wave_manifest(
            [],
            execution_digest="a" * 64,
            job_ids=(),
            enqueue_time_ms=1,
        )
    with pytest.raises(PTGSmallWaveValidationError, match="must be a mapping"):
        manifest_module._canonicalize_control_payload(object(), ordinal=0)
    assert manifest_module._canonicalize_control_payload(
        {"tuple": ("synthetic",)}, ordinal=0
    ) == {"tuple": ("synthetic",)}
    with pytest.raises(PTGSmallWaveValidationError, match="must be a PTGSmall"):
        manifest_module.validate_ptg_small_wave_manifest(object())

    unbound = manifest_module.build_ptg_small_wave_manifest(
        [{"run_id": "synthetic-run", "params": {}}],
        execution_digest="a" * 64,
        job_ids=("job-0",),
        enqueue_time_ms=1,
    )
    with pytest.raises(PTGSmallWaveValidationError, match="not bound"):
        manifest_module.validate_ptg_small_wave_manifest(unbound)


def test_manifest_validator_rejects_invalid_job_definitions():
    wave_manifest = make_manifest(2)
    first_job, second_job = wave_manifest.jobs
    invalid_variants = (
        replace(wave_manifest, queue_name="arq:wrong"),
        replace(wave_manifest, enqueue_time_ms=-1),
        replace(wave_manifest, jobs=()),
        replace(wave_manifest, jobs=(object(),)),
        replace(
            wave_manifest,
            jobs=(
                first_job,
                replace(second_job, job_id=first_job.job_id),
            ),
        ),
        replace(
            wave_manifest,
            jobs=(
                replace(
                    first_job,
                    serialized_job=b"not-an-arq-message",
                    serialized_job_digest=sha256_hex(b"not-an-arq-message"),
                ),
                second_job,
            ),
        ),
    )
    for invalid_manifest in invalid_variants:
        with pytest.raises((PTGSmallWaveValidationError, PTGSmallWaveAttestationError)):
            manifest_module.validate_ptg_small_wave_manifest(invalid_manifest)


def test_manifest_validator_rejects_changed_serialized_job():
    wave_manifest = make_manifest(2)
    first_job, second_job = wave_manifest.jobs
    changed_bytes = manifest_module.arq_serialize_job(
        "foreign_function",
        ({"run_id": "synthetic-run", "params": {}},),
        {},
        None,
        first_job.score_ms,
        serializer=manifest_module.serialize_job,
    )
    changed_job = replace(
        first_job,
        serialized_job=changed_bytes,
        serialized_job_digest=sha256_hex(changed_bytes),
    )
    changed_jobs = (changed_job, second_job)
    changed_jobs_digest = manifest_module._calculate_jobs_digest(
        changed_jobs,
        wave_manifest.protocol_identity,
        wave_manifest.serializer_identity,
    )
    changed_manifest = replace(
        wave_manifest,
        jobs=changed_jobs,
        jobs_digest=changed_jobs_digest,
        manifest_digest=manifest_module._calculate_manifest_digest(
            wave_id=wave_manifest.wave_id,
            queue_name=wave_manifest.queue_name,
            enqueue_time_ms=wave_manifest.enqueue_time_ms,
            job_count=len(changed_jobs),
            jobs_digest=changed_jobs_digest,
            protocol_identity=wave_manifest.protocol_identity,
            serializer_identity=wave_manifest.serializer_identity,
        ),
    )
    with pytest.raises(PTGSmallWaveAttestationError, match="definition is invalid"):
        manifest_module.validate_ptg_small_wave_manifest(changed_manifest)

@pytest.mark.asyncio
async def test_slot_registration_handles_conflicts_retries_and_missing_slot(
    monkeypatch: pytest.MonkeyPatch,
):
    redis = FakeRedis()
    wave_manifest = make_manifest(2)
    monkeypatch.setattr(barrier, "_SLOT_REGISTRATION_MAX_ATTEMPTS", 1)
    redis.watch_failures_remaining = 1
    with pytest.raises(PTGSmallWaveConflictError, match="kept changing"):
        await register_ptg_small_wave_slot(
            redis,
            wave_manifest.reference,
            slot=0,
            pod_uid="pod-00",
        )

    retry_redis = FakeRedis()
    retry_redis.watch_failures_remaining = 1
    monkeypatch.setattr(barrier, "_SLOT_REGISTRATION_MAX_ATTEMPTS", 2)
    registration_after_retry = await register_ptg_small_wave_slot(
        retry_redis,
        wave_manifest.reference,
        slot=0,
        pod_uid="pod-00",
    )
    assert registration_after_retry.slot == 0

    redis, wave_manifest, _receipt = await _published_wave()
    redis.hashes[wave_manifest.ready_key].pop("0")
    with pytest.raises(PTGSmallWaveValidationError, match="without this exact slot"):
        await register_ptg_small_wave_slot(
            redis,
            wave_manifest.reference,
            slot=0,
            pod_uid="pod-00",
        )


@pytest.mark.asyncio
async def test_barrier_notification_times_out_then_returns_release():
    redis, wave_manifest, receipt = await _published_wave()
    registration = _slot_identity(wave_manifest, slot=0, pod_uid="pod-00")
    pubsub = redis.pubsub()
    with pytest.raises(PTGSmallWaveBarrierTimeout, match="did not arrive"):
        await barrier._wait_for_release_notification(
            redis,
            pubsub,
            wave_manifest.reference,
            registration,
            timeout_seconds=0,
        )

    pubsub.messages.extend(
        [
            {"type": "subscribe", "data": b"ignored"},
            {"type": "message", "data": receipt.release_payload},
        ]
    )
    observed = await barrier._wait_for_release_notification(
        redis,
        pubsub,
        wave_manifest.reference,
        registration,
        timeout_seconds=1,
    )
    assert observed.release_digest == receipt.release_digest


@pytest.mark.asyncio
async def test_notified_release_must_match_stored_release(
    monkeypatch: pytest.MonkeyPatch,
):
    redis, wave_manifest, _receipt = await _published_wave()
    registration = _slot_identity(wave_manifest, slot=0, pod_uid="pod-00")
    first = SimpleNamespace(release_digest="a" * 64)
    second = SimpleNamespace(release_digest="b" * 64)
    redis.values[wave_manifest.release_key] = b"stored"
    calls = iter((first, second))
    monkeypatch.setattr(
        barrier,
        "validate_ptg_small_wave_barrier_release",
        lambda *_args: next(calls),
    )
    with pytest.raises(PTGSmallWaveAttestationError, match="differ"):
        await barrier._validate_notified_release(
            redis,
            wave_manifest.reference,
            registration,
            b"notified",
        )
