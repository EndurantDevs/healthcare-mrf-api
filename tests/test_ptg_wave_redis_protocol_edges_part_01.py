# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partitioned exact PTG Redis protocol edge contracts."""

from __future__ import annotations

from tests.test_ptg_wave_redis_protocol_edges import (
    FakeRedis,
    PTGSmallWaveAttestationError,
    PTGSmallWaveBarrierReceipt,
    PTGSmallWaveValidationError,
    RUNTIME_IDENTITY,
    _published_wave,
    _reencoded_release,
    _reorder_ready_slots,
    _slot_bytes,
    _slot_identity,
    attestation,
    barrier,
    canonical_json_bytes,
    create_ptg_small_wave_slot_identity,
    make_manifest,
    manifest_module,
    math,
    present_ordinals,
    pytest,
    queued_ordinals,
    register_ptg_small_wave_slot,
    replace,
    scalar_sequence,
    validate_released_partition,
    verified_job_ordinals,
)


def test_ready_slot_parser_rejects_invalid_shape_membership_and_identity():
    wave_manifest = make_manifest(2)
    reference = wave_manifest.reference
    first = _slot_bytes(wave_manifest, slot=0, pod_uid="pod-first")
    second = _slot_bytes(wave_manifest, slot=1, pod_uid="pod-second")

    with pytest.raises(PTGSmallWaveAttestationError, match="invalid Redis shape"):
        attestation.parse_ptg_small_wave_ready_slots(
            reference,
            [],
            exact=False,
        )
    with pytest.raises(PTGSmallWaveAttestationError, match="unexpected slot"):
        attestation.parse_ptg_small_wave_ready_slots(
            reference,
            {"12": first},
            exact=False,
        )
    with pytest.raises(PTGSmallWaveAttestationError, match="exactly slots"):
        attestation.parse_ptg_small_wave_ready_slots(
            reference,
            {"0": first},
            exact=True,
        )
    with pytest.raises(PTGSmallWaveAttestationError, match="does not match"):
        attestation.parse_ptg_small_wave_ready_slots(
            reference,
            {"0": second},
            exact=False,
        )

    foreign = replace(
        _slot_identity(wave_manifest, slot=0, pod_uid="pod-foreign"),
        wave_id="f" * 64,
    )
    with pytest.raises(PTGSmallWaveAttestationError, match="another wave"):
        attestation.parse_ptg_small_wave_ready_slots(
            reference,
            {"0": canonical_json_bytes(foreign.as_mapping())},
            exact=False,
        )

    duplicate_first = _slot_bytes(
        wave_manifest,
        slot=0,
        pod_uid="pod-duplicate",
    )
    duplicate_second = _slot_bytes(
        wave_manifest,
        slot=1,
        pod_uid="pod-duplicate",
    )
    with pytest.raises(PTGSmallWaveAttestationError, match="repeats a pod_uid"):
        attestation.parse_ptg_small_wave_ready_slots(
            reference,
            {"0": duplicate_first, "1": duplicate_second},
            exact=False,
        )

@pytest.mark.asyncio
async def test_release_parser_accepts_exact_and_rejects_payload_shape():
    _redis, wave_manifest, receipt = await _published_wave()

    assert (
        attestation.parse_ptg_small_wave_controller_release(
            wave_manifest,
            receipt.release_payload,
        )
        == receipt
    )
    with pytest.raises(PTGSmallWaveAttestationError, match="not canonical"):
        attestation.parse_ptg_small_wave_controller_release(
            wave_manifest,
            receipt.release_payload + b" ",
        )
    with pytest.raises(PTGSmallWaveAttestationError, match="fields are not exact"):
        attestation.parse_ptg_small_wave_controller_release(
            wave_manifest,
            _reencoded_release(
                receipt,
                lambda payload: payload.pop("schema_version"),
            ),
        )
    with pytest.raises(PTGSmallWaveAttestationError, match="schema version"):
        attestation.parse_ptg_small_wave_controller_release(
            wave_manifest,
            _reencoded_release(
                receipt,
                lambda payload: payload.__setitem__("schema_version", 2),
            ),
        )


@pytest.mark.asyncio
async def test_release_parser_rejects_binding_slot_and_digest_mutations():
    _redis, wave_manifest, receipt = await _published_wave()
    with pytest.raises(PTGSmallWaveAttestationError, match="does not match"):
        attestation.parse_ptg_small_wave_controller_release(
            wave_manifest,
            _reencoded_release(
                receipt,
                lambda payload: payload.__setitem__("wave_id", "f" * 64),
            ),
        )
    with pytest.raises(PTGSmallWaveAttestationError, match="exactly ready slots"):
        attestation.parse_ptg_small_wave_controller_release(
            wave_manifest,
            _reencoded_release(
                receipt,
                lambda payload: payload.__setitem__("ready_slots", []),
            ),
        )
    with pytest.raises(PTGSmallWaveAttestationError, match="digest is invalid"):
        attestation.parse_ptg_small_wave_controller_release(
            wave_manifest,
            _reencoded_release(
                receipt,
                lambda payload: payload.__setitem__(
                    "ready_slots_digest",
                    "f" * 64,
                ),
            ),
        )
    with pytest.raises(PTGSmallWaveAttestationError, match="exactly ready slots"):
        attestation.parse_ptg_small_wave_controller_release(
            wave_manifest,
            _reencoded_release(receipt, _reorder_ready_slots),
        )


@pytest.mark.asyncio
async def test_barrier_release_rejects_invalid_identity_and_absent_slot():
    _redis, wave_manifest, receipt = await _published_wave()
    reference = wave_manifest.reference
    registration = _slot_identity(wave_manifest, slot=0, pod_uid="pod-00")
    barrier_receipt = attestation.validate_ptg_small_wave_barrier_release(
        reference,
        registration,
        receipt.release_payload,
    )
    assert isinstance(barrier_receipt, PTGSmallWaveBarrierReceipt)
    assert barrier_receipt.release_digest == receipt.release_digest

    with pytest.raises(PTGSmallWaveAttestationError, match="not a PTGSmall"):
        attestation.validate_ptg_small_wave_barrier_release(
            reference,
            object(),
            receipt.release_payload,
        )
    with pytest.raises(PTGSmallWaveAttestationError, match="excludes this slot"):
        attestation.validate_ptg_small_wave_barrier_release(
            reference,
            replace(registration, pod_uid="pod-replaced"),
            receipt.release_payload,
        )

@pytest.mark.parametrize(
    ("candidate", "message"),
    [
        ({1: "not-a-string-key"}, "keys must be strings"),
        ({"unordered": {1}}, "must not contain an unordered set"),
        ({"nonfinite": math.inf}, "must not contain a non-finite float"),
        ({"unsupported": object()}, "unsupported value type"),
    ],
)
def test_manifest_payload_canonicalization_rejects_ambiguous_inputs(
    candidate,
    message,
):
    with pytest.raises(PTGSmallWaveValidationError, match=message):
        manifest_module._canonicalize_control_payload(candidate, ordinal=0)

@pytest.mark.parametrize(
    ("enqueue_time_ms", "job_ids", "message"),
    [
        (True, ("job-0",), "must be an integer"),
        (-1, ("job-0",), "must not be negative"),
        (1, (), "exactly match"),
        (1, ("job-0", "job-0"), "exactly match"),
    ],
)
def test_manifest_builder_rejects_nonexact_durable_inputs(
    enqueue_time_ms,
    job_ids,
    message,
):
    with pytest.raises(PTGSmallWaveValidationError, match=message):
        manifest_module.build_ptg_small_wave_manifest(
            [{"run_id": "synthetic-run", "params": {}}],
            execution_digest="a" * 64,
            job_ids=job_ids,
            enqueue_time_ms=enqueue_time_ms,
        )

    with pytest.raises(PTGSmallWaveValidationError, match="runtime_identity"):
        manifest_module.build_ptg_small_wave_manifest(
            [{"run_id": "synthetic-run", "params": {}}],
            execution_digest="a" * 64,
            job_ids=("job-0",),
            enqueue_time_ms=1,
            runtime_identity=object(),
        )

def test_manifest_validation_rejects_partial_or_changed_immutable_fields():
    wave_manifest = make_manifest(2)
    first_job = wave_manifest.jobs[0]

    variants = (
        replace(wave_manifest, config_identity=None),
        replace(wave_manifest, runtime_identity_digest="f" * 64),
        replace(wave_manifest, jobs=list(wave_manifest.jobs)),
        replace(
            wave_manifest,
            jobs=(
                replace(first_job, score_ms=first_job.score_ms + 1),
                wave_manifest.jobs[1],
            ),
        ),
        replace(
            wave_manifest,
            jobs=(
                replace(first_job, serialized_job="not-bytes"),
                wave_manifest.jobs[1],
            ),
        ),
    )
    for invalid_manifest in variants:
        with pytest.raises(PTGSmallWaveValidationError):
            manifest_module.validate_ptg_small_wave_manifest(invalid_manifest)

    unbound = manifest_module.build_ptg_small_wave_manifest(
        [{"run_id": "synthetic-run", "params": {}}],
        execution_digest="a" * 64,
        job_ids=("job-0",),
        enqueue_time_ms=1,
    )
    assert manifest_module.bind_ptg_small_wave_runtime_identity(
        unbound,
        RUNTIME_IDENTITY,
    ).runtime_identity_digest is not None

@pytest.mark.asyncio
async def test_slot_registration_refuses_identity_collisions_and_invalid_timeouts():
    redis = FakeRedis()
    wave_manifest = make_manifest(2)
    reference = wave_manifest.reference

    with pytest.raises(PTGSmallWaveValidationError, match="integer from 0 through 11"):
        create_ptg_small_wave_slot_identity(
            reference,
            slot=True,
            pod_uid="pod-boolean-slot",
        )

    first = await register_ptg_small_wave_slot(
        redis,
        reference,
        slot=0,
        pod_uid="pod-shared",
    )
    assert (
        await register_ptg_small_wave_slot(
            redis,
            reference,
            slot=0,
            pod_uid="pod-shared",
        )
        == first
    )

    with pytest.raises(PTGSmallWaveValidationError, match="different identity"):
        await register_ptg_small_wave_slot(
            redis,
            reference,
            slot=0,
            pod_uid="pod-other",
        )
    with pytest.raises(PTGSmallWaveValidationError, match="exactly one wave slot"):
        await register_ptg_small_wave_slot(
            redis,
            reference,
            slot=1,
            pod_uid="pod-shared",
        )

    for invalid_timeout in (True, 0, -1, math.inf):
        with pytest.raises(PTGSmallWaveValidationError):
            barrier._validate_timeout(invalid_timeout)

def test_unclaimed_membership_helpers_accept_exact_queue_and_scalars():
    wave_manifest = make_manifest(2)
    first_job, second_job = wave_manifest.jobs

    valid_queue_values = [
        (first_job.job_id.encode(), first_job.score_ms),
        (second_job.job_id.encode(), second_job.score_ms),
    ]
    assert queued_ordinals(wave_manifest, valid_queue_values) == (0, 1)
    assert verified_job_ordinals(
        wave_manifest,
        [first_job.serialized_job, second_job.serialized_job],
    ) == (0, 1)
    assert present_ordinals(
        [None, b"result"],
        expected_count=2,
        label="result",
    ) == (1,)


def test_unclaimed_queue_helpers_reject_invalid_and_foreign_entries():
    wave_manifest = make_manifest(2)
    first_job = wave_manifest.jobs[0]
    with pytest.raises(PTGSmallWaveAttestationError, match="invalid shape"):
        queued_ordinals(wave_manifest, {})
    with pytest.raises(PTGSmallWaveAttestationError, match="entry has an invalid shape"):
        queued_ordinals(wave_manifest, [object()])
    with pytest.raises(PTGSmallWaveAttestationError, match="missing member"):
        queued_ordinals(wave_manifest, [(None, first_job.score_ms)])

    with pytest.raises(PTGSmallWaveAttestationError, match="foreign or repeated"):
        queued_ordinals(
            wave_manifest,
            [
                (first_job.job_id.encode(), first_job.score_ms),
                (first_job.job_id.encode(), first_job.score_ms),
            ],
        )
    with pytest.raises(PTGSmallWaveAttestationError, match="score"):
        queued_ordinals(
            wave_manifest,
            [(first_job.job_id.encode(), first_job.score_ms + 0.5)],
        )
    with pytest.raises(PTGSmallWaveAttestationError, match="non-UTF-8"):
        queued_ordinals(
            wave_manifest,
            [(b"\xff", first_job.score_ms)],
        )


def test_unclaimed_helpers_reject_tampering_and_invalid_partitions():
    wave_manifest = make_manifest(2)
    first_job, second_job = wave_manifest.jobs
    with pytest.raises(PTGSmallWaveAttestationError, match="tampered"):
        verified_job_ordinals(
            wave_manifest,
            [first_job.serialized_job + b"x", second_job.serialized_job],
        )
    with pytest.raises(PTGSmallWaveAttestationError, match="invalid shape"):
        scalar_sequence(
            [None],
            expected_count=2,
            label="result",
        )

    validate_released_partition(wave_manifest, (0,), (0,), (1,))
    with pytest.raises(PTGSmallWaveAttestationError, match="subsets differ"):
        validate_released_partition(wave_manifest, (0,), (), (1,))
    with pytest.raises(PTGSmallWaveAttestationError, match="complete stable partition"):
        validate_released_partition(wave_manifest, (0,), (0,), (0,))
