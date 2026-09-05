# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic edge contracts for exact PTG Redis wave protocol state."""


from __future__ import annotations


import json


import math


from dataclasses import replace


from types import SimpleNamespace


from unittest.mock import AsyncMock


import pytest


from arq.constants import in_progress_key_prefix, job_key_prefix, result_key_prefix


from redis.exceptions import ResponseError


from process import _ptg_wave_redis_attestation as attestation


from process import _ptg_wave_redis_barrier as barrier


from process import _ptg_wave_redis_cleanup as cleanup_module


from process import _ptg_wave_redis_encoding as encoding


from process import _ptg_wave_redis_manifest as manifest_module


from process import _ptg_wave_redis_models as redis_models


from process import _ptg_wave_redis_reference as reference_module


from process import _ptg_wave_redis_restore as restore_module


from process import _ptg_wave_redis_unclaimed as unclaimed


from process import _ptg_wave_redis_unclaimed_models as unclaimed_models


from process import ptg_wave_redis as redis_module


from process import ptg_wave_redis_adapter as adapter_module


from process import ptg_wave_worker_claim_adapter as claim_adapter


from process._ptg_wave_redis_cleanup_plan import (
    plan_ptg_small_wave_terminal_cleanup,
)


from process._ptg_wave_redis_models import (
    PTGSmallWaveAttestationError,
    PTGSmallWaveBarrierTimeout,
    PTGSmallWaveCleanupActiveError,
    PTGSmallWaveConflictError,
    PTGSmallWaveValidationError,
    canonical_json_bytes,
    sha256_hex,
)


from process._ptg_wave_redis_reference import (
    create_ptg_small_wave_slot_identity,
)


from process._ptg_wave_redis_unclaimed_validation import (
    present_ordinals,
    queued_ordinals,
    scalar_sequence,
    validate_released_partition,
    verified_job_ordinals,
)


from process.ptg_wave_redis import (
    PTGSmallWaveBarrierReceipt,
    publish_ptg_small_wave,
    register_ptg_small_wave_slot,
)


from tests.ptg_wave_redis_test_support import (
    FakeRedis,
    RUNTIME_IDENTITY,
    manifest as make_manifest,
    register_all,
)


def _slot_identity(wave_manifest, *, slot: int, pod_uid: str):
    return create_ptg_small_wave_slot_identity(
        wave_manifest.reference,
        slot=slot,
        pod_uid=pod_uid,
    )


def _slot_bytes(wave_manifest, *, slot: int, pod_uid: str) -> bytes:
    return canonical_json_bytes(
        _slot_identity(
            wave_manifest,
            slot=slot,
            pod_uid=pod_uid,
        ).as_mapping()
    )


async def _published_wave(count: int = 2):
    redis = FakeRedis()
    wave_manifest = make_manifest(count)
    await register_all(redis, wave_manifest)
    receipt = await publish_ptg_small_wave(redis, wave_manifest)
    return redis, wave_manifest, receipt


def _release_mapping(receipt) -> dict:
    return json.loads(receipt.release_payload)


def _reencoded_release(receipt, mutate) -> bytes:
    payload = _release_mapping(receipt)
    mutate(payload)
    return canonical_json_bytes(payload)


def _reorder_ready_slots(payload: dict) -> None:
    payload["ready_slots"] = list(reversed(payload["ready_slots"]))
    payload["ready_slots_digest"] = sha256_hex(
        canonical_json_bytes(payload["ready_slots"])
    )


def _adapter_identity(wave_manifest, **overrides):
    reference = wave_manifest.reference
    values_by_field = {
        "wave_digest": reference.wave_id,
        "queue": reference.queue_name,
        "worker_class": "process.PTGSmall",
        "slot_index": 0,
        "pod_uid": "pod-00",
        "manifest_digest": reference.manifest_digest,
        "jobs_digest": reference.jobs_digest,
        "job_count": reference.job_count,
        "config_identity": reference.config_identity,
        "manifest_identity": reference.kubernetes_manifest_identity,
        "image_identity": reference.image_identity,
        "runtime_image_identity": reference.runtime_image_identity,
    }
    values_by_field.update(overrides)
    return SimpleNamespace(**values_by_field)


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

def test_claim_adapter_rejects_invalid_run_and_worker_context(
    monkeypatch: pytest.MonkeyPatch,
):
    wave_manifest = make_manifest(2)
    reference = wave_manifest.reference
    params_by_field = {
        "_wave_id": "wave-synthetic",
        "_wave_digest": reference.wave_id,
        "_wave_job_id": "job-synthetic",
        "_expected_queue": reference.queue_name,
        "_expected_worker_class": "process.PTGSmall",
    }

    with pytest.raises(RuntimeError, match="requires a run_id"):
        claim_adapter.exact_wave_claim_values(
            {"job_id": "job-synthetic"},
            params_by_field,
            run_id="",
            claim_attempt_token="a" * 32,
        )
    monkeypatch.setattr(
        claim_adapter.PTGWaveWorkerIdentity,
        "from_environment",
        lambda: _adapter_identity(wave_manifest, wave_digest="f" * 64),
    )
    with pytest.raises(RuntimeError, match="execution digest"):
        claim_adapter.exact_wave_claim_values(
            {"job_id": "job-synthetic"},
            params_by_field,
            run_id="run-synthetic",
            claim_attempt_token="a" * 32,
        )
    monkeypatch.setattr(
        claim_adapter.PTGWaveWorkerIdentity,
        "from_environment",
        lambda: _adapter_identity(wave_manifest, queue="arq:wrong"),
    )
    with pytest.raises(RuntimeError, match="worker lane"):
        claim_adapter.exact_wave_claim_values(
            {"job_id": "job-synthetic"},
            params_by_field,
            run_id="run-synthetic",
            claim_attempt_token="a" * 32,
        )
    assert claim_adapter._context_job_id(object()) == ""


def test_reference_validator_rejects_malformed_wave_identity():
    reference = make_manifest(2).reference
    with pytest.raises(PTGSmallWaveValidationError, match="must be a PTGSmall"):
        reference_module.validate_ptg_small_wave_reference(object())
    with pytest.raises(PTGSmallWaveValidationError, match="queue name"):
        reference_module.validate_ptg_small_wave_reference(
            replace(reference, queue_name="arq:wrong"),
        )
    with pytest.raises(PTGSmallWaveValidationError, match="runtime identity digest"):
        reference_module.validate_ptg_small_wave_reference(
            replace(reference, runtime_identity_digest="f" * 64),
        )
    with pytest.raises(PTGSmallWaveValidationError, match="integer from 0 through 11"):
        reference_module.create_ptg_small_wave_slot_identity(
            reference,
            slot=True,
            pod_uid="pod-bool",
        )


def test_restore_rejects_empty_and_invalid_payloads():
    wave_manifest = make_manifest(2)
    restore_options_by_field = {
        "execution_digest": wave_manifest.wave_id,
        "jobs_digest": wave_manifest.jobs_digest,
        "manifest_digest": wave_manifest.manifest_digest,
        "protocol_identity": wave_manifest.protocol_identity,
        "serializer_identity": wave_manifest.serializer_identity,
    }
    with pytest.raises(PTGSmallWaveValidationError, match="non-empty"):
        restore_module.restore_ptg_small_wave_manifest([], **restore_options_by_field)
    with pytest.raises(PTGSmallWaveValidationError, match="must contain"):
        restore_module.restore_ptg_small_wave_manifest(
            [object()],
            **restore_options_by_field,
        )


def test_cleanup_models_expose_exact_attestation_mappings():
    pre_cleanup = redis_models.PTGSmallWavePreCleanupAttestation(
        wave_id="wave",
        queue_name="queue",
        manifest_digest="a" * 64,
        jobs_digest="b" * 64,
        job_count=1,
        image_identity="registry.example/worker@sha256:" + "c" * 64,
        release_digest="d" * 64,
        target_key_count=8,
        queue_entry_count=0,
        job_payload_count=0,
        result_count=1,
        retry_count=0,
        in_progress_count=0,
        health_check_count=0,
        result_presence_digest="e" * 64,
        attestation_digest="f" * 64,
    )
    cleanup_receipt = redis_models.PTGSmallWaveCleanupReceipt(
        wave_id="wave",
        manifest_digest="a" * 64,
        target_key_count=8,
        deleted_key_count=8,
        pre_cleanup_attestation_digest="f" * 64,
        pre_cleanup_attestation=pre_cleanup,
    )
    post_cleanup = redis_models.PTGSmallWavePostCleanupAttestation(
        wave_id="wave",
        manifest_digest="a" * 64,
        target_key_count=8,
        absent_target_count=8,
        attestation_digest="f" * 64,
    )
    unclaimed_post = unclaimed_models.PTGWaveUnclaimedRedisCleanupAttestation(
        wave_id="wave",
        manifest_digest="a" * 64,
        target_key_count=8,
        absent_target_count=8,
        expected_attestation_digest="f" * 64,
        attestation_digest="e" * 64,
    )
    assert cleanup_receipt.as_mapping()["pre_cleanup"] == pre_cleanup.as_mapping()
    assert post_cleanup.as_mapping()["attestation_digest"] == "f" * 64
    assert unclaimed_post.as_mapping()["attestation_digest"] == "e" * 64
