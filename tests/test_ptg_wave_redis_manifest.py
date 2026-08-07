# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace

import pytest

from process import _ptg_wave_redis_manifest as manifest_module
from process.ptg_wave_redis import (
    PTG_SMALL_WAVE_PROTOCOL_IDENTITY,
    PTG_SMALL_WAVE_SERIALIZER_IDENTITY,
    PTGSmallWaveValidationError,
    bind_ptg_small_wave_runtime_identity,
    build_ptg_small_wave_manifest,
    restore_ptg_small_wave_manifest,
)
from tests.ptg_wave_redis_test_support import (
    EXECUTION_DIGEST,
    IMAGE_IDENTITY,
    RUNTIME_IDENTITY,
    RUNTIME_IMAGE_IDENTITY,
    job_ids,
    manifest,
    payloads,
)


def test_manifest_is_deterministic_and_uses_the_required_dedicated_queue() -> None:
    original = payloads()
    reordered = payloads()
    reordered[0] = {
        "params": {"test_mode": True, "plan_ids": ["plan-00"]},
        "run_id": "wave-run-00",
    }

    first = build_ptg_small_wave_manifest(
        original,
        execution_digest=EXECUTION_DIGEST,
        job_ids=job_ids(12),
        enqueue_time_ms=1_700_000_000_000,
    )
    second = build_ptg_small_wave_manifest(
        reordered,
        execution_digest=EXECUTION_DIGEST,
        job_ids=job_ids(12),
        enqueue_time_ms=1_700_000_000_000,
    )

    assert first == second
    assert first.wave_id == EXECUTION_DIGEST
    assert first.queue_name == f"arq:PTGSmall:wave:{EXECUTION_DIGEST}"
    assert first.job_ids == job_ids(12)
    assert first.protocol_identity == PTG_SMALL_WAVE_PROTOCOL_IDENTITY
    assert first.serializer_identity == PTG_SMALL_WAVE_SERIALIZER_IDENTITY
    assert all(job.serialized_job for job in first.jobs)


@pytest.mark.parametrize("count", [1, 11, 12, 13, 25, 256])
def test_manifest_supports_variable_job_counts_with_ordered_ordinals(count: int) -> None:
    wave_manifest = manifest(count)

    assert len(wave_manifest.jobs) == count
    assert tuple(job.ordinal for job in wave_manifest.jobs) == tuple(range(count))
    assert wave_manifest.job_ids == job_ids(count)


def test_manifest_preserves_explicit_ordered_durable_job_ids() -> None:
    ordered_job_ids = (
        "db:control-run/0001",
        "550e8400-e29b-41d4-a716-446655440000",
        "durable-job#0003",
    )

    wave_manifest = manifest(3, ordered_job_ids=ordered_job_ids)

    assert wave_manifest.wave_id == EXECUTION_DIGEST
    assert wave_manifest.job_ids == ordered_job_ids
    assert tuple(job.job_id for job in wave_manifest.jobs) == ordered_job_ids


def test_post_render_identity_binding_preserves_exact_manifest_bytes() -> None:
    draft = build_ptg_small_wave_manifest(
        payloads(2),
        execution_digest=EXECUTION_DIGEST,
        job_ids=job_ids(2),
        enqueue_time_ms=1_700_000_000_000,
    )
    serialized_jobs = tuple(job.serialized_job for job in draft.jobs)

    with pytest.raises(PTGSmallWaveValidationError, match="must be bound"):
        _ = draft.reference

    bound = bind_ptg_small_wave_runtime_identity(draft, RUNTIME_IDENTITY)
    assert tuple(job.serialized_job for job in bound.jobs) == serialized_jobs
    assert bound.jobs_digest == draft.jobs_digest
    assert bound.manifest_digest == draft.manifest_digest
    assert bound.image_identity == IMAGE_IDENTITY
    assert bound.runtime_image_identity == RUNTIME_IMAGE_IDENTITY
    assert IMAGE_IDENTITY.rsplit(":", 1)[-1] != RUNTIME_IMAGE_IDENTITY.split(":", 1)[-1]
    assert bind_ptg_small_wave_runtime_identity(bound, RUNTIME_IDENTITY) is bound

    changed_runtime = replace(
        RUNTIME_IDENTITY,
        runtime_image_identity="sha256:" + "f" * 64,
    )
    changed = bind_ptg_small_wave_runtime_identity(draft, changed_runtime)
    assert changed.manifest_digest == bound.manifest_digest
    assert changed.jobs_digest == bound.jobs_digest
    assert changed.runtime_identity_digest != bound.runtime_identity_digest
    with pytest.raises(PTGSmallWaveValidationError, match="already bound"):
        bind_ptg_small_wave_runtime_identity(bound, changed_runtime)


def test_restore_uses_exact_admitted_objects_without_reserializing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admitted = manifest(3)

    def reject_reserialization(*_args, **_kwargs):
        raise AssertionError("restore must not serialize an admitted job")

    monkeypatch.setattr(manifest_module, "arq_serialize_job", reject_reserialization)
    restored = restore_ptg_small_wave_manifest(
        admitted.jobs,
        execution_digest=admitted.wave_id,
        jobs_digest=admitted.jobs_digest,
        manifest_digest=admitted.manifest_digest,
        protocol_identity=admitted.protocol_identity,
        serializer_identity=admitted.serializer_identity,
    )

    assert restored.jobs is admitted.jobs
    assert all(
        restored_job is admitted_job
        and restored_job.serialized_job is admitted_job.serialized_job
        for restored_job, admitted_job in zip(restored.jobs, admitted.jobs)
    )
    assert set(restored.jobs[0].as_manifest_mapping()) == {
        "ordinal",
        "job_id",
        "score_ms",
        "serialized_job_digest",
    }
    assert restored.jobs_digest == admitted.jobs_digest
    assert restored.manifest_digest == admitted.manifest_digest
    rebound = bind_ptg_small_wave_runtime_identity(restored, RUNTIME_IDENTITY)
    assert rebound.jobs is admitted.jobs
    assert rebound.reference == admitted.reference


def test_restore_recomputes_each_persisted_digest_and_ordering() -> None:
    admitted = manifest(2)
    first_job = admitted.jobs[0]
    changed_bytes = first_job.serialized_job[:-1] + bytes(
        [first_job.serialized_job[-1] ^ 1]
    )
    restore_options_by_name = {
        "execution_digest": admitted.wave_id,
        "jobs_digest": admitted.jobs_digest,
        "manifest_digest": admitted.manifest_digest,
        "protocol_identity": admitted.protocol_identity,
        "serializer_identity": admitted.serializer_identity,
    }

    with pytest.raises(PTGSmallWaveValidationError, match="does not match its bytes"):
        restore_ptg_small_wave_manifest(
            (replace(first_job, serialized_job=changed_bytes), admitted.jobs[1]),
            **restore_options_by_name,
        )
    with pytest.raises(PTGSmallWaveValidationError, match="jobs digest"):
        restore_ptg_small_wave_manifest(
            admitted.jobs,
            **{**restore_options_by_name, "jobs_digest": "f" * 64},
        )
    with pytest.raises(PTGSmallWaveValidationError, match="manifest digest"):
        restore_ptg_small_wave_manifest(
            admitted.jobs,
            **{**restore_options_by_name, "manifest_digest": "f" * 64},
        )
    with pytest.raises(PTGSmallWaveValidationError, match="ordered from zero"):
        restore_ptg_small_wave_manifest(
            tuple(reversed(admitted.jobs)),
            **restore_options_by_name,
        )


def test_manifest_digests_bind_exact_jobs_enqueue_and_protocol_identities() -> None:
    task_payloads = payloads(2)
    build_options_by_name = {
        "execution_digest": EXECUTION_DIGEST,
        "job_ids": job_ids(2),
        "enqueue_time_ms": 1_700_000_000_000,
    }
    baseline = build_ptg_small_wave_manifest(task_payloads, **build_options_by_name)
    changed_payloads = payloads(2)
    changed_payloads[1]["params"]["plan_ids"] = ["changed-plan"]
    changed_payload = build_ptg_small_wave_manifest(
        changed_payloads,
        **build_options_by_name,
    )
    changed_ids = build_ptg_small_wave_manifest(
        task_payloads,
        **{**build_options_by_name, "job_ids": tuple(reversed(job_ids(2)))},
    )
    changed_enqueue = build_ptg_small_wave_manifest(
        task_payloads,
        **{**build_options_by_name, "enqueue_time_ms": 1_700_000_000_001},
    )
    changed_protocol = build_ptg_small_wave_manifest(
        task_payloads,
        **{
            **build_options_by_name,
            "protocol_identity": "healthporta.ptg-small.exact-wave.v2",
        },
    )
    changed_serializer = build_ptg_small_wave_manifest(
        task_payloads,
        **{
            **build_options_by_name,
            "serializer_identity": "arq-0.28.process-msgpack.v2",
        },
    )
    changed_execution = build_ptg_small_wave_manifest(
        task_payloads,
        **{**build_options_by_name, "execution_digest": "b" * 64},
    )

    job_bound_variants = (
        baseline,
        changed_payload,
        changed_ids,
        changed_enqueue,
        changed_protocol,
        changed_serializer,
    )
    assert len({wave.jobs_digest for wave in job_bound_variants}) == len(
        job_bound_variants
    )
    assert len({wave.manifest_digest for wave in job_bound_variants}) == len(
        job_bound_variants
    )
    assert changed_execution.jobs_digest == baseline.jobs_digest
    assert changed_execution.manifest_digest != baseline.manifest_digest


def test_manifest_rejects_noncanonical_execution_or_nonexact_job_ids() -> None:
    with pytest.raises(PTGSmallWaveValidationError, match="64 lowercase hexadecimal"):
        build_ptg_small_wave_manifest(
            payloads(1),
            execution_digest="A" * 64,
            job_ids=("db-job-1",),
            enqueue_time_ms=1_700_000_000_000,
        )
    with pytest.raises(PTGSmallWaveValidationError, match="exactly match"):
        build_ptg_small_wave_manifest(
            payloads(2),
            execution_digest=EXECUTION_DIGEST,
            job_ids=("db-job-1",),
            enqueue_time_ms=1_700_000_000_000,
        )
    with pytest.raises(PTGSmallWaveValidationError, match="must be unique"):
        build_ptg_small_wave_manifest(
            payloads(2),
            execution_digest=EXECUTION_DIGEST,
            job_ids=("db-job-1", "db-job-1"),
            enqueue_time_ms=1_700_000_000_000,
        )
