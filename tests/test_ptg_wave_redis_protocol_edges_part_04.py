# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partitioned exact PTG Redis protocol edge contracts."""

from __future__ import annotations

from tests.test_ptg_wave_redis_protocol_edges import (
    PTGSmallWaveValidationError,
    _adapter_identity,
    claim_adapter,
    make_manifest,
    pytest,
    redis_models,
    reference_module,
    replace,
    restore_module,
    unclaimed_models,
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
