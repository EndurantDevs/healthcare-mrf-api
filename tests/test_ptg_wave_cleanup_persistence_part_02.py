# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partitioned exact-wave cleanup persistence contracts."""

from __future__ import annotations

from tests.test_ptg_wave_cleanup_persistence import (
    _cleanup_operation,
    _kubernetes_evidence,
    _post_cleanup,
    _pre_cleanup,
    _wave,
    cleanup,
    copy,
    pytest,
)


@pytest.mark.parametrize(
    ("builder", "validator", "message"),
    [
        (_post_cleanup, cleanup._validate_redis_post_cleanup_evidence, "cleanup evidence"),
        (_pre_cleanup, cleanup._validate_redis_pre_cleanup_evidence, "pre-cleanup evidence"),
    ],
)
def test_normal_redis_attestation_validators_reject_nonobjects_and_drift(
    builder, validator, message,
):
    wave = _wave()
    valid = builder(wave)
    assert validator(wave, valid) == valid
    with pytest.raises(cleanup.PTGWaveStateConflict, match="must be an object"):
        validator(wave, [])
    invalid = copy.deepcopy(valid)
    invalid["attestation_digest"] = "f" * 64
    with pytest.raises(cleanup.PTGWaveStateConflict, match=message):
        validator(wave, invalid)

@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("result_count", True),
        ("result_count", -1),
        ("result_count", 3),
        ("health_check_count", 2),
        ("queue_entry_count", 1),
    ],
)
def test_pre_cleanup_attestation_rejects_nonidle_counts(field, value):
    wave = _wave()
    evidence = _pre_cleanup(wave)
    evidence[field] = value
    with pytest.raises(cleanup.PTGWaveStateConflict, match="idleness"):
        cleanup._validate_redis_pre_cleanup_evidence(wave, evidence)

def test_cleanup_operation_validators_cover_normal_and_unclaimed_shapes():
    wave = _wave()
    pre = _pre_cleanup(wave)
    normal = _cleanup_operation(wave, pre)
    assert cleanup._validate_redis_cleanup_operation(wave, normal, pre) == normal
    with pytest.raises(cleanup.PTGWaveStateConflict, match="must be an object"):
        cleanup._validate_redis_cleanup_operation(wave, [], pre)
    invalid_by_field = dict(normal, deleted_key_count=True)
    with pytest.raises(cleanup.PTGWaveStateConflict, match="not exact"):
        cleanup._validate_redis_cleanup_operation(wave, invalid_by_field, pre)

    unclaimed_by_field = {
        "schema_version": "healthporta.ptg-wave.redis-unclaimed-cleanup.v1",
        "wave_id": wave.wave_digest,
        "manifest_digest": wave.manifest_digest,
        "target_key_count": 4 + 4 * wave.intent_count,
        "deleted_key_count": 1,
        "expected_attestation_digest": pre["attestation_digest"],
        "attestation": pre,
    }
    assert cleanup._validate_unclaimed_redis_cleanup_operation(
        wave,
        unclaimed_by_field,
        pre,
    ) == unclaimed_by_field
    with pytest.raises(cleanup.PTGWaveStateConflict, match="must be an object"):
        cleanup._validate_unclaimed_redis_cleanup_operation(wave, [], pre)
    with pytest.raises(cleanup.PTGWaveStateConflict, match="not exact"):
        cleanup._validate_unclaimed_redis_cleanup_operation(
            wave,
            dict(unclaimed_by_field, deleted_key_count=-1),
            pre,
        )

def test_unclaimed_post_cleanup_and_digest_validation():
    wave = _wave()
    pre = _pre_cleanup(wave)
    unsigned_by_field = {
        "schema_version": "healthporta.ptg-wave.redis-unclaimed-post-cleanup.v1",
        "wave_id": wave.wave_digest,
        "manifest_digest": wave.manifest_digest,
        "target_key_count": 4 + 4 * wave.intent_count,
        "absent_target_count": 4 + 4 * wave.intent_count,
        "expected_attestation_digest": pre["attestation_digest"],
    }
    post_by_field = {
        **unsigned_by_field,
        "attestation_digest": cleanup.sha256_digest(cleanup.canonical_json(unsigned_by_field)),
    }
    assert cleanup._validate_unclaimed_redis_post_cleanup(wave, post_by_field, pre) == post_by_field
    with pytest.raises(cleanup.PTGWaveStateConflict, match="must be an object"):
        cleanup._validate_unclaimed_redis_post_cleanup(wave, [], pre)
    with pytest.raises(cleanup.PTGWaveStateConflict, match="did not prove"):
        cleanup._validate_unclaimed_redis_post_cleanup(
            wave,
            dict(post_by_field, absent_target_count=0),
            pre,
        )

    assert cleanup._digest_like("a" * 64, "digest") == "a" * 64
    for invalid in (None, "a" * 63, "g" * 64):
        with pytest.raises(cleanup.PTGWaveStateConflict, match="SHA-256"):
            cleanup._digest_like(invalid, "digest")

def test_kubernetes_absence_validator_binds_job_and_never_created_job():
    wave = _wave(kubernetes_delete_ticket="ticket")
    evidence = _kubernetes_evidence(wave)
    assert cleanup._validate_kubernetes_absence_evidence(wave, evidence) == evidence
    with pytest.raises(cleanup.PTGWaveStateConflict, match="must be an object"):
        cleanup._validate_kubernetes_absence_evidence(wave, [])
    with pytest.raises(cleanup.PTGWaveStateConflict, match="does not prove"):
        cleanup._validate_kubernetes_absence_evidence(
            wave,
            dict(evidence, pod_count=1),
        )

    never_created = _wave(
        kubernetes_delete_ticket="ticket",
        kubernetes_job_uid=None,
        kubernetes_job_receipt_digest=None,
    )
    never_created_evidence = _kubernetes_evidence(never_created)
    assert cleanup._validate_kubernetes_absence_evidence(
        never_created,
        never_created_evidence,
    ) == never_created_evidence
