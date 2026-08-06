"""Receipt contracts for claimed-before-start PTG failure waves."""

from __future__ import annotations

import copy

import pytest

import process.ptg_wave_failure as failure
from process.ptg_wave_cleanup import _validate_redis_cleanup_evidence
from process.ptg_wave_state import (
    PTGWaveStateConflict,
    canonical_json,
    sha256_digest,
)
from tests.test_ptg_wave_claimed_prestart_failure import (
    _KEY,
    _claims,
    _intents,
    _kubernetes_failure,
    _link,
    _outcomes,
    _redis_idle,
    _wave,
)


@pytest.mark.parametrize(
    "redis_change",
    ["result", "retry", "in_progress", "health"],
)
def test_claimed_prestart_receipt_rejects_redis_progress_or_active_state(redis_change):
    wave = _wave()
    redis = _redis_idle(wave)
    if redis_change == "result":
        redis["result_ordinals"] = [0]
        redis["queued_ordinals"] = [1, 2]
        redis["job_ordinals"] = [1, 2]
    elif redis_change == "retry":
        redis["retry_ordinals"] = [0]
    elif redis_change == "in_progress":
        redis["in_progress_ordinals"] = [0]
    else:
        redis["health_check_present"] = True
    redis["attestation_digest"] = sha256_digest(canonical_json({
        name: value
        for name, value in redis.items()
        if name != "attestation_digest"
    }))
    with pytest.raises(failure.PTGWaveFailureConflict, match="active lifecycle|progress"):
        failure._claimed_prestart_failure_receipt(
            wave,
            claimed_ordinals=[0, 2],
            kubernetes_evidence=_kubernetes_failure(wave),
            redis_evidence=redis,
        )


def test_claimed_prestart_receipt_rejects_mixed_kubernetes_terminal_state():
    wave = _wave()
    kubernetes = _kubernetes_failure(wave)
    kubernetes["job_succeeded"] = 1
    kubernetes["attestation_digest"] = sha256_digest(canonical_json({
        name: value
        for name, value in kubernetes.items()
        if name != "attestation_digest"
    }))
    with pytest.raises(failure.PTGWaveFailureConflict, match="not exact"):
        failure._claimed_prestart_failure_receipt(
            wave,
            claimed_ordinals=[0, 2],
            kubernetes_evidence=kubernetes,
            redis_evidence=_redis_idle(wave),
        )


def _cleanup_receipt(wave, redis):
    operation_map = {
        "schema_version": "healthporta.ptg-wave.redis-unclaimed-cleanup.v1",
        "wave_id": wave.wave_digest,
        "manifest_digest": wave.manifest_digest,
        "target_key_count": 4 + (4 * wave.intent_count),
        "deleted_key_count": 2 + (2 * wave.intent_count),
        "expected_attestation_digest": redis["attestation_digest"],
        "attestation": redis,
    }
    post_cleanup_evidence_map = {
        "schema_version": "healthporta.ptg-wave.redis-unclaimed-post-cleanup.v1",
        "wave_id": wave.wave_digest,
        "manifest_digest": wave.manifest_digest,
        "target_key_count": 4 + (4 * wave.intent_count),
        "absent_target_count": 4 + (4 * wave.intent_count),
        "expected_attestation_digest": redis["attestation_digest"],
    }
    return {
        "schema_version": "healthporta.ptg-wave.redis-cleanup.v1",
        "operation_ticket": wave.redis_cleanup_ticket,
        "mode": "executed",
        "pre_cleanup": redis,
        "operation_receipt": operation_map,
        "post_cleanup": {
            **post_cleanup_evidence_map,
            "attestation_digest": sha256_digest(
                canonical_json(post_cleanup_evidence_map)
            ),
        },
    }


def test_claimed_prestart_terminal_and_cleanup_modes_retain_first_witnesses():
    """Retain first terminal and cleanup witnesses across reconciliation."""

    wave = _wave()
    intents = _intents()
    claims = _claims(wave, intents)
    outcomes = _outcomes(intents)
    kubernetes = _kubernetes_failure(wave)
    redis = _redis_idle(wave)
    receipt = failure._claimed_prestart_failure_receipt(
        wave,
        claimed_ordinals=[0, 2],
        kubernetes_evidence=kubernetes,
        redis_evidence=redis,
    )
    wave.failure_receipt = receipt
    wave.failure_receipt_digest = sha256_digest(canonical_json(receipt))
    _link(wave, outcomes)
    terminal = failure.verify_claimed_prestart_dead_letter_terminal_eligibility(
        wave,
        intents,
        claims,
        outcomes,
        {"kubernetes": kubernetes, "redis": redis},
        key=_KEY,
    )
    assert terminal["mode"] == "claimed_prestart_failure"
    assert terminal["claimed_ordinals"] == [0, 2]
    wave.terminal_summary = terminal
    cleanup_receipt_map = _cleanup_receipt(wave, redis)
    assert (
        _validate_redis_cleanup_evidence(wave, cleanup_receipt_map)
        == cleanup_receipt_map
    )
    changed = copy.deepcopy(cleanup_receipt_map)
    changed["pre_cleanup"]["health_check_present"] = True
    changed["pre_cleanup"]["attestation_digest"] = sha256_digest(canonical_json({
        name: expected_value
        for name, expected_value in changed["pre_cleanup"].items()
        if name != "attestation_digest"
    }))
    with pytest.raises(
        PTGWaveStateConflict,
        match="terminal pre-cleanup attestation",
    ):
        _validate_redis_cleanup_evidence(wave, changed)
