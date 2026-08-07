# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partitioned exact-wave failure and controller edge contracts."""

from __future__ import annotations

from tests.test_ptg_wave_failure_controller_edges import (
    AsyncMock,
    Mock,
    _LINKAGE_KEY,
    _Transaction,
    _claimed_receipt,
    _intent,
    _outcome,
    _preclaim_evidence,
    _redis_failure_receipt,
    _unclaimed_receipt,
    _wave,
    canonical_json,
    failure_kubernetes,
    failure_receipts,
    failure_snapshots,
    failure_terminal,
    failure_types,
    failure_validation,
    pytest,
    sha256_digest,
    types,
)


@pytest.mark.asyncio
async def test_unclaimed_failure_snapshot_replays_and_rejects_invalid_slots(monkeypatch):
    receipt_by_field = {"receipt": "synthetic"}
    wave = _wave(state="slots_waiting")
    session = object()
    facade = types.SimpleNamespace(
        db=types.SimpleNamespace(transaction=lambda: _Transaction(session)),
        _locked_wave=AsyncMock(return_value=wave),
    )
    monkeypatch.setattr(
        failure_snapshots,
        "_require_unclaimed_failure_receipt",
        Mock(return_value=receipt_by_field),
    )
    monkeypatch.setattr(
        failure_snapshots, "_unclaimed_snapshot_rows", AsyncMock(return_value=([], []))
    )
    persist = AsyncMock(return_value="digest")
    monkeypatch.setattr(failure_snapshots, "persist_dead_letter_snapshot", persist)
    assert await failure_snapshots.snapshot_unclaimed_dead_letter_outcomes(
        facade, wave.wave_id, failure_receipt=receipt_by_field
    ) == "digest"
    persist.assert_awaited_once()

    with pytest.raises(failure_types.PTGWaveFailureConflict, match="12-slot"):
        failure_snapshots._ready_slots_by_number(
            _wave(kubernetes_ready_attestation={"slots": []})
        )
    malformed = _wave()
    malformed.kubernetes_ready_attestation["slots"][0]["slot"] = 12
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="12-slot"):
        failure_snapshots._ready_slots_by_number(malformed)


@pytest.mark.asyncio
async def test_claimed_failure_snapshot_binds_evidence_and_state(monkeypatch):
    wave = _wave(state="slots_waiting")
    session = object()
    facade = types.SimpleNamespace(
        db=types.SimpleNamespace(transaction=lambda: _Transaction(session)),
        _locked_wave=AsyncMock(return_value=wave),
    )
    kubernetes_evidence, redis_evidence = {"k": 1}, {"r": 1}
    claimed_receipt_by_field = {
        "kubernetes_evidence": kubernetes_evidence,
        "redis_evidence": redis_evidence,
    }
    wave.state = "awaiting_linkage"
    wave.outcomes_digest = "a" * 64
    monkeypatch.setattr(
        failure_snapshots,
        "_require_claimed_prestart_failure_receipt",
        Mock(return_value=claimed_receipt_by_field),
    )
    assert failure_snapshots._existing_claimed_outcomes_digest(
        wave, kubernetes_evidence, redis_evidence
    ) == wave.outcomes_digest
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="conflicts"):
        failure_snapshots._existing_claimed_outcomes_digest(
            wave, {"k": "foreign"}, redis_evidence
        )
    monkeypatch.setattr(
        failure_snapshots, "_existing_claimed_outcomes_digest", Mock(return_value="digest")
    )
    assert await failure_snapshots.snapshot_claimed_prestart_dead_letter_outcomes(
        facade,
        wave.wave_id,
        kubernetes_evidence=kubernetes_evidence,
        redis_evidence=redis_evidence,
    ) == "digest"
    wave.state = "slots_waiting"
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="not expected"):
        await failure_snapshots.snapshot_claimed_prestart_dead_letter_outcomes(
            facade,
            wave.wave_id,
            kubernetes_evidence=kubernetes_evidence,
            redis_evidence=redis_evidence,
        )


def test_failure_receipt_confirmation_requires_valid_digest(monkeypatch):
    wave = _wave()
    confirmed_by_field = {"reason": "pre_claim_failure"}
    wave.failure_receipt = confirmed_by_field
    wave.failure_receipt_digest = sha256_digest(canonical_json(confirmed_by_field))
    monkeypatch.setattr(
        failure_receipts, "_require_failure_receipt", Mock(return_value=confirmed_by_field)
    )
    assert failure_receipts._confirmed_failure_reason(wave) == "pre_claim_failure"
    wave.failure_receipt_digest = "0" * 64
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="digest is corrupt"):
        failure_receipts._confirmed_failure_reason(wave)


def test_unclaimed_receipt_rejects_invalid_post_absence():
    wave = _wave()
    envelope = _unclaimed_receipt(
        wave,
        reason="pre_claim_failure",
        evidence=_preclaim_evidence(wave),
        origin_state="released",
        operation="worker_start",
        ticket=None,
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="origin state"):
        failure_receipts._validate_unclaimed_receipt_envelope(
            wave, envelope, require_origin_state=True
        )

    post_wave = _wave(
        state="slots_waiting", kubernetes_job_uid=None, kubernetes_job_receipt_digest=None
    )
    post = _unclaimed_receipt(
        post_wave,
        reason="kubernetes_post_absent",
        evidence={},
        origin_state="slots_waiting",
        operation="kubernetes_post",
        ticket=post_wave.k8s_post_ticket,
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="POST absence"):
        failure_receipts._validate_kubernetes_post_absence(post_wave, post, {})


def test_failure_receipt_rejects_invalid_release_and_preclaim_absence():
    wave = _wave()
    redis_wave = _wave(state="redis_releasing", redis_release_ticket="release-ticket")
    redis = _unclaimed_receipt(
        redis_wave,
        reason="redis_release_absent",
        evidence={},
        origin_state="foreign",
        operation="redis_release",
        ticket="release-ticket",
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="Redis release"):
        failure_receipts._validate_redis_release_absence(redis_wave, redis, {})

    preclaim = _unclaimed_receipt(
        wave,
        reason="pre_claim_failure",
        evidence=_preclaim_evidence(wave),
        origin_state="executing",
        operation="foreign",
        ticket=None,
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="pre-claim"):
        failure_receipts._validate_preclaim_failure(wave, preclaim, preclaim["evidence"])


def test_claimed_failure_receipt_requires_bound_operation_and_state():
    wave = _wave()
    claimed = _claimed_receipt(wave, claimed_ordinals=[0])
    claimed["operation"] = "foreign"
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="does not bind"):
        failure_receipts._validate_claimed_receipt_envelope(
            wave, claimed, require_origin_state=False
        )
    claimed = _claimed_receipt(wave, claimed_ordinals=[0])
    wave.state = "released"
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="origin state"):
        failure_receipts._validate_claimed_receipt_envelope(
            wave, claimed, require_origin_state=True
        )

def test_failure_validation_residual_fail_closed_branches(monkeypatch):
    wave = _wave(linkage_ack={}, linkage_ack_digest="a" * 64)
    monkeypatch.setattr(
        failure_validation,
        "_validate_linkage_ack",
        Mock(side_effect=failure_validation.PTGWaveStateConflict("invalid")),
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="does not cover"):
        failure_validation._verify_linkage(wave, [], key=_LINKAGE_KEY)

    claimed = _claimed_receipt(wave, claimed_ordinals=[0])
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="differs"):
        failure_validation._validate_redis_receipt_envelope(
            wave, claimed, _redis_failure_receipt(wave)
        )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="fields are not exact"):
        failure_validation._validate_redis_receipt_envelope(wave, {}, {})
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="does not bind"):
        failure_validation._validate_redis_receipt_envelope(
            wave,
            {},
            _redis_failure_receipt(wave, wave_id="foreign"),
        )
    corrupt = _redis_failure_receipt(wave)
    corrupt["attestation_digest"] = "0" * 64
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="digest is invalid"):
        failure_validation._validate_redis_receipt_envelope(wave, {}, corrupt)

    no_ready_wave = _wave(kubernetes_ready_attestation=None)
    corrupt_ready = _redis_failure_receipt(no_ready_wave)
    corrupt_ready["ready_slots_digest"] = "0" * 64
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="membership digest"):
        failure_validation._validate_redis_ready_membership(
            no_ready_wave,
            {"reason": "redis_release_absent", "origin_state": "redis_releasing"},
            corrupt_ready,
            False,
        )

def test_failure_terminal_and_kubernetes_residual_fail_closed_branches(monkeypatch):
    intent = _intent(0)
    outcome = _outcome(intent, status="dead_letter")
    receipt_by_field = {"failure": "synthetic"}
    wave = _wave(intent_count=1, outcomes_digest="0" * 64)
    monkeypatch.setattr(
        failure_terminal, "_require_unclaimed_failure_receipt", Mock(return_value=receipt_by_field)
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="outcomes digest"):
        failure_terminal.verify_unclaimed_dead_letter_terminal_eligibility(
            wave, [intent], [], [outcome], {}, key=_LINKAGE_KEY
        )
    dead_letter_records = failure_terminal._dead_letter_records([intent], [outcome], "invalid")
    wave.outcomes_digest = failure_types._outcomes_digest(dead_letter_records)
    wave.failure_receipt_digest = "0" * 64
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="receipt digest"):
        failure_terminal.verify_unclaimed_dead_letter_terminal_eligibility(
            wave, [intent], [], [outcome], {}, key=_LINKAGE_KEY
        )

    claimed = _wave(intent_count=1, outcomes_digest="0" * 64)
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="outcomes digest"):
        failure_terminal.verify_claimed_prestart_terminal_eligibility(
            claimed, [intent], [], [outcome], {}, key=_LINKAGE_KEY
        )

    delete_wave = _wave(kubernetes_delete_evidence={"observed": True})
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="persisted receipt"):
        failure_kubernetes._verify_failure_kubernetes(
            delete_wave, {"reason": "redis_release_absent"}, {"observed": False}
        )
