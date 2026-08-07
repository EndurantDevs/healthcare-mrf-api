# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partitioned exact-wave failure and controller edge contracts."""

from __future__ import annotations

from tests.test_ptg_wave_failure_controller_edges import (
    AsyncMock,
    Mock,
    _LINKAGE_KEY,
    _SequenceSession,
    _Transaction,
    _claim,
    _intent,
    _outcome,
    _redis_failure_receipt,
    _wave,
    canonical_json,
    failure,
    failure_persistence,
    failure_snapshots,
    failure_terminal,
    failure_types,
    failure_validation,
    outcomes,
    pytest,
    sha256_digest,
    types,
)


def test_failure_validation_verifies_signed_linkage_ack():
    wave = _wave(intent_count=1, outcomes_digest="a" * 64)
    terminal_outcome = _outcome(_intent(0), status="dead_letter")
    unsigned_ack_by_field = {
        "schema_version": "healthporta.ptg-wave-linkage-ack.v1",
        "wave_id": wave.wave_id,
        "wave_digest": wave.wave_digest,
        "intent_count": wave.intent_count,
        "mapping_digest": outcomes.linkage_mapping_digest([terminal_outcome]),
        "outcomes_digest": wave.outcomes_digest,
    }
    wave.linkage_ack = {
        **unsigned_ack_by_field,
        "signature": outcomes.sign_linkage_ack(unsigned_ack_by_field, key=_LINKAGE_KEY),
    }
    wave.linkage_ack_digest = sha256_digest(canonical_json(wave.linkage_ack))
    failure_validation._verify_linkage(
        wave, [terminal_outcome], key=_LINKAGE_KEY
    )
    wave.linkage_ack_digest = "0" * 64
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="does not cover"):
        failure_validation._verify_linkage(
            wave, [terminal_outcome], key=_LINKAGE_KEY
        )
    wave.linkage_ack = None
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="requires linkage"):
        failure_validation._verify_linkage(
            wave, [terminal_outcome], key=_LINKAGE_KEY
        )


def test_failure_validation_rejects_bad_redis_envelope():
    wave = _wave(kubernetes_ready_attestation=None)
    receipt = _redis_failure_receipt(wave)
    assert failure_validation._validate_redis_receipt_envelope(
        wave, {"reason": "kubernetes_post_absent"}, receipt
    ) == (receipt, False)
    receipt["health_check_present"] = 1
    receipt["attestation_digest"] = sha256_digest(canonical_json({
        name: field_value
        for name, field_value in receipt.items()
        if name != "attestation_digest"
    }))
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="health-check"):
        failure_validation._validate_redis_receipt_envelope(
            wave, {"reason": "kubernetes_post_absent"}, receipt
        )


def test_failure_validation_requires_canonical_ready_membership():
    ready_wave = _wave()
    expected_ready_slots = failure_validation._expected_redis_ready_slots(ready_wave)
    failure_validation._validate_partial_ready_membership(
        expected_ready_slots, expected_ready_slots[:1]
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="membership is invalid"):
        failure_validation._validate_partial_ready_membership(expected_ready_slots, "invalid")
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="canonical Kubernetes"):
        failure_validation._validate_partial_ready_membership(
            expected_ready_slots, list(reversed(expected_ready_slots[:2]))
        )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="lacks exact"):
        failure_validation._expected_redis_ready_slots(
            _wave(kubernetes_ready_attestation={"slots": []})
        )


def test_failure_validation_rejects_release_presence():
    wave = _wave(kubernetes_ready_attestation=None)
    lifecycle = failure_validation.FailureRedisOrdinals(
        queued=set(),
        jobs=set(),
        results=set(),
        retries=set(),
        in_progress=set(),
    )
    failure_validation._validate_redis_release(
        wave,
        {"reason": "kubernetes_post_absent"},
        _redis_failure_receipt(wave),
        [],
        lifecycle,
        require_release_absent=True,
    )
    invalid_release = _redis_failure_receipt(wave, release_present=True)
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="presence conflicts"):
        failure_validation._validate_redis_release(
            wave,
            {"reason": "kubernetes_post_absent"},
            invalid_release,
            [],
            lifecycle,
            require_release_absent=True,
        )

@pytest.mark.asyncio
async def test_failure_snapshot_unclaimed_paths_and_row_guards(monkeypatch):
    wave = _wave(state="slots_waiting")
    intents = [_intent(0), _intent(1)]
    session = _SequenceSession([[], intents, []])
    runs = [types.SimpleNamespace(status="failed"), types.SimpleNamespace(status="failed")]
    monkeypatch.setattr(
        failure_snapshots, "_locked_wave_runs", AsyncMock(return_value=runs)
    )
    assert await failure_snapshots._unclaimed_snapshot_rows(
        session, wave, wave.wave_id
    ) == (intents, runs)

    claimed_session = _SequenceSession([[0]])
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="claimed wave"):
        await failure_snapshots._unclaimed_snapshot_rows(
            claimed_session, wave, wave.wave_id
        )

    missing_intents = _SequenceSession([[], [_intent(0)]])
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="every admitted"):
        await failure_snapshots._unclaimed_snapshot_rows(
            missing_intents, wave, wave.wave_id
        )

    complete_intents = [_intent(0), _intent(1)]
    short_runs = _SequenceSession([[], complete_intents])
    monkeypatch.setattr(
        failure_snapshots, "_locked_wave_runs", AsyncMock(return_value=[object()])
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="every admitted ImportRun"):
        await failure_snapshots._unclaimed_snapshot_rows(
            short_runs, wave, wave.wave_id
        )

    succeeded_runs = [
        types.SimpleNamespace(status="succeeded"),
        types.SimpleNamespace(status="failed"),
    ]
    succeeded = _SequenceSession([[], complete_intents])
    monkeypatch.setattr(
        failure_snapshots, "_locked_wave_runs", AsyncMock(return_value=succeeded_runs)
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="successful"):
        await failure_snapshots._unclaimed_snapshot_rows(
            succeeded, wave, wave.wave_id
        )

    existing = _SequenceSession([[0]])
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="already exist"):
        await failure_snapshots._require_no_existing_outcomes(existing, wave.wave_id)

@pytest.mark.asyncio
async def test_failure_snapshot_wrappers_event_markers_and_claimed_guards(monkeypatch):
    receipt_by_field = {"receipt": "synthetic"}
    wave = _wave(
        state="awaiting_linkage",
        failure_receipt_digest=sha256_digest(canonical_json(receipt_by_field)),
        outcomes_digest="a" * 64,
    )
    session = object()
    facade = types.SimpleNamespace(
        db=types.SimpleNamespace(transaction=lambda: _Transaction(session)),
        _locked_wave=AsyncMock(return_value=wave),
    )
    assert await failure_snapshots.snapshot_unclaimed_dead_letter_outcomes(
        facade, wave.wave_id, failure_receipt=receipt_by_field
    ) == wave.outcomes_digest

    wave.failure_receipt_digest = "0" * 64
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="conflicts"):
        await failure_snapshots.snapshot_unclaimed_dead_letter_outcomes(
            facade, wave.wave_id, failure_receipt=receipt_by_field
        )

    wave.state = "foreign"
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="not expected"):
        await failure_snapshots.snapshot_unclaimed_dead_letter_outcomes(
            facade, wave.wave_id, failure_receipt=receipt_by_field
        )

    marker_session = _SequenceSession([
        [types.SimpleNamespace(_mapping={"outer_run_id": "run-synthetic-1"})]
    ])
    assert await failure_snapshots._worker_start_event_ordinals(
        marker_session, [_intent(0), _intent(1)]
    ) == [1]
    invalid_marker_session = _SequenceSession([[("foreign-run",)]])
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="observation is invalid"):
        await failure_snapshots._worker_start_event_ordinals(
            invalid_marker_session, [_intent(0)]
        )

    claimed_wave = _wave(state="released")
    claimed_session = _SequenceSession([[_intent(0)]])
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="every admitted"):
        await failure_snapshots._claimed_snapshot_rows(
            claimed_session, claimed_wave, claimed_wave.wave_id
        )

    intents = [_intent(0), _intent(1)]
    claimed_rows = _SequenceSession([intents, [_claim(claimed_wave, intents[0])]])
    monkeypatch.setattr(
        failure_snapshots,
        "_locked_wave_runs",
        AsyncMock(return_value=[object(), object()]),
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="conflict"):
        await failure_snapshots._claimed_snapshot_rows(
            claimed_rows, claimed_wave, claimed_wave.wave_id
        )

@pytest.mark.asyncio
async def test_failure_facade_and_persistence_write_exact_dead_letter_state(monkeypatch):
    monkeypatch.setattr(failure, "_snapshot_unclaimed", AsyncMock(return_value="digest"))
    assert await failure.snapshot_unclaimed_dead_letter_outcomes(
        "wave-synthetic", failure_receipt={"synthetic": True}
    ) == "digest"

    monkeypatch.setattr(
        failure_persistence,
        "PTGImportWaveOutcome",
        lambda **kwargs: types.SimpleNamespace(**kwargs),
    )
    transition = AsyncMock()
    facade = types.SimpleNamespace(_transition=transition)
    wave = _wave()
    intents = [_intent(0), _intent(1)]
    runs = [types.SimpleNamespace(), types.SimpleNamespace()]
    snapshot = failure_persistence.DeadLetterSnapshot(
        session=_SequenceSession([]),
        wave=wave,
        wave_id=wave.wave_id,
        intents=intents,
        runs=runs,
        receipt={"failure": "synthetic"},
        receipt_digest="a" * 64,
        is_claimed_prestart=False,
    )
    digest = await failure_persistence.persist_dead_letter_snapshot(facade, snapshot)
    assert len(digest) == 64
    assert all(run.status == "dead_letter" for run in runs)
    assert all("worker claim" in run.phase_detail for run in runs)
    transition.assert_awaited_once()

    claimed_run = types.SimpleNamespace()
    failure_persistence._dead_letter_runs(
        [claimed_run], object(), is_claimed_prestart=True
    )
    assert claimed_run.error["code"] == "ptg_exact_wave_claimed_prestart_failure"
    assert claimed_run.progress["message"] == "dead letter"

def test_failure_terminal_and_type_helpers_reject_any_nonexact_evidence(monkeypatch):
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="SHA-256"):
        failure_types._digest("invalid", "synthetic")
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="contiguous"):
        failure_types._rows_by_ordinal([types.SimpleNamespace(ordinal=1)])
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="object"):
        failure_types._require_mapping([], "synthetic")

    wave = _wave(intent_count=1)
    intent = _intent(0)
    outcome = _outcome(intent, status="dead_letter")
    dead_letter_records = failure_terminal._dead_letter_records(
        [intent], [outcome], "not dead letter"
    )
    wave.outcomes_digest = failure_types._outcomes_digest(dead_letter_records)
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="every admitted"):
        failure_terminal._require_exact_coverage(wave, [], [], "every admitted")
    outcome.status = "failed"
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="not dead letter"):
        failure_terminal._dead_letter_records(
            [intent], [outcome], "not dead letter"
        )

    receipt_by_field = {"claimed_ordinals": [0]}
    wave.failure_receipt = receipt_by_field
    wave.failure_receipt_digest = sha256_digest(canonical_json(receipt_by_field))
    monkeypatch.setattr(
        failure_terminal,
        "_require_claimed_prestart_failure_receipt",
        Mock(return_value=receipt_by_field),
    )
    assert failure_terminal._claimed_failure_receipt(wave, [0])[0] == receipt_by_field
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="differ"):
        failure_terminal._claimed_failure_receipt(wave, [])
    wave.failure_receipt_digest = "0" * 64
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="digest is corrupt"):
        failure_terminal._claimed_failure_receipt(wave, [0])

    monkeypatch.setattr(
        failure_terminal, "_verify_failure_kubernetes", Mock(return_value={"k": 1})
    )
    monkeypatch.setattr(
        failure_terminal, "_verify_failure_redis", Mock(return_value={"r": 1})
    )
    assert failure_terminal._terminal_receipt_evidence(
        wave,
        receipt_by_field,
        {"kubernetes": {}, "redis": {}},
        receipt_name="synthetic",
        fields_error="exact fields",
    ) == ({"k": 1}, {"r": 1})
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="exact fields"):
        failure_terminal._terminal_receipt_evidence(
            wave,
            receipt_by_field,
            {"kubernetes": {}},
            receipt_name="synthetic",
            fields_error="exact fields",
        )
