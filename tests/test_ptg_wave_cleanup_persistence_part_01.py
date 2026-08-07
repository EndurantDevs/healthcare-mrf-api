# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partitioned exact-wave cleanup persistence contracts."""

from __future__ import annotations

from tests.test_ptg_wave_cleanup_persistence import (
    AsyncMock,
    Mock,
    _Result,
    _Session,
    _cleanup_evidence,
    _install_wave,
    _kubernetes_evidence,
    _wave,
    cleanup,
    copy,
    pytest,
)


@pytest.mark.asyncio
async def test_begin_terminalizing_requires_both_linkage_witnesses(monkeypatch):
    wave = _wave(state="awaiting_linkage", outcomes_digest=None)
    _install_wave(monkeypatch, wave)
    with pytest.raises(cleanup.PTGWaveStateConflict, match="outcomes and linkage"):
        await cleanup.begin_terminalizing(wave.wave_id)
    wave.outcomes_digest = "6" * 64
    wave.linkage_ack_digest = None
    with pytest.raises(cleanup.PTGWaveStateConflict, match="outcomes and linkage"):
        await cleanup.begin_terminalizing(wave.wave_id)
    wave.linkage_ack_digest = "7" * 64
    transition = AsyncMock()
    monkeypatch.setattr(cleanup, "_transition", transition)
    await cleanup.begin_terminalizing(wave.wave_id)
    assert transition.await_args.args[2] == "terminalizing"

@pytest.mark.asyncio
@pytest.mark.parametrize("verification_mode", ["normal", "unclaimed", "claimed"])
async def test_terminal_evidence_selects_the_exact_verifier(
    monkeypatch, verification_mode,
):
    wave = _wave(state="terminalizing")
    if verification_mode != "normal":
        wave.failure_receipt_digest = "f" * 64
    if verification_mode == "claimed":
        wave.failure_receipt = {"reason": "claimed_prestart_failure"}
    session = _install_wave(
        monkeypatch,
        wave,
        _Session(_Result(rows=["intent"]), _Result(rows=["claim"]), _Result(rows=["outcome"])),
    )
    monkeypatch.setattr(
        cleanup,
        "is_claimed_prestart_failure_receipt",
        Mock(return_value=verification_mode == "claimed"),
    )
    verifiers_by_field = {
        "normal": "verify_terminal_eligibility",
        "unclaimed": "verify_unclaimed_dead_letter_terminal_eligibility",
        "claimed": "verify_claimed_prestart_dead_letter_terminal_eligibility",
    }
    verifier = Mock(return_value={"verified": verification_mode})
    monkeypatch.setattr(cleanup, verifiers_by_field[verification_mode], verifier)
    transition = AsyncMock()
    monkeypatch.setattr(cleanup, "_transition", transition)

    digest = await cleanup.persist_terminal_evidence(wave.wave_id, {"external": True})
    assert digest == cleanup.sha256_digest(
        cleanup.canonical_json({"verified": verification_mode})
    )
    verifier.assert_called_once_with(
        wave,
        ["intent"],
        ["claim"],
        ["outcome"],
        {"external": True},
    )
    assert transition.await_args.args[2] == "cleaning"
    assert session.results == []

@pytest.mark.asyncio
async def test_terminal_evidence_rejects_wrong_state_and_missing_linkage(monkeypatch):
    wave = _wave(state="cleaning")
    _install_wave(monkeypatch, wave)
    with pytest.raises(cleanup.PTGWaveStateConflict, match="not expected"):
        await cleanup.persist_terminal_evidence(wave.wave_id, {})
    wave.state = "terminalizing"
    wave.outcomes_digest = None
    with pytest.raises(cleanup.PTGWaveStateConflict, match="lacks stable"):
        await cleanup.persist_terminal_evidence(wave.wave_id, {})

@pytest.mark.asyncio
async def test_redis_cleanup_marker_is_one_shot(monkeypatch):
    wave = _wave(redis_cleanup_ticket="existing")
    session = _install_wave(monkeypatch, wave)
    assert await cleanup.mark_redis_cleanup_started(
        wave.wave_id,
        operation_ticket="candidate",
    ) == {"owner": False}

    wave.redis_cleanup_ticket = None
    wave.state = "terminalizing"
    with pytest.raises(cleanup.PTGWaveStateConflict, match="not expected"):
        await cleanup.mark_redis_cleanup_started(wave.wave_id, operation_ticket="candidate")
    wave.state = "cleaning"
    wave.terminal_evidence_digest = None
    with pytest.raises(cleanup.PTGWaveStateConflict, match="not expected"):
        await cleanup.mark_redis_cleanup_started(wave.wave_id, operation_ticket="candidate")
    wave.terminal_evidence_digest = "8" * 64
    wave.redis_cleanup_started_at = object()
    with pytest.raises(cleanup.PTGWaveStateConflict, match="GET only"):
        await cleanup.mark_redis_cleanup_started(wave.wave_id, operation_ticket="candidate")

    wave.redis_cleanup_started_at = None
    receipt = await cleanup.mark_redis_cleanup_started(
        wave.wave_id,
        operation_ticket="candidate",
    )
    assert receipt["owner"] is True
    assert receipt["release_digest"] == "5" * 64
    assert session.flush_count == 1

@pytest.mark.asyncio
async def test_redis_cleanup_evidence_first_write_replay_and_conflict(monkeypatch):
    with pytest.raises(cleanup.PTGWaveStateConflict, match="must be an object"):
        await cleanup.record_redis_cleanup_absent("wave-unit", [])

    wave = _wave(redis_cleanup_ticket="ticket", redis_cleanup_started_at=object())
    session = _install_wave(monkeypatch, wave)
    evidence = _cleanup_evidence(wave)
    digest = await cleanup.record_redis_cleanup_absent(wave.wave_id, evidence)
    assert wave.redis_cleanup_evidence_digest == digest
    assert session.flush_count == 1
    assert await cleanup.record_redis_cleanup_absent(wave.wave_id, evidence) == digest
    wave.redis_cleanup_evidence_digest = "f" * 64
    with pytest.raises(cleanup.PTGWaveStateConflict, match="conflicts"):
        await cleanup.record_redis_cleanup_absent(wave.wave_id, evidence)
    wave.state = "terminalizing"
    with pytest.raises(cleanup.PTGWaveStateConflict, match="not expected"):
        await cleanup.record_redis_cleanup_absent(wave.wave_id, evidence)

@pytest.mark.asyncio
async def test_kubernetes_delete_marker_replays_and_requires_redis_absence(monkeypatch):
    wave = _wave(kubernetes_delete_ticket="existing")
    _install_wave(monkeypatch, wave)
    assert await cleanup.mark_kubernetes_delete_started(
        wave.wave_id,
        operation_ticket="candidate",
    ) == {"owner": False}

    wave.kubernetes_delete_ticket = None
    wave.redis_cleanup_evidence_digest = None
    with pytest.raises(cleanup.PTGWaveStateConflict, match="Redis absence"):
        await cleanup.mark_kubernetes_delete_started(wave.wave_id, operation_ticket="candidate")
    wave.redis_cleanup_evidence_digest = "c" * 64
    wave.kubernetes_delete_started_at = object()
    with pytest.raises(cleanup.PTGWaveStateConflict, match="GET only"):
        await cleanup.mark_kubernetes_delete_started(wave.wave_id, operation_ticket="candidate")


@pytest.mark.asyncio
async def test_kubernetes_delete_marker_starts_normal_deletion(monkeypatch):
    wave = _wave(redis_cleanup_evidence_digest="c" * 64)
    session = _install_wave(monkeypatch, wave)

    wave.kubernetes_delete_started_at = None
    receipt = await cleanup.mark_kubernetes_delete_started(
        wave.wave_id,
        operation_ticket="candidate",
    )
    assert receipt["delete_permitted"] is True
    assert session.flush_count == 1


@pytest.mark.asyncio
async def test_kubernetes_delete_marker_allows_never_created_job(monkeypatch):
    never_created = _wave(
        kubernetes_job_uid=None,
        kubernetes_job_receipt_digest=None,
        redis_cleanup_evidence_digest="c" * 64,
    )
    _install_wave(monkeypatch, never_created)
    receipt = await cleanup.mark_kubernetes_delete_started(
        never_created.wave_id,
        operation_ticket="never-created",
    )
    assert receipt["delete_permitted"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides_by_field",
    (
        {"k8s_post_started_at": None},
        {"kubernetes_job_receipt_digest": "9" * 64},
    ),
)
async def test_kubernetes_delete_marker_rejects_never_created_job_with_post_state(
    monkeypatch,
    overrides_by_field,
):
    invalid_wave = _wave(
        kubernetes_job_uid=None,
        redis_cleanup_evidence_digest="c" * 64,
        **overrides_by_field,
    )
    _install_wave(monkeypatch, invalid_wave)
    with pytest.raises(cleanup.PTGWaveStateConflict, match="persisted POST"):
        await cleanup.mark_kubernetes_delete_started(
            invalid_wave.wave_id,
            operation_ticket="never-created",
        )


@pytest.mark.asyncio
async def test_kubernetes_delete_marker_allows_early_failure(monkeypatch):
    early = _wave(
        state="terminalizing",
        failure_receipt={"reason": "redis_release_absent"},
        linkage_ack_digest="7" * 64,
    )
    _install_wave(monkeypatch, early)
    assert (await cleanup.mark_kubernetes_delete_started(
        early.wave_id,
        operation_ticket="early",
    ))["owner"] is True

@pytest.mark.asyncio
async def test_kubernetes_absence_first_write_replay_and_early_failure(monkeypatch):
    with pytest.raises(cleanup.PTGWaveStateConflict, match="must be an object"):
        await cleanup.record_kubernetes_delete_absent("wave-unit", [])

    wave = _wave(kubernetes_delete_ticket="ticket", kubernetes_delete_started_at=object())
    session = _install_wave(monkeypatch, wave)
    evidence = _kubernetes_evidence(wave)
    digest = await cleanup.record_kubernetes_delete_absent(wave.wave_id, evidence)
    assert wave.kubernetes_delete_evidence_digest == digest
    assert session.flush_count == 1
    assert await cleanup.record_kubernetes_delete_absent(wave.wave_id, evidence) == digest
    wave.kubernetes_delete_evidence_digest = "f" * 64
    with pytest.raises(cleanup.PTGWaveStateConflict, match="conflicts"):
        await cleanup.record_kubernetes_delete_absent(wave.wave_id, evidence)

    wrong = _wave(state="terminalizing", kubernetes_delete_started_at=None)
    _install_wave(monkeypatch, wrong)
    with pytest.raises(cleanup.PTGWaveStateConflict, match="not expected"):
        await cleanup.record_kubernetes_delete_absent(wrong.wave_id, evidence)

    early = _wave(
        state="terminalizing",
        failure_receipt={"reason": "redis_release_absent"},
        kubernetes_delete_ticket="ticket",
        kubernetes_delete_started_at=object(),
    )
    _install_wave(monkeypatch, early)
    early_evidence = _kubernetes_evidence(early)
    assert await cleanup.record_kubernetes_delete_absent(
        early.wave_id,
        early_evidence,
    )

@pytest.mark.asyncio
async def test_final_cleanup_requires_all_exact_receipts_and_derives_state(monkeypatch):
    wave = _wave(state="terminalizing")
    _install_wave(monkeypatch, wave)
    with pytest.raises(cleanup.PTGWaveStateConflict, match="before terminal evidence"):
        await cleanup.persist_cleanup_and_terminal(wave.wave_id)

    wave.state = "cleaning"
    wave.redis_cleanup_started_at = object()
    with pytest.raises(cleanup.PTGWaveStateConflict, match="markers"):
        await cleanup.persist_cleanup_and_terminal(wave.wave_id)

    wave.redis_cleanup_ticket = "redis-ticket"
    wave.redis_cleanup_evidence = _cleanup_evidence(wave)
    wave.redis_cleanup_evidence_digest = cleanup.sha256_digest(
        cleanup.canonical_json(wave.redis_cleanup_evidence)
    )
    wave.kubernetes_delete_ticket = "delete-ticket"
    wave.kubernetes_delete_started_at = object()
    wave.kubernetes_delete_evidence = _kubernetes_evidence(wave)
    wave.kubernetes_delete_evidence_digest = "corrupt"
    with pytest.raises(cleanup.PTGWaveStateConflict, match="digest is corrupt"):
        await cleanup.persist_cleanup_and_terminal(wave.wave_id)

    wave.kubernetes_delete_evidence_digest = cleanup.sha256_digest(
        cleanup.canonical_json(wave.kubernetes_delete_evidence)
    )
    session = _install_wave(monkeypatch, wave, _Session(_Result(rows=["outcome"])))
    monkeypatch.setattr(cleanup, "_derive_terminal_state", Mock(return_value="succeeded"))
    transition = AsyncMock()
    monkeypatch.setattr(cleanup, "_transition", transition)
    digest = await cleanup.persist_cleanup_and_terminal(wave.wave_id)
    assert len(digest) == 64
    assert transition.await_args.args[2] == "succeeded"
    assert transition.await_args.kwargs["values"]["cleanup_summary"]["terminal_state"] == "succeeded"
    assert session.results == []

def test_normal_cleanup_validators_accept_executed_and_get_only_receipts():
    wave = _wave(redis_cleanup_ticket="ticket")
    executed = _cleanup_evidence(wave)
    assert cleanup._validate_redis_cleanup_evidence(wave, executed) == executed
    get_only = _cleanup_evidence(wave, mode="get_only_reconciled")
    assert cleanup._validate_redis_cleanup_evidence(wave, get_only) == get_only
    invented = copy.deepcopy(get_only)
    invented["operation_receipt"] = {}
    with pytest.raises(cleanup.PTGWaveStateConflict, match="cannot invent"):
        cleanup._validate_redis_cleanup_evidence(wave, invented)

def test_failure_cleanup_dispatches_to_failure_specific_validators(monkeypatch):
    wave = _wave(
        redis_cleanup_ticket="ticket",
        failure_receipt={"reason": "unclaimed"},
        failure_receipt_digest="f" * 64,
    )
    pre_by_field = {"attestation_digest": "b" * 64}
    wave.terminal_summary = {"redis_pre_cleanup": pre_by_field}
    evidence_by_field = {
        "schema_version": "healthporta.ptg-wave.redis-cleanup.v1",
        "operation_ticket": "ticket",
        "mode": "executed",
        "pre_cleanup": pre_by_field,
        "operation_receipt": {"operation": True},
        "post_cleanup": {"post": True},
    }
    verify = Mock()
    validate_post = Mock()
    validate_operation = Mock()
    monkeypatch.setattr(cleanup, "_verify_failure_redis", verify)
    monkeypatch.setattr(cleanup, "_validate_unclaimed_redis_post_cleanup", validate_post)
    monkeypatch.setattr(cleanup, "_validate_unclaimed_redis_cleanup_operation", validate_operation)
    assert cleanup._validate_redis_cleanup_evidence(wave, evidence_by_field) == evidence_by_field
    verify.assert_called_once()
    validate_post.assert_called_once()
    validate_operation.assert_called_once()

@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda evidence: [], "must be an object"),
        (lambda evidence: {**evidence, "mode": "retry"}, "one-shot operation"),
        (lambda evidence: {**evidence, "pre_cleanup": {}}, "lost its terminal"),
    ],
)
def test_cleanup_envelope_rejects_shape_operation_and_witness_drift(mutate, message):
    wave = _wave(redis_cleanup_ticket="ticket")
    evidence = _cleanup_evidence(wave)
    with pytest.raises(cleanup.PTGWaveStateConflict, match=message):
        cleanup._validate_redis_cleanup_evidence(wave, mutate(evidence))
