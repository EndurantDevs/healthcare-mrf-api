

"""Direct terminal, cleanup, and deletion controller branch contracts."""


from __future__ import annotations


import types


from unittest.mock import AsyncMock, Mock


import pytest


from process import ptg_wave_controller_terminal as terminal


class _Hold(RuntimeError):
    pass


class _StateConflict(RuntimeError):
    pass


class _ContractError(RuntimeError):
    pass


class _FailureConflict(RuntimeError):
    pass


def _wave(**overrides):
    fields_by_field = {
        "wave_id": "wave-unit",
        "wave_digest": "1" * 64,
        "state": "executing",
        "failure_receipt": None,
        "failure_receipt_digest": None,
        "kubernetes_delete_evidence": {"absent": True},
        "kubernetes_delete_evidence_digest": None,
        "kubernetes_delete_ticket": "delete-ticket",
        "redis_cleanup_evidence_digest": None,
        "kubernetes_ready_attestation_digest": "2" * 64,
        "kubernetes_manifest": {"kind": "Job"},
        "terminal_summary": None,
    }
    fields_by_field.update(overrides)
    return types.SimpleNamespace(**fields_by_field)


def _bundle(**overrides):
    return types.SimpleNamespace(wave=_wave(**overrides), intents=())


def _controller(**overrides):
    values_by_field = {
        "PTGWaveControllerHold": _Hold,
        "PTGWaveStateConflict": _StateConflict,
        "PTGWaveContractError": _ContractError,
        "PTGWaveFailureConflict": _FailureConflict,
        "_ticket": Mock(side_effect=lambda value: f"ticket-{value}"),
    }
    values_by_field.update(overrides)
    return types.SimpleNamespace(**values_by_field)


def _claimed_controller(*, job=object(), pods=None):
    if pods is None:
        pods = [object()] * 12
    return _controller(
        get_wave_job=Mock(return_value=job),
        list_wave_pods=Mock(return_value=pods),
        _initial_kubernetes_attestation=Mock(return_value=object()),
        attest_terminal_ptg_wave_kubernetes_objects=Mock(return_value=object()),
        attest_ptg_wave_pre_cleanup=AsyncMock(return_value=object()),
        _kubernetes_terminal_receipt=Mock(return_value={"kube": True}),
        _redis_terminal_receipt=Mock(return_value={"redis": True}),
        persist_terminal_evidence=AsyncMock(),
    )


def _redis_cleanup_controller(*, owner, failure):
    operation_by_field = {"owner": owner, "operation_ticket": "ticket"}
    return _controller(
        mark_redis_cleanup_started=AsyncMock(return_value=operation_by_field),
        _failure_redis_attestation_digest=Mock(return_value="a" * 64),
        cleanup_ptg_small_wave_unclaimed_failure_redis=AsyncMock(return_value=object()),
        attest_ptg_small_wave_unclaimed_failure_redis_post_cleanup=AsyncMock(
            return_value=object(),
        ),
        cleanup_ptg_small_wave_terminal_state=AsyncMock(return_value=object()),
        attest_ptg_wave_post_cleanup=AsyncMock(return_value=object()),
        _redis_cleanup_receipt=Mock(return_value={"cleanup": True}),
        record_redis_cleanup_absent=AsyncMock(),
        mark_uncertain=AsyncMock(),
    )


def _preclaim_controller(*, job=object(), terminal_failure=True):
    return _controller(
        get_wave_job=Mock(return_value=job),
        _job_reports_terminal_failure=Mock(return_value=terminal_failure),
        list_wave_pods=Mock(return_value=[object()] * 12),
        _initial_kubernetes_attestation=Mock(return_value=object()),
        attest_preclaim_failure_ptg_wave_kubernetes_objects=Mock(
            return_value=types.SimpleNamespace(as_mapping=lambda: {"kube": True}),
        ),
    )


def _delete_controller(*, owner=True, permitted=True):
    return _controller(
        mark_kubernetes_delete_started=AsyncMock(
            return_value={
                "owner": owner,
                "delete_permitted": permitted,
                "job_uid": "job-uid",
                "operation_ticket": "ticket",
            },
        ),
        delete_wave_job=Mock(),
        _observe_kubernetes_delete_absence=AsyncMock(return_value={"absent": True}),
        record_kubernetes_delete_absent=AsyncMock(),
        mark_uncertain=AsyncMock(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
async def test_terminal_proof_dispatches_by_failure_receipt(failure, monkeypatch):
    bundle = _bundle(failure_receipt_digest=("f" * 64 if failure else None))
    claimed = AsyncMock()
    failed = AsyncMock()
    monkeypatch.setattr(terminal, "_persist_claimed_terminal_proof", claimed)
    monkeypatch.setattr(terminal, "_persist_failure_terminal_proof", failed)
    await terminal.persist_terminal_proof(_controller(), bundle, object(), object())
    if failure:
        failed.assert_awaited_once()
        claimed.assert_not_awaited()
    else:
        claimed.assert_awaited_once()
        failed.assert_not_awaited()

@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        ("kubernetes_post_absent", {"post": "absent"}),
        ("pre_claim_failure", {"preclaim": True}),
        ("claimed_prestart_failure", {"claimed": True}),
    ],
)
def test_failure_kubernetes_evidence_selects_reason_specific_witness(reason, expected):
    receipt_by_field = {"reason": reason}
    if reason == "kubernetes_post_absent":
        receipt_by_field["evidence"] = expected
    elif reason == "pre_claim_failure":
        receipt_by_field["evidence"] = expected
    else:
        receipt_by_field["kubernetes_evidence"] = expected
    assert terminal._failure_kubernetes_evidence(
        _controller(),
        _wave(failure_receipt=receipt_by_field),
    ) == expected

def test_failure_kubernetes_evidence_requires_early_absence_and_known_reason():
    controller = _controller()
    wave = _wave(
        failure_receipt={"reason": "redis_release_absent"},
        kubernetes_delete_evidence_digest=None,
    )
    with pytest.raises(_Hold, match="must be absent"):
        terminal._failure_kubernetes_evidence(controller, wave)
    wave.kubernetes_delete_evidence_digest = "2" * 64
    assert terminal._failure_kubernetes_evidence(controller, wave) == {"absent": True}
    for receipt in (None, {"reason": "unknown"}):
        with pytest.raises(_StateConflict, match="unsupported"):
            terminal._failure_kubernetes_evidence(
                controller,
                _wave(failure_receipt=receipt),
            )

@pytest.mark.asyncio
async def test_failure_terminal_proof_requires_stable_redis_attestation():
    controller = _controller(
        attest_ptg_small_wave_unclaimed_failure_redis=AsyncMock(
            side_effect=RuntimeError("redis unstable"),
        ),
        persist_terminal_evidence=AsyncMock(),
    )
    bundle = _bundle(
        failure_receipt={"reason": "pre_claim_failure", "evidence": {"kube": True}},
        failure_receipt_digest="f" * 64,
    )
    with pytest.raises(_Hold, match="redis unstable"):
        await terminal._persist_failure_terminal_proof(
            controller, bundle, object(), object(),
        )
    controller.attest_ptg_small_wave_unclaimed_failure_redis.side_effect = None
    controller.attest_ptg_small_wave_unclaimed_failure_redis.return_value = (
        types.SimpleNamespace(as_mapping=lambda: {"redis": True})
    )
    await terminal._persist_failure_terminal_proof(
        controller, bundle, object(), object(),
    )
    controller.persist_terminal_evidence.assert_awaited_once_with(
        "wave-unit",
        {"kubernetes": {"kube": True}, "redis": {"redis": True}},
    )

@pytest.mark.asyncio
async def test_claimed_terminal_proof_waits_for_exact_job_pods_and_attestations():
    bundle = _bundle()
    with pytest.raises(_Hold, match="Job is absent"):
        await terminal._persist_claimed_terminal_proof(
            _claimed_controller(job=None), bundle, object(), object(),
        )
    with pytest.raises(_Hold, match="membership is incomplete"):
        await terminal._persist_claimed_terminal_proof(
            _claimed_controller(pods=[]), bundle, object(), object(),
        )
    failure = _claimed_controller()
    failure.attest_terminal_ptg_wave_kubernetes_objects.side_effect = RuntimeError("drift")
    with pytest.raises(_Hold, match="drift"):
        await terminal._persist_claimed_terminal_proof(
            failure, bundle, object(), object(),
        )

    success = _claimed_controller()
    await terminal._persist_claimed_terminal_proof(
        success, bundle, object(), object(),
    )
    success.persist_terminal_evidence.assert_awaited_once_with(
        "wave-unit",
        {"kubernetes": {"kube": True}, "redis": {"redis": True}},
    )

@pytest.mark.asyncio
async def test_cleanup_reconciliation_advances_redis_delete_then_terminal(monkeypatch):
    controller = _controller(
        _reconcile_kubernetes_delete=AsyncMock(),
        persist_cleanup_and_terminal=AsyncMock(),
    )
    redis_cleanup = AsyncMock()
    monkeypatch.setattr(terminal, "_reconcile_redis_cleanup", redis_cleanup)
    bundle = _bundle()
    await terminal.reconcile_cleanup(controller, bundle, object(), object())
    redis_cleanup.assert_awaited_once()

    bundle.wave.redis_cleanup_evidence_digest = "2" * 64
    await terminal.reconcile_cleanup(controller, bundle, object(), object())
    controller._reconcile_kubernetes_delete.assert_awaited_once_with(
        bundle,
        expected_state="cleaning",
    )

    bundle.wave.kubernetes_delete_evidence_digest = "3" * 64
    await terminal.reconcile_cleanup(controller, bundle, object(), object())
    controller.persist_cleanup_and_terminal.assert_awaited_once_with("wave-unit")

@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
@pytest.mark.parametrize("owner", [False, True])
async def test_redis_cleanup_uses_failure_or_normal_owner_specific_operation(
    failure, owner,
):
    controller = _redis_cleanup_controller(owner=owner, failure=failure)
    wave = _wave(failure_receipt_digest=("f" * 64 if failure else None))
    await terminal._reconcile_redis_cleanup(controller, wave, object(), object())
    if failure:
        assert controller.cleanup_ptg_small_wave_unclaimed_failure_redis.await_count == int(owner)
        controller.attest_ptg_small_wave_unclaimed_failure_redis_post_cleanup.assert_awaited_once()
    else:
        assert controller.cleanup_ptg_small_wave_terminal_state.await_count == int(owner)
        controller.attest_ptg_wave_post_cleanup.assert_awaited_once()
    controller.record_redis_cleanup_absent.assert_awaited_once()

@pytest.mark.asyncio
@pytest.mark.parametrize("owner", [False, True])
async def test_redis_cleanup_fences_only_owner_ambiguity(owner):
    controller = _redis_cleanup_controller(owner=owner, failure=False)
    controller.attest_ptg_wave_post_cleanup.side_effect = RuntimeError("ambiguous")
    with pytest.raises(RuntimeError, match="ambiguous"):
        await terminal._reconcile_redis_cleanup(
            controller,
            _wave(),
            object(),
            object(),
        )
    if owner:
        controller.mark_uncertain.assert_awaited_once()
    else:
        controller.mark_uncertain.assert_not_awaited()

@pytest.mark.asyncio
async def test_preclaim_snapshot_gates_on_readiness_job_failure_and_attestation(monkeypatch):
    bundle = _bundle(kubernetes_ready_attestation_digest=None)
    assert not await terminal.should_snapshot_preclaim_failure(
        _preclaim_controller(), bundle, object(), object(),
    )
    bundle.wave.kubernetes_ready_attestation_digest = "2" * 64
    with pytest.raises(_Hold, match="Job is absent"):
        await terminal.should_snapshot_preclaim_failure(
            _preclaim_controller(job=None), bundle, object(), object(),
        )
    assert not await terminal.should_snapshot_preclaim_failure(
        _preclaim_controller(terminal_failure=False),
        bundle,
        object(),
        object(),
    )
    invalid = _preclaim_controller()
    invalid.attest_preclaim_failure_ptg_wave_kubernetes_objects.side_effect = _ContractError("drift")
    with pytest.raises(_Hold, match="drift"):
        await terminal.should_snapshot_preclaim_failure(
            invalid, bundle, object(), object(),
        )

    unclaimed = AsyncMock()
    claimed = AsyncMock()
    monkeypatch.setattr(terminal, "_snapshot_unclaimed_prestart", unclaimed)
    monkeypatch.setattr(terminal, "_snapshot_claimed_prestart", claimed)
    bundle.wave.state = "slots_waiting"
    assert await terminal.should_snapshot_preclaim_failure(
        _preclaim_controller(), bundle, object(), object(),
    )
    unclaimed.assert_awaited_once()
    bundle.wave.state = "executing"
    assert await terminal.should_snapshot_preclaim_failure(
        _preclaim_controller(), bundle, object(), object(),
    )
    claimed.assert_awaited_once()

@pytest.mark.asyncio
async def test_preclaim_snapshot_helpers_persist_unclaimed_or_claimed_evidence():
    attestation = types.SimpleNamespace(as_mapping=lambda: {"kube": True})
    unclaimed_controller = _controller(
        _unclaimed_failure_receipt=Mock(return_value={"failure": True}),
        snapshot_unclaimed_dead_letter_outcomes=AsyncMock(),
    )
    wave = _wave(state="slots_waiting")
    await terminal._snapshot_unclaimed_prestart(
        unclaimed_controller,
        wave,
        attestation,
    )
    unclaimed_controller.snapshot_unclaimed_dead_letter_outcomes.assert_awaited_once()

    claimed_controller = _controller(
        attest_ptg_small_wave_unclaimed_failure_redis=AsyncMock(
            return_value=types.SimpleNamespace(as_mapping=lambda: {"redis": True}),
        ),
        snapshot_claimed_prestart_dead_letter_outcomes=AsyncMock(),
    )
    await terminal._snapshot_claimed_prestart(
        claimed_controller,
        wave,
        object(),
        object(),
        attestation,
    )
    claimed_controller.snapshot_claimed_prestart_dead_letter_outcomes.assert_awaited_once_with(
        "wave-unit",
        kubernetes_evidence={"kube": True},
        redis_evidence={"redis": True},
    )

    claimed_controller.snapshot_claimed_prestart_dead_letter_outcomes.side_effect = _FailureConflict("claim")
    with pytest.raises(_Hold, match="claim"):
        await terminal._snapshot_claimed_prestart(
            claimed_controller, wave, object(), object(), attestation,
        )
    claimed_controller.snapshot_claimed_prestart_dead_letter_outcomes.side_effect = RuntimeError("redis")
    with pytest.raises(_Hold, match="exact idle Redis"):
        await terminal._snapshot_claimed_prestart(
            claimed_controller, wave, object(), object(), attestation,
        )

def test_terminal_job_failure_parser_and_early_stop_reason():
    controller = _controller()
    with pytest.raises(_Hold, match="observation is invalid"):
        terminal.has_terminal_job_failure(controller, [])
    assert not terminal.has_terminal_job_failure(controller, {})
    assert not terminal.has_terminal_job_failure(controller, {"status": {"conditions": {}}})
    assert not terminal.has_terminal_job_failure(
        controller,
        {"status": {"conditions": [None, {"type": "Complete", "status": "True"}]}},
    )
    assert terminal.has_terminal_job_failure(
        controller,
        {"status": {"conditions": [{"type": "Failed", "status": "True"}]}},
    )
    assert not terminal.needs_early_kubernetes_stop(_wave(failure_receipt=None))
    assert terminal.needs_early_kubernetes_stop(
        _wave(failure_receipt={"reason": "redis_release_absent"}),
    )

@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("owner", "permitted", "delete_count"),
    [(True, True, 1), (True, False, 0), (False, True, 0)],
)
async def test_kubernetes_delete_mutates_only_for_permitted_owner(
    owner, permitted, delete_count,
):
    controller = _delete_controller(owner=owner, permitted=permitted)
    await terminal.reconcile_kubernetes_delete(
        controller,
        _bundle(),
        expected_state="cleaning",
    )
    assert controller.delete_wave_job.call_count == delete_count
    controller.record_kubernetes_delete_absent.assert_awaited_once()

@pytest.mark.asyncio
@pytest.mark.parametrize("owner", [False, True])
async def test_kubernetes_delete_preserves_holds_and_fences_owner_errors(owner):
    controller = _delete_controller(owner=owner)
    controller._observe_kubernetes_delete_absence.side_effect = _Hold("waiting")
    with pytest.raises(_Hold, match="waiting"):
        await terminal.reconcile_kubernetes_delete(
            controller, _bundle(), expected_state="cleaning",
        )
    controller.mark_uncertain.assert_not_awaited()

    controller._observe_kubernetes_delete_absence.side_effect = RuntimeError("ambiguous")
    with pytest.raises(RuntimeError, match="ambiguous"):
        await terminal.reconcile_kubernetes_delete(
            controller, _bundle(), expected_state="cleaning",
        )
    if owner:
        controller.mark_uncertain.assert_awaited_once()
    else:
        controller.mark_uncertain.assert_not_awaited()

@pytest.mark.asyncio
async def test_kubernetes_absence_observation_waits_for_both_job_and_pods():
    controller = _controller(
        wave_absence_observation=Mock(
            return_value={"job_absent": False, "pods_absent": True},
        ),
        _kubernetes_absence_receipt=Mock(return_value={"receipt": True}),
    )
    with pytest.raises(_Hold, match="waiting"):
        await terminal.observe_kubernetes_delete_absence(
            controller, _bundle(), "ticket",
        )
    controller.wave_absence_observation.return_value = {
        "job_absent": True,
        "pods_absent": False,
    }
    with pytest.raises(_Hold, match="waiting"):
        await terminal.observe_kubernetes_delete_absence(
            controller, _bundle(), "ticket",
        )
    controller.wave_absence_observation.return_value = {
        "job_absent": True,
        "pods_absent": True,
    }
    assert await terminal.observe_kubernetes_delete_absence(
        controller, _bundle(), "ticket",
    ) == {"receipt": True}

@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
async def test_get_only_redis_cleanup_selects_matching_absence_attestation(failure):
    controller = _controller(
        _failure_redis_attestation_digest=Mock(return_value="a" * 64),
        attest_ptg_small_wave_unclaimed_failure_redis_post_cleanup=AsyncMock(
            return_value=object(),
        ),
        attest_ptg_wave_post_cleanup=AsyncMock(return_value=object()),
        _redis_cleanup_receipt=Mock(return_value={"cleanup": True}),
        record_redis_cleanup_absent=AsyncMock(),
    )
    bundle = _bundle(failure_receipt_digest=("f" * 64 if failure else None))
    await terminal.reconcile_redis_cleanup_get_only(
        controller,
        bundle,
        object(),
        object(),
        "ticket",
    )
    if failure:
        controller.attest_ptg_small_wave_unclaimed_failure_redis_post_cleanup.assert_awaited_once()
        controller.attest_ptg_wave_post_cleanup.assert_not_awaited()
    else:
        controller.attest_ptg_wave_post_cleanup.assert_awaited_once()
        controller.attest_ptg_small_wave_unclaimed_failure_redis_post_cleanup.assert_not_awaited()
    controller.record_redis_cleanup_absent.assert_awaited_once()

def test_failure_redis_digest_requires_a_lowercase_sha256():
    controller = _controller()
    assert terminal.failure_redis_attestation_digest(
        controller,
        _wave(terminal_summary={"redis_pre_cleanup": {"attestation_digest": "a" * 64}}),
    ) == "a" * 64
    for summary in (
        None,
        {},
        {"redis_pre_cleanup": None},
        {"redis_pre_cleanup": {"attestation_digest": None}},
        {"redis_pre_cleanup": {"attestation_digest": "a" * 63}},
        {"redis_pre_cleanup": {"attestation_digest": "G" * 64}},
    ):
        with pytest.raises(_StateConflict, match="exact Redis"):
            terminal.failure_redis_attestation_digest(
                controller,
                _wave(terminal_summary=summary),
            )
