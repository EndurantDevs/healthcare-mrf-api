# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partitioned exact-wave controller ownership contracts."""

from __future__ import annotations

from tests.test_ptg_wave_controller_terminal_owner import (
    AsyncMock,
    Mock,
    _Hold,
    _StateConflict,
    _bundle,
    _controller,
    _delete_controller,
    _wave,
    pytest,
    terminal,
)


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
