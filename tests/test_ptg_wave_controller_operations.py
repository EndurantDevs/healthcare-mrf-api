"""Direct branch contracts for materialization and read-only recovery steps."""

from __future__ import annotations

import types
from unittest.mock import AsyncMock, Mock

import pytest

from process import ptg_wave_controller_operations as operations


class _Hold(RuntimeError):
    pass


class _StateConflict(RuntimeError):
    pass


class _ContractError(RuntimeError):
    pass


def _wave(**overrides):
    fields_by_field = {
        "wave_id": "wave-unit",
        "wave_digest": "1" * 64,
        "state": "slots_waiting",
        "uncertainty_resume_state": None,
        "kubernetes_manifest": {"metadata": {"name": "ptg-wave-unit"}},
        "kubernetes_manifest_identity": "2" * 64,
        "kubernetes_job_receipt_digest": "3" * 64,
    }
    fields_by_field.update(overrides)
    return types.SimpleNamespace(**fields_by_field)


def _bundle(**wave_overrides):
    return types.SimpleNamespace(wave=_wave(**wave_overrides), intents=())


def _controller(**overrides):
    values_by_field = {
        "PTGWaveControllerHold": _Hold,
        "PTGWaveStateConflict": _StateConflict,
        "PTGWaveContractError": _ContractError,
        "BARRIER_FACTORY": "factory.path",
        "_ticket": Mock(side_effect=lambda value: f"ticket-{value}"),
    }
    values_by_field.update(overrides)
    return types.SimpleNamespace(**values_by_field)


@pytest.mark.asyncio
async def test_materialize_persists_exact_restored_and_rendered_identity():
    restored = types.SimpleNamespace(
        manifest_digest="3" * 64,
        jobs_digest="4" * 64,
        jobs=(1, 2),
    )
    manifest_by_field = {"kind": "Job"}
    contract = types.SimpleNamespace(
        image="registry.example/engine@sha256:" + "5" * 64,
        runtime_image_identity="sha256:" + "6" * 64,
        config_identity="7" * 64,
        manifest_identity="8" * 64,
    )
    controller = _controller(
        _require_ptg_only_idle=AsyncMock(),
        restore_wave_manifest=Mock(return_value=restored),
        build_ptg_wave_job=Mock(return_value=manifest_by_field),
        validate_ptg_wave_job_manifest=Mock(return_value=contract),
        canonical_json=Mock(return_value=b"manifest"),
        persist_materialization=AsyncMock(),
    )
    bundle = _bundle()
    await operations.materialize_wave(
        controller,
        bundle,
        object(),
        image=contract.image,
        runtime_image=contract.runtime_image_identity,
    )
    controller._require_ptg_only_idle.assert_awaited_once()
    assert controller.build_ptg_wave_job.call_args.kwargs["job_count"] == 2
    assert controller.persist_materialization.await_args.kwargs["image_digest"] == "5" * 64


@pytest.mark.asyncio
async def test_post_job_has_one_owner_and_fences_ambiguous_failure():
    bundle = _bundle()
    no_owner = _controller(
        mark_kubernetes_post_started=AsyncMock(return_value={"owner": False}),
    )
    await operations.post_wave_job_once(no_owner, bundle)

    success = _controller(
        mark_kubernetes_post_started=AsyncMock(
            return_value={"owner": True, "manifest": {"kind": "Job"}},
        ),
        post_wave_job=Mock(return_value={"metadata": {"uid": "job"}}),
        _kubernetes_job_receipt=Mock(return_value={"receipt": True}),
        record_kubernetes_job_created=AsyncMock(),
        mark_uncertain=AsyncMock(),
    )
    await operations.post_wave_job_once(success, bundle)
    success.record_kubernetes_job_created.assert_awaited_once_with(
        "wave-unit",
        {"receipt": True},
    )
    success.mark_uncertain.assert_not_awaited()

    failure = _controller(
        mark_kubernetes_post_started=AsyncMock(
            return_value={"owner": True, "manifest": {"kind": "Job"}},
        ),
        post_wave_job=Mock(side_effect=RuntimeError("ambiguous")),
        mark_uncertain=AsyncMock(),
    )
    with pytest.raises(RuntimeError, match="ambiguous"):
        await operations.post_wave_job_once(failure, bundle)
    failure.mark_uncertain.assert_awaited_once_with(
        "wave-unit",
        expected_state="slots_waiting",
    )


def _slot_controller(*, job=object(), pods=None, ready=True, released=False, owner=False):
    if pods is None:
        pods = [object()] * 12
    kubernetes = object()
    return _controller(
        get_wave_job=Mock(return_value=job),
        list_wave_pods=Mock(return_value=pods),
        _kubernetes_job_receipt=Mock(return_value={"job": True}),
        record_kubernetes_job_created=AsyncMock(),
        attest_ptg_wave_kubernetes_objects=Mock(return_value=kubernetes),
        inspect_ptg_small_wave_readiness=AsyncMock(
            return_value=types.SimpleNamespace(
                ready=ready,
                released=released,
                registered_slots=tuple(range(12)),
            ),
        ),
        _assert_slot_membership=Mock(),
        _kubernetes_ready_receipt=Mock(return_value={"ready": True}),
        record_kubernetes_ready=AsyncMock(),
        mark_redis_release_started=AsyncMock(return_value=owner),
        _reconcile_redis_release=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_slot_reconciliation_waits_for_job_pods_attestation_and_redis():
    manifest = types.SimpleNamespace(reference=object())
    bundle = _bundle(kubernetes_job_receipt_digest=None)

    with pytest.raises(_Hold, match="Job is not observable"):
        await operations.reconcile_slots(
            _slot_controller(job=None), bundle, manifest, object(),
        )
    with pytest.raises(_Hold, match="exactly 12"):
        await operations.reconcile_slots(
            _slot_controller(pods=[]), bundle, manifest, object(),
        )

    attestation_failure = _slot_controller()
    attestation_failure.attest_ptg_wave_kubernetes_objects.side_effect = _ContractError("drift")
    with pytest.raises(_Hold, match="drift"):
        await operations.reconcile_slots(
            attestation_failure,
            bundle,
            manifest,
            object(),
        )

    for ready, released in ((False, False), (True, True)):
        with pytest.raises(_Hold, match="unreleased Redis"):
            await operations.reconcile_slots(
                _slot_controller(ready=ready, released=released),
                bundle,
                manifest,
                object(),
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", [False, True])
async def test_slot_reconciliation_records_job_readiness_and_single_release(owner):
    controller = _slot_controller(owner=owner)
    bundle = _bundle(kubernetes_job_receipt_digest=None)
    await operations.reconcile_slots(
        controller,
        bundle,
        types.SimpleNamespace(reference=object()),
        object(),
    )
    controller.record_kubernetes_job_created.assert_awaited_once()
    controller.record_kubernetes_ready.assert_awaited_once()
    if owner:
        controller._reconcile_redis_release.assert_awaited_once()
    else:
        controller._reconcile_redis_release.assert_not_awaited()

    existing_receipt = _slot_controller(owner=False)
    await operations.reconcile_slots(
        existing_receipt,
        _bundle(kubernetes_job_receipt_digest="3" * 64),
        types.SimpleNamespace(reference=object()),
        object(),
    )
    existing_receipt.record_kubernetes_job_created.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("mutate", [False, True])
async def test_redis_release_uses_publish_or_get_only_readback(mutate):
    receipt = object()
    controller = _controller(
        publish_ptg_small_wave=AsyncMock(return_value=receipt),
        read_ptg_small_wave_release=AsyncMock(return_value=receipt),
        _redis_release_receipt=Mock(return_value={"release": True}),
        record_redis_release=AsyncMock(),
        mark_uncertain=AsyncMock(),
    )
    await operations.reconcile_redis_release(
        controller,
        _bundle(),
        object(),
        object(),
        mutate=mutate,
    )
    if mutate:
        controller.publish_ptg_small_wave.assert_awaited_once()
        controller.read_ptg_small_wave_release.assert_not_awaited()
    else:
        controller.read_ptg_small_wave_release.assert_awaited_once()
        controller.publish_ptg_small_wave.assert_not_awaited()
    controller.record_redis_release.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("mutate", [False, True])
async def test_redis_release_fences_only_a_mutating_ambiguous_result(mutate):
    controller = _controller(
        publish_ptg_small_wave=AsyncMock(side_effect=RuntimeError("redis")),
        read_ptg_small_wave_release=AsyncMock(side_effect=RuntimeError("redis")),
        mark_uncertain=AsyncMock(),
    )
    with pytest.raises(RuntimeError, match="redis"):
        await operations.reconcile_redis_release(
            controller,
            _bundle(),
            object(),
            object(),
            mutate=mutate,
        )
    if mutate:
        controller.mark_uncertain.assert_awaited_once()
    else:
        controller.mark_uncertain.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_uncertain_post_release_and_cleanup_are_get_only():
    job = object()
    controller = _controller(
        get_wave_job=Mock(return_value=None),
        _kubernetes_job_receipt=Mock(return_value={"job": True}),
        resolve_uncertainty=AsyncMock(),
        record_kubernetes_job_created=AsyncMock(),
        restore_wave_manifest=Mock(return_value=object()),
        read_ptg_small_wave_release=AsyncMock(return_value={"redis": True}),
        _redis_release_receipt=Mock(return_value={"release": True}),
        record_redis_release=AsyncMock(),
    )
    bundle = _bundle(
        state="uncertain",
        uncertainty_resume_state="slots_waiting",
    )
    with pytest.raises(_Hold, match="POST remains absent"):
        await operations.reconcile_uncertain(controller, bundle, object())
    controller.get_wave_job.return_value = job
    assert await operations.reconcile_uncertain(
        controller,
        bundle,
        object(),
    ) == "kubernetes-post-reconciled"

    bundle.wave.uncertainty_resume_state = "redis_releasing"
    assert await operations.reconcile_uncertain(
        controller,
        bundle,
        object(),
    ) == "redis-release-reconciled"
    controller.read_ptg_small_wave_release.side_effect = RuntimeError("absent")
    with pytest.raises(_Hold, match="release remains absent"):
        await operations.reconcile_uncertain(controller, bundle, object())
    controller.read_ptg_small_wave_release.side_effect = None

    bundle.wave.uncertainty_resume_state = "cleaning"
    assert await operations.reconcile_uncertain(
        controller,
        bundle,
        object(),
    ) == "cleanup-get-only-reconciliation"
    bundle.wave.uncertainty_resume_state = "terminalizing"
    with pytest.raises(_Hold, match="failure reconciliation"):
        await operations.reconcile_uncertain(controller, bundle, object())


def _recovery(operation, *, mutation_permitted=False, ticket="ticket"):
    return types.SimpleNamespace(
        operation=operation,
        mutation_permitted=mutation_permitted,
        ticket=ticket,
    )


@pytest.mark.asyncio
async def test_recovery_dispatch_rejects_mutation_and_unknown_operations():
    controller = _controller()
    with pytest.raises(_StateConflict, match="permits mutation"):
        await operations.reconcile_read_only_recovery(
            controller,
            _bundle(),
            object(),
            _recovery("kubernetes_post", mutation_permitted=True),
        )
    with pytest.raises(_StateConflict, match="unsupported"):
        await operations.reconcile_read_only_recovery(
            controller,
            _bundle(),
            object(),
            _recovery("unknown"),
        )


@pytest.mark.asyncio
async def test_kubernetes_post_recovery_handles_present_or_proven_absent_objects():
    controller = _controller(
        get_wave_job=Mock(return_value=object()),
        list_wave_pods=Mock(return_value=[]),
        _kubernetes_job_receipt=Mock(return_value={"job": True}),
        resolve_uncertainty=AsyncMock(),
        record_kubernetes_job_created=AsyncMock(),
        _unclaimed_failure_receipt=Mock(return_value={"failure": True}),
        snapshot_unclaimed_dead_letter_outcomes=AsyncMock(),
    )
    bundle = _bundle(state="uncertain")
    assert await operations.reconcile_read_only_recovery(
        controller,
        bundle,
        object(),
        _recovery("kubernetes_post"),
    ) == "kubernetes-post-reconciled"
    controller.resolve_uncertainty.assert_awaited_once()

    bundle.wave.state = "slots_waiting"
    assert await operations._recover_kubernetes_post(
        controller,
        bundle,
        object(),
        _recovery("kubernetes_post"),
    ) == "kubernetes-post-reconciled"
    assert controller.resolve_uncertainty.await_count == 1

    controller.get_wave_job.return_value = None
    controller.list_wave_pods.return_value = [object()]
    with pytest.raises(_Hold, match="Pods without its Job"):
        await operations._recover_kubernetes_post(
            controller,
            bundle,
            object(),
            _recovery("kubernetes_post"),
        )
    controller.list_wave_pods.return_value = []
    assert await operations._recover_kubernetes_post(
        controller,
        bundle,
        object(),
        _recovery("kubernetes_post"),
    ) == "kubernetes-post-absent-dead-lettered"
    controller.snapshot_unclaimed_dead_letter_outcomes.assert_awaited_once()

    bundle.wave.state = "uncertain"
    await operations._recover_kubernetes_post(
        controller,
        bundle,
        object(),
        _recovery("kubernetes_post"),
    )
    assert controller.resolve_uncertainty.await_count == 2


@pytest.mark.asyncio
async def test_redis_release_recovery_requires_stable_exact_attestation():
    controller = _controller(
        restore_wave_manifest=Mock(return_value=object()),
        attest_ptg_small_wave_unclaimed_failure_redis=AsyncMock(
            side_effect=RuntimeError("unstable"),
        ),
        resolve_uncertainty=AsyncMock(),
        _redis_release_receipt=Mock(return_value={"release": True}),
        record_redis_release=AsyncMock(),
        _unclaimed_failure_receipt=Mock(return_value={"failure": True}),
        snapshot_unclaimed_dead_letter_outcomes=AsyncMock(),
    )
    bundle = _bundle(state="uncertain")
    with pytest.raises(_Hold, match="stable attestation"):
        await operations._recover_redis_release(
            controller,
            bundle,
            object(),
            _recovery("redis_release"),
        )

    controller.attest_ptg_small_wave_unclaimed_failure_redis.side_effect = None
    controller.attest_ptg_small_wave_unclaimed_failure_redis.return_value = types.SimpleNamespace(
        release_present=True,
        release_receipt=None,
        as_mapping=lambda: {"exact": True},
    )
    with pytest.raises(_StateConflict, match="lacks its receipt"):
        await operations._recover_redis_release(
            controller,
            bundle,
            object(),
            _recovery("redis_release"),
        )
    attestation = controller.attest_ptg_small_wave_unclaimed_failure_redis.return_value
    attestation.release_receipt = object()
    assert await operations._recover_redis_release(
        controller,
        bundle,
        object(),
        _recovery("redis_release"),
    ) == "redis-release-reconciled"

    bundle.wave.state = "redis_releasing"
    attestation.release_present = False
    assert await operations._recover_redis_release(
        controller,
        bundle,
        object(),
        _recovery("redis_release"),
    ) == "redis-release-absent-dead-lettered"
    controller.snapshot_unclaimed_dead_letter_outcomes.assert_awaited_once()


@pytest.mark.asyncio
async def test_cleanup_and_delete_recovery_preserve_persisted_resume_state():
    controller = _controller(
        resolve_uncertainty=AsyncMock(),
        _reconcile_redis_cleanup_get_only=AsyncMock(),
        restore_wave_manifest=Mock(return_value=object()),
        _observe_kubernetes_delete_absence=AsyncMock(return_value={"absent": True}),
        record_kubernetes_delete_absent=AsyncMock(),
    )
    bundle = _bundle(state="uncertain", uncertainty_resume_state="cleaning")
    assert await operations.reconcile_read_only_recovery(
        controller,
        bundle,
        object(),
        _recovery("redis_cleanup"),
    ) == "redis-cleanup-get-only-reconciled"
    bundle.wave.state = "cleaning"
    await operations._recover_redis_cleanup(
        controller,
        bundle,
        object(),
        _recovery("redis_cleanup"),
    )

    bundle.wave.state = "uncertain"
    bundle.wave.uncertainty_resume_state = "terminalizing"
    assert await operations._recover_kubernetes_delete(
        controller,
        bundle,
        object(),
        _recovery("kubernetes_delete"),
    ) == "kubernetes-delete-get-only-reconciled"
    bundle.wave.state = "cleaning"
    await operations._recover_kubernetes_delete(
        controller,
        bundle,
        object(),
        _recovery("kubernetes_delete"),
    )
    bundle.wave.state = "released"
    with pytest.raises(_StateConflict, match="invalid persisted"):
        await operations._recover_kubernetes_delete(
            controller,
            bundle,
            object(),
            _recovery("kubernetes_delete"),
        )
