"""Focused controller integration contracts for the exact PTG wave."""

from __future__ import annotations

import copy
import types
from unittest.mock import AsyncMock, Mock

import pytest

import process.ptg_wave_controller as controller
from tests.ptg_wave_controller_test_support import (
    claimed_prestart_wave as _claimed_prestart_wave,
    failed_job_object as _failed_job_object,
)
from process.ptg_wave_cleanup import (
    PTGWaveStateConflict,
    _validate_kubernetes_absence_evidence,
    _validate_redis_cleanup_evidence,
)
from process.ptg_wave_failure import PTGWaveReadOnlyRecovery
from process.ptg_wave_state import canonical_json, sha256_digest


_WAVE = "5" * 64
_MANIFEST = "4" * 64
_JOBS = "6" * 64
_RELEASE = "7" * 64
_PIN = "registry/unit@sha256:" + "1" * 64


def _wave(**overrides):
    wave_field_map = {
        "wave_id": "wave-unit",
        "wave_digest": _WAVE,
        "state": "uncertain",
        "uncertainty_resume_state": "slots_waiting",
        "intent_count": 2,
        "manifest_digest": _MANIFEST,
        "jobs_digest": _JOBS,
        "release_queue": f"arq:PTGSmall:wave:{_WAVE}",
        "pinned_image_reference": _PIN,
        "kubernetes_manifest_identity": "3" * 64,
        "kubernetes_manifest": {"metadata": {"name": "ptg-wave-unit"}},
        "kubernetes_job_uid": None,
        "kubernetes_job_receipt_digest": None,
        "kubernetes_ready_attestation": None,
        "kubernetes_ready_attestation_digest": None,
        "k8s_post_ticket": "post-ticket",
        "redis_release_ticket": None,
        "redis_release_attestation_digest": None,
        "redis_release_attestation": {"release_digest": _RELEASE},
        "failure_receipt": None,
        "failure_receipt_digest": None,
        "terminal_summary": None,
        "redis_cleanup_ticket": "redis-clean-ticket",
        "redis_cleanup_evidence_digest": None,
        "kubernetes_delete_ticket": "delete-ticket",
        "kubernetes_delete_evidence_digest": None,
    }
    wave_field_map.update(overrides)
    return types.SimpleNamespace(**wave_field_map)


@pytest.mark.asyncio
async def test_ambiguous_post_absence_uses_get_only_and_dead_letters_all_n(monkeypatch):
    wave = _wave()
    bundle = controller.PTGWaveBundle(wave=wave, intents=())
    monkeypatch.setattr(controller, "get_wave_job", Mock(return_value=None))
    monkeypatch.setattr(controller, "list_wave_pods", Mock(return_value=[]))
    resolve = AsyncMock()
    snapshot = AsyncMock(return_value="outcomes")
    monkeypatch.setattr(controller, "resolve_uncertainty", resolve)
    monkeypatch.setattr(controller, "snapshot_unclaimed_dead_letter_outcomes", snapshot)
    post = Mock(side_effect=AssertionError("POST must never be retried"))
    monkeypatch.setattr(controller, "post_wave_job", post)

    reconciliation_result = await controller._reconcile_read_only_recovery(
        bundle,
        object(),
        PTGWaveReadOnlyRecovery(
            "kubernetes_post", "post-ticket",
            "get_exact_job_by_persisted_name_and_manifest_identity", False,
        ),
    )

    assert reconciliation_result == "kubernetes-post-absent-dead-lettered"
    resolve.assert_awaited_once_with("wave-unit", reconciled_state="slots_waiting")
    receipt = snapshot.await_args.kwargs["failure_receipt"]
    assert receipt["operation_ticket"] == "post-ticket"
    assert receipt["evidence"]["job_absent"] is True
    assert receipt["evidence"]["pods_absent"] is True
    assert receipt["unclaimed_ordinals_digest"] == sha256_digest(canonical_json({
        "schema_version": 1, "wave_id": "wave-unit", "ordinals": [0, 1],
    }))
    post.assert_not_called()


@pytest.mark.asyncio
async def test_ambiguous_redis_release_exact_absence_dead_letters_without_republish(monkeypatch):
    redis_evidence_map = {
        "schema_version": "redis-failure",
        "release_present": False,
    }
    attestation = types.SimpleNamespace(
        release_present=False,
        release_receipt=None,
        as_mapping=lambda: redis_evidence_map,
    )
    wave = _wave(
        uncertainty_resume_state="redis_releasing",
        redis_release_ticket="release-ticket",
    )
    bundle = controller.PTGWaveBundle(wave=wave, intents=())
    monkeypatch.setattr(controller, "restore_wave_manifest", Mock(return_value=object()))
    monkeypatch.setattr(
        controller,
        "attest_ptg_small_wave_unclaimed_failure_redis",
        AsyncMock(return_value=attestation),
    )
    monkeypatch.setattr(controller, "resolve_uncertainty", AsyncMock())
    snapshot = AsyncMock()
    monkeypatch.setattr(controller, "snapshot_unclaimed_dead_letter_outcomes", snapshot)
    publish = AsyncMock(side_effect=AssertionError("release must never be republished"))
    monkeypatch.setattr(controller, "publish_ptg_small_wave", publish)

    reconciliation_result = await controller._reconcile_read_only_recovery(
        bundle,
        object(),
        PTGWaveReadOnlyRecovery(
            "redis_release", "release-ticket", "get_exact_release_receipt", False,
        ),
    )

    assert reconciliation_result == "redis-release-absent-dead-lettered"
    receipt = snapshot.await_args.kwargs["failure_receipt"]
    assert receipt["reason"] == "redis_release_absent"
    assert receipt["evidence"] == redis_evidence_map
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_ambiguous_redis_release_present_records_first_receipt_without_republish(monkeypatch):
    release = object()
    attestation = types.SimpleNamespace(
        release_present=True,
        release_receipt=release,
        as_mapping=lambda: {},
    )
    wave = _wave(
        uncertainty_resume_state="redis_releasing",
        redis_release_ticket="release-ticket",
    )
    bundle = controller.PTGWaveBundle(wave=wave, intents=())
    monkeypatch.setattr(controller, "restore_wave_manifest", Mock(return_value=object()))
    monkeypatch.setattr(
        controller,
        "attest_ptg_small_wave_unclaimed_failure_redis",
        AsyncMock(return_value=attestation),
    )
    monkeypatch.setattr(controller, "resolve_uncertainty", AsyncMock())
    monkeypatch.setattr(controller, "_redis_release_receipt", Mock(return_value={"exact": True}))
    receipt_record = AsyncMock()
    monkeypatch.setattr(controller, "record_redis_release", receipt_record)
    publish = AsyncMock(side_effect=AssertionError("release must never be republished"))
    monkeypatch.setattr(controller, "publish_ptg_small_wave", publish)

    reconciliation_result = await controller._reconcile_read_only_recovery(
        bundle,
        object(),
        PTGWaveReadOnlyRecovery(
            "redis_release", "release-ticket", "get_exact_release_receipt", False,
        ),
    )

    assert reconciliation_result == "redis-release-reconciled"
    receipt_record.assert_awaited_once_with("wave-unit", {"exact": True})
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_early_delete_recovery_observes_then_restores_terminalizing(monkeypatch):
    wave = _wave(
        state="uncertain",
        uncertainty_resume_state="terminalizing",
        kubernetes_delete_ticket="delete-ticket",
    )
    bundle = controller.PTGWaveBundle(wave=wave, intents=())
    absence_evidence_map = {"exact": "absence"}
    monkeypatch.setattr(
        controller,
        "_observe_kubernetes_delete_absence",
        AsyncMock(return_value=absence_evidence_map),
    )
    resolve = AsyncMock()
    receipt_record = AsyncMock()
    monkeypatch.setattr(controller, "resolve_uncertainty", resolve)
    monkeypatch.setattr(
        controller,
        "record_kubernetes_delete_absent",
        receipt_record,
    )

    reconciliation_result = await controller._reconcile_read_only_recovery(
        bundle,
        object(),
        PTGWaveReadOnlyRecovery(
            "kubernetes_delete", "delete-ticket",
            "get_exact_job_and_labeled_pods_absence", False,
        ),
    )

    assert reconciliation_result == "kubernetes-delete-get-only-reconciled"
    resolve.assert_awaited_once_with("wave-unit", reconciled_state="terminalizing")
    receipt_record.assert_awaited_once_with("wave-unit", absence_evidence_map)


@pytest.mark.asyncio
async def test_never_created_job_cleanup_honors_delete_permitted_false(monkeypatch):
    wave = _wave(
        state="cleaning",
        uncertainty_resume_state=None,
        redis_cleanup_evidence_digest="8" * 64,
    )
    bundle = controller.PTGWaveBundle(wave=wave, intents=())
    monkeypatch.setattr(
        controller,
        "mark_kubernetes_delete_started",
        AsyncMock(return_value={
            "owner": True, "delete_permitted": False, "job_uid": None,
        }),
    )
    delete = Mock(side_effect=AssertionError("no DELETE is permitted"))
    monkeypatch.setattr(controller, "delete_wave_job", delete)
    monkeypatch.setattr(
        controller,
        "wave_absence_observation",
        Mock(return_value={"job_absent": True, "pod_count": 0, "pods_absent": True}),
    )
    operation_record = AsyncMock(return_value="digest")
    monkeypatch.setattr(
        controller,
        "record_kubernetes_delete_absent",
        operation_record,
    )

    await controller._reconcile_cleanup(bundle, object(), object())

    delete.assert_not_called()
    absence_evidence_map = operation_record.await_args.args[1]
    assert absence_evidence_map["delete_permitted"] is False
    assert absence_evidence_map["job_uid"] is None
    assert (
        absence_evidence_map["job_absent"]
        is absence_evidence_map["pods_absent"]
        is True
    )


def _pre_cleanup(wave):
    pre_cleanup_map = {
        "schema_version": 1,
        "wave_id": wave.wave_digest,
        "queue_name": wave.release_queue,
        "manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest,
        "job_count": wave.intent_count,
        "image_identity": wave.pinned_image_reference,
        "release_digest": _RELEASE,
        "target_key_count": 4 + 4 * wave.intent_count,
        "queue_entry_count": 0,
        "job_payload_count": 0,
        "result_count": 2,
        "retry_count": 0,
        "in_progress_count": 0,
        "health_check_count": 1,
        "result_presence_digest": "9" * 64,
    }
    return {
        **pre_cleanup_map,
        "attestation_digest": sha256_digest(canonical_json(pre_cleanup_map)),
    }


def _post_cleanup(wave):
    post_cleanup_map = {
        "schema_version": 1,
        "wave_id": wave.wave_digest,
        "manifest_digest": wave.manifest_digest,
        "target_key_count": 4 + 4 * wave.intent_count,
        "absent_target_count": 4 + 4 * wave.intent_count,
    }
    return {
        **post_cleanup_map,
        "attestation_digest": sha256_digest(canonical_json(post_cleanup_map)),
    }


def test_redis_cleanup_receipt_binds_terminal_evidence():
    wave = _wave(state="cleaning")
    pre = _pre_cleanup(wave)
    wave.terminal_summary = {"redis_pre_cleanup": pre}
    operation_map = {
        "schema_version": 1,
        "wave_id": wave.wave_digest,
        "manifest_digest": wave.manifest_digest,
        "target_key_count": 4 + 4 * wave.intent_count,
        "deleted_key_count": 6,
        "pre_cleanup_attestation_digest": pre["attestation_digest"],
        "pre_cleanup": pre,
    }
    cleanup_evidence_map = {
        "schema_version": "healthporta.ptg-wave.redis-cleanup.v1",
        "operation_ticket": wave.redis_cleanup_ticket,
        "mode": "executed",
        "pre_cleanup": pre,
        "operation_receipt": operation_map,
        "post_cleanup": _post_cleanup(wave),
    }
    assert (
        _validate_redis_cleanup_evidence(wave, cleanup_evidence_map)
        == cleanup_evidence_map
    )
    tampered = copy.deepcopy(cleanup_evidence_map)
    tampered["post_cleanup"]["target_key_count"] -= 1
    with pytest.raises(PTGWaveStateConflict, match="every exact target"):
        _validate_redis_cleanup_evidence(wave, tampered)


def test_kubernetes_absence_receipt_binds_job_name_uid_and_digest():
    wave = _wave(kubernetes_job_uid="job-uid")
    evidence = controller._kubernetes_absence_receipt(
        wave, {"job_absent": True, "pod_count": 0, "pods_absent": True},
    )
    assert _validate_kubernetes_absence_evidence(wave, evidence) == evidence
    evidence["job_uid"] = "other"
    with pytest.raises(PTGWaveStateConflict, match="exact Job and Pods"):
        _validate_kubernetes_absence_evidence(wave, evidence)


@pytest.mark.asyncio
async def test_terminal_job_with_claims_uses_exact_claimed_prestart_snapshot(
    monkeypatch,
):
    """Require claimed-prestart terminal proof to reuse persisted witnesses."""

    wave = _claimed_prestart_wave(_wave)
    bundle = controller.PTGWaveBundle(wave=wave, intents=())
    kubernetes_evidence_map = {"schema_version": "exact-kubernetes-failure"}
    redis_evidence_map = {"schema_version": "exact-idle-redis"}
    monkeypatch.setattr(
        controller,
        "get_wave_job",
        Mock(return_value=_failed_job_object()),
    )
    monkeypatch.setattr(controller, "list_wave_pods", Mock(return_value=[object()] * 12))
    monkeypatch.setattr(controller, "_initial_kubernetes_attestation", Mock(return_value=object()))
    monkeypatch.setattr(
        controller,
        "attest_preclaim_failure_ptg_wave_kubernetes_objects",
        Mock(
            return_value=types.SimpleNamespace(
                as_mapping=lambda: kubernetes_evidence_map
            )
        ),
    )
    monkeypatch.setattr(
        controller,
        "attest_ptg_small_wave_unclaimed_failure_redis",
        AsyncMock(
            return_value=types.SimpleNamespace(
                as_mapping=lambda: redis_evidence_map
            )
        ),
    )
    claimed = AsyncMock()
    unclaimed = AsyncMock(side_effect=AssertionError("released failure used old path"))
    monkeypatch.setattr(
        controller,
        "snapshot_claimed_prestart_dead_letter_outcomes",
        claimed,
    )
    monkeypatch.setattr(
        controller,
        "snapshot_unclaimed_dead_letter_outcomes",
        unclaimed,
    )

    assert await controller._maybe_snapshot_preclaim_failure(
        bundle,
        object(),
        object(),
    )
    claimed.assert_awaited_once_with(
        wave.wave_id,
        kubernetes_evidence=kubernetes_evidence_map,
        redis_evidence=redis_evidence_map,
    )
    unclaimed.assert_not_awaited()


@pytest.mark.asyncio
async def test_claimed_prestart_active_redis_holds_without_snapshot(monkeypatch):
    wave = _wave(
        state="executing",
        uncertainty_resume_state=None,
        kubernetes_job_uid="job-unit",
        kubernetes_ready_attestation={"slots": [
            {"slot": slot, "pod_uid": f"pod-{slot}"}
            for slot in range(12)
        ]},
        kubernetes_ready_attestation_digest="a" * 64,
    )
    bundle = controller.PTGWaveBundle(wave=wave, intents=())
    monkeypatch.setattr(
        controller,
        "get_wave_job",
        Mock(return_value={
            "status": {
                "conditions": [{"type": "Failed", "status": "True"}],
            },
        }),
    )
    monkeypatch.setattr(controller, "list_wave_pods", Mock(return_value=[object()] * 12))
    monkeypatch.setattr(controller, "_initial_kubernetes_attestation", Mock(return_value=object()))
    monkeypatch.setattr(
        controller,
        "attest_preclaim_failure_ptg_wave_kubernetes_objects",
        Mock(return_value=types.SimpleNamespace(as_mapping=lambda: {"exact": True})),
    )
    monkeypatch.setattr(
        controller,
        "attest_ptg_small_wave_unclaimed_failure_redis",
        AsyncMock(side_effect=RuntimeError("in-progress key remains")),
    )
    claimed = AsyncMock()
    monkeypatch.setattr(
        controller,
        "snapshot_claimed_prestart_dead_letter_outcomes",
        claimed,
    )

    with pytest.raises(
        controller.PTGWaveControllerHold,
        match="exact idle Redis evidence",
    ):
        await controller._maybe_snapshot_preclaim_failure(
            bundle,
            object(),
            object(),
        )
    claimed.assert_not_awaited()


@pytest.mark.asyncio
async def test_claimed_prestart_terminal_proof_reobserves_the_persisted_modes(
    monkeypatch,
):
    kubernetes_evidence_map = {"schema_version": "first-kubernetes-failure"}
    redis_evidence_map = {"schema_version": "same-idle-redis"}
    wave = _wave(
        state="terminalizing",
        uncertainty_resume_state=None,
        failure_receipt={
            "reason": "claimed_prestart_failure",
            "kubernetes_evidence": kubernetes_evidence_map,
        },
        failure_receipt_digest="f" * 64,
    )
    bundle = controller.PTGWaveBundle(wave=wave, intents=())
    monkeypatch.setattr(
        controller,
        "attest_ptg_small_wave_unclaimed_failure_redis",
        AsyncMock(
            return_value=types.SimpleNamespace(
                as_mapping=lambda: redis_evidence_map
            )
        ),
    )
    persist = AsyncMock()
    monkeypatch.setattr(controller, "persist_terminal_evidence", persist)

    await controller._persist_terminal_proof(bundle, object(), object())

    persist.assert_awaited_once_with(
        wave.wave_id,
        {
            "kubernetes": kubernetes_evidence_map,
            "redis": redis_evidence_map,
        },
    )
