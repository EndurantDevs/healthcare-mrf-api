"""Terminal proof, cleanup, and PTG-isolation controller steps."""

from __future__ import annotations

import asyncio
from types import ModuleType
from typing import Any


async def persist_terminal_proof(
    controller: ModuleType,
    bundle: Any,
    manifest: Any,
    redis: Any,
) -> None:
    """Persist claimed or failure terminal evidence from exact observations."""

    if bundle.wave.failure_receipt_digest is not None:
        await _persist_failure_terminal_proof(
            controller, bundle, manifest, redis
        )
        return
    await _persist_claimed_terminal_proof(controller, bundle, manifest, redis)


def _failure_kubernetes_evidence(
    controller: ModuleType, wave: Any
) -> dict[str, Any]:
    failure_receipt = (
        wave.failure_receipt if isinstance(wave.failure_receipt, dict) else {}
    )
    reason = failure_receipt.get("reason")
    if reason == "redis_release_absent":
        if wave.kubernetes_delete_evidence_digest is None:
            raise controller.PTGWaveControllerHold(
                "unreleased failure Job must be absent before terminal proof"
            )
        return wave.kubernetes_delete_evidence
    if reason in {"kubernetes_post_absent", "pre_claim_failure"}:
        return failure_receipt.get("evidence")
    if reason == "claimed_prestart_failure":
        return failure_receipt.get("kubernetes_evidence")
    raise controller.PTGWaveStateConflict(
        "exact-wave failure reason is unsupported"
    )


async def _persist_failure_terminal_proof(
    controller: ModuleType,
    bundle: Any,
    manifest: Any,
    redis: Any,
) -> None:
    wave = bundle.wave
    kubernetes_evidence = _failure_kubernetes_evidence(controller, wave)
    try:
        redis_idle = (
            await controller.attest_ptg_small_wave_unclaimed_failure_redis(
                redis, manifest
            )
        )
    except Exception as exc:
        raise controller.PTGWaveControllerHold(str(exc)) from exc
    await controller.persist_terminal_evidence(
        wave.wave_id,
        {
            "kubernetes": kubernetes_evidence,
            "redis": redis_idle.as_mapping(),
        },
    )


async def _persist_claimed_terminal_proof(
    controller: ModuleType,
    bundle: Any,
    manifest: Any,
    redis: Any,
) -> None:
    wave = bundle.wave
    actual_job = await asyncio.to_thread(
        controller.get_wave_job, wave.wave_digest
    )
    if actual_job is None:
        raise controller.PTGWaveControllerHold(
            "terminal Kubernetes Job is absent before proof"
        )
    pods = await asyncio.to_thread(controller.list_wave_pods, wave.wave_digest)
    if len(pods) != 12:
        raise controller.PTGWaveControllerHold(
            "terminal Kubernetes Pod membership is incomplete"
        )
    initial_attestation = controller._initial_kubernetes_attestation(wave)
    try:
        terminal_attestation = (
            controller.attest_terminal_ptg_wave_kubernetes_objects(
                wave.kubernetes_manifest,
                initial_attestation,
                actual_job,
                pods,
            )
        )
        redis_idle = await controller.attest_ptg_wave_pre_cleanup(
            redis, manifest
        )
    except Exception as exc:
        raise controller.PTGWaveControllerHold(str(exc)) from exc
    await controller.persist_terminal_evidence(
        wave.wave_id,
        {
            "kubernetes": controller._kubernetes_terminal_receipt(
                wave, terminal_attestation
            ),
            "redis": controller._redis_terminal_receipt(redis_idle),
        },
    )


async def reconcile_cleanup(
    controller: ModuleType,
    bundle: Any,
    manifest: Any,
    redis: Any,
) -> None:
    """Perform one ticketed cleanup or advance to terminal."""

    wave = bundle.wave
    if wave.redis_cleanup_evidence_digest is None:
        await _reconcile_redis_cleanup(controller, wave, manifest, redis)
        return
    if wave.kubernetes_delete_evidence_digest is None:
        await controller._reconcile_kubernetes_delete(
            bundle, expected_state="cleaning"
        )
        return
    await controller.persist_cleanup_and_terminal(wave.wave_id)


async def _reconcile_redis_cleanup(
    controller: ModuleType,
    wave: Any,
    manifest: Any,
    redis: Any,
) -> None:
    operation = await controller.mark_redis_cleanup_started(
        wave.wave_id,
        operation_ticket=controller._ticket("redis-cleanup"),
    )
    try:
        cleanup_receipt = None
        if wave.failure_receipt_digest is not None:
            expected_digest = controller._failure_redis_attestation_digest(wave)
            if operation.get("owner"):
                cleanup_receipt = (
                    await controller.cleanup_ptg_small_wave_unclaimed_failure_redis(
                        redis,
                        manifest,
                        expected_attestation_digest=expected_digest,
                    )
                )
            absence = (
                await controller.attest_ptg_small_wave_unclaimed_failure_redis_post_cleanup(
                    redis,
                    manifest,
                    expected_attestation_digest=expected_digest,
                )
            )
        else:
            if operation.get("owner"):
                cleanup_receipt = (
                    await controller.cleanup_ptg_small_wave_terminal_state(
                        redis, manifest
                    )
                )
            absence = await controller.attest_ptg_wave_post_cleanup(
                redis, manifest
            )
        await controller.record_redis_cleanup_absent(
            wave.wave_id,
            controller._redis_cleanup_receipt(
                wave, operation, cleanup_receipt, absence
            ),
        )
    except BaseException:
        if operation.get("owner"):
            await controller.mark_uncertain(
                wave.wave_id, expected_state="cleaning"
            )
        raise


async def should_snapshot_preclaim_failure(
    controller: ModuleType,
    bundle: Any,
    manifest: Any,
    redis: Any,
) -> bool:
    """Persist a prestart failure only after the original 12 Pods fail."""

    wave = bundle.wave
    if wave.kubernetes_ready_attestation_digest is None:
        return False
    actual_job = await asyncio.to_thread(
        controller.get_wave_job, wave.wave_digest
    )
    if actual_job is None:
        raise controller.PTGWaveControllerHold(
            "attested Kubernetes Job is absent before terminal reconciliation"
        )
    if not controller._job_reports_terminal_failure(actual_job):
        return False
    actual_pods = await asyncio.to_thread(
        controller.list_wave_pods, wave.wave_digest
    )
    try:
        failure_attestation = (
            controller.attest_preclaim_failure_ptg_wave_kubernetes_objects(
                wave.kubernetes_manifest,
                controller._initial_kubernetes_attestation(wave),
                actual_job,
                actual_pods,
            )
        )
    except controller.PTGWaveContractError as exc:
        raise controller.PTGWaveControllerHold(str(exc)) from exc
    if wave.state == "slots_waiting":
        await _snapshot_unclaimed_prestart(
            controller, wave, failure_attestation
        )
        return True
    await _snapshot_claimed_prestart(
        controller, wave, manifest, redis, failure_attestation
    )
    return True


async def _snapshot_unclaimed_prestart(
    controller: ModuleType, wave: Any, failure_attestation: Any
) -> None:
    await controller.snapshot_unclaimed_dead_letter_outcomes(
        wave.wave_id,
        failure_receipt=controller._unclaimed_failure_receipt(
            wave,
            origin_state=wave.state,
            reason="pre_claim_failure",
            operation="worker_start",
            operation_ticket=None,
            evidence=failure_attestation.as_mapping(),
        ),
    )


async def _snapshot_claimed_prestart(
    controller: ModuleType,
    wave: Any,
    manifest: Any,
    redis: Any,
    failure_attestation: Any,
) -> None:
    try:
        redis_idle = (
            await controller.attest_ptg_small_wave_unclaimed_failure_redis(
                redis, manifest
            )
        )
        await controller.snapshot_claimed_prestart_dead_letter_outcomes(
            wave.wave_id,
            kubernetes_evidence=failure_attestation.as_mapping(),
            redis_evidence=redis_idle.as_mapping(),
        )
    except controller.PTGWaveFailureConflict as exc:
        raise controller.PTGWaveControllerHold(str(exc)) from exc
    except Exception as exc:
        raise controller.PTGWaveControllerHold(
            "claimed-prestart failure lacks exact idle Redis evidence"
        ) from exc


def has_terminal_job_failure(controller: ModuleType, actual_job: object) -> bool:
    """Return whether the exact Kubernetes Job has a failed condition."""

    if not isinstance(actual_job, dict):
        raise controller.PTGWaveControllerHold(
            "Kubernetes Job observation is invalid"
        )
    status = actual_job.get("status")
    if not isinstance(status, dict):
        return False
    conditions = status.get("conditions")
    return isinstance(conditions, list) and any(
        isinstance(condition, dict)
        and condition.get("type") == "Failed"
        and condition.get("status") == "True"
        for condition in conditions
    )


def needs_early_kubernetes_stop(wave: Any) -> bool:
    """Return whether an unreleased failure requires early Job deletion."""

    failure_receipt = wave.failure_receipt
    return (
        isinstance(failure_receipt, dict)
        and failure_receipt.get("reason") == "redis_release_absent"
    )


async def reconcile_kubernetes_delete(
    controller: ModuleType,
    bundle: Any,
    *,
    expected_state: str,
) -> None:
    """Apply one ticketed Kubernetes deletion and attest exact absence."""

    wave = bundle.wave
    operation = await controller.mark_kubernetes_delete_started(
        wave.wave_id,
        operation_ticket=controller._ticket("k8s-delete"),
    )
    try:
        if operation.get("owner") and operation.get("delete_permitted"):
            await asyncio.to_thread(
                controller.delete_wave_job,
                wave.wave_digest,
                operation["job_uid"],
            )
        evidence = await controller._observe_kubernetes_delete_absence(
            bundle,
            operation.get("operation_ticket") or wave.kubernetes_delete_ticket,
        )
        await controller.record_kubernetes_delete_absent(
            wave.wave_id, evidence
        )
    except controller.PTGWaveControllerHold:
        raise
    except BaseException:
        if operation.get("owner"):
            await controller.mark_uncertain(
                wave.wave_id, expected_state=expected_state
            )
        raise


async def observe_kubernetes_delete_absence(
    controller: ModuleType,
    bundle: Any,
    operation_ticket: str | None,
) -> dict[str, Any]:
    """Observe, but never mutate, exact Job and Pod absence."""

    wave = bundle.wave
    absence_observation_map = await asyncio.to_thread(
        controller.wave_absence_observation, wave.wave_digest
    )
    if not (
        absence_observation_map["job_absent"]
        and absence_observation_map["pods_absent"]
    ):
        raise controller.PTGWaveControllerHold(
            "waiting for exact Kubernetes Job and Pod absence"
        )
    return controller._kubernetes_absence_receipt(
        wave,
        absence_observation_map,
        operation_ticket=operation_ticket,
    )


async def reconcile_redis_cleanup_get_only(
    controller: ModuleType,
    bundle: Any,
    manifest: Any,
    redis: Any,
    operation_ticket: str,
) -> None:
    """Attest cleanup after an ambiguous ticket without replaying mutation."""

    wave = bundle.wave
    if wave.failure_receipt_digest is not None:
        expected_digest = controller._failure_redis_attestation_digest(wave)
        absence = (
            await controller.attest_ptg_small_wave_unclaimed_failure_redis_post_cleanup(
                redis,
                manifest,
                expected_attestation_digest=expected_digest,
            )
        )
    else:
        absence = await controller.attest_ptg_wave_post_cleanup(redis, manifest)
    await controller.record_redis_cleanup_absent(
        wave.wave_id,
        controller._redis_cleanup_receipt(
            wave,
            {"owner": False, "operation_ticket": operation_ticket},
            None,
            absence,
        ),
    )


def failure_redis_attestation_digest(
    controller: ModuleType, wave: Any
) -> str:
    """Return the exact pre-cleanup Redis failure attestation digest."""

    terminal_summary = wave.terminal_summary
    pre_cleanup = (
        terminal_summary.get("redis_pre_cleanup")
        if isinstance(terminal_summary, dict)
        else None
    )
    attestation_digest = (
        pre_cleanup.get("attestation_digest")
        if isinstance(pre_cleanup, dict)
        else None
    )
    if (
        not isinstance(attestation_digest, str)
        or len(attestation_digest) != 64
        or any(
            character not in "0123456789abcdef"
            for character in attestation_digest
        )
    ):
        raise controller.PTGWaveStateConflict(
            "failure cleanup lacks its exact Redis pre-cleanup digest"
        )
    return attestation_digest
