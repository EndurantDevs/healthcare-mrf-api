"""Materialization, release, and read-only recovery controller steps."""

from __future__ import annotations

import asyncio
from types import ModuleType
from typing import Any


async def materialize_wave(
    controller: ModuleType,
    bundle: Any,
    redis: Any,
    *,
    image: str,
    runtime_image: str,
) -> None:
    """Persist the exact Redis and Kubernetes execution identities."""

    await controller._require_ptg_only_idle(bundle, redis)
    restored = controller.restore_wave_manifest(bundle)
    manifest = controller.build_ptg_wave_job(
        wave_digest=bundle.wave.wave_digest,
        manifest_digest=restored.manifest_digest,
        jobs_digest=restored.jobs_digest,
        job_count=len(restored.jobs),
        image=image,
        runtime_image_identity=runtime_image,
        barrier_factory=controller.BARRIER_FACTORY,
    )
    contract = controller.validate_ptg_wave_job_manifest(manifest)
    await controller.persist_materialization(
        bundle.wave.wave_id,
        manifest=manifest,
        manifest_bytes=controller.canonical_json(manifest),
        image_reference=contract.image,
        image_digest=contract.image.rsplit("@sha256:", 1)[1],
        runtime_image_identity=contract.runtime_image_identity,
        config_identity=contract.config_identity,
        manifest_identity=contract.manifest_identity,
    )


async def post_wave_job_once(controller: ModuleType, bundle: Any) -> None:
    """Apply one ticketed Kubernetes POST at most once."""

    wave_id = bundle.wave.wave_id
    operation = await controller.mark_kubernetes_post_started(
        wave_id,
        operation_ticket=controller._ticket("k8s-post"),
    )
    if not operation.get("owner"):
        return
    try:
        actual_job = await asyncio.to_thread(
            controller.post_wave_job, operation["manifest"]
        )
        receipt = controller._kubernetes_job_receipt(
            operation["manifest"], actual_job
        )
        await controller.record_kubernetes_job_created(wave_id, receipt)
    except BaseException:
        await controller.mark_uncertain(
            wave_id, expected_state="slots_waiting"
        )
        raise


async def reconcile_slots(
    controller: ModuleType,
    bundle: Any,
    manifest: Any,
    redis: Any,
) -> None:
    """Attest the exact 12 slots and begin the single Redis release."""

    wave = bundle.wave
    job_manifest = wave.kubernetes_manifest
    actual_job = await asyncio.to_thread(
        controller.get_wave_job, wave.wave_digest
    )
    if actual_job is None:
        raise controller.PTGWaveControllerHold(
            "ticketed Kubernetes Job is not observable"
        )
    if wave.kubernetes_job_receipt_digest is None:
        await controller.record_kubernetes_job_created(
            wave.wave_id,
            controller._kubernetes_job_receipt(job_manifest, actual_job),
        )
    actual_pods = await asyncio.to_thread(
        controller.list_wave_pods, wave.wave_digest
    )
    if len(actual_pods) != 12:
        raise controller.PTGWaveControllerHold("waiting for exactly 12 wave Pods")
    try:
        kubernetes = controller.attest_ptg_wave_kubernetes_objects(
            job_manifest, actual_job, actual_pods
        )
    except controller.PTGWaveContractError as exc:
        raise controller.PTGWaveControllerHold(str(exc)) from exc
    ready = await controller.inspect_ptg_small_wave_readiness(
        redis, manifest.reference
    )
    if not ready.ready or ready.released:
        raise controller.PTGWaveControllerHold(
            "waiting for exact unreleased Redis slot readiness"
        )
    controller._assert_slot_membership(kubernetes, ready.registered_slots)
    await controller.record_kubernetes_ready(
        wave.wave_id,
        controller._kubernetes_ready_receipt(job_manifest, kubernetes),
    )
    is_release_owner = await controller.mark_redis_release_started(
        wave.wave_id,
        operation_ticket=controller._ticket("redis-release"),
    )
    if is_release_owner:
        await controller._reconcile_redis_release(
            bundle, manifest, redis, mutate=True
        )


async def reconcile_redis_release(
    controller: ModuleType,
    bundle: Any,
    manifest: Any,
    redis: Any,
    *,
    mutate: bool,
) -> None:
    """Apply or read back the exact ticketed Redis release."""

    try:
        release_receipt = (
            await controller.publish_ptg_small_wave(redis, manifest)
            if mutate
            else await controller.read_ptg_small_wave_release(redis, manifest)
        )
        await controller.record_redis_release(
            bundle.wave.wave_id,
            controller._redis_release_receipt(release_receipt),
        )
    except BaseException:
        if mutate:
            await controller.mark_uncertain(
                bundle.wave.wave_id,
                expected_state="redis_releasing",
            )
        raise


async def reconcile_uncertain(
    controller: ModuleType, bundle: Any, redis: Any
) -> str:
    """Read back a legacy uncertain operation without guessing a retry."""

    wave = bundle.wave
    resume_state = wave.uncertainty_resume_state
    if resume_state == "slots_waiting":
        actual_job = await asyncio.to_thread(
            controller.get_wave_job, wave.wave_digest
        )
        if actual_job is None:
            raise controller.PTGWaveControllerHold(
                "ambiguous Kubernetes POST remains absent"
            )
        receipt = controller._kubernetes_job_receipt(
            wave.kubernetes_manifest, actual_job
        )
        await controller.resolve_uncertainty(
            wave.wave_id, reconciled_state="slots_waiting"
        )
        await controller.record_kubernetes_job_created(wave.wave_id, receipt)
        return "kubernetes-post-reconciled"
    manifest = controller.restore_wave_manifest(bundle)
    if resume_state == "redis_releasing":
        try:
            receipt = await controller.read_ptg_small_wave_release(redis, manifest)
        except Exception as exc:
            raise controller.PTGWaveControllerHold(
                "ambiguous Redis release remains absent"
            ) from exc
        await controller.resolve_uncertainty(
            wave.wave_id, reconciled_state="redis_releasing"
        )
        await controller.record_redis_release(
            wave.wave_id, controller._redis_release_receipt(receipt)
        )
        return "redis-release-reconciled"
    if resume_state == "cleaning":
        await controller.resolve_uncertainty(
            wave.wave_id, reconciled_state="cleaning"
        )
        return "cleanup-get-only-reconciliation"
    raise controller.PTGWaveControllerHold(
        "uncertain wave requires failure reconciliation"
    )


async def reconcile_read_only_recovery(
    controller: ModuleType,
    bundle: Any,
    redis: Any,
    recovery: Any,
) -> str:
    """Resolve a committed external-operation ticket without replaying it."""

    if recovery.mutation_permitted:
        raise controller.PTGWaveStateConflict(
            "read-only recovery unexpectedly permits mutation"
        )
    handler_by_operation = {
        "kubernetes_post": _recover_kubernetes_post,
        "redis_release": _recover_redis_release,
        "redis_cleanup": _recover_redis_cleanup,
        "kubernetes_delete": _recover_kubernetes_delete,
    }
    handler = handler_by_operation.get(recovery.operation)
    if handler is None:
        raise controller.PTGWaveStateConflict(
            "unsupported exact-wave recovery operation"
        )
    return await handler(controller, bundle, redis, recovery)


async def _recover_kubernetes_post(
    controller: ModuleType,
    bundle: Any,
    redis: Any,
    recovery: Any,
) -> str:
    del redis
    wave = bundle.wave
    actual_job = await asyncio.to_thread(
        controller.get_wave_job, wave.wave_digest
    )
    if actual_job is not None:
        receipt = controller._kubernetes_job_receipt(
            wave.kubernetes_manifest, actual_job
        )
        if wave.state == "uncertain":
            await controller.resolve_uncertainty(
                wave.wave_id, reconciled_state="slots_waiting"
            )
        await controller.record_kubernetes_job_created(wave.wave_id, receipt)
        return "kubernetes-post-reconciled"
    pods = await asyncio.to_thread(controller.list_wave_pods, wave.wave_digest)
    if pods:
        raise controller.PTGWaveControllerHold(
            "Kubernetes POST recovery found labeled Pods without its Job"
        )
    if wave.state == "uncertain":
        await controller.resolve_uncertainty(
            wave.wave_id, reconciled_state="slots_waiting"
        )
    absence_observation_map = {
        "wave_digest": wave.wave_digest,
        "manifest_identity": wave.kubernetes_manifest_identity,
        "job_name": wave.kubernetes_manifest.get("metadata", {}).get("name"),
        "job_absent": True,
        "pod_count": 0,
        "pods_absent": True,
    }
    await controller.snapshot_unclaimed_dead_letter_outcomes(
        wave.wave_id,
        failure_receipt=controller._unclaimed_failure_receipt(
            wave,
            origin_state="slots_waiting",
            reason="kubernetes_post_absent",
            operation="kubernetes_post",
            operation_ticket=recovery.ticket,
            evidence=absence_observation_map,
        ),
    )
    return "kubernetes-post-absent-dead-lettered"


async def _recover_redis_release(
    controller: ModuleType,
    bundle: Any,
    redis: Any,
    recovery: Any,
) -> str:
    wave = bundle.wave
    manifest = controller.restore_wave_manifest(bundle)
    try:
        attestation = (
            await controller.attest_ptg_small_wave_unclaimed_failure_redis(
                redis, manifest
            )
        )
    except Exception as exc:
        raise controller.PTGWaveControllerHold(
            "Redis release recovery lacks an exact stable attestation"
        ) from exc
    if wave.state == "uncertain":
        await controller.resolve_uncertainty(
            wave.wave_id, reconciled_state="redis_releasing"
        )
    if attestation.release_present:
        if attestation.release_receipt is None:
            raise controller.PTGWaveStateConflict(
                "released Redis witness lacks its receipt"
            )
        await controller.record_redis_release(
            wave.wave_id,
            controller._redis_release_receipt(attestation.release_receipt),
        )
        return "redis-release-reconciled"
    await controller.snapshot_unclaimed_dead_letter_outcomes(
        wave.wave_id,
        failure_receipt=controller._unclaimed_failure_receipt(
            wave,
            origin_state="redis_releasing",
            reason="redis_release_absent",
            operation="redis_release",
            operation_ticket=recovery.ticket,
            evidence=attestation.as_mapping(),
        ),
    )
    return "redis-release-absent-dead-lettered"


async def _recover_redis_cleanup(
    controller: ModuleType,
    bundle: Any,
    redis: Any,
    recovery: Any,
) -> str:
    wave = bundle.wave
    if wave.state == "uncertain":
        await controller.resolve_uncertainty(
            wave.wave_id, reconciled_state="cleaning"
        )
    await controller._reconcile_redis_cleanup_get_only(
        bundle,
        controller.restore_wave_manifest(bundle),
        redis,
        recovery.ticket,
    )
    return "redis-cleanup-get-only-reconciled"


async def _recover_kubernetes_delete(
    controller: ModuleType,
    bundle: Any,
    redis: Any,
    recovery: Any,
) -> str:
    del redis
    wave = bundle.wave
    resume_state = (
        wave.uncertainty_resume_state
        if wave.state == "uncertain"
        else wave.state
    )
    if resume_state not in {"terminalizing", "cleaning"}:
        raise controller.PTGWaveStateConflict(
            "Kubernetes deletion recovery has an invalid persisted resume state"
        )
    evidence = await controller._observe_kubernetes_delete_absence(
        bundle, recovery.ticket
    )
    if wave.state == "uncertain":
        await controller.resolve_uncertainty(
            wave.wave_id, reconciled_state=resume_state
        )
    await controller.record_kubernetes_delete_absent(wave.wave_id, evidence)
    return "kubernetes-delete-get-only-reconciled"
