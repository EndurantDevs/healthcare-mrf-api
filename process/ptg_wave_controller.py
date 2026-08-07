# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Durable reconciliation loop for one exact PTGSmall import wave."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import sys
from dataclasses import dataclass
from typing import Any, Iterable

from arq import create_pool
from arq.constants import health_check_key_suffix
from sqlalchemy import exists, select

from api.ptg_wave_kubernetes import (
    PTGWaveContractError,
    build_ptg_wave_job,
    validate_ptg_wave_job_manifest,
)
from api.ptg_wave_kubernetes_attestation import (
    PTGWaveKubernetesAttestation,
    attest_existing_ptg_wave_job,
    attest_ptg_wave_kubernetes_objects,
)
from api.ptg_wave_kubernetes_client import (
    delete_wave_job,
    get_wave_job,
    list_generic_ptg_jobs,
    list_wave_pods,
    post_wave_job,
    wave_absence_observation,
)
from api.ptg_wave_kubernetes_terminal_attestation import (
    attest_terminal_ptg_wave_kubernetes_objects,
)
from api.ptg_wave_kubernetes_failure_attestation import (
    attest_preclaim_failure_ptg_wave_kubernetes_objects,
)
from db.models import ImportRun, PTGImportWave, PTGImportWaveIntent, db
from process._ptg_wave_redis_models import (
    PTGSmallWaveJob,
    PTGSmallWaveManifest,
    PTGSmallWaveReceipt,
    PTGSmallWaveRuntimeIdentity,
)
from process.ptg_parts.ptg_wave_admission_fence import (
    PTG_ACTIVE_RUN_STATES,
    PTG_WAVE_CAPACITY_OWNING_STATES,
    PTG_WAVE_FENCED_IMPORTERS,
)
from process.ptg_wave_failure import (
    PTGWaveFailureConflict,
    PTGWaveReadOnlyRecovery,
    read_only_recovery_plan,
    snapshot_claimed_prestart_dead_letter_outcomes,
    snapshot_unclaimed_dead_letter_outcomes,
)
from process.ptg_wave_cleanup import (
    begin_terminalizing,
    mark_kubernetes_delete_started,
    mark_redis_cleanup_started,
    persist_cleanup_and_terminal,
    persist_terminal_evidence,
    record_kubernetes_delete_absent,
    record_redis_cleanup_absent,
)
from process.ptg_wave_outcomes import snapshot_terminal_outcomes
from process.ptg_wave_redis import (
    attest_ptg_small_wave_unclaimed_failure_redis,
    attest_ptg_small_wave_unclaimed_failure_redis_post_cleanup,
    attest_ptg_wave_post_cleanup,
    attest_ptg_wave_pre_cleanup,
    bind_ptg_small_wave_runtime_identity,
    cleanup_ptg_small_wave_terminal_state,
    cleanup_ptg_small_wave_unclaimed_failure_redis,
    inspect_ptg_small_wave_readiness,
    publish_ptg_small_wave,
    read_ptg_small_wave_release,
    restore_ptg_small_wave_manifest,
)
from process.ptg_wave_state import (
    PTGWaveStateConflict,
    canonical_json,
    mark_kubernetes_post_started,
    mark_redis_release_started,
    mark_uncertain,
    persist_materialization,
    record_kubernetes_job_created,
    record_kubernetes_ready,
    record_redis_release,
    resolve_uncertainty,
    sha256_digest,
)
from process.redis_config import build_redis_settings
from process import ptg_wave_controller_isolation as _controller_isolation
from process import ptg_wave_controller_operations as _controller_operations
from process import ptg_wave_controller_receipts as _controller_receipts
from process import ptg_wave_controller_terminal as _controller_terminal


logger = logging.getLogger(__name__)
CONTROLLER_ENABLED_ENV = "HLTHPRT_PTG_WAVE_CONTROLLER_ENABLED"
WORKER_IMAGE_ENV = "HLTHPRT_PTG_WAVE_WORKER_IMAGE"
RUNTIME_IMAGE_ENV = "HLTHPRT_PTG_WAVE_RUNTIME_IMAGE_IDENTITY"
BARRIER_FACTORY = "process.ptg_wave_redis_adapter.create_ptg_wave_redis_barrier"
_PTG_BASE_QUEUES = (
    "arq:PTG",
    "arq:PTGSmall",
    "arq:PTGNormal",
    "arq:PTGLarge",
    "arq:PTGHuge",
    "arq:PTGCandidateAudit",
)


class PTGWaveControllerHold(RuntimeError):
    """The controller observed a safe, nonterminal condition and must wait."""


@dataclass(frozen=True)
class PTGWaveBundle:
    wave: PTGImportWave
    intents: tuple[PTGImportWaveIntent, ...]


def is_controller_enabled(environ: dict[str, str] | None = None) -> bool:
    """Return whether the exact PTG wave controller is explicitly enabled."""

    env = os.environ if environ is None else environ
    return str(env.get(CONTROLLER_ENABLED_ENV) or "").strip().lower() in {
        "1", "true", "yes", "on",
    }


controller_enabled = is_controller_enabled


def _controller_runtime_config() -> tuple[str, str]:
    image = str(os.getenv(WORKER_IMAGE_ENV) or "").strip()
    runtime_image = str(os.getenv(RUNTIME_IMAGE_ENV) or "").strip()
    # The pure Job builder performs the exact canonical validation.  Validate
    # once at controller startup too so enablement is fail-closed.
    build_ptg_wave_job(
        wave_digest="0" * 64,
        manifest_digest="1" * 64,
        jobs_digest="2" * 64,
        job_count=1,
        image=image,
        runtime_image_identity=runtime_image,
        barrier_factory=BARRIER_FACTORY,
    )
    return image, runtime_image


async def load_capacity_owning_wave() -> PTGWaveBundle | None:
    """Load the sole capacity-owning PTG wave and its complete intent set."""

    async with db.session() as session:
        waves = (await session.execute(
            select(PTGImportWave)
            .where(PTGImportWave.state.in_(PTG_WAVE_CAPACITY_OWNING_STATES))
            .order_by(PTGImportWave.created_at, PTGImportWave.wave_id)
            .limit(2)
        )).scalars().all()
        if not waves:
            return None
        if len(waves) != 1:
            raise PTGWaveStateConflict("PTG wave capacity ownership is ambiguous")
        wave = waves[0]
        intents = tuple((await session.execute(
            select(PTGImportWaveIntent)
            .where(PTGImportWaveIntent.wave_id == wave.wave_id)
            .order_by(PTGImportWaveIntent.ordinal)
        )).scalars().all())
    if len(intents) != wave.intent_count or [item.ordinal for item in intents] != list(range(wave.intent_count)):
        raise PTGWaveStateConflict("persisted exact-wave intents are incomplete")
    return PTGWaveBundle(wave=wave, intents=intents)


def restore_wave_manifest(bundle: PTGWaveBundle) -> PTGSmallWaveManifest:
    """Restore the exact persisted Redis manifest without reserializing jobs."""

    wave = bundle.wave
    jobs = tuple(
        PTGSmallWaveJob(
            ordinal=intent.ordinal,
            job_id=intent.job_id,
            score_ms=wave.enqueue_time_ms,
            serialized_job=bytes(intent.serialized_job),
            serialized_job_digest=intent.serialized_job_digest,
        )
        for intent in bundle.intents
    )
    manifest = restore_ptg_small_wave_manifest(
        jobs,
        execution_digest=wave.wave_digest,
        jobs_digest=wave.jobs_digest,
        manifest_digest=wave.manifest_digest,
        protocol_identity=wave.protocol_identity,
        serializer_identity=wave.serializer_identity,
    )
    if wave.kubernetes_manifest_identity is None:
        return manifest
    return bind_ptg_small_wave_runtime_identity(
        manifest,
        PTGSmallWaveRuntimeIdentity(
            config_identity=wave.kubernetes_config_identity,
            kubernetes_manifest_identity=wave.kubernetes_manifest_identity,
            image_identity=wave.pinned_image_reference,
            runtime_image_identity=wave.runtime_image_identity,
        ),
    )


async def reconcile_ptg_wave_once(redis: Any, *, image: str, runtime_image: str) -> str:
    """Advance at most one safe state-machine step for the capacity-owning wave."""

    bundle = await load_capacity_owning_wave()
    if bundle is None:
        return "idle"
    wave = bundle.wave
    recovery = read_only_recovery_plan(wave)
    if recovery is not None:
        return await _reconcile_read_only_recovery(bundle, redis, recovery)
    if wave.state == "uncertain":
        return await _reconcile_uncertain(bundle, redis)
    if wave.state == "admitted":
        await _materialize(bundle, redis, image=image, runtime_image=runtime_image)
        return "materialized"
    manifest = restore_wave_manifest(bundle)
    if wave.state in {"slots_waiting", "released", "executing"}:
        if await _maybe_snapshot_preclaim_failure(bundle, manifest, redis):
            return "preclaim-failure-dead-lettered"
    if wave.state == "materializing":
        await _post_job_once(bundle)
        return "kubernetes-post-started"
    if wave.state == "slots_waiting":
        await _reconcile_slots(bundle, manifest, redis)
        return "slots-waiting"
    if wave.state == "redis_releasing":
        await _reconcile_redis_release(bundle, manifest, redis, mutate=False)
        return "redis-reconciling"
    if wave.state == "released":
        return "released"
    if wave.state == "executing":
        if await _all_wave_runs_terminal(bundle):
            await snapshot_terminal_outcomes(wave.wave_id)
            return "outcomes-snapshotted"
        return "executing"
    if wave.state == "awaiting_linkage":
        if wave.linkage_ack_digest:
            await begin_terminalizing(wave.wave_id)
            return "terminalizing"
        return "awaiting-linkage"
    if wave.state == "terminalizing":
        if _requires_early_kubernetes_stop(wave) and wave.kubernetes_delete_evidence_digest is None:
            await _reconcile_kubernetes_delete(bundle, expected_state="terminalizing")
            return "failure-kubernetes-stopping"
        await _persist_terminal_proof(bundle, manifest, redis)
        return "terminal-proof-persisted"
    if wave.state == "cleaning":
        await _reconcile_cleanup(bundle, manifest, redis)
        return "cleaning"
    raise PTGWaveStateConflict(f"unsupported capacity-owning wave state: {wave.state}")


async def _materialize(
    bundle: PTGWaveBundle,
    redis: Any,
    *,
    image: str,
    runtime_image: str,
) -> None:
    await _controller_operations.materialize_wave(
        sys.modules[__name__],
        bundle,
        redis,
        image=image,
        runtime_image=runtime_image,
    )


async def _post_job_once(bundle: PTGWaveBundle) -> None:
    await _controller_operations.post_wave_job_once(
        sys.modules[__name__], bundle
    )


async def _reconcile_slots(
    bundle: PTGWaveBundle,
    manifest: PTGSmallWaveManifest,
    redis: Any,
) -> None:
    await _controller_operations.reconcile_slots(
        sys.modules[__name__], bundle, manifest, redis
    )


async def _reconcile_redis_release(
    bundle: PTGWaveBundle,
    manifest: PTGSmallWaveManifest,
    redis: Any,
    *,
    mutate: bool,
) -> None:
    await _controller_operations.reconcile_redis_release(
        sys.modules[__name__], bundle, manifest, redis, mutate=mutate
    )


async def _reconcile_uncertain(bundle: PTGWaveBundle, redis: Any) -> str:
    return await _controller_operations.reconcile_uncertain(
        sys.modules[__name__], bundle, redis
    )


async def _reconcile_read_only_recovery(
    bundle: PTGWaveBundle,
    redis: Any,
    recovery: PTGWaveReadOnlyRecovery,
) -> str:
    return await _controller_operations.reconcile_read_only_recovery(
        sys.modules[__name__], bundle, redis, recovery
    )


async def _persist_terminal_proof(
    bundle: PTGWaveBundle,
    manifest: PTGSmallWaveManifest,
    redis: Any,
) -> None:
    await _controller_terminal.persist_terminal_proof(
        sys.modules[__name__], bundle, manifest, redis
    )


async def _reconcile_cleanup(
    bundle: PTGWaveBundle,
    manifest: PTGSmallWaveManifest,
    redis: Any,
) -> None:
    await _controller_terminal.reconcile_cleanup(
        sys.modules[__name__], bundle, manifest, redis
    )


async def _should_snapshot_preclaim_failure(
    bundle: PTGWaveBundle,
    manifest: PTGSmallWaveManifest,
    redis: Any,
) -> bool:
    return await _controller_terminal.should_snapshot_preclaim_failure(
        sys.modules[__name__], bundle, manifest, redis
    )


def _has_terminal_job_failure(actual_job: object) -> bool:
    return _controller_terminal.has_terminal_job_failure(sys.modules[__name__], actual_job)


_needs_early_kubernetes_stop = _controller_terminal.needs_early_kubernetes_stop


async def _reconcile_kubernetes_delete(
    bundle: PTGWaveBundle,
    *,
    expected_state: str,
) -> None:
    await _controller_terminal.reconcile_kubernetes_delete(
        sys.modules[__name__], bundle, expected_state=expected_state
    )


async def _observe_kubernetes_delete_absence(
    bundle: PTGWaveBundle,
    operation_ticket: str | None,
) -> dict[str, Any]:
    return await _controller_terminal.observe_kubernetes_delete_absence(
        sys.modules[__name__], bundle, operation_ticket
    )


async def _reconcile_redis_cleanup_get_only(
    bundle: PTGWaveBundle,
    manifest: PTGSmallWaveManifest,
    redis: Any,
    operation_ticket: str,
) -> None:
    await _controller_terminal.reconcile_redis_cleanup_get_only(
        sys.modules[__name__],
        bundle,
        manifest,
        redis,
        operation_ticket,
    )


def _failure_redis_attestation_digest(wave: PTGImportWave) -> str:
    return _controller_terminal.failure_redis_attestation_digest(
        sys.modules[__name__], wave
    )


async def _require_ptg_only_idle(bundle: PTGWaveBundle, redis: Any) -> None:
    await _controller_isolation.require_ptg_only_idle(
        sys.modules[__name__], bundle, redis
    )


_is_generic_job_nonterminal = _controller_isolation.is_generic_job_nonterminal


async def _has_only_terminal_wave_runs(bundle: PTGWaveBundle) -> bool:
    return await _controller_isolation.has_only_terminal_wave_runs(
        sys.modules[__name__], bundle
    )


_kubernetes_job_receipt = _controller_receipts.kubernetes_job_receipt
_kubernetes_ready_receipt = _controller_receipts.kubernetes_ready_receipt
_assert_slot_membership = _controller_receipts.assert_slot_membership
_redis_release_receipt = _controller_receipts.redis_release_receipt
_initial_kubernetes_attestation = _controller_receipts.initial_kubernetes_attestation
_kubernetes_terminal_receipt = _controller_receipts.kubernetes_terminal_receipt
_redis_terminal_receipt = _controller_receipts.redis_terminal_receipt
_redis_post_cleanup_receipt = _controller_receipts.redis_post_cleanup_receipt
_redis_cleanup_receipt = _controller_receipts.redis_cleanup_receipt
_kubernetes_absence_receipt = _controller_receipts.kubernetes_absence_receipt
_ticket = _controller_receipts.operation_ticket
_unclaimed_failure_receipt = _controller_receipts.unclaimed_failure_receipt


async def run_ptg_wave_controller(redis: Any, *, image: str, runtime_image: str) -> None:
    """Continuously reconcile the exact PTG wave while remaining fail-closed."""

    interval = max(float(os.getenv("HLTHPRT_PTG_WAVE_CONTROLLER_INTERVAL_SECONDS", "2")), 0.25)
    while True:
        try:
            await reconcile_ptg_wave_once(redis, image=image, runtime_image=runtime_image)
        except PTGWaveControllerHold:
            logger.debug("exact PTG wave reconciliation is waiting", exc_info=True)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("exact PTG wave reconciliation failed closed")
        await asyncio.sleep(interval)


async def start_ptg_wave_controller(app: Any) -> None:
    """Start one controller task only when explicit runtime configuration permits."""

    if not controller_enabled():
        return
    image, runtime_image = _controller_runtime_config()
    redis = await create_pool(build_redis_settings())
    app.ctx.ptg_wave_redis = redis
    app.ctx.ptg_wave_controller_task = asyncio.create_task(
        run_ptg_wave_controller(redis, image=image, runtime_image=runtime_image),
        name="ptg-exact-wave-controller",
    )


async def stop_ptg_wave_controller(app: Any) -> None:
    """Stop the controller task and close its Redis client without touching work."""

    task = getattr(app.ctx, "ptg_wave_controller_task", None)
    if task is not None:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    redis = getattr(app.ctx, "ptg_wave_redis", None)
    if redis is not None:
        close = getattr(redis, "aclose", None) or getattr(redis, "close", None)
        if close is not None:
            result = close()
            if asyncio.iscoroutine(result):
                await result


_maybe_snapshot_preclaim_failure = _should_snapshot_preclaim_failure
_job_reports_terminal_failure = _has_terminal_job_failure
_requires_early_kubernetes_stop = _needs_early_kubernetes_stop
_generic_job_nonterminal = _is_generic_job_nonterminal
_all_wave_runs_terminal = _has_only_terminal_wave_runs


__all__ = [
    "PTGWaveControllerHold",
    "controller_enabled",
    "load_capacity_owning_wave",
    "reconcile_ptg_wave_once",
    "restore_wave_manifest",
    "run_ptg_wave_controller",
    "start_ptg_wave_controller",
    "stop_ptg_wave_controller",
]
