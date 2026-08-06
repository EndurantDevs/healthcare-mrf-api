"""PTG-only capacity and exact-run isolation checks."""

from __future__ import annotations

import asyncio
from types import ModuleType
from typing import Any


async def require_ptg_only_idle(
    controller: ModuleType, bundle: Any, redis: Any
) -> None:
    """Require no generic PTG DB, Redis, or Kubernetes work."""

    wave = bundle.wave
    non_wave_run_exists = controller.exists(
        controller.select(controller.PTGImportWaveIntent.run_id).where(
            controller.PTGImportWaveIntent.run_id == controller.ImportRun.run_id
        )
    )
    active_run_rows = (
        await controller.db.execute(
            controller.select(controller.ImportRun.run_id)
            .where(
                controller.ImportRun.importer.in_(
                    controller.PTG_WAVE_FENCED_IMPORTERS
                ),
                controller.ImportRun.status.in_(
                    controller.PTG_ACTIVE_RUN_STATES
                ),
                ~non_wave_run_exists,
            )
            .limit(1)
        )
    ).all()
    if active_run_rows:
        raise controller.PTGWaveControllerHold(
            "non-wave PTG database work is active"
        )
    await _require_generic_redis_idle(controller, redis)
    generic_jobs = await asyncio.to_thread(controller.list_generic_ptg_jobs)
    if any(controller._generic_job_nonterminal(job) for job in generic_jobs):
        raise controller.PTGWaveControllerHold(
            "a generic PTG Kubernetes Job is nonterminal"
        )
    if wave.intent_count != len(bundle.intents):
        raise controller.PTGWaveStateConflict(
            "wave preflight lost an admitted intent"
        )


async def _require_generic_redis_idle(
    controller: ModuleType, redis: Any
) -> None:
    async with redis.pipeline(transaction=True) as redis_pipeline:
        for queue_name in controller._PTG_BASE_QUEUES:
            redis_pipeline.zcard(queue_name)
            redis_pipeline.get(
                queue_name + controller.health_check_key_suffix
            )
        queue_health_values = await redis_pipeline.execute()
    if any(
        int(queue_health_values[index])
        for index in range(0, len(queue_health_values), 2)
    ):
        raise controller.PTGWaveControllerHold(
            "a generic PTG queue is not empty"
        )
    if any(
        queue_health_values[index] is not None
        for index in range(1, len(queue_health_values), 2)
    ):
        raise controller.PTGWaveControllerHold(
            "a generic PTG worker health key is present"
        )


def is_generic_job_nonterminal(job: dict[str, Any]) -> bool:
    """Return whether a generic PTG Job still owns live capacity."""

    metadata = job.get("metadata") if isinstance(job.get("metadata"), dict) else {}
    if metadata.get("deletionTimestamp"):
        return True
    status = job.get("status") if isinstance(job.get("status"), dict) else {}
    active_count = int(status.get("active") or 0)
    succeeded_count = int(status.get("succeeded") or 0)
    failed_count = int(status.get("failed") or 0)
    return active_count > 0 or (succeeded_count == 0 and failed_count == 0)


async def has_only_terminal_wave_runs(
    controller: ModuleType, bundle: Any
) -> bool:
    """Return whether every exact-wave ImportRun is terminal."""

    run_status_rows = (
        await controller.db.execute(
            controller.select(controller.ImportRun.status)
            .join(
                controller.PTGImportWaveIntent,
                controller.PTGImportWaveIntent.run_id
                == controller.ImportRun.run_id,
            )
            .where(
                controller.PTGImportWaveIntent.wave_id
                == bundle.wave.wave_id
            )
            .order_by(controller.PTGImportWaveIntent.ordinal)
        )
    ).all()
    terminal_statuses = {"succeeded", "failed", "canceled", "dead_letter"}
    return len(run_status_rows) == bundle.wave.intent_count and all(
        run_status_row[0] in terminal_statuses
        for run_status_row in run_status_rows
    )
