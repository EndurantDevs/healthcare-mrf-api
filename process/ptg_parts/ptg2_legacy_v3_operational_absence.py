# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact no-scan Redis and worker absence evidence for legacy repair."""

from __future__ import annotations

import asyncio
import inspect
import os
from typing import Any, Mapping

from arq import create_pool

from api.control_workers import worker_state
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job


_PTG_QUEUES = (
    "arq:PTG",
    "arq:PTGSmall",
    "arq:PTGNormal",
    "arq:PTGLarge",
    "arq:PTGHuge",
    "arq:PTGCandidateAudit",
)
_PTG_WORKER_CLASSES = (
    "process.PTG",
    "process.PTGSmall",
    "process.PTGNormal",
    "process.PTGLarge",
    "process.PTGHuge",
    "process.PTGCandidateAudit",
)


async def _close_redis(redis: Any) -> None:
    close = getattr(redis, "aclose", None) or getattr(redis, "close", None)
    if close is None:
        return
    close_result = close()
    if inspect.isawaitable(close_result):
        await close_result


def _worker_identity_payload(
    outer_run: Mapping[str, Any],
    *,
    worker_class: str,
) -> dict[str, Any]:
    params = outer_run.get("params")
    params_by_name = params if isinstance(params, Mapping) else {}
    worker_payload_by_field = {
        "run_id": outer_run.get("run_id"),
        "importer": "ptg",
        "status": outer_run.get("status"),
        "worker_class": worker_class,
        "import_id": outer_run.get("import_id")
        or params_by_name.get("import_id"),
    }
    return {
        field_name: field_value
        for field_name, field_value in worker_payload_by_field.items()
        if field_value not in (None, "")
    }


def _operational_run_rows(
    outer_runs: list[Mapping[str, Any]],
    event_rows: list[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    run_by_id: dict[str, Mapping[str, Any]] = {}
    for outer_run in outer_runs:
        run_id = str(outer_run.get("run_id") or "").strip()
        if run_id:
            run_by_id[run_id] = outer_run
    for event_by_field in event_rows:
        event_run_id = str(
            event_by_field.get("outer_run_id") or ""
        ).strip()
        if event_run_id and event_run_id not in run_by_id:
            run_by_id[event_run_id] = {"run_id": event_run_id}
    return [run_by_id[run_id] for run_id in sorted(run_by_id)]


def _operational_job_ids(
    run_rows: list[Mapping[str, Any]],
    event_rows: list[Mapping[str, Any]],
) -> list[str]:
    job_ids: set[str] = set()
    for outer_run in run_rows:
        run_id = str(outer_run.get("run_id") or "").strip()
        if not run_id:
            continue
        job_ids.add(f"ptg_start_{run_id}")
        metrics = outer_run.get("metrics")
        metrics_by_name = metrics if isinstance(metrics, Mapping) else {}
        recorded_job_id = str(metrics_by_name.get("job_id") or "").strip()
        if recorded_job_id:
            job_ids.add(recorded_job_id)
    for event_by_field in event_rows:
        attempt_id = str(event_by_field.get("attempt_id") or "").strip()
        if attempt_id:
            job_ids.add(attempt_id)
    return sorted(job_ids)


async def _redis_operational_counts(job_ids: list[str]) -> tuple[int, int]:
    queue_memberships = 0
    redis_key_count = 0
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    try:
        for job_id in job_ids:
            for queue_name in _PTG_QUEUES:
                if await redis.zscore(queue_name, job_id) is not None:
                    queue_memberships += 1
            redis_key_count += int(
                await redis.exists(
                    f"arq:job:{job_id}",
                    f"arq:retry:{job_id}",
                    f"arq:in-progress:{job_id}",
                )
                or 0
            )
    finally:
        await _close_redis(redis)
    return queue_memberships, redis_key_count


async def _worker_operational_counts(
    run_rows: list[Mapping[str, Any]],
) -> tuple[int, int]:
    worker_present_count = 0
    worker_running_count = 0
    for outer_run in run_rows:
        for worker_class in _PTG_WORKER_CLASSES:
            state = await asyncio.to_thread(
                worker_state,
                _worker_identity_payload(
                    outer_run,
                    worker_class=worker_class,
                ),
            )
            items = state.get("items") if isinstance(state, Mapping) else None
            for item in items if isinstance(items, list) else []:
                if item.get("running"):
                    worker_running_count += 1
                if item.get("job_status") not in (None, "missing"):
                    worker_present_count += 1
    return worker_running_count, worker_present_count


async def load_exact_operational_absence(
    outer_runs: list[Mapping[str, Any]],
    event_rows: list[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Check exact Redis and worker identities without scanning or mutation."""

    events = event_rows or []
    run_rows = _operational_run_rows(outer_runs, events)
    job_ids = _operational_job_ids(run_rows, events)
    if os.getenv("HLTHPRT_WORKER_LAUNCHER", "process").strip().lower() != (
        "kubernetes"
    ):
        return {
            "contract": "ptg_source_attempt_external_absence_v1",
            "job_identity_count": len(job_ids),
            "queue_count": len(_PTG_QUEUES),
            "queue_memberships": None,
            "redis_exact_key_count": None,
            "worker_running_count": None,
            "worker_present_count": None,
            "worker_spec_count": len(_PTG_WORKER_CLASSES),
            "worker_probe_supported": False,
            "exact_external_absence": False,
        }
    queue_memberships, redis_key_count = await _redis_operational_counts(
        job_ids
    )
    worker_running_count, worker_present_count = (
        await _worker_operational_counts(run_rows)
    )
    has_exact_absence = (
        queue_memberships == 0
        and redis_key_count == 0
        and worker_running_count == 0
        and worker_present_count == 0
    )
    return {
        "contract": "ptg_source_attempt_external_absence_v1",
        "job_identity_count": len(job_ids),
        "queue_count": len(_PTG_QUEUES),
        "queue_memberships": queue_memberships,
        "redis_exact_key_count": redis_key_count,
        "worker_running_count": worker_running_count,
        "worker_present_count": worker_present_count,
        "worker_spec_count": len(_PTG_WORKER_CLASSES),
        "worker_probe_supported": True,
        "exact_external_absence": has_exact_absence,
    }


__all__ = ["load_exact_operational_absence"]
