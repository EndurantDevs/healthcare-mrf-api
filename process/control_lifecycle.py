# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
import asyncio
import hashlib
import os
from contextlib import suppress
from inspect import signature
from importlib import import_module
from typing import Any

import redis
from sqlalchemy import func, update

from db.models import ImportRun, db
from process.control_cancel import ImportCancelledError
from process.import_status_events import enqueue_status_event, flush_status_events, isoformat_utc
from process.live_progress import enqueue_live_progress, reset_live_progress_context, set_live_progress_context
from process.redis_config import build_redis_settings


_TERMINAL_STATUSES = {"succeeded", "failed", "canceled", "cancelled", "dead_letter"}
_control_run_db_throttle_redis: redis.Redis | None = None


async def control_single_job_start(
    ctx: dict[str, Any],
    task: dict[str, Any] | None = None,
    **_arq_metadata: Any,
) -> dict[str, Any]:
    """Run a single-job importer while updating the unified import_run registry."""
    payload = task if isinstance(task, dict) else {}
    run_id = str(payload.get("run_id") or "").strip()
    importer = str(payload.get("importer") or payload.get("target_function") or "unknown").strip()
    target_module = str(payload.get("target_module") or "").strip()
    target_function = str(payload.get("target_function") or "").strip()
    call_style = str(payload.get("call_style") or "ctx_task").strip()
    run_shutdown = bool(payload.get("run_shutdown"))
    target_task = payload.get("task") if isinstance(payload.get("task"), dict) else {}
    if run_id:
        target_task = {**target_task, "run_id": run_id}
        ctx["control_run_id"] = run_id
        ctx.setdefault("context", {})["control_run_id"] = run_id
    if not target_module or not target_function:
        await mark_control_run(
            run_id,
            status="failed",
            phase_detail="control wrapper target missing",
            progress_message="target missing",
            error={"code": "control_target_missing", "message": "target_module and target_function are required"},
        )
        raise RuntimeError("target_module and target_function are required")

    started_at = dt.datetime.now(dt.UTC).isoformat()
    live_token = None
    heartbeat_task = None
    if run_id:
        live_token = set_live_progress_context(
            run_id=run_id,
            importer=importer,
            status="running",
            started_at=started_at,
            source="import-control-heartbeat",
            confidence="heartbeat",
        )
        heartbeat_task = asyncio.create_task(_live_progress_heartbeat(run_id, importer, target_function, started_at))

    await mark_control_run(run_id, status="running", phase_detail=f"{target_function} running", progress_message="running")
    try:
        module = import_module(target_module)
        fn = getattr(module, target_function)
        if call_style == "kwargs":
            accepted = signature(fn).parameters
            kwargs = {key: value for key, value in target_task.items() if key in accepted}
            result = await fn(**kwargs)
        else:
            result = await fn(ctx, target_task)
        if run_shutdown:
            shutdown = getattr(module, "shutdown", None)
            if shutdown is None:
                raise RuntimeError(f"{target_module} does not expose shutdown(ctx)")
            await shutdown(ctx)
            ctx.setdefault("context", {})["run"] = 0
    except ImportCancelledError:
        await mark_control_run(run_id, status="canceled", phase_detail=f"{target_function} canceled", progress_message="canceled")
        await _flush_terminal_status_events()
        return {"status": "canceled", "run_id": run_id}
    except asyncio.CancelledError as exc:
        await mark_control_run(
            run_id,
            status="failed",
            phase_detail=f"{target_function} interrupted",
            progress_message="interrupted",
            error={"code": "import_interrupted", "message": "worker task was cancelled"},
        )
        await _flush_terminal_status_events()
        return {"status": "failed", "run_id": run_id, "error": str(exc)}
    except Exception as exc:
        await mark_control_run(
            run_id,
            status="failed",
            phase_detail=f"{target_function} failed",
            progress_message="failed",
            error={"code": "import_failed", "message": str(exc)},
        )
        await _flush_terminal_status_events()
        raise
    finally:
        await _stop_live_progress_heartbeat(heartbeat_task)
        if live_token is not None:
            reset_live_progress_context(live_token)
    terminal_progress = _terminal_progress_from_result(target_function, result)
    terminal_metrics = result if isinstance(result, dict) else ({"result": result} if isinstance(result, (int, float, str, bool)) else None)
    await mark_control_run(
        run_id,
        status="succeeded",
        phase_detail=f"{target_function} succeeded",
        progress_message="succeeded",
        metrics=terminal_metrics,
        progress=terminal_progress,
    )
    await _flush_terminal_status_events()
    return {"status": "succeeded", "run_id": run_id, "result": result}


async def _live_progress_heartbeat(run_id: str, importer: str, target_function: str, started_at: str) -> None:
    interval = float(os.getenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "15"))
    if interval <= 0:
        return
    phase = f"{target_function} running"
    while True:
        await asyncio.sleep(interval)
        enqueue_live_progress(
            run_id=run_id,
            importer=importer,
            status="running",
            phase=phase,
            unit="run",
            done=0,
            total=1,
            pct=0,
            message="running",
            started_at=started_at,
            source="import-control-heartbeat",
            confidence="heartbeat",
        )


async def _stop_live_progress_heartbeat(task: asyncio.Task | None) -> None:
    if task is None or task.done():
        return
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


def _terminal_progress_from_result(target_function: str, result: Any) -> dict[str, Any] | None:
    if isinstance(result, int):
        return {
            "unit": "items",
            "done": result,
            "total": result,
            "pct": 100,
            "message": "succeeded",
            "phase": f"{target_function} succeeded",
        }
    if not isinstance(result, dict):
        return None
    count_keys = (
        "rows",
        "row_count",
        "rows_imported",
        "count",
        "processed",
        "code_rows",
        "synonym_rows",
        "relationship_rows",
        "clinical_area_rows",
    )
    count_value = next(
        (value for key in count_keys if isinstance((value := result.get(key)), int) and value >= 0),
        None,
    )
    if count_value is None:
        return None
    return {
        "unit": "items",
        "done": count_value,
        "total": count_value,
        "pct": 100,
        "message": "succeeded",
        "phase": f"{target_function} succeeded",
    }


async def _flush_terminal_status_events() -> None:
    timeout = float(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_TERMINAL_FLUSH_SECONDS", "0.25"))
    if timeout <= 0:
        return
    await flush_status_events(timeout_seconds=timeout)


async def mark_control_run(
    run_id: str,
    *,
    status: str,
    phase_detail: str,
    progress_message: str,
    error: dict[str, Any] | None = None,
    metrics: dict[str, Any] | None = None,
    progress: dict[str, Any] | None = None,
    snapshot_id: str | None = None,
) -> None:
    if not run_id:
        return
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    done = 1 if status in {"succeeded", "failed", "canceled", "dead_letter"} else 0
    progress_payload = progress or {
        "unit": "run",
        "total": 1,
        "done": done,
        "pct": 100 if done else 0,
        "message": progress_message,
    }
    live_started_at = now if status == "running" else None
    live_finished_at = now if done else None
    values: dict[str, Any] = {
        "status": status,
        "phase_detail": phase_detail,
        "heartbeat_at": now,
        "progress": progress_payload,
        "error": error,
    }
    if metrics is not None:
        values["metrics"] = metrics
    if snapshot_id:
        values["snapshot_id"] = snapshot_id
    if status == "running":
        values["started_at"] = func.coalesce(ImportRun.started_at, now)
        values["finished_at"] = None
    if done:
        values["finished_at"] = now
    if await _should_update_control_run_db(
        run_id=run_id,
        status=status,
        phase_detail=phase_detail,
        error=error,
        snapshot_id=snapshot_id,
    ):
        stmt = update(ImportRun).where(ImportRun.run_id == run_id)
        if status in {"running", "succeeded", "failed", "dead_letter"}:
            stmt = stmt.where(ImportRun.status.notin_(["canceling", "canceled"]))
        await _execute_control_run_update(stmt.values(**values))
    live_payload = {
        **progress_payload,
        "run_id": run_id,
        "status": status,
        "phase": progress_payload.get("phase") or phase_detail,
        "message": progress_payload.get("message") or progress_message,
        "started_at": isoformat_utc(live_started_at) if live_started_at else None,
        "finished_at": isoformat_utc(live_finished_at) if live_finished_at else None,
        "snapshot_id": snapshot_id,
        "publish_event": False,
    }
    enqueue_live_progress(**live_payload)
    enqueue_status_event(
        {
            "run_id": run_id,
            "status": status,
            "phase_detail": phase_detail,
            "progress": progress_payload,
            "metrics": metrics or {},
            "error": error,
            "snapshot_id": snapshot_id,
            "heartbeat_at": isoformat_utc(now),
            "started_at": isoformat_utc(live_started_at) if live_started_at else None,
            "finished_at": isoformat_utc(live_finished_at) if live_finished_at else None,
        }
    )


async def _should_update_control_run_db(
    *,
    run_id: str,
    status: str,
    phase_detail: str,
    error: dict[str, Any] | None,
    snapshot_id: str | None,
) -> bool:
    status_key = str(status or "").strip().lower()
    if status_key != "running" or status_key in _TERMINAL_STATUSES or error or snapshot_id:
        return True
    throttle_seconds = _control_run_db_update_throttle_seconds()
    if throttle_seconds <= 0:
        return True
    slot_key = _control_run_db_update_slot_key(run_id=run_id, status=status_key, phase_detail=phase_detail)
    return await asyncio.to_thread(_claim_control_run_db_update_slot, slot_key, throttle_seconds)


def _control_run_db_update_throttle_seconds() -> float:
    raw = os.getenv("HLTHPRT_CONTROL_RUN_DB_UPDATE_THROTTLE_SECONDS", "30")
    try:
        return max(float(raw), 0.0)
    except (TypeError, ValueError):
        return 30.0


def _control_run_db_update_slot_key(*, run_id: str, status: str, phase_detail: str) -> str:
    digest = hashlib.sha256(f"{run_id}\0{status}\0{phase_detail}".encode("utf-8")).hexdigest()[:24]
    return f"hp:control-run-db-update:{digest}"


def _claim_control_run_db_update_slot(slot_key: str, throttle_seconds: float) -> bool:
    try:
        return bool(_control_run_db_throttle_client().set(slot_key, "1", nx=True, px=max(int(throttle_seconds * 1000), 1)))
    except Exception:
        return True


def _control_run_db_throttle_client() -> redis.Redis:
    global _control_run_db_throttle_redis  # pylint: disable=global-statement
    if _control_run_db_throttle_redis is None:
        settings = build_redis_settings()
        _control_run_db_throttle_redis = redis.Redis(
            host=settings.host,
            port=settings.port,
            db=settings.database,
            password=settings.password,
            socket_connect_timeout=settings.conn_timeout,
            socket_timeout=settings.conn_timeout,
        )
    return _control_run_db_throttle_redis


async def _execute_control_run_update(stmt: Any) -> None:
    previous_override = getattr(db, "_database_override", None)
    base_database = os.getenv("HLTHPRT_DB_DATABASE", "postgres")
    db._database_override = base_database  # type: ignore[attr-defined]
    try:
        await db.connect()
        await db.execute(stmt)
    finally:
        db._database_override = previous_override  # type: ignore[attr-defined]
        if previous_override != base_database:
            await db.connect()
