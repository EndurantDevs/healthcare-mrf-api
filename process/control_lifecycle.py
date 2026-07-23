# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
import asyncio
import hashlib
import logging
import os
import uuid
from contextlib import suppress
from functools import lru_cache
from inspect import signature
from importlib import import_module
from typing import Any

import redis
from sqlalchemy import and_, func, or_, update

from db.models import ImportRun, db
from process.control_cancel import ImportCancelledError
from process.import_status_events import (
    bind_status_event_loop,
    flush_status_events,
    isoformat_utc,
)
from process.live_progress import (
    current_live_progress_context,
    enqueue_live_progress,
    progress_payload_from_live,
    read_live_progress,
    reset_live_progress_context,
    set_live_progress_context,
    write_live_progress,
)
from process.redis_config import build_redis_settings


_TERMINAL_STATUSES = {"succeeded", "failed", "canceled", "cancelled", "dead_letter"}
_CONTROL_RUN_MARKED = True
_CONTROL_RUN_NOT_MARKED = False
logger = logging.getLogger(__name__)


async def control_single_job_start(
    ctx: dict[str, Any],
    task: dict[str, Any] | None = None,
    **_arq_metadata: Any,
) -> dict[str, Any]:
    """Run a single-job importer while updating the unified import_run registry."""
    payload = task if isinstance(task, dict) else {}
    bind_status_event_loop()
    run_id = str(payload.get("run_id") or "").strip()
    importer = str(payload.get("importer") or payload.get("target_function") or "unknown").strip()
    target_module = str(payload.get("target_module") or "").strip()
    target_function = str(payload.get("target_function") or "").strip()
    call_style = str(payload.get("call_style") or "ctx_task").strip()
    run_shutdown = bool(payload.get("run_shutdown"))
    target_task = payload.get("task") if isinstance(payload.get("task"), dict) else {}
    ctx = _isolated_control_job_context(ctx, run_id)

    if run_id:
        target_task = {**target_task, "run_id": run_id}

    started_at = dt.datetime.now(dt.UTC).isoformat(timespec="microseconds")
    attempt_id = f"{run_id}:{uuid.uuid4().hex}" if run_id else None
    live_token = None
    heartbeat_task = None
    if run_id:
        live_token = set_live_progress_context(
            run_id=run_id,
            importer=importer,
            status="running",
            started_at=started_at,
            attempt_id=attempt_id,
            attempt_started_at=started_at,
        )

    try:
        attempt_claimed = await mark_control_run(
            run_id,
            status="running",
            phase_detail=f"{target_function} running",
            progress_message="running",
            attempt_id=attempt_id,
            attempt_started_at=started_at if run_id else None,
        )
    except BaseException:
        if live_token is not None:
            reset_live_progress_context(live_token)
        raise
    if run_id and attempt_claimed is not True:
        if live_token is not None:
            reset_live_progress_context(live_token)
        return {
            "status": "skipped",
            "run_id": run_id,
            "reason": "newer_attempt_active",
        }
    if not target_module or not target_function:
        try:
            await mark_control_run(
                run_id,
                status="failed",
                phase_detail="control wrapper target missing",
                progress_message="target missing",
                error={
                    "code": "control_target_missing",
                    "message": (
                        "target_module and target_function are required"
                    ),
                },
                attempt_id=attempt_id,
                attempt_started_at=started_at if run_id else None,
            )
            await _flush_terminal_status_events()
        finally:
            if live_token is not None:
                reset_live_progress_context(live_token)
        raise RuntimeError("target_module and target_function are required")
    if run_id:
        heartbeat_task = asyncio.create_task(
            _live_progress_heartbeat(
                run_id,
                importer,
                target_function,
                started_at,
                attempt_id=attempt_id,
                attempt_started_at=started_at,
            )
        )
    try:
        module = import_module(target_module)
        target_result = await _invoke_control_target(
            module,
            target_function=target_function,
            call_style=call_style,
            control_context=ctx,
            target_task_by_field=target_task,
        )
        if run_shutdown:
            shutdown = getattr(module, "shutdown", None)
            if shutdown is None:
                raise RuntimeError(f"{target_module} does not expose shutdown(ctx)")
            await shutdown(ctx)
            ctx.setdefault("context", {})["run"] = 0
    except ImportCancelledError:
        await mark_control_run(
            run_id,
            status="canceled",
            phase_detail=f"{target_function} canceled",
            progress_message="canceled",
            attempt_id=attempt_id,
            attempt_started_at=started_at if run_id else None,
        )
        await _flush_terminal_status_events()
        return {"status": "canceled", "run_id": run_id}
    except asyncio.CancelledError as exc:
        await mark_control_run(
            run_id,
            status="failed",
            phase_detail=f"{target_function} interrupted",
            progress_message="interrupted",
            error={"code": "import_interrupted", "message": "worker task was cancelled"},
            attempt_id=attempt_id,
            attempt_started_at=started_at if run_id else None,
        )
        await _flush_terminal_status_events()
        return {"status": "failed", "run_id": run_id, "error": str(exc)}
    except Exception as exc:
        await mark_control_run(
            run_id,
            status="failed",
            phase_detail=f"{target_function} failed",
            progress_message="failed",
            error=_control_failure_error(exc), metrics=_terminal_metrics_from_context(ctx.get("context")),
            attempt_id=attempt_id,
            attempt_started_at=started_at if run_id else None,
        )
        await _flush_terminal_status_events()
        raise
    finally:
        await _stop_live_progress_heartbeat(heartbeat_task)
        if live_token is not None:
            reset_live_progress_context(live_token)
    terminal_metrics = _terminal_metrics_from_result(
        target_result,
        context=ctx.get("context") if run_shutdown else None,
    )
    terminal_progress = _terminal_progress_from_result(
        target_function,
        terminal_metrics or target_result,
    )
    preserve_finished_at = bool(
        run_shutdown
        and isinstance(ctx.get("context"), dict)
        and ctx["context"].get("preserve_control_run_finished_at")
    )
    await mark_control_run(
        run_id,
        status="succeeded",
        phase_detail=f"{target_function} succeeded",
        progress_message="succeeded",
        metrics=terminal_metrics,
        progress=terminal_progress,
        preserve_finished_at=preserve_finished_at,
        attempt_id=attempt_id,
        attempt_started_at=started_at if run_id else None,
    )
    await _flush_terminal_status_events()
    return {
        "status": "succeeded",
        "run_id": run_id,
        "result": target_result,
    }


async def _invoke_control_target(
    module: Any,
    *,
    target_function: str,
    call_style: str,
    control_context: dict[str, Any],
    target_task_by_field: dict[str, Any],
) -> Any:
    """Invoke one configured importer using its declared calling convention."""

    target_callable = getattr(module, target_function)
    if call_style != "kwargs":
        return await target_callable(control_context, target_task_by_field)
    accepted_parameters = signature(target_callable).parameters
    accepted_kwargs_by_name = {
        field_name: field_value
        for field_name, field_value in target_task_by_field.items()
        if field_name in accepted_parameters
    }
    return await target_callable(**accepted_kwargs_by_name)


async def _live_progress_heartbeat(
    run_id: str,
    importer: str,
    target_function: str,
    started_at: str,
    *,
    attempt_id: str | None = None,
    attempt_started_at: str | None = None,
) -> None:
    interval = float(os.getenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "15"))
    if interval <= 0:
        return
    phase = f"{target_function} running"
    while True:
        await asyncio.sleep(interval)
        has_heartbeat_ownership = False
        try:
            has_heartbeat_ownership = (
                await _is_control_run_heartbeat_persisted(
                    run_id,
                    target_function,
                    attempt_id=attempt_id,
                    attempt_started_at=attempt_started_at,
                )
            )
        except Exception:
            logger.debug("Failed to persist live import heartbeat for run %s", run_id, exc_info=True)
        if not has_heartbeat_ownership:
            continue
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
            attempt_id=attempt_id,
            attempt_started_at=attempt_started_at,
            source="engine-heartbeat",
            confidence="heartbeat",
        )


async def _is_control_run_heartbeat_persisted(
    run_id: str,
    target_function: str,
    *,
    attempt_id: str | None = None,
    attempt_started_at: str | None = None,
) -> bool:
    live = read_live_progress(run_id)
    if attempt_id and attempt_started_at and not _is_progress_from_attempt(
        live,
        attempt_id=attempt_id,
        attempt_started_at=attempt_started_at,
    ):
        live = None
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    update_values_by_field = _control_run_heartbeat_update_values(
        target_function,
        live,
        now,
        attempt_id=attempt_id,
        attempt_started_at=attempt_started_at,
    )
    blocked_statuses = sorted(_TERMINAL_STATUSES | {"canceling"})
    stmt = (
        update(ImportRun)
        .where(ImportRun.run_id == run_id)
        .where(ImportRun.status.notin_(blocked_statuses))
    )
    if attempt_id and attempt_started_at:
        stmt = _where_control_attempt(
            stmt,
            attempt_id=attempt_id,
            attempt_started_at=attempt_started_at,
        )
    affected_rows = await _execute_control_run_update(
        stmt.values(**update_values_by_field).returning(ImportRun.run_id)
    )
    return affected_rows == 1


_persist_control_run_heartbeat = _is_control_run_heartbeat_persisted


def _control_run_heartbeat_update_values(
    target_function: str,
    live: dict[str, Any] | None,
    now: dt.datetime,
    *,
    attempt_id: str | None = None,
    attempt_started_at: str | None = None,
) -> dict[str, Any]:
    fallback_phase = f"{target_function} running"
    if live:
        progress_payload = progress_payload_from_live(live) or {
            "unit": "run",
            "done": 0,
            "total": 1,
            "pct": 0,
            "message": "running",
            "phase": fallback_phase,
        }
        phase_detail = str(live.get("phase") or progress_payload.get("phase") or fallback_phase)[:128]
    else:
        phase_detail = fallback_phase
        progress_payload = {
            "unit": "run",
            "done": 0,
            "total": 1,
            "pct": 0,
            "message": "running",
            "phase": fallback_phase,
        }
    progress_payload = dict(progress_payload)
    if attempt_id and attempt_started_at:
        progress_payload["attempt_id"] = attempt_id
        progress_payload["attempt_started_at"] = attempt_started_at
    return {
        "status": "running",
        "phase_detail": phase_detail,
        "heartbeat_at": now,
        "progress": progress_payload,
        "finished_at": None,
    }


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


def _terminal_metrics_from_result(result: Any, *, context: Any = None) -> dict[str, Any] | None:
    metrics = dict(result) if isinstance(result, dict) else (
        {"result": result} if isinstance(result, (int, float, str, bool)) else None
    )
    context_metrics = _terminal_metrics_from_context(context)
    if metrics is None:
        return context_metrics
    if context_metrics:
        return {**context_metrics, **metrics}
    return metrics


def _terminal_metrics_from_context(context: Any) -> dict[str, Any] | None:
    if not isinstance(context, dict):
        return None
    keys = (
        "staged_rows",
        "rows",
        "npi_rows",
        "inferred_rows",
        "multi_source_rows",
        "support_counts",
        "refresh_mode",
        "serving_only_refresh",
        "support_stage_skipped",
        "inline_source_evidence",
        "split_array_aggregates",
        "source_table_shards",
        "source_select_count",
        "source_concurrency",
        "raw_location_key_index_skipped",
        "enrich_shards",
        "enrich_concurrency",
        "aggregate_shards",
        "aggregate_concurrency",
        "stage_index_concurrency",
        "stage_index_profile",
        "post_publish_index_profile",
        "post_publish_index_concurrency",
        "post_publish_index_concurrently",
        "post_publish_index_pending",
        "post_publish_index_total",
        "post_publish_index_completed",
        "post_publish_index_timings",
        "post_publish_index_error",
        "post_publish_skipped_indexes",
        "raw_group_index_profile",
        "stage_index_timings",
        "aggregate_source_record_ids",
        "unlogged_stage",
        "published_elapsed_seconds",
        "publish_validation",
        "phase_timings",
        "audit", "skipped_stage_indexes",
    )
    metrics = {key: context[key] for key in keys if key in context}
    staged_rows = metrics.get("staged_rows")
    if "rows" not in metrics and isinstance(staged_rows, int):
        metrics["rows"] = staged_rows
    return metrics or None


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
    preserve_finished_at: bool = False,
    attempt_id: str | None = None,
    attempt_started_at: str | None = None,
):
    """Persist and publish one authoritative control-run lifecycle transition."""

    if not run_id:
        return _CONTROL_RUN_NOT_MARKED
    attempt_id, attempt_started_at = _control_attempt_for_run(
        run_id,
        attempt_id=attempt_id,
        attempt_started_at=attempt_started_at,
    )
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    is_terminal = status in {
        "succeeded",
        "failed",
        "canceled",
        "cancelled",
        "dead_letter",
    }
    if progress is not None:
        progress_by_field = progress
    elif status == "succeeded":
        progress_by_field = {
            "unit": "run",
            "total": 1,
            "done": 1,
            "pct": 100,
            "message": progress_message,
        }
    elif is_terminal:
        live_progress_by_field = read_live_progress(run_id) or {}
        if (
            attempt_id
            and attempt_started_at
            and not _is_progress_from_attempt(
                live_progress_by_field,
                attempt_id=attempt_id,
                attempt_started_at=attempt_started_at,
            )
        ):
            live_progress_by_field = {}
        progress_by_field = progress_payload_from_live(
            live_progress_by_field
        ) or {
            "unit": "run",
            "total": 1,
            "done": 0,
            "pct": 0,
        }
        progress_by_field["message"] = progress_message
    else:
        progress_by_field = {
            "unit": "run",
            "total": 1,
            "done": 0,
            "pct": 0,
            "message": progress_message,
        }
    progress_by_field = dict(progress_by_field)
    if attempt_id and attempt_started_at:
        progress_by_field["attempt_id"] = attempt_id
        progress_by_field["attempt_started_at"] = attempt_started_at
    live_started_at = (attempt_started_at or now) if status == "running" else None
    live_finished_at = now if is_terminal and not preserve_finished_at else None
    values: dict[str, Any] = {
        "status": status,
        "phase_detail": phase_detail,
        "heartbeat_at": now,
        "progress": progress_by_field,
        "error": error,
    }
    if metrics is not None:
        values["metrics"] = metrics
    if snapshot_id:
        values["snapshot_id"] = snapshot_id
    if status == "running":
        values["started_at"] = func.coalesce(ImportRun.started_at, now)
        values["finished_at"] = None
    if is_terminal and not preserve_finished_at:
        values["finished_at"] = now
    should_update_database = bool(
        status == "running" and attempt_id and attempt_started_at
    ) or await _should_update_control_run_db(
        run_id=run_id,
        status=status,
        phase_detail=phase_detail,
        error=error,
        snapshot_id=snapshot_id,
    )
    if should_update_database:
        stmt = update(ImportRun).where(ImportRun.run_id == run_id)
        if status == "running":
            stmt = stmt.where(
                ImportRun.status.notin_(
                    sorted(_TERMINAL_STATUSES | {"canceling"})
                )
            )
            if not (attempt_id and attempt_started_at):
                stmt = stmt.where(
                    ImportRun.progress[
                        "attempt_started_at"
                    ].as_string().is_(None)
                )
        elif status in {"running", "succeeded", "failed", "dead_letter"}:
            stmt = stmt.where(ImportRun.status.notin_(["canceling", "canceled"]))
        if status == "running" and attempt_id and attempt_started_at:
            stmt = _where_newer_control_attempt_claim(
                stmt,
                attempt_id=attempt_id,
                attempt_started_at=attempt_started_at,
            )
        elif is_terminal and attempt_id and attempt_started_at:
            stmt = _where_control_attempt(
                stmt,
                attempt_id=attempt_id,
                attempt_started_at=attempt_started_at,
            )
        affected_rows = await _execute_control_run_update(
            stmt.values(**values).returning(ImportRun.run_id)
        )
        if affected_rows != 1:
            return _CONTROL_RUN_NOT_MARKED
    live_payload = {
        **progress_by_field,
        "run_id": run_id,
        "status": status,
        "phase": progress_by_field.get("phase") or phase_detail,
        "message": progress_by_field.get("message") or progress_message,
        "started_at": isoformat_utc(live_started_at) if live_started_at else None,
        "finished_at": isoformat_utc(live_finished_at) if live_finished_at else None,
        "snapshot_id": snapshot_id,
        "publish_event": False,
    }
    status_event_by_field = {
        "run_id": run_id,
        "status": status,
        "phase_detail": phase_detail,
        "progress": progress_by_field,
        "metrics": metrics or {},
        "error": error,
        "snapshot_id": snapshot_id,
        "heartbeat_at": isoformat_utc(now),
        "started_at": isoformat_utc(live_started_at) if live_started_at else None,
        "finished_at": (
            isoformat_utc(live_finished_at)
            if live_finished_at
            else None
        ),
    }
    await asyncio.to_thread(
        write_live_progress,
        **live_payload,
        status_event_payload=status_event_by_field,
    )
    return _CONTROL_RUN_MARKED


def _control_attempt_for_run(
    run_id: str,
    *,
    attempt_id: str | None,
    attempt_started_at: str | None,
) -> tuple[str | None, str | None]:
    """Inherit wrapper ownership for progress emitted by its target."""

    if attempt_id or attempt_started_at:
        return attempt_id, attempt_started_at
    context = current_live_progress_context()
    if str(context.get("run_id") or "").strip() != run_id:
        return None, None
    context_attempt_id = str(context.get("attempt_id") or "").strip()
    context_started_at = str(
        context.get("attempt_started_at") or ""
    ).strip()
    if not context_attempt_id or not context_started_at:
        return None, None
    return context_attempt_id, context_started_at


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


@lru_cache(maxsize=1)
def _control_run_db_throttle_client() -> redis.Redis:
    settings = build_redis_settings()
    return redis.Redis(
        host=settings.host,
        port=settings.port,
        db=settings.database,
        password=settings.password,
        socket_connect_timeout=settings.conn_timeout,
        socket_timeout=settings.conn_timeout,
    )


def _where_control_attempt(
    stmt: Any,
    *,
    attempt_id: str,
    attempt_started_at: str,
) -> Any:
    """Fence a control-plane write to one immutable execution attempt."""

    return stmt.where(
        ImportRun.progress["attempt_id"].as_string() == attempt_id,
        ImportRun.progress["attempt_started_at"].as_string()
        == attempt_started_at,
    )


def _where_newer_control_attempt_claim(
    stmt: Any,
    *,
    attempt_id: str,
    attempt_started_at: str,
) -> Any:
    """Claim a run only when no newer immutable attempt already owns it."""

    stored_attempt_id = ImportRun.progress["attempt_id"].as_string()
    stored_started_at = ImportRun.progress[
        "attempt_started_at"
    ].as_string()
    return stmt.where(
        or_(
            stored_started_at.is_(None),
            stored_started_at < attempt_started_at,
            and_(
                stored_started_at == attempt_started_at,
                stored_attempt_id == attempt_id,
            ),
        )
    )


def _is_progress_from_attempt(
    progress: dict[str, Any] | None,
    *,
    attempt_id: str,
    attempt_started_at: str,
) -> bool:
    """Accept live detail only when the caller already owns its identity."""

    if not isinstance(progress, dict):
        return False
    return (
        str(progress.get("attempt_id") or "") == attempt_id
        and str(progress.get("attempt_started_at") or "")
        == attempt_started_at
    )


async def _execute_control_run_update(stmt: Any) -> int:
    previous_override = getattr(db, "_database_override", None)
    base_database = os.getenv("HLTHPRT_DB_DATABASE", "postgres")
    db._database_override = base_database
    try:
        await db.connect()
        result = await db.execute(stmt)
        if result is None:
            return 0
        try:
            returned_rows = result.all()
        except Exception:
            return 0
        return len(returned_rows)
    finally:
        db._database_override = previous_override
        if previous_override != base_database:
            await db.connect()


def _isolated_control_job_context(ctx: dict[str, Any], run_id: str) -> dict[str, Any]:
    """Copy mutable run state so concurrent ARQ jobs cannot overwrite each other."""
    isolated_context_map = dict(ctx)
    shared_context_map = ctx.get("context")
    job_context_map = (
        dict(shared_context_map) if isinstance(shared_context_map, dict) else {}
    )
    for key in ("audit", "finished_at", "preserve_control_run_finished_at"):
        job_context_map.pop(key, None)
    if run_id:
        isolated_context_map["control_run_id"] = run_id
        job_context_map["control_run_id"] = run_id
    else:
        isolated_context_map.pop("control_run_id", None)
        job_context_map.pop("control_run_id", None)
    isolated_context_map["context"] = job_context_map
    return isolated_context_map


def _control_failure_error(exc: Exception) -> dict[str, Any]:
    """Preserve stable retry semantics exposed by importer-specific errors."""

    error = {
        "code": str(getattr(exc, "control_error_code", "import_failed")),
        "message": str(exc).strip() or type(exc).__name__,
    }
    retryable = getattr(exc, "retryable", None)
    if isinstance(retryable, bool):
        error["retryable"] = retryable
    return error
