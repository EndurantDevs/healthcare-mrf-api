# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from typing import Any


class ImportCancelledError(RuntimeError):
    """Raised when the control-plane cancel flag is set for a run."""


def run_id_from_task(task: dict[str, Any] | None) -> str | None:
    """Extract a normalized control run identifier from a task."""
    if not isinstance(task, dict):
        return None
    run_id = str(task.get("run_id") or "").strip()
    return run_id or None


async def raise_if_cancelled(ctx: dict[str, Any], task: dict[str, Any] | None) -> None:
    """Raise the control-plane cancellation signal when requested."""
    run_id = run_id_from_task(task)
    if not run_id:
        return
    redis = ctx.get("redis")
    if redis is None:
        return
    value = await redis.get(f"cancel:{run_id}")
    if value in {b"1", "1", 1, True}:
        raise ImportCancelledError(f"import run {run_id} was cancelled")


def _cancel_progress_by_name(
    *,
    canceled_now: bool,
    current_progress: dict[str, Any],
) -> dict[str, Any]:
    """Return terminal or in-progress cancellation progress."""

    cancel_progress_by_name = {
        "unit": "run",
        "total": 1,
        "done": 1 if canceled_now else 0,
        "pct": 100 if canceled_now else current_progress.get("pct", 0),
        "message": "canceled" if canceled_now else "cancel requested",
    }
    attempt_pair = _cancel_attempt_pair(current_progress)
    if attempt_pair is not None:
        attempt_id, attempt_started_at = attempt_pair
        cancel_progress_by_name["attempt_id"] = attempt_id
        cancel_progress_by_name["attempt_started_at"] = attempt_started_at
    return cancel_progress_by_name


def _cancel_attempt_pair(
    progress_by_name: dict[str, Any],
) -> tuple[str, str] | None:
    """Return a complete exact-attempt fence or no attempt fence."""

    attempt_id = progress_by_name.get("attempt_id")
    attempt_started_at = progress_by_name.get("attempt_started_at")
    if (
        type(attempt_id) is str
        and bool(attempt_id)
        and attempt_id == attempt_id.strip()
        and type(attempt_started_at) is str
        and bool(attempt_started_at)
        and attempt_started_at == attempt_started_at.strip()
    ):
        return attempt_id, attempt_started_at
    return None


def cancel_attempt_statement(statement, current_run, progress):
    """Preserve an exact cancel attempt and reject a stale unclaimed CodeSets read."""
    from db.models import ImportRun

    attempt_pair = _cancel_attempt_pair(progress)
    if attempt_pair is not None:
        return statement.where(
            ImportRun.progress["attempt_id"].as_string() == attempt_pair[0],
            ImportRun.progress["attempt_started_at"].as_string() == attempt_pair[1],
        )
    if current_run.get("importer") == "code-sets":
        return statement.where(
            ImportRun.status == current_run["status"],
            ImportRun.progress["attempt_id"].as_string().is_(None),
            ImportRun.progress["attempt_started_at"].as_string().is_(None),
        )
    return statement


def _cancel_state_by_name(
    *,
    canceled_before_start: bool,
    has_terminalized_active_worker: bool,
    current_progress: dict[str, Any],
) -> dict[str, Any]:
    """Build persisted status and progress for a cancellation request."""

    canceled_now = canceled_before_start or has_terminalized_active_worker
    phase_detail = "cancel requested"
    if has_terminalized_active_worker:
        phase_detail = "canceled active worker"
    elif canceled_before_start:
        phase_detail = "canceled before start"
    return {
        "canceled_now": canceled_now,
        "status": "canceled" if canceled_now else "canceling",
        "phase_detail": phase_detail,
        "progress": _cancel_progress_by_name(
            canceled_now=canceled_now,
            current_progress=current_progress,
        ),
    }


def is_queued_code_sets_cancel(current):
    """Recognize an unclaimed cancellation that can resume after a signal error."""
    progress = current.get("progress") if isinstance(current.get("progress"), dict) else {}
    return current.get("importer") == "code-sets" and (
        current["status"] == "queued"
        or current["status"] == "canceling"
        and all(progress.get(key) is None for key in ("attempt_id", "attempt_started_at"))
    )


async def is_queued_code_sets_cancel_reserved(database, current, requested_at):
    """Commit the observed queued intent before any external worker signal."""
    import asyncio

    from sqlalchemy import text, update

    from db.models import ImportRun
    from process.control_lifecycle import _where_no_profile_handoff

    if database._transaction_binding() is not None or database._reader_binding() is not None:
        raise RuntimeError("CodeSets cancellation requires its own commit boundary")
    if not is_queued_code_sets_cancel(current):
        raise RuntimeError("CodeSets cancellation requires its observed queued intent")
    progress = current.get("progress") if isinstance(current.get("progress"), dict) else {}
    statement = _where_no_profile_handoff(
        update(ImportRun)
        .where(
            ImportRun.run_id == current["run_id"],
            ImportRun.importer == "code-sets",
            ImportRun.status == current["status"],
            ImportRun.finished_at.is_(None),
        )
        .values(
            status="canceling",
            phase_detail="cancel requested",
            heartbeat_at=requested_at,
            progress=_cancel_progress_by_name(canceled_now=False, current_progress=progress),
        )
    )
    async with asyncio.timeout(8), database.transaction():
        await database.execute(text("SET LOCAL search_path TO pg_catalog,pg_temp"))
        return await database.status(cancel_attempt_statement(statement, current, progress)) == 1


async def has_committed_code_sets_cancel_reservation(database, current, queue, job_id, adapter):
    """Authenticate the persisted unclaimed cancellation and its exact recorded queue identity."""
    import asyncio

    from sqlalchemy import func, select, text

    from db.models import ImportRun
    from process.control_lifecycle import _where_no_profile_handoff

    metrics = current.get("metrics")
    progress = current.get("progress") if current.get("progress") is not None else {}
    expected_by_name = {
        "enqueue_adapter": "arq_single_job",
        "queue": queue,
        "job_id": job_id,
        "function": adapter["function"],
    }
    if (
        current.get("importer") != "code-sets"
        or current.get("engine") != "healthcare-mrf-api"
        or current.get("status") not in {"queued", "canceling"}
        or type(metrics) is not dict
        or {key: metrics.get(key) for key in expected_by_name} != expected_by_name
        or queue != adapter["queue"]
        or type(job_id) is not str
        or not job_id
        or job_id != job_id.strip()
        or type(progress) is not dict
        or any(progress.get(key) is not None for key in ("attempt_id", "attempt_started_at"))
        or database._transaction_binding() is not None
        or database._reader_binding() is not None
    ):
        return False
    statement = _where_no_profile_handoff(
        select(ImportRun.run_id)
        .where(
            ImportRun.run_id == current["run_id"],
            ImportRun.importer == "code-sets",
            ImportRun.engine == current["engine"],
            ImportRun.node_id.is_not_distinct_from(current.get("node_id")),
            ImportRun.status == "canceling",
            ImportRun.phase_detail == "cancel requested",
            ImportRun.finished_at.is_(None),
            ImportRun.heartbeat_at.is_not(None),
            func.json_typeof(ImportRun.metrics["job_id"]) == "string",
            *(ImportRun.metrics[key].as_string() == expected_value for key, expected_value in expected_by_name.items()),
            func.json_typeof(ImportRun.progress) == "object",
            ImportRun.progress["attempt_id"].as_string().is_(None),
            ImportRun.progress["attempt_started_at"].as_string().is_(None),
            ImportRun.progress["message"].as_string() == "cancel requested",
        )
        .with_for_update()
    )
    async with asyncio.timeout(8), database.transaction():
        await database.execute(text("SET LOCAL search_path TO pg_catalog,pg_temp"))
        return (await database.execute(statement)).scalar_one_or_none() == current["run_id"]
