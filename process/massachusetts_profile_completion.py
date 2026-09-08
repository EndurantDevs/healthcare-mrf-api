# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Commit BORIM publication and its exact Import Control attempt together."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from sqlalchemy import func, select, update

from db.models import ImportRun, ProviderProfileImportRun, db
from process.control_cancel import raise_if_cancelled
from process.control_lifecycle import suppress_control_run_heartbeat_persistence
from process import massachusetts_profile_store as store

IMPORTER = "massachusetts-borim-profile"


def _attempt(ctx, task, run_row):
    """Bind a control attempt to the source run's frozen manifest, or CLI mode."""
    control_run_id = task.get("run_id")
    if control_run_id != run_row["source_manifest"].get("control_run_id"):
        raise ValueError("massachusetts_profile_control_run_mismatch")
    if control_run_id is None:
        return None
    context = ctx.get("context") or {}
    attempt_by_field = {
        "run_id": control_run_id,
        "attempt_id": context.get("_control_attempt_id"),
        "attempt_started_at": context.get("_control_attempt_started_at"),
    }
    if any(not isinstance(value, str) or not value.strip() for value in attempt_by_field.values()):
        raise ValueError("massachusetts_profile_control_attempt_missing")
    return attempt_by_field


async def _locked_control_run(attempt):
    control_table = ImportRun.__table__
    return await db.first(select(control_table).where(
        control_table.c.run_id == attempt["run_id"]).with_for_update())


def _is_attempt(control_run, attempt):
    if control_run is None or control_run["importer"] != IMPORTER:
        return False
    progress_by_field = control_run["progress"] or {}
    return all(progress_by_field.get(key) == attempt[key] for key in ("attempt_id", "attempt_started_at"))


async def _finish_source(run_row, metrics):
    if run_row["source_manifest"]["max_providers"] is not None:
        return await store.finish_unpublished_run(run_row["run_id"], metrics)
    return await store.publish_run(
        run_row["run_id"], metrics=metrics,
        expected_current_run_id=run_row["source_manifest"]["expected_current_run_id"],
    )


def _terminal_progress(result):
    completed = result["requested_licenses"]
    phase = "published" if result["published"] else "test completed without publication"
    return {"unit": "license", "done": completed, "total": completed, "pct": 100,
            "phase": f"{IMPORTER} {phase}", "message": "succeeded"}


async def _commit_control(attempt, result):
    """Fence the success update again and return database-authored timestamps."""
    control_table = ImportRun.__table__
    progress_by_field = {**_terminal_progress(result), **{key: attempt[key] for key in ("attempt_id", "attempt_started_at")}}
    committed = await db.first(update(control_table).where(
        control_table.c.run_id == attempt["run_id"], control_table.c.importer == IMPORTER,
        control_table.c.status == "running",
        control_table.c.progress["attempt_id"].as_string() == attempt["attempt_id"],
        control_table.c.progress["attempt_started_at"].as_string() == attempt["attempt_started_at"],
    ).values(status="succeeded", phase_detail=progress_by_field["phase"], error=None,
             heartbeat_at=func.timezone("UTC", func.transaction_timestamp()),
             finished_at=func.timezone("UTC", func.transaction_timestamp()),
             progress=progress_by_field, metrics=result).returning(control_table))
    if committed is None:
        raise RuntimeError("massachusetts_profile_control_attempt_changed")
    return dict(committed._mapping)


async def _reconcile_commit(attempt, run_row, result):
    """Wait for an uncertain commit on a fresh task-owned database transaction."""
    try:
        async with db.transaction():
            await db.status("SET LOCAL lock_timeout='30s'")
            await db.status("SET LOCAL statement_timeout='35s'")
            control_row = await _locked_control_run(attempt)
            control_run = dict(control_row._mapping) if control_row is not None else None
            if (not _is_attempt(control_run, attempt) or control_run["status"] != "succeeded"
                    or control_run["metrics"] != result
                    or control_run["progress"] != {**_terminal_progress(result), **{
                        key: attempt[key] for key in ("attempt_id", "attempt_started_at")}}
                    or control_run["phase_detail"] != _terminal_progress(result)["phase"]):
                return None
            source_table = ProviderProfileImportRun.__table__
            source_row = await db.first(select(source_table).where(source_table.c.run_id == run_row["run_id"]))
            source_metrics_by_field = {key: value for key, value in result.items() if key not in {"run_id", "previous_run_id"}}
            if (source_row is None or source_row.status != "completed"
                    or source_row.source_key != store.SOURCE_KEY or source_row.schema_version != store.SCHEMA_VERSION
                    or source_row.source_manifest != run_row["source_manifest"] or source_row.metrics != source_metrics_by_field):
                return None
            return control_run
    except Exception:
        return None


async def _shielded_reconciliation(attempt, run_row, result):
    reconciliation = asyncio.create_task(_reconcile_commit(attempt, run_row, result))
    while True:
        try:
            return await asyncio.shield(reconciliation)
        except asyncio.CancelledError:
            if reconciliation.cancelled():
                raise


def _install_committed_result(ctx, committed, result):
    """Install the wrapper's marker only after confirmed durable completion."""
    timestamps_by_field = {}
    for field in ("heartbeat_at", "finished_at"):
        value = committed[field]
        if not isinstance(value, datetime):
            raise RuntimeError("massachusetts_profile_control_timestamp_invalid")
        timestamps_by_field[field] = value.replace(tzinfo=timezone.utc).isoformat(timespec="microseconds")
    context = ctx.setdefault("context", {})
    context["preserve_control_run_finished_at"] = True
    context["_control_committed_heartbeat_at"] = timestamps_by_field["heartbeat_at"]
    context["_control_committed_finished_at"] = timestamps_by_field["finished_at"]
    context["_control_committed_result"] = {**result, "terminal_progress": _terminal_progress(result)}
    context["control_run_terminal_committed"] = True
    current_task = asyncio.current_task()
    while current_task is not None and current_task.cancelling():
        current_task.uncancel()
    return context["_control_committed_result"]


async def complete_run(ctx, task, run_row, metrics):
    """Finish the source and control attempt atomically, including bounded runs."""
    attempt = _attempt(ctx, task, run_row)
    if attempt is None:
        result = await _finish_source(run_row, metrics)
        return {**result, "terminal_progress": _terminal_progress(result)}
    result = committed = None
    try:
        async with suppress_control_run_heartbeat_persistence(attempt["run_id"]), db.transaction():
            control_row = await _locked_control_run(attempt)
            control_run = dict(control_row._mapping) if control_row is not None else None
            if not _is_attempt(control_run, attempt) or control_run["status"] != "running":
                raise RuntimeError("massachusetts_profile_control_attempt_changed")
            await raise_if_cancelled(ctx, task)
            result = await _finish_source(run_row, metrics)
            committed = await _commit_control(attempt, result)
    except BaseException:
        if committed is None:
            raise
        committed = await _shielded_reconciliation(attempt, run_row, result)
        if committed is None:
            raise
    return _install_committed_result(ctx, committed, result)
