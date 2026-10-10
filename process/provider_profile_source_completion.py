# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Commit one source publication and its exact managed control attempt together."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import import_module

from sqlalchemy import func, select, text, update

from db.models import ImportRun, ProviderProfileImportRun, db
from process.control_cancel import raise_if_cancelled
from process.control_lifecycle import (
    _SOURCE_PROFILE_IMPORTERS_BY_MODULE,
    _TERMINAL_STATUSES,
    suppress_control_run_heartbeat_persistence,
)
from process.provider_profile_source_store import ACTIVE_STATUSES, SourceProfileStore


async def source_profile_finish_startup(ctx):
    """Resume the exact local run selected by the existing finish-worker launcher."""
    from process.ext.utils import db_startup

    run_id = os.environ.get("HLTHPRT_CONTROL_RUN_ID", "")
    if not run_id or run_id != run_id.strip():
        raise ValueError("source profile finish run is required")
    await db_startup(ctx)
    return await source_profile_finish(ctx, run_id)


async def source_profile_finish(ctx, run_id):
    """Keep published control attempts retryable until worker-local maintenance settles."""
    from process import source_profile_result_archive as archive

    archive._require(run_id == os.environ.get("HLTHPRT_CONTROL_RUN_ID"), "source profile finish selection differs")
    finished = None
    try:
        async with asyncio.timeout(300), db.transaction() as session:
            await session.execute(text("SET LOCAL lock_timeout='500ms'"))
            await session.execute(text("SET LOCAL statement_timeout='60s'"))
            control_row = await db.first(
                select(ImportRun.__table__).where(ImportRun.run_id == run_id).with_for_update()
            )
            archive._require(control_row is not None, "source profile finish run is unavailable")
            control_by_field = dict(control_row._mapping)
            receipt = await archive.require_native_maintenance(session, control_by_field)
            if control_by_field["status"] == "succeeded":
                archive._require(
                    isinstance(control_by_field["metrics"].get("source_profile_maintenance"), dict),
                    "source profile maintenance outcome is unavailable",
                )
                return control_by_field["metrics"]
            outcome = await _native_source_maintenance(receipt)
            finished = await _commit_native_maintenance(control_by_field, receipt, outcome)
    except BaseException:
        if finished is None:
            raise
        readback = asyncio.create_task(_read_native_maintenance_commit(run_id, finished))
        while True:
            try:
                confirmed = await asyncio.shield(readback)
                break
            except asyncio.CancelledError:
                if readback.cancelled():
                    raise
        if confirmed is None:
            raise
    return finished


async def _native_source_maintenance(receipt):
    """Use each producer's local artifact root and its existing best-effort policy."""
    from process import source_profile_result_archive as archive

    handoff = receipt["handoff"]
    module_name = next(
        name for name, importer in _SOURCE_PROFILE_IMPORTERS_BY_MODULE.items() if importer == handoff["importer_id"]
    )
    module = import_module(module_name)
    artifact_root = module._artifact_root()
    if handoff["importer_id"] == archive.PROJECTION_IMPORTER:
        source = await db.first(
            select(ProviderProfileImportRun.__table__).where(
                ProviderProfileImportRun.run_id == handoff["source_run_id"]
            )
        )
        days = source.source_manifest["retention"]["failed_run_days"]
        archive._require(type(days) is int and days >= 0, "source profile retention policy differs")
        result = await module._apply_post_success_retention(
            run_id=handoff["source_run_id"], metrics={}, artifact_root=artifact_root, failed_retention_days=days
        )
        return result["retention"]
    try:
        return await archive._native_completion(handoff["importer_id"]).store.retain_source_history(artifact_root)
    except Exception as error:
        return {"status": "failed", "error": {"type": type(error).__name__, "message": str(error)}}


async def _commit_native_maintenance(run, receipt, outcome):
    """Only control reporting changes; published source metrics remain immutable."""
    from process import source_profile_result_archive as archive

    handoff = receipt["handoff"]
    metrics_by_field = {
        **run["metrics"],
        "source_profile_maintenance": {"handoff_sha256": handoff["handoff_sha256"], "outcome": outcome},
    }
    changed = await db.first(
        update(ImportRun.__table__)
        .where(
            ImportRun.run_id == run["run_id"],
            ImportRun.status == "finalizing",
            ImportRun.phase_detail == archive.NATIVE_MAINTENANCE_PHASE,
            ImportRun.progress["attempt_id"].as_string() == handoff["attempt_id"],
            ImportRun.progress["attempt_started_at"].as_string() == handoff["attempt_started_at"],
        )
        .values(
            status="succeeded",
            phase_detail=receipt["terminal_progress"]["phase"],
            finished_at=func.timezone("UTC", func.transaction_timestamp()),
            heartbeat_at=func.timezone("UTC", func.transaction_timestamp()),
            metrics=metrics_by_field,
            progress={**run["progress"], **receipt["terminal_progress"]},
        )
        .returning(ImportRun.run_id)
    )
    archive._require(changed is not None, "source profile maintenance attempt changed")
    return metrics_by_field


async def _read_native_maintenance_commit(run_id, expected):
    """Resolve an uncertain COMMIT on a fresh task-owned transaction without cleanup."""
    from process import source_profile_result_archive as archive

    try:
        async with asyncio.timeout(40), db.transaction() as session:
            await session.execute(text("SET LOCAL lock_timeout='30s'"))
            run = await db.first(select(ImportRun.__table__).where(ImportRun.run_id == run_id).with_for_update())
            if (
                run is not None
                and run.status == "succeeded"
                and run.finished_at is not None
                and run.metrics == expected
            ):
                await archive.require_native_maintenance(session, dict(run._mapping))
                return expected
    except Exception:
        return None
    return None


@dataclass(frozen=True)
class SourceProfileCompletion:
    """Bind managed completion to one immutable source store and importer identity."""

    store: SourceProfileStore
    importer: str

    def _attempt(self, ctx, task, run_row):
        """Bind a managed run attempt to the source run's frozen manifest."""
        control_run_id = task.get("run_id")
        if control_run_id != run_row["source_manifest"].get("control_run_id"):
            raise ValueError(f"{self.store.policy.error_prefix}_control_run_mismatch")
        context = ctx.get("context") or {}
        attempt_by_field = {
            "run_id": control_run_id,
            "attempt_id": context.get("_control_attempt_id"),
            "attempt_started_at": context.get("_control_attempt_started_at"),
        }
        if any(not isinstance(value, str) or not value.strip() for value in attempt_by_field.values()):
            raise ValueError(f"{self.store.policy.error_prefix}_control_attempt_missing")
        return attempt_by_field

    async def _locked_control_run(self, attempt):
        control_table = ImportRun.__table__
        return await db.first(
            select(control_table).where(control_table.c.run_id == attempt["run_id"]).with_for_update()
        )

    async def reconcile_failed_control_runs(self):
        """Recover managed acquisitions whose control owner has durably stopped."""
        table = ProviderProfileImportRun.__table__
        candidates = await db.all(
            select(table.c.run_id, table.c.source_manifest).where(
                table.c.source_key == self.store.policy.source_key,
                table.c.schema_version == self.store.policy.schema_version,
                table.c.status.in_(ACTIVE_STATUSES),
            )
        )
        recovered_run_ids = []
        for candidate in candidates:
            manifest = candidate.source_manifest
            control_id = manifest.get("control_run_id") if isinstance(manifest, dict) else None
            if not isinstance(control_id, str) or not control_id.strip():
                continue
            async with db.transaction():
                # Match completion's lock order; a live or ambiguous owner keeps its claim.
                owner = await self._locked_control_run({"run_id": control_id})
                if (
                    owner is None
                    or owner.importer != self.importer
                    or owner.status == "succeeded"
                    or owner.status not in _TERMINAL_STATUSES
                ):
                    continue
                await self.store._lock_source()
                source_run = await self.store._read_run(candidate.run_id)
                if (
                    source_run["status"] not in ACTIVE_STATUSES
                    or source_run["source_manifest"].get("control_run_id") != control_id
                ):
                    continue
                publication = await self.store.read_publication()
                if publication and candidate.run_id in (publication["current_run_id"], publication["previous_run_id"]):
                    raise RuntimeError(f"{self.store.policy.error_prefix}_recovery_published_run")
                await self.store.mark_run_failed(candidate.run_id, f"control owner is {owner.status}")
                recovered_run_ids.append(candidate.run_id)
        return recovered_run_ids

    def _is_attempt(self, control_run, attempt):
        if control_run is None or control_run["importer"] != self.importer:
            return False
        progress_by_field = control_run["progress"] or {}
        return all(progress_by_field.get(key) == attempt[key] for key in ("attempt_id", "attempt_started_at"))

    async def _finish_source(self, run_row, metrics):
        if run_row["source_manifest"]["max_providers"] is not None:
            return await self.store.finish_unpublished_run(run_row["run_id"], metrics)
        return await self.store.publish_run(
            run_row["run_id"],
            metrics=metrics,
            expected_current_run_id=run_row["source_manifest"]["expected_current_run_id"],
        )

    def _terminal_progress(self, result):
        completed = result["requested_licenses"]
        phase = "published" if result["published"] else "test completed without publication"
        return {
            "unit": "license",
            "done": completed,
            "total": completed,
            "pct": 100,
            "phase": f"{self.importer} {phase}",
            "message": "succeeded",
        }

    async def _commit_control(self, attempt, source_result):
        """Fence the success update again and return database-authored timestamps."""
        control_table = ImportRun.__table__
        progress_by_field = {
            **self._terminal_progress(source_result),
            **{key: attempt[key] for key in ("attempt_id", "attempt_started_at")},
        }
        committed = await db.first(
            update(control_table)
            .where(
                control_table.c.run_id == attempt["run_id"],
                control_table.c.importer == self.importer,
                control_table.c.status == "running",
                control_table.c.progress["attempt_id"].as_string() == attempt["attempt_id"],
                control_table.c.progress["attempt_started_at"].as_string() == attempt["attempt_started_at"],
            )
            .values(
                status="succeeded",
                phase_detail=progress_by_field["phase"],
                error=None,
                heartbeat_at=func.timezone("UTC", func.transaction_timestamp()),
                finished_at=func.timezone("UTC", func.transaction_timestamp()),
                progress=progress_by_field,
                metrics=source_result,
            )
            .returning(control_table)
        )
        if committed is None:
            raise RuntimeError(f"{self.store.policy.error_prefix}_control_attempt_changed")
        return dict(committed._mapping)

    async def _reconcile_commit(self, attempt, run_row, source_result):
        """Wait for an uncertain commit on a fresh task-owned database transaction."""
        try:
            async with db.transaction():
                await db.status("SET LOCAL lock_timeout='30s'")
                await db.status("SET LOCAL statement_timeout='35s'")
                control_row = await self._locked_control_run(attempt)
                control_run = dict(control_row._mapping) if control_row is not None else None
                if (
                    not self._is_attempt(control_run, attempt)
                    or control_run["status"] != "succeeded"
                    or control_run["metrics"] != source_result
                    or control_run["progress"]
                    != {
                        **self._terminal_progress(source_result),
                        **{key: attempt[key] for key in ("attempt_id", "attempt_started_at")},
                    }
                    or control_run["phase_detail"] != self._terminal_progress(source_result)["phase"]
                ):
                    return None
                source_table = ProviderProfileImportRun.__table__
                source_row = await db.first(select(source_table).where(source_table.c.run_id == run_row["run_id"]))
                source_metrics_by_field = {
                    key: metric_value
                    for key, metric_value in source_result.items()
                    if key not in {"run_id", "previous_run_id"}
                }
                if (
                    source_row is None
                    or source_row.status != "completed"
                    or source_row.source_key != self.store.policy.source_key
                    or source_row.schema_version != self.store.policy.schema_version
                    or source_row.source_manifest != run_row["source_manifest"]
                    or source_row.metrics != source_metrics_by_field
                ):
                    return None
                return control_run
        except Exception:
            return None

    async def _shielded_reconciliation(self, attempt, run_row, result):
        reconciliation = asyncio.create_task(self._reconcile_commit(attempt, run_row, result))
        while True:
            try:
                return await asyncio.shield(reconciliation)
            except asyncio.CancelledError:
                if reconciliation.cancelled():
                    raise

    def _install_committed_result(self, ctx, committed, result):
        """Install the wrapper's marker only after confirmed durable completion."""
        timestamps_by_field = {}
        for field in ("heartbeat_at", "finished_at"):
            value = committed[field]
            if not isinstance(value, datetime):
                raise RuntimeError(f"{self.store.policy.error_prefix}_control_timestamp_invalid")
            timestamps_by_field[field] = value.replace(tzinfo=timezone.utc).isoformat(timespec="microseconds")
        context = ctx.setdefault("context", {})
        context["preserve_control_run_finished_at"] = True
        context["_control_committed_heartbeat_at"] = timestamps_by_field["heartbeat_at"]
        context["_control_committed_finished_at"] = timestamps_by_field["finished_at"]
        context["_control_committed_result"] = {**result, "terminal_progress": self._terminal_progress(result)}
        context["control_run_terminal_committed"] = True
        current_task = asyncio.current_task()
        while current_task is not None and current_task.cancelling():
            current_task.uncancel()
        return context["_control_committed_result"]

    async def complete_run(self, ctx, task, run_row, metrics):
        """Finish the source and control attempt atomically, including bounded runs."""
        attempt = self._attempt(ctx, task, run_row)
        if run_row["source_manifest"]["max_providers"] is None:
            from process import source_profile_result_archive as archive

            schema = ProviderProfileImportRun.__table__.schema or "mrf"
            async with db.transaction() as session:
                requires_handoff = await archive.is_native_handoff_required(session, schema)
            if requires_handoff or run_row["source_manifest"].get("bundle_contract") is not None:
                await raise_if_cancelled(ctx, task)
                async with suppress_control_run_heartbeat_persistence(attempt["run_id"]):
                    return await archive.handoff_native_publication(
                        db,
                        ctx,
                        importer_id=self.importer,
                        schema=schema,
                        source_run_id=run_row["run_id"],
                        metrics=metrics,
                    )
        completion_result = committed = None
        try:
            async with suppress_control_run_heartbeat_persistence(attempt["run_id"]), db.transaction():
                control_row = await self._locked_control_run(attempt)
                control_run = dict(control_row._mapping) if control_row is not None else None
                if not self._is_attempt(control_run, attempt) or control_run["status"] != "running":
                    raise RuntimeError(f"{self.store.policy.error_prefix}_control_attempt_changed")
                await raise_if_cancelled(ctx, task)
                completion_result = await self._finish_source(run_row, metrics)
                committed = await self._commit_control(attempt, completion_result)
        except BaseException:
            if committed is None:
                raise
            committed = await self._shielded_reconciliation(attempt, run_row, completion_result)
            if committed is None:
                raise
        return self._install_committed_result(ctx, committed, completion_result)
