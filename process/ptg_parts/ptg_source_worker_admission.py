# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed durable admission for one queued PTG worker task."""

from __future__ import annotations

from typing import Any, Mapping

from process.ptg_parts import ptg_source_attempt_actions as source_actions


def _skipped(run_id: str, reason: str) -> dict[str, Any]:
    response_by_field: dict[str, Any] = {
        "status": "skipped",
        "reason": reason,
    }
    if run_id:
        response_by_field["run_id"] = run_id
    return response_by_field


async def guard_ptg_worker_start(
    task_payload: Mapping[str, Any],
    *,
    run_id: str,
    attempt_id: str | None,
) -> dict[str, Any] | None:
    """Return a skipped response or append admission before worker work."""

    try:
        task_source_id = source_actions.source_file_import_id_from_payload(
            task_payload,
            required=False,
        )
    except ValueError:
        reason = (
            "source_attempt_identity_mismatch"
            if run_id
            else "source_attempt_id_invalid"
        )
        return _skipped(run_id, reason)
    if not run_id:
        return (
            _skipped(run_id, "source_attempt_run_id_required")
            if task_source_id is not None
            else None
        )
    try:
        admitted_run = await source_actions.admit_existing_outer_run_action(
            run_id=run_id,
            event_kind="worker_start_admitted",
            attempt_id=attempt_id,
            expected_source_file_import_id=task_source_id,
        )
    except source_actions.PTGSourceAttemptIdentityError:
        return _skipped(run_id, "source_attempt_identity_mismatch")
    except (
        source_actions.PTGSourceAttemptFencedError,
        source_actions.PTGSourceAttemptTerminalError,
    ):
        return _skipped(run_id, "source_attempt_reconciled")
    if admitted_run is None:
        return _skipped(run_id, "run_missing")
    if str(admitted_run.get("importer") or "") != "ptg":
        return _skipped(run_id, "run_importer_mismatch")
    return None


__all__ = ["guard_ptg_worker_start"]
