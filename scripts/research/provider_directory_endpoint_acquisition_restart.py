"""Guarded fresh-root state transitions for Provider Directory acquisitions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


ACTIVE_STATUSES = {"queued", "starting", "running", "finalizing", "canceling"}
TERMINAL_STATUSES = {"succeeded", "failed", "canceled", "cancelled", "dead_letter"}
RESTARTABLE_FAILURE_STATUSES = {
    "metric_validation_failed",
    "failed",
    "bounded",
    "canceled",
    "cancelled",
    "unknown_terminal",
}


class HarnessConflict(RuntimeError):
    """Raised when serial acquisition state cannot be changed safely."""


@dataclass(frozen=True)
class RestartLineage:
    """Immutable prior lineage captured before a guarded fresh root."""

    prior_status: str
    root_run_id: str
    current_run_id: str
    run_ids: tuple[str, ...]


def restart_history(entry_state: dict[str, Any]) -> list[dict[str, Any]]:
    """Return validated restart audit records from one entry state."""

    history = entry_state.get("restart_history", [])
    if not isinstance(history, list) or not all(isinstance(record, dict) for record in history):
        raise HarnessConflict("restart audit history is ambiguous")
    return history


def archived_run_ids(entry_state: dict[str, Any]) -> set[str]:
    """Return run identifiers archived by prior fresh-root recoveries."""

    archived_ids: set[str] = set()
    for restart_record in restart_history(entry_state):
        prior_lineage = restart_record.get("prior_lineage")
        if not isinstance(prior_lineage, dict):
            raise HarnessConflict("restart audit lineage is ambiguous")
        run_ids = prior_lineage.get("run_ids")
        if not isinstance(run_ids, list) or not all(isinstance(run_id, str) and run_id for run_id in run_ids):
            raise HarnessConflict("restart audit run history is ambiguous")
        archived_ids.update(run_ids)
    return archived_ids


def fresh_root_generation(entry_state: dict[str, Any]) -> int:
    """Return the deterministic idempotency generation for the next root."""

    return len(restart_history(entry_state))


def restart_lineage(entry: dict[str, Any], entry_state: dict[str, Any]) -> RestartLineage:
    """Validate and capture the lineage eligible for a fresh root."""

    entry_id = str(entry["entry_id"])
    if entry["launch_mode"] != "create":
        raise HarnessConflict(f"{entry_id}: only create entries may start a fresh root")
    prior_status = str(entry_state.get("status") or "")
    if prior_status not in RESTARTABLE_FAILURE_STATUSES:
        raise HarnessConflict(
            f"{entry_id}: {prior_status or 'unstarted'} is not a retryable terminal failure"
        )
    root_run_id = entry_state.get("root_run_id")
    current_run_id = entry_state.get("current_run_id")
    run_ids = entry_state.get("run_ids")
    if not isinstance(run_ids, list) or not run_ids or not all(
        isinstance(run_id, str) and run_id for run_id in run_ids
    ):
        raise HarnessConflict(f"{entry_id}: prior run history is ambiguous")
    if (
        not isinstance(root_run_id, str)
        or not isinstance(current_run_id, str)
        or root_run_id not in run_ids
        or current_run_id not in run_ids
    ):
        raise HarnessConflict(f"{entry_id}: prior root/current lineage is ambiguous")
    return RestartLineage(prior_status, root_run_id, current_run_id, tuple(run_ids))


def validate_restart_run(
    entry_id: str,
    lineage: RestartLineage,
    run_record: dict[str, Any],
    metric_errors: list[str],
    *,
    resume_required: bool,
    has_retry_child: bool,
    has_bounded_metrics: bool,
) -> None:
    """Reject active, successful, resumable, or mismatched restart targets."""

    run_status = str(run_record.get("status") or "")
    if run_status in ACTIVE_STATUSES:
        raise HarnessConflict(f"{entry_id}: current run {lineage.current_run_id} is active")
    if run_status in {"", "pending", "unstarted"}:
        raise HarnessConflict(
            f"{entry_id}: current run {lineage.current_run_id} is unstarted or has no terminal status"
        )
    if resume_required:
        raise HarnessConflict(f"{entry_id}: pagination retry lineage must resume instead of restarting")
    if run_status == "succeeded" and not metric_errors:
        raise HarnessConflict(f"{entry_id}: current run is successfully and fully validated")
    if has_retry_child:
        raise HarnessConflict(f"{entry_id}: current run already has retry lineage")
    _validate_terminal_state(
        entry_id,
        lineage.prior_status,
        run_status,
        metric_errors,
        has_bounded_metrics,
    )


def _validate_terminal_state(
    entry_id: str,
    prior_status: str,
    run_status: str,
    metric_errors: list[str],
    has_bounded_metrics: bool,
) -> None:
    if prior_status == "metric_validation_failed" and (
        run_status != "succeeded" or not metric_errors
    ):
        raise HarnessConflict(f"{entry_id}: metric validation failure does not match current run")
    if prior_status == "bounded" and not has_bounded_metrics:
        raise HarnessConflict(f"{entry_id}: bounded state does not match current run")
    if prior_status == "failed" and run_status not in {"failed", "dead_letter"}:
        raise HarnessConflict(f"{entry_id}: failed state does not match current run")
    if prior_status in {"canceled", "cancelled"} and run_status not in {
        "canceled",
        "cancelled",
    }:
        raise HarnessConflict(f"{entry_id}: canceled state does not match current run")
    if prior_status == "unknown_terminal" and run_status in TERMINAL_STATUSES:
        raise HarnessConflict(f"{entry_id}: unknown terminal state does not match current run")


def archive_restart(
    entry_state: dict[str, Any],
    lineage: RestartLineage,
    run_summary: dict[str, Any],
    metric_errors: list[str],
    restarted_at: str,
) -> None:
    """Append an auditable prior-lineage record before clearing launch state."""

    history = restart_history(entry_state)
    history.append(
        {
            "restarted_at": restarted_at,
            "reason": "guarded_fresh_root_recovery",
            "prior_status": lineage.prior_status,
            "prior_lineage": {
                "root_run_id": lineage.root_run_id,
                "current_run_id": lineage.current_run_id,
                "run_ids": list(lineage.run_ids),
            },
            "prior_last_run": run_summary,
            "prior_metric_errors": metric_errors,
        }
    )
    entry_state["restart_history"] = history


def clear_launch_lineage(entry_state: dict[str, Any]) -> None:
    """Clear only mutable launch pointers while preserving run history."""

    launch_fields = (
        "root_run_id",
        "current_run_id",
        "last_run",
        "action",
        "request",
        "metric_errors",
        "message",
    )
    for field_name in launch_fields:
        entry_state.pop(field_name, None)
    entry_state["status"] = "pending"
