"""Safe terminal summaries for Provider Directory acquisition reports."""

from __future__ import annotations

import json
import re
from typing import Any


SENSITIVE_TEXT_PATTERN = re.compile(
    r"(?i)(?:bearer\s+\S+|token|secret|password|authorization|api[_-]?key|credential)"
)


def _has_bounded_metrics(run_record: dict[str, Any]) -> bool:
    metrics = run_record.get("metrics") if isinstance(run_record.get("metrics"), dict) else {}
    stats = metrics.get("resource_fetch_stats") if isinstance(metrics.get("resource_fetch_stats"), dict) else {}
    return any(
        isinstance(resource_stats, dict) and resource_stats.get("sources_bounded", 0)
        for resource_stats in stats.values()
    )


def _terminal_error_summary(error: Any) -> dict[str, str] | None:
    if not isinstance(error, dict):
        return None
    safe_field_names = ("code", "type", "status", "reason", "message")
    terminal_error_by_field = {
        name: str(error[name])[:500]
        for name in safe_field_names
        if isinstance(error.get(name), (str, int, float, bool))
        and "..." not in str(error[name])
        and not SENSITIVE_TEXT_PATTERN.search(str(error[name]))
    }
    return terminal_error_by_field or None


def _run_summary(run_record: dict[str, Any]) -> dict[str, Any]:
    metrics = run_record.get("metrics") if isinstance(run_record.get("metrics"), dict) else {}
    params_by_name = run_record.get("params") if isinstance(run_record.get("params"), dict) else {}
    run_summary_dict = {
        "run_id": run_record.get("run_id"), "status": run_record.get("status"), "created_at": run_record.get("created_at"),
        "finished_at": run_record.get("finished_at"), "retry_of_run_id": params_by_name.get("retry_of_run_id"),
        "source_ids": metrics.get("source_ids"), "sources_probed": metrics.get("sources_probed"),
        "selected_sources": metrics.get("source_import_sources_selected"), "selected_groups": metrics.get("source_import_groups_attempted"),
        "pagination_resume_required": metrics.get("pagination_resume_required"),
        "resource_outcomes": metrics.get("resource_fetch_stats"),
        "effective_acquisition": {"sources_probed": metrics.get("sources_probed"), "selected_sources": metrics.get("source_import_sources_selected"), "selected_groups": metrics.get("source_import_groups_attempted"), "completed_source_ids": metrics.get("resource_fetch_completed_source_ids"), "bulk_export": metrics.get("bulk_export_mode")},
    }
    terminal_error = _terminal_error_summary(run_record.get("error"))
    if terminal_error:
        run_summary_dict["terminal_error"] = terminal_error
    return run_summary_dict
