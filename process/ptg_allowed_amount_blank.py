"""Safe projection of a durable allowed-amount result without payments."""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from typing import Any


ALLOWED_AMOUNT_BLANK_ERROR = (
    "PTG2 allowed-amount import produced no payment evidence"
)
_COUNT_FIELDS = (
    "allowed_amount_plans",
    "allowed_amount_items",
    "allowed_amount_blocks",
    "allowed_amount_payments",
    "allowed_amount_provider_payments",
    "allowed_amount_npi_references",
    "allowed_amount_unique_tins",
)


def _field(candidate: Any, name: str) -> Any:
    if isinstance(candidate, Mapping):
        return candidate.get(name)
    return getattr(candidate, name, None)


def _object(candidate: Any) -> dict[str, Any] | None:
    return dict(candidate) if isinstance(candidate, Mapping) else None


def _month(candidate: Any) -> str | None:
    if isinstance(candidate, (dt.datetime, dt.date)):
        candidate = candidate.isoformat()
    text = str(candidate or "").strip()
    if len(text) == 7:
        text += "-01"
    try:
        parsed = dt.date.fromisoformat(text)
    except ValueError:
        return None
    return parsed.strftime("%Y-%m") if parsed.day == 1 else None


def _count(candidate: Any) -> int | None:
    return candidate if type(candidate) is int and candidate >= 0 else None


def is_allowed_amount_blank_error(candidate: Any) -> bool:
    """Return whether an outer error is the exact durable-empty sentinel."""

    return bool(
        isinstance(candidate, Mapping)
        and candidate.get("code") == "ptg_import_failed"
        and candidate.get("message") == ALLOWED_AMOUNT_BLANK_ERROR
    )


def _is_matching_blank_run(
    source_file_import_id: str,
    source_key: str,
    expected_month: str | None,
    plan_ids: Sequence[str],
    plan_market_types: Sequence[str],
    outer_error: Any,
    engine_run: Any,
) -> bool:
    expected_import_run_id = f"ptg2:{source_file_import_id}"
    options_map = _object(_field(engine_run, "options"))
    return bool(
        source_file_import_id
        and source_key
        and expected_month is not None
        and is_allowed_amount_blank_error(outer_error)
        and engine_run is not None
        and _field(engine_run, "import_run_id") == expected_import_run_id
        and _field(engine_run, "status") == "failed"
        and _month(_field(engine_run, "import_month")) == expected_month
        and _field(engine_run, "finished_at") is not None
        and _field(engine_run, "error") == ALLOWED_AMOUNT_BLANK_ERROR
        and options_map is not None
        and options_map.get("source_key") == source_key
        and options_map.get("plan_ids") == list(plan_ids)
        and options_map.get("plan_market_types") == list(plan_market_types)
    )


def _blank_summary(
    engine_run: Any,
    engine_snapshot: Any,
    expected_import_run_id: str,
    expected_month: str,
) -> tuple[str, dict[str, Any]] | None:
    report_map = _object(_field(engine_run, "report"))
    manifest_map = _object(_field(engine_snapshot, "manifest"))
    snapshot_id = report_map.get("snapshot_id") if report_map else None
    allowed_amount_lane_map = (
        _object(report_map.get("allowed_amount_lane")) if report_map else None
    )
    successful_files = (
        allowed_amount_lane_map.get("successful_files")
        if allowed_amount_lane_map
        else None
    )
    if (
        not isinstance(snapshot_id, str)
        or not snapshot_id
        or allowed_amount_lane_map is None
        or allowed_amount_lane_map.get("files_attempted") != 1
        or allowed_amount_lane_map.get("files_processed") != 1
        or allowed_amount_lane_map.get("files_failed") != 0
        or allowed_amount_lane_map.get("files_skipped") != 0
        or allowed_amount_lane_map.get("failed_files") != []
        or not isinstance(successful_files, list)
        or len(successful_files) != 1
        or _field(engine_snapshot, "snapshot_id") != snapshot_id
        or _field(engine_snapshot, "import_run_id") != expected_import_run_id
        or _month(_field(engine_snapshot, "import_month")) != expected_month
        or _field(engine_snapshot, "status") != "failed"
        or manifest_map is None
        or manifest_map.get("snapshot_id") != snapshot_id
        or manifest_map.get("error") != ALLOWED_AMOUNT_BLANK_ERROR
        or manifest_map.get("allowed_amount_lane") != allowed_amount_lane_map
    ):
        return None
    file_result_map = _object(successful_files[0])
    summary_map = (
        _object(file_result_map.get("summary")) if file_result_map else None
    )
    if (
        file_result_map is None
        or file_result_map.get("source_type") != "allowed_amounts"
        or file_result_map.get("success") is not True
        or file_result_map.get("skipped") is not False
        or file_result_map.get("error") is not None
        or summary_map is None
        or summary_map.get("allowed_amount_evidence") is not False
    ):
        return None
    return snapshot_id, summary_map


def allowed_amount_blank_metrics(
    *,
    source_file_import_id: str,
    source_key: str,
    import_month: Any,
    plan_ids: Sequence[str],
    plan_market_types: Sequence[str],
    outer_error: Any,
    engine_run: Any,
    engine_snapshot: Any,
) -> dict[str, Any] | None:
    """Return public terminal metrics only for one exact durable blank result."""

    expected_import_run_id = f"ptg2:{source_file_import_id}"
    expected_month = _month(import_month)
    if not _is_matching_blank_run(
        source_file_import_id,
        source_key,
        expected_month,
        plan_ids,
        plan_market_types,
        outer_error,
        engine_run,
    ) or engine_snapshot is None:
        return None
    blank_summary = _blank_summary(
        engine_run,
        engine_snapshot,
        expected_import_run_id,
        expected_month,
    )
    if blank_summary is None:
        return None
    snapshot_id, summary_map = blank_summary
    counts_by_name = {
        name: _count(summary_map.get(name)) for name in _COUNT_FIELDS
    }
    if (
        any(count is None for count in counts_by_name.values())
        or counts_by_name["allowed_amount_payments"] != 0
        or counts_by_name["allowed_amount_provider_payments"] != 0
    ):
        return None

    return {
        "status": "blank",
        "import_run_id": expected_import_run_id,
        "snapshot_id": snapshot_id,
        "source_key": source_key,
        "import_month": expected_month,
        "snapshot_status": "failed",
        "files_attempted": 1,
        "files_processed": 1,
        "files_failed": 0,
        "files_skipped": 0,
        "file_domains": ["allowed_amounts"],
        **counts_by_name,
        "allowed_amount_evidence": False,
    }


__all__ = [
    "ALLOWED_AMOUNT_BLANK_ERROR",
    "allowed_amount_blank_metrics",
    "is_allowed_amount_blank_error",
]
