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


def _field(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(name)
    return getattr(value, name, None)


def _object(value: Any) -> dict[str, Any] | None:
    return dict(value) if isinstance(value, Mapping) else None


def _month(value: Any) -> str | None:
    if isinstance(value, (dt.datetime, dt.date)):
        value = value.isoformat()
    text = str(value or "").strip()
    if len(text) == 7:
        text += "-01"
    try:
        parsed = dt.date.fromisoformat(text)
    except ValueError:
        return None
    return parsed.strftime("%Y-%m") if parsed.day == 1 else None


def _count(value: Any) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _outer_error_is_blank(value: Any) -> bool:
    return bool(
        isinstance(value, Mapping)
        and value.get("code") == "ptg_import_failed"
        and value.get("message") == ALLOWED_AMOUNT_BLANK_ERROR
    )


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
    options = _object(_field(engine_run, "options"))
    report = _object(_field(engine_run, "report"))
    manifest = _object(_field(engine_snapshot, "manifest"))
    if (
        not source_file_import_id
        or not source_key
        or expected_month is None
        or not _outer_error_is_blank(outer_error)
        or engine_run is None
        or engine_snapshot is None
        or _field(engine_run, "import_run_id") != expected_import_run_id
        or _field(engine_run, "status") != "failed"
        or _month(_field(engine_run, "import_month")) != expected_month
        or _field(engine_run, "finished_at") is None
        or _field(engine_run, "error") != ALLOWED_AMOUNT_BLANK_ERROR
        or options is None
        or options.get("source_key") != source_key
        or options.get("plan_ids") != list(plan_ids)
        or options.get("plan_market_types") != list(plan_market_types)
        or report is None
        or manifest is None
    ):
        return None

    snapshot_id = report.get("snapshot_id")
    lane = _object(report.get("allowed_amount_lane"))
    successful_files = lane.get("successful_files") if lane else None
    if (
        not isinstance(snapshot_id, str)
        or not snapshot_id
        or lane is None
        or lane.get("files_attempted") != 1
        or lane.get("files_processed") != 1
        or lane.get("files_failed") != 0
        or lane.get("files_skipped") != 0
        or lane.get("failed_files") != []
        or not isinstance(successful_files, list)
        or len(successful_files) != 1
        or _field(engine_snapshot, "snapshot_id") != snapshot_id
        or _field(engine_snapshot, "import_run_id") != expected_import_run_id
        or _month(_field(engine_snapshot, "import_month")) != expected_month
        or _field(engine_snapshot, "status") != "failed"
        or manifest.get("snapshot_id") != snapshot_id
        or manifest.get("error") != ALLOWED_AMOUNT_BLANK_ERROR
        or manifest.get("allowed_amount_lane") != lane
    ):
        return None

    file_result = _object(successful_files[0])
    summary = _object(file_result.get("summary")) if file_result else None
    if (
        file_result is None
        or file_result.get("source_type") != "allowed_amounts"
        or file_result.get("success") is not True
        or file_result.get("skipped") is not False
        or file_result.get("error") is not None
        or summary is None
        or summary.get("allowed_amount_evidence") is not False
    ):
        return None
    counts = {name: _count(summary.get(name)) for name in _COUNT_FIELDS}
    if (
        any(value is None for value in counts.values())
        or counts["allowed_amount_payments"] != 0
        or counts["allowed_amount_provider_payments"] != 0
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
        **counts,
        "allowed_amount_evidence": False,
    }


__all__ = ["ALLOWED_AMOUNT_BLANK_ERROR", "allowed_amount_blank_metrics"]
