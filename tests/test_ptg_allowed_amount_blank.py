"""Durable allowed-amount blank projection tests."""

from __future__ import annotations

import copy
import datetime as dt
from types import SimpleNamespace

from process.ptg_allowed_amount_blank import (
    ALLOWED_AMOUNT_BLANK_ERROR,
    allowed_amount_blank_metrics,
)


def _allowed_amount_lane() -> dict:
    return {
        "files_attempted": 1,
        "files_processed": 1,
        "files_failed": 0,
        "files_skipped": 0,
        "failed_files": [],
        "successful_files": [
            {
                "source_type": "allowed_amounts",
                "success": True,
                "skipped": False,
                "error": None,
                "summary": {
                    "allowed_amount_plans": 2,
                    "allowed_amount_items": 0,
                    "allowed_amount_blocks": 0,
                    "allowed_amount_payments": 0,
                    "allowed_amount_provider_payments": 0,
                    "allowed_amount_npi_references": 0,
                    "allowed_amount_unique_tins": 0,
                    "allowed_amount_evidence": False,
                },
            }
        ],
    }


def _blank_state() -> dict:
    allowed_amount_lane_map = _allowed_amount_lane()
    return {
        "source_file_import_id": "source-import-neutral",
        "source_key": "ptg_source_neutral",
        "import_month": "2026-08",
        "plan_ids": ["plan-neutral"],
        "plan_market_types": ["group"],
        "outer_error": {
            "code": "ptg_import_failed",
            "message": ALLOWED_AMOUNT_BLANK_ERROR,
        },
        "engine_run": SimpleNamespace(
            import_run_id="ptg2:source-import-neutral",
            import_month=dt.date(2026, 8, 1),
            status="failed",
            finished_at=dt.datetime(2026, 8, 10, 12, 0),
            error=ALLOWED_AMOUNT_BLANK_ERROR,
            options={
                "source_key": "ptg_source_neutral",
                "plan_ids": ["plan-neutral"],
                "plan_market_types": ["group"],
            },
            report={
                "snapshot_id": "ptg2:202608:snapshot-neutral",
                "allowed_amount_lane": allowed_amount_lane_map,
            },
        ),
        "engine_snapshot": SimpleNamespace(
            snapshot_id="ptg2:202608:snapshot-neutral",
            import_run_id="ptg2:source-import-neutral",
            import_month=dt.date(2026, 8, 1),
            status="failed",
            manifest={
                "snapshot_id": "ptg2:202608:snapshot-neutral",
                "error": ALLOWED_AMOUNT_BLANK_ERROR,
                "allowed_amount_lane": copy.deepcopy(allowed_amount_lane_map),
            },
        ),
    }


def test_projects_only_exact_durable_blank_result() -> None:
    blank_state = _blank_state()
    metrics = allowed_amount_blank_metrics(**blank_state)
    assert metrics == {
        "status": "blank",
        "import_run_id": "ptg2:source-import-neutral",
        "snapshot_id": "ptg2:202608:snapshot-neutral",
        "source_key": "ptg_source_neutral",
        "import_month": "2026-08",
        "snapshot_status": "failed",
        "files_attempted": 1,
        "files_processed": 1,
        "files_failed": 0,
        "files_skipped": 0,
        "file_domains": ["allowed_amounts"],
        "allowed_amount_plans": 2,
        "allowed_amount_items": 0,
        "allowed_amount_blocks": 0,
        "allowed_amount_payments": 0,
        "allowed_amount_provider_payments": 0,
        "allowed_amount_npi_references": 0,
        "allowed_amount_unique_tins": 0,
        "allowed_amount_evidence": False,
    }
    for durable_state in (
        blank_state["engine_run"].report,
        blank_state["engine_snapshot"].manifest,
    ):
        durable_state["allowed_amount_lane"]["successful_files"][0][
            "summary"
        ]["allowed_amount_provider_payments"] = 1
    assert allowed_amount_blank_metrics(**blank_state) is None
    blank_state = _blank_state()
    blank_state["engine_snapshot"].status = "validated"
    assert allowed_amount_blank_metrics(**blank_state) is None
    blank_state = _blank_state()
    blank_state["engine_run"].import_run_id = "ptg2:wrong-source"
    assert allowed_amount_blank_metrics(**blank_state) is None
    blank_state = _blank_state()
    blank_state["engine_snapshot"].manifest["allowed_amount_lane"][
        "successful_files"
    ][0]["summary"]["allowed_amount_plans"] = 3
    assert allowed_amount_blank_metrics(**blank_state) is None


def test_blank_projection_accepts_mappings_and_rejects_evidence_drift() -> None:
    blank_state = _blank_state()
    blank_state["engine_run"] = vars(blank_state["engine_run"])
    assert allowed_amount_blank_metrics(**blank_state) is not None
    lane = blank_state["engine_run"]["report"]["allowed_amount_lane"]
    blank_state["engine_snapshot"].manifest["allowed_amount_lane"] = lane
    lane["successful_files"][0]["summary"][
        "allowed_amount_evidence"
    ] = True
    assert allowed_amount_blank_metrics(**blank_state) is None
