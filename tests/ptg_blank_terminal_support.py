"""Synthetic durable blank state for ordinary terminal receipt tests."""

from __future__ import annotations

import copy
import datetime as dt
from types import SimpleNamespace

from process.ptg_allowed_amount_blank import ALLOWED_AMOUNT_BLANK_ERROR
from process.ptg_wave_ordinary_terminal_receipt import (
    ORDINARY_TERMINAL_REQUEST_SCHEMA,
)
from tests.ptg_wave_ordinary_terminal_receipt_support import (
    IMPORT_MONTH,
    NODE_ID,
    OPERATION_ID,
    ORDINARY_IMPORT_ID,
    ORDINARY_PLAN_IDS,
    ORDINARY_RUN_ID,
    PLAN_MARKET_TYPES,
    SNAPSHOT_ID,
    _ordinary_run_maps,
    direct_v6_boundary,
)


def _blank_metrics(source_key: str) -> dict:
    return {
        "status": "blank",
        "import_run_id": f"ptg2:{ORDINARY_IMPORT_ID}",
        "snapshot_id": SNAPSHOT_ID,
        "source_key": source_key,
        "import_month": IMPORT_MONTH,
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


def _blank_engine_state(source_key: str) -> tuple[SimpleNamespace, SimpleNamespace]:
    allowed_amount_lane_map = {
        "files_attempted": 1,
        "files_processed": 1,
        "files_failed": 0,
        "files_skipped": 0,
        "failed_files": [],
        "successful_files": [{
            "source_type": "allowed_amounts",
            "success": True,
            "skipped": False,
            "error": None,
            "summary": {
                **{
                    name: count
                    for name, count in _blank_metrics(source_key).items()
                    if name.startswith("allowed_amount_")
                }
            },
        }],
    }
    engine_import_run_id = f"ptg2:{ORDINARY_IMPORT_ID}"
    engine_run = SimpleNamespace(
        import_run_id=engine_import_run_id,
        import_month=dt.date(2026, 8, 1),
        status="failed",
        finished_at=dt.datetime(2026, 8, 10, 13, 14, 14, 999999),
        options={
            "source_key": source_key,
            "plan_ids": ORDINARY_PLAN_IDS,
            "plan_market_types": PLAN_MARKET_TYPES,
            "max_files": 1,
        },
        report={
            "snapshot_id": SNAPSHOT_ID,
            "allowed_amount_lane": allowed_amount_lane_map,
        },
        error=ALLOWED_AMOUNT_BLANK_ERROR,
    )
    snapshot = SimpleNamespace(
        snapshot_id=SNAPSHOT_ID,
        import_run_id=engine_import_run_id,
        import_month=dt.date(2026, 8, 1),
        status="failed",
        manifest={
            "snapshot_id": SNAPSHOT_ID,
            "allowed_amount_lane": copy.deepcopy(allowed_amount_lane_map),
            "error": ALLOWED_AMOUNT_BLANK_ERROR,
        },
    )
    return engine_run, snapshot


def blank_ordinary_result(monkeypatch) -> dict:
    """Build one failed allowed-amount result with exact blank evidence."""

    wave, intents, quarantine, frozen_params, direct_intent = (
        direct_v6_boundary(monkeypatch, source_type="allowed_amounts")
    )
    source_key = direct_intent["source_key"]
    run_params, _ = _ordinary_run_maps(
        frozen_params,
        direct_intent,
        source_key,
    )
    engine_run, engine_snapshot = _blank_engine_state(source_key)
    outer_error_map = {
        "code": "ptg_import_failed",
        "message": ALLOWED_AMOUNT_BLANK_ERROR,
    }
    run = SimpleNamespace(
        run_id=ORDINARY_RUN_ID,
        engine="healthcare-mrf-api",
        node_id=NODE_ID,
        importer="ptg",
        status="failed",
        params=run_params,
        metrics=_blank_metrics(source_key),
        error=outer_error_map,
        snapshot_id=None,
        import_id=ORDINARY_IMPORT_ID,
        source_file_import_id=ORDINARY_IMPORT_ID,
        finished_at=dt.datetime(2026, 8, 10, 13, 14, 15, 123456),
    )
    return {
        "request": {
            "schema": ORDINARY_TERMINAL_REQUEST_SCHEMA,
            "key_id": "fixture-epoch-2026-08",
            "operation_id": OPERATION_ID,
            "member_ordinal": 0,
            "source_file_import_id": ORDINARY_IMPORT_ID,
            "run_id": ORDINARY_RUN_ID,
        },
        "wave": wave,
        "intent": intents[0],
        "quarantine": quarantine,
        "run": run,
        "engine_run": engine_run,
        "engine_snapshot": engine_snapshot,
    }


__all__ = ["blank_ordinary_result"]
