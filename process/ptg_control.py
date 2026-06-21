# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime as dt
import os
from contextlib import contextmanager
from typing import Any

from db.models import db
from process.control_cancel import ImportCancelledError, raise_if_cancelled
from process.control_lifecycle import (
    _live_progress_heartbeat,
    _stop_live_progress_heartbeat,
    mark_control_run,
)
from process.import_status_events import flush_status_events
from process.ptg import main as ptg_main
from process.ptg_parts.config import (
    PTG2_FILE_PROCESS_CONCURRENCY_ENV,
    PTG2_RUST_EVENT_QUEUE_ENV,
    PTG2_RUST_PARSE_IN_WORKERS_ENV,
    PTG2_RUST_PROVIDER_REF_CHUNK_ITEMS_ENV,
    PTG2_RUST_PROVIDER_REF_BYTE_PRELOAD_ENV,
    PTG2_RUST_PROVIDER_REF_QUEUE_ENV,
    PTG2_RUST_PROVIDER_REF_RAW_CHUNK_BYTES_ENV,
    PTG2_RUST_PROVIDER_REF_WORKERS_ENV,
    PTG2_RUST_PROVIDER_REFS_IN_WORKERS_ENV,
    PTG2_RUST_WORK_QUEUE_ENV,
    PTG2_RUST_WORKERS_ENV,
)

PTG_CONTROL_QUEUE_NAME = "arq:PTG"
_TERMINAL_RUN_STATUSES = {"succeeded", "failed", "canceled", "cancelled", "dead_letter"}
_TERMINAL_SOURCE_IMPORT_STATUSES = {"succeeded", "failed", "canceled", "cancelled", "unsupported", "dead_letter"}


async def ptg_control_start(ctx, task: dict[str, Any] | None = None):
    payload = task if isinstance(task, dict) else {}
    run_id = str(payload.get("run_id") or "").strip()
    params = payload.get("params") if isinstance(payload.get("params"), dict) else payload
    source_file_import_id = str(
        payload.get("source_file_import_id") or params.get("source_file_import_id") or ""
    ).strip()
    stale_result = await _stale_ptg_job_result(run_id, source_file_import_id)
    if stale_result is not None:
        return stale_result
    heartbeat_task = None
    try:
        await mark_control_run(run_id, status="running", phase_detail="ptg import running", progress_message="running")
        if run_id:
            started_at = dt.datetime.now(dt.UTC).isoformat()
            heartbeat_task = asyncio.create_task(
                _live_progress_heartbeat(run_id, "ptg", "ptg_control_start", started_at)
            )
        await raise_if_cancelled(ctx, payload)
        _assert_expected_lane(params)
        with _ptg_lane_environment(params):
            result = await ptg_main(
                test_mode=bool(params.get("test_mode", params.get("test", False))),
                toc_urls=_string_list(params.get("toc_urls") or params.get("toc_url")),
                toc_list=params.get("toc_list"),
                in_network_url=params.get("in_network_url"),
                allowed_url=params.get("allowed_url"),
                provider_ref_url=params.get("provider_ref_url"),
                import_id=params.get("import_id"),
                source_key=params.get("source_key"),
                import_month=params.get("import_month"),
                max_files=_optional_int(params.get("max_files")),
                max_items=_optional_int(params.get("max_items")),
                plan_ids=_string_list(params.get("plan_ids") or params.get("plan_id")),
                plan_name_contains=_string_list(params.get("plan_name_contains")),
                plan_market_types=_string_list(params.get("plan_market_types") or params.get("plan_market_type")),
                file_url_contains=_string_list(params.get("file_url_contains")),
                reuse_raw_artifacts=bool(params.get("reuse_raw_artifacts", True)),
                keep_partial_artifacts=params.get("keep_partial_artifacts"),
                control_run_id=run_id,
            )
    except ImportCancelledError:
        await mark_control_run(run_id, status="canceled", phase_detail="ptg import canceled", progress_message="canceled")
        await _flush_terminal_status_events()
        return {"status": "canceled", "run_id": run_id}
    except Exception as exc:
        await mark_control_run(
            run_id,
            status="failed",
            phase_detail="ptg import failed",
            progress_message="failed",
            error={"code": "ptg_import_failed", "message": str(exc)},
        )
        await _flush_terminal_status_events()
        raise
    finally:
        await _stop_live_progress_heartbeat(heartbeat_task)
    result_metrics = result if isinstance(result, dict) else {}
    await mark_control_run(
        run_id,
        status="succeeded",
        phase_detail="ptg import succeeded",
        progress_message="succeeded",
        metrics=result_metrics or None,
        snapshot_id=str(result_metrics.get("snapshot_id") or "").strip() or None,
    )
    await _flush_terminal_status_events()
    return {**result_metrics, "status": "succeeded", "run_id": run_id}


async def _stale_ptg_job_result(run_id: str, source_file_import_id: str) -> dict[str, Any] | None:
    if not run_id or not source_file_import_id:
        return None
    row = await db.first(
        """
        SELECT sfi.engine_run_id, sfi.status, ir.status
          FROM hp_import_control.source_file_import sfi
          LEFT JOIN mrf.import_run ir ON ir.run_id = :run_id
         WHERE sfi.source_file_import_id = :source_file_import_id
         LIMIT 1
        """,
        run_id=run_id,
        source_file_import_id=source_file_import_id,
    )
    if row is None:
        return {
            "status": "skipped",
            "run_id": run_id,
            "source_file_import_id": source_file_import_id,
            "reason": "source_file_import_missing",
        }
    current_engine_run_id = str(row[0] or "").strip()
    source_status = str(row[1] or "").strip().lower()
    run_status = str(row[2] or "").strip().lower()
    if run_status in _TERMINAL_RUN_STATUSES:
        return {
            "status": "skipped",
            "run_id": run_id,
            "source_file_import_id": source_file_import_id,
            "current_engine_run_id": current_engine_run_id or None,
            "reason": f"run_{run_status}",
        }
    if current_engine_run_id and current_engine_run_id != run_id:
        return {
            "status": "skipped",
            "run_id": run_id,
            "source_file_import_id": source_file_import_id,
            "current_engine_run_id": current_engine_run_id,
            "reason": "superseded_source_import_run",
        }
    if source_status in _TERMINAL_SOURCE_IMPORT_STATUSES:
        return {
            "status": "skipped",
            "run_id": run_id,
            "source_file_import_id": source_file_import_id,
            "current_engine_run_id": current_engine_run_id or None,
            "reason": f"source_import_{source_status}",
        }
    return None


async def _flush_terminal_status_events() -> None:
    timeout = float(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_TERMINAL_FLUSH_SECONDS", "0.25"))
    if timeout <= 0:
        return
    await flush_status_events(timeout_seconds=timeout)


def _assert_expected_lane(params: dict[str, Any]) -> None:
    expected_queue = str(params.get("_expected_queue") or "").strip()
    active_queue = os.getenv("HLTHPRT_ACTIVE_WORKER_QUEUE", "").strip()
    if expected_queue and active_queue and expected_queue != active_queue:
        raise RuntimeError(f"PTG payload expected {expected_queue}, but active worker queue is {active_queue}")
    expected_class = str(params.get("_expected_worker_class") or "").strip()
    active_class = os.getenv("HLTHPRT_ACTIVE_WORKER_CLASS", "").strip()
    if expected_class and active_class and expected_class != active_class:
        raise RuntimeError(f"PTG payload expected {expected_class}, but active worker class is {active_class}")


@contextmanager
def _ptg_lane_environment(params: dict[str, Any]):
    overrides = {
        PTG2_RUST_WORKERS_ENV: _optional_env_value(params.get("_scanner_rust_workers")),
        PTG2_RUST_PARSE_IN_WORKERS_ENV: _bool_env_value(params.get("_scanner_parse_in_workers")),
        PTG2_RUST_WORK_QUEUE_ENV: _optional_env_value(params.get("_scanner_work_queue")),
        PTG2_RUST_EVENT_QUEUE_ENV: _optional_env_value(params.get("_scanner_event_queue")),
        PTG2_RUST_PROVIDER_REFS_IN_WORKERS_ENV: _bool_env_value(
            params.get("_scanner_provider_refs_in_workers")
        ),
        PTG2_RUST_PROVIDER_REF_WORKERS_ENV: _optional_env_value(params.get("_scanner_provider_ref_workers")),
        PTG2_RUST_PROVIDER_REF_QUEUE_ENV: _optional_env_value(params.get("_scanner_provider_ref_queue")),
        PTG2_RUST_PROVIDER_REF_CHUNK_ITEMS_ENV: _optional_env_value(
            params.get("_scanner_provider_ref_chunk_items")
        ),
        PTG2_RUST_PROVIDER_REF_RAW_CHUNK_BYTES_ENV: _optional_env_value(
            params.get("_scanner_provider_ref_raw_chunk_bytes")
        ),
        PTG2_RUST_PROVIDER_REF_BYTE_PRELOAD_ENV: _bool_env_value(
            params.get("_scanner_provider_ref_byte_preload")
        ),
        PTG2_FILE_PROCESS_CONCURRENCY_ENV: _optional_env_value(params.get("_file_process_concurrency")),
    }
    previous: dict[str, str | None] = {}
    try:
        for name, value in overrides.items():
            if value is None:
                continue
            previous[name] = os.environ.get(name)
            os.environ[name] = value
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _optional_env_value(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _bool_env_value(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return "true" if str(value).strip().lower() in {"1", "true", "yes", "on"} else "false"


def _string_list(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else None
    if isinstance(value, (list, tuple)):
        result = [str(item).strip() for item in value if str(item).strip()]
        return result or None
    return None


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)
