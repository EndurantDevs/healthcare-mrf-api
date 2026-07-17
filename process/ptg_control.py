# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime as dt
import os
import threading
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
from process.live_progress import write_live_progress
from process.ptg_parts.config import (
    PTG2_FILE_PROCESS_CONCURRENCY_ENV,
    PTG2_MANIFEST_MERGE_CHUNK_BYTES_ENV,
    PTG2_MANIFEST_MERGE_SORT_WORKERS_ENV,
    PTG2_RUST_EVENT_QUEUE_ENV,
    PTG2_RUST_PARSE_IN_WORKERS_ENV,
    PTG2_RUST_PROVIDER_REF_CHUNK_ITEMS_ENV,
    PTG2_RUST_PROVIDER_REF_QUEUE_ENV,
    PTG2_RUST_PROVIDER_REF_RAW_CHUNK_BYTES_ENV,
    PTG2_RUST_PROVIDER_REF_WORKERS_ENV,
    PTG2_RUST_PROVIDER_REFS_IN_WORKERS_ENV,
    PTG2_RUST_SPLIT_NEGOTIATED_RATES_ENV,
    PTG2_RUST_TOP_LEVEL_BYTE_SCAN_ENV,
    PTG2_RUST_RAW_CHUNK_BYTES_ENV,
    PTG2_RUST_WORK_QUEUE_ENV,
    PTG2_RUST_WORKERS_ENV,
)

PTG_CONTROL_QUEUE_NAME = "arq:PTG"
PTG_CONTROL_HEARTBEAT_SOURCE = "engine-heartbeat"
_TERMINAL_RUN_STATUSES = {"succeeded", "failed", "canceled", "cancelled", "dead_letter"}


async def ptg_control_start(ctx, task: dict[str, Any] | None = None):
    """Run one PTG control task with cancellation and heartbeat handling."""
    task_payload = task if isinstance(task, dict) else {}
    run_id = str(task_payload.get("run_id") or "").strip()
    params = (
        task_payload.get("params")
        if isinstance(task_payload.get("params"), dict)
        else task_payload
    )
    stale_result = await _stale_ptg_job_result(run_id)
    if stale_result is not None:
        return stale_result
    heartbeat_task = None
    heartbeat_stop = None
    try:
        await mark_control_run(run_id, status="running", phase_detail="ptg import running", progress_message="running")
        if run_id:
            started_at = dt.datetime.now(dt.UTC).isoformat()
            heartbeat_task = asyncio.create_task(
                _live_progress_heartbeat(run_id, "ptg", "ptg_control_start", started_at)
            )
            heartbeat_stop = _start_threaded_ptg_heartbeat(run_id, started_at)
        await raise_if_cancelled(ctx, task_payload)
        _assert_expected_lane(params)
        with _ptg_lane_environment(params):
            import_result = await ptg_main(
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
                source_network_names=_string_list(params.get("source_network_names") or params.get("source_network_name")),
                reuse_raw_artifacts=bool(params.get("reuse_raw_artifacts", True)),
                keep_partial_artifacts=params.get("keep_partial_artifacts"),
                control_run_id=run_id,
            )
    except ImportCancelledError:
        await mark_control_run(run_id, status="canceled", phase_detail="ptg import canceled", progress_message="canceled")
        await _flush_terminal_status_events()
        return {"status": "canceled", "run_id": run_id}
    except asyncio.CancelledError:
        await mark_control_run(
            run_id,
            status="failed",
            phase_detail="ptg import interrupted",
            progress_message="interrupted",
            error={"code": "import_interrupted", "message": "worker task was cancelled"},
        )
        await _flush_terminal_status_events()
        raise
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
        _stop_threaded_ptg_heartbeat(heartbeat_stop)
        await _stop_live_progress_heartbeat(heartbeat_task)
    result_metrics = import_result if isinstance(import_result, dict) else {}
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


def _start_threaded_ptg_heartbeat(run_id: str, started_at: str) -> threading.Event:
    stop_event = threading.Event()
    interval = float(os.getenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "15"))
    if interval <= 0:
        return stop_event

    def _heartbeat() -> None:
        while not stop_event.wait(interval):
            write_live_progress(
                run_id=run_id,
                importer="ptg",
                status="running",
                phase="ptg import running",
                unit="run",
                done=0,
                total=1,
                pct=0,
                message="running",
                started_at=started_at,
                source=PTG_CONTROL_HEARTBEAT_SOURCE,
                confidence="heartbeat",
                publish_event=False,
            )

    thread = threading.Thread(target=_heartbeat, name=f"ptg-heartbeat-{run_id[:12]}", daemon=True)
    thread.start()
    return stop_event


def _stop_threaded_ptg_heartbeat(stop_event: threading.Event | None) -> None:
    if stop_event is not None:
        stop_event.set()


async def _stale_ptg_job_result(run_id: str) -> dict[str, Any] | None:
    if not run_id:
        return None
    row = await db.first(
        """
        SELECT ir.status
          FROM mrf.import_run ir
         WHERE ir.run_id = :run_id
         LIMIT 1
        """,
        run_id=run_id,
    )
    if row is None:
        return None
    run_status = str(row[0] or "").strip().lower()
    if run_status in _TERMINAL_RUN_STATUSES:
        return {
            "status": "skipped",
            "run_id": run_id,
            "reason": f"run_{run_status}",
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
    lane_environment_by_name = {
        PTG2_RUST_WORKERS_ENV: _optional_env_value(params.get("_scanner_rust_workers")),
        PTG2_RUST_PARSE_IN_WORKERS_ENV: _bool_env_value(params.get("_scanner_parse_in_workers")),
        PTG2_RUST_TOP_LEVEL_BYTE_SCAN_ENV: _bool_env_value(params.get("_scanner_top_level_byte_scan")),
        PTG2_RUST_WORK_QUEUE_ENV: _optional_env_value(params.get("_scanner_work_queue")),
        PTG2_RUST_EVENT_QUEUE_ENV: _optional_env_value(params.get("_scanner_event_queue")),
        PTG2_RUST_SPLIT_NEGOTIATED_RATES_ENV: _optional_env_value(
            params.get("_scanner_split_negotiated_rates")
        ),
        PTG2_RUST_RAW_CHUNK_BYTES_ENV: _optional_env_value(params.get("_scanner_raw_chunk_bytes")),
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
        PTG2_MANIFEST_MERGE_CHUNK_BYTES_ENV: _optional_env_value(
            params.get("_manifest_merge_chunk_bytes")
        ),
        PTG2_MANIFEST_MERGE_SORT_WORKERS_ENV: _optional_env_value(
            params.get("_manifest_merge_sort_workers")
        ),
        PTG2_FILE_PROCESS_CONCURRENCY_ENV: _optional_env_value(params.get("_file_process_concurrency")),
    }
    previous_environment_by_name: dict[str, str | None] = {}
    try:
        for name, environment_value in lane_environment_by_name.items():
            if environment_value is None:
                continue
            previous_environment_by_name[name] = os.environ.get(name)
            os.environ[name] = environment_value
        yield
    finally:
        for name, environment_value in previous_environment_by_name.items():
            if environment_value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = environment_value


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
        normalized_values = [
            str(item).strip() for item in value if str(item).strip()
        ]
        return normalized_values or None
    return None


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)
