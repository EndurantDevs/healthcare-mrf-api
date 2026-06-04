# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
from typing import Any

from process.control_cancel import ImportCancelledError, raise_if_cancelled
from process.control_lifecycle import mark_control_run
from process.import_status_events import flush_status_events
from process.ptg import main as ptg_main

PTG_CONTROL_QUEUE_NAME = "arq:PTG"


async def ptg_control_start(ctx, task: dict[str, Any] | None = None):
    payload = task if isinstance(task, dict) else {}
    run_id = str(payload.get("run_id") or "").strip()
    params = payload.get("params") if isinstance(payload.get("params"), dict) else payload
    try:
        await mark_control_run(run_id, status="running", phase_detail="ptg import running", progress_message="running")
        await raise_if_cancelled(ctx, payload)
        await ptg_main(
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
    await mark_control_run(run_id, status="succeeded", phase_detail="ptg import succeeded", progress_message="succeeded")
    await _flush_terminal_status_events()
    return {"status": "succeeded", "run_id": run_id}


async def _flush_terminal_status_events() -> None:
    timeout = float(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_TERMINAL_FLUSH_SECONDS", "0.25"))
    if timeout <= 0:
        return
    await flush_status_events(timeout_seconds=timeout)


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
