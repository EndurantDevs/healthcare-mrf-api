# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Asynchronous best-effort status events for import-control."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import time
import urllib.request
from typing import Any

ENGINE_NAME = "healthcare-mrf-api"
TERMINAL_STATUSES = {"succeeded", "failed", "canceled", "cancelled", "dead_letter"}
_TIMESTAMP_KEYS = ("created_at", "started_at", "finished_at", "heartbeat_at")


def isoformat_utc(value: Any) -> Any:
    """Serialize a naive-UTC datetime (or ISO string) as timezone-aware UTC ISO-8601.

    DB columns store naive UTC; consumers (the import-control brain and the UI)
    need an explicit offset, so attach UTC at the serialization boundary.
    """
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return value
        try:
            value = dt.datetime.fromisoformat(raw[:-1] + "+00:00" if raw.endswith("Z") else raw)
        except ValueError:
            return value
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.UTC)
        return value.astimezone(dt.UTC).isoformat()
    return value

_queue: asyncio.Queue[dict[str, Any]] | None = None
_worker: asyncio.Task | None = None
_last_sent_by_run: dict[str, tuple[float, str, str]] = {}


def enqueue_status_event(payload: dict[str, Any]) -> None:
    """Queue a status event without blocking the importer."""
    if not _import_control_url():
        return
    run_id = str(payload.get("run_id") or "").strip()
    if not run_id:
        return
    status = str(payload.get("status") or "").strip()
    phase = str(payload.get("phase_detail") or (payload.get("progress") or {}).get("phase") or "").strip()
    terminal = status in TERMINAL_STATUSES
    now = time.monotonic()
    if not terminal:
        throttle_seconds = _throttle_seconds()
        previous = _last_sent_by_run.get(run_id)
        if previous is not None and previous[1] == status and previous[2] == phase and now - previous[0] < throttle_seconds:
            return
        _last_sent_by_run[run_id] = (now, status, phase)
    event = _event_payload(payload)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    queue = _ensure_queue(loop)
    if queue.full():
        try:
            queue.get_nowait()
            queue.task_done()
        except asyncio.QueueEmpty:
            pass
    try:
        queue.put_nowait(event)
    except asyncio.QueueFull:
        return


async def flush_status_events(timeout_seconds: float = 2.0) -> None:
    queue = _queue
    if queue is None:
        return
    try:
        await asyncio.wait_for(queue.join(), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        return


def _event_payload(payload: dict[str, Any]) -> dict[str, Any]:
    event = {
        "engine": ENGINE_NAME,
        "node_id": os.getenv("HLTHPRT_IMPORT_NODE_ID"),
        **payload,
    }
    for key in _TIMESTAMP_KEYS:
        if event.get(key) is not None:
            event[key] = isoformat_utc(event[key])
    return event


def _ensure_queue(loop: asyncio.AbstractEventLoop) -> asyncio.Queue[dict[str, Any]]:
    global _queue, _worker
    if _queue is None:
        _queue = asyncio.Queue(maxsize=max(int(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_QUEUE_SIZE", "256")), 1))
    if _worker is None or _worker.done():
        _worker = loop.create_task(_publisher_worker(_queue))
    return _queue


async def _publisher_worker(queue: asyncio.Queue[dict[str, Any]]) -> None:
    while True:
        event = await queue.get()
        try:
            await asyncio.to_thread(_post_event, event)
        except Exception:
            pass
        finally:
            queue.task_done()


def _post_event(event: dict[str, Any]) -> None:
    base_url = _import_control_url()
    if not base_url:
        return
    body = json.dumps(event, default=str).encode("utf-8")
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/v1/runs/events",
        data=body,
        method="POST",
        headers={"content-type": "application/json", **_auth_headers()},
    )
    with urllib.request.urlopen(request, timeout=_timeout_seconds()) as response:  # noqa: S310 - internal control URL
        response.read()


def _auth_headers() -> dict[str, str]:
    token = str(os.getenv("HLTHPRT_IMPORT_CONTROL_TOKEN") or os.getenv("HLTHPRT_CONTROL_API_TOKEN") or "").strip()
    return {"Authorization": f"Bearer {token}"} if token else {}


def _import_control_url() -> str:
    return str(os.getenv("HLTHPRT_IMPORT_CONTROL_URL") or os.getenv("HP_IMPORT_CONTROL_BASE_URL") or "").strip()


def _timeout_seconds() -> float:
    return max(float(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_TIMEOUT_SECONDS", "1.0")), 0.1)


def _throttle_seconds() -> float:
    return max(float(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_THROTTLE_SECONDS", "5.0")), 0.0)
