# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Asynchronous best-effort events for an optional operator-provided sink."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
import time
import urllib.request
from dataclasses import dataclass
from typing import Any

ENGINE_NAME = "healthcare-mrf-api"
TERMINAL_STATUSES = {"succeeded", "failed", "canceled", "cancelled", "dead_letter"}
_TIMESTAMP_KEYS = ("created_at", "started_at", "finished_at", "heartbeat_at")
logger = logging.getLogger(__name__)


def isoformat_utc(value: Any) -> Any:
    """Serialize a naive-UTC datetime (or ISO string) as timezone-aware UTC ISO-8601.

    DB columns store naive UTC. Attach UTC at the serialization boundary so
    generic event consumers do not have to infer the storage convention.
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

@dataclass
class _PublisherState:
    queue: asyncio.Queue[dict[str, Any]] | None = None
    worker: asyncio.Task | None = None


_publisher_state = _PublisherState()
_last_sent_by_run: dict[str, tuple[float, str, str]] = {}


def enqueue_status_event(payload: dict[str, Any]) -> None:
    """Queue a status event without blocking the importer."""
    if not _status_event_url():
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
            logger.debug("status event queue drained before oldest-event eviction")
    try:
        queue.put_nowait(event)
    except asyncio.QueueFull:
        return


async def flush_status_events(timeout_seconds: float = 2.0) -> None:
    """Wait briefly for queued status events without blocking shutdown forever."""

    queue = _publisher_state.queue
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
    if _publisher_state.queue is None:
        _publisher_state.queue = asyncio.Queue(
            maxsize=max(int(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_QUEUE_SIZE", "256")), 1)
        )
    if _publisher_state.worker is None or _publisher_state.worker.done():
        _publisher_state.worker = loop.create_task(
            _publisher_worker(_publisher_state.queue)
        )
    return _publisher_state.queue


async def _publisher_worker(queue: asyncio.Queue[dict[str, Any]]) -> None:
    while True:
        event = await queue.get()
        try:
            await asyncio.to_thread(_post_event, event)
        except Exception as exc:
            logger.debug(
                "failed to publish import status event run_id=%s status=%s: %s",
                event.get("run_id"),
                event.get("status"),
                exc,
            )
        finally:
            queue.task_done()


def _post_event(event: dict[str, Any]) -> None:
    event_url = _status_event_url()
    if not event_url:
        return
    body = json.dumps(event, default=str).encode("utf-8")
    request = urllib.request.Request(
        event_url,
        data=body,
        method="POST",
        headers={"content-type": "application/json", **_auth_headers()},
    )
    with urllib.request.urlopen(request, timeout=_timeout_seconds()) as response:
        response.read()


def _auth_headers() -> dict[str, str]:
    token = str(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_TOKEN") or "").strip()
    return {"Authorization": f"Bearer {token}"} if token else {}


def _status_event_url() -> str:
    return str(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_URL") or "").strip()


def _timeout_seconds() -> float:
    return max(float(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_TIMEOUT_SECONDS", "1.0")), 0.1)


def _throttle_seconds() -> float:
    return max(float(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_THROTTLE_SECONDS", "5.0")), 0.0)
