# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Asynchronous best-effort events for an optional operator-provided sink."""

from __future__ import annotations

import asyncio
from collections import deque
import datetime as dt
import json
import logging
import os
import threading
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Any

from process.ptg_parts.frozen_rate_privacy import (
    project_frozen_status_event,
)

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
    loop: asyncio.AbstractEventLoop | None = None
    pending: deque[dict[str, Any]] = field(default_factory=deque)
    lock: threading.Lock = field(default_factory=threading.Lock)
    coalesced_by_run: dict[str, dict[str, Any]] = field(default_factory=dict)
    flush_handle_by_run: dict[str, asyncio.TimerHandle] = field(default_factory=dict)
    pending_terminal_events: deque[dict[str, Any]] = field(default_factory=deque)
    terminal_event_by_run: dict[str, dict[str, Any]] = field(default_factory=dict)
    terminal_delivery_by_run: dict[str, asyncio.Future[bool]] = field(
        default_factory=dict
    )


_publisher_state = _PublisherState()
_last_sent_by_run: dict[str, tuple[float, str]] = {}


def bind_status_event_loop() -> None:
    """Bind the publisher to the current loop before worker-thread updates."""

    if not _status_event_url():
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    with _publisher_state.lock:
        owner = _publisher_state.loop
        if owner is not None and owner is not loop and owner.is_running():
            return
    _ensure_queue(loop)


def enqueue_status_event(status_payload: dict[str, Any]) -> None:
    """Queue a status event without blocking the importer."""
    if not _status_event_url():
        return
    run_id = str(status_payload.get("run_id") or "").strip()
    if not run_id:
        return
    event = _event_payload(project_frozen_status_event(status_payload))
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    with _publisher_state.lock:
        bound_loop = _publisher_state.loop
        if bound_loop is not None and not bound_loop.is_running():
            bound_loop = None
        if bound_loop is None and loop is None:
            _append_pending_event_locked(event)
            return
    if bound_loop is None:
        _accept_event_on_loop(loop, event)
        return
    if bound_loop is loop:
        _accept_event_on_loop(bound_loop, event)
        return
    try:
        bound_loop.call_soon_threadsafe(_accept_event_on_loop, bound_loop, event)
    except RuntimeError:
        with _publisher_state.lock:
            _append_pending_event_locked(event)


def _event_stage_signature(event: dict[str, Any]) -> str:
    """Return the fields that are allowed to bypass rate limiting."""

    progress_by_field = event.get("progress")
    if not isinstance(progress_by_field, dict):
        progress_by_field = {}
    return json.dumps(
        {
            "status": event.get("status"),
            "phase": event.get("phase_detail") or progress_by_field.get("phase"),
            "attempt_id": progress_by_field.get("attempt_id"),
            "stage_id": progress_by_field.get("stage_id"),
            "stage_ordinal": progress_by_field.get("stage_ordinal"),
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _append_pending_event_locked(event: dict[str, Any]) -> None:
    pending_limit = max(
        int(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_QUEUE_SIZE", "256")),
        1,
    )
    run_id = str(event.get("run_id") or "").strip()
    if run_id:
        has_pending_terminal = any(
            str(pending.get("run_id") or "").strip() == run_id
            and str(pending.get("status") or "").strip().lower()
            in TERMINAL_STATUSES
            for pending in _publisher_state.pending
        )
        status = str(event.get("status") or "").strip().lower()
        if has_pending_terminal and status not in TERMINAL_STATUSES:
            return
        _publisher_state.pending = deque(
            pending
            for pending in _publisher_state.pending
            if str(pending.get("run_id") or "").strip() != run_id
        )
    while len(_publisher_state.pending) >= pending_limit:
        _publisher_state.pending.popleft()
    _publisher_state.pending.append(event)


def _accept_event_on_loop(
    loop: asyncio.AbstractEventLoop,
    event: dict[str, Any],
) -> None:
    """Rate-limit and enqueue one event on the publisher's owning loop."""

    queue = _ensure_queue(loop)
    run_id = str(event.get("run_id") or "").strip()
    if not run_id:
        return
    status = str(event.get("status") or "").strip().lower()
    if status in TERMINAL_STATUSES:
        _cancel_coalesced(run_id)
        previous_delivery = _publisher_state.terminal_delivery_by_run.get(run_id)
        if previous_delivery is not None and not previous_delivery.done():
            previous_delivery.set_result(False)
        _publisher_state.terminal_event_by_run[run_id] = event
        _publisher_state.terminal_delivery_by_run[run_id] = loop.create_future()
        _publisher_state.pending_terminal_events.append(event)
        _publish_event_now(queue, event)
        return
    if run_id in _publisher_state.terminal_event_by_run:
        return

    now = time.monotonic()
    stage_signature = _event_stage_signature(event)
    previous = _last_sent_by_run.get(run_id)
    throttle_seconds = _throttle_seconds()
    if (
        previous is None
        or previous[1] != stage_signature
        or throttle_seconds <= 0
        or now - previous[0] >= throttle_seconds
    ):
        _cancel_coalesced(run_id)
        _publish_event_now(queue, event, now=now, stage_signature=stage_signature)
        return

    _publisher_state.coalesced_by_run[run_id] = event
    handle = _publisher_state.flush_handle_by_run.get(run_id)
    if handle is None or handle.cancelled():
        remaining = max(throttle_seconds - (now - previous[0]), 0.0)
        _publisher_state.flush_handle_by_run[run_id] = loop.call_later(
            remaining,
            _flush_coalesced_event,
            loop,
            run_id,
        )


def _publish_event_now(
    queue: asyncio.Queue[dict[str, Any]],
    event: dict[str, Any],
    *,
    now: float | None = None,
    stage_signature: str | None = None,
) -> None:
    run_id = str(event.get("run_id") or "").strip()
    if run_id:
        _last_sent_by_run[run_id] = (
            time.monotonic() if now is None else now,
            stage_signature or _event_stage_signature(event),
        )
    _put_event(queue, event)


def _flush_coalesced_event(
    loop: asyncio.AbstractEventLoop,
    run_id: str,
) -> None:
    _publisher_state.flush_handle_by_run.pop(run_id, None)
    event = _publisher_state.coalesced_by_run.pop(run_id, None)
    if event is None:
        return
    queue = _ensure_queue(loop)
    _publish_event_now(queue, event)


def _cancel_coalesced(run_id: str) -> None:
    handle = _publisher_state.flush_handle_by_run.pop(run_id, None)
    if handle is not None:
        handle.cancel()
    _publisher_state.coalesced_by_run.pop(run_id, None)


def _flush_all_coalesced(loop: asyncio.AbstractEventLoop) -> None:
    for run_id in tuple(_publisher_state.coalesced_by_run):
        _cancel_handle = _publisher_state.flush_handle_by_run.pop(run_id, None)
        if _cancel_handle is not None:
            _cancel_handle.cancel()
        event = _publisher_state.coalesced_by_run.pop(run_id, None)
        if event is not None:
            _publish_event_now(_ensure_queue(loop), event)


def _put_event(queue: asyncio.Queue[dict[str, Any]], event: dict[str, Any]) -> None:
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

    if not _status_event_url():
        return
    loop = asyncio.get_running_loop()
    queue = _ensure_queue(loop)
    await asyncio.sleep(0)
    _flush_all_coalesced(loop)
    try:
        await asyncio.wait_for(queue.join(), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        return


async def flush_terminal_status_event(
    run_id: str,
    *,
    timeout_seconds: float | None = None,
) -> None:
    """Wait for one terminal event instead of the publisher's whole backlog."""

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id or not _status_event_url():
        return
    timeout = _terminal_flush_timeout_seconds(timeout_seconds)
    if timeout <= 0:
        return
    _ensure_queue(asyncio.get_running_loop())
    await asyncio.sleep(0)
    delivery = _publisher_state.terminal_delivery_by_run.get(normalized_run_id)
    if delivery is None:
        logger.warning(
            "terminal import status event was not queued run_id=%s",
            normalized_run_id,
        )
        return
    try:
        await asyncio.wait_for(asyncio.shield(delivery), timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(
            "timed out publishing terminal import status event run_id=%s timeout=%.2fs",
            normalized_run_id,
            timeout,
        )


def _event_payload(payload: dict[str, Any]) -> dict[str, Any]:
    event_by_field = {
        "engine": ENGINE_NAME,
        "node_id": os.getenv("HLTHPRT_IMPORT_NODE_ID"),
        **payload,
    }
    for key in _TIMESTAMP_KEYS:
        if event_by_field.get(key) is not None:
            event_by_field[key] = isoformat_utc(event_by_field[key])
    return event_by_field


def _ensure_queue(loop: asyncio.AbstractEventLoop) -> asyncio.Queue[dict[str, Any]]:
    with _publisher_state.lock:
        if _publisher_state.loop is not loop:
            for handle in _publisher_state.flush_handle_by_run.values():
                handle.cancel()
            for delivery in _publisher_state.terminal_delivery_by_run.values():
                if not delivery.done():
                    delivery.cancel()
            _publisher_state.coalesced_by_run.clear()
            _publisher_state.flush_handle_by_run.clear()
            _publisher_state.pending_terminal_events.clear()
            _publisher_state.terminal_event_by_run.clear()
            _publisher_state.terminal_delivery_by_run.clear()
            _publisher_state.loop = loop
            _publisher_state.queue = None
            _publisher_state.worker = None
        if _publisher_state.queue is None:
            _publisher_state.queue = asyncio.Queue(
                maxsize=max(
                    int(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_QUEUE_SIZE", "256")),
                    1,
                )
            )
        queue = _publisher_state.queue
        pending_events = tuple(_publisher_state.pending)
        _publisher_state.pending.clear()
        worker = _publisher_state.worker
        if worker is None or worker.done():
            _publisher_state.worker = loop.create_task(_publisher_worker(queue))
    for event in pending_events:
        loop.call_soon(_accept_event_on_loop, loop, event)
    return queue


async def _publisher_worker(queue: asyncio.Queue[dict[str, Any]]) -> None:
    while True:
        is_from_queue = not _publisher_state.pending_terminal_events
        event = (
            await queue.get()
            if is_from_queue
            else _publisher_state.pending_terminal_events.popleft()
        )
        run_id = str(event.get("run_id") or "").strip()
        terminal_event = _publisher_state.terminal_event_by_run.get(run_id)
        delivery = _publisher_state.terminal_delivery_by_run.get(run_id)
        if terminal_event is not None and (
            event is not terminal_event or (delivery is not None and delivery.done())
        ):
            if is_from_queue:
                queue.task_done()
            continue
        is_terminal = event is terminal_event
        try:
            await asyncio.to_thread(_post_event, event)
        except Exception as exc:
            if is_terminal:
                logger.warning(
                    "failed to publish terminal import status event run_id=%s status=%s: %s",
                    event.get("run_id"),
                    event.get("status"),
                    exc,
                )
                _resolve_terminal_delivery(event, False)
            else:
                logger.debug(
                    "failed to publish import status event run_id=%s status=%s: %s",
                    event.get("run_id"),
                    event.get("status"),
                    exc,
                )
        else:
            if is_terminal:
                _resolve_terminal_delivery(event, True)
        finally:
            if is_from_queue:
                queue.task_done()


def _resolve_terminal_delivery(event: dict[str, Any], delivered: bool) -> None:
    run_id = str(event.get("run_id") or "").strip()
    if _publisher_state.terminal_event_by_run.get(run_id) is not event:
        return
    delivery = _publisher_state.terminal_delivery_by_run.get(run_id)
    if delivery is not None and not delivery.done():
        delivery.set_result(delivered)


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


def _terminal_flush_timeout_seconds(timeout_seconds: float | None) -> float:
    configured = (
        float(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_TERMINAL_FLUSH_SECONDS", "2.25"))
        if timeout_seconds is None
        else float(timeout_seconds)
    )
    if configured <= 0:
        return 0.0
    return max(configured, 2 * _timeout_seconds() + 0.25)


def _throttle_seconds() -> float:
    return max(float(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_THROTTLE_SECONDS", "5.0")), 0.0)
