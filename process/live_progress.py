# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Best-effort Redis-backed live progress for controlled imports."""

from __future__ import annotations

import asyncio
import contextvars
import datetime as dt
import json
import math
import os
import threading
from collections import OrderedDict
from functools import lru_cache
from typing import Any
from urllib.parse import urlsplit

import redis

from process.import_status_events import bind_status_event_loop, enqueue_status_event
from process.redis_config import build_redis_settings

IMPORT_LIVE_PROGRESS_TTL_SECONDS = int(
    os.getenv(
        "HLTHPRT_IMPORT_LIVE_PROGRESS_TTL_SECONDS",
        os.getenv("HLTHPRT_PTG_LIVE_PROGRESS_TTL_SECONDS", str(2 * 24 * 60 * 60)),
    )
)
IMPORT_LIVE_PROGRESS_STALE_SECONDS = int(
    os.getenv(
        "HLTHPRT_IMPORT_LIVE_PROGRESS_STALE_SECONDS",
        os.getenv("HLTHPRT_PTG_LIVE_PROGRESS_STALE_SECONDS", "90"),
    )
)

_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar("import_live_progress_context", default={})
_PROGRESS_WRITE_LOCK_COUNT = 64
_progress_write_locks = tuple(
    threading.Lock() for _ in range(_PROGRESS_WRITE_LOCK_COUNT)
)
_SEQUENCE_CACHE_MAX_PER_STRIPE = 128
_event_sequences = tuple(
    OrderedDict() for _ in range(_PROGRESS_WRITE_LOCK_COUNT)
)
_progress_sequences = tuple(
    OrderedDict() for _ in range(_PROGRESS_WRITE_LOCK_COUNT)
)
_HEARTBEAT_SOURCE = "engine-heartbeat"
_PROGRESS_FIELDS = (
    "unit",
    "done",
    "total",
    "pct",
    "message",
    "phase",
    "step",
    "label",
    "eta_seconds",
    "estimated_finish_at",
)
_RICH_PROGRESS_FIELDS = (
    "attempt_id",
    "attempt_started_at",
    "event_seq",
    "progress_seq",
    "stage_id",
    "stage_ordinal",
    "stage_pct",
    "phase_pct",
    "basis",
    "denominator_state",
    "file_index",
    "file_count",
    "file_name",
    "counters",
    "throughput",
    "elapsed_seconds",
    "work_done",
    "work_total",
    "file_weight",
    "file_weight_pct",
    "dominant_file",
    "observed_at",
    "progressed_at",
)
_CARRY_FORWARD_FIELDS = (*_PROGRESS_FIELDS, *_RICH_PROGRESS_FIELDS)
_PROGRESS_SNAPSHOT_FIELDS = tuple(
    key
    for key in _CARRY_FORWARD_FIELDS
    if key not in {"event_seq", "observed_at"}
)


def live_progress_key(run_id: str) -> str:
    """Return the Redis key for one controlled import's live progress."""

    return f"import:progress:{run_id}"


def set_live_progress_context(**payload: Any) -> contextvars.Token:
    """Bind nonempty progress defaults to the current asynchronous context."""

    data = {
        **current_live_progress_context(),
        **{
            key: value
            for key, value in payload.items()
            if value not in (None, "")
        },
    }
    return _context.set(data)


def reset_live_progress_context(token: contextvars.Token) -> None:
    """Restore the progress context represented by a prior context token."""

    _context.reset(token)


def current_live_progress_context() -> dict[str, Any]:
    """Return a detached copy of the current import progress context."""

    return dict(_context.get() or {})


def write_live_progress(**payload: Any) -> None:
    """Persist normalized progress and emit its best-effort status event."""

    context = current_live_progress_context()
    run_id = str(payload.get("run_id") or context.get("run_id") or "").strip()
    if not run_id:
        return

    now = _utc_now()
    observed_at = now.isoformat() + "Z"
    with _progress_lock_for(run_id):
        merged = {
            "run_id": run_id,
            "attempt_id": payload.get("attempt_id") or context.get("attempt_id") or run_id,
            "importer": payload.get("importer") or context.get("importer") or "unknown",
            "status": payload.get("status") or context.get("status") or "running",
            "source": payload.get("source") or context.get("source") or "import-live-progress",
            "confidence": payload.get("confidence") or context.get("confidence") or "live",
            "updated_at": observed_at,
            "observed_at": observed_at,
            **{key: value for key, value in context.items() if key != "run_id"},
            **{key: value for key, value in payload.items() if value is not None},
        }
        if not merged.get("attempt_started_at") and merged.get("started_at"):
            merged["attempt_started_at"] = merged["started_at"]
        publish_event = bool(merged.pop("publish_event", True))
        previous = _read_live_progress_payload(run_id)
        if previous:
            _merge_previous_progress(merged, previous, now=now)
        status = str(merged.get("status") or "").lower()
        terminal = status in {"succeeded", "failed", "canceled", "cancelled", "dead_letter"}
        succeeded = status == "succeeded"
        _normalize_progress_fields(merged, succeeded=succeeded)
        _normalize_estimate_fields(merged, now=now, terminal=terminal)
        _sequence_progress(merged, previous, now=now, succeeded=succeeded)
        if "label" in merged:
            merged["label"] = _safe_label(str(merged["label"]))
        try:
            _redis().setex(
                live_progress_key(run_id),
                IMPORT_LIVE_PROGRESS_TTL_SECONDS,
                json.dumps(merged, default=str),
            )
        except Exception:
            pass

    if publish_event:
        enqueue_status_event(
            {
                "run_id": run_id,
                "importer": merged.get("importer"),
                "status": merged.get("status") or "running",
                "phase_detail": str(merged.get("phase") or "")[:128] or None,
                "progress": progress_payload_from_live(merged),
                "estimate": estimate_payload_from_live(merged),
                "snapshot_id": merged.get("snapshot_id"),
                "heartbeat_at": merged.get("observed_at") or merged.get("updated_at"),
            }
        )


def enqueue_live_progress(**payload: Any) -> None:
    """Schedule a nonblocking live-progress write from sync or async callers."""

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        write_live_progress(**payload)
        return
    bind_status_event_loop()
    try:
        loop.create_task(asyncio.to_thread(write_live_progress, **payload))
    except Exception:
        return


def read_live_progress(run_id: str) -> dict[str, Any] | None:
    """Return fresh persisted progress, excluding missing or stale payloads."""

    run_id = str(run_id or "").strip()
    if not run_id:
        return None
    payload = _read_live_progress_payload(run_id)
    if payload is None:
        return None
    updated_at = _parse_datetime(payload.get("updated_at"))
    if updated_at is not None:
        age = (_utc_now() - updated_at).total_seconds()
        if age > max(IMPORT_LIVE_PROGRESS_STALE_SECONDS, 1):
            return None
    return payload


def _read_live_progress_payload(run_id: str) -> dict[str, Any] | None:
    try:
        raw = _redis().get(live_progress_key(run_id))
    except Exception:
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def progress_payload_from_live(live: dict[str, Any]) -> dict[str, Any]:
    """Project live state into the stable control-plane progress contract."""

    payload = {
        "unit": live.get("unit") or "run",
        "done": live.get("done"),
        "total": live.get("total"),
        "pct": live.get("pct"),
        "message": live.get("message") or live.get("phase") or "running",
        "phase": live.get("phase"),
        "detail": live.get("detail"),
        "updated_at": live.get("updated_at"),
        **{key: live.get(key) for key in _RICH_PROGRESS_FIELDS},
    }
    return {key: value for key, value in payload.items() if value is not None}


def estimate_payload_from_live(live: dict[str, Any]) -> dict[str, Any]:
    """Project live state into the stable runtime-estimate contract."""

    payload = {
        "eta_seconds": live.get("eta_seconds"),
        "estimated_finish_at": live.get("estimated_finish_at"),
        "confidence": live.get("confidence") or "live",
        "source": live.get("source") or "import-live-progress",
        "updated_at": live.get("updated_at"),
    }
    return {key: value for key, value in payload.items() if value is not None}


@lru_cache(maxsize=1)
def _redis() -> redis.Redis:
    settings = build_redis_settings()
    return redis.Redis(
        host=settings.host,
        port=settings.port,
        password=settings.password,
        db=settings.database,
        socket_connect_timeout=1.0,
        socket_timeout=1.0,
    )


def _normalize_progress_fields(merged: dict[str, Any], *, succeeded: bool) -> None:
    pct = _coerce_float(merged.get("pct"))
    done = _coerce_float(merged.get("done"))
    total = _coerce_float(merged.get("total"))
    if pct is None and done is not None and total and total > 0:
        pct = (done / total) * 100.0
    if pct is not None:
        merged["pct"] = 100.0 if succeeded else max(0.0, min(pct, 99.9))
    if succeeded and (merged.get("done") is None) and merged.get("total") is not None:
        merged["done"] = merged.get("total")


def _merge_previous_progress(
    merged: dict[str, Any],
    previous: dict[str, Any],
    *,
    now: dt.datetime,
) -> None:
    """Carry forward rich progress while accepting a new observation."""

    incoming_snapshot = dict(merged)
    _preserve_progress_for_heartbeat(merged, previous, now=now)
    if _incoming_progress_is_older(incoming_snapshot, previous):
        _preserve_progress_snapshot(merged, previous)
    for key in _CARRY_FORWARD_FIELDS:
        if merged.get(key) is None and previous.get(key) is not None:
            merged[key] = previous[key]
    for key in ("importer", "source", "confidence"):
        if not merged.get(key) or merged.get(key) == "unknown":
            merged[key] = previous.get(key) or merged.get(key)
    previous_started_at = _parse_datetime(previous.get("started_at"))
    current_started_at = _parse_datetime(merged.get("started_at"))
    if previous_started_at is not None and (
        current_started_at is None or previous_started_at <= current_started_at
    ):
        merged["started_at"] = previous.get("started_at")


def _incoming_progress_is_older(
    incoming: dict[str, Any],
    previous: dict[str, Any],
) -> bool:
    """Fence delayed concurrent callbacks without hiding new observations."""

    incoming_attempt = str(incoming.get("attempt_id") or "")
    previous_attempt = str(previous.get("attempt_id") or "")
    if incoming_attempt and previous_attempt and incoming_attempt != previous_attempt:
        return False
    incoming_stage = _coerce_int(incoming.get("stage_ordinal"))
    previous_stage = _coerce_int(previous.get("stage_ordinal"))
    if (
        incoming_stage is not None
        and previous_stage is not None
        and incoming_stage != previous_stage
    ):
        return incoming_stage < previous_stage
    incoming_progress_seq = _coerce_int(incoming.get("progress_seq"))
    previous_progress_seq = _coerce_int(previous.get("progress_seq"))
    if (
        incoming_progress_seq is not None
        and previous_progress_seq is not None
        and incoming_progress_seq != previous_progress_seq
    ):
        return incoming_progress_seq < previous_progress_seq
    incoming_pct = _coerce_float(incoming.get("pct"))
    previous_pct = _coerce_float(previous.get("pct"))
    if (
        incoming_pct is not None
        and previous_pct is not None
        and incoming_pct < previous_pct
    ):
        return True
    incoming_stage_pct = _coerce_float(incoming.get("stage_pct"))
    previous_stage_pct = _coerce_float(previous.get("stage_pct"))
    return bool(
        incoming_stage_pct is not None
        and previous_stage_pct is not None
        and incoming_stage_pct < previous_stage_pct
    )


def _preserve_progress_snapshot(
    merged: dict[str, Any],
    previous: dict[str, Any],
) -> None:
    for key in _PROGRESS_SNAPSHOT_FIELDS:
        if previous.get(key) is not None:
            merged[key] = previous[key]
        else:
            merged.pop(key, None)


def _sequence_progress(
    merged: dict[str, Any],
    previous: dict[str, Any] | None,
    *,
    now: dt.datetime,
    succeeded: bool,
) -> None:
    """Assign monotonic observation and movement sequences for one run."""

    previous = previous or {}
    run_id = str(merged.get("run_id") or "")
    event_sequences = _sequence_cache_for(_event_sequences, run_id)
    previous_event_seq = max(
        _coerce_int(previous.get("event_seq")) or 0,
        event_sequences.get(run_id, 0),
    )
    requested_event_seq = _coerce_int(merged.get("event_seq")) or 0
    merged["event_seq"] = max(previous_event_seq + 1, requested_event_seq)
    _remember_sequence(event_sequences, run_id, merged["event_seq"])
    progress_sequences = _sequence_cache_for(_progress_sequences, run_id)
    previous_progress_seq = max(
        _coerce_int(previous.get("progress_seq")) or 0,
        progress_sequences.get(run_id, 0),
    )
    requested_progress_seq = _coerce_int(merged.get("progress_seq")) or 0
    movement = succeeded or (
        str(merged.get("source") or "") != _HEARTBEAT_SOURCE
        and _movement_signature(merged) != _movement_signature(previous)
    )
    if movement:
        merged["progress_seq"] = max(
            previous_progress_seq + 1,
            requested_progress_seq,
        )
        merged["progressed_at"] = now.isoformat() + "Z"
    else:
        merged["progress_seq"] = previous_progress_seq
        progressed_at = previous.get("progressed_at")
        if progressed_at is not None:
            merged["progressed_at"] = progressed_at
    _remember_sequence(progress_sequences, run_id, merged["progress_seq"])


def _movement_signature(progress: dict[str, Any]) -> str:
    fields = {
        key: progress.get(key)
        for key in (
            "stage_id",
            "stage_ordinal",
            "stage_pct",
            "phase_pct",
            "unit",
            "done",
            "total",
            "pct",
            "basis",
            "file_index",
            "file_count",
            "file_name",
            "counters",
        )
        if progress.get(key) is not None
    }
    return json.dumps(fields, sort_keys=True, separators=(",", ":"), default=str)


def _preserve_progress_for_heartbeat(
    merged: dict[str, Any],
    previous: dict[str, Any],
    *,
    now: dt.datetime,
) -> None:
    if str(merged.get("source") or "") != _HEARTBEAT_SOURCE:
        return
    if str(previous.get("source") or "") in {"", _HEARTBEAT_SOURCE}:
        return
    for key in _PROGRESS_FIELDS:
        if previous.get(key) is not None:
            merged[key] = previous[key]
    for key in ("source", "confidence"):
        if previous.get(key):
            merged[key] = previous[key]


def _normalize_estimate_fields(merged: dict[str, Any], *, now: dt.datetime, terminal: bool) -> None:
    eta_seconds = _coerce_float(merged.get("eta_seconds"))
    if eta_seconds is None and not terminal:
        done = _coerce_float(merged.get("done"))
        total = _coerce_float(merged.get("total"))
        elapsed = _coerce_float(merged.get("elapsed_seconds"))
        if elapsed is None:
            started_at = _parse_datetime(merged.get("started_at"))
            if started_at is not None:
                elapsed = max((now - started_at).total_seconds(), 0.0)
        if done is not None and total is not None and total > done > 0 and elapsed and elapsed > 0:
            eta_seconds = (total - done) * (elapsed / done)
    if eta_seconds is not None and eta_seconds >= 0 and math.isfinite(eta_seconds):
        merged["eta_seconds"] = eta_seconds
        merged["estimated_finish_at"] = (now + dt.timedelta(seconds=eta_seconds)).isoformat() + "Z"


def _safe_label(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme and parsed.netloc:
        path_tail = parsed.path.rsplit("/", 1)[-1]
        return f"{parsed.netloc}/{path_tail}" if path_tail else parsed.netloc
    return value[:256]


def _coerce_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _coerce_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _progress_lock_for(run_id: str) -> threading.Lock:
    return _progress_write_locks[hash(run_id) % len(_progress_write_locks)]


def _sequence_cache_for(
    caches: tuple[OrderedDict[str, int], ...],
    run_id: str,
) -> OrderedDict[str, int]:
    """Return sequence state guarded by the run's striped progress lock."""

    return caches[hash(run_id) % len(caches)]


def _remember_sequence(
    cache: OrderedDict[str, int],
    run_id: str,
    value: int,
) -> None:
    """Bound best-effort outage continuity without growing per-process state forever."""

    cache[run_id] = value
    cache.move_to_end(run_id)
    while len(cache) > _SEQUENCE_CACHE_MAX_PER_STRIPE:
        cache.popitem(last=False)


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC).replace(tzinfo=None)


def _parse_datetime(value: Any) -> dt.datetime | None:
    if isinstance(value, dt.datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed.astimezone(dt.UTC).replace(tzinfo=None) if parsed.tzinfo else parsed
