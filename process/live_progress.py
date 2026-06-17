# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Best-effort Redis-backed live progress for controlled imports."""

from __future__ import annotations

import asyncio
import contextvars
import datetime as dt
import json
import math
import os
from typing import Any
from urllib.parse import urlsplit

import redis

from process.import_status_events import enqueue_status_event
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
_redis_client: redis.Redis | None = None
_HEARTBEAT_SOURCE = "import-control-heartbeat"
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


def live_progress_key(run_id: str) -> str:
    return f"import:progress:{run_id}"


def set_live_progress_context(**payload: Any) -> contextvars.Token:
    data = {key: value for key, value in payload.items() if value not in (None, "")}
    return _context.set(data)


def reset_live_progress_context(token: contextvars.Token) -> None:
    _context.reset(token)


def current_live_progress_context() -> dict[str, Any]:
    return dict(_context.get() or {})


def write_live_progress(**payload: Any) -> None:
    context = current_live_progress_context()
    run_id = str(payload.get("run_id") or context.get("run_id") or "").strip()
    if not run_id:
        return

    now = _utc_now()
    merged = {
        "run_id": run_id,
        "importer": payload.get("importer") or context.get("importer") or "unknown",
        "status": payload.get("status") or context.get("status") or "running",
        "source": payload.get("source") or context.get("source") or "import-live-progress",
        "confidence": payload.get("confidence") or context.get("confidence") or "live",
        "updated_at": now.isoformat() + "Z",
        **{key: value for key, value in context.items() if key != "run_id"},
        **{key: value for key, value in payload.items() if value is not None},
    }
    publish_event = bool(merged.pop("publish_event", True))
    previous = _read_live_progress_payload(run_id) if _should_merge_previous(merged) else None
    if previous:
        _preserve_progress_for_heartbeat(merged, previous, now=now)
        for key in ("importer", "source", "confidence"):
            if not merged.get(key) or merged.get(key) == "unknown":
                merged[key] = previous.get(key) or merged.get(key)
        previous_started_at = _parse_datetime(previous.get("started_at"))
        current_started_at = _parse_datetime(merged.get("started_at"))
        if previous_started_at is not None and (
            current_started_at is None or previous_started_at <= current_started_at
        ):
            merged["started_at"] = previous.get("started_at")
    status = str(merged.get("status") or "").lower()
    terminal = status in {"succeeded", "failed", "canceled", "cancelled", "dead_letter"}
    _normalize_progress_fields(merged, terminal=terminal)
    _normalize_estimate_fields(merged, now=now, terminal=terminal)
    if "label" in merged:
        merged["label"] = _safe_label(str(merged["label"]))

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
                "heartbeat_at": merged.get("updated_at"),
            }
        )

    try:
        _redis().setex(live_progress_key(run_id), IMPORT_LIVE_PROGRESS_TTL_SECONDS, json.dumps(merged, default=str))
    except Exception:
        return


def enqueue_live_progress(**payload: Any) -> None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        write_live_progress(**payload)
        return
    try:
        loop.create_task(asyncio.to_thread(write_live_progress, **payload))
    except Exception:
        return


def read_live_progress(run_id: str) -> dict[str, Any] | None:
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
    payload = {
        "unit": live.get("unit") or "run",
        "done": live.get("done"),
        "total": live.get("total"),
        "pct": live.get("pct"),
        "message": live.get("message") or live.get("phase") or "running",
        "phase": live.get("phase"),
        "updated_at": live.get("updated_at"),
    }
    return {key: value for key, value in payload.items() if value is not None}


def estimate_payload_from_live(live: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "eta_seconds": live.get("eta_seconds"),
        "estimated_finish_at": live.get("estimated_finish_at"),
        "confidence": live.get("confidence") or "live",
        "source": live.get("source") or "import-live-progress",
        "updated_at": live.get("updated_at"),
    }
    return {key: value for key, value in payload.items() if value is not None}


def _redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        settings = build_redis_settings()
        _redis_client = redis.Redis(
            host=settings.host,
            port=settings.port,
            password=settings.password,
            db=settings.database,
            socket_connect_timeout=1.0,
            socket_timeout=1.0,
        )
    return _redis_client


def _normalize_progress_fields(merged: dict[str, Any], *, terminal: bool) -> None:
    pct = _coerce_float(merged.get("pct"))
    done = _coerce_float(merged.get("done"))
    total = _coerce_float(merged.get("total"))
    if pct is None and done is not None and total and total > 0:
        pct = (done / total) * 100.0
    if pct is not None:
        merged["pct"] = 100.0 if terminal else max(0.0, min(pct, 99.9))
    if terminal and (merged.get("done") is None) and merged.get("total") is not None:
        merged["done"] = merged.get("total")


def _should_merge_previous(merged: dict[str, Any]) -> bool:
    if merged.get("started_at"):
        return True
    if not merged.get("started_at") and merged.get("done") is not None and merged.get("total") is not None:
        return True
    return str(merged.get("importer") or "") == "unknown"


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
    previous_updated_at = _parse_datetime(previous.get("updated_at"))
    if previous_updated_at is not None:
        age_seconds = (now - previous_updated_at).total_seconds()
        if age_seconds > max(IMPORT_LIVE_PROGRESS_STALE_SECONDS, 1):
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
