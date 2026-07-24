# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Best-effort Redis-backed live progress for controlled imports."""

from __future__ import annotations

import asyncio
import contextvars
import datetime as dt
import json
import logging
import math
import os
import threading
import time
import uuid
from collections import OrderedDict
from functools import lru_cache
from typing import Any
from urllib.parse import urlsplit

import redis

from process.import_status_events import bind_status_event_loop, enqueue_status_event
from process.redis_config import build_redis_settings

logger = logging.getLogger(__name__)

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
    "stage_pct_lower_bound",
    "pct_lower_bound",
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
    "weighted_compressed_input_bytes_lower_bound",
    "observed_at",
    "progressed_at",
)
_CARRY_FORWARD_FIELDS = (*_PROGRESS_FIELDS, *_RICH_PROGRESS_FIELDS)
_PROGRESS_SNAPSHOT_FIELDS = tuple(
    key
    for key in _CARRY_FORWARD_FIELDS
    if key not in {"event_seq", "observed_at"}
)
_ATTEMPT_CURRENT = "current"
_ATTEMPT_NEWER = "newer"
_ATTEMPT_REJECT = "reject"
_ATTEMPT_SEQUENCE_FIELDS = (
    "event_seq",
    "progress_seq",
    "progressed_at",
)
_PROGRESS_CAS_RETRIES = 8
_PROGRESS_LOCK_WAIT_SECONDS = 0.25
_PROGRESS_LOCK_LEASE_MILLISECONDS = 5_000
_PROGRESS_CAS_SCRIPT = """
local current = redis.call("GET", KEYS[1])
local expected_missing = ARGV[1]
local expected = ARGV[2]
if expected_missing == "1" then
    if current then
        return 0
    end
elseif (not current) or current ~= expected then
    return 0
end
redis.call("SETEX", KEYS[1], tonumber(ARGV[3]), ARGV[4])
return 1
"""
_PROGRESS_UNLOCK_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
"""


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


def is_live_progress_written(**progress_by_field: Any) -> bool:
    """Atomically persist progress and enqueue its event in attempt order."""

    context = current_live_progress_context()
    run_id = str(
        progress_by_field.get("run_id") or context.get("run_id") or ""
    ).strip()
    if not run_id:
        return False

    now = _utc_now()
    observed_at = now.isoformat() + "Z"
    status_event_payload = progress_by_field.pop("status_event_payload", None)
    with _progress_lock_for(run_id):
        try:
            redis_client = _redis()
            lock_token = _acquire_progress_publication_lock(
                redis_client,
                run_id,
            )
        except Exception:
            logger.debug(
                "Unable to acquire atomic live-progress publication lock",
                exc_info=True,
            )
            return False
        if lock_token is None:
            return False
        try:
            return _is_live_progress_written_with_cas(
                redis_client=redis_client,
                run_id=run_id,
                context=context,
                progress_by_field=progress_by_field,
                observed_at=observed_at,
                now=now,
                status_event_payload=(
                    status_event_payload
                    if isinstance(status_event_payload, dict)
                    else None
                ),
            )
        finally:
            _release_progress_publication_lock(
                redis_client,
                run_id,
                lock_token,
            )


write_live_progress = is_live_progress_written


def _is_live_progress_written_with_cas(
    *,
    redis_client: redis.Redis,
    run_id: str,
    context: dict[str, Any],
    progress_by_field: dict[str, Any],
    observed_at: str,
    now: dt.datetime,
    status_event_payload: dict[str, Any] | None,
) -> bool:
    """Compare-and-swap one snapshot while its event-order lock is held."""

    key = live_progress_key(run_id)
    for _attempt in range(_PROGRESS_CAS_RETRIES):
        try:
            previous_raw = redis_client.get(key)
        except Exception:
            logger.debug("Unable to read live progress for CAS", exc_info=True)
            return False
        previous = _decode_live_progress_payload(previous_raw)
        merged = _merged_live_progress_candidate(
            run_id=run_id,
            context=context,
            progress_by_field=progress_by_field,
            observed_at=observed_at,
            now=now,
            previous=previous,
        )
        if merged is None:
            return False
        candidate = json.dumps(merged, default=str)
        try:
            accepted = redis_client.eval(
                _PROGRESS_CAS_SCRIPT,
                1,
                key,
                "1" if previous_raw is None else "0",
                previous_raw or b"",
                IMPORT_LIVE_PROGRESS_TTL_SECONDS,
                candidate,
            )
        except Exception:
            logger.debug("Unable to CAS live progress", exc_info=True)
            return False
        if int(accepted or 0) != 1:
            continue
        publish_event = bool(progress_by_field.get("publish_event", True))
        if status_event_payload is not None:
            enqueue_status_event(
                _status_event_for_accepted_progress(
                    status_event_payload,
                    merged,
                )
            )
        elif publish_event:
            enqueue_status_event(_default_status_event(merged))
        return True
    logger.debug("Live progress CAS contention exceeded bounded retry budget")
    return False


def _write_live_progress_with_cas(**arguments_by_name: Any):
    """Preserve the internal CAS seam used by concurrency regression tests."""

    progress_by_field = arguments_by_name.pop("payload")
    is_written = _is_live_progress_written_with_cas(
        progress_by_field=progress_by_field,
        **arguments_by_name,
    )
    return is_written


def _merged_live_progress_candidate(
    *,
    run_id: str,
    context: dict[str, Any],
    progress_by_field: dict[str, Any],
    observed_at: str,
    now: dt.datetime,
    previous: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Merge one observation with the last snapshot for the same attempt."""

    candidate_by_field = _new_live_progress_candidate_by_field(
        run_id=run_id,
        context=context,
        progress_by_field=progress_by_field,
        observed_at=observed_at,
    )
    sequence_previous = previous
    if previous:
        attempt_disposition = _merge_previous_progress(
            candidate_by_field,
            previous,
            now=now,
        )
        if attempt_disposition == _ATTEMPT_REJECT:
            return None
        if attempt_disposition == _ATTEMPT_NEWER:
            _reset_attempt_sequences(run_id)
            sequence_previous = None
    status = str(candidate_by_field.get("status") or "").lower()
    is_terminal = status in {
        "succeeded",
        "failed",
        "canceled",
        "cancelled",
        "dead_letter",
    }
    has_succeeded = status == "succeeded"
    if not is_terminal:
        _clear_indeterminate_semantic_progress_fields(candidate_by_field)
    _normalize_progress_fields(candidate_by_field, succeeded=has_succeeded)
    _normalize_estimate_fields(
        candidate_by_field,
        now=now,
        terminal=is_terminal,
    )
    _sequence_progress(
        candidate_by_field,
        sequence_previous,
        now=now,
        succeeded=has_succeeded,
    )
    if "label" in candidate_by_field:
        candidate_by_field["label"] = _safe_label(
            str(candidate_by_field["label"])
        )
    return candidate_by_field


def _clear_indeterminate_semantic_progress_fields(
    candidate_by_field: dict[str, Any],
) -> None:
    """Prevent stale bounded fields from masking current lower-bound work."""

    progress_basis = str(
        candidate_by_field.get("basis")
        or candidate_by_field.get("unit")
        or ""
    ).strip().lower()
    denominator_state = str(
        candidate_by_field.get("denominator_state") or ""
    ).strip().lower()
    if (
        progress_basis != "semantic_work"
        or denominator_state not in {"unknown", "lower_bound"}
    ):
        return
    for field_name in (
        "pct",
        "stage_pct",
        "phase_pct",
        "total",
        "work_total",
        "eta_seconds",
        "estimated_finish_at",
    ):
        candidate_by_field.pop(field_name, None)


def _new_live_progress_candidate_by_field(
    *,
    run_id: str,
    context: dict[str, Any],
    progress_by_field: dict[str, Any],
    observed_at: str,
) -> dict[str, Any]:
    candidate_by_field = {
        "run_id": run_id,
        "attempt_id": (
            progress_by_field.get("attempt_id")
            or context.get("attempt_id")
            or run_id
        ),
        "importer": (
            progress_by_field.get("importer")
            or context.get("importer")
            or "unknown"
        ),
        "status": (
            progress_by_field.get("status")
            or context.get("status")
            or "running"
        ),
        "source": (
            progress_by_field.get("source")
            or context.get("source")
            or "import-live-progress"
        ),
        "confidence": (
            progress_by_field.get("confidence")
            or context.get("confidence")
            or "live"
        ),
        "updated_at": observed_at,
        "observed_at": observed_at,
        **{
            field_name: field_value
            for field_name, field_value in context.items()
            if field_name != "run_id"
        },
        **{
            field_name: field_value
            for field_name, field_value in progress_by_field.items()
            if field_value is not None and field_name != "publish_event"
        },
    }
    if (
        not candidate_by_field.get("attempt_started_at")
        and candidate_by_field.get("started_at")
    ):
        candidate_by_field["attempt_started_at"] = candidate_by_field[
            "started_at"
        ]
    return candidate_by_field


def _default_status_event(merged: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": merged["run_id"],
        "importer": merged.get("importer"),
        "status": merged.get("status") or "running",
        "phase_detail": str(merged.get("phase") or "")[:128] or None,
        "progress": progress_payload_from_live(merged),
        "estimate": estimate_payload_from_live(merged),
        "snapshot_id": merged.get("snapshot_id"),
        "heartbeat_at": merged.get("observed_at") or merged.get("updated_at"),
    }


def _status_event_for_accepted_progress(
    status_event_payload: dict[str, Any],
    merged: dict[str, Any],
) -> dict[str, Any]:
    event_by_field = dict(status_event_payload)
    progress_by_field = dict(event_by_field.get("progress") or {})
    progress_by_field["attempt_id"] = merged.get("attempt_id")
    progress_by_field["attempt_started_at"] = merged.get("attempt_started_at")
    progress_by_field["event_seq"] = merged.get("event_seq")
    progress_by_field["progress_seq"] = merged.get("progress_seq")
    event_by_field["progress"] = progress_by_field
    event_by_field["heartbeat_at"] = (
        merged.get("observed_at")
        or merged.get("updated_at")
        or event_by_field.get("heartbeat_at")
    )
    return event_by_field


def _progress_publication_lock_key(run_id: str) -> str:
    return f"import:progress-lock:{run_id}"


def _acquire_progress_publication_lock(
    redis_client: redis.Redis,
    run_id: str,
) -> str | None:
    token = uuid.uuid4().hex
    deadline = time.monotonic() + _PROGRESS_LOCK_WAIT_SECONDS
    key = _progress_publication_lock_key(run_id)
    while True:
        if redis_client.set(
            key,
            token,
            nx=True,
            px=_PROGRESS_LOCK_LEASE_MILLISECONDS,
        ):
            return token
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.002)


def _release_progress_publication_lock(
    redis_client: redis.Redis,
    run_id: str,
    token: str,
) -> None:
    try:
        redis_client.eval(
            _PROGRESS_UNLOCK_SCRIPT,
            1,
            _progress_publication_lock_key(run_id),
            token,
        )
    except Exception:
        logger.debug("Unable to release live-progress publication lock", exc_info=True)


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
    return _decode_live_progress_payload(raw)


def _decode_live_progress_payload(raw: Any) -> dict[str, Any] | None:
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
) -> str:
    """Carry forward rich progress while accepting a new observation."""

    attempt_disposition = _attempt_disposition(merged, previous)
    if attempt_disposition == _ATTEMPT_REJECT:
        return attempt_disposition
    if attempt_disposition == _ATTEMPT_NEWER:
        for key in _ATTEMPT_SEQUENCE_FIELDS:
            merged.pop(key, None)
        _carry_forward_run_metadata(merged, previous)
        return attempt_disposition

    incoming_by_field = dict(merged)
    _preserve_progress_for_heartbeat(merged, previous, now=now)
    if _is_incoming_progress_older(incoming_by_field, previous):
        _preserve_progress_snapshot(merged, previous)
    for key in _CARRY_FORWARD_FIELDS:
        if merged.get(key) is None and previous.get(key) is not None:
            merged[key] = previous[key]
    _carry_forward_run_metadata(merged, previous)
    return attempt_disposition


def _attempt_disposition(
    incoming: dict[str, Any],
    previous: dict[str, Any],
) -> str:
    """Order attempt identities only when their start timestamps prove it."""

    incoming_attempt = str(incoming.get("attempt_id") or "").strip()
    previous_attempt = str(previous.get("attempt_id") or "").strip()
    incoming_started_at = _attempt_started_at(incoming)
    previous_started_at = _attempt_started_at(previous)
    run_id = str(incoming.get("run_id") or previous.get("run_id") or "").strip()
    if incoming_attempt == previous_attempt:
        # Legacy callbacks used run_id for every retry. Compare their attempt
        # timestamps before treating equal fallback IDs as the same execution.
        if incoming_started_at is not None and previous_started_at is not None:
            if incoming_started_at > previous_started_at:
                return _ATTEMPT_NEWER
            if incoming_started_at < previous_started_at:
                return _ATTEMPT_REJECT
        elif incoming_started_at is None and previous_started_at is not None:
            return _ATTEMPT_REJECT
        return _ATTEMPT_CURRENT

    if incoming_started_at is None:
        return _ATTEMPT_REJECT
    if previous_started_at is None:
        # A timestamped attempt may supersede a legacy row that had only the
        # run-id fallback. A malformed timestamp on a named attempt is not
        # enough evidence to let a different attempt replace it.
        return (
            _ATTEMPT_NEWER
            if not previous_attempt or previous_attempt == run_id
            else _ATTEMPT_REJECT
        )
    if incoming_started_at > previous_started_at:
        return _ATTEMPT_NEWER
    if incoming_started_at < previous_started_at:
        return _ATTEMPT_REJECT
    # Thread callbacks that lack context use run_id as their attempt fallback.
    # Accept that alias only when its exact timestamp proves the same attempt.
    if run_id and run_id in {incoming_attempt, previous_attempt}:
        return _ATTEMPT_CURRENT
    return _ATTEMPT_REJECT


def _attempt_started_at(progress: dict[str, Any]) -> dt.datetime | None:
    """Parse an attempt timestamp, falling back only for legacy missing fields."""

    attempt_started_at = progress.get("attempt_started_at")
    if attempt_started_at not in (None, ""):
        return _parse_datetime(attempt_started_at)
    return _parse_datetime(progress.get("started_at"))


def _carry_forward_run_metadata(
    merged: dict[str, Any],
    previous: dict[str, Any],
) -> None:
    """Retain run-level identity without leaking prior-attempt progress."""

    for key in ("importer", "source", "confidence"):
        if not merged.get(key) or merged.get(key) == "unknown":
            merged[key] = previous.get(key) or merged.get(key)
    previous_started_at = _parse_datetime(previous.get("started_at"))
    current_started_at = _parse_datetime(merged.get("started_at"))
    if previous_started_at is not None and (
        current_started_at is None or previous_started_at <= current_started_at
    ):
        merged["started_at"] = previous.get("started_at")


def _is_incoming_progress_older(
    incoming: dict[str, Any],
    previous: dict[str, Any],
) -> bool:
    """Fence delayed concurrent callbacks without hiding new observations."""

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
    fields_by_name = {
        key: progress.get(key)
        for key in (
            "stage_id",
            "stage_ordinal",
            "stage_pct",
            "stage_pct_lower_bound",
            "phase_pct",
            "unit",
            "done",
            "total",
            "pct",
            "pct_lower_bound",
            "basis",
            "file_index",
            "file_count",
            "file_name",
            "counters",
            "weighted_compressed_input_bytes_lower_bound",
        )
        if progress.get(key) is not None
    }
    return json.dumps(
        fields_by_name,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


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
            started_at = _parse_datetime(merged.get("attempt_started_at"))
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


def _reset_attempt_sequences(run_id: str) -> None:
    """Discard process-local sequence continuity from the prior attempt."""

    _sequence_cache_for(_event_sequences, run_id).pop(run_id, None)
    _sequence_cache_for(_progress_sequences, run_id).pop(run_id, None)


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
