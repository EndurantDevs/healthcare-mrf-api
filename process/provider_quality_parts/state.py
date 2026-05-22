# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Redis-backed run and materialization state helpers."""

from __future__ import annotations

import time
from typing import Any

from process.provider_quality_parts.config import (
    MAT_PHASE_SEQUENCE,
    PROVIDER_QUALITY_REDIS_TTL_SECONDS,
)


def _safe_int(raw: Any, default: int = 0) -> int:
    if raw is None:
        return default
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _state_key(run_id: str, suffix: str) -> str:
    return f"provider_quality:{run_id}:{suffix}"


def _mat_phase_key(run_id: str) -> str:
    return _state_key(run_id, "mat_phase")


def _mat_total_key(run_id: str) -> str:
    return _state_key(run_id, "mat_total")


def _mat_done_key(run_id: str) -> str:
    return _state_key(run_id, "mat_done")


def _mat_failed_key(run_id: str) -> str:
    return _state_key(run_id, "mat_failed")


def _mat_phase_started_at_key(run_id: str) -> str:
    return _state_key(run_id, "mat_phase_started_at")


def _mat_phase_duration_key(run_id: str, phase: str) -> str:
    return _state_key(run_id, f"mat_durations:{phase}")


def _decode_redis_str(raw: Any) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8")
    return str(raw or "")


async def _reset_materialize_state(redis, run_id: str) -> None:
    duration_keys = [_mat_phase_duration_key(run_id, phase) for phase in MAT_PHASE_SEQUENCE]
    await redis.delete(
        _mat_phase_key(run_id),
        _mat_total_key(run_id),
        _mat_done_key(run_id),
        _mat_failed_key(run_id),
        _mat_phase_started_at_key(run_id),
        *duration_keys,
    )


async def _set_materialize_phase(redis, run_id: str, phase: str, total: int = 0) -> None:
    await redis.set(_mat_phase_key(run_id), phase, ex=PROVIDER_QUALITY_REDIS_TTL_SECONDS)
    await redis.set(_mat_total_key(run_id), int(total), ex=PROVIDER_QUALITY_REDIS_TTL_SECONDS)
    await redis.set(_mat_done_key(run_id), 0, ex=PROVIDER_QUALITY_REDIS_TTL_SECONDS)
    await redis.set(_mat_failed_key(run_id), 0, ex=PROVIDER_QUALITY_REDIS_TTL_SECONDS)
    await redis.set(
        _mat_phase_started_at_key(run_id),
        f"{time.time():.6f}",
        ex=PROVIDER_QUALITY_REDIS_TTL_SECONDS,
    )
    await redis.delete(_mat_phase_duration_key(run_id, phase))


async def _get_materialize_phase(redis, run_id: str) -> str:
    return _decode_redis_str(await redis.get(_mat_phase_key(run_id)))


async def _get_materialize_progress(redis, run_id: str) -> tuple[int, int, int]:
    total = _safe_int(await redis.get(_mat_total_key(run_id)), 0)
    done = _safe_int(await redis.get(_mat_done_key(run_id)), 0)
    failed = _safe_int(await redis.get(_mat_failed_key(run_id)), 0)
    return total, done, failed


async def _get_materialize_phase_elapsed_seconds(redis, run_id: str) -> float:
    raw_started = await redis.get(_mat_phase_started_at_key(run_id))
    try:
        started_at = float(_decode_redis_str(raw_started))
    except (TypeError, ValueError):
        return 0.0
    if started_at <= 0:
        return 0.0
    return max(0.0, time.time() - started_at)


async def _mark_materialize_done(redis, run_id: str) -> None:
    await redis.incrby(_mat_done_key(run_id), 1)
    await redis.expire(_mat_done_key(run_id), PROVIDER_QUALITY_REDIS_TTL_SECONDS)


async def _mark_materialize_failed(redis, run_id: str) -> None:
    await redis.incrby(_mat_failed_key(run_id), 1)
    await redis.expire(_mat_failed_key(run_id), PROVIDER_QUALITY_REDIS_TTL_SECONDS)


async def _record_materialize_phase_duration(redis, run_id: str, phase: str, duration_sec: float) -> None:
    key = _mat_phase_duration_key(run_id, phase)
    await redis.rpush(key, f"{max(duration_sec, 0.0):.6f}")
    await redis.expire(key, PROVIDER_QUALITY_REDIS_TTL_SECONDS)
