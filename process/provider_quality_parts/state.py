# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Redis-backed run and materialization state helpers."""

from __future__ import annotations

import asyncio
import logging
import statistics
import time
from typing import Any

from process.provider_quality_parts.config import (
    MAT_PHASE_SEQUENCE,
    PROVIDER_QUALITY_GLOBAL_FINALIZE_LOCK_KEY,
    PROVIDER_QUALITY_GLOBAL_FINALIZE_LOCK_TTL_SECONDS,
    PROVIDER_QUALITY_MARK_DONE_RETRIES,
    PROVIDER_QUALITY_MARK_DONE_RETRY_BASE_SECONDS,
    PROVIDER_QUALITY_MARK_DONE_RETRY_MAX_SECONDS,
    PROVIDER_QUALITY_REDIS_TTL_SECONDS,
)

logger = logging.getLogger("process.provider_quality")


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


async def _log_materialize_phase_summary(redis, run_id: str, phase: str, *, total: int, done: int, failed: int) -> None:
    key = _mat_phase_duration_key(run_id, phase)
    raw_values = await redis.lrange(key, 0, -1)
    durations: list[float] = []
    for raw in raw_values:
        try:
            durations.append(float(_decode_redis_str(raw)))
        except (TypeError, ValueError):
            continue

    if durations:
        logger.info(
            "provider quality materialize phase complete run_id=%s phase=%s total=%s done=%s failed=%s shard_sec_min=%.3f shard_sec_median=%.3f shard_sec_max=%.3f",
            run_id,
            phase,
            total,
            done,
            failed,
            min(durations),
            statistics.median(durations),
            max(durations),
        )
        return

    logger.info(
        "provider quality materialize phase complete run_id=%s phase=%s total=%s done=%s failed=%s",
        run_id,
        phase,
        total,
        done,
        failed,
    )


async def _init_run_state(redis, run_id: str, total_chunks: int) -> None:
    total_key = _state_key(run_id, "total_chunks")
    done_key = _state_key(run_id, "done_chunks")
    lock_key = _state_key(run_id, "finalize_lock")
    finalized_key = _state_key(run_id, "finalized")
    duration_keys = [_mat_phase_duration_key(run_id, phase) for phase in MAT_PHASE_SEQUENCE]

    await redis.delete(
        total_key,
        done_key,
        lock_key,
        finalized_key,
        _mat_phase_key(run_id),
        _mat_total_key(run_id),
        _mat_done_key(run_id),
        _mat_failed_key(run_id),
        *duration_keys,
    )
    await redis.set(total_key, str(total_chunks))
    await redis.expire(total_key, PROVIDER_QUALITY_REDIS_TTL_SECONDS)
    await redis.sadd(done_key, "__init__")
    await redis.srem(done_key, "__init__")
    await redis.expire(done_key, PROVIDER_QUALITY_REDIS_TTL_SECONDS)


async def _increment_total_chunks(redis, run_id: str, delta: int) -> None:
    if delta <= 0:
        return
    total_key = _state_key(run_id, "total_chunks")
    await redis.incrby(total_key, int(delta))
    await redis.expire(total_key, PROVIDER_QUALITY_REDIS_TTL_SECONDS)


async def _mark_chunk_done(redis, run_id: str, chunk_id: str) -> None:
    done_key = _state_key(run_id, "done_chunks")
    await redis.sadd(done_key, chunk_id)
    await redis.expire(done_key, PROVIDER_QUALITY_REDIS_TTL_SECONDS)


async def _mark_chunk_done_with_retry(redis, run_id: str, chunk_id: str) -> None:
    last_exc: Exception | None = None
    for attempt in range(1, PROVIDER_QUALITY_MARK_DONE_RETRIES + 1):
        try:
            await _mark_chunk_done(redis, run_id, chunk_id)
            return
        except Exception as exc:  # pylint: disable=broad-exception-caught
            last_exc = exc
            if attempt >= PROVIDER_QUALITY_MARK_DONE_RETRIES:
                break
            delay = min(
                PROVIDER_QUALITY_MARK_DONE_RETRY_BASE_SECONDS * (2 ** (attempt - 1)),
                PROVIDER_QUALITY_MARK_DONE_RETRY_MAX_SECONDS,
            )
            logger.warning(
                "Retrying mark_chunk_done for run_id=%s chunk_id=%s after error (%s/%s): %r",
                run_id,
                chunk_id,
                attempt,
                PROVIDER_QUALITY_MARK_DONE_RETRIES,
                exc,
            )
            await asyncio.sleep(delay)
    if last_exc is not None:
        raise last_exc


async def _get_run_progress(redis, run_id: str, expected_default: int) -> tuple[int, int]:
    total_key = _state_key(run_id, "total_chunks")
    done_key = _state_key(run_id, "done_chunks")
    total_chunks = _safe_int(await redis.get(total_key), expected_default)
    done_chunks = _safe_int(await redis.scard(done_key), 0)
    return total_chunks, done_chunks


async def _claim_finalize_lock(redis, run_id: str) -> bool:
    lock_key = _state_key(run_id, "finalize_lock")
    lock_set = await redis.set(lock_key, run_id, ex=PROVIDER_QUALITY_REDIS_TTL_SECONDS, nx=True)
    if lock_set:
        return True
    return bool(await redis.get(lock_key))


async def _claim_global_finalize_lock(redis, run_id: str) -> bool:
    lock_set = await redis.set(
        PROVIDER_QUALITY_GLOBAL_FINALIZE_LOCK_KEY,
        run_id,
        ex=PROVIDER_QUALITY_GLOBAL_FINALIZE_LOCK_TTL_SECONDS,
        nx=True,
    )
    if lock_set:
        return True
    current_owner = _decode_redis_str(await redis.get(PROVIDER_QUALITY_GLOBAL_FINALIZE_LOCK_KEY))
    return current_owner == run_id


async def _release_global_finalize_lock(redis, run_id: str) -> None:
    current_owner = _decode_redis_str(await redis.get(PROVIDER_QUALITY_GLOBAL_FINALIZE_LOCK_KEY))
    if current_owner == run_id:
        await redis.delete(PROVIDER_QUALITY_GLOBAL_FINALIZE_LOCK_KEY)
