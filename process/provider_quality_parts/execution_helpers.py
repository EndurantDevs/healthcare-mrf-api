# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Execution, progress, and retry helpers for provider-quality imports."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from db.connection import db
from process.ext.utils import push_objects
from process.provider_quality_parts.config import (
    PROVIDER_QUALITY_DB_DEADLOCK_BASE_DELAY_SECONDS,
    PROVIDER_QUALITY_DB_DEADLOCK_RETRIES,
    PROVIDER_QUALITY_SHARD_JIT,
    PROVIDER_QUALITY_SHARD_MAX_PARALLEL_GATHER,
    PROVIDER_QUALITY_SHARD_WORK_MEM,
)
from process.provider_quality_parts.lifecycle import _npi_shard_predicate

logger = logging.getLogger("process.provider_quality")


def _print_row_progress(stage: str, parsed: int, accepted: int, start_time: float, final: bool = False) -> None:
    elapsed = max(time.monotonic() - start_time, 0.001)
    rate = parsed / elapsed
    line = f"\r[rows:{stage}] parsed={parsed:,} accepted={accepted:,} rate={rate:,.0f} rows/s"
    print(line, end="\n" if final else "", flush=True)


def _step_start(label: str) -> float:
    start = time.monotonic()
    print(f"[step] START {label}", flush=True)
    return start


def _step_end(label: str, started_at: float) -> None:
    elapsed = max(time.monotonic() - started_at, 0.001)
    print(f"[step] DONE  {label} in {elapsed:.1f}s", flush=True)


def _is_deadlock_error(exc: BaseException) -> bool:
    return "deadlock detected" in str(exc).lower()


async def _execute_shard_sql(sql: str, **params: Any) -> None:
    async with db.transaction():
        await db.status(f"SET LOCAL work_mem = '{PROVIDER_QUALITY_SHARD_WORK_MEM}';")
        await db.status(f"SET LOCAL jit = {PROVIDER_QUALITY_SHARD_JIT};")
        await db.status(
            f"SET LOCAL max_parallel_workers_per_gather = {PROVIDER_QUALITY_SHARD_MAX_PARALLEL_GATHER};"
        )
        await db.status(sql, **params)


async def _count_shard_rows(schema: str, table_name: str, *, year: int, shard_id: int, shard_count: int) -> int:
    stmt = db.text(
        f"""
        SELECT COUNT(*)::bigint AS row_count
        FROM {schema}.{table_name} t
        WHERE t.year = :year
          AND {_npi_shard_predicate("t.npi")}
        """
    )
    async with db.session() as session:
        result = await session.execute(
            stmt,
            {
                "year": year,
                "shard_id": shard_id,
                "shard_count": shard_count,
            },
        )
        return int(result.scalar() or 0)


async def _push_objects_with_retry(
    rows: list[dict[str, Any]],
    cls: type,
    *,
    rewrite: bool = False,
    use_copy: bool = True,
) -> None:
    if not rows:
        return
    for attempt in range(1, PROVIDER_QUALITY_DB_DEADLOCK_RETRIES + 1):
        try:
            await push_objects(rows, cls, rewrite=rewrite, use_copy=use_copy)
            return
        except Exception as exc:  # pylint: disable=broad-exception-caught
            if not _is_deadlock_error(exc) or attempt >= PROVIDER_QUALITY_DB_DEADLOCK_RETRIES:
                raise
            delay = PROVIDER_QUALITY_DB_DEADLOCK_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "Deadlock while inserting into %s (attempt %s/%s), retrying in %.2fs",
                cls.__tablename__,
                attempt,
                PROVIDER_QUALITY_DB_DEADLOCK_RETRIES,
                delay,
            )
            await asyncio.sleep(delay)


def _row_allowed_for_test(row_number: int) -> bool:
    return row_number % 11 == 0
