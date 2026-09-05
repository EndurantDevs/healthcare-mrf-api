# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""ARQ shard jobs for provider-quality materialization."""

from __future__ import annotations

import logging
import time
from typing import Any

from process.ext.utils import ensure_database, get_import_schema
from process.provider_quality_parts.cohort_context import _build_cohort_materialization_context
from process.provider_quality_parts.cohort_sql import (
    _cohort_sql_phase_2_lsh_shard,
    _cohort_sql_phase_5_measure_shard,
    _cohort_sql_phase_6_domain_shard,
    _cohort_sql_phase_7_score_shard,
)
from process.provider_quality_parts.config import (
    MAT_PHASE_2_BUILD_LSH_SHARDED,
    MAT_PHASE_5_BUILD_MEASURE_SHARDED,
    MAT_PHASE_6_BUILD_DOMAIN_SHARDED,
    MAT_PHASE_7_BUILD_SCORE_SHARDED,
    PROVIDER_QUALITY_MIN_YEAR,
)
from process.provider_quality_parts.execution_helpers import (
    _count_shard_rows,
    _execute_shard_sql,
)
from process.provider_quality_parts.model_helpers import _staging_classes
from process.provider_quality_parts.state import (
    _mark_materialize_done,
    _mark_materialize_failed,
    _record_materialize_phase_duration,
    _safe_int,
)

logger = logging.getLogger("process.provider_quality")


def _materialize_shard_task_values(task: dict[str, Any] | None) -> tuple[str, str, str, bool, int, int, int]:
    payload = task or {}
    run_id = str(payload.get("run_id") or "")
    stage_suffix = str(payload.get("stage_suffix") or "")
    schema = str(payload.get("schema") or get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", bool(payload.get("test_mode"))))
    test_mode = bool(payload.get("test_mode", False))
    year = max(_safe_int(payload.get("year"), PROVIDER_QUALITY_MIN_YEAR), 2013)
    shard_count = max(_safe_int(payload.get("shard_count"), 1), 1)
    shard_id = _safe_int(payload.get("shard_id"), 0)
    if shard_id < 0 or shard_id >= shard_count:
        raise RuntimeError(f"Invalid shard payload: shard_id={shard_id} shard_count={shard_count}")
    if not run_id or not stage_suffix:
        raise RuntimeError("Shard payload is missing required fields: run_id/stage_suffix.")
    return run_id, stage_suffix, schema, test_mode, year, shard_id, shard_count


async def _count_materialize_shard_rows(
    schema: str,
    target_table: str,
    *,
    run_id: str,
    phase: str,
    year: int,
    shard_id: int,
    shard_count: int,
    checkpoint: str,
) -> int | None:
    try:
        return await _count_shard_rows(
            schema,
            target_table,
            year=year,
            shard_id=shard_id,
            shard_count=shard_count,
        )
    except Exception as exc:
        logger.warning(
            "provider quality materialize shard %s-count failed run_id=%s phase=%s year=%s shard_id=%s/%s table=%s error=%s",
            checkpoint,
            run_id,
            phase,
            year,
            shard_id,
            shard_count,
            target_table,
            exc,
        )
        return None


async def _materialize_shard_rows(
    task_values: tuple[str, str, str, bool, int, int, int],
    *,
    sql_builder,
    phase: str,
    target_table_key: str,
    include_run_id: bool,
) -> tuple[int | None, int | None]:
    run_id, stage_suffix, schema, test_mode, year, shard_id, shard_count = task_values
    await ensure_database(test_mode)
    classes = _staging_classes(stage_suffix, schema)
    cohort_context = await _build_cohort_materialization_context(classes, schema)
    target_table = str(cohort_context[target_table_key])
    count_options_by_name = {
        "run_id": run_id,
        "phase": phase,
        "year": year,
        "shard_id": shard_id,
        "shard_count": shard_count,
    }
    rows_before = await _count_materialize_shard_rows(
        schema,
        target_table,
        **count_options_by_name,
        checkpoint="pre",
    )
    parameter_by_name: dict[str, Any] = {
        "year": year,
        "shard_id": shard_id,
        "shard_count": shard_count,
    }
    if include_run_id:
        parameter_by_name["run_id"] = run_id
    await _execute_shard_sql(sql_builder(cohort_context), **parameter_by_name)
    rows_after = await _count_materialize_shard_rows(
        schema,
        target_table,
        **count_options_by_name,
        checkpoint="post",
    )
    return rows_before, rows_after


async def _run_materialize_shard_job(
    ctx,
    task: dict[str, Any] | None,
    *,
    sql_builder,
    phase: str,
    target_table_key: str,
    include_run_id: bool = False,
) -> dict[str, Any]:
    """Run one provider-quality materialization shard."""
    task_values = _materialize_shard_task_values(task)
    run_id, _stage_suffix, _schema, _test_mode, year, shard_id, shard_count = task_values
    redis = ctx.get("redis")
    started_at = time.monotonic()
    try:
        rows_before, rows_after = await _materialize_shard_rows(
            task_values,
            sql_builder=sql_builder,
            phase=phase,
            target_table_key=target_table_key,
            include_run_id=include_run_id,
        )
        duration_sec = max(time.monotonic() - started_at, 0.0)
        rows_deleted = rows_before
        rows_inserted = rows_after
        logger.info(
            "provider quality materialize shard done run_id=%s phase=%s year=%s shard_id=%s shard_count=%s duration_sec=%.3f rows_deleted=%s rows_inserted=%s rows_final=%s",
            run_id,
            phase,
            year,
            shard_id,
            shard_count,
            duration_sec,
            rows_deleted,
            rows_inserted,
            rows_after,
        )
        if redis is not None:
            await _mark_materialize_done(redis, run_id)
            await _record_materialize_phase_duration(redis, run_id, phase, duration_sec)
    except Exception:
        if redis is not None:
            await _mark_materialize_failed(redis, run_id)
        logger.exception(
            "provider quality materialize shard failed run_id=%s phase=%s year=%s shard_id=%s shard_count=%s",
            run_id,
            phase,
            year,
            shard_id,
            shard_count,
        )
        raise
    return {
        "ok": True,
        "run_id": run_id,
        "phase": phase,
        "year": year,
        "shard_id": shard_id,
        "shard_count": shard_count,
    }


async def provider_quality_materialize_lsh_shard(
    ctx, task: dict[str, Any] | None = None, **_kwargs: Any
) -> dict[str, Any]:
    """Materialize one provider-quality LSH shard."""
    return await _run_materialize_shard_job(
        ctx,
        task,
        sql_builder=_cohort_sql_phase_2_lsh_shard,
        phase=MAT_PHASE_2_BUILD_LSH_SHARDED,
        target_table_key="lsh_table",
    )


async def provider_quality_materialize_measure_shard(
    ctx, task: dict[str, Any] | None = None, **_kwargs: Any
) -> dict[str, Any]:
    """Materialize one provider-quality measure shard."""
    return await _run_materialize_shard_job(
        ctx,
        task,
        sql_builder=_cohort_sql_phase_5_measure_shard,
        phase=MAT_PHASE_5_BUILD_MEASURE_SHARDED,
        target_table_key="measure_table",
    )


async def provider_quality_materialize_domain_shard(
    ctx, task: dict[str, Any] | None = None, **_kwargs: Any
) -> dict[str, Any]:
    """Materialize one provider-quality domain shard."""
    return await _run_materialize_shard_job(
        ctx,
        task,
        sql_builder=_cohort_sql_phase_6_domain_shard,
        phase=MAT_PHASE_6_BUILD_DOMAIN_SHARDED,
        target_table_key="domain_table",
    )


async def provider_quality_materialize_score_shard(
    ctx, task: dict[str, Any] | None = None, **_kwargs: Any
) -> dict[str, Any]:
    """Materialize one provider-quality score shard."""
    return await _run_materialize_shard_job(
        ctx,
        task,
        sql_builder=_cohort_sql_phase_7_score_shard,
        phase=MAT_PHASE_7_BUILD_SCORE_SHARDED,
        target_table_key="score_table",
        include_run_id=True,
    )
