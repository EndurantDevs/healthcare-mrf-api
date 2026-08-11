# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Persistence helpers for reviewed numeric-grid address alias runs."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text

from db.models import db
from process.address_numeric_grid_alias_support import (
    _candidate_digest,
    _candidate_sample,
    _relation,
)
from process.ext import address_alias_audit_sql, address_alias_sql


async def _alias_state(
    session: Any,
    *,
    schema: str,
    lock: bool,
) -> tuple[int, int, int]:
    suffix = " FOR UPDATE" if lock else ""
    state_record = (
        await session.execute(
            text(
                address_alias_sql.active_alias_generation_sql(schema=schema)
                .strip()
                .removesuffix(";")
                + suffix
            )
        )
    ).first()
    if state_record is None:
        raise RuntimeError("address alias singleton state is missing")
    schema_version = int(state_record.schema_version)
    ruleset_version = int(state_record.active_ruleset_version)
    generation = int(state_record.generation)
    if schema_version != address_alias_sql.ADDRESS_ALIAS_SCHEMA_VERSION:
        raise RuntimeError(f"unsupported address alias schema version: {schema_version}")
    if ruleset_version != address_alias_sql.NUMERIC_GRID_ALIAS_RULESET_VERSION:
        raise RuntimeError(f"unsupported numeric-grid alias ruleset: {ruleset_version}")
    return schema_version, ruleset_version, generation


async def _insert_run(
    *,
    schema: str,
    run_id: str,
    mode: str,
    state_code: str | None,
    zip_prefix: str | None,
    shadow_run_id: str | None,
    reviewed_digest: str | None,
    reviewed_by: str | None,
) -> None:
    runs = _relation(schema, address_alias_sql.ADDRESS_ALIAS_RUN_TABLE)
    await db.status(
        f"""
        INSERT INTO {runs} (
            run_id, alias_kind, ruleset_version, mode, status,
            reviewed_shadow_run_id, reviewed_candidate_digest, reviewed_by,
            scope_state_code, scope_zip_prefix
        )
        VALUES (
            CAST(:run_id AS uuid), :alias_kind, :ruleset_version, :mode, 'running',
            CAST(:shadow_run_id AS uuid), :reviewed_digest, :reviewed_by,
            :state_code, :zip_prefix
        );
        """,
        run_id=run_id,
        alias_kind=address_alias_sql.NUMERIC_GRID_ALIAS_KIND,
        ruleset_version=address_alias_sql.NUMERIC_GRID_ALIAS_RULESET_VERSION,
        mode=mode,
        shadow_run_id=shadow_run_id,
        reviewed_digest=reviewed_digest,
        reviewed_by=reviewed_by,
        state_code=state_code,
        zip_prefix=zip_prefix,
    )


async def _mark_failed(schema: str, run_id: str, exc: BaseException) -> str | None:
    """Mark only a still-running job failed, preserving committed outcomes."""
    runs = _relation(schema, address_alias_sql.ADDRESS_ALIAS_RUN_TABLE)
    status_record = await db.first(
        f"""
        WITH marked AS (
            UPDATE {runs}
               SET status = 'failed',
                   error_text = LEFT(:error_text, 4000),
                   completed_at = now()
             WHERE run_id = CAST(:run_id AS uuid)
               AND status = 'running'
            RETURNING status
        )
        SELECT status FROM marked
        UNION ALL
        SELECT status
        FROM {runs}
        WHERE run_id = CAST(:run_id AS uuid)
          AND NOT EXISTS (SELECT 1 FROM marked)
        LIMIT 1;
        """,
        run_id=run_id,
        error_text=str(exc),
    )
    return str(status_record.status) if status_record is not None else None


def _candidate_scope_by_field(
    *,
    run_id: str,
    state_code: str | None,
    zip_prefix: str | None,
    retry_shadow_run_id: str | None,
) -> dict[str, str | None]:
    return {
        "run_id": run_id,
        "scope_state_code": state_code,
        "scope_zip_prefix": zip_prefix,
        "retry_shadow_run_id": retry_shadow_run_id,
    }


async def _archive_source_counts(
    session: Any,
    *,
    schema: str,
    archive: str,
    scope_by_field: dict[str, str | None],
) -> tuple[int, int]:
    archive_rows = int(
        (await session.execute(text(f"SELECT count(*) FROM {archive};"))).scalar() or 0
    )
    source_count = int(
        (
            await session.execute(
                text(
                    address_alias_sql.numeric_grid_source_count_sql(
                        schema=schema,
                        archive=archive,
                    )
                ),
                scope_by_field,
            )
        ).scalar()
        or 0
    )
    return archive_rows, source_count


async def _candidate_metrics_by_reason(
    session: Any,
    *,
    schema: str,
    archive: str,
    run_id: str,
    source_count: int,
    scope_by_field: dict[str, str | None],
) -> dict[str, int]:
    raw_metric_map = (
        await session.execute(
            text(address_alias_sql.candidate_metrics_sql(schema=schema)),
            {"run_id": run_id},
        )
    ).scalar() or {}
    decoded_metric_map = (
        json.loads(raw_metric_map)
        if isinstance(raw_metric_map, str)
        else dict(raw_metric_map)
    )
    metrics_by_reason = {
        str(metric_name): int(metric_count or 0)
        for metric_name, metric_count in decoded_metric_map.items()
    }
    active_skipped = int(
        (
            await session.execute(
                text(
                    address_alias_audit_sql.numeric_grid_skipped_source_count_sql(
                        schema=schema,
                        archive=archive,
                    )
                ),
                scope_by_field,
            )
        ).scalar()
        or 0
    )
    candidate_sources = metrics_by_reason.get("candidate_sources", 0)
    metrics_by_reason["active_skipped"] = active_skipped
    metrics_by_reason["no_candidate"] = max(
        source_count - active_skipped - candidate_sources,
        0,
    )
    return metrics_by_reason


async def _shadow_run(
    session: Any,
    *,
    schema: str,
    archive: str,
    run_id: str,
    state_code: str | None,
    zip_prefix: str | None,
    sample_limit: int,
    retry_shadow_run_id: str | None,
) -> tuple[dict[str, int], str, list[dict[str, Any]], int, int]:
    """Persist and summarize one deterministic candidate snapshot."""
    scope_by_field = _candidate_scope_by_field(
        run_id=run_id,
        state_code=state_code,
        zip_prefix=zip_prefix,
        retry_shadow_run_id=retry_shadow_run_id,
    )
    archive_rows, source_count = await _archive_source_counts(
        session,
        schema=schema,
        archive=archive,
        scope_by_field=scope_by_field,
    )
    await session.execute(
        text(
            address_alias_sql.numeric_grid_candidate_insert_sql(
                schema=schema,
                archive=archive,
            )
        ),
        scope_by_field,
    )
    metrics_by_reason = await _candidate_metrics_by_reason(
        session,
        schema=schema,
        archive=archive,
        run_id=run_id,
        source_count=source_count,
        scope_by_field=scope_by_field,
    )
    candidate_records = (
        await session.execute(
            text(address_alias_sql.candidate_rows_sql(schema=schema)),
            {"run_id": run_id},
        )
    ).all()
    return (
        metrics_by_reason,
        _candidate_digest(candidate_records),
        _candidate_sample(candidate_records, sample_limit),
        archive_rows,
        source_count,
    )


async def _load_reviewed_shadow(
    *,
    schema: str,
    shadow_run_id: str,
    expected_digest: str,
) -> dict[str, Any]:
    runs = _relation(schema, address_alias_sql.ADDRESS_ALIAS_RUN_TABLE)
    shadow_record = await db.first(
        f"""
        SELECT *
        FROM {runs}
        WHERE run_id = CAST(:run_id AS uuid)
          AND mode = 'shadow';
        """,
        run_id=shadow_run_id,
    )
    if shadow_record is None:
        raise ValueError("reviewed shadow run was not found")
    shadow_by_field = dict(shadow_record._mapping)
    if shadow_by_field.get("status") != "sealed":
        raise ValueError("reviewed shadow run must be sealed")
    if shadow_by_field.get("candidate_digest") != expected_digest:
        raise ValueError("reviewed candidate digest does not match the sealed shadow run")
    return shadow_by_field


async def _approve_shadow_candidates(
    session: Any,
    *,
    schema: str,
    shadow_run_id: str,
    reviewer: str,
) -> None:
    candidates = _relation(schema, address_alias_sql.ADDRESS_ALIAS_CANDIDATE_TABLE)
    runs = _relation(schema, address_alias_sql.ADDRESS_ALIAS_RUN_TABLE)
    await session.execute(
        text(
            f"""
            WITH approved AS (
                UPDATE {candidates}
                   SET review_status = 'approved',
                       reviewed_by = :reviewed_by,
                       reviewed_at = now()
                 WHERE run_id = CAST(:shadow_run_id AS uuid)
                   AND decision = 'eligible'
                   AND review_status = 'pending'
                RETURNING 1
            )
            UPDATE {runs}
               SET reviewed_by = :reviewed_by,
                   reviewed_at = now()
             WHERE run_id = CAST(:shadow_run_id AS uuid)
               AND reviewed_by IS NULL
               AND reviewed_at IS NULL
               AND EXISTS (SELECT 1 FROM approved);
            """
        ),
        {"shadow_run_id": shadow_run_id, "reviewed_by": reviewer},
    )
