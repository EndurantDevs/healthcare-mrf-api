# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Persistence helpers for reviewed numeric-grid address alias runs."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from sqlalchemy import text

from db.models import db
from process.address_evidence_alias_native import try_native_evidence_shadow
from process.address_numeric_grid_alias_support import (
    _candidate_digest,
    _candidate_sample,
    _relation,
)
from process.ext import (
    address_alias_audit_sql,
    address_alias_snapshot_sql,
    address_alias_sql,
    address_evidence_alias_sql,
)


def _run_completion_sql(schema: str) -> str:
    runs = _relation(schema, address_alias_sql.ADDRESS_ALIAS_RUN_TABLE)
    return f"""
        UPDATE {runs}
           SET status = :status,
               candidate_digest = :candidate_digest,
               archive_row_count = :archive_rows,
               source_count = :source_count,
               candidate_source_count = :candidate_sources,
               candidate_row_count = :candidate_rows,
               no_candidate_count = :no_candidate,
               active_skipped_count = :active_skipped,
               eligible_count = :eligible,
               ambiguous_count = :ambiguous,
               insufficient_provenance_count = :insufficient,
               reason_buckets = CAST(:reason_buckets AS jsonb),
               sample_rows = CAST(:sample_rows AS jsonb),
               completed_at = now()
         WHERE run_id = CAST(:run_id AS uuid);
    """


def _completion_parameters(execution: Any) -> dict[str, Any]:
    metric_map = execution.metrics_by_reason
    reason_map = {
        reason_name: metric_map.get(reason_name, 0)
        for reason_name in (
            "eligible",
            "ambiguous",
            "no_candidate",
            "active_skipped",
            "insufficient_provenance",
        )
    }
    return {
        "status": execution.final_status,
        "candidate_digest": execution.digest,
        "archive_rows": execution.archive_rows,
        "source_count": execution.source_count,
        "candidate_sources": metric_map.get("candidate_sources", 0),
        "candidate_rows": metric_map.get("candidate_rows", 0),
        "no_candidate": metric_map.get("no_candidate", 0),
        "active_skipped": metric_map.get("active_skipped", 0),
        "eligible": metric_map.get("eligible", 0),
        "ambiguous": metric_map.get("ambiguous", 0),
        "insufficient": metric_map.get("insufficient_provenance", 0),
        "reason_buckets": json.dumps(reason_map, sort_keys=True),
        "sample_rows": json.dumps(execution.sample_rows, sort_keys=True),
        "run_id": execution.run_id,
    }


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
    if ruleset_version != address_alias_sql.ADDRESS_ALIAS_RULESET_VERSION:
        raise RuntimeError(f"unsupported address alias ruleset: {ruleset_version}")
    return schema_version, ruleset_version, generation


async def _insert_run(
    *,
    run_by_field: dict[str, Any],
) -> None:
    runs = _relation(
        str(run_by_field["schema"]),
        address_alias_sql.ADDRESS_ALIAS_RUN_TABLE,
    )
    parameters_by_name = dict(run_by_field)
    parameters_by_name.pop("schema")
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
        **parameters_by_name,
    )


async def _mark_failed(
    schema: str,
    run_id: str,
    exc: BaseException,
    *,
    deadline_monotonic: float | None = None,
) -> str | None:
    """Mark only a still-running job failed, preserving committed outcomes."""
    runs = _relation(schema, address_alias_sql.ADDRESS_ALIAS_RUN_TABLE)
    statement = f"""
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
        """
    failure_parameters_by_name = {"run_id": run_id, "error_text": str(exc)}
    if deadline_monotonic is None:
        status_record = await db.first(statement, **failure_parameters_by_name)
        return str(status_record.status) if status_record is not None else None

    remaining_seconds = deadline_monotonic - asyncio.get_running_loop().time()
    if remaining_seconds <= 0:
        raise TimeoutError("address alias lifecycle deadline elapsed before failure marking")
    timeout_milliseconds = max(1, int(remaining_seconds * 1000))
    async with asyncio.timeout(remaining_seconds):
        async with db.transaction() as session:
            await session.execute(
                text(f"SET LOCAL lock_timeout = '{timeout_milliseconds}ms';")
            )
            await session.execute(
                text(f"SET LOCAL statement_timeout = '{timeout_milliseconds}ms';")
            )
            status_record = (
                await session.execute(text(statement), failure_parameters_by_name)
            ).first()
    return str(status_record.status) if status_record is not None else None


async def _archive_source_counts(
    session: Any,
    *,
    schema: str,
    archive: str,
    scope_by_field: dict[str, str | None],
    alias_kind: str,
) -> tuple[int, int]:
    archive_rows = int(
        (await session.execute(text(f"SELECT count(*) FROM {archive};"))).scalar() or 0
    )
    source_count = int(
        (
            await session.execute(
                text(
                    (
                        address_evidence_alias_sql.evidence_source_count_sql
                        if alias_kind
                        == address_alias_sql.EVIDENCE_ADDRESS_MATCH_ALIAS_KIND
                        else address_alias_sql.numeric_grid_source_count_sql
                    )(schema=schema, archive=archive)
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
    alias_kind: str,
    active_skipped_override: int | None = None,
) -> dict[str, int]:
    raw_metric_map = (
        await session.execute(
            text(address_alias_snapshot_sql.candidate_metrics_sql(schema=schema)),
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
    active_skipped = active_skipped_override
    if active_skipped is None:
        active_skipped = int(
            (
                await session.execute(
                    text(
                        (
                            address_evidence_alias_sql.evidence_skipped_source_count_sql
                            if alias_kind
                            == address_alias_sql.EVIDENCE_ADDRESS_MATCH_ALIAS_KIND
                            else address_alias_audit_sql.numeric_grid_skipped_source_count_sql
                        )(schema=schema, archive=archive)
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


async def _materialize_candidate_snapshot(
    session: Any,
    *,
    schema: str,
    archive: str,
    scope_by_field: dict[str, str | None],
    alias_kind: str,
    cleanup_deadline_monotonic: float | None = None,
) -> tuple[int, int, int | None]:
    run_id = str(scope_by_field["run_id"])
    native_summary = None
    if alias_kind == address_alias_sql.EVIDENCE_ADDRESS_MATCH_ALIAS_KIND:
        native_summary = await try_native_evidence_shadow(
            session,
            schema=schema,
            archive=archive,
            run_id=run_id,
            state_code=scope_by_field["scope_state_code"],
            zip_prefix=scope_by_field["scope_zip_prefix"],
            retry_shadow_run_id=scope_by_field["retry_shadow_run_id"],
            cleanup_deadline_monotonic=cleanup_deadline_monotonic,
        )
    if native_summary is None:
        archive_rows, source_count = await _archive_source_counts(
            session,
            schema=schema,
            archive=archive,
            scope_by_field=scope_by_field,
            alias_kind=alias_kind,
        )
        await session.execute(
            text(
                (
                    address_evidence_alias_sql.evidence_candidate_insert_sql
                    if alias_kind == address_alias_sql.EVIDENCE_ADDRESS_MATCH_ALIAS_KIND
                    else address_alias_sql.numeric_grid_candidate_insert_sql
                )(schema=schema, archive=archive)
            ),
            scope_by_field,
        )
    else:
        archive_rows = int(native_summary["archive_rows"])
        source_count = int(native_summary["source_count"])
    active_skipped_override = (
        int(native_summary["active_skipped"]) if native_summary is not None else None
    )
    return archive_rows, source_count, active_skipped_override


async def _shadow_run(
    session: Any,
    *,
    schema: str,
    archive: str,
    scope_by_field: dict[str, str | None],
    sample_limit: int,
    alias_kind: str = address_alias_sql.NUMERIC_GRID_ALIAS_KIND,
    cleanup_deadline_monotonic: float | None = None,
) -> tuple[dict[str, int], str, list[dict[str, Any]], int, int]:
    """Persist and summarize one deterministic candidate snapshot."""
    run_id = str(scope_by_field["run_id"])
    archive_rows, source_count, active_skipped_override = (
        await _materialize_candidate_snapshot(
            session,
            schema=schema,
            archive=archive,
            scope_by_field=scope_by_field,
            alias_kind=alias_kind,
            cleanup_deadline_monotonic=cleanup_deadline_monotonic,
        )
    )
    metrics_by_reason = await _candidate_metrics_by_reason(
        session,
        schema=schema,
        archive=archive,
        run_id=run_id,
        source_count=source_count,
        scope_by_field=scope_by_field,
        alias_kind=alias_kind,
        active_skipped_override=active_skipped_override,
    )
    candidate_records = (
        await session.execute(
            text(
                (
                    address_alias_snapshot_sql.evidence_candidate_rows_sql
                    if alias_kind
                    == address_alias_sql.EVIDENCE_ADDRESS_MATCH_ALIAS_KIND
                    else address_alias_snapshot_sql.candidate_rows_sql
                )(schema=schema)
            ),
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
    alias_kind: str = address_alias_sql.NUMERIC_GRID_ALIAS_KIND,
    ruleset_version: int = address_alias_sql.NUMERIC_GRID_ALIAS_RULESET_VERSION,
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
    if shadow_by_field.get("alias_kind") != alias_kind:
        raise ValueError("reviewed shadow alias kind differs from the requested policy")
    if int(shadow_by_field.get("ruleset_version") or 0) != ruleset_version:
        raise ValueError("reviewed shadow ruleset differs from the requested policy")
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
