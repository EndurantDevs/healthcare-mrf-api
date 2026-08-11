# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reviewed offline discovery and activation for numeric-grid address aliases."""

from __future__ import annotations

import json
import os
import uuid
from typing import Any, Awaitable, Callable

from sqlalchemy import text

from db.models import db
from process.address_numeric_grid_alias_support import (
    NumericGridAliasResult,
    _candidate_digest,
    _candidate_sample,
    _normalize_scope,
    _relation,
    _reviewed_digest,
    _reviewer,
    _statement_timeout,
)
from process.ext import address_alias_audit_sql, address_alias_sql
from process.ext.address_canon import _archive_lock_key, archive_table_name


async def _alias_state(session: Any, *, schema: str, lock: bool) -> tuple[int, int, int]:
    suffix = " FOR UPDATE" if lock else ""
    row = (
        await session.execute(
            text(
                address_alias_sql.active_alias_generation_sql(schema=schema)
                .strip()
                .removesuffix(";")
                + suffix
            )
        )
    ).first()
    if row is None:
        raise RuntimeError("address alias singleton state is missing")
    schema_version = int(row.schema_version)
    ruleset_version = int(row.active_ruleset_version)
    generation = int(row.generation)
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
    row = await db.first(
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
    return str(row.status) if row is not None else None


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
    scope = {
        "run_id": run_id,
        "scope_state_code": state_code,
        "scope_zip_prefix": zip_prefix,
        "retry_shadow_run_id": retry_shadow_run_id,
    }
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
                scope,
            )
        ).scalar()
        or 0
    )
    await session.execute(
        text(
            address_alias_sql.numeric_grid_candidate_insert_sql(
                schema=schema,
                archive=archive,
            )
        ),
        scope,
    )
    raw_metrics = (
        await session.execute(
            text(address_alias_sql.candidate_metrics_sql(schema=schema)),
            {"run_id": run_id},
        )
    ).scalar() or {}
    metrics = json.loads(raw_metrics) if isinstance(raw_metrics, str) else dict(raw_metrics)
    metrics = {str(key): int(value or 0) for key, value in metrics.items()}
    active_skipped = int(
        (
            await session.execute(
                text(
                    address_alias_audit_sql.numeric_grid_skipped_source_count_sql(
                        schema=schema,
                        archive=archive,
                    )
                ),
                scope,
            )
        ).scalar()
        or 0
    )
    candidate_sources = metrics.get("candidate_sources", 0)
    metrics["active_skipped"] = active_skipped
    metrics["no_candidate"] = max(
        source_count - active_skipped - candidate_sources,
        0,
    )
    candidate_rows = (
        await session.execute(
            text(address_alias_sql.candidate_rows_sql(schema=schema)),
            {"run_id": run_id},
        )
    ).all()
    return (
        metrics,
        _candidate_digest(candidate_rows),
        _candidate_sample(candidate_rows, sample_limit),
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
    row = await db.first(
        f"""
        SELECT *
        FROM {runs}
        WHERE run_id = CAST(:run_id AS uuid)
          AND mode = 'shadow';
        """,
        run_id=shadow_run_id,
    )
    if row is None:
        raise ValueError("reviewed shadow run was not found")
    shadow = dict(row._mapping)
    if shadow.get("status") != "sealed":
        raise ValueError("reviewed shadow run must be sealed")
    if shadow.get("candidate_digest") != expected_digest:
        raise ValueError("reviewed candidate digest does not match the sealed shadow run")
    return shadow


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


async def run_numeric_grid_alias(
    *,
    mode: str = "off",
    schema: str | None = None,
    state_code: str | None = None,
    zip_prefix: str | None = None,
    alias_run_id: str | None = None,
    expected_candidate_sha256: str | None = None,
    reviewed_by: str | None = None,
    sample_limit: int = 20,
    timeout: str = "10min",
    cancel_check: Callable[[], Awaitable[None]] | None = None,
) -> NumericGridAliasResult:
    """Run a sealed shadow or apply its exact reviewed candidate set."""
    operation = address_alias_sql.numeric_grid_alias_mode(mode)
    db_schema = schema or os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"
    if operation == "off":
        async with db.transaction() as session:
            _, _, generation = await _alias_state(session, schema=db_schema, lock=False)
        return NumericGridAliasResult(
            run_id=None,
            mode=operation,
            status="off",
            candidate_digest=None,
            source_count=0,
            candidate_sources=0,
            candidate_rows=0,
            no_candidate=0,
            active_skipped=0,
            eligible=0,
            ambiguous=0,
            insufficient_provenance=0,
            promoted=0,
            generation=generation,
            sample_rows=[],
        )

    normalized_state, normalized_zip = _normalize_scope(state_code, zip_prefix)
    normalized_timeout = _statement_timeout(timeout)
    reviewed_digest = None
    reviewer = None
    shadow: dict[str, Any] | None = None
    if operation == "apply":
        shadow_run_id = str(alias_run_id or "").strip()
        try:
            uuid.UUID(shadow_run_id)
        except (ValueError, TypeError):
            raise ValueError("apply requires a valid alias_run_id") from None
        reviewed_digest = _reviewed_digest(expected_candidate_sha256)
        reviewer = _reviewer(reviewed_by)
        shadow = await _load_reviewed_shadow(
            schema=db_schema,
            shadow_run_id=shadow_run_id,
            expected_digest=reviewed_digest,
        )
        if normalized_state not in {None, shadow.get("scope_state_code")}:
            raise ValueError("apply state scope differs from the reviewed shadow")
        if normalized_zip not in {None, shadow.get("scope_zip_prefix")}:
            raise ValueError("apply ZIP scope differs from the reviewed shadow")
        normalized_state = shadow.get("scope_state_code")
        normalized_zip = shadow.get("scope_zip_prefix")
    else:
        shadow_run_id = None

    run_id = str(uuid.uuid4())
    await _insert_run(
        schema=db_schema,
        run_id=run_id,
        mode=operation,
        state_code=normalized_state,
        zip_prefix=normalized_zip,
        shadow_run_id=shadow_run_id,
        reviewed_digest=reviewed_digest,
        reviewed_by=reviewer,
    )
    try:
        if cancel_check:
            await cancel_check()
        async with db.transaction() as session:
            isolation = "READ COMMITTED" if operation == "apply" else "REPEATABLE READ"
            await session.execute(text(f"SET TRANSACTION ISOLATION LEVEL {isolation};"))
            await session.execute(
                text(f"SET LOCAL lock_timeout = '{normalized_timeout}';")
            )
            await session.execute(
                text(f"SET LOCAL statement_timeout = '{normalized_timeout}';")
            )
            selected_archive_table = archive_table_name()
            if operation == "apply":
                await session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:lock_key));"),
                    {
                        "lock_key": _archive_lock_key(
                            db_schema,
                            selected_archive_table,
                            "resolve",
                        )
                    },
                )
            await session.execute(
                text(address_alias_sql.alias_advisory_xact_lock_sql()),
            )
            if operation == "apply":
                await session.execute(
                    text(
                        f"LOCK TABLE {_relation(db_schema, address_alias_sql.ADDRESS_ALIAS_CANDIDATE_TABLE)} "
                        "IN SHARE MODE;"
                    )
                )
            _, _, generation = await _alias_state(session, schema=db_schema, lock=True)
            owned_run = (
                await session.execute(
                    text(
                        f"""
                        SELECT status
                        FROM {_relation(db_schema, address_alias_sql.ADDRESS_ALIAS_RUN_TABLE)}
                        WHERE run_id = CAST(:run_id AS uuid)
                          AND status = 'running'
                        FOR UPDATE;
                        """
                    ),
                    {"run_id": run_id},
                )
            ).first()
            if owned_run is None:
                raise RuntimeError("numeric-grid alias run is no longer running")
            if operation == "apply":
                locked_shadow = (
                    await session.execute(
                        text(
                            f"""
                            SELECT status, candidate_digest
                            FROM {_relation(db_schema, address_alias_sql.ADDRESS_ALIAS_RUN_TABLE)}
                            WHERE run_id = CAST(:shadow_run_id AS uuid)
                              AND mode = 'shadow'
                            FOR SHARE;
                            """
                        ),
                        {"shadow_run_id": shadow_run_id},
                    )
                ).first()
                if (
                    locked_shadow is None
                    or locked_shadow.status != "sealed"
                    or locked_shadow.candidate_digest != reviewed_digest
                ):
                    raise RuntimeError("reviewed shadow changed before apply")
                revoked_history = (
                    await session.execute(
                        text(
                            address_alias_sql.revoked_shadow_alias_sql(
                                schema=db_schema,
                            )
                        ),
                        {"shadow_run_id": shadow_run_id},
                    )
                ).first()
                if revoked_history:
                    raise RuntimeError(
                        "reviewed shadow contains a revoked alias; run a new shadow "
                        f"before apply: source={revoked_history.source_address_key}"
                    )
                reviewed_candidate_rows = (
                    await session.execute(
                        text(
                            address_alias_sql.candidate_rows_sql(schema=db_schema)
                            .strip()
                            .removesuffix(";")
                            + " FOR SHARE;"
                        ),
                        {"run_id": shadow_run_id},
                    )
                ).all()
                if _candidate_digest(reviewed_candidate_rows) != reviewed_digest:
                    raise RuntimeError(
                        "reviewed shadow candidate rows no longer match the sealed digest"
                    )
            archive = _relation(db_schema, selected_archive_table)
            metrics, digest, samples, archive_rows, source_count = await _shadow_run(
                session,
                schema=db_schema,
                archive=archive,
                run_id=run_id,
                state_code=normalized_state,
                zip_prefix=normalized_zip,
                sample_limit=sample_limit,
                retry_shadow_run_id=shadow_run_id,
            )
            if cancel_check:
                await cancel_check()
            if operation == "apply" and digest != reviewed_digest:
                raise RuntimeError("candidate set changed after review; run a new shadow")

            promoted = 0
            final_generation = generation
            if operation == "apply":
                conflict = (
                    await session.execute(
                        text(address_alias_sql.active_alias_conflict_sql(schema=db_schema)),
                        {"apply_run_id": run_id},
                    )
                ).first()
                if conflict:
                    raise RuntimeError(
                        "active alias target conflicts with reviewed candidate: "
                        f"source={conflict.source_address_key} "
                        f"active={conflict.active_target_address_key} "
                        f"candidate={conflict.candidate_target_address_key}"
                    )
                await _approve_shadow_candidates(
                    session,
                    schema=db_schema,
                    shadow_run_id=shadow_run_id,
                    reviewer=reviewer or "",
                )
                promotion = (
                    await session.execute(
                        text(address_alias_sql.promote_reviewed_aliases_sql(schema=db_schema)),
                        {
                            "shadow_run_id": shadow_run_id,
                            "apply_run_id": run_id,
                            "candidate_digest": digest,
                        },
                    )
                ).first()
                promoted = int(promotion.promoted_count or 0)
                _, _, final_generation = await _alias_state(
                    session,
                    schema=db_schema,
                    lock=False,
                )

            final_status = "applied" if operation == "apply" else "sealed"
            runs = _relation(db_schema, address_alias_sql.ADDRESS_ALIAS_RUN_TABLE)
            reason_buckets = {
                "eligible": metrics.get("eligible", 0),
                "ambiguous": metrics.get("ambiguous", 0),
                "no_candidate": metrics.get("no_candidate", 0),
                "active_skipped": metrics.get("active_skipped", 0),
                "insufficient_provenance": metrics.get(
                    "insufficient_provenance",
                    0,
                ),
            }
            await session.execute(
                text(
                    f"""
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
                ),
                {
                    "status": final_status,
                    "candidate_digest": digest,
                    "archive_rows": archive_rows,
                    "source_count": source_count,
                    "candidate_sources": metrics.get("candidate_sources", 0),
                    "candidate_rows": metrics.get("candidate_rows", 0),
                    "no_candidate": metrics.get("no_candidate", 0),
                    "active_skipped": metrics.get("active_skipped", 0),
                    "eligible": metrics.get("eligible", 0),
                    "ambiguous": metrics.get("ambiguous", 0),
                    "insufficient": metrics.get("insufficient_provenance", 0),
                    "reason_buckets": json.dumps(reason_buckets, sort_keys=True),
                    "sample_rows": json.dumps(samples, sort_keys=True),
                    "run_id": run_id,
                },
            )
        return NumericGridAliasResult(
            run_id=run_id,
            mode=operation,
            status=final_status,
            candidate_digest=digest,
            source_count=source_count,
            candidate_sources=metrics.get("candidate_sources", 0),
            candidate_rows=metrics.get("candidate_rows", 0),
            no_candidate=metrics.get("no_candidate", 0),
            active_skipped=metrics.get("active_skipped", 0),
            eligible=metrics.get("eligible", 0),
            ambiguous=metrics.get("ambiguous", 0),
            insufficient_provenance=metrics.get("insufficient_provenance", 0),
            promoted=promoted,
            generation=final_generation,
            sample_rows=samples,
        )
    except Exception as exc:
        await _mark_failed(db_schema, run_id, exc)
        raise
