# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reviewed, target-only bootstrap of strict address source evidence."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from sqlalchemy import text

from db.models import db
from process.address_numeric_grid_alias import (
    _alias_state,
    _insert_run,
    _load_reviewed_shadow,
    _mark_failed,
)
from process.address_numeric_grid_alias_support import (
    _candidate_digest,
    _relation,
    _reviewed_digest,
    _reviewer,
    _statement_timeout,
)
from process.ext import address_alias_sql, address_strict_source_backfill_sql
from process.ext.address_canon import _archive_lock_key, archive_table_name


@dataclass(frozen=True)
class StrictSourceBackfillResult:
    run_id: str
    status: str
    reviewed_shadow_run_id: str
    reviewed_candidate_digest: str
    evidence_digest: str
    target_count: int
    evidence_target_count: int
    evidence_pair_count: int
    updated_target_count: int
    source_target_counts: dict[str, int]
    missing_relations: list[str]
    generation: int


def _target_limit(value: int) -> int:
    limit = int(value)
    if limit < 1 or limit > 10_000:
        raise ValueError("max_targets must be between 1 and 10000")
    return limit


async def _relation_exists(
    session: Any,
    *,
    schema: str,
    table: str,
) -> bool:
    return bool(
        (
            await session.execute(
                text(address_strict_source_backfill_sql.relation_exists_sql()),
                {"qualified_relation": f"{schema}.{table}"},
            )
        ).scalar()
    )


async def _assert_address_key_index(
    session: Any,
    *,
    schema: str,
    table: str,
) -> None:
    indexed = bool(
        (
            await session.execute(
                text(
                    address_strict_source_backfill_sql.address_key_index_exists_sql()
                ),
                {"schema_name": schema, "table_name": table},
            )
        ).scalar()
    )
    if not indexed:
        raise RuntimeError(
            f"strict source backfill requires a valid leading address_key index: "
            f"{schema}.{table}"
        )


async def run_strict_source_backfill(
    *,
    alias_run_id: str,
    expected_candidate_sha256: str,
    reviewed_by: str,
    schema: str | None = None,
    max_targets: int = 256,
    timeout: str = "10min",
    cancel_check: Callable[[], Awaitable[None]] | None = None,
) -> StrictSourceBackfillResult:
    """Attest only target keys from one exact sealed numeric-grid shadow."""
    db_schema = schema or os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"
    shadow_run_id = str(alias_run_id or "").strip()
    try:
        uuid.UUID(shadow_run_id)
    except (ValueError, TypeError):
        raise ValueError("backfill requires a valid alias_run_id") from None
    reviewed_digest = _reviewed_digest(expected_candidate_sha256)
    reviewer = _reviewer(reviewed_by)
    target_limit = _target_limit(max_targets)
    normalized_timeout = _statement_timeout(timeout)
    shadow = await _load_reviewed_shadow(
        schema=db_schema,
        shadow_run_id=shadow_run_id,
        expected_digest=reviewed_digest,
    )
    run_id = str(uuid.uuid4())
    await _insert_run(
        schema=db_schema,
        run_id=run_id,
        mode="backfill",
        state_code=shadow.get("scope_state_code"),
        zip_prefix=shadow.get("scope_zip_prefix"),
        shadow_run_id=shadow_run_id,
        reviewed_digest=reviewed_digest,
        reviewed_by=reviewer,
    )
    try:
        if cancel_check:
            await cancel_check()
        async with db.transaction() as session:
            # Cooperative archive writers are fenced before evidence reads.
            # READ COMMITTED ensures a waiter sees commits made before those
            # advisory locks were acquired rather than retaining a stale
            # pre-wait snapshot.
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;"))
            await session.execute(
                text(f"SET LOCAL lock_timeout = '{normalized_timeout}';")
            )
            await session.execute(
                text(f"SET LOCAL statement_timeout = '{normalized_timeout}';")
            )
            selected_archive_table = archive_table_name()
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
            await session.execute(text(address_alias_sql.alias_advisory_xact_lock_sql()))
            await session.execute(
                text(
                    address_strict_source_backfill_sql.lock_candidates_sql(
                        schema=db_schema
                    )
                )
            )
            _, _, generation = await _alias_state(
                session,
                schema=db_schema,
                lock=True,
            )
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
                raise RuntimeError("reviewed shadow changed before strict evidence backfill")

            archive = _relation(db_schema, selected_archive_table)
            await session.execute(
                text(
                    address_strict_source_backfill_sql.create_reviewed_candidates_sql(
                        schema=db_schema
                    )
                ),
                {"shadow_run_id": shadow_run_id},
            )
            reviewed_candidate_rows = (
                await session.execute(
                    text(
                        address_strict_source_backfill_sql.reviewed_candidate_rows_sql()
                    )
                )
            ).all()
            if _candidate_digest(reviewed_candidate_rows) != reviewed_digest:
                raise RuntimeError(
                    "reviewed shadow candidate rows no longer match the sealed digest"
                )
            drifted_target_count = int(
                (
                    await session.execute(
                        text(
                            address_strict_source_backfill_sql.drifted_target_count_sql(
                                archive=archive
                            )
                        )
                    )
                ).scalar()
                or 0
            )
            if drifted_target_count:
                raise RuntimeError(
                    "reviewed shadow target identity or merge state changed; "
                    "run a new shadow"
                )
            await session.execute(
                text(
                    address_strict_source_backfill_sql.create_targets_sql(
                        archive=archive,
                    )
                )
            )
            await session.execute(
                text(address_strict_source_backfill_sql.create_target_index_sql())
            )
            await session.execute(
                text(address_strict_source_backfill_sql.analyze_targets_sql())
            )
            target_count = int(
                (
                    await session.execute(
                        text(address_strict_source_backfill_sql.target_count_sql())
                    )
                ).scalar()
                or 0
            )
            if target_count > target_limit:
                raise RuntimeError(
                    f"strict source backfill target count {target_count} exceeds "
                    f"max_targets {target_limit}"
                )
            if cancel_check:
                await cancel_check()

            await session.execute(
                text(address_strict_source_backfill_sql.create_evidence_sql())
            )
            missing_relations: list[str] = []
            for projection in address_strict_source_backfill_sql.SOURCE_PROJECTIONS:
                if not await _relation_exists(
                    session,
                    schema=db_schema,
                    table=projection.table,
                ):
                    missing_relations.append(projection.table)
                    continue
                await _assert_address_key_index(
                    session,
                    schema=db_schema,
                    table=projection.table,
                )
                await session.execute(
                    text(
                        address_strict_source_backfill_sql.evidence_insert_sql(
                            schema=db_schema,
                            projection=projection,
                        )
                    )
                )
                if cancel_check:
                    await cancel_check()

            evidence_rows = (
                await session.execute(
                    text(address_strict_source_backfill_sql.evidence_rows_sql())
                )
            ).all()
            evidence_digest = _candidate_digest(evidence_rows)
            evidence_target_count = int(
                (
                    await session.execute(
                        text(
                            address_strict_source_backfill_sql.evidence_target_count_sql()
                        )
                    )
                ).scalar()
                or 0
            )
            evidence_pair_count = int(
                (
                    await session.execute(
                        text(
                            address_strict_source_backfill_sql.evidence_pair_count_sql()
                        )
                    )
                ).scalar()
                or 0
            )
            metric_rows = (
                await session.execute(
                    text(address_strict_source_backfill_sql.evidence_metrics_sql())
                )
            ).all()
            source_target_counts = {
                str(row.source_name): int(row.target_count or 0) for row in metric_rows
            }
            updated_target_count = int(
                (
                    await session.execute(
                        text(
                            address_strict_source_backfill_sql.apply_evidence_sql(
                                archive=archive
                            )
                        )
                    )
                ).scalar()
                or 0
            )
            reason_buckets = {
                "evidence_target_count": evidence_target_count,
                "evidence_pair_count": evidence_pair_count,
                "updated_target_count": updated_target_count,
                "source_target_counts": source_target_counts,
                "missing_relations": sorted(missing_relations),
            }
            await session.execute(
                text(
                    f"""
                    UPDATE {_relation(db_schema, address_alias_sql.ADDRESS_ALIAS_RUN_TABLE)}
                       SET status = 'backfilled',
                           evidence_digest = :evidence_digest,
                           backfill_target_count = :target_count,
                           evidence_target_count = :evidence_target_count,
                           evidence_pair_count = :evidence_pair_count,
                           provenance_update_count = :updated_target_count,
                           reviewed_at = now(),
                           reason_buckets = CAST(:reason_buckets AS jsonb),
                           completed_at = now()
                     WHERE run_id = CAST(:run_id AS uuid);
                    """
                ),
                {
                    "evidence_digest": evidence_digest,
                    "target_count": target_count,
                    "evidence_target_count": evidence_target_count,
                    "evidence_pair_count": evidence_pair_count,
                    "updated_target_count": updated_target_count,
                    "reason_buckets": json.dumps(reason_buckets, sort_keys=True),
                    "run_id": run_id,
                },
            )
        return StrictSourceBackfillResult(
            run_id=run_id,
            status="backfilled",
            reviewed_shadow_run_id=shadow_run_id,
            reviewed_candidate_digest=reviewed_digest,
            evidence_digest=evidence_digest,
            target_count=target_count,
            evidence_target_count=evidence_target_count,
            evidence_pair_count=evidence_pair_count,
            updated_target_count=updated_target_count,
            source_target_counts=source_target_counts,
            missing_relations=sorted(missing_relations),
            generation=generation,
        )
    except Exception as exc:
        await _mark_failed(db_schema, run_id, exc)
        raise
