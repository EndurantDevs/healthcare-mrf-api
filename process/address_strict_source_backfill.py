# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reviewed, target-only bootstrap of strict address source evidence."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from sqlalchemy import text

from db.models import db
from process.address_numeric_grid_alias_store import (
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


@dataclass(frozen=True)
class StrictSourceBackfillRequest:
    alias_run_id: str
    expected_candidate_sha256: str
    reviewed_by: str
    schema: str | None = None
    max_targets: int = 256
    timeout: str = "10min"
    cancel_check: Callable[[], Awaitable[None]] | None = None


@dataclass
class _BackfillExecution:
    request: StrictSourceBackfillRequest
    schema: str
    shadow_run_id: str
    reviewed_digest: str
    reviewer: str
    target_limit: int
    timeout: str
    shadow_by_field: dict[str, Any]
    run_id: str
    archive: str = ""
    generation: int = 0
    target_count: int = 0
    evidence_digest: str = ""
    evidence_target_count: int = 0
    evidence_pair_count: int = 0
    updated_target_count: int = 0
    target_count_by_source: dict[str, int] = field(default_factory=dict)
    missing_relations: list[str] = field(default_factory=list)


def _target_limit(value: int) -> int:
    limit = int(value)
    if limit < 1 or limit > 10_000:
        raise ValueError("max_targets must be between 1 and 10000")
    return limit


async def _has_relation(
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
    has_index = bool(
        (
            await session.execute(
                text(
                    address_strict_source_backfill_sql.address_key_index_exists_sql()
                ),
                {"schema_name": schema, "table_name": table},
            )
        ).scalar()
    )
    if not has_index:
        raise RuntimeError(
            f"strict source backfill requires a valid leading address_key index: "
            f"{schema}.{table}"
        )


class _StrictSourceBackfillRunner:
    def __init__(self, request: StrictSourceBackfillRequest) -> None:
        self.request = request
        self.execution: _BackfillExecution | None = None

    async def execute(self) -> StrictSourceBackfillResult:
        """Execute one reviewed target-evidence backfill."""
        self.execution = await self._prepare_execution()
        await self._insert_execution_run()
        try:
            await self._check_cancelled()
            async with db.transaction() as session:
                await self._execute_locked(session)
            return self._result()
        except Exception as exc:
            execution = self._required_execution()
            await _mark_failed(execution.schema, execution.run_id, exc)
            raise

    async def _prepare_execution(self) -> _BackfillExecution:
        schema = (
            self.request.schema
            or os.getenv("HLTHPRT_DB_SCHEMA")
            or os.getenv("DB_SCHEMA")
            or "mrf"
        )
        shadow_run_id = str(self.request.alias_run_id or "").strip()
        try:
            uuid.UUID(shadow_run_id)
        except (ValueError, TypeError):
            raise ValueError("backfill requires a valid alias_run_id") from None
        reviewed_digest = _reviewed_digest(self.request.expected_candidate_sha256)
        shadow_by_field = await _load_reviewed_shadow(
            schema=schema,
            shadow_run_id=shadow_run_id,
            expected_digest=reviewed_digest,
        )
        return _BackfillExecution(
            request=self.request,
            schema=schema,
            shadow_run_id=shadow_run_id,
            reviewed_digest=reviewed_digest,
            reviewer=_reviewer(self.request.reviewed_by),
            target_limit=_target_limit(self.request.max_targets),
            timeout=_statement_timeout(self.request.timeout),
            shadow_by_field=shadow_by_field,
            run_id=str(uuid.uuid4()),
        )

    async def _insert_execution_run(self) -> None:
        execution = self._required_execution()
        await _insert_run(
            schema=execution.schema,
            run_id=execution.run_id,
            mode="backfill",
            state_code=execution.shadow_by_field.get("scope_state_code"),
            zip_prefix=execution.shadow_by_field.get("scope_zip_prefix"),
            shadow_run_id=execution.shadow_run_id,
            reviewed_digest=execution.reviewed_digest,
            reviewed_by=execution.reviewer,
        )

    async def _execute_locked(self, session: Any) -> None:
        await self._configure_and_lock(session)
        await self._validate_reviewed_shadow(session)
        await self._materialize_targets(session)
        await self._collect_source_evidence(session)
        await self._seal_run(session)

    async def _configure_and_lock(self, session: Any) -> None:
        execution = self._required_execution()
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;"))
        await session.execute(text(f"SET LOCAL lock_timeout = '{execution.timeout}';"))
        await session.execute(
            text(f"SET LOCAL statement_timeout = '{execution.timeout}';")
        )
        archive_name = archive_table_name()
        execution.archive = _relation(execution.schema, archive_name)
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:lock_key));"),
            {
                "lock_key": _archive_lock_key(
                    execution.schema,
                    archive_name,
                    "resolve",
                )
            },
        )
        await session.execute(text(address_alias_sql.alias_advisory_xact_lock_sql()))
        await session.execute(
            text(
                address_strict_source_backfill_sql.lock_candidates_sql(
                    schema=execution.schema
                )
            )
        )
        _, _, execution.generation = await _alias_state(
            session,
            schema=execution.schema,
            lock=True,
        )

    async def _validate_reviewed_shadow(self, session: Any) -> None:
        execution = self._required_execution()
        runs = _relation(execution.schema, address_alias_sql.ADDRESS_ALIAS_RUN_TABLE)
        shadow_record = (
            await session.execute(
                text(
                    f"""
                    SELECT status, candidate_digest
                    FROM {runs}
                    WHERE run_id = CAST(:shadow_run_id AS uuid)
                      AND mode = 'shadow'
                    FOR SHARE;
                    """
                ),
                {"shadow_run_id": execution.shadow_run_id},
            )
        ).first()
        if (
            shadow_record is None
            or shadow_record.status != "sealed"
            or shadow_record.candidate_digest != execution.reviewed_digest
        ):
            raise RuntimeError(
                "reviewed shadow changed before strict evidence backfill"
            )
        await session.execute(
            text(
                address_strict_source_backfill_sql.create_reviewed_candidates_sql(
                    schema=execution.schema
                )
            ),
            {"shadow_run_id": execution.shadow_run_id},
        )
        candidate_records = (
            await session.execute(
                text(address_strict_source_backfill_sql.reviewed_candidate_rows_sql())
            )
        ).all()
        if _candidate_digest(candidate_records) != execution.reviewed_digest:
            raise RuntimeError(
                "reviewed shadow candidate rows no longer match the sealed digest"
            )
        await self._reject_drifted_targets(session)

    async def _reject_drifted_targets(self, session: Any) -> None:
        execution = self._required_execution()
        drifted_target_count = int(
            (
                await session.execute(
                    text(
                        address_strict_source_backfill_sql.drifted_target_count_sql(
                            archive=execution.archive
                        )
                    )
                )
            ).scalar()
            or 0
        )
        if drifted_target_count:
            raise RuntimeError(
                "reviewed shadow target identity or merge state changed; run a new shadow"
            )

    async def _materialize_targets(self, session: Any) -> None:
        execution = self._required_execution()
        await session.execute(
            text(
                address_strict_source_backfill_sql.create_targets_sql(
                    archive=execution.archive
                )
            )
        )
        await session.execute(
            text(address_strict_source_backfill_sql.create_target_index_sql())
        )
        await session.execute(
            text(address_strict_source_backfill_sql.analyze_targets_sql())
        )
        execution.target_count = int(
            (
                await session.execute(
                    text(address_strict_source_backfill_sql.target_count_sql())
                )
            ).scalar()
            or 0
        )
        if execution.target_count > execution.target_limit:
            raise RuntimeError(
                f"strict source backfill target count {execution.target_count} exceeds "
                f"max_targets {execution.target_limit}"
            )
        await self._check_cancelled()

    async def _collect_source_evidence(self, session: Any) -> None:
        execution = self._required_execution()
        await session.execute(
            text(address_strict_source_backfill_sql.create_evidence_sql())
        )
        for projection in address_strict_source_backfill_sql.SOURCE_PROJECTIONS:
            if not await _has_relation(
                session,
                schema=execution.schema,
                table=projection.table,
            ):
                execution.missing_relations.append(projection.table)
                continue
            await _assert_address_key_index(
                session,
                schema=execution.schema,
                table=projection.table,
            )
            await session.execute(
                text(
                    address_strict_source_backfill_sql.evidence_insert_sql(
                        schema=execution.schema,
                        projection=projection,
                    )
                )
            )
            await self._check_cancelled()
        await self._read_and_apply_evidence(session)

    async def _read_and_apply_evidence(self, session: Any) -> None:
        execution = self._required_execution()
        evidence_records = (
            await session.execute(
                text(address_strict_source_backfill_sql.evidence_rows_sql())
            )
        ).all()
        execution.evidence_digest = _candidate_digest(evidence_records)
        execution.evidence_target_count = await self._scalar_count(
            session,
            address_strict_source_backfill_sql.evidence_target_count_sql(),
        )
        execution.evidence_pair_count = await self._scalar_count(
            session,
            address_strict_source_backfill_sql.evidence_pair_count_sql(),
        )
        metric_records = (
            await session.execute(
                text(address_strict_source_backfill_sql.evidence_metrics_sql())
            )
        ).all()
        execution.target_count_by_source = {
            str(metric_record.source_name): int(metric_record.target_count or 0)
            for metric_record in metric_records
        }
        execution.updated_target_count = await self._scalar_count(
            session,
            address_strict_source_backfill_sql.apply_evidence_sql(
                archive=execution.archive
            ),
        )

    async def _seal_run(self, session: Any) -> None:
        execution = self._required_execution()
        runs = _relation(execution.schema, address_alias_sql.ADDRESS_ALIAS_RUN_TABLE)
        reason_map = {
            "evidence_target_count": execution.evidence_target_count,
            "evidence_pair_count": execution.evidence_pair_count,
            "updated_target_count": execution.updated_target_count,
            "source_target_counts": execution.target_count_by_source,
            "missing_relations": sorted(execution.missing_relations),
        }
        await session.execute(
            text(
                f"""
                UPDATE {runs}
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
                "evidence_digest": execution.evidence_digest,
                "target_count": execution.target_count,
                "evidence_target_count": execution.evidence_target_count,
                "evidence_pair_count": execution.evidence_pair_count,
                "updated_target_count": execution.updated_target_count,
                "reason_buckets": json.dumps(reason_map, sort_keys=True),
                "run_id": execution.run_id,
            },
        )

    async def _scalar_count(self, session: Any, sql: str) -> int:
        return int((await session.execute(text(sql))).scalar() or 0)

    async def _check_cancelled(self) -> None:
        if self.request.cancel_check:
            await self.request.cancel_check()

    def _result(self) -> StrictSourceBackfillResult:
        execution = self._required_execution()
        return StrictSourceBackfillResult(
            run_id=execution.run_id,
            status="backfilled",
            reviewed_shadow_run_id=execution.shadow_run_id,
            reviewed_candidate_digest=execution.reviewed_digest,
            evidence_digest=execution.evidence_digest,
            target_count=execution.target_count,
            evidence_target_count=execution.evidence_target_count,
            evidence_pair_count=execution.evidence_pair_count,
            updated_target_count=execution.updated_target_count,
            source_target_counts=execution.target_count_by_source,
            missing_relations=sorted(execution.missing_relations),
            generation=execution.generation,
        )

    def _required_execution(self) -> _BackfillExecution:
        if self.execution is None:
            raise RuntimeError("strict source backfill execution was not prepared")
        return self.execution


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
    request = StrictSourceBackfillRequest(
        alias_run_id=alias_run_id,
        expected_candidate_sha256=expected_candidate_sha256,
        reviewed_by=reviewed_by,
        schema=schema,
        max_targets=max_targets,
        timeout=timeout,
        cancel_check=cancel_check,
    )
    return await _StrictSourceBackfillRunner(request).execute()
