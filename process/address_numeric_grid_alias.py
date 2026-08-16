# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reviewed offline discovery and activation for numeric-grid address aliases."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text

from db.models import db
from process.address_numeric_grid_alias_store import (
    _alias_state,
    _approve_shadow_candidates,
    _completion_parameters,
    _insert_run,
    _load_reviewed_shadow,
    _mark_failed,
    _run_completion_sql,
    _shadow_run,
)
from process.address_numeric_grid_alias_support import (
    NumericGridAliasRequest,
    NumericGridAliasResult,
    _candidate_digest,
    _normalize_scope,
    _relation,
    _reviewed_digest,
    _reviewer,
    _statement_timeout,
)
from process.ext import (
    address_alias_snapshot_sql,
    address_alias_sql,
    address_evidence_alias_sql,
)
from process.ext.address_canon import _archive_lock_key, archive_table_name


@dataclass
class _AliasExecution:
    request: NumericGridAliasRequest
    operation: str
    schema: str
    state_code: str | None
    zip_prefix: str | None
    timeout: str
    run_id: str
    alias_kind: str
    ruleset_version: int
    shadow_run_id: str | None = None
    reviewed_digest: str | None = None
    reviewer: str | None = None
    archive: str = ""
    generation: int = 0
    final_generation: int = 0
    archive_rows: int = 0
    source_count: int = 0
    promoted: int = 0
    digest: str = ""
    final_status: str = ""
    metrics_by_reason: dict[str, int] = field(default_factory=dict)
    sample_rows: list[dict[str, Any]] = field(default_factory=list)


class _NumericGridAliasRunner:
    def __init__(self, request: NumericGridAliasRequest) -> None:
        self.request = request
        self.execution: _AliasExecution | None = None

    async def execute(self) -> NumericGridAliasResult:
        """Execute one validated alias workflow request."""
        operation = address_alias_sql.numeric_grid_alias_mode(self.request.mode)
        alias_kind = str(self.request.alias_kind or "").strip()
        ruleset_version = address_alias_sql.alias_ruleset(alias_kind)
        schema = (
            self.request.schema
            or os.getenv("HLTHPRT_DB_SCHEMA")
            or os.getenv("DB_SCHEMA")
            or "mrf"
        )
        if operation == "off":
            return await self._off_result(schema, alias_kind)
        self.execution = await self._prepare_execution(
            operation,
            schema,
            alias_kind,
            ruleset_version,
        )
        await self._insert_execution_run()
        try:
            await self._check_cancelled()
            async with db.transaction() as session:
                await self._execute_locked(session)
            return self._result()
        except Exception as exc:
            await _mark_failed(schema, self.execution.run_id, exc)
            raise

    async def _off_result(
        self,
        schema: str,
        alias_kind: str,
    ) -> NumericGridAliasResult:
        async with db.transaction() as session:
            _, _, generation = await _alias_state(session, schema=schema, lock=False)
        return NumericGridAliasResult(
            run_id=None,
            mode="off",
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
            alias_kind=alias_kind,
        )

    async def _prepare_execution(
        self,
        operation: str,
        schema: str,
        alias_kind: str = address_alias_sql.NUMERIC_GRID_ALIAS_KIND,
        ruleset_version: int = address_alias_sql.NUMERIC_GRID_ALIAS_RULESET_VERSION,
    ) -> _AliasExecution:
        state_code, zip_prefix = _normalize_scope(
            self.request.state_code,
            self.request.zip_prefix,
        )
        execution = _AliasExecution(
            request=self.request,
            operation=operation,
            schema=schema,
            state_code=state_code,
            zip_prefix=zip_prefix,
            timeout=_statement_timeout(self.request.timeout),
            run_id=str(uuid.uuid4()),
            alias_kind=alias_kind,
            ruleset_version=ruleset_version,
        )
        if operation == "apply":
            await self._bind_reviewed_shadow(execution)
        return execution

    async def _bind_reviewed_shadow(self, execution: _AliasExecution) -> None:
        shadow_run_id = str(self.request.alias_run_id or "").strip()
        try:
            uuid.UUID(shadow_run_id)
        except (ValueError, TypeError):
            raise ValueError("apply requires a valid alias_run_id") from None
        reviewed_digest = _reviewed_digest(self.request.expected_candidate_sha256)
        shadow_by_field = await _load_reviewed_shadow(
            schema=execution.schema,
            shadow_run_id=shadow_run_id,
            expected_digest=reviewed_digest,
            alias_kind=execution.alias_kind,
            ruleset_version=execution.ruleset_version,
        )
        if execution.state_code not in {None, shadow_by_field.get("scope_state_code")}:
            raise ValueError("apply state scope differs from the reviewed shadow")
        if execution.zip_prefix not in {None, shadow_by_field.get("scope_zip_prefix")}:
            raise ValueError("apply ZIP scope differs from the reviewed shadow")
        execution.state_code = shadow_by_field.get("scope_state_code")
        execution.zip_prefix = shadow_by_field.get("scope_zip_prefix")
        execution.shadow_run_id = shadow_run_id
        execution.reviewed_digest = reviewed_digest
        execution.reviewer = _reviewer(self.request.reviewed_by)

    async def _insert_execution_run(self) -> None:
        execution = self._required_execution()
        await _insert_run(
            run_by_field={
                "schema": execution.schema,
                "run_id": execution.run_id,
                "mode": execution.operation,
                "state_code": execution.state_code,
                "zip_prefix": execution.zip_prefix,
                "shadow_run_id": execution.shadow_run_id,
                "reviewed_digest": execution.reviewed_digest,
                "reviewed_by": execution.reviewer,
                "alias_kind": execution.alias_kind,
                "ruleset_version": execution.ruleset_version,
            },
        )

    async def _execute_locked(self, session: Any) -> None:
        await self._configure_and_lock(session)
        await self._lock_owned_run(session)
        await self._validate_reviewed_shadow(session)
        await self._collect_candidate_snapshot(session)
        await self._promote_reviewed_candidates(session)
        await self._seal_run(session)

    async def _configure_and_lock(self, session: Any) -> None:
        execution = self._required_execution()
        isolation = (
            "READ COMMITTED" if execution.operation == "apply" else "REPEATABLE READ"
        )
        await session.execute(text(f"SET TRANSACTION ISOLATION LEVEL {isolation};"))
        await session.execute(text(f"SET LOCAL lock_timeout = '{execution.timeout}';"))
        await session.execute(
            text(f"SET LOCAL statement_timeout = '{execution.timeout}';")
        )
        archive_name = archive_table_name()
        execution.archive = _relation(execution.schema, archive_name)
        if execution.operation == "apply":
            await session.execute(
                text("SELECT pg_advisory_xact_lock(hashtext(:lock_key));"),
                {"lock_key": _archive_lock_key(execution.schema, archive_name, "resolve")},
            )
        await session.execute(text(address_alias_sql.alias_advisory_xact_lock_sql()))
        if execution.operation == "apply":
            candidates = _relation(
                execution.schema,
                address_alias_sql.ADDRESS_ALIAS_CANDIDATE_TABLE,
            )
            await session.execute(text(f"LOCK TABLE {candidates} IN SHARE MODE;"))
        _, _, execution.generation = await _alias_state(
            session,
            schema=execution.schema,
            lock=True,
        )
        execution.final_generation = execution.generation

    async def _lock_owned_run(self, session: Any) -> None:
        execution = self._required_execution()
        runs = _relation(execution.schema, address_alias_sql.ADDRESS_ALIAS_RUN_TABLE)
        owned_run = (
            await session.execute(
                text(
                    f"""
                    SELECT status
                    FROM {runs}
                    WHERE run_id = CAST(:run_id AS uuid)
                      AND status = 'running'
                    FOR UPDATE;
                    """
                ),
                {"run_id": execution.run_id},
            )
        ).first()
        if owned_run is None:
            raise RuntimeError("address alias run is no longer running")

    async def _validate_reviewed_shadow(self, session: Any) -> None:
        """Recheck the reviewed policy, digest, and revocation fences."""
        execution = self._required_execution()
        if execution.operation != "apply":
            return
        runs = _relation(execution.schema, address_alias_sql.ADDRESS_ALIAS_RUN_TABLE)
        shadow_record = (
            await session.execute(
                text(
                    f"""
                    SELECT status, candidate_digest, alias_kind, ruleset_version
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
            or shadow_record.alias_kind != execution.alias_kind
            or int(shadow_record.ruleset_version) != execution.ruleset_version
        ):
            raise RuntimeError("reviewed shadow changed before apply")
        await self._reject_revoked_shadow(session)
        candidate_records = (
            await session.execute(
                text(
                    (
                        address_alias_snapshot_sql.evidence_candidate_rows_sql
                        if execution.alias_kind
                        == address_alias_sql.EVIDENCE_ADDRESS_MATCH_ALIAS_KIND
                        else address_alias_snapshot_sql.candidate_rows_sql
                    )(schema=execution.schema)
                    .strip()
                    .removesuffix(";")
                    + " FOR SHARE;"
                ),
                {"run_id": execution.shadow_run_id},
            )
        ).all()
        if _candidate_digest(candidate_records) != execution.reviewed_digest:
            raise RuntimeError(
                "reviewed shadow candidate rows no longer match the sealed digest"
            )

    async def _reject_revoked_shadow(self, session: Any) -> None:
        execution = self._required_execution()
        revoked_record = (
            await session.execute(
                text(
                    address_alias_sql.revoked_shadow_alias_sql(
                        schema=execution.schema,
                    )
                ),
                {"shadow_run_id": execution.shadow_run_id},
            )
        ).first()
        if revoked_record:
            raise RuntimeError(
                "reviewed shadow contains a revoked alias; run a new shadow "
                f"before apply: source={revoked_record.source_address_key}"
            )

    async def _collect_candidate_snapshot(self, session: Any) -> None:
        execution = self._required_execution()
        if execution.alias_kind == address_alias_sql.EVIDENCE_ADDRESS_MATCH_ALIAS_KIND:
            stale_rows = int(
                (
                    await session.execute(
                        text(
                            address_evidence_alias_sql.evidence_input_stale_count_sql(
                                schema=execution.schema
                            )
                        ),
                        {"alias_generation": execution.generation},
                    )
                ).scalar()
                or 0
            )
            if stale_rows:
                raise RuntimeError(
                    "evidence alias discovery requires a current full "
                    "entity-address-unified rebuild"
                )
        (
            execution.metrics_by_reason,
            execution.digest,
            execution.sample_rows,
            execution.archive_rows,
            execution.source_count,
        ) = await _shadow_run(
            session,
            schema=execution.schema,
            archive=execution.archive,
            scope_by_field={
                "run_id": execution.run_id,
                "scope_state_code": execution.state_code,
                "scope_zip_prefix": execution.zip_prefix,
                "retry_shadow_run_id": execution.shadow_run_id,
            },
            sample_limit=execution.request.sample_limit,
            alias_kind=execution.alias_kind,
        )
        await self._check_cancelled()
        if (
            execution.operation == "apply"
            and execution.digest != execution.reviewed_digest
        ):
            raise RuntimeError("candidate set changed after review; run a new shadow")

    async def _promote_reviewed_candidates(self, session: Any) -> None:
        execution = self._required_execution()
        if execution.operation != "apply":
            return
        conflict_record = (
            await session.execute(
                text(
                    address_alias_sql.active_alias_conflict_sql(
                        schema=execution.schema
                    )
                ),
                {"apply_run_id": execution.run_id},
            )
        ).first()
        if conflict_record:
            raise RuntimeError(
                "active alias target conflicts with reviewed candidate: "
                f"source={conflict_record.source_address_key} "
                f"active={conflict_record.active_target_address_key} "
                f"candidate={conflict_record.candidate_target_address_key}"
            )
        await _approve_shadow_candidates(
            session,
            schema=execution.schema,
            shadow_run_id=execution.shadow_run_id or "",
            reviewer=execution.reviewer or "",
        )
        promotion_record = (
            await session.execute(
                text(
                    address_alias_sql.promote_reviewed_aliases_sql(
                        schema=execution.schema
                    )
                ),
                {
                    "shadow_run_id": execution.shadow_run_id,
                    "apply_run_id": execution.run_id,
                    "candidate_digest": execution.digest,
                    "alias_kind": execution.alias_kind,
                    "ruleset_version": execution.ruleset_version,
                },
            )
        ).first()
        execution.promoted = int(promotion_record.promoted_count or 0)
        _, _, execution.final_generation = await _alias_state(
            session,
            schema=execution.schema,
            lock=False,
        )

    async def _seal_run(self, session: Any) -> None:
        execution = self._required_execution()
        execution.final_status = (
            "applied" if execution.operation == "apply" else "sealed"
        )
        await session.execute(
            text(_run_completion_sql(execution.schema)),
            _completion_parameters(execution),
        )

    async def _check_cancelled(self) -> None:
        if self.request.cancel_check:
            await self.request.cancel_check()

    def _result(self) -> NumericGridAliasResult:
        execution = self._required_execution()
        metric_map = execution.metrics_by_reason
        return NumericGridAliasResult(
            run_id=execution.run_id,
            mode=execution.operation,
            status=execution.final_status,
            candidate_digest=execution.digest,
            source_count=execution.source_count,
            candidate_sources=metric_map.get("candidate_sources", 0),
            candidate_rows=metric_map.get("candidate_rows", 0),
            no_candidate=metric_map.get("no_candidate", 0),
            active_skipped=metric_map.get("active_skipped", 0),
            eligible=metric_map.get("eligible", 0),
            ambiguous=metric_map.get("ambiguous", 0),
            insufficient_provenance=metric_map.get("insufficient_provenance", 0),
            promoted=execution.promoted,
            generation=execution.final_generation,
            sample_rows=execution.sample_rows,
            alias_kind=execution.alias_kind,
        )

    def _required_execution(self) -> _AliasExecution:
        if self.execution is None:
            raise RuntimeError("address alias execution was not prepared")
        return self.execution


async def run_numeric_grid_alias(
    request: NumericGridAliasRequest | None = None,
    **request_options: Any,
) -> NumericGridAliasResult:
    """Run a sealed shadow or apply its exact reviewed candidate set."""
    if request is not None and request_options:
        raise TypeError("pass a request object or keyword options, not both")
    resolved_request = request or NumericGridAliasRequest(**request_options)
    return await _NumericGridAliasRunner(resolved_request).execute()
