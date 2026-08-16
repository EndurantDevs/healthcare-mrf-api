# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Controlled, one-way revocation for a reviewed numeric-grid alias."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass

from sqlalchemy import text

from db.models import db
from process.address_numeric_grid_alias import _alias_state, _insert_run, _mark_failed
from process.address_numeric_grid_alias_support import (
    _relation,
    _reviewer,
    _statement_timeout,
)
from process.ext import address_alias_sql


@dataclass(frozen=True)
class NumericGridAliasRevokeResult:
    run_id: str
    status: str
    source_address_key: str
    target_address_key: str
    revoked_reason: str
    revoked_by: str
    generation: int
    alias_kind: str = address_alias_sql.NUMERIC_GRID_ALIAS_KIND


@dataclass(frozen=True)
class _RevokeContext:
    schema: str
    source_key: str
    target_key: str
    reason: str
    reviewer: str
    timeout: str
    run_id: str
    alias_kind: str
    ruleset_version: int


def _uuid(value: str | None, *, name: str) -> str:
    normalized = str(value or "").strip().lower()
    try:
        return str(uuid.UUID(normalized))
    except (ValueError, TypeError):
        raise ValueError(f"{name} must be a valid UUID") from None


def _reason(value: str | None) -> str:
    reason = str(value or "").strip()
    if not reason:
        raise ValueError("revoke requires a non-empty reason")
    return reason[:2000]


async def _lock_active_alias(
    session,
    *,
    schema: str,
    aliases: str,
    source_key: str,
    target_key: str,
    timeout: str,
    alias_kind: str = address_alias_sql.NUMERIC_GRID_ALIAS_KIND,
    ruleset_version: int = address_alias_sql.NUMERIC_GRID_ALIAS_RULESET_VERSION,
) -> None:
    await session.execute(text(f"SET LOCAL lock_timeout = '{timeout}';"))
    await session.execute(text(f"SET LOCAL statement_timeout = '{timeout}';"))
    await session.execute(text(address_alias_sql.alias_advisory_xact_lock_sql()))
    await _alias_state(session, schema=schema, lock=True)
    active_alias = (
        await session.execute(
            text(
                f"""
                SELECT source_address_key::text, target_address_key::text
                FROM {aliases}
                WHERE source_address_key = CAST(:source_key AS uuid)
                  AND alias_kind = :alias_kind
                  AND ruleset_version = :ruleset_version
                  AND revoked_at IS NULL
                FOR UPDATE;
                """
            ),
            {
                "source_key": source_key,
                "alias_kind": alias_kind,
                "ruleset_version": ruleset_version,
            },
        )
    ).first()
    if active_alias is None:
        raise RuntimeError("active address alias was not found")
    if str(active_alias.target_address_key) != target_key:
        raise RuntimeError(
            "active alias target differs from expected target: "
            f"active={active_alias.target_address_key} expected={target_key}"
        )


async def _revoke_alias_row(
    session,
    *,
    aliases: str,
    source_key: str,
    revoke_reason: str,
    reviewer: str,
    run_id: str,
    alias_kind: str,
    ruleset_version: int,
) -> None:
    await session.execute(
        text(
            f"""
            UPDATE {aliases}
               SET revoked_at = now(),
                   revoked_reason = :reason,
                   revoked_by = :reviewed_by,
                   revoke_run_id = CAST(:run_id AS uuid),
                   updated_at = now()
             WHERE source_address_key = CAST(:source_key AS uuid)
               AND alias_kind = :alias_kind
               AND ruleset_version = :ruleset_version
               AND revoked_at IS NULL;
            """
        ),
        {
            "source_key": source_key,
            "reason": revoke_reason,
            "reviewed_by": reviewer,
            "run_id": run_id,
            "alias_kind": alias_kind,
            "ruleset_version": ruleset_version,
        },
    )


async def _seal_revoke_run(
    session,
    *,
    runs: str,
    run_id: str,
    reason_map: dict[str, str],
) -> None:
    await session.execute(
        text(
            f"""
            UPDATE {runs}
               SET status = 'revoked',
                   reviewed_at = now(),
                   reason_buckets = CAST(:reason_buckets AS jsonb),
                   completed_at = now()
             WHERE run_id = CAST(:run_id AS uuid);
            """
        ),
        {
            "reason_buckets": json.dumps(reason_map, sort_keys=True),
            "run_id": run_id,
        },
    )


async def _prepare_revoke_context(
    *,
    source_address_key: str,
    expected_target_address_key: str,
    reason: str,
    reviewed_by: str,
    schema: str | None,
    timeout: str,
    alias_kind: str,
) -> _RevokeContext:
    normalized_alias_kind = str(alias_kind or "").strip()
    ruleset_version = address_alias_sql.alias_ruleset(normalized_alias_kind)
    context = _RevokeContext(
        schema=schema
        or os.getenv("HLTHPRT_DB_SCHEMA")
        or os.getenv("DB_SCHEMA")
        or "mrf",
        source_key=_uuid(source_address_key, name="source_address_key"),
        target_key=_uuid(
            expected_target_address_key,
            name="expected_target_address_key",
        ),
        reason=_reason(reason),
        reviewer=_reviewer(reviewed_by),
        timeout=_statement_timeout(timeout),
        run_id=str(uuid.uuid4()),
        alias_kind=normalized_alias_kind,
        ruleset_version=ruleset_version,
    )
    await _insert_run(
        run_by_field={
            "schema": context.schema,
            "run_id": context.run_id,
            "mode": "revoke",
            "state_code": None,
            "zip_prefix": None,
            "shadow_run_id": None,
            "reviewed_digest": None,
            "reviewed_by": context.reviewer,
            "alias_kind": context.alias_kind,
            "ruleset_version": context.ruleset_version,
        },
    )
    return context


async def _execute_revoke(context: _RevokeContext) -> NumericGridAliasRevokeResult:
    aliases = _relation(context.schema, address_alias_sql.ADDRESS_ALIAS_TABLE)
    runs = _relation(context.schema, address_alias_sql.ADDRESS_ALIAS_RUN_TABLE)
    async with db.transaction() as session:
        await _lock_active_alias(
            session,
            schema=context.schema,
            aliases=aliases,
            source_key=context.source_key,
            target_key=context.target_key,
            timeout=context.timeout,
            alias_kind=context.alias_kind,
            ruleset_version=context.ruleset_version,
        )
        await _revoke_alias_row(
            session,
            aliases=aliases,
            source_key=context.source_key,
            revoke_reason=context.reason,
            reviewer=context.reviewer,
            run_id=context.run_id,
            alias_kind=context.alias_kind,
            ruleset_version=context.ruleset_version,
        )
        _, _, generation = await _alias_state(
            session,
            schema=context.schema,
            lock=False,
        )
        await _seal_revoke_run(
            session,
            runs=runs,
            run_id=context.run_id,
            reason_map={
                "source_address_key": context.source_key,
                "target_address_key": context.target_key,
                "revoked_reason": context.reason,
            },
        )
    return NumericGridAliasRevokeResult(
        run_id=context.run_id,
        status="revoked",
        source_address_key=context.source_key,
        target_address_key=context.target_key,
        revoked_reason=context.reason,
        revoked_by=context.reviewer,
        generation=generation,
        alias_kind=context.alias_kind,
    )


async def revoke_numeric_grid_alias(
    *,
    source_address_key: str,
    expected_target_address_key: str,
    reason: str,
    reviewed_by: str,
    schema: str | None = None,
    timeout: str = "30s",
    alias_kind: str = address_alias_sql.NUMERIC_GRID_ALIAS_KIND,
) -> NumericGridAliasRevokeResult:
    """Revoke exactly one active alias and advance the active-set generation."""
    context = await _prepare_revoke_context(
        source_address_key=source_address_key,
        expected_target_address_key=expected_target_address_key,
        reason=reason,
        reviewed_by=reviewed_by,
        schema=schema,
        timeout=timeout,
        alias_kind=alias_kind,
    )
    try:
        return await _execute_revoke(context)
    except Exception as exc:
        await _mark_failed(context.schema, context.run_id, exc)
        raise
