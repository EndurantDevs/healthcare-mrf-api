# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable non-gating reconciliation for the legacy global PTG pointer."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_lifecycle_lock import (
    PTG2LifecycleLockDeferred,
    acquire_ptg2_source_lifecycle_lock,
    configure_ptg2_lifecycle_transaction,
    is_retryable_lifecycle_database_error,
)
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema


PTG2_LEGACY_GLOBAL_PROJECTION_QUEUE_TABLE = (
    "ptg2_legacy_global_pointer_projection_queue"
)
_GLOBAL_POINTER_LOCK_IDENTITY = "legacy_global_snapshot_pointer"
_LOCK_TIMEOUT = "50ms"
_STATEMENT_TIMEOUT = "500ms"
_LEASE_SECONDS = 30
_RETRY_SECONDS = 2


@dataclass(frozen=True)
class PTG2LegacyGlobalProjectionDrain:
    claimed: int = 0
    reconciled: int = 0
    deferred: int = 0
    lease_lost: int = 0


def _normalized_source_key(source_key: str) -> str:
    normalized = str(source_key or "").strip().lower()
    if not normalized or len(normalized) > 96:
        raise ValueError("PTG legacy projection source key is invalid")
    return normalized


async def mark_legacy_global_projection_dirty(
    session: Any,
    *,
    schema_name: str,
    source_key: str,
) -> None:
    """Persist one source-local generation inside its pointer transaction."""

    schema = _quote_ident(schema_name)
    await session.execute(
        db.text(
            f"""
            INSERT INTO {schema}.{PTG2_LEGACY_GLOBAL_PROJECTION_QUEUE_TABLE}
                (source_key, requested_generation, applied_generation,
                 available_at, created_at, updated_at)
            VALUES
                (:source_key, 1, 0, transaction_timestamp(),
                 transaction_timestamp(), transaction_timestamp())
            ON CONFLICT (source_key) DO UPDATE
                SET requested_generation =
                        {PTG2_LEGACY_GLOBAL_PROJECTION_QUEUE_TABLE}.
                            requested_generation + 1,
                    available_at = transaction_timestamp(),
                    updated_at = transaction_timestamp()
            """
        ),
        {"source_key": _normalized_source_key(source_key)},
    )


async def _claim_projection(
    *,
    source_key: str | None,
) -> dict[str, Any] | None:
    schema = _quote_ident(resolve_ptg2_schema())
    lease_token = uuid.uuid4().hex
    source_filter = "AND source_key = :source_key" if source_key else ""
    claim_parameters_by_name: dict[str, Any] = {
        "lease_token": lease_token,
        "lease_seconds": _LEASE_SECONDS,
    }
    if source_key:
        claim_parameters_by_name["source_key"] = _normalized_source_key(source_key)
    try:
        async with db.transaction() as session:
            await configure_ptg2_lifecycle_transaction(
                session,
                lock_timeout=_LOCK_TIMEOUT,
                statement_timeout=_STATEMENT_TIMEOUT,
            )
            claim_result = await session.execute(
                db.text(
                    f"""
                    WITH claimable AS (
                        SELECT source_key, requested_generation
                          FROM {schema}.{PTG2_LEGACY_GLOBAL_PROJECTION_QUEUE_TABLE}
                         WHERE applied_generation < requested_generation
                           AND available_at <= transaction_timestamp()
                           AND (lease_token IS NULL
                                OR lease_until <= transaction_timestamp())
                           {source_filter}
                         ORDER BY available_at, updated_at, source_key
                         FOR UPDATE SKIP LOCKED
                         LIMIT 1
                    )
                    UPDATE {schema}.{PTG2_LEGACY_GLOBAL_PROJECTION_QUEUE_TABLE}
                           AS work
                       SET lease_token = :lease_token,
                           lease_until = transaction_timestamp()
                               + make_interval(secs => :lease_seconds),
                           updated_at = transaction_timestamp()
                      FROM claimable
                     WHERE work.source_key = claimable.source_key
                    RETURNING work.source_key,
                              claimable.requested_generation AS claimed_generation,
                              work.lease_token
                    """
                ),
                claim_parameters_by_name,
            )
            claimed = claim_result.mappings().one_or_none()
            return dict(claimed) if claimed is not None else None
    except Exception as exc:
        if is_retryable_lifecycle_database_error(exc):
            return None
        raise


async def _authoritative_global_projection(session: Any, *, schema: str) -> None:
    winner_result = await session.execute(
        db.text(
            f"""
            SELECT current_source.source_key,
                   current_source.snapshot_id,
                   current_source.previous_snapshot_id,
                   current_source.updated_at,
                   snapshot.published_at
              FROM {schema}.ptg2_current_source_snapshot AS current_source
              JOIN {schema}.ptg2_snapshot AS snapshot
                ON snapshot.snapshot_id = current_source.snapshot_id
             WHERE snapshot.status = 'published'
               AND EXISTS (
                    SELECT 1
                      FROM {schema}.ptg2_current_plan_source AS current_plan
                     WHERE current_plan.source_key = current_source.source_key
                       AND current_plan.snapshot_id = current_source.snapshot_id
               )
             ORDER BY snapshot.published_at DESC NULLS LAST,
                      current_source.updated_at DESC NULLS LAST,
                      current_source.snapshot_id DESC,
                      current_source.source_key
             LIMIT 1
            """
        )
    )
    winner = winner_result.mappings().one_or_none()
    if winner is None:
        await session.execute(
            db.text(
                f"DELETE FROM {schema}.ptg2_current_snapshot "
                "WHERE slot = 'current'"
            )
        )
        return
    await session.execute(
        db.text(
            f"""
            INSERT INTO {schema}.ptg2_current_snapshot
                (slot, snapshot_id, previous_snapshot_id, updated_at)
            VALUES
                ('current', :snapshot_id, :previous_snapshot_id,
                 transaction_timestamp())
            ON CONFLICT (slot) DO UPDATE
                SET snapshot_id = EXCLUDED.snapshot_id,
                    previous_snapshot_id = EXCLUDED.previous_snapshot_id,
                    updated_at = EXCLUDED.updated_at
            """
        ),
        {
            "snapshot_id": winner["snapshot_id"],
            "previous_snapshot_id": winner["previous_snapshot_id"],
        },
    )


async def _is_authoritative_projection_applied() -> bool:
    schema = _quote_ident(resolve_ptg2_schema())
    try:
        async with db.transaction() as session:
            await acquire_ptg2_source_lifecycle_lock(
                session,
                source_key=_GLOBAL_POINTER_LOCK_IDENTITY,
                lock_timeout=_LOCK_TIMEOUT,
                statement_timeout=_STATEMENT_TIMEOUT,
            )
            await _authoritative_global_projection(session, schema=schema)
    except Exception as exc:
        if isinstance(exc, PTG2LifecycleLockDeferred) or (
            is_retryable_lifecycle_database_error(exc)
        ):
            return False
        raise
    return True


async def _is_projection_finish_committed(
    claim: dict[str, Any],
    *,
    reconciled: bool,
) -> bool:
    schema = _quote_ident(resolve_ptg2_schema())
    async with db.transaction() as session:
        await configure_ptg2_lifecycle_transaction(
            session,
            lock_timeout=_LOCK_TIMEOUT,
            statement_timeout=_STATEMENT_TIMEOUT,
        )
        if reconciled:
            statement = f"""
                UPDATE {schema}.{PTG2_LEGACY_GLOBAL_PROJECTION_QUEUE_TABLE}
                   SET applied_generation = GREATEST(
                           applied_generation, :claimed_generation),
                       lease_token = NULL,
                       lease_until = NULL,
                       available_at = transaction_timestamp(),
                       updated_at = transaction_timestamp()
                 WHERE source_key = :source_key
                   AND lease_token = :lease_token
                RETURNING source_key
            """
            finish_parameters_by_name = dict(claim)
        else:
            statement = f"""
                UPDATE {schema}.{PTG2_LEGACY_GLOBAL_PROJECTION_QUEUE_TABLE}
                   SET lease_token = NULL,
                       lease_until = NULL,
                       available_at = transaction_timestamp()
                           + make_interval(secs => :retry_seconds),
                       updated_at = transaction_timestamp()
                 WHERE source_key = :source_key
                   AND lease_token = :lease_token
                RETURNING source_key
            """
            finish_parameters_by_name = {**claim, "retry_seconds": _RETRY_SECONDS}
        finish_result = await session.execute(
            db.text(statement), finish_parameters_by_name
        )
        return finish_result.scalar() is not None


async def drain_legacy_global_projection_queue(
    *,
    max_requests: int = 8,
    source_key: str | None = None,
) -> PTG2LegacyGlobalProjectionDrain:
    """Recompute authoritative compatibility state from bounded leased work."""

    if max_requests < 1 or max_requests > 64:
        raise ValueError("PTG legacy projection drain limit is invalid")
    claimed = reconciled = deferred = lease_lost = 0
    for _index in range(max_requests):
        claim = await _claim_projection(source_key=source_key)
        if claim is None:
            break
        claimed += 1
        is_reconciled = await _is_authoritative_projection_applied()
        if await _is_projection_finish_committed(
            claim, reconciled=is_reconciled
        ):
            if is_reconciled:
                reconciled += 1
            else:
                deferred += 1
        else:
            lease_lost += 1
        if source_key:
            break
    return PTG2LegacyGlobalProjectionDrain(
        claimed=claimed,
        reconciled=reconciled,
        deferred=deferred,
        lease_lost=lease_lost,
    )


__all__ = [
    "PTG2_LEGACY_GLOBAL_PROJECTION_QUEUE_TABLE",
    "PTG2LegacyGlobalProjectionDrain",
    "drain_legacy_global_projection_queue",
    "mark_legacy_global_projection_dirty",
]
