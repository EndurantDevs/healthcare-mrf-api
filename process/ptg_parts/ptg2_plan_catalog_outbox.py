# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable, source-local work queue for immutable PTG plan metadata."""

from __future__ import annotations

import asyncio
import datetime
import uuid
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_lifecycle_lock import (
    configure_ptg2_lifecycle_transaction,
    is_retryable_lifecycle_database_error,
)
from process.ptg_parts.ptg2_plan_catalog import (
    PTG2PlanCatalogConflict,
    attempt_publish_immutable_plan_catalog,
)
from process.ptg_parts.ptg2_plan_catalog_payload import (
    OUTBOX_MAX_ALIAS_ROWS,
    OUTBOX_MAX_PAYLOAD_BYTES,
    OUTBOX_MAX_PLAN_ROWS,
    PTG2PlanCatalogOutboxConflict,
    bounded_catalog_chunks,
    canonical_chunk,
    canonical_request_payload,
    json_value,
    row_mapping,
    validated_snapshot_id,
)
from process.ptg_parts.ptg2_plan_catalog_outbox_store import (
    PTG2_PLAN_CATALOG_OUTBOX_TABLE,
    enqueue_catalog_chunk,
)
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema


_OUTBOX_LOCK_TIMEOUT = "50ms"
_OUTBOX_STATEMENT_TIMEOUT = "500ms"
_OUTBOX_LEASE_SECONDS = 30
_OUTBOX_RETRY_SECONDS = 2
_OUTBOX_MAX_WALL_SECONDS = 5.0


@dataclass(frozen=True)
class PTG2PlanCatalogOutboxRequest:
    request_id: str
    snapshot_id: str
    payload_sha256: str
    request_ids: tuple[str, ...]


@dataclass(frozen=True)
class PTG2PlanCatalogOutboxDrain:
    claimed: int = 0
    persisted: int = 0
    deferred: int = 0
    poisoned: int = 0
    lease_lost: int = 0


async def enqueue_immutable_plan_catalog(
    session: Any,
    *,
    snapshot_id: str,
    plan_rows: Sequence[Mapping[str, Any]],
    alias_rows: Sequence[Mapping[str, Any]],
) -> PTG2PlanCatalogOutboxRequest:
    """Enqueue exact compatibility work inside the caller's local transaction."""

    normalized_snapshot_id = validated_snapshot_id(snapshot_id)
    normalized_plans, normalized_aliases, overall_payload_sha256 = (
        canonical_request_payload(plan_rows=plan_rows, alias_rows=alias_rows)
    )
    if not normalized_plans and not normalized_aliases:
        raise ValueError("PTG plan catalog outbox request is empty")
    chunks = bounded_catalog_chunks(normalized_plans, normalized_aliases)
    chunk_count = len(chunks)
    schema = _quote_ident(resolve_ptg2_schema())
    request_ids: list[str] = []
    for chunk_index, chunk in enumerate(chunks):
        request_ids.append(
            await enqueue_catalog_chunk(
                session,
                schema=schema,
                snapshot_id=normalized_snapshot_id,
                chunk_index=chunk_index,
                chunk_count=chunk_count,
                chunk=chunk,
            )
        )
    return PTG2PlanCatalogOutboxRequest(
        request_id=request_ids[0],
        snapshot_id=normalized_snapshot_id,
        payload_sha256=overall_payload_sha256,
        request_ids=tuple(request_ids),
    )


async def _claim_request(
    *,
    request_id: str | None = None,
    excluded_snapshot_ids: Sequence[str] = (),
) -> dict[str, Any] | None:
    schema = _quote_ident(resolve_ptg2_schema())
    lease_token = uuid.uuid4().hex
    request_filter, claim_parameters_by_name = _claim_filter(
        request_id=request_id,
        excluded_snapshot_ids=excluded_snapshot_ids,
        lease_token=lease_token,
    )
    try:
        async with db.transaction() as session:
            await configure_ptg2_lifecycle_transaction(
                session,
                lock_timeout=_OUTBOX_LOCK_TIMEOUT,
                statement_timeout=_OUTBOX_STATEMENT_TIMEOUT,
            )
            claim_result = await session.execute(
                db.text(
                    f"""
                    WITH claimable AS (
                        SELECT request_id
                          FROM {schema}.{PTG2_PLAN_CATALOG_OUTBOX_TABLE}
                         WHERE available_at <= transaction_timestamp()
                           AND terminal_at IS NULL
                           AND (lease_token IS NULL
                                OR lease_until <= transaction_timestamp())
                           {request_filter}
                         ORDER BY available_at, created_at, request_id
                         FOR UPDATE SKIP LOCKED
                         LIMIT 1
                    )
                    UPDATE {schema}.{PTG2_PLAN_CATALOG_OUTBOX_TABLE} AS work
                       SET lease_token = :lease_token,
                           lease_until = transaction_timestamp()
                               + make_interval(secs => :lease_seconds),
                           attempt_count = attempt_count + 1,
                           updated_at = transaction_timestamp()
                      FROM claimable
                     WHERE work.request_id = claimable.request_id
                    RETURNING work.request_id, work.snapshot_id,
                              work.payload_sha256, work.plan_rows,
                              work.alias_rows, work.plan_count,
                              work.alias_count, work.payload_bytes,
                              work.lease_token
                    """
                ),
                claim_parameters_by_name,
            )
            claimed_row = claim_result.one_or_none()
            return row_mapping(claimed_row) if claimed_row is not None else None
    except Exception as exc:
        if is_retryable_lifecycle_database_error(exc):
            return None
        raise


def _claim_filter(
    *,
    request_id: str | None,
    excluded_snapshot_ids: Sequence[str],
    lease_token: str,
) -> tuple[str, dict[str, Any]]:
    """Build the bounded claim predicate and its exact parameters."""

    claim_parameters_by_name: dict[str, Any] = {
        "lease_token": lease_token,
        "lease_seconds": _OUTBOX_LEASE_SECONDS,
    }
    if request_id:
        claim_parameters_by_name["request_id"] = validated_snapshot_id(request_id)
        return "AND request_id = :request_id", claim_parameters_by_name
    claim_parameters_by_name["excluded_snapshot_ids"] = list(
        excluded_snapshot_ids
    )
    return (
        "AND NOT (snapshot_id = ANY(CAST(:excluded_snapshot_ids AS text[])))",
        claim_parameters_by_name,
    )


def _claimed_rows(
    claim: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    plans = json_value(claim.get("plan_rows"))
    aliases = json_value(claim.get("alias_rows"))
    if not isinstance(plans, list) or not isinstance(aliases, list):
        raise PTG2PlanCatalogOutboxConflict("PTG plan catalog outbox payload is invalid")
    normalized_plans, normalized_aliases, payload_sha256 = canonical_request_payload(
        plan_rows=plans,
        alias_rows=aliases,
    )
    canonical, chunk_payload_sha256 = canonical_chunk(
        normalized_plans,
        normalized_aliases,
    )
    if (
        payload_sha256 != claim.get("payload_sha256")
        or chunk_payload_sha256 != payload_sha256
        or len(normalized_plans) != int(claim["plan_count"])
        or len(normalized_aliases) != int(claim["alias_count"])
        or len(canonical) != int(claim.get("payload_bytes") or -1)
        or len(normalized_plans) > OUTBOX_MAX_PLAN_ROWS
        or len(normalized_aliases) > OUTBOX_MAX_ALIAS_ROWS
        or len(canonical) > OUTBOX_MAX_PAYLOAD_BYTES
    ):
        raise PTG2PlanCatalogOutboxConflict(
            "PTG plan catalog outbox payload digest changed"
        )
    created_at = datetime.datetime.now(datetime.UTC)
    return (
        [
            {**plan_row, "created_at": created_at}
            for plan_row in normalized_plans
        ],
        [
            {**alias_row, "created_at": created_at}
            for alias_row in normalized_aliases
        ],
    )


async def _is_claim_finish_committed(
    claim: Mapping[str, Any], *, persisted: bool
) -> bool:
    schema = _quote_ident(resolve_ptg2_schema())
    async with db.transaction() as session:
        await configure_ptg2_lifecycle_transaction(
            session,
            lock_timeout=_OUTBOX_LOCK_TIMEOUT,
            statement_timeout=_OUTBOX_STATEMENT_TIMEOUT,
        )
        if persisted:
            statement = (
                f"DELETE FROM {schema}.{PTG2_PLAN_CATALOG_OUTBOX_TABLE} "
                "WHERE request_id = :request_id AND lease_token = :lease_token"
            )
            completion_parameters_by_name = {
                "request_id": claim["request_id"],
                "lease_token": claim["lease_token"],
            }
        else:
            statement = f"""
                UPDATE {schema}.{PTG2_PLAN_CATALOG_OUTBOX_TABLE}
                   SET lease_token = NULL,
                       lease_until = NULL,
                       available_at = transaction_timestamp()
                           + make_interval(secs => :retry_seconds),
                       updated_at = transaction_timestamp()
                 WHERE request_id = :request_id
                   AND lease_token = :lease_token
            """
            completion_parameters_by_name = {
                "request_id": claim["request_id"],
                "lease_token": claim["lease_token"],
                "retry_seconds": _OUTBOX_RETRY_SECONDS,
            }
        completion_result = await session.execute(
            db.text(statement + " RETURNING request_id"),
            completion_parameters_by_name,
        )
        return completion_result.scalar() is not None


async def _is_claim_renewed(claim: Mapping[str, Any]) -> bool:
    schema = _quote_ident(resolve_ptg2_schema())
    async with db.transaction() as session:
        await configure_ptg2_lifecycle_transaction(
            session,
            lock_timeout=_OUTBOX_LOCK_TIMEOUT,
            statement_timeout=_OUTBOX_STATEMENT_TIMEOUT,
        )
        renewal_result = await session.execute(
            db.text(
                f"""
                UPDATE {schema}.{PTG2_PLAN_CATALOG_OUTBOX_TABLE}
                   SET lease_until = transaction_timestamp()
                       + make_interval(secs => :lease_seconds),
                       updated_at = transaction_timestamp()
                 WHERE request_id = :request_id
                   AND lease_token = :lease_token
                   AND terminal_at IS NULL
                RETURNING request_id
                """
            ),
            {
                "request_id": claim["request_id"],
                "lease_token": claim["lease_token"],
                "lease_seconds": _OUTBOX_LEASE_SECONDS,
            },
        )
        return renewal_result.scalar() is not None


async def _is_claim_poisoned(
    claim: Mapping[str, Any],
    *,
    error_code: str,
) -> bool:
    schema = _quote_ident(resolve_ptg2_schema())
    async with db.transaction() as session:
        await configure_ptg2_lifecycle_transaction(
            session,
            lock_timeout=_OUTBOX_LOCK_TIMEOUT,
            statement_timeout=_OUTBOX_STATEMENT_TIMEOUT,
        )
        poison_result = await session.execute(
            db.text(
                f"""
                UPDATE {schema}.{PTG2_PLAN_CATALOG_OUTBOX_TABLE}
                   SET lease_token = NULL,
                       lease_until = NULL,
                       terminal_error_code = :error_code,
                       terminal_at = transaction_timestamp(),
                       updated_at = transaction_timestamp()
                 WHERE request_id = :request_id
                   AND lease_token = :lease_token
                   AND terminal_at IS NULL
                RETURNING request_id
                """
            ),
            {
                "request_id": claim["request_id"],
                "lease_token": claim["lease_token"],
                "error_code": str(error_code)[:64],
            },
        )
        return poison_result.scalar() is not None


async def _drain_catalog_claim(
    claim: Mapping[str, Any],
    *,
    max_wall_seconds: float,
) -> str:
    """Process one leased claim and return its fenced terminal outcome."""

    try:
        plan_rows, alias_rows = _claimed_rows(claim)
    except PTG2PlanCatalogOutboxConflict:
        is_poisoned = await _is_claim_poisoned(
            claim, error_code="invalid_payload"
        )
        return "poisoned" if is_poisoned else "lease_lost"
    if not await _is_claim_renewed(claim):
        return "lease_lost"
    try:
        async with asyncio.timeout(max_wall_seconds):
            status = await attempt_publish_immutable_plan_catalog(
                plan_rows=plan_rows,
                alias_rows=alias_rows,
            )
    except PTG2PlanCatalogConflict:
        is_poisoned = await _is_claim_poisoned(
            claim, error_code="immutable_conflict"
        )
        return "poisoned" if is_poisoned else "lease_lost"
    except TimeoutError:
        status = "deferred"
    is_finished = await _is_claim_finish_committed(
        claim, persisted=status == "persisted"
    )
    return status if is_finished else "lease_lost"


async def drain_immutable_plan_catalog_outbox(
    *,
    max_requests: int = 8,
    request_id: str | None = None,
    max_wall_seconds: float = _OUTBOX_MAX_WALL_SECONDS,
) -> PTG2PlanCatalogOutboxDrain:
    """Publish bounded leased requests; failed shared keys remain replayable."""

    if max_requests < 1 or max_requests > 64:
        raise ValueError("PTG plan catalog drain limit is invalid")
    if max_wall_seconds <= 0 or max_wall_seconds > _OUTBOX_LEASE_SECONDS / 2:
        raise ValueError("PTG plan catalog wall-time bound is invalid")
    claimed = persisted = deferred = poisoned = lease_lost = 0
    claimed_snapshot_ids: list[str] = []
    for _index in range(max_requests):
        claim = await _claim_request(
            request_id=request_id,
            excluded_snapshot_ids=claimed_snapshot_ids,
        )
        if claim is None:
            break
        claimed += 1
        claimed_snapshot_ids.append(str(claim["snapshot_id"]))
        status = await _drain_catalog_claim(
            claim,
            max_wall_seconds=max_wall_seconds,
        )
        if status == "persisted":
            persisted += 1
        elif status == "deferred":
            deferred += 1
        elif status == "poisoned":
            poisoned += 1
        else:
            lease_lost += 1
        if request_id:
            break
    return PTG2PlanCatalogOutboxDrain(
        claimed=claimed,
        persisted=persisted,
        deferred=deferred,
        poisoned=poisoned,
        lease_lost=lease_lost,
    )


__all__ = [
    "PTG2_PLAN_CATALOG_OUTBOX_TABLE",
    "PTG2PlanCatalogOutboxConflict",
    "PTG2PlanCatalogOutboxDrain",
    "PTG2PlanCatalogOutboxRequest",
    "drain_immutable_plan_catalog_outbox",
    "enqueue_immutable_plan_catalog",
]
