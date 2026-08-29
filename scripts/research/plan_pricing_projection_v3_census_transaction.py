# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Rollback-only PostgreSQL transaction fence for the projection-v3 census."""

from __future__ import annotations

import asyncio
import sys
from typing import Any, Awaitable, Callable, Iterable, TypeVar

from sqlalchemy import text

from api import plan_pricing_projection_v3 as projection
from api.plan_pricing_projection_contract import lock_provider_generation
from api.plan_pricing_projection_v3_types import _BuildState
from api.ptg2_db_sidecars import (
    PRICE_MEMBERSHIP_ALIAS_INDEX_RETAINED_BYTES_PER_BLOCK,
    PRICE_MEMBERSHIP_TRANSIENT_BYTES_PER_FRAGMENT,
)
from db.connection import db

TEMP_RELATIONS = (
    "plan_pricing_provider_set_stage",
    "plan_pricing_provider_member_stage",
    "plan_pricing_provider_npi_materialized_stage",
    "plan_pricing_provider_npi_pending_stage",
    "plan_pricing_code_occurrence_stage",
    "plan_pricing_price_rate_stage",
    "plan_pricing_rate_frequency_stage",
    "plan_pricing_provider_cell_stage",
    "plan_pricing_eligible_member_cell_stage",
    "plan_pricing_set_cell_stage",
)
_Result = TypeVar("_Result")


def declared_occurrence_rows(
    binding_projections: Iterable[Any],
    code_identity: tuple[str, str],
) -> int:
    """Match the executable occurrence cap before empty-price filtering."""

    from api import ptg2_serving as serving

    return sum(
        serving._declared_geo_rate_count(
            binding.code_rows_by_identity.get(code_identity) or ()
        )
        for binding in binding_projections
    )


async def projection_stage_counts(session: Any) -> dict[str, int]:
    """Return the exact staged provider counts retained by the census."""

    counts_result = await session.execute(text("""
            SELECT
                (SELECT COUNT(*) FROM plan_pricing_provider_set_stage)
                    AS provider_set_count,
                (SELECT COUNT(*) FROM plan_pricing_provider_member_stage)
                    AS provider_membership_count,
                (SELECT COALESCE(MAX(membership_count), 0) FROM
                    plan_pricing_provider_set_stage)
                    AS maximum_provider_set_membership_count,
                (SELECT COUNT(*) FROM plan_pricing_provider_cell_stage)
                    AS provider_cell_count,
                (SELECT COALESCE(SUM(OCTET_LENGTH(fragment)), 0)
                   FROM plan_pricing_provider_cell_stage)
                    AS provider_fragment_byte_count,
                (SELECT COUNT(*) FROM plan_pricing_provider_npi_materialized_stage)
                    AS provider_npi_count,
                (SELECT COUNT(*)
                   FROM plan_pricing_provider_npi_pending_stage)
                    AS pending_npi_count,
                (SELECT COUNT(*) FROM plan_pricing_provider_set_stage
                  WHERE membership_count = 0)
                    AS referenced_empty_provider_set_count
            """))
    return {
        field_name: int(field_value)
        for field_name, field_value in counts_result.mappings().one().items()
    }


def price_membership_cache_counts(state: _BuildState) -> dict[str, int]:
    """Return source bounds for retained indexes and unsplittable reads, not RSS."""

    cache = state.price_membership_alias_cache
    cached_block_count = len(cache.identity_by_block)
    retained_identity_bytes = (
        cached_block_count * PRICE_MEMBERSHIP_ALIAS_INDEX_RETAINED_BYTES_PER_BLOCK
    )
    return {
        "price_membership_cached_block_count": cached_block_count,
        "price_membership_identity_retained_bytes": retained_identity_bytes,
        "price_membership_metadata_fragment_count": cache.metadata_record_count,
        "price_membership_maximum_fragments_per_block": (cache.maximum_fragment_count),
        "price_membership_singleton_peak_bytes": (
            retained_identity_bytes
            + cache.maximum_fragment_count
            * PRICE_MEMBERSHIP_TRANSIENT_BYTES_PER_FRAGMENT
        ),
    }


async def _temporary_relation_residue(session: Any) -> list[str]:
    residue_result = await session.execute(
        text("""
            SELECT relname
              FROM pg_class
             WHERE relnamespace = pg_my_temp_schema()
               AND relname = ANY(CAST(:relation_names AS text[]))
             ORDER BY relname
            """),
        {"relation_names": list(TEMP_RELATIONS)},
    )
    return [str(name) for name in residue_result.scalars()]


async def _finish_rollback(
    session: Any,
    transaction: Any,
    receipt_by_field: dict[str, Any],
) -> None:
    if transaction.is_active:
        await transaction.rollback()
    receipt_by_field["rollback_complete"] = not session.in_transaction()
    receipt_by_field["temporary_relations_after_rollback"] = (
        await _temporary_relation_residue(session)
    )
    await session.rollback()


async def _await_cleanup_task(
    task: asyncio.Task[Any],
    *,
    propagate_cancellation: bool,
) -> Any:
    """Drain cleanup through repeated cancellation."""

    pending_cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            pending_cancellation = exc
    result = task.result()
    if pending_cancellation is not None and propagate_cancellation:
        raise pending_cancellation
    return result


async def _require_owned_temporary_relations(session: Any, transaction: Any) -> None:
    if not transaction.is_active or tuple(
        await _temporary_relation_residue(session)
    ) != tuple(sorted(TEMP_RELATIONS)):
        raise RuntimeError("pricing projection census TEMP staging is incomplete")


async def _rollback_only(
    receipt_by_field: dict[str, Any],
    operation: Callable[[Any], Awaitable[dict[str, Any]]],
) -> dict[str, Any]:
    """Create owned TEMP stages and always roll back the owning session."""

    async with db.session() as session:
        transaction = await session.begin()
        try:
            await session.execute(text("SET LOCAL lock_timeout = '5s'"))
            await session.execute(text("SET LOCAL statement_timeout = '20min'"))
            await session.execute(text("SET LOCAL work_mem = '32MB'"))
            await lock_provider_generation(session)
            await projection._create_stage_tables(session)
            await _require_owned_temporary_relations(session, transaction)
            return await operation(session)
        finally:
            active_error = sys.exc_info()[1]
            cleanup_task = asyncio.create_task(
                _finish_rollback(session, transaction, receipt_by_field)
            )
            try:
                await _await_cleanup_task(
                    cleanup_task,
                    propagate_cancellation=active_error is None,
                )
            except BaseException as cleanup_error:
                if active_error is None:
                    raise
                receipt_by_field["rollback_error"] = {
                    "type": type(cleanup_error).__name__,
                }


async def _drain_cancelled_task(
    task: asyncio.Task[Any],
    cancellation: asyncio.CancelledError,
    receipt_by_field: dict[str, Any] | None,
) -> None:
    if not task.done():
        task.cancel()
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    try:
        task.result()
    except asyncio.CancelledError:
        raise cancellation
    except BaseException as task_error:
        if receipt_by_field is not None:
            receipt_by_field["rollback_task_error"] = {
                "type": type(task_error).__name__,
            }
    raise cancellation


async def cancellation_safe(
    operation: Awaitable[_Result],
    receipt_by_field: dict[str, Any] | None = None,
) -> _Result:
    """Cancel an owned task once, then drain it through caller cancellation."""

    task = asyncio.create_task(operation)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError as cancellation:
        await _drain_cancelled_task(task, cancellation, receipt_by_field)
        raise AssertionError("cancelled task drain returned")


async def rollback_only(
    receipt_by_field: dict[str, Any],
    operation: Callable[[Any], Awaitable[dict[str, Any]]],
) -> dict[str, Any]:
    """Run one rollback-only session and drain it through repeated cancellation."""

    return await cancellation_safe(
        _rollback_only(receipt_by_field, operation),
        receipt_by_field,
    )
