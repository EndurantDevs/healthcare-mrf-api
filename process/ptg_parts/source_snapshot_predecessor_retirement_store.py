# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Locked reads and exact writes for predecessor-retention retirement."""

from __future__ import annotations

import datetime
from typing import Any, Mapping

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.source_snapshot_predecessor_retirement_types import (
    PTG2PredecessorRetirementConflict,
    PredecessorRetirementContext,
    PredecessorRetirementDecision,
    PredecessorRetirementRequest,
)
from process.ptg_parts.source_snapshot_predecessor_retirement_sql import (
    CONTROL_CONTEXT_QUERY_TEMPLATES,
    GLOBAL_POINTER_UPDATE,
    MRF_CONTEXT_QUERY_TEMPLATES,
    PLAN_POINTER_UPDATE,
    POSTCHECK_TEMPLATE,
    ROLLBACK_PIN_DELETE,
    SOURCE_POINTER_UPDATE,
)
from process.ptg_parts.source_snapshot_rollback_types import (
    ROLLBACK_PIN_OWNER_TYPE,
)


_LOCK_CONTENTION_SQLSTATES = frozenset({"40P01", "55P03"})


def _schema_sql(template: str, schema: str) -> str:
    return template.replace("__SCHEMA__", schema)


def _row_mapping(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    return dict(getattr(row, "_mapping", row))


def _database_sqlstate(error: Exception) -> str:
    pending_errors = [error]
    visited_error_ids: set[int] = set()
    while pending_errors:
        candidate = pending_errors.pop()
        if id(candidate) in visited_error_ids:
            continue
        visited_error_ids.add(id(candidate))
        for field in ("sqlstate", "pgcode"):
            value = getattr(candidate, field, None)
            if value:
                return str(value)
        for field in ("orig", "__cause__", "__context__"):
            nested = getattr(candidate, field, None)
            if isinstance(nested, Exception):
                pending_errors.append(nested)
    return ""


async def _all(
    session: Any,
    statement: str,
    params: Mapping[str, Any],
) -> tuple[Mapping[str, Any], ...]:
    result = await session.execute(db.text(statement), dict(params))
    return tuple(_row_mapping(row) for row in result.all())


async def _one(
    session: Any,
    statement: str,
    params: Mapping[str, Any],
) -> dict[str, Any]:
    result = await session.execute(db.text(statement), dict(params))
    return _row_mapping(result.one_or_none())


async def load_retirement_audit(
    session: Any,
    *,
    schema_name: str,
    idempotency_key: str,
) -> dict[str, Any]:
    """Lock and return an existing idempotency record, if any."""

    schema = _quote_ident(schema_name)
    return await _one(
        session,
        f"""
        SELECT idempotency_key, request_digest, source_key,
               current_snapshot_id, predecessor_snapshot_id,
               rollback_pin_mode, rollback_owner_id,
               actor, reason, retired_at,
               cleared_source_pointer_count, cleared_plan_pointer_count,
               cleared_global_pointer_count, deleted_rollback_pin_count
          FROM {schema}.ptg2_predecessor_retirement_audit
         WHERE idempotency_key = :idempotency_key
         FOR UPDATE
        """,
        {"idempotency_key": idempotency_key},
    )


async def load_retirement_context(
    session: Any,
    *,
    schema_name: str,
    control_schema_name: str,
    request: PredecessorRetirementRequest,
) -> PredecessorRetirementContext:
    """Lock control then MRF; NOWAIT until all writers share the lifecycle lock."""

    schema = _quote_ident(schema_name)
    control_schema = _quote_ident(control_schema_name)
    pair_params_by_name = {
        "source_key": request.source_key,
        "current_snapshot_id": request.current_snapshot_id,
        "predecessor_snapshot_id": request.predecessor_snapshot_id,
    }
    await session.execute(
        db.text(
            f"LOCK TABLE {control_schema}.hp_plan_release_binding, "
            f"{control_schema}.hp_snapshot_pin "
            "IN SHARE ROW EXCLUSIVE MODE"
        )
    )
    context_records_by_surface = await _load_context_surfaces(
        session,
        control_schema,
        CONTROL_CONTEXT_QUERY_TEMPLATES,
        pair_params_by_name,
    )
    try:
        await session.execute(
            db.text(
                f"LOCK TABLE {schema}.plan_release_snapshot_binding, "
                f"{schema}.ptg2_snapshot_pin "
                "IN SHARE ROW EXCLUSIVE MODE NOWAIT"
            )
        )
    except Exception as exc:
        if _database_sqlstate(exc) not in _LOCK_CONTENTION_SQLSTATES:
            raise
        raise PTG2PredecessorRetirementConflict(
            "predecessor retirement is contending with a release update; retry"
        ) from exc
    context_records_by_surface.update(
        await _load_context_surfaces(
            session,
            schema,
            MRF_CONTEXT_QUERY_TEMPLATES,
            pair_params_by_name,
        )
    )
    return PredecessorRetirementContext(**context_records_by_surface)


async def _load_context_surfaces(
    session: Any,
    schema: str,
    query_templates: tuple[tuple[str, str], ...],
    params_by_name: Mapping[str, Any],
) -> dict[str, tuple[Mapping[str, Any], ...]]:
    records_by_surface = {}
    for surface_name, query_template in query_templates:
        records_by_surface[surface_name] = await _all(
            session,
            _schema_sql(query_template, schema),
            params_by_name,
        )
    return records_by_surface


async def _require_one_changed(
    session: Any,
    statement: str,
    params: Mapping[str, Any],
    *,
    conflict_message: str,
) -> None:
    result = await session.execute(db.text(statement), dict(params))
    if result.one_or_none() is None:
        raise PTG2PredecessorRetirementConflict(conflict_message)


async def _require_changed_count(
    session: Any,
    statement: str,
    params: Mapping[str, Any],
    *,
    expected_count: int,
    conflict_message: str,
) -> None:
    result = await session.execute(db.text(statement), dict(params))
    if hasattr(result, "all"):
        changed_count = len(result.all())
    else:
        changed_count = int(result.one_or_none() is not None)
    if changed_count != expected_count:
        raise PTG2PredecessorRetirementConflict(conflict_message)


async def apply_predecessor_retirement(
    session: Any,
    *,
    schema_name: str,
    request: PredecessorRetirementRequest,
    decision: PredecessorRetirementDecision,
) -> None:
    """Clear only exact predecessor pointers and delete only the exact pin."""

    schema = _quote_ident(schema_name)
    params_by_name = request.audit_coordinates()
    await _clear_source_predecessor(session, schema, params_by_name)
    await _clear_plan_predecessors(
        session,
        schema,
        params_by_name,
        expected_count=decision.plan_pointer_count,
    )
    if decision.global_pointer_count:
        await _clear_global_predecessor(session, schema, params_by_name)
    if decision.deleted_rollback_pin_count:
        await _delete_exact_rollback_pin(session, schema, params_by_name)


async def _clear_source_predecessor(
    session: Any,
    schema: str,
    params_by_name: Mapping[str, Any],
) -> None:
    await _require_one_changed(
        session,
        _schema_sql(SOURCE_POINTER_UPDATE, schema),
        params_by_name,
        conflict_message="source pointer changed during predecessor retirement",
    )


async def _clear_plan_predecessors(
    session: Any,
    schema: str,
    params_by_name: Mapping[str, Any],
    *,
    expected_count: int,
) -> None:
    await _require_changed_count(
        session,
        _schema_sql(PLAN_POINTER_UPDATE, schema),
        params_by_name,
        expected_count=expected_count,
        conflict_message=(
            "source plan pointers changed during predecessor retirement"
        ),
    )


async def _clear_global_predecessor(
    session: Any,
    schema: str,
    params_by_name: Mapping[str, Any],
) -> None:
    await _require_one_changed(
        session,
        _schema_sql(GLOBAL_POINTER_UPDATE, schema),
        params_by_name,
        conflict_message="global pointer changed during predecessor retirement",
    )


async def _delete_exact_rollback_pin(
    session: Any,
    schema: str,
    params_by_name: Mapping[str, Any],
) -> None:
    await _require_one_changed(
        session,
        _schema_sql(ROLLBACK_PIN_DELETE, schema),
        {
            **params_by_name,
            "rollback_owner_type": ROLLBACK_PIN_OWNER_TYPE,
        },
        conflict_message="rollback pin changed during predecessor retirement",
    )


async def postcheck_predecessor_retirement(
    session: Any,
    *,
    schema_name: str,
    control_schema_name: str,
    request: PredecessorRetirementRequest,
) -> None:
    """Require zero live predecessor references while preserving lineage."""

    schema = _quote_ident(schema_name)
    control_schema = _quote_ident(control_schema_name)
    postcheck_sql = _schema_sql(POSTCHECK_TEMPLATE, schema).replace(
        "__CONTROL_SCHEMA__",
        control_schema,
    )
    postcheck_by_field = await _one(
        session,
        postcheck_sql,
        request.audit_coordinates(),
    )
    reference_fields = (
        "global_references",
        "source_references",
        "plan_references",
        "pin_references",
        "release_references",
        "control_release_references",
        "control_pin_references",
    )
    if any(
        int(postcheck_by_field.get(field) or 0) != 0
        for field in reference_fields
    ):
        raise PTG2PredecessorRetirementConflict(
            "predecessor still has live references after retirement"
        )
    if (
        int(postcheck_by_field.get("preserved_lineage") or 0) != 1
        or int(postcheck_by_field.get("preserved_current_pointer") or 0)
        != 1
    ):
        raise PTG2PredecessorRetirementConflict(
            "current snapshot or immutable snapshot lineage changed"
        )


async def database_utc_timestamp(session: Any) -> datetime.datetime:
    """Return the transaction's authoritative database timestamp."""

    result = await _one(
        session,
        "SELECT transaction_timestamp() AS retired_at",
        {},
    )
    retired_at = result.get("retired_at")
    if not isinstance(retired_at, datetime.datetime):
        raise RuntimeError("database did not return a predecessor retirement time")
    return retired_at


async def insert_retirement_audit(
    session: Any,
    *,
    schema_name: str,
    request: PredecessorRetirementRequest,
    decision: PredecessorRetirementDecision,
    retired_at: datetime.datetime | None = None,
) -> dict[str, Any]:
    """Insert the immutable audit record in the pointer transaction."""

    if retired_at is None:
        retired_at = await database_utc_timestamp(session)
    params_by_name: dict[str, Any] = {
        **request.audit_coordinates(),
        "retired_at": retired_at,
        "cleared_source_pointer_count": decision.source_pointer_count,
        "cleared_plan_pointer_count": decision.plan_pointer_count,
        "cleared_global_pointer_count": decision.global_pointer_count,
        "deleted_rollback_pin_count": decision.deleted_rollback_pin_count,
    }
    inserted_audit_by_field = await _one(
        session,
        f"""
        INSERT INTO {_quote_ident(schema_name)}.ptg2_predecessor_retirement_audit
            (idempotency_key, request_digest, source_key,
             current_snapshot_id, predecessor_snapshot_id,
             rollback_pin_mode, rollback_owner_id,
             actor, reason, retired_at,
             cleared_source_pointer_count, cleared_plan_pointer_count,
             cleared_global_pointer_count, deleted_rollback_pin_count)
        VALUES
            (:idempotency_key, :request_digest, :source_key,
             :current_snapshot_id, :predecessor_snapshot_id,
             :rollback_pin_mode, :rollback_owner_id,
             :actor, :reason, :retired_at,
             :cleared_source_pointer_count, :cleared_plan_pointer_count,
             :cleared_global_pointer_count, :deleted_rollback_pin_count)
        RETURNING idempotency_key, retired_at
        """,
        params_by_name,
    )
    if (
        inserted_audit_by_field.get("idempotency_key")
        != request.idempotency_key
    ):
        raise PTG2PredecessorRetirementConflict(
            "predecessor retirement audit insert did not persist"
        )
    return {**params_by_name, **inserted_audit_by_field}


__all__ = [
    "apply_predecessor_retirement",
    "insert_retirement_audit",
    "load_retirement_audit",
    "load_retirement_context",
    "postcheck_predecessor_retirement",
]
