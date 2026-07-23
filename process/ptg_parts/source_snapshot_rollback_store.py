# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transactional reads and writes for published-snapshot rollback."""

from __future__ import annotations

import datetime
from typing import Any, Mapping

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.source_pointers import _allowed_source_pointer_key
from process.ptg_parts.source_snapshot_rollback_relations import (
    database_utc_timestamp,
    load_target_attestation,
    load_target_snapshot_scope,
)
from process.ptg_parts.source_snapshot_rollback_types import (
    PTG2SourceSnapshotRollbackConflict,
    RollbackContext,
    RollbackDecision,
)


def _row_mapping(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    return dict(getattr(row, "_mapping", row))


async def _one(
    session: Any,
    statement: str,
    params_by_name: Mapping[str, Any],
) -> dict[str, Any]:
    query_result = await session.execute(
        db.text(statement),
        dict(params_by_name),
    )
    return _row_mapping(query_result.one_or_none())


async def _all(
    session: Any,
    statement: str,
    params_by_name: Mapping[str, Any],
) -> tuple[Mapping[str, Any], ...]:
    query_result = await session.execute(
        db.text(statement),
        dict(params_by_name),
    )
    return tuple(_row_mapping(row) for row in query_result.all())


async def _load_source_pointer(
    session: Any,
    schema: str,
    source_key: str,
) -> dict[str, Any]:
    return await _one(
        session,
        f"""
        SELECT source_key, snapshot_id, previous_snapshot_id, import_month
          FROM {schema}.ptg2_current_source_snapshot
         WHERE source_key = :source_key
         FOR UPDATE
        """,
        {"source_key": source_key},
    )


async def _load_target_snapshot(
    session: Any,
    schema: str,
    snapshot_id: str,
) -> dict[str, Any]:
    return await _one(
        session,
        f"""
        SELECT snapshot.snapshot_id, snapshot.import_month, snapshot.status,
               snapshot.published_at, snapshot.manifest,
               binding.snapshot_key,
               layout.state AS layout_state,
               layout.generation AS layout_generation,
               layout.mapping_digest, layout.support_digest
          FROM {schema}.ptg2_snapshot AS snapshot
          JOIN {schema}.ptg2_v3_snapshot_binding AS binding
            ON binding.snapshot_id = snapshot.snapshot_id
          JOIN {schema}.ptg2_v3_snapshot_layout AS layout
            ON layout.snapshot_key = binding.snapshot_key
         WHERE snapshot.snapshot_id = :snapshot_id
         FOR SHARE OF snapshot, binding, layout
        """,
        {"snapshot_id": snapshot_id},
    )


async def _load_expected_snapshot(
    session: Any,
    schema: str,
    expected_current_snapshot_id: str,
) -> dict[str, Any]:
    return await _one(
        session,
        f"""
        SELECT snapshot_id, status, manifest
          FROM {schema}.ptg2_snapshot
         WHERE snapshot_id = :expected_current_snapshot_id
         FOR SHARE
        """,
        {"expected_current_snapshot_id": expected_current_snapshot_id},
    )


async def _load_rollback_pin(
    session: Any,
    schema: str,
    *,
    owner_type: str,
    owner_id: str,
    snapshot_id: str,
) -> dict[str, Any]:
    return await _one(
        session,
        f"""
        SELECT owner_type, owner_id, snapshot_id, reason
          FROM {schema}.ptg2_snapshot_pin
         WHERE owner_type = :owner_type
           AND owner_id = :owner_id
           AND snapshot_id = :snapshot_id
         FOR SHARE
        """,
        {
            "owner_type": owner_type,
            "owner_id": owner_id,
            "snapshot_id": snapshot_id,
        },
    )


async def _load_target_plan_scopes(
    session: Any,
    schema: str,
    snapshot_id: str,
) -> tuple[Mapping[str, Any], ...]:
    return await _all(
        session,
        f"""
        SELECT plan_id, plan_market_type
          FROM {schema}.ptg2_v3_snapshot_plan_scope
         WHERE snapshot_id = :snapshot_id
         ORDER BY plan_id, plan_market_type
         FOR SHARE
        """,
        {"snapshot_id": snapshot_id},
    )


async def _load_source_plan_pointers(
    session: Any,
    schema: str,
    source_key: str,
) -> tuple[Mapping[str, Any], ...]:
    return await _all(
        session,
        f"""
        SELECT plan_source_key, plan_id, plan_market_type, import_month,
               source_key, snapshot_id, previous_snapshot_id
          FROM {schema}.ptg2_current_plan_source
         WHERE source_key = :source_key
         ORDER BY plan_source_key
         FOR UPDATE
        """,
        {"source_key": source_key},
    )


async def _load_global_pointer(
    session: Any,
    schema: str,
) -> dict[str, Any]:
    return await _one(
        session,
        f"""
        SELECT pointer.snapshot_id, pointer.previous_snapshot_id,
               current_index.source_key
          FROM {schema}.ptg2_current_snapshot AS pointer
          LEFT JOIN LATERAL (
              SELECT snapshot.manifest->'serving_index'->>'source_key'
                         AS source_key
                FROM {schema}.ptg2_snapshot AS snapshot
               WHERE snapshot.snapshot_id = pointer.snapshot_id
          ) AS current_index ON TRUE
         WHERE pointer.slot = 'current'
         FOR UPDATE OF pointer
        """,
        {},
    )


async def _load_allowed_pointer(
    session: Any,
    schema: str,
    source_key: str,
) -> dict[str, Any]:
    return await _one(
        session,
        f"""
        SELECT pointer.source_key, pointer.snapshot_id,
               pointer.previous_snapshot_id, pointer.import_month,
               current_snapshot.import_month AS current_snapshot_import_month,
               previous_snapshot.import_month AS previous_snapshot_import_month
          FROM {schema}.ptg2_current_source_snapshot AS pointer
          LEFT JOIN {schema}.ptg2_snapshot AS current_snapshot
            ON current_snapshot.snapshot_id = pointer.snapshot_id
          LEFT JOIN {schema}.ptg2_snapshot AS previous_snapshot
            ON previous_snapshot.snapshot_id = pointer.previous_snapshot_id
         WHERE pointer.source_key = :allowed_source_key
         FOR UPDATE OF pointer
        """,
        {"allowed_source_key": _allowed_source_pointer_key(source_key)},
    )


async def load_rollback_context(
    session: Any,
    *,
    schema_name: str,
    source_key: str,
    snapshot_id: str,
    expected_current_snapshot_id: str,
    rollback_owner_type: str,
    rollback_owner_id: str,
) -> RollbackContext:
    """Load and lock all metadata and pointers used by one rollback decision."""

    schema = _quote_ident(schema_name)
    return RollbackContext(
        source_pointer_by_field=await _load_source_pointer(
            session, schema, source_key
        ),
        target_snapshot_by_field=await _load_target_snapshot(
            session, schema, snapshot_id
        ),
        expected_snapshot_by_field=await _load_expected_snapshot(
            session, schema, expected_current_snapshot_id
        ),
        rollback_pin_by_field=await _load_rollback_pin(
            session,
            schema,
            owner_type=rollback_owner_type,
            owner_id=rollback_owner_id,
            snapshot_id=snapshot_id,
        ),
        target_snapshot_scope_by_field=await load_target_snapshot_scope(
            session, schema, snapshot_id
        ),
        target_attestation_by_field=await load_target_attestation(
            session, schema, snapshot_id
        ),
        target_plan_scope_records=await _load_target_plan_scopes(
            session, schema, snapshot_id
        ),
        source_plan_pointer_records=await _load_source_plan_pointers(
            session, schema, source_key
        ),
        global_pointer_by_field=await _load_global_pointer(session, schema),
        allowed_pointer_by_field=await _load_allowed_pointer(
            session, schema, source_key
        ),
    )


async def _require_changed_row(
    session: Any,
    statement: str,
    params_by_name: Mapping[str, Any],
    *,
    failure_message: str,
) -> None:
    query_result = await session.execute(
        db.text(statement),
        dict(params_by_name),
    )
    if query_result.one_or_none() is None:
        raise PTG2SourceSnapshotRollbackConflict(failure_message)


async def _reverse_source_pointer(
    session: Any,
    schema: str,
    pointer_params_by_name: Mapping[str, Any],
) -> None:
    await _require_changed_row(
        session,
        f"""
        UPDATE {schema}.ptg2_current_source_snapshot
           SET snapshot_id = :snapshot_id,
               previous_snapshot_id = :expected_current_snapshot_id,
               import_month = :import_month,
               updated_at = :updated_at
         WHERE source_key = :source_key
           AND snapshot_id = :expected_current_snapshot_id
           AND previous_snapshot_id = :snapshot_id
        RETURNING source_key
        """,
        pointer_params_by_name,
        failure_message="source pointer changed while rollback was executing",
    )


async def _replace_plan_pointers(
    session: Any,
    schema: str,
    *,
    source_key: str,
    pointer_entries: list[dict[str, Any]],
) -> None:
    await session.execute(
        db.text(
            f"DELETE FROM {schema}.ptg2_current_plan_source "
            "WHERE source_key = :source_key"
        ),
        {"source_key": source_key},
    )
    await session.execute(
        db.text(
            f"""
            INSERT INTO {schema}.ptg2_current_plan_source
                (plan_source_key, plan_id, plan_market_type, import_month,
                 source_key, snapshot_id, previous_snapshot_id, updated_at)
            VALUES
                (:plan_source_key, :plan_id, :plan_market_type, :import_month,
                 :source_key, :snapshot_id, :previous_snapshot_id, :updated_at)
            """
        ),
        pointer_entries,
    )


async def _reverse_global_pointer(
    session: Any,
    schema: str,
    pointer_params_by_name: Mapping[str, Any],
) -> None:
    await _require_changed_row(
        session,
        f"""
        UPDATE {schema}.ptg2_current_snapshot
           SET snapshot_id = :snapshot_id,
               previous_snapshot_id = :expected_current_snapshot_id,
               updated_at = :updated_at
         WHERE slot = 'current'
           AND snapshot_id = :expected_current_snapshot_id
           AND previous_snapshot_id = :snapshot_id
        RETURNING slot
        """,
        pointer_params_by_name,
        failure_message="global pointer changed while rollback was executing",
    )


async def _delete_allowed_pointer(
    session: Any,
    schema: str,
    *,
    allowed_source_key: str,
    expected_current_snapshot_id: str,
) -> None:
    await _require_changed_row(
        session,
        f"""
        DELETE FROM {schema}.ptg2_current_source_snapshot
         WHERE source_key = :allowed_source_key
           AND snapshot_id = :expected_current_snapshot_id
           AND previous_snapshot_id IS NULL
        RETURNING source_key
        """,
        {
            "allowed_source_key": allowed_source_key,
            "expected_current_snapshot_id": expected_current_snapshot_id,
        },
        failure_message="allowed-amount pointer changed while rollback was executing",
    )


async def _reverse_allowed_pointer(
    session: Any,
    schema: str,
    *,
    allowed_source_key: str,
    expected_current_snapshot_id: str,
    target_snapshot_id: str,
    allowed_import_month: datetime.date,
    updated_at: datetime.datetime,
) -> None:
    await _require_changed_row(
        session,
        f"""
        UPDATE {schema}.ptg2_current_source_snapshot
           SET snapshot_id = :target_snapshot_id,
               previous_snapshot_id = :expected_current_snapshot_id,
               import_month = :allowed_import_month,
               updated_at = :updated_at
         WHERE source_key = :allowed_source_key
           AND snapshot_id = :expected_current_snapshot_id
           AND previous_snapshot_id = :target_snapshot_id
        RETURNING source_key
        """,
        {
            "allowed_source_key": allowed_source_key,
            "target_snapshot_id": target_snapshot_id,
            "expected_current_snapshot_id": expected_current_snapshot_id,
            "allowed_import_month": allowed_import_month,
            "updated_at": updated_at,
        },
        failure_message="allowed-amount pointer changed while rollback was executing",
    )


async def _apply_allowed_decision(
    session: Any,
    schema: str,
    *,
    source_key: str,
    expected_current_snapshot_id: str,
    import_month: datetime.date,
    updated_at: datetime.datetime,
    decision: RollbackDecision,
) -> None:
    allowed_source_key = _allowed_source_pointer_key(source_key)
    if decision.allowed_action == "delete":
        await _delete_allowed_pointer(
            session,
            schema,
            allowed_source_key=allowed_source_key,
            expected_current_snapshot_id=expected_current_snapshot_id,
        )
    elif decision.allowed_action == "reverse":
        await _reverse_allowed_pointer(
            session,
            schema,
            allowed_source_key=allowed_source_key,
            expected_current_snapshot_id=expected_current_snapshot_id,
            target_snapshot_id=str(decision.allowed_snapshot_id),
            allowed_import_month=decision.allowed_import_month,
            updated_at=updated_at,
        )


async def apply_rollback(
    session: Any,
    *,
    schema_name: str,
    source_key: str,
    snapshot_id: str,
    expected_current_snapshot_id: str,
    target_import_month: datetime.date,
    updated_at: datetime.datetime,
    decision: RollbackDecision,
) -> None:
    """Apply a validated pointer decision without touching snapshot metadata."""

    schema = _quote_ident(schema_name)
    pointer_params_by_name = {
        "source_key": source_key,
        "snapshot_id": snapshot_id,
        "expected_current_snapshot_id": expected_current_snapshot_id,
        "import_month": target_import_month,
        "updated_at": updated_at,
    }
    await _reverse_source_pointer(session, schema, pointer_params_by_name)
    pointer_entries = [
        {**dict(entry_by_field), "updated_at": updated_at}
        for entry_by_field in decision.plan_pointer_entries
    ]
    await _replace_plan_pointers(
        session,
        schema,
        source_key=source_key,
        pointer_entries=pointer_entries,
    )
    if decision.should_reverse_global_pointer:
        await _reverse_global_pointer(session, schema, pointer_params_by_name)
    await _apply_allowed_decision(
        session,
        schema,
        source_key=source_key,
        expected_current_snapshot_id=expected_current_snapshot_id,
        import_month=target_import_month,
        updated_at=updated_at,
        decision=decision,
    )
