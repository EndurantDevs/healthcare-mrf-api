# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed rollback of one published source snapshot to its pinned predecessor."""

from __future__ import annotations

import os
from typing import Any

from db.connection import db
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_lifecycle_lock
from process.ptg_parts.source_pointers import _allowed_source_pointer_key
from process.ptg_parts.source_snapshot_rollback_state import rollback_decision
from process.ptg_parts.source_snapshot_rollback_types import (
    ROLLBACK_PIN_OWNER_TYPE,
    RollbackDecision,
)
from process.ptg_parts.source_snapshot_rollback_store import (
    apply_rollback,
    database_utc_timestamp,
    load_rollback_context,
)


def _schema_name() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _normalized_coordinates(
    *,
    source_key: str,
    snapshot_id: str,
    expected_current_snapshot_id: str,
    rollback_owner_id: str,
) -> tuple[str, str, str, str]:
    normalized_coordinates = (
        str(source_key or "").strip().lower(),
        str(snapshot_id or "").strip(),
        str(expected_current_snapshot_id or "").strip(),
        str(rollback_owner_id or "").strip(),
    )
    if not all(normalized_coordinates):
        raise ValueError(
            "source_key, snapshot_id, expected_current_snapshot_id, "
            "and rollback_owner_id are required"
        )
    if normalized_coordinates[1] == normalized_coordinates[2]:
        raise ValueError("rollback target and expected current snapshot must differ")
    return normalized_coordinates


def _clear_ptg2_snapshot_cache() -> None:
    try:
        from api.ptg2_snapshot import _PTG2_SNAPSHOT_RESOLVE_CACHE
    except Exception:
        return
    _PTG2_SNAPSHOT_RESOLVE_CACHE.clear()


def _rollback_report(
    *,
    source_key: str,
    snapshot_id: str,
    expected_current_snapshot_id: str,
    decision: RollbackDecision,
) -> dict[str, Any]:
    report_by_field: dict[str, Any] = {
        "status": (
            "already_rolled_back"
            if decision.is_already_rolled_back
            else "rolled_back"
        ),
        "source_key": source_key,
        "snapshot_id": snapshot_id,
        "previous_snapshot_id": expected_current_snapshot_id,
        "plan_source_count": len(decision.plan_pointer_entries),
        "global_pointer": (
            "reversed"
            if decision.should_reverse_global_pointer
            else "unchanged"
        ),
        "idempotent": decision.is_already_rolled_back,
    }
    if decision.allowed_action not in {"unchanged", "verified"}:
        report_by_field["allowed_amount_pointer"] = {
            "status": (
                "removed"
                if decision.allowed_action == "delete"
                else "reversed"
            ),
            "source_key": _allowed_source_pointer_key(source_key),
            "snapshot_id": decision.allowed_snapshot_id,
            "previous_snapshot_id": decision.allowed_previous_snapshot_id,
        }
    return report_by_field


async def rollback_pinned_ptg2_source_snapshot(
    *,
    source_key: str,
    snapshot_id: str,
    expected_current_snapshot_id: str,
    rollback_owner_id: str,
) -> dict[str, Any]:
    """Atomically reverse a published source pointer to its exact pinned predecessor."""

    (
        normalized_source_key,
        normalized_snapshot_id,
        normalized_expected_snapshot_id,
        normalized_owner_id,
    ) = _normalized_coordinates(
        source_key=source_key,
        snapshot_id=snapshot_id,
        expected_current_snapshot_id=expected_current_snapshot_id,
        rollback_owner_id=rollback_owner_id,
    )
    schema_name = _schema_name()
    async with db.transaction() as session:
        await acquire_ptg2_lifecycle_lock(session)
        rollback_context = await load_rollback_context(
            session,
            schema_name=schema_name,
            source_key=normalized_source_key,
            snapshot_id=normalized_snapshot_id,
            expected_current_snapshot_id=normalized_expected_snapshot_id,
            rollback_owner_type=ROLLBACK_PIN_OWNER_TYPE,
            rollback_owner_id=normalized_owner_id,
        )
        decision = rollback_decision(
            rollback_context,
            source_key=normalized_source_key,
            snapshot_id=normalized_snapshot_id,
            expected_current_snapshot_id=normalized_expected_snapshot_id,
            rollback_owner_id=normalized_owner_id,
        )
        if not decision.is_already_rolled_back:
            updated_at = await database_utc_timestamp(session)
            await apply_rollback(
                session,
                schema_name=schema_name,
                source_key=normalized_source_key,
                snapshot_id=normalized_snapshot_id,
                expected_current_snapshot_id=normalized_expected_snapshot_id,
                target_import_month=rollback_context.target_snapshot_by_field[
                    "import_month"
                ],
                updated_at=updated_at,
                decision=decision,
            )
    _clear_ptg2_snapshot_cache()
    return _rollback_report(
        source_key=normalized_source_key,
        snapshot_id=normalized_snapshot_id,
        expected_current_snapshot_id=normalized_expected_snapshot_id,
        decision=decision,
    )
