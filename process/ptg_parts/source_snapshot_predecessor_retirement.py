# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Atomic audited retirement of one exact PTG predecessor retention set."""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any, Mapping

from db.connection import db
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_lifecycle_lock
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_parts.source_snapshot_predecessor_retirement_state import (
    predecessor_retirement_decision,
)
from process.ptg_parts.source_snapshot_predecessor_retirement_store import (
    apply_predecessor_retirement,
    insert_retirement_audit,
    load_retirement_audit,
    load_retirement_context,
    postcheck_predecessor_retirement,
)
from process.ptg_parts.source_snapshot_predecessor_retirement_types import (
    PTG2PredecessorRetirementConflict,
    PredecessorRetirementContext,
    PredecessorRetirementRequest,
)
from process.ptg_parts.source_snapshot_control_policy import (
    retirement_manifest_source_key,
)
from process.ptg_parts.source_snapshot_shared_layout import (
    validate_retirement_shared_layout,
)


_MAX_LENGTH_BY_FIELD = {
    "source_key": 96,
    "current_snapshot_id": 96,
    "predecessor_snapshot_id": 96,
    "rollback_pin_mode": 16,
    "rollback_owner_id": 96,
    "actor": 128,
    "reason": 512,
    "idempotency_key": 160,
}
_CONTROL_SCHEMA_ENV = "HLTHPRT_" + "IMP" + "ORT_CONTROL_SCHEMA"
_CONTROL_SCHEMA_DEFAULT = "hp_" + "imp" + "ort_control"


def _control_schema_name() -> str:
    return os.getenv(_CONTROL_SCHEMA_ENV) or _CONTROL_SCHEMA_DEFAULT


def _validate_normalized_request_coordinates(
    coordinates_by_field: Mapping[str, Any],
) -> None:
    required_fields = (
        "source_key",
        "current_snapshot_id",
        "predecessor_snapshot_id",
        "rollback_pin_mode",
        "actor",
        "reason",
        "idempotency_key",
    )
    if not all(coordinates_by_field[field] for field in required_fields):
        raise ValueError(
            "source_key, current_snapshot_id, predecessor_snapshot_id, "
            "rollback_pin_mode, actor, reason, and idempotency_key are required"
        )
    pin_mode = str(coordinates_by_field["rollback_pin_mode"])
    normalized_owner_id = coordinates_by_field["rollback_owner_id"]
    if pin_mode not in {"absent", "owned"}:
        raise ValueError("rollback_pin_mode must be owned or absent")
    if pin_mode == "owned" and normalized_owner_id is None:
        raise ValueError("rollback_owner_id is required for owned pin mode")
    if pin_mode == "absent" and normalized_owner_id is not None:
        raise ValueError("rollback_owner_id must be omitted for absent pin mode")
    oversized_fields = [
        field
        for field, field_text in coordinates_by_field.items()
        if field_text is not None
        and len(str(field_text)) > _MAX_LENGTH_BY_FIELD[field]
    ]
    if oversized_fields:
        raise ValueError(f"{oversized_fields[0]} exceeds its maximum length")
    if (
        coordinates_by_field["current_snapshot_id"]
        == coordinates_by_field["predecessor_snapshot_id"]
    ):
        raise ValueError("current and predecessor snapshots must differ")


def normalized_predecessor_retirement_request(
    *,
    source_key: str,
    current_snapshot_id: str,
    predecessor_snapshot_id: str,
    rollback_pin_mode: str,
    rollback_owner_id: str | None,
    actor: str,
    reason: str,
    idempotency_key: str,
) -> PredecessorRetirementRequest:
    """Normalize bounded coordinates and derive their stable request digest."""

    coordinates_by_field: dict[str, Any] = {
        "source_key": str(source_key or "").strip().lower(),
        "current_snapshot_id": str(current_snapshot_id or "").strip(),
        "predecessor_snapshot_id": str(predecessor_snapshot_id or "").strip(),
        "rollback_pin_mode": str(rollback_pin_mode or "").strip().lower(),
        "rollback_owner_id": str(rollback_owner_id or "").strip() or None,
        "actor": str(actor or "").strip(),
        "reason": str(reason or "").strip(),
        "idempotency_key": str(idempotency_key or "").strip(),
    }
    _validate_normalized_request_coordinates(coordinates_by_field)
    request_digest = hashlib.sha256(
        json.dumps(
            coordinates_by_field,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return PredecessorRetirementRequest(
        **coordinates_by_field,
        request_digest=request_digest,
    )


def _is_exact_audit_replay(
    audit_record: Mapping[str, Any],
    request: PredecessorRetirementRequest,
) -> bool:
    return all(
        (
            audit_record.get(field) is None
            if value is None
            else str(audit_record.get(field) or "") == value
        )
        for field, value in request.audit_coordinates().items()
    )


def _retirement_report(
    audit_record: Mapping[str, Any],
    *,
    idempotent: bool,
) -> dict[str, Any]:
    return {
        "status": "already_retired" if idempotent else "retired",
        "source_key": str(audit_record.get("source_key") or ""),
        "current_snapshot_id": str(
            audit_record.get("current_snapshot_id") or ""
        ),
        "predecessor_snapshot_id": str(
            audit_record.get("predecessor_snapshot_id") or ""
        ),
        "rollback_pin_mode": str(
            audit_record.get("rollback_pin_mode") or ""
        ),
        "rollback_owner_id": audit_record.get("rollback_owner_id"),
        "idempotency_key": str(audit_record.get("idempotency_key") or ""),
        "retired_at": audit_record.get("retired_at"),
        "cleared_source_pointer_count": int(
            audit_record.get("cleared_source_pointer_count") or 0
        ),
        "cleared_plan_pointer_count": int(
            audit_record.get("cleared_plan_pointer_count") or 0
        ),
        "cleared_global_pointer_count": int(
            audit_record.get("cleared_global_pointer_count") or 0
        ),
        "deleted_rollback_pin_count": int(
            audit_record.get("deleted_rollback_pin_count") or 0
        ),
        "idempotent": idempotent,
    }


def _predecessor_snapshot(
    context: PredecessorRetirementContext,
    predecessor_snapshot_id: str,
) -> dict[str, Any]:
    return next(
        (
            dict(snapshot)
            for snapshot in context.snapshot_records
            if str(snapshot.get("snapshot_id") or "").strip()
            == predecessor_snapshot_id
        ),
        {},
    )


async def _validate_predecessor_removal_contract(
    session: Any,
    *,
    schema_name: str,
    context: PredecessorRetirementContext,
    request: PredecessorRetirementRequest,
) -> None:
    predecessor_snapshot = _predecessor_snapshot(
        context,
        request.predecessor_snapshot_id,
    )
    try:
        retirement_manifest_source_key(
            predecessor_snapshot,
            request.source_key,
        )
        await validate_retirement_shared_layout(
            session,
            schema=schema_name,
            snapshot_id=request.predecessor_snapshot_id,
            snapshot=predecessor_snapshot,
        )
    except (RuntimeError, ValueError) as exc:
        raise PTG2PredecessorRetirementConflict(
            f"predecessor does not satisfy the snapshot removal contract: {exc}"
        ) from exc


async def _execute_predecessor_retirement(
    session: Any,
    *,
    schema_name: str,
    control_schema_name: str,
    request: PredecessorRetirementRequest,
) -> dict[str, Any]:
    await acquire_ptg2_lifecycle_lock(session)
    existing_audit = await load_retirement_audit(
        session,
        schema_name=schema_name,
        idempotency_key=request.idempotency_key,
    )
    if existing_audit:
        if not _is_exact_audit_replay(existing_audit, request):
            raise PTG2PredecessorRetirementConflict(
                "idempotency key was already used for a different "
                "predecessor retirement request"
            )
        return _retirement_report(existing_audit, idempotent=True)
    context = await load_retirement_context(
        session,
        schema_name=schema_name,
        control_schema_name=control_schema_name,
        request=request,
    )
    decision = predecessor_retirement_decision(
        context,
        source_key=request.source_key,
        current_snapshot_id=request.current_snapshot_id,
        predecessor_snapshot_id=request.predecessor_snapshot_id,
        rollback_pin_mode=request.rollback_pin_mode,
        rollback_owner_id=request.rollback_owner_id,
    )
    await _validate_predecessor_removal_contract(
        session,
        schema_name=schema_name,
        context=context,
        request=request,
    )
    await apply_predecessor_retirement(
        session,
        schema_name=schema_name,
        request=request,
        decision=decision,
    )
    await postcheck_predecessor_retirement(
        session,
        schema_name=schema_name,
        control_schema_name=control_schema_name,
        request=request,
    )
    audit_record = await insert_retirement_audit(
        session,
        schema_name=schema_name,
        request=request,
        decision=decision,
    )
    return _retirement_report(audit_record, idempotent=False)


async def retire_ptg2_source_predecessor(
    *,
    source_key: str,
    current_snapshot_id: str,
    predecessor_snapshot_id: str,
    rollback_pin_mode: str,
    rollback_owner_id: str | None,
    actor: str,
    reason: str,
    idempotency_key: str,
) -> dict[str, Any]:
    """Retire exact rollback retention without creating another generation."""

    request = normalized_predecessor_retirement_request(
        source_key=source_key,
        current_snapshot_id=current_snapshot_id,
        predecessor_snapshot_id=predecessor_snapshot_id,
        rollback_pin_mode=rollback_pin_mode,
        rollback_owner_id=rollback_owner_id,
        actor=actor,
        reason=reason,
        idempotency_key=idempotency_key,
    )
    schema_name = resolve_ptg2_schema()
    control_schema_name = _control_schema_name()
    async with db.transaction() as session:
        return await _execute_predecessor_retirement(
            session,
            schema_name=schema_name,
            control_schema_name=control_schema_name,
            request=request,
        )


__all__ = [
    "normalized_predecessor_retirement_request",
    "retire_ptg2_source_predecessor",
]
