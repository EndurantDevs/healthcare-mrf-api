# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure fail-closed policy for exact predecessor-retention retirement."""

from __future__ import annotations

import json
from typing import Any, Mapping

from process.ptg_parts.source_snapshot_predecessor_retirement_types import (
    PTG2PredecessorRetirementConflict,
    PredecessorRetirementContext,
    PredecessorRetirementDecision,
)
from process.ptg_parts.source_snapshot_rollback_types import (
    ROLLBACK_PIN_OWNER_TYPE,
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _manifest_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not isinstance(value, str):
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _snapshot_source_key(snapshot: Mapping[str, Any]) -> str:
    manifest = _manifest_mapping(snapshot.get("manifest"))
    serving_index = _manifest_mapping(manifest.get("serving_index"))
    return _text(serving_index.get("source_key")).lower()


def _validate_snapshot_lineage(
    records: tuple[Mapping[str, Any], ...],
    *,
    source_key: str,
    current_snapshot_id: str,
    predecessor_snapshot_id: str,
) -> None:
    snapshot_by_id = {
        _text(record.get("snapshot_id")): record for record in records
    }
    current = snapshot_by_id.get(current_snapshot_id)
    predecessor = snapshot_by_id.get(predecessor_snapshot_id)
    if (
        len(snapshot_by_id) != 2
        or current is None
        or predecessor is None
        or _text(current.get("previous_snapshot_id"))
        != predecessor_snapshot_id
        or _text(current.get("status")).lower() != "published"
        or _text(predecessor.get("status")).lower() != "published"
        or _snapshot_source_key(current) != source_key
        or _snapshot_source_key(predecessor) != source_key
    ):
        raise PTG2PredecessorRetirementConflict(
            "snapshot lineage does not match the requested current/predecessor pair"
        )


def _is_pointer_pair_match(
    record: Mapping[str, Any],
    *,
    current_snapshot_id: str,
    predecessor_snapshot_id: str,
) -> bool:
    return (
        _text(record.get("snapshot_id")) == current_snapshot_id
        and _text(record.get("previous_snapshot_id"))
        == predecessor_snapshot_id
    )


def _validate_source_pointer(
    records: tuple[Mapping[str, Any], ...],
    *,
    source_key: str,
    current_snapshot_id: str,
    predecessor_snapshot_id: str,
) -> int:
    requested_source_records = tuple(
        record
        for record in records
        if _text(record.get("source_key")).lower() == source_key
    )
    if (
        len(requested_source_records) != 1
        or not _is_pointer_pair_match(
            requested_source_records[0],
            current_snapshot_id=current_snapshot_id,
            predecessor_snapshot_id=predecessor_snapshot_id,
        )
    ):
        raise PTG2PredecessorRetirementConflict(
            "source pointer does not match the requested current/predecessor pair"
        )
    if len(records) != 1:
        raise PTG2PredecessorRetirementConflict(
            "predecessor has an unexpected live reference"
        )
    return 1


def _validate_plan_pointers(
    pointer_records: tuple[Mapping[str, Any], ...],
    *,
    source_key: str,
    current_snapshot_id: str,
    predecessor_snapshot_id: str,
) -> int:
    requested_source_records = tuple(
        pointer_by_field
        for pointer_by_field in pointer_records
        if _text(pointer_by_field.get("source_key")).lower() == source_key
    )
    if any(
        _text(pointer_by_field.get("snapshot_id"))
        == predecessor_snapshot_id
        for pointer_by_field in pointer_records
    ):
        raise PTG2PredecessorRetirementConflict(
            "source plan pointer is an unexpected live reference"
        )
    if not requested_source_records or any(
        not _is_pointer_pair_match(
            pointer_by_field,
            current_snapshot_id=current_snapshot_id,
            predecessor_snapshot_id=predecessor_snapshot_id,
        )
        for pointer_by_field in requested_source_records
    ):
        raise PTG2PredecessorRetirementConflict(
            "source plan pointer does not match the requested "
            "current/predecessor pair"
        )
    if len(requested_source_records) != len(pointer_records):
        raise PTG2PredecessorRetirementConflict(
            "predecessor has an unexpected live reference"
        )
    return len(pointer_records)


def _validate_global_pointer(
    pointer_records: tuple[Mapping[str, Any], ...],
    *,
    current_snapshot_id: str,
    predecessor_snapshot_id: str,
) -> int:
    applicable_records = tuple(
        pointer_by_field
        for pointer_by_field in pointer_records
        if _text(pointer_by_field.get("snapshot_id"))
        == current_snapshot_id
    )
    predecessor_references = tuple(
        pointer_by_field
        for pointer_by_field in pointer_records
        if _text(pointer_by_field.get("snapshot_id"))
        == predecessor_snapshot_id
        or _text(pointer_by_field.get("previous_snapshot_id"))
        == predecessor_snapshot_id
    )
    if applicable_records:
        if (
            len(applicable_records) != 1
            or not _is_pointer_pair_match(
                applicable_records[0],
                current_snapshot_id=current_snapshot_id,
                predecessor_snapshot_id=predecessor_snapshot_id,
            )
            or predecessor_references != applicable_records
        ):
            raise PTG2PredecessorRetirementConflict(
                "applicable global pointer does not match the requested "
                "current/predecessor pair"
            )
        return 1
    if predecessor_references:
        raise PTG2PredecessorRetirementConflict(
            "predecessor has an unexpected live reference"
        )
    return 0


def _validate_exclusive_pin(
    pin_records: tuple[Mapping[str, Any], ...],
    *,
    predecessor_snapshot_id: str,
    rollback_pin_mode: str,
    rollback_owner_id: str | None,
) -> int:
    if rollback_pin_mode == "absent":
        if pin_records:
            raise PTG2PredecessorRetirementConflict(
                "predecessor has a non-target retention pin"
            )
        return 0
    if rollback_pin_mode != "owned" or not rollback_owner_id:
        raise PTG2PredecessorRetirementConflict(
            "rollback pin expectation is invalid"
        )
    matching_pin_records = tuple(
        pin_by_field
        for pin_by_field in pin_records
        if _text(pin_by_field.get("owner_type")) == ROLLBACK_PIN_OWNER_TYPE
        and _text(pin_by_field.get("owner_id")) == rollback_owner_id
        and _text(pin_by_field.get("snapshot_id"))
        == predecessor_snapshot_id
        and bool(_text(pin_by_field.get("reason")))
    )
    if len(matching_pin_records) != 1:
        raise PTG2PredecessorRetirementConflict(
            "predecessor does not have the exact rollback pin"
        )
    if len(pin_records) != 1:
        raise PTG2PredecessorRetirementConflict(
            "predecessor has a non-target retention pin"
        )
    return 1


def predecessor_retirement_decision(
    context: PredecessorRetirementContext,
    *,
    source_key: str,
    current_snapshot_id: str,
    predecessor_snapshot_id: str,
    rollback_pin_mode: str,
    rollback_owner_id: str | None,
) -> PredecessorRetirementDecision:
    """Validate every live retention surface before authorizing any write."""

    _validate_snapshot_lineage(
        context.snapshot_records,
        source_key=source_key,
        current_snapshot_id=current_snapshot_id,
        predecessor_snapshot_id=predecessor_snapshot_id,
    )
    source_count = _validate_source_pointer(
        context.source_pointer_records,
        source_key=source_key,
        current_snapshot_id=current_snapshot_id,
        predecessor_snapshot_id=predecessor_snapshot_id,
    )
    plan_count = _validate_plan_pointers(
        context.plan_pointer_records,
        source_key=source_key,
        current_snapshot_id=current_snapshot_id,
        predecessor_snapshot_id=predecessor_snapshot_id,
    )
    global_count = _validate_global_pointer(
        context.global_pointer_records,
        current_snapshot_id=current_snapshot_id,
        predecessor_snapshot_id=predecessor_snapshot_id,
    )
    deleted_pin_count = _validate_exclusive_pin(
        context.pin_records,
        predecessor_snapshot_id=predecessor_snapshot_id,
        rollback_pin_mode=rollback_pin_mode,
        rollback_owner_id=rollback_owner_id,
    )
    if context.control_pin_records:
        raise PTG2PredecessorRetirementConflict(
            "predecessor has a non-target retention pin"
        )
    if (
        context.release_binding_records
        or context.control_release_binding_records
    ):
        raise PTG2PredecessorRetirementConflict(
            "predecessor has a plan release binding"
        )
    return PredecessorRetirementDecision(
        source_pointer_count=source_count,
        plan_pointer_count=plan_count,
        global_pointer_count=global_count,
        deleted_rollback_pin_count=deleted_pin_count,
    )


__all__ = ["predecessor_retirement_decision"]
