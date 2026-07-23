# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Allowed-amount pointer validation for exact source rollback."""

from __future__ import annotations

import datetime
import json
from typing import Any, Mapping

from process.ptg_parts.allowed_amounts import PTG2_ALLOWED_AMOUNT_CONTRACT
from process.ptg_parts.domain import PTG2_DOMAIN_ALLOWED_AMOUNT
from process.ptg_parts.source_pointers import _allowed_source_pointer_key
from process.ptg_parts.source_snapshot_rollback_types import (
    PTG2SourceSnapshotRollbackConflict,
)


def _manifest_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _allowed_pointer_predecessor(
    snapshot_by_field: Mapping[str, Any],
    *,
    source_key: str,
) -> tuple[bool, str | None]:
    """Return whether a snapshot advanced the optional allowed-amount pointer."""

    manifest_by_field = _manifest_mapping(snapshot_by_field.get("manifest"))
    raw_allowed_index = manifest_by_field.get("allowed_amount_index")
    if raw_allowed_index is None:
        return False, None
    if not isinstance(raw_allowed_index, dict):
        raise ValueError("published snapshot allowed-amount index must be an object")
    allowed_index_by_field = dict(raw_allowed_index)
    required_value_by_field = {
        "contract": PTG2_ALLOWED_AMOUNT_CONTRACT,
        "arch_version": "postgres_binary_v3",
        "storage": "postgresql",
        "data_domain": PTG2_DOMAIN_ALLOWED_AMOUNT,
        "source_key": source_key,
        "current_source_key": _allowed_source_pointer_key(source_key),
    }
    for field_name, expected_value in required_value_by_field.items():
        if allowed_index_by_field.get(field_name) != expected_value:
            raise ValueError(
                "published snapshot allowed-amount index has an invalid "
                f"{field_name} binding"
            )
    if allowed_index_by_field.get("snapshot_scoped") is not True:
        raise ValueError(
            "published snapshot allowed-amount index is not snapshot scoped"
        )
    predecessor = (
        str(allowed_index_by_field.get("previous_snapshot_id") or "").strip()
        or None
    )
    return True, predecessor


def _completed_pointer_decision(
    pointer_by_field: Mapping[str, Any],
    *,
    predecessor: str | None,
    expected_current_snapshot_id: str,
) -> tuple[str, str | None, str | None, datetime.date | None]:
    """Verify the allowed pointer already has the exact reversed state."""

    current_snapshot_id = str(pointer_by_field.get("snapshot_id") or "") or None
    previous_snapshot_id = (
        str(pointer_by_field.get("previous_snapshot_id") or "") or None
    )
    required_pair = (
        (predecessor, expected_current_snapshot_id)
        if predecessor is not None
        else (None, None)
    )
    if (current_snapshot_id, previous_snapshot_id) != required_pair:
        raise PTG2SourceSnapshotRollbackConflict(
            "allowed-amount pointer does not match an exact completed rollback"
        )
    current_import_month = pointer_by_field.get("current_snapshot_import_month")
    if (
        predecessor is not None
        and (
            not isinstance(current_import_month, datetime.date)
            or pointer_by_field.get("import_month") != current_import_month
        )
    ):
        raise PTG2SourceSnapshotRollbackConflict(
            "allowed-amount pointer has an invalid completed import month"
        )
    return (
        "verified",
        predecessor,
        expected_current_snapshot_id,
        current_import_month,
    )


def _forward_pointer_decision(
    pointer_by_field: Mapping[str, Any],
    *,
    predecessor: str | None,
    expected_current_snapshot_id: str,
) -> tuple[str, str | None, str | None, datetime.date | None]:
    """Return the symmetric reverse or delete operation for a live pointer."""

    current_snapshot_id = str(pointer_by_field.get("snapshot_id") or "") or None
    previous_snapshot_id = (
        str(pointer_by_field.get("previous_snapshot_id") or "") or None
    )
    if (
        current_snapshot_id != expected_current_snapshot_id
        or previous_snapshot_id != predecessor
    ):
        raise PTG2SourceSnapshotRollbackConflict(
            "allowed-amount pointer does not match its published predecessor"
        )
    if predecessor is None:
        return "delete", None, None, None
    predecessor_import_month = pointer_by_field.get(
        "previous_snapshot_import_month"
    )
    if not isinstance(predecessor_import_month, datetime.date):
        raise PTG2SourceSnapshotRollbackConflict(
            "allowed-amount predecessor has no retained import month"
        )
    return (
        "reverse",
        predecessor,
        expected_current_snapshot_id,
        predecessor_import_month,
    )


def allowed_pointer_decision(
    expected_snapshot_by_field: Mapping[str, Any],
    pointer_by_field: Mapping[str, Any],
    *,
    source_key: str,
    expected_current_snapshot_id: str,
    is_already_rolled_back: bool,
) -> tuple[str, str | None, str | None, datetime.date | None]:
    """Validate the optional allowed pointer and return its exact action."""

    expected_owns_pointer, predecessor = _allowed_pointer_predecessor(
        expected_snapshot_by_field,
        source_key=source_key,
    )
    current_snapshot_id = str(pointer_by_field.get("snapshot_id") or "") or None
    if not expected_owns_pointer:
        if current_snapshot_id == expected_current_snapshot_id:
            raise PTG2SourceSnapshotRollbackConflict(
                "allowed-amount pointer is not declared by the current snapshot"
            )
        return "unchanged", None, None, None
    if is_already_rolled_back:
        return _completed_pointer_decision(
            pointer_by_field,
            predecessor=predecessor,
            expected_current_snapshot_id=expected_current_snapshot_id,
        )
    return _forward_pointer_decision(
        pointer_by_field,
        predecessor=predecessor,
        expected_current_snapshot_id=expected_current_snapshot_id,
    )
