# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared state types for exact published-snapshot rollback."""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Any, Mapping


ROLLBACK_PIN_OWNER_TYPE = "ptg_v4_rollback"


class PTG2SourceSnapshotRollbackConflict(RuntimeError):
    """Raised when the live pointer state or rollback pin is stale."""


@dataclass(frozen=True)
class RollbackContext:
    """Locked snapshot metadata and live pointers used for one decision."""

    source_pointer_by_field: Mapping[str, Any]
    target_snapshot_by_field: Mapping[str, Any]
    expected_snapshot_by_field: Mapping[str, Any]
    rollback_pin_by_field: Mapping[str, Any]
    target_snapshot_scope_by_field: Mapping[str, Any]
    target_attestation_by_field: Mapping[str, Any]
    target_plan_scope_records: tuple[Mapping[str, Any], ...]
    source_plan_pointer_records: tuple[Mapping[str, Any], ...]
    global_pointer_by_field: Mapping[str, Any]
    allowed_pointer_by_field: Mapping[str, Any]


@dataclass(frozen=True)
class RollbackDecision:
    """Validated pointer changes, or an exact no-write retry result."""

    is_already_rolled_back: bool
    plan_pointer_entries: tuple[Mapping[str, Any], ...]
    should_reverse_global_pointer: bool
    allowed_action: str
    allowed_snapshot_id: str | None = None
    allowed_previous_snapshot_id: str | None = None
    allowed_import_month: datetime.date | None = None
