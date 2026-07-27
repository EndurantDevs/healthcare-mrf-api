# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Types for exact PTG predecessor-retention retirement."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


class PTG2PredecessorRetirementConflict(ValueError):
    """Raised when requested retirement coordinates do not match live state."""


@dataclass(frozen=True)
class PredecessorRetirementRequest:
    """Normalized coordinates and immutable request identity."""

    source_key: str
    current_snapshot_id: str
    predecessor_snapshot_id: str
    rollback_pin_mode: str
    rollback_owner_id: str | None
    actor: str
    reason: str
    idempotency_key: str
    request_digest: str

    def audit_coordinates(self) -> dict[str, Any]:
        """Return the exact values persisted in the immutable audit."""

        return {
            "source_key": self.source_key,
            "current_snapshot_id": self.current_snapshot_id,
            "predecessor_snapshot_id": self.predecessor_snapshot_id,
            "rollback_pin_mode": self.rollback_pin_mode,
            "rollback_owner_id": self.rollback_owner_id,
            "actor": self.actor,
            "reason": self.reason,
            "idempotency_key": self.idempotency_key,
            "request_digest": self.request_digest,
        }


@dataclass(frozen=True)
class PredecessorRetirementContext:
    """Locked database records used by the retirement policy."""

    snapshot_records: tuple[Mapping[str, Any], ...]
    source_pointer_records: tuple[Mapping[str, Any], ...]
    plan_pointer_records: tuple[Mapping[str, Any], ...]
    global_pointer_records: tuple[Mapping[str, Any], ...]
    pin_records: tuple[Mapping[str, Any], ...]
    control_pin_records: tuple[Mapping[str, Any], ...]
    release_binding_records: tuple[Mapping[str, Any], ...]
    control_release_binding_records: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class PredecessorRetirementDecision:
    """Exact pointer counts approved for compare-and-swap mutation."""

    source_pointer_count: int
    plan_pointer_count: int
    global_pointer_count: int
    deleted_rollback_pin_count: int


__all__ = [
    "PTG2PredecessorRetirementConflict",
    "PredecessorRetirementContext",
    "PredecessorRetirementDecision",
    "PredecessorRetirementRequest",
]
