# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared PTG2 serving data containers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PTG2ServingIndex:
    snapshot_id: str
    version: int
    plans: dict[str, Any]
    procedures: dict[str, Any]
    providers: dict[str, Any]
    rates: dict[str, Any]
    source_uri: str | None = None

    @classmethod
    def from_payload(cls, payload: dict[str, Any], source_uri: str | None = None) -> "PTG2ServingIndex":
        return cls(
            snapshot_id=str(payload.get("snapshot_id") or ""),
            version=int(payload.get("version") or 1),
            plans=dict(payload.get("plans") or {}),
            procedures=dict(payload.get("procedures") or {}),
            providers={str(k): v for k, v in dict(payload.get("providers") or {}).items()},
            rates=dict(payload.get("rates") or {}),
            source_uri=source_uri,
        )


@dataclass(frozen=True)
class PTG2ServingTables:
    serving_table: str | None = None
    price_code_set_table: str | None = None
    price_atom_table: str | None = None
    price_set_entry_table: str | None = None
    procedure_table: str | None = None
    provider_set_table: str | None = None
    provider_set_component_table: str | None = None
    provider_set_entry_table: str | None = None
    provider_entry_component_table: str | None = None
    provider_group_member_table: str | None = None
    provider_group_location_table: str | None = None
