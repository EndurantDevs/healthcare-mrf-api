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
    price_atom_table: str | None = None
    code_count_table: str | None = None
    provider_group_member_table: str | None = None
    storage: str | None = None
    type: str | None = None
    snapshot_scoped: bool = False
    source_key: str | None = None
    artifact_uri: str | None = None
    artifacts: dict[str, Any] | None = None
    id_storage: str = "hex"

    @property
    def is_manifest_backed_snapshot(self) -> bool:
        storage = (self.storage or "").strip().lower()
        return storage == "manifest_snapshot"

    @property
    def uses_uuid_ids(self) -> bool:
        return (self.id_storage or "").strip().lower() == "uuid"
