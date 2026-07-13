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
    def from_payload(
        cls, payload: dict[str, Any], source_uri: str | None = None
    ) -> "PTG2ServingIndex":
        """Build an immutable serving index from its serialized payload."""

        return cls(
            snapshot_id=str(payload.get("snapshot_id") or ""),
            version=int(payload.get("version") or 1),
            plans=dict(payload.get("plans") or {}),
            procedures=dict(payload.get("procedures") or {}),
            providers={
                str(k): v for k, v in dict(payload.get("providers") or {}).items()
            },
            rates=dict(payload.get("rates") or {}),
            source_uri=source_uri,
        )


@dataclass(frozen=True)
class PTG2ServingTables:
    snapshot_id: str | None = None
    arch_version: str | None = None
    storage: str | None = None
    price_atom_constant_values: dict[str, Any] | None = None
    shared_snapshot_key: int | None = None
    storage_generation: str | None = None
    cold_lookup_contract: str | None = None
    price_dictionary_item_count: int | None = None
    price_dictionary_block_bytes: int | None = None
    atom_key_bits: int | None = None
    price_key_block_span: int | None = None
    atom_key_block_span: int | None = None
    serving_table_layout: str | None = None
    shared_block_layout: str | None = None
    source_count: int | None = None
    code_count: int | None = None
    coverage_scope_id: str | None = None
    plan_id: str | None = None
    plan_market_type: str | None = None
    source_trace_set_hash: str | None = None
    network_names: list[str] | None = None
    source_key: str | None = None
    audit_sample: dict[str, Any] | None = None
    source_set: dict[str, Any] | None = None
    database_evidence: dict[str, Any] | None = None

    @property
    def uses_shared_blocks(self) -> bool:
        """Return true only for the strict cache-free V3 storage contract."""
        return (
            (self.arch_version or "").strip().lower() == "postgres_binary_v3"
            and (self.storage_generation or "").strip().lower() == "shared_blocks_v3"
            and (self.cold_lookup_contract or "").strip().lower() == "ptg_v3_cold_v2"
            and (self.shared_block_layout or "").strip().lower()
            == "dense_shared_blocks_v3"
            and isinstance(self.shared_snapshot_key, int)
            and not isinstance(self.shared_snapshot_key, bool)
            and self.shared_snapshot_key > 0
            and isinstance(self.source_count, int)
            and not isinstance(self.source_count, bool)
            and 0 < self.source_count <= 2**31
        )
