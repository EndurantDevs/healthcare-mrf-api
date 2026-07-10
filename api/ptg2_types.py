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
    arch_version: str | None = None
    provider_scope_strategy: str | None = None
    materialized_tables: dict[str, Any] | None = None
    price_code_set_table: str | None = None
    price_atom_table: str | None = None
    price_atom_table_layout: str | None = None
    price_atom_dictionary_table: str | None = None
    price_atom_constant_keys: dict[str, Any] | None = None
    price_atom_constant_values: dict[str, Any] | None = None
    price_set_entry_table: str | None = None
    procedure_table: str | None = None
    code_count_table: str | None = None
    provider_set_table: str | None = None
    provider_set_component_table: str | None = None
    provider_set_entry_table: str | None = None
    provider_entry_component_table: str | None = None
    provider_group_member_table: str | None = None
    provider_npi_scope_table: str | None = None
    provider_group_location_table: str | None = None
    provider_group_rate_scope_table: str | None = None
    provider_set_dictionary_table: str | None = None
    serving_binary_table: str | None = None
    serving_table_layout: str | None = None
    source_trace_set_hash: str | None = None
    network_names: list[str] | None = None
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

    @property
    def effective_arch_version(self) -> str:
        """Return the serving architecture, inferring legacy manifests when needed."""
        arch_version = (self.arch_version or "").strip().lower()
        if arch_version:
            return arch_version
        if self.provider_group_rate_scope_table and self.provider_set_component_table:
            return "materialized_v1"
        if (
            self.is_manifest_backed_snapshot
            and not self.provider_group_rate_scope_table
            and not self.provider_set_component_table
        ):
            if self.serving_binary_table:
                return "postgres_binary_v1"
            return "sidecar_scope_v1"
        return "legacy_mixed_v1"

    @property
    def uses_sidecar_provider_scope(self) -> bool:
        """Return true when provider-set membership is served from sidecar artifacts."""
        strategy = (self.provider_scope_strategy or "").strip().lower()
        return strategy == "sidecar_provider_scope" or self.effective_arch_version in {
            "sidecar_scope_v1",
            "postgres_binary_v1",
            "postgres_binary_v2",
        }
