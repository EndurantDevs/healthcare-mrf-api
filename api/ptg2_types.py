# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared PTG2 serving data containers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:
    from api.ptg2_db_sidecars import ForwardReadBudget
    from process.ptg_parts.ptg2_v4_taxonomy_candidates import V4InferredTaxonomyProjectionRule

from process.ptg_parts.ptg2_physical_binding import (
    PTG2PhysicalBinding,
    PTG2PhysicalBindingError,
)
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourcePublication,
)


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
        """Build an immutable serving index from its serialized payload."""

        return cls(
            snapshot_id=str(payload.get("snapshot_id") or ""),
            version=int(payload.get("version") or 1),
            plans=dict(payload.get("plans") or {}),
            procedures=dict(payload.get("procedures") or {}),
            providers={
                str(provider_key): provider_payload
                for provider_key, provider_payload in dict(payload.get("providers") or {}).items()
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
    provider_shard_span: int | None = None
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
    source_witness: dict[str, Any] | None = None
    source_set: dict[str, Any] | None = None
    database_evidence: dict[str, Any] | None = None
    provider_graph_v4_hot_prefix: dict[str, Any] | None = None
    provider_graph_v4_inferred_taxonomy_candidates: dict[str, Any] | None = None
    provider_tax_identity_source_publication: TaxIdentitySourcePublication | None = None
    physical_binding: PTG2PhysicalBinding | None = None

    def __post_init__(self):
        """The descriptor is a carrier; every LOCAL read still requires fresh native resolution."""
        if self.physical_binding is not None and (
            not isinstance(self.physical_binding, PTG2PhysicalBinding)
            or self.snapshot_id != self.physical_binding.snapshot_id
            or self.shared_snapshot_key != self.physical_binding.payload_snapshot_key
        ):
            raise PTG2PhysicalBindingError("PTG snapshot-local serving descriptor is not available")

    @property
    def uses_shared_blocks(self) -> bool:
        """Return true for a strict sealed V3-price shared-block layout."""

        generation = (self.storage_generation or "").strip().lower()
        expected_layout = {
            "shared_blocks_v3": "dense_shared_blocks_v3",
            "shared_blocks_v4": "packed_snapshot_maps_v4",
        }.get(generation)
        return (
            (self.arch_version or "").strip().lower() == "postgres_binary_v3"
            and expected_layout is not None
            and (self.cold_lookup_contract or "").strip().lower() == "ptg_v3_cold_v2"
            and (self.shared_block_layout or "").strip().lower() == expected_layout
            and isinstance(self.shared_snapshot_key, int)
            and not isinstance(self.shared_snapshot_key, bool)
            and self.shared_snapshot_key > 0
            and isinstance(self.source_count, int)
            and not isinstance(self.source_count, bool)
            and 0 < self.source_count <= 2**31
        )

    @property
    def uses_v4_graph(self) -> bool:
        """Return true only for the packed V4 provider-graph generation."""

        return self.uses_shared_blocks and ((self.storage_generation or "").strip().lower() == "shared_blocks_v4")


@dataclass(frozen=True)
class _V4PatternCompletionRequest:
    """Inputs and sealed caps for one selected-pattern completion."""

    code_rows: list[Mapping[str, Any]]
    prefix_rows: list[dict[str, Any]]
    candidate_provider_set_keys: tuple[int, ...]
    source_trace_set_hash: str | None
    network_names: list[str]
    descending: bool
    is_source_exhausted: bool
    maximum_occurrences: int
    maximum_code_sets: int
    scan_budget: ForwardReadBudget


@dataclass(frozen=True)
class _ProviderExpansionRequest:
    """Shared immutable inputs for one cost-ordered provider expansion."""

    code_rows: list[Mapping[str, Any]]
    args: Mapping[str, Any]
    snapshot_id: str
    source_trace_set_hash: str | None
    network_names: list[str]
    target_count: int
    descending: bool


@dataclass(frozen=True)
class _V4PatternTaxonomyRequest(_ProviderExpansionRequest):
    """Inputs for an exact pattern-quotient taxonomy expansion."""

    projection_rule: V4InferredTaxonomyProjectionRule
    candidates: Any


@dataclass(frozen=True)
class _V4PatternContext:
    """Sealed coordinates and caps for one pattern-quotient selection."""

    request: _V4PatternTaxonomyRequest
    normalized_target_count: int
    maximum_occurrences: int
    declared_occurrences: int
    scan_budget: ForwardReadBudget
    pattern_keys: tuple[int, ...]
    snapshot_key: int
    schema_name: str


@dataclass(frozen=True)
class _V4TaxonomyRequest(_ProviderExpansionRequest):
    """Inputs for an exact inferred-taxonomy expansion."""

    projection_manifest: Mapping[str, Any]
    projection_rule: V4InferredTaxonomyProjectionRule


@dataclass(frozen=True)
class _V4DirectContext:
    """Immutable caps and source coordinates for one direct-layout request."""

    request: _V4TaxonomyRequest
    candidates: Any
    snapshot_key: int
    maximum_occurrences: int
    maximum_code_sets: int
    declared_occurrences: int
    scan_budget: ForwardReadBudget
    candidate_npi_keys: tuple[int, ...]


@dataclass(frozen=True)
class _V4DirectSetPrefix:
    """Authenticated candidate intersection for one ordered set prefix."""

    candidate_npi_keys: tuple[int, ...]
    is_complete: bool


@dataclass(frozen=True)
class _V4PatternPrefix:
    """Authenticated pattern prefix and its selected completion scope."""

    serving_rows: list[dict[str, Any]]
    selected_occurrences: tuple[tuple[int, int], ...]
    selected_npi_keys: tuple[int, ...]
    completion_provider_set_keys: tuple[int, ...]
    completion_pattern_keys_by_set: dict[int, tuple[int, ...]]
    is_candidate_prefix_exhausted: bool
    is_source_exhausted: bool


@dataclass(frozen=True)
class _V4DirectPrefix:
    """One authenticated direct-layout rate prefix and its ranked candidates."""

    serving_rows: list[dict[str, Any]]
    selected_occurrences: tuple[tuple[int, int], ...]
    is_candidate_prefix_exhausted: bool
    is_source_exhausted: bool


@dataclass(frozen=True)
class _V4DirectResolved:
    """Selected NPI identities and exact memberships from one ranked prefix."""

    prefix: _V4DirectPrefix
    selected_npi_keys: tuple[int, ...]
    selected_npis: tuple[int, ...]
    npi_by_key: dict[int, int]
    provider_set_keys_by_npi: dict[int, tuple[int, ...]]
