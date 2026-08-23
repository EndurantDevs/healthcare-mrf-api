# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validated subprocess bridge for the adaptive Rust V4 graph compiler."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import re
import shutil
import signal
import stat
import struct
import time
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Iterable, Mapping

from process.ptg_parts.canonical import canonical_json_dumps
from process.ptg_parts.live_progress import (
    current_live_progress_context,
    write_live_progress,
)


PTG2_V4_GRAPH_COMPILER_BIN_ENV = "HLTHPRT_PTG2_PROVIDER_GRAPH_V4_BIN"
PTG2_V4_GRAPH_SUMMARY_FORMAT = "ptg2_provider_graph_v4_factor_adaptive_v1"
# Dense-owner metadata is bounded separately from the multi-gigabyte graph model.
PTG2_V4_GRAPH_SUMMARY_MAX_BYTES = 256 * 1024 * 1024
PTG2_V4_GRAPH_ERROR_TAIL_BYTES = 8 * 1024
PTG2_V4_SHARED_FORMAT_VERSION = 2
PTG2_V4_GRAPH_CHECKPOINT_FORMAT = "ptg2_provider_graph_v4_checkpoint_v2"
PTG2_V4_GRAPH_CHECKPOINT_NAME = "v4-complete.json"
PTG2_V4_GRAPH_SCRATCH_OWNER_NAME = ".ptg2-v4-compiler-owned-v1"
PTG2_V4_GRAPH_SCRATCH_OWNER_BYTES = b"ptg2-v4-python-compiler-scratch-v1\n"
PTG2_V4_GRAPH_CHECKPOINT_MAX_BYTES = 512 * 1024
PTG2_V4_GRAPH_HEARTBEAT_SECONDS = 4.0
PTG2_V4_ADAPTIVE_LAYOUT_DECISION_CONTRACT = (
    "ptg2_provider_graph_v4_adaptive_layout_decision_v1"
)
PTG2_V4_ADAPTIVE_LAYOUT_COST_CONTRACT = "encoded_persistent_projection_v1"
PTG2_V4_ADAPTIVE_LAYOUT_SELECTION_POLICY = (
    "lowest_eligible_encoded_persistent_projection_bytes_direct_exact_tie_v1"
)
PTG2_V4_PROGRESS_PREFIX = b"PTG2_V4_PROGRESS\t"
PTG2_V4_PROGRESS_VERSION = 1
PTG2_V4_PROGRESS_MAX_LINE_BYTES = 16 * 1024
PTG2_V4_GRAPH_MAX_MODEL_BYTES_ENV = "HLTHPRT_PTG2_V4_GRAPH_MAX_ESTIMATED_MODEL_BYTES"
PTG2_V4_GRAPH_MAX_FACTOR_EDGES_ENV = "HLTHPRT_PTG2_V4_GRAPH_MAX_FACTOR_EDGES"
PTG2_V4_GRAPH_MEMBER_PAGE_BYTES_ENV = "HLTHPRT_PTG2_V4_GRAPH_MEMBER_PAGE_BYTES"
PTG2_V4_GRAPH_LOCATOR_PAGE_BYTES_ENV = "HLTHPRT_PTG2_V4_GRAPH_LOCATOR_PAGE_BYTES"
PTG2_V4_GRAPH_HEAVY_OWNER_THRESHOLD_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_HEAVY_OWNER_MEMBER_THRESHOLD"
)
PTG2_V4_GRAPH_HEAVY_MIN_SAVINGS_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_HEAVY_BITMAP_MINIMUM_SAVINGS_BYTES"
)
PTG2_V4_GRAPH_MAX_SET_PATTERNS_ENV = "HLTHPRT_PTG2_V4_GRAPH_MAX_SET_PATTERNS_PER_SET"
PTG2_V4_GRAPH_MAX_SET_COMPONENTS_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_SET_COMPONENTS_PER_FALLBACK_SET"
)
PTG2_V4_GRAPH_MAX_ONLINE_GROUP_KEYS_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_ONLINE_GROUP_KEYS_PER_SET"
)
PTG2_V4_GRAPH_MAX_ONLINE_SOURCE_OWNERS_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_ONLINE_SOURCE_OWNERS_PER_SET"
)
PTG2_V4_GRAPH_MAX_ONLINE_SOURCE_MEMBERS_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_ONLINE_SOURCE_MEMBERS_PER_SET"
)
PTG2_V4_GRAPH_MAX_ONLINE_SOURCE_PAGES_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_ONLINE_SOURCE_PAGES_PER_SET"
)
PTG2_V4_GRAPH_MAX_ONLINE_SOURCE_BYTES_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_ONLINE_SOURCE_BYTES_PER_SET"
)
PTG2_V4_GRAPH_ONLINE_GROUP_NPI_BATCH_SIZE_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_ONLINE_GROUP_NPI_BATCH_SIZE"
)
PTG2_V4_GRAPH_MAX_ONLINE_GROUP_NPI_MEMBERS_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_ONLINE_GROUP_NPI_MEMBERS_PER_SET"
)
PTG2_V4_GRAPH_MAX_ONLINE_GROUP_NPI_LOCATOR_PAGES_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_ONLINE_GROUP_NPI_LOCATOR_PAGES_PER_SET"
)
PTG2_V4_GRAPH_MAX_ONLINE_GROUP_NPI_MEMBER_PAGES_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_ONLINE_GROUP_NPI_MEMBER_PAGES_PER_SET"
)
PTG2_V4_GRAPH_MAX_ONLINE_GROUP_NPI_BYTES_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_ONLINE_GROUP_NPI_BYTES_PER_SET"
)
PTG2_V4_GRAPH_MAX_ONLINE_GROUP_NPI_BATCHES_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_ONLINE_GROUP_NPI_BATCHES_PER_SET"
)
PTG2_V4_GRAPH_PROVIDER_EXPANSION_RATE_PAGE_ROWS_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_PROVIDER_EXPANSION_RATE_PAGE_ROWS"
)
PTG2_V4_GRAPH_MAX_ONLINE_PROVIDER_EXPANSION_RATE_ROWS_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_ONLINE_PROVIDER_EXPANSION_RATE_ROWS"
)
PTG2_V4_GRAPH_MAX_ONLINE_PROVIDER_EXPANSION_PROVIDER_SETS_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_ONLINE_PROVIDER_EXPANSION_PROVIDER_SETS"
)
PTG2_V4_GRAPH_MAX_ONLINE_PROVIDER_EXPANSION_GRAPH_BATCHES_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_ONLINE_PROVIDER_EXPANSION_GRAPH_BATCHES"
)
PTG2_V4_GRAPH_NPI_PREFIX_TARGET_ENV = "HLTHPRT_PTG2_V4_GRAPH_NPI_PREFIX_TARGET"
PTG2_V4_GRAPH_MAX_NPI_PREFIX_OVERRIDE_OWNERS_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_NPI_PREFIX_OVERRIDE_OWNERS"
)
PTG2_V4_GRAPH_MAX_NPI_PREFIX_OVERRIDE_BYTES_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_NPI_PREFIX_OVERRIDE_BYTES"
)
PTG2_V4_GRAPH_DEFAULT_MAX_MODEL_BYTES = 16 * 1024 * 1024 * 1024
PTG2_V4_GRAPH_DEFAULT_MAX_FACTOR_EDGES = 2_000_000_000
PTG2_V4_GRAPH_DEFAULT_MAX_INFERRED_TAXONOMY_CANDIDATES = 37_000
PTG2_V4_GRAPH_DEFAULT_MAX_CANDIDATE_PATTERN_MEMBERS = 131_072
PTG2_V4_NPI_SCOPE_FORMAT = "ptg2_provider_graph_v4_npi_scope_v1"
PTG2_V4_NPI_SCOPE_ARTIFACT_FORMAT = "ptg2_provider_npi_scope_pg_binary_int8_v1"
PTG2_V4_NPI_SCOPE_BINDING_CONTRACT = "provider_npi_scope_to_provider_npi_group_v1"
PTG2_V4_NPI_SCOPE_BINDING_HASH_DOMAIN = b"ptg2:v4:provider-npi-scope-binding:v1\x00"
PTG2_V4_NPI_SCOPE_SHARD_BINDING_CONTRACT = "provider_npi_scope_shard_binding_v1"
PTG2_V4_NPI_SCOPE_SHARD_BINDING_HASH_DOMAIN = (
    b"ptg2:v4:provider-npi-scope-shard-binding:v1\x00"
)
PTG2_V4_NPI_SCOPE_RETENTION_CONTRACT = "shared_v4_publication_scratch_v1"
PTG2_V4_DENSE_MEMBERSHIP_FORMAT = (
    "magic8:uint32_le_version:uint64_le_entry_count:"
    "uint64_le_member_global_count:"
    "index(owner16:uint64_le_offset:uint32_le_count):"
    "member_globals16:members_uint32_le"
)
PTG2_V4_INFERRED_TAXONOMY_INPUT_CONTRACT = "ptg2_v4_inferred_taxonomy_compiler_input_v1"
PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT = "snapshot_npi_live_catalog_individual_v1"
PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT = "sorted_u32le_v1"
PTG2_V4_GRAPH_ENCODING_OPTION_NAMES = (
    "member_page_bytes",
    "locator_page_bytes",
    "heavy_owner_member_threshold",
    "heavy_bitmap_minimum_savings_bytes",
    "max_set_patterns_per_set",
    "max_set_components_per_fallback_set",
    "max_online_group_keys_per_set",
    "max_online_source_owners_per_set",
    "max_online_source_members_per_set",
    "max_online_source_pages_per_set",
    "max_online_source_bytes_per_set",
    "online_group_npi_batch_size",
    "max_online_group_npi_members_per_set",
    "max_online_group_npi_locator_pages_per_set",
    "max_online_group_npi_member_pages_per_set",
    "max_online_group_npi_bytes_per_set",
    "max_online_group_npi_batches_per_set",
    "provider_expansion_rate_page_rows",
    "max_online_provider_expansion_rate_rows",
    "max_online_provider_expansion_provider_sets",
    "max_online_provider_expansion_graph_batches",
    "npi_prefix_target",
    "max_npi_prefix_override_owners",
    "max_npi_prefix_override_bytes",
    "max_online_inferred_taxonomy_candidates",
    "max_online_candidate_pattern_projection_members",
)

_PG_COPY_HEADER = b"PGCOPY\n\xff\r\n\0" + struct.pack(">II", 0, 0)
_PG_COPY_TRAILER = struct.pack(">h", -1)
_ARTIFACT_FIELD_BY_NAME = {
    "provider_set_component": "provider_set_component",
    "provider_component_group": "provider_component_group",
    "provider_group_npi": "provider_group_npi",
    "provider_npi_group": "provider_npi_group",
    "provider_npi_scope": "provider_npi_scope",
    "provider_group_tax_identity": "provider_group_tax_identity",
}
_REQUIRED_MEMBERSHIP_SHARD_FIELDS = frozenset(
    {
        "provider_set_component",
        "provider_component_group",
        "provider_group_npi",
        "provider_npi_group",
    }
)
_REQUIRED_SHARD_FIELDS = frozenset(_ARTIFACT_FIELD_BY_NAME.values())
_NPI_SCOPE_FIELD = "provider_npi_scope"
_TAX_IDENTITY_FIELD = "provider_group_tax_identity"
_TAX_IDENTITY_FORMAT = "ptg2_provider_group_tax_identity_v1"
_TAX_IDENTITY_VERSION = 1
_TAX_IDENTITY_RECORD_BYTES = 65
_TAX_IDENTITY_NORMALIZATION_CONTRACT = "ein_ascii_digits_or_2_7_hyphen_v1"
_TAX_IDENTITY_HMAC_CONTRACT = "hmac_sha256_ptg_tin_v1"
_TAX_IDENTITY_CANDIDATE_PREFIX_CONTRACT = "tin_id_128=first_16_bytes(tin_hmac_sha256)"
_TAX_IDENTITY_AUTHORITY_CONTRACT = "tin_hmac_sha256_full_32_bytes_authoritative"
_TAX_IDENTITY_PROJECTION_CONTRACT = "ptg2_provider_tax_identity_projection_v1"
_TAX_SOURCE_ORDINAL_CONTRACT = "snapshot_shard_id_sorted_lsb0_bitmap_v1"
_TAX_SOURCE_ORDINAL_FIXED_UPPER_BOUND_BYTES = 256
_TAX_SOURCE_IDENTITY_COPY_UPPER_BOUND = 2
_TAX_IDENTITY_GROUP_ENTRY_UPPER_BOUND_BYTES = 256
_TAX_IDENTITY_DICTIONARY_ENTRY_UPPER_BOUND_BYTES = 128
_TAX_POLICY_DESCRIPTOR_HASH_DOMAIN = b"PTG2V4TINPOLICY\x01"
_TAX_SOURCE_ORDINAL_HASH_DOMAIN = b"PTG2V4TAXORD\x01"
_TAX_POLICY_ID = re.compile(r"ptg-tin-hmac-sha256-v1:[a-z0-9](?:[a-z0-9._-]{0,31})\Z")
_TAX_STATE_CODE_BY_NAME = {
    "matched_ein": 1,
    "missing": 2,
    "malformed": 3,
    "unsupported_type": 4,
}
_OUTPUT_FILE_BY_NAME = {
    "graph_blocks": ("v4-graph-blocks.copy", 10),
    "graph_references": ("v4-graph-references.jsonl", None),
    "provider_groups": ("v4-provider-groups.copy", 2),
    "provider_components": ("v4-provider-components.copy", 2),
    "npi_scope": ("v4-npi-scope.copy", 2),
    "provider_set_audit_npi": ("v4-provider-set-audit-npi.copy", 3),
    "provider_set_npi_prefix_overrides": (
        "v4-provider-set-npi-prefix-overrides.copy",
        3,
    ),
    "provider_tax_identities": ("v4-provider-tax-identities.copy", 3),
    "provider_group_tax_identities": (
        "v4-provider-group-tax-identities.copy",
        4,
    ),
    "inferred_taxonomy_candidates": (
        "v4-inferred-taxonomy-candidates.copy",
        15,
    ),
    "patterns": ("v4-patterns.copy", 3),
}
_PROGRESS_PHASES = (
    "resource_admission",
    "load_factors",
    "build_model",
    "derive_patterns",
    "derive_npi_patterns",
    "select_layout",
    "emit_relations",
    "emit_bitmaps",
    "emit_dictionaries",
    "complete",
)
_PROGRESS_PHASE_BOUNDS = {
    "resource_admission": (0.0, 2.0),
    "load_factors": (2.0, 20.0),
    "build_model": (20.0, 34.0),
    "derive_patterns": (34.0, 60.0),
    "derive_npi_patterns": (60.0, 72.0),
    "select_layout": (72.0, 74.0),
    "emit_relations": (74.0, 90.0),
    "emit_bitmaps": (90.0, 94.0),
    "emit_dictionaries": (94.0, 99.0),
    "complete": (100.0, 100.0),
}


@dataclass(frozen=True)
class V4GraphOutputArtifact:
    """One authenticated compiler output consumed by publication."""

    name: str
    path: Path
    byte_count: int
    sha256: str
    row_count: int


@dataclass(frozen=True)
class V4GraphNpiScopePreparation:
    """Authenticated exact snapshot NPI universe emitted before catalog lookup."""

    copy_path: Path
    manifest: Mapping[str, Any]
    graph_artifact_entries: tuple[Mapping[str, Any], ...]
    source_scope_directory: Path

    def cleanup(self) -> None:
        """Remove only compiler-owned scope scratch."""

        self.copy_path.unlink(missing_ok=True)
        shutil.rmtree(self.source_scope_directory, ignore_errors=True)


@dataclass(frozen=True)
class V4GraphCompilationResult:
    """Validated adaptive graph artifacts with recoverable scratch ownership."""

    scratch_directory: Path
    summary_path: Path
    block_copy_path: Path
    reference_manifest_path: Path
    group_copy_path: Path
    component_copy_path: Path
    npi_copy_path: Path
    provider_set_audit_npi_copy_path: Path
    provider_set_npi_prefix_override_copy_path: Path
    provider_tax_identity_copy_path: Path
    provider_group_tax_identity_copy_path: Path
    pattern_copy_path: Path | None
    inferred_taxonomy_copy_path: Path
    selected_layout: str
    direct_complete_encoded_bytes: int
    pattern_complete_encoded_bytes: int
    selected_encoded_bytes: int
    block_count: int
    relation_summaries: tuple[Mapping[str, Any], ...]
    heavy_bitmaps: tuple[Mapping[str, Any], ...]
    observe: Mapping[str, Any]
    output_artifacts: tuple[V4GraphOutputArtifact, ...]
    resource_admission: Mapping[str, Any]
    checkpoint_reused: bool
    summary: Mapping[str, Any]

    def cleanup(self) -> None:
        """Remove compiler scratch after publication or rollback."""

        shutil.rmtree(self.scratch_directory, ignore_errors=True)


@dataclass(frozen=True)
class _CompilerSummaryExpectation:
    """Inputs and limits that authenticate one native compiler summary."""

    input_bytes: int
    factor_edges: int
    factor_owners: int
    options: Mapping[str, int]
    tax_identity: Mapping[str, Any] | None
    taxonomy_rule_count: int | None


class V4GraphResourceAdmissionError(RuntimeError):
    """The compiler rejected declared factor scale before opening the model."""


@dataclass
class _CompilerProgressState:
    seq: int = 0
    phase_index: int = -1
    phase: str = "starting"
    done: int = 0
    total: int = 1
    unit: str = "stage"
    elapsed_ms: int = 0
    terminal: bool = False
    phase_pct: float = 0.0

    def is_accepted(self, raw: Any) -> bool:
        """Validate and retain one strictly monotonic compiler progress event."""

        if not isinstance(raw, dict):
            return False
        try:
            version = _strict_nonnegative_int(
                raw.get("version"), label="progress version"
            )
            seq = _strict_nonnegative_int(raw.get("seq"), label="progress seq")
            done = _strict_nonnegative_int(raw.get("done"), label="progress done")
            total = _strict_nonnegative_int(raw.get("total"), label="progress total")
            elapsed_ms = _strict_nonnegative_int(
                raw.get("elapsed_ms"), label="progress elapsed_ms"
            )
        except RuntimeError:
            return False
        phase = raw.get("phase")
        unit = raw.get("unit")
        terminal = raw.get("terminal")
        if (
            version != PTG2_V4_PROGRESS_VERSION
            or seq != self.seq + 1
            or isinstance(terminal, bool) is False
            or not isinstance(phase, str)
            or phase not in _PROGRESS_PHASE_BOUNDS
            or not isinstance(unit, str)
            or not unit
            or done > total
            or elapsed_ms < self.elapsed_ms
        ):
            return False
        phase_index = _PROGRESS_PHASES.index(phase)
        if phase_index < self.phase_index:
            return False
        if phase_index == self.phase_index and (
            done < self.done or total != self.total or unit != self.unit
        ):
            return False
        if terminal and not (phase == "complete" and done == total and total > 0):
            return False
        if self.terminal:
            return False
        lower, upper = _PROGRESS_PHASE_BOUNDS[phase]
        fraction = (done / total) if total else 1.0
        phase_pct = lower + (upper - lower) * fraction
        if phase_pct < self.phase_pct:
            return False
        self.seq = seq
        self.phase_index = phase_index
        self.phase = phase
        self.done = done
        self.total = total
        self.unit = unit
        self.elapsed_ms = elapsed_ms
        self.terminal = terminal
        self.phase_pct = phase_pct
        return True


def _strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    object_by_field: dict[str, Any] = {}
    for key, value in pairs:
        if key in object_by_field:
            raise RuntimeError(f"V4 graph compiler JSON repeats field {key!r}")
        object_by_field[key] = value
    return object_by_field


def _load_json_bytes(payload: bytes, *, label: str) -> Any:
    try:
        return json.loads(payload, object_pairs_hook=_strict_json_object)
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"V4 graph compiler {label} is invalid JSON") from exc


def _strict_nonnegative_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeError(f"V4 graph compiler has invalid {label}")
    if value > 2**63 - 1:
        raise RuntimeError(f"V4 graph compiler has out-of-range {label}")
    return value


def _adaptive_layout_evidence_digest(evidence: Mapping[str, Any]) -> str:
    """Hash one canonical layout decision without its self-authenticating digest."""

    return hashlib.sha256(
        canonical_json_dumps(dict(evidence)).encode("utf-8")
    ).hexdigest()


_ADAPTIVE_DECISION_FIELDS = frozenset(
    {
        "contract",
        "cost_contract",
        "selection_policy",
        "compiler_options",
        "selected_representation",
        "selected_encoded_bytes",
        "direct",
        "pattern",
        "decision_digest",
    }
)
_ADAPTIVE_OPTION_FIELDS = frozenset(PTG2_V4_GRAPH_ENCODING_OPTION_NAMES) | {
    "max_estimated_model_bytes",
    "max_factor_edges",
}
_ADAPTIVE_COST_FIELDS = (
    "graph_encoded_bytes",
    "mapping_persistence_encoded_bytes",
    "inferred_taxonomy_encoded_bytes",
    "map_payload_encoded_bytes",
    "map_coordinate_count",
    "map_pack_count",
    "map_object_kind_count",
    "complete_persistent_encoded_bytes",
)
_ADAPTIVE_CANDIDATE_FIELDS = frozenset(
    {
        "eligible",
        *_ADAPTIVE_COST_FIELDS,
        "inferred_taxonomy_eligible",
        "inferred_taxonomy_rejection_reason",
        "inferred_taxonomy_rejection_rule_digest",
        "inferred_taxonomy_rejection_observed_count",
        "inferred_taxonomy_rejection_cap",
    }
)
_ADAPTIVE_DIRECT_FIELDS = _ADAPTIVE_CANDIDATE_FIELDS | {
    "complete_prefix_eligible",
    "complete_prefix_projection_encoded_bytes",
}
_ADAPTIVE_PATTERN_FIELDS = _ADAPTIVE_CANDIDATE_FIELDS | {
    "component_fallback_eligible",
    "unsafe_component_set_count",
    "sparse_prefix_eligible",
    "sparse_prefix_owner_count",
    "sparse_prefix_member_count",
    "sparse_prefix_raw_bytes",
    "sparse_prefix_projection_encoded_bytes",
}


def _validated_adaptive_options(raw_options: Any) -> dict[str, int]:
    """Validate the complete compiler-option vector in one decision."""

    if (
        not isinstance(raw_options, Mapping)
        or set(raw_options) != _ADAPTIVE_OPTION_FIELDS
    ):
        raise RuntimeError("V4 adaptive layout compiler option fields changed")
    options_by_name = {
        field_name: _strict_nonnegative_int(
            raw_options.get(field_name),
            label=f"adaptive layout compiler_options.{field_name}",
        )
        for field_name in sorted(_ADAPTIVE_OPTION_FIELDS)
    }
    if not all(options_by_name.values()):
        raise RuntimeError("V4 adaptive layout compiler options are invalid")
    return options_by_name


def _is_adaptive_cost_consistent(
    candidate_by_field: Mapping[str, Any],
) -> bool:
    """Return whether one candidate's encoded cost components reconcile."""

    return (
        candidate_by_field["complete_persistent_encoded_bytes"]
        == candidate_by_field["graph_encoded_bytes"]
        + candidate_by_field["mapping_persistence_encoded_bytes"]
        + candidate_by_field["inferred_taxonomy_encoded_bytes"]
        and candidate_by_field["map_payload_encoded_bytes"]
        == candidate_by_field["map_coordinate_count"] * 52
        + candidate_by_field["map_pack_count"] * 80
        and candidate_by_field["map_payload_encoded_bytes"]
        <= candidate_by_field["mapping_persistence_encoded_bytes"]
        and bool(candidate_by_field["map_coordinate_count"])
        == bool(candidate_by_field["map_pack_count"])
        and bool(candidate_by_field["map_pack_count"])
        == bool(candidate_by_field["map_object_kind_count"])
        and candidate_by_field["map_object_kind_count"]
        <= candidate_by_field["map_pack_count"]
        <= candidate_by_field["map_coordinate_count"]
    )


def _validate_taxonomy_witness(
    candidate_by_field: Mapping[str, Any],
    *,
    candidate_name: str,
) -> None:
    """Validate selected or rejected taxonomy-projection evidence."""

    is_taxonomy_eligible = candidate_by_field.get("inferred_taxonomy_eligible")
    if not isinstance(is_taxonomy_eligible, bool):
        raise RuntimeError(
            f"V4 adaptive layout {candidate_name} taxonomy eligibility is invalid"
        )
    rejection_values = (
        candidate_by_field.get("inferred_taxonomy_rejection_reason"),
        candidate_by_field.get("inferred_taxonomy_rejection_rule_digest"),
        candidate_by_field.get("inferred_taxonomy_rejection_observed_count"),
        candidate_by_field.get("inferred_taxonomy_rejection_cap"),
    )
    if is_taxonomy_eligible:
        if any(rejection_value is not None for rejection_value in rejection_values):
            raise RuntimeError(
                f"V4 adaptive layout {candidate_name} taxonomy witness is invalid"
            )
        return
    reason, rule_digest, observed_count, cap = rejection_values
    if (
        reason != "pattern_projection_cap_exceeded"
        or not isinstance(rule_digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", rule_digest) is None
        or _strict_nonnegative_int(
            observed_count,
            label=f"{candidate_name} taxonomy observed count",
        )
        <= _strict_nonnegative_int(
            cap,
            label=f"{candidate_name} taxonomy cap",
        )
    ):
        raise RuntimeError(
            f"V4 adaptive layout {candidate_name} taxonomy witness is invalid"
        )


def _validated_adaptive_candidate(
    raw_candidate: Any,
    *,
    candidate_name: str,
    expected_fields: frozenset[str],
) -> dict[str, Any]:
    """Validate fields shared by direct and pattern layout candidates."""

    if not isinstance(raw_candidate, Mapping) or set(raw_candidate) != expected_fields:
        raise RuntimeError("V4 adaptive layout candidate fields changed")
    candidate_by_field = dict(raw_candidate)
    if not isinstance(candidate_by_field.get("eligible"), bool):
        raise RuntimeError(
            f"V4 adaptive layout {candidate_name} eligibility is invalid"
        )
    for field_name in _ADAPTIVE_COST_FIELDS:
        candidate_by_field[field_name] = _strict_nonnegative_int(
            candidate_by_field.get(field_name),
            label=f"adaptive layout {candidate_name}.{field_name}",
        )
    if not _is_adaptive_cost_consistent(candidate_by_field):
        raise RuntimeError(f"V4 adaptive layout {candidate_name} cost is inconsistent")
    _validate_taxonomy_witness(
        candidate_by_field,
        candidate_name=candidate_name,
    )
    return candidate_by_field


def _validate_direct_candidate(
    direct_by_field: dict[str, Any],
    options_by_name: Mapping[str, int],
) -> None:
    """Validate complete-prefix and taxonomy eligibility for direct layout."""

    if not isinstance(direct_by_field.get("complete_prefix_eligible"), bool):
        raise RuntimeError("V4 adaptive direct prefix eligibility is invalid")
    projection_bytes = _strict_nonnegative_int(
        direct_by_field.get("complete_prefix_projection_encoded_bytes"),
        label="adaptive layout direct prefix projection bytes",
    )
    direct_by_field["complete_prefix_projection_encoded_bytes"] = projection_bytes
    is_expected_prefix_eligible = (
        projection_bytes <= options_by_name["max_npi_prefix_override_bytes"]
        and projection_bytes <= options_by_name["max_estimated_model_bytes"]
    )
    is_expected_direct_eligible = (
        is_expected_prefix_eligible and direct_by_field["inferred_taxonomy_eligible"]
    )
    if (
        direct_by_field["complete_prefix_eligible"] is not is_expected_prefix_eligible
        or direct_by_field["eligible"] is not is_expected_direct_eligible
    ):
        raise RuntimeError("V4 adaptive direct eligibility is inconsistent")


def _validate_pattern_candidate(
    pattern_by_field: dict[str, Any],
    options_by_name: Mapping[str, int],
) -> None:
    """Validate component and sparse-prefix eligibility for pattern layout."""

    for field_name in ("component_fallback_eligible", "sparse_prefix_eligible"):
        if not isinstance(pattern_by_field.get(field_name), bool):
            raise RuntimeError(f"V4 adaptive pattern {field_name} is invalid")
    numeric_fields = (
        "unsafe_component_set_count",
        "sparse_prefix_owner_count",
        "sparse_prefix_member_count",
        "sparse_prefix_raw_bytes",
        "sparse_prefix_projection_encoded_bytes",
    )
    for field_name in numeric_fields:
        pattern_by_field[field_name] = _strict_nonnegative_int(
            pattern_by_field.get(field_name),
            label=f"adaptive layout pattern.{field_name}",
        )
    is_component_eligible = pattern_by_field["unsafe_component_set_count"] == 0
    is_sparse_eligible = (
        pattern_by_field["sparse_prefix_owner_count"]
        <= options_by_name["max_npi_prefix_override_owners"]
        and pattern_by_field["sparse_prefix_member_count"]
        <= pattern_by_field["sparse_prefix_owner_count"]
        * options_by_name["npi_prefix_target"]
        and pattern_by_field["sparse_prefix_raw_bytes"]
        <= options_by_name["max_npi_prefix_override_bytes"]
    )
    is_pattern_eligible = (
        is_component_eligible
        and is_sparse_eligible
        and pattern_by_field["inferred_taxonomy_eligible"]
    )
    if (
        pattern_by_field["component_fallback_eligible"] is not is_component_eligible
        or pattern_by_field["sparse_prefix_eligible"] is not is_sparse_eligible
        or pattern_by_field["eligible"] is not is_pattern_eligible
        or pattern_by_field["sparse_prefix_raw_bytes"]
        != pattern_by_field["sparse_prefix_member_count"] * 4
        or pattern_by_field["sparse_prefix_projection_encoded_bytes"]
        < pattern_by_field["sparse_prefix_raw_bytes"]
    ):
        raise RuntimeError("V4 adaptive pattern eligibility is inconsistent")


def _adaptive_selected_representation(
    direct_by_field: Mapping[str, Any],
    pattern_by_field: Mapping[str, Any],
) -> str:
    """Choose the least-byte eligible layout with a direct exact tie."""

    if (
        pattern_by_field["eligible"]
        and pattern_by_field["complete_persistent_encoded_bytes"]
        < direct_by_field["complete_persistent_encoded_bytes"]
    ):
        return "pattern_v1"
    if direct_by_field["eligible"]:
        return "direct_v1"
    if pattern_by_field["eligible"]:
        return "pattern_v1"
    raise RuntimeError("V4 adaptive layout has no eligible representation")


def _validate_adaptive_digest(evidence_by_field: Mapping[str, Any]) -> None:
    """Authenticate a decision against its canonical evidence bytes."""

    decision_digest = evidence_by_field.get("decision_digest")
    if (
        not isinstance(decision_digest, str)
        or re.fullmatch(
            r"[0-9a-f]{64}",
            decision_digest,
        )
        is None
    ):
        raise RuntimeError("V4 adaptive layout decision digest is invalid")
    digest_input_by_field = dict(evidence_by_field)
    digest_input_by_field.pop("decision_digest")
    if decision_digest != _adaptive_layout_evidence_digest(digest_input_by_field):
        raise RuntimeError("V4 adaptive layout decision digest changed")


def validate_v4_adaptive_layout_decision(
    raw: Any,
) -> dict[str, Any]:
    """Validate and return one complete source-independent layout decision."""

    if not isinstance(raw, Mapping):
        raise RuntimeError("V4 adaptive layout decision is not an object")
    evidence_by_field = dict(raw)
    if set(evidence_by_field) != _ADAPTIVE_DECISION_FIELDS:
        raise RuntimeError("V4 adaptive layout decision fields changed")
    if (
        evidence_by_field.get("contract") != PTG2_V4_ADAPTIVE_LAYOUT_DECISION_CONTRACT
        or evidence_by_field.get("cost_contract")
        != PTG2_V4_ADAPTIVE_LAYOUT_COST_CONTRACT
        or evidence_by_field.get("selection_policy")
        != PTG2_V4_ADAPTIVE_LAYOUT_SELECTION_POLICY
    ):
        raise RuntimeError("V4 adaptive layout decision contract changed")
    options_by_name = _validated_adaptive_options(
        evidence_by_field.get("compiler_options")
    )
    direct_by_field = _validated_adaptive_candidate(
        evidence_by_field.get("direct"),
        candidate_name="direct",
        expected_fields=_ADAPTIVE_DIRECT_FIELDS,
    )
    pattern_by_field = _validated_adaptive_candidate(
        evidence_by_field.get("pattern"),
        candidate_name="pattern",
        expected_fields=_ADAPTIVE_PATTERN_FIELDS,
    )
    _validate_direct_candidate(direct_by_field, options_by_name)
    _validate_pattern_candidate(pattern_by_field, options_by_name)
    expected_representation = _adaptive_selected_representation(
        direct_by_field,
        pattern_by_field,
    )
    selected_encoded_bytes = _strict_nonnegative_int(
        evidence_by_field.get("selected_encoded_bytes"),
        label="adaptive layout selected encoded bytes",
    )
    selected_by_field = (
        pattern_by_field if expected_representation == "pattern_v1" else direct_by_field
    )
    if (
        evidence_by_field.get("selected_representation") != expected_representation
        or selected_encoded_bytes
        != selected_by_field["complete_persistent_encoded_bytes"]
    ):
        raise RuntimeError("V4 adaptive layout decision changed")
    _validate_adaptive_digest(evidence_by_field)
    return evidence_by_field


def _is_summary_field_enabled(
    summary_by_field: Mapping[str, Any],
    field_name: str,
) -> bool:
    field_value = summary_by_field.get(field_name)
    if not isinstance(field_value, bool):
        raise RuntimeError("V4 adaptive layout compiler eligibility is invalid")
    return field_value


def _summary_candidate_costs(
    summary_by_field: Mapping[str, Any],
    *,
    prefix: str,
) -> dict[str, Any]:
    return {
        "graph_encoded_bytes": summary_by_field.get(f"{prefix}_graph_encoded_bytes"),
        "mapping_persistence_encoded_bytes": summary_by_field.get(
            f"{prefix}_mapping_persistence_encoded_bytes"
        ),
        "inferred_taxonomy_encoded_bytes": summary_by_field.get(
            f"{prefix}_inferred_taxonomy_encoded_bytes"
        ),
        "map_payload_encoded_bytes": summary_by_field.get(
            f"{prefix}_map_payload_encoded_bytes"
        ),
        "map_coordinate_count": summary_by_field.get(f"{prefix}_map_coordinate_count"),
        "map_pack_count": summary_by_field.get(f"{prefix}_map_pack_count"),
        "map_object_kind_count": summary_by_field.get(
            f"{prefix}_map_object_kind_count"
        ),
        "complete_persistent_encoded_bytes": summary_by_field.get(
            f"{prefix}_complete_encoded_bytes"
        ),
    }


def _summary_taxonomy_fields(
    summary_by_field: Mapping[str, Any],
    *,
    prefix: str,
    is_eligible: bool,
) -> dict[str, Any]:
    return {
        "inferred_taxonomy_eligible": is_eligible,
        "inferred_taxonomy_rejection_reason": summary_by_field.get(
            f"{prefix}_inferred_taxonomy_rejection_reason"
        ),
        "inferred_taxonomy_rejection_rule_digest": summary_by_field.get(
            f"{prefix}_inferred_taxonomy_rejection_rule_digest"
        ),
        "inferred_taxonomy_rejection_observed_count": summary_by_field.get(
            f"{prefix}_inferred_taxonomy_rejection_observed_count"
        ),
        "inferred_taxonomy_rejection_cap": summary_by_field.get(
            f"{prefix}_inferred_taxonomy_rejection_cap"
        ),
    }


def _direct_candidate_from_summary(
    summary_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    is_prefix_eligible = _is_summary_field_enabled(
        summary_by_field,
        "direct_layout_complete_prefix_eligible",
    )
    is_taxonomy_eligible = _is_summary_field_enabled(
        summary_by_field,
        "direct_inferred_taxonomy_eligible",
    )
    return {
        "eligible": is_prefix_eligible and is_taxonomy_eligible,
        "complete_prefix_eligible": is_prefix_eligible,
        "complete_prefix_projection_encoded_bytes": summary_by_field.get(
            "direct_complete_prefix_projection_encoded_bytes"
        ),
        **_summary_candidate_costs(summary_by_field, prefix="direct"),
        **_summary_taxonomy_fields(
            summary_by_field,
            prefix="direct",
            is_eligible=is_taxonomy_eligible,
        ),
    }


def _pattern_candidate_from_summary(
    summary_by_field: Mapping[str, Any],
    observe_by_name: Mapping[str, Any],
) -> dict[str, Any]:
    is_sparse_eligible = _is_summary_field_enabled(
        summary_by_field,
        "pattern_layout_sparse_prefix_eligible",
    )
    is_degree_eligible = _is_summary_field_enabled(
        summary_by_field,
        "pattern_layout_serving_degree_eligible",
    )
    is_taxonomy_eligible = _is_summary_field_enabled(
        summary_by_field,
        "pattern_inferred_taxonomy_eligible",
    )
    unsafe_component_count = _strict_nonnegative_int(
        observe_by_name.get("unsafe_pattern_component_set_count"),
        label="adaptive layout unsafe component count",
    )
    return {
        "eligible": is_degree_eligible and is_taxonomy_eligible,
        "component_fallback_eligible": unsafe_component_count == 0,
        "unsafe_component_set_count": unsafe_component_count,
        "sparse_prefix_eligible": is_sparse_eligible,
        "sparse_prefix_owner_count": summary_by_field.get(
            "pattern_sparse_prefix_owner_count"
        ),
        "sparse_prefix_member_count": summary_by_field.get(
            "pattern_sparse_prefix_member_count"
        ),
        "sparse_prefix_raw_bytes": summary_by_field.get(
            "pattern_sparse_prefix_raw_bytes"
        ),
        "sparse_prefix_projection_encoded_bytes": summary_by_field.get(
            "pattern_sparse_prefix_projection_encoded_bytes"
        ),
        **_summary_candidate_costs(summary_by_field, prefix="pattern"),
        **_summary_taxonomy_fields(
            summary_by_field,
            prefix="pattern",
            is_eligible=is_taxonomy_eligible,
        ),
    }


def _adaptive_options_from_summary(
    summary_by_field: Mapping[str, Any],
    resource_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        **{
            option_name: summary_by_field.get(option_name)
            for option_name in PTG2_V4_GRAPH_ENCODING_OPTION_NAMES
        },
        "max_estimated_model_bytes": resource_by_field.get("max_estimated_model_bytes"),
        "max_factor_edges": resource_by_field.get("max_factor_edges"),
    }


def adaptive_layout_decision(
    summary_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Build canonical sealed decision evidence from compiler geometry."""

    observe_by_name = summary_by_field.get("observe")
    if not isinstance(observe_by_name, Mapping):
        raise RuntimeError("V4 adaptive layout compiler diagnostics are invalid")
    resource_by_field = summary_by_field.get("resource_admission")
    if not isinstance(resource_by_field, Mapping):
        raise RuntimeError("V4 adaptive layout resource limits are invalid")
    selected_layout = summary_by_field.get("selected_layout")
    if selected_layout not in {"direct", "pattern"}:
        raise RuntimeError("V4 adaptive layout compiler selection is invalid")
    evidence_by_field = {
        "contract": PTG2_V4_ADAPTIVE_LAYOUT_DECISION_CONTRACT,
        "cost_contract": PTG2_V4_ADAPTIVE_LAYOUT_COST_CONTRACT,
        "selection_policy": PTG2_V4_ADAPTIVE_LAYOUT_SELECTION_POLICY,
        "compiler_options": _adaptive_options_from_summary(
            summary_by_field,
            resource_by_field,
        ),
        "selected_representation": (
            "pattern_v1" if selected_layout == "pattern" else "direct_v1"
        ),
        "selected_encoded_bytes": summary_by_field.get("selected_encoded_bytes"),
        "direct": _direct_candidate_from_summary(summary_by_field),
        "pattern": _pattern_candidate_from_summary(
            summary_by_field,
            observe_by_name,
        ),
    }
    evidence_by_field["decision_digest"] = _adaptive_layout_evidence_digest(
        evidence_by_field
    )
    return validate_v4_adaptive_layout_decision(evidence_by_field)


v4_adaptive_layout_decision_from_summary = adaptive_layout_decision


def _observe_counter(observe: Mapping[str, Any], name: str) -> int:
    return _strict_nonnegative_int(observe.get(name), label=f"observe.{name}")


@dataclass(frozen=True)
class _PrefixDiagnosticContext:
    provider_set_count: int
    simulated_set_count: int
    override_owner_count: int
    groups_to_target_percentiles: tuple[int, ...]
    source_maxima_by_dimension: Mapping[str, int]
    source_limits_by_dimension: Mapping[str, int]
    group_npi_maxima_by_dimension: Mapping[str, int]
    group_npi_limits_by_dimension: Mapping[str, int]


@dataclass(frozen=True)
class _PrefixCounts:
    provider_sets: int
    group_unsafe_sets: int
    physical_unsafe_sets: int
    simulated_sets: int
    override_owners: int
    override_members: int
    override_raw_bytes: int
    group_merge_visits: int


def _prefix_counts(observe: Mapping[str, Any]) -> _PrefixCounts:
    """Read exact prefix-simulation aggregate counters."""

    return _PrefixCounts(
        provider_sets=_observe_counter(observe, "provider_set_count"),
        group_unsafe_sets=_observe_counter(
            observe,
            "npi_prefix_group_unsafe_set_count",
        ),
        physical_unsafe_sets=_observe_counter(
            observe,
            "npi_prefix_physical_unsafe_set_count",
        ),
        simulated_sets=_observe_counter(
            observe,
            "npi_prefix_simulated_set_count",
        ),
        override_owners=_observe_counter(
            observe,
            "npi_prefix_override_owner_count",
        ),
        override_members=_observe_counter(
            observe,
            "npi_prefix_override_member_count",
        ),
        override_raw_bytes=_observe_counter(
            observe,
            "npi_prefix_override_raw_bytes",
        ),
        group_merge_visits=_observe_counter(
            observe,
            "npi_prefix_group_merge_member_visits",
        ),
    )


def _prefix_source_work(
    observe: Mapping[str, Any],
    options: Mapping[str, int],
) -> tuple[dict[str, int], dict[str, int], bool]:
    source_maxima_by_dimension = {
        dimension: _observe_counter(observe, f"maximum_online_source_{dimension}_work")
        for dimension in ("owner", "member", "page", "byte")
    }
    source_limits_by_dimension = {
        "owner": int(options["max_online_source_owners_per_set"]),
        "member": int(options["max_online_source_members_per_set"]),
        "page": int(options["max_online_source_pages_per_set"]),
        "byte": int(options["max_online_source_bytes_per_set"]),
    }
    has_physical_overflow = any(
        source_maxima_by_dimension[dimension] > source_limits_by_dimension[dimension]
        for dimension in source_limits_by_dimension
    )
    return (
        source_maxima_by_dimension,
        source_limits_by_dimension,
        has_physical_overflow,
    )


def _prefix_group_npi_work(
    observe: Mapping[str, Any],
    options: Mapping[str, int],
) -> tuple[dict[str, int], dict[str, int], bool]:
    dimensions = ("member", "locator_page", "member_page", "byte", "batch")
    group_npi_maxima_by_dimension = {
        dimension: _observe_counter(
            observe,
            f"maximum_online_group_npi_{dimension}_work",
        )
        for dimension in dimensions
    }
    option_name_by_dimension = {
        "member": "max_online_group_npi_members_per_set",
        "locator_page": "max_online_group_npi_locator_pages_per_set",
        "member_page": "max_online_group_npi_member_pages_per_set",
        "byte": "max_online_group_npi_bytes_per_set",
        "batch": "max_online_group_npi_batches_per_set",
    }
    group_npi_limits_by_dimension = {
        dimension: int(options[option_name_by_dimension[dimension]])
        for dimension in dimensions
    }
    has_physical_overflow = any(
        group_npi_maxima_by_dimension[dimension]
        > group_npi_limits_by_dimension[dimension]
        for dimension in dimensions
    )
    return (
        group_npi_maxima_by_dimension,
        group_npi_limits_by_dimension,
        has_physical_overflow,
    )


def _has_inconsistent_prefix_totals(
    counts: _PrefixCounts,
    options: Mapping[str, int],
    percentiles: tuple[int, ...],
    has_physical_overflow: bool,
    complete_direct_coverage: bool,
) -> bool:
    """Return whether aggregate simulation, override, or cap evidence drifts."""

    unsafe_union_lower_bound = max(
        counts.group_unsafe_sets,
        counts.physical_unsafe_sets,
    )
    unsafe_union_upper_bound = counts.group_unsafe_sets + counts.physical_unsafe_sets
    return (
        unsafe_union_lower_bound > counts.simulated_sets
        or (complete_direct_coverage and counts.override_owners != counts.provider_sets)
        or (
            not complete_direct_coverage
            and (
                counts.override_owners < unsafe_union_lower_bound
                or counts.override_owners > unsafe_union_upper_bound
                or counts.override_owners
                > int(options["max_npi_prefix_override_owners"])
                or counts.override_raw_bytes
                > int(options["max_npi_prefix_override_bytes"])
            )
        )
        or counts.simulated_sets != counts.provider_sets
        or counts.override_members
        > counts.override_owners * int(options["npi_prefix_target"])
        or counts.override_raw_bytes != counts.override_members * 4
        or list(percentiles) != sorted(percentiles)
        or (
            counts.simulated_sets == 0
            and (counts.group_merge_visits or any(percentiles))
        )
        or has_physical_overflow != bool(counts.physical_unsafe_sets)
    )


def _validate_prefix_totals(
    observe: Mapping[str, Any],
    options: Mapping[str, int],
    *,
    complete_direct_coverage: bool,
) -> _PrefixDiagnosticContext:
    """Validate aggregate prefix simulations and return their work contract."""

    counts = _prefix_counts(observe)
    percentiles = tuple(
        _observe_counter(observe, f"npi_prefix_groups_to_target_{suffix}")
        for suffix in ("p50", "p95", "p99", "max")
    )
    (
        source_maxima_by_dimension,
        source_limits_by_dimension,
        has_source_overflow,
    ) = _prefix_source_work(observe, options)
    (
        group_npi_maxima_by_dimension,
        group_npi_limits_by_dimension,
        has_group_npi_overflow,
    ) = _prefix_group_npi_work(observe, options)
    has_physical_overflow = has_source_overflow or has_group_npi_overflow
    if _has_inconsistent_prefix_totals(
        counts,
        options,
        percentiles,
        has_physical_overflow,
        complete_direct_coverage,
    ):
        raise RuntimeError("V4 graph NPI-prefix diagnostics are inconsistent")
    return _PrefixDiagnosticContext(
        provider_set_count=counts.provider_sets,
        simulated_set_count=counts.simulated_sets,
        override_owner_count=counts.override_owners,
        groups_to_target_percentiles=percentiles,
        source_maxima_by_dimension=source_maxima_by_dimension,
        source_limits_by_dimension=source_limits_by_dimension,
        group_npi_maxima_by_dimension=group_npi_maxima_by_dimension,
        group_npi_limits_by_dimension=group_npi_limits_by_dimension,
    )


def _prefix_work_by_dimension(
    observe: Mapping[str, Any],
    field_prefix: str,
) -> dict[str, int]:
    return {
        dimension: _observe_counter(observe, f"{field_prefix}_{dimension}_work")
        for dimension in ("owner", "member", "page", "byte")
    }


def _prefix_group_npi_work_by_dimension(
    observe: Mapping[str, Any],
    field_prefix: str,
) -> dict[str, int]:
    return {
        dimension: _observe_counter(
            observe,
            f"{field_prefix}_{dimension}_work",
        )
        for dimension in (
            "member",
            "locator_page",
            "member_page",
            "byte",
            "batch",
        )
    }


def _has_prefix_owner_values(
    owner_key: Any,
    member_digest: Any,
    mode_flags: tuple[bool, ...],
    counters: tuple[int, ...],
    source_work_by_dimension: Mapping[str, int],
    group_npi_work_by_dimension: Mapping[str, int],
) -> bool:
    """Return whether an optional canary-owner record contains any evidence."""

    return (
        owner_key is not None
        or member_digest is not None
        or any(mode_flags)
        or any(counters)
        or any(source_work_by_dimension.values())
        or any(group_npi_work_by_dimension.values())
    )


def _has_work_overflow(
    work_by_dimension: Mapping[str, int],
    limit_by_dimension: Mapping[str, int],
) -> bool:
    """Return whether any physical-work dimension exceeds its reference."""

    return any(
        work_by_dimension[dimension] > limit_by_dimension[dimension]
        for dimension in limit_by_dimension
    )


@dataclass(frozen=True)
class _WorstPrefixOwner:
    key: Any
    groups_to_target: int
    uses_override: Any
    uses_component: Any
    member_count: int
    member_digest: Any
    source_work: Mapping[str, int]
    group_npi_work: Mapping[str, int]


def _worst_prefix_owner(
    observe: Mapping[str, Any],
) -> _WorstPrefixOwner:
    """Read the highest-risk simulated-owner evidence."""

    return _WorstPrefixOwner(
        key=observe.get("npi_prefix_worst_provider_set_key"),
        groups_to_target=_observe_counter(
            observe,
            "npi_prefix_worst_groups_to_target",
        ),
        uses_override=observe.get("npi_prefix_worst_provider_set_uses_override"),
        uses_component=observe.get("npi_prefix_worst_uses_component_fallback"),
        member_count=_observe_counter(
            observe,
            "npi_prefix_worst_member_count",
        ),
        member_digest=observe.get("npi_prefix_worst_member_digest"),
        source_work=_prefix_work_by_dimension(
            observe,
            "npi_prefix_worst_source",
        ),
        group_npi_work=_prefix_group_npi_work_by_dimension(
            observe,
            "npi_prefix_worst_group_npi",
        ),
    )


def _validate_worst_prefix_owner(
    observe: Mapping[str, Any],
    options: Mapping[str, int],
    context: _PrefixDiagnosticContext,
) -> tuple[int | None, bool]:
    """Validate the highest-risk simulated owner, including override work."""

    worst_owner = _worst_prefix_owner(observe)
    if not isinstance(worst_owner.uses_override, bool) or not isinstance(
        worst_owner.uses_component, bool
    ):
        raise RuntimeError("V4 graph worst provider-set mode is invalid")
    if context.simulated_set_count == 0:
        if _has_prefix_owner_values(
            worst_owner.key,
            worst_owner.member_digest,
            (worst_owner.uses_override, worst_owner.uses_component),
            (worst_owner.groups_to_target, worst_owner.member_count),
            worst_owner.source_work,
            worst_owner.group_npi_work,
        ):
            raise RuntimeError(
                "V4 graph empty worst-owner diagnostics are inconsistent"
            )
    elif isinstance(worst_owner.key, bool) or not isinstance(
        worst_owner.key,
        int,
    ):
        raise RuntimeError("V4 graph worst provider-set key is invalid")
    elif (
        # Rust ranks this canary across the full online-owner risk tuple. Group
        # count is only one dimension, so the winner may be below the
        # population maximum while still being the highest-risk owner.
        worst_owner.groups_to_target > context.groups_to_target_percentiles[-1]
        or worst_owner.member_count > int(options["npi_prefix_target"])
        or not isinstance(worst_owner.member_digest, str)
        or _has_work_overflow(
            worst_owner.source_work,
            context.source_maxima_by_dimension,
        )
        or _has_work_overflow(
            worst_owner.group_npi_work,
            context.group_npi_maxima_by_dimension,
        )
        or (worst_owner.uses_override and context.override_owner_count == 0)
    ):
        raise RuntimeError("V4 graph worst-owner diagnostics are inconsistent")
    return worst_owner.key, worst_owner.uses_override


@dataclass(frozen=True)
class _OnlinePrefixOwner:
    key: Any
    groups_to_target: int
    is_exact: Any
    uses_component: Any
    member_count: int
    member_digest: Any
    group_work_bound: int
    source_work: Mapping[str, int]
    group_npi_work: Mapping[str, int]


def _online_prefix_owner(
    observe: Mapping[str, Any],
) -> _OnlinePrefixOwner:
    """Read the highest-risk ordinary online-owner evidence."""

    return _OnlinePrefixOwner(
        key=observe.get("npi_prefix_worst_online_provider_set_key"),
        groups_to_target=_observe_counter(
            observe,
            "npi_prefix_worst_online_groups_to_target",
        ),
        is_exact=observe.get("npi_prefix_worst_online_groups_to_target_exact"),
        uses_component=observe.get("npi_prefix_worst_online_uses_component_fallback"),
        member_count=_observe_counter(
            observe,
            "npi_prefix_worst_online_member_count",
        ),
        member_digest=observe.get("npi_prefix_worst_online_member_digest"),
        group_work_bound=_observe_counter(
            observe,
            "npi_prefix_worst_online_group_work_bound",
        ),
        source_work=_prefix_work_by_dimension(
            observe,
            "npi_prefix_worst_online_source",
        ),
        group_npi_work=_prefix_group_npi_work_by_dimension(
            observe,
            "npi_prefix_worst_online_group_npi",
        ),
    )


def _validate_online_prefix_owner(
    observe: Mapping[str, Any],
    options: Mapping[str, int],
    context: _PrefixDiagnosticContext,
) -> int | None:
    """Validate the highest-risk owner that remains on the online graph path."""

    online_owner = _online_prefix_owner(observe)
    has_online_owner = context.override_owner_count < context.provider_set_count
    if not isinstance(online_owner.is_exact, bool) or not isinstance(
        online_owner.uses_component, bool
    ):
        raise RuntimeError("V4 graph worst-online exactness is invalid")
    if not has_online_owner:
        if _has_prefix_owner_values(
            online_owner.key,
            online_owner.member_digest,
            (online_owner.is_exact, online_owner.uses_component),
            (
                online_owner.groups_to_target,
                online_owner.member_count,
                online_owner.group_work_bound,
            ),
            online_owner.source_work,
            online_owner.group_npi_work,
        ):
            raise RuntimeError(
                "V4 graph empty worst-online diagnostics are inconsistent"
            )
    elif (
        isinstance(online_owner.key, bool)
        or not isinstance(online_owner.key, int)
        or online_owner.groups_to_target > online_owner.group_work_bound
        or online_owner.member_count > int(options["npi_prefix_target"])
        or not isinstance(online_owner.member_digest, str)
        or online_owner.group_work_bound > int(options["max_online_group_keys_per_set"])
        or _has_work_overflow(
            online_owner.source_work,
            context.source_limits_by_dimension,
        )
        or _has_work_overflow(
            online_owner.group_npi_work,
            context.group_npi_limits_by_dimension,
        )
    ):
        raise RuntimeError("V4 graph worst-online diagnostics are inconsistent")
    return online_owner.key


def _validate_prefix_diagnostics(
    observe: Mapping[str, Any],
    options: Mapping[str, int],
    *,
    complete_direct_coverage: bool,
) -> tuple[int | None, int | None, bool]:
    """Cross-check bounded prefix totals and both recorded canary owners."""

    context = _validate_prefix_totals(
        observe,
        options,
        complete_direct_coverage=complete_direct_coverage,
    )
    worst_key, worst_uses_override = _validate_worst_prefix_owner(
        observe,
        options,
        context,
    )
    online_key = _validate_online_prefix_owner(observe, options, context)
    return worst_key, online_key, worst_uses_override


def _validate_npi_pattern_diagnostics(observe: Mapping[str, Any]) -> None:
    percentiles = [
        _observe_counter(observe, f"npi_patterns_per_npi_{suffix}")
        for suffix in ("p50", "p95", "p99")
    ]
    maximum = _observe_counter(observe, "maximum_patterns_per_npi")
    actual_pattern_visits = _observe_counter(observe, "group_set_expansion_edge_visits")
    logical_pattern_visits = _observe_counter(observe, "group_set_incidence_count")
    if (
        percentiles != sorted(percentiles)
        or (percentiles and percentiles[-1] > maximum)
        or actual_pattern_visits > logical_pattern_visits
    ):
        raise RuntimeError("V4 graph pattern-memo diagnostics are inconsistent")


def _positive_env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(
            f"V4 graph compiler environment {name} is not an integer"
        ) from exc
    if value <= 0 or value > 2**63 - 1:
        raise RuntimeError(f"V4 graph compiler environment {name} is out of range")
    return value


def _physical_option_defaults() -> dict[str, int]:
    """Read physical graph-encoding defaults from the worker environment."""

    return {
        "member_page_bytes": _positive_env_int(
            PTG2_V4_GRAPH_MEMBER_PAGE_BYTES_ENV, 16 * 1024
        ),
        "locator_page_bytes": _positive_env_int(
            PTG2_V4_GRAPH_LOCATOR_PAGE_BYTES_ENV, 16 * 1024
        ),
        "heavy_owner_member_threshold": _positive_env_int(
            PTG2_V4_GRAPH_HEAVY_OWNER_THRESHOLD_ENV, 4096
        ),
        "heavy_bitmap_minimum_savings_bytes": _positive_env_int(
            PTG2_V4_GRAPH_HEAVY_MIN_SAVINGS_ENV, 512
        ),
        "max_set_patterns_per_set": _positive_env_int(
            PTG2_V4_GRAPH_MAX_SET_PATTERNS_ENV, 1024
        ),
        "max_set_components_per_fallback_set": _positive_env_int(
            PTG2_V4_GRAPH_MAX_SET_COMPONENTS_ENV, 4096
        ),
    }


def _hot_prefix_option_defaults() -> dict[str, int]:
    """Read sealed online traversal limits from the worker environment."""

    return {
        "max_online_group_keys_per_set": _positive_env_int(
            PTG2_V4_GRAPH_MAX_ONLINE_GROUP_KEYS_ENV, 4096
        ),
        "max_online_source_owners_per_set": _positive_env_int(
            PTG2_V4_GRAPH_MAX_ONLINE_SOURCE_OWNERS_ENV, 4096
        ),
        "max_online_source_members_per_set": _positive_env_int(
            PTG2_V4_GRAPH_MAX_ONLINE_SOURCE_MEMBERS_ENV, 16_384
        ),
        "max_online_source_pages_per_set": _positive_env_int(
            PTG2_V4_GRAPH_MAX_ONLINE_SOURCE_PAGES_ENV, 64
        ),
        "max_online_source_bytes_per_set": _positive_env_int(
            PTG2_V4_GRAPH_MAX_ONLINE_SOURCE_BYTES_ENV, 1024 * 1024
        ),
        "online_group_npi_batch_size": _positive_env_int(
            PTG2_V4_GRAPH_ONLINE_GROUP_NPI_BATCH_SIZE_ENV, 32
        ),
        "max_online_group_npi_members_per_set": _positive_env_int(
            PTG2_V4_GRAPH_MAX_ONLINE_GROUP_NPI_MEMBERS_ENV, 32_768
        ),
        "max_online_group_npi_locator_pages_per_set": _positive_env_int(
            PTG2_V4_GRAPH_MAX_ONLINE_GROUP_NPI_LOCATOR_PAGES_ENV, 16
        ),
        "max_online_group_npi_member_pages_per_set": _positive_env_int(
            PTG2_V4_GRAPH_MAX_ONLINE_GROUP_NPI_MEMBER_PAGES_ENV, 128
        ),
        "max_online_group_npi_bytes_per_set": _positive_env_int(
            PTG2_V4_GRAPH_MAX_ONLINE_GROUP_NPI_BYTES_ENV, 4 * 1024 * 1024
        ),
        "max_online_group_npi_batches_per_set": _positive_env_int(
            PTG2_V4_GRAPH_MAX_ONLINE_GROUP_NPI_BATCHES_ENV, 4
        ),
        "npi_prefix_target": _positive_env_int(
            PTG2_V4_GRAPH_NPI_PREFIX_TARGET_ENV, 201
        ),
        "max_npi_prefix_override_owners": _positive_env_int(
            PTG2_V4_GRAPH_MAX_NPI_PREFIX_OVERRIDE_OWNERS_ENV, 250_000
        ),
        "max_npi_prefix_override_bytes": _positive_env_int(
            PTG2_V4_GRAPH_MAX_NPI_PREFIX_OVERRIDE_BYTES_ENV,
            256 * 1024 * 1024,
        ),
    }


def _provider_expansion_option_defaults() -> dict[str, int]:
    """Read sealed incremental provider-expansion limits."""

    return {
        "provider_expansion_rate_page_rows": _positive_env_int(
            PTG2_V4_GRAPH_PROVIDER_EXPANSION_RATE_PAGE_ROWS_ENV, 64
        ),
        "max_online_provider_expansion_rate_rows": _positive_env_int(
            PTG2_V4_GRAPH_MAX_ONLINE_PROVIDER_EXPANSION_RATE_ROWS_ENV, 256
        ),
        "max_online_provider_expansion_provider_sets": _positive_env_int(
            PTG2_V4_GRAPH_MAX_ONLINE_PROVIDER_EXPANSION_PROVIDER_SETS_ENV, 64
        ),
        "max_online_provider_expansion_graph_batches": _positive_env_int(
            PTG2_V4_GRAPH_MAX_ONLINE_PROVIDER_EXPANSION_GRAPH_BATCHES_ENV, 64
        ),
    }


def _inferred_taxonomy_option_defaults() -> dict[str, int]:
    """Return source-independent taxonomy projection limits."""

    return {
        "max_online_inferred_taxonomy_candidates": (
            PTG2_V4_GRAPH_DEFAULT_MAX_INFERRED_TAXONOMY_CANDIDATES
        ),
        "max_online_candidate_pattern_projection_members": (
            PTG2_V4_GRAPH_DEFAULT_MAX_CANDIDATE_PATTERN_MEMBERS
        ),
    }


def _resource_admission_option_defaults() -> dict[str, int]:
    """Read build-time memory and factor-edge admission limits."""

    return {
        "max_estimated_model_bytes": _positive_env_int(
            PTG2_V4_GRAPH_MAX_MODEL_BYTES_ENV,
            PTG2_V4_GRAPH_DEFAULT_MAX_MODEL_BYTES,
        ),
        "max_factor_edges": _positive_env_int(
            PTG2_V4_GRAPH_MAX_FACTOR_EDGES_ENV,
            PTG2_V4_GRAPH_DEFAULT_MAX_FACTOR_EDGES,
        ),
    }


def _effective_compiler_options(
    options: Mapping[str, int] | None,
) -> dict[str, int]:
    """Merge explicit compiler options over all environment-derived defaults."""

    options_by_name = dict(options or {})
    defaults_by_name = {
        **_physical_option_defaults(),
        **_hot_prefix_option_defaults(),
        **_provider_expansion_option_defaults(),
        **_inferred_taxonomy_option_defaults(),
        **_resource_admission_option_defaults(),
    }
    for name, default_value in defaults_by_name.items():
        options_by_name.setdefault(name, default_value)
    return options_by_name


def v4_graph_encoding_policy(
    options: Mapping[str, int] | None = None,
) -> dict[str, int]:
    """Return only output-affecting V4 compiler options for layout identity."""

    effective = _effective_compiler_options(options)
    return {name: int(effective[name]) for name in PTG2_V4_GRAPH_ENCODING_OPTION_NAMES}


def _strict_sha256(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or value != value.lower():
        raise RuntimeError(f"V4 graph compiler has invalid {label}")
    try:
        digest = bytes.fromhex(value)
    except ValueError as exc:
        raise RuntimeError(f"V4 graph compiler has invalid {label}") from exc
    if len(digest) != 32:
        raise RuntimeError(f"V4 graph compiler has invalid {label}")
    return value


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            hasher.update(chunk)
    return hasher.hexdigest()


def _regular_file_without_symlink(
    raw_path: str | Path,
    *,
    label: str,
) -> tuple[Path, os.stat_result]:
    """Resolve one existing regular file only after rejecting a link leaf."""

    unresolved = Path(raw_path).expanduser()
    try:
        metadata = unresolved.lstat()
    except OSError as exc:
        raise RuntimeError(f"V4 {label} is unavailable") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise RuntimeError(f"V4 {label} is not a regular file")
    try:
        return unresolved.resolve(strict=True), metadata
    except OSError as exc:
        raise RuntimeError(f"V4 {label} is unavailable") from exc


def _private_scratch_child(
    raw_path: str | Path,
    *,
    label: str,
) -> tuple[Path, bool]:
    """Resolve one compiler scratch child beneath a private owned directory."""

    unresolved = Path(raw_path).expanduser().absolute()
    try:
        parent_metadata = unresolved.parent.lstat()
    except OSError as exc:
        raise RuntimeError(f"V4 {label} parent is unavailable") from exc
    if (
        stat.S_ISLNK(parent_metadata.st_mode)
        or not stat.S_ISDIR(parent_metadata.st_mode)
        or parent_metadata.st_uid != os.getuid()
        or stat.S_IMODE(parent_metadata.st_mode) & 0o077
    ):
        raise RuntimeError(f"V4 {label} parent is not private")
    parent = unresolved.parent.resolve(strict=True)
    path = parent / unresolved.name
    if not os.path.lexists(unresolved):
        return path, False
    try:
        path_metadata = unresolved.lstat()
    except OSError as exc:
        raise RuntimeError(f"V4 {label} changed") from exc
    if stat.S_ISLNK(path_metadata.st_mode):
        raise RuntimeError(f"V4 {label} is a symbolic link")
    return path, True


def _compiler_output_owner_path(output: Path) -> Path:
    return output / PTG2_V4_GRAPH_SCRATCH_OWNER_NAME


def _validate_compiler_output_owner(output: Path) -> None:
    owner, _metadata = _regular_file_without_symlink(
        _compiler_output_owner_path(output),
        label="graph compiler scratch owner",
    )
    if (
        owner.parent != output
        or owner.read_bytes() != PTG2_V4_GRAPH_SCRATCH_OWNER_BYTES
    ):
        raise RuntimeError("V4 graph compiler scratch ownership changed")


def _create_compiler_output(output: Path) -> None:
    output.mkdir(mode=0o700)
    owner = _compiler_output_owner_path(output)
    try:
        with owner.open("xb") as owner_file:
            owner_file.write(PTG2_V4_GRAPH_SCRATCH_OWNER_BYTES)
            owner_file.flush()
            os.fsync(owner_file.fileno())
    except BaseException:
        shutil.rmtree(output, ignore_errors=True)
        raise


def _artifact_manifest(raw_entry: Mapping[str, Any]) -> tuple[dict[str, Any], int]:
    path_text = raw_entry.get("path")
    if not isinstance(path_text, str) or not path_text.strip():
        raise RuntimeError("V4 graph factor artifact lacks a path")
    path, path_metadata = _regular_file_without_symlink(
        path_text,
        label="graph factor artifact",
    )
    record_format = raw_entry.get("record_format")
    if not isinstance(record_format, str) or not record_format:
        raise RuntimeError("V4 graph factor artifact has invalid record_format")
    sha256 = _strict_sha256(raw_entry.get("sha256"), label="input sha256")
    byte_count = _strict_nonnegative_int(
        raw_entry.get("byte_count"), label="input byte_count"
    )
    owner_count = _strict_nonnegative_int(
        raw_entry.get("owner_count"), label="input owner_count"
    )
    member_count = _strict_nonnegative_int(
        raw_entry.get("member_count"), label="input member_count"
    )
    if byte_count <= 0 or path_metadata.st_size != byte_count:
        raise RuntimeError(f"V4 graph factor byte count changed: {path}")
    metadata: dict[str, Any] = {
        "record_format": record_format,
        "sha256": sha256,
        "byte_count": byte_count,
        "owner_count": owner_count,
        "member_count": member_count,
    }
    member_global_count = raw_entry.get("member_global_count")
    if member_global_count is not None:
        metadata["member_global_count"] = _strict_nonnegative_int(
            member_global_count, label="input member_global_count"
        )
    for name in ("name", "source_shard_id", "shard_id"):
        metadata_value = raw_entry.get(name)
        if metadata_value is not None:
            if not isinstance(metadata_value, str):
                raise RuntimeError(f"V4 graph factor artifact has invalid {name}")
            metadata[name] = metadata_value
    return {"path": str(path), "metadata": metadata}, byte_count


def _length_prefixed_digest_field(digest: Any, value: bytes) -> None:
    digest.update(len(value).to_bytes(4, "big"))
    digest.update(value)


def _npi_scope_binding_digest(metadata: Mapping[str, Any]) -> str:
    digest = hashlib.sha256()
    digest.update(PTG2_V4_NPI_SCOPE_BINDING_HASH_DOMAIN)
    _length_prefixed_digest_field(
        digest,
        str(metadata["record_format"]).encode("ascii"),
    )
    digest.update(bytes.fromhex(str(metadata["sha256"])))
    for field_name in ("byte_count", "row_count"):
        digest.update(int(metadata[field_name]).to_bytes(8, "big"))
    digest.update(bytes.fromhex(str(metadata["provider_npi_group_sha256"])))
    _length_prefixed_digest_field(
        digest,
        str(metadata["provider_npi_group_record_format"]).encode("ascii"),
    )
    for field_name in (
        "provider_npi_group_byte_count",
        "provider_npi_group_owner_count",
        "provider_npi_group_member_count",
        "provider_npi_group_member_global_count",
    ):
        digest.update(int(metadata[field_name]).to_bytes(8, "big"))
    return digest.hexdigest()


def _npi_scope_shard_binding_digest(
    binding_sha256: str,
    shard_id: str,
) -> str:
    digest = hashlib.sha256()
    digest.update(PTG2_V4_NPI_SCOPE_SHARD_BINDING_HASH_DOMAIN)
    digest.update(bytes.fromhex(binding_sha256))
    _length_prefixed_digest_field(digest, shard_id.encode("utf-8"))
    return digest.hexdigest()


def _npi_scope_metadata(
    raw_entry: Mapping[str, Any],
    *,
    shard_id: str,
) -> dict[str, Any]:
    integer_label_by_field = {
        "byte_count": "NPI scope byte_count",
        "row_count": "NPI scope row_count",
        "provider_npi_group_byte_count": "NPI scope reciprocal byte_count",
        "provider_npi_group_owner_count": "NPI scope reciprocal owner_count",
        "provider_npi_group_member_count": "NPI scope reciprocal member_count",
        "provider_npi_group_member_global_count": (
            "NPI scope reciprocal member_global_count"
        ),
    }
    scope_by_field = {
        field_name: _strict_nonnegative_int(
            raw_entry.get(field_name),
            label=field_label,
        )
        for field_name, field_label in integer_label_by_field.items()
    }
    scope_by_field.update(
        {
            "record_format": raw_entry.get("record_format"),
            "sha256": _strict_sha256(
                raw_entry.get("sha256"),
                label="NPI scope sha256",
            ),
            "provider_npi_group_sha256": _strict_sha256(
                raw_entry.get("provider_npi_group_sha256"),
                label="NPI scope reciprocal sha256",
            ),
            "provider_npi_group_record_format": raw_entry.get(
                "provider_npi_group_record_format"
            ),
            "binding_contract": raw_entry.get("binding_contract"),
            "binding_sha256": _strict_sha256(
                raw_entry.get("binding_sha256"),
                label="NPI scope binding sha256",
            ),
            "shard_binding_contract": raw_entry.get("shard_binding_contract"),
            "shard_binding_sha256": _strict_sha256(
                raw_entry.get("shard_binding_sha256"),
                label="NPI scope shard binding sha256",
            ),
            "retention_contract": raw_entry.get("retention_contract"),
            "name": "provider_npi_scope",
            "source_shard_id": shard_id,
        }
    )
    return scope_by_field


def _is_valid_npi_scope_binding(
    scope_by_field: Mapping[str, Any],
    reciprocal_by_field: Mapping[str, Any],
    *,
    path: Path,
    path_metadata: os.stat_result,
    shard_id: str,
) -> bool:
    expected_bytes = len(_PG_COPY_HEADER) + int(scope_by_field["row_count"]) * 14 + 2
    return (
        scope_by_field["record_format"] == PTG2_V4_NPI_SCOPE_ARTIFACT_FORMAT
        and scope_by_field["binding_contract"] == PTG2_V4_NPI_SCOPE_BINDING_CONTRACT
        and scope_by_field["shard_binding_contract"]
        == PTG2_V4_NPI_SCOPE_SHARD_BINDING_CONTRACT
        and scope_by_field["retention_contract"] == PTG2_V4_NPI_SCOPE_RETENTION_CONTRACT
        and scope_by_field["byte_count"] == expected_bytes
        and path_metadata.st_size == expected_bytes
        and _sha256_file(path) == scope_by_field["sha256"]
        and scope_by_field["row_count"] == reciprocal_by_field["owner_count"]
        and scope_by_field["provider_npi_group_sha256"] == reciprocal_by_field["sha256"]
        and scope_by_field["provider_npi_group_record_format"]
        == reciprocal_by_field["record_format"]
        and scope_by_field["provider_npi_group_byte_count"]
        == reciprocal_by_field["byte_count"]
        and scope_by_field["provider_npi_group_owner_count"]
        == reciprocal_by_field["owner_count"]
        and scope_by_field["provider_npi_group_member_count"]
        == reciprocal_by_field["member_count"]
        and scope_by_field["provider_npi_group_member_global_count"]
        == reciprocal_by_field.get("member_global_count", 0)
        and scope_by_field["binding_sha256"]
        == _npi_scope_binding_digest(scope_by_field)
        and scope_by_field["shard_binding_sha256"]
        == _npi_scope_shard_binding_digest(
            str(scope_by_field["binding_sha256"]),
            shard_id,
        )
    )


def _npi_scope_artifact_manifest(
    raw_entry: Mapping[str, Any],
    *,
    reciprocal: Mapping[str, Any],
    shard_id: str,
) -> dict[str, Any]:
    """Authenticate one scanner scope against its reciprocal owner index."""

    path_text = raw_entry.get("path")
    if not isinstance(path_text, str) or not path_text.strip():
        raise RuntimeError("V4 provider NPI scope artifact lacks a path")
    path, path_metadata = _regular_file_without_symlink(
        path_text,
        label="provider NPI scope artifact",
    )
    scope_by_field = _npi_scope_metadata(raw_entry, shard_id=shard_id)
    if not _is_valid_npi_scope_binding(
        scope_by_field,
        reciprocal["metadata"],
        path=path,
        path_metadata=path_metadata,
        shard_id=shard_id,
    ):
        raise RuntimeError(
            "V4 provider NPI scope binding does not match its reciprocal graph"
        )
    return {"path": str(path), "metadata": scope_by_field}


def _validated_tax_identity_artifact_counts(
    raw_entry: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate per-state scanner counts for one tax artifact."""

    count_by_name = {
        name: _strict_nonnegative_int(raw_entry.get(name), label=f"tax input {name}")
        for name in (
            "row_count",
            "provider_group_count",
            "matched_ein_count",
            "missing_count",
            "malformed_count",
            "unsupported_type_count",
        )
    }
    state_count = sum(
        count_by_name[name]
        for name in (
            "matched_ein_count",
            "missing_count",
            "malformed_count",
            "unsupported_type_count",
        )
    )
    if (
        count_by_name["provider_group_count"] != count_by_name["row_count"]
        or state_count != count_by_name["row_count"]
    ):
        raise RuntimeError("V4 provider tax identity counts are inconsistent")
    return count_by_name


def _validated_tax_identity_artifact_metadata(
    raw_entry: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate the authenticated metadata contract for one tax artifact."""

    record_format = raw_entry.get("record_format")
    token_policy_id = raw_entry.get("token_policy_id")
    if (
        record_format != _TAX_IDENTITY_FORMAT
        or raw_entry.get("version") != _TAX_IDENTITY_VERSION
        or raw_entry.get("record_bytes") != _TAX_IDENTITY_RECORD_BYTES
        or raw_entry.get("normalization_contract")
        != _TAX_IDENTITY_NORMALIZATION_CONTRACT
        or raw_entry.get("hmac_contract") != _TAX_IDENTITY_HMAC_CONTRACT
        or raw_entry.get("final") is not True
        or not isinstance(token_policy_id, str)
        or len(token_policy_id.encode("ascii", errors="ignore")) != len(token_policy_id)
        or len(token_policy_id.encode("ascii")) > 55
        or _TAX_POLICY_ID.fullmatch(token_policy_id) is None
    ):
        raise RuntimeError("V4 provider tax identity artifact metadata is invalid")
    digest = _strict_sha256(raw_entry.get("sha256"), label="tax input sha256")
    byte_count = _strict_nonnegative_int(
        raw_entry.get("byte_count"), label="tax input byte_count"
    )
    count_by_name = _validated_tax_identity_artifact_counts(raw_entry)
    return {
        "record_format": record_format,
        "sha256": digest,
        "byte_count": byte_count,
        **count_by_name,
        "version": _TAX_IDENTITY_VERSION,
        "record_bytes": _TAX_IDENTITY_RECORD_BYTES,
        "token_policy_id": token_policy_id,
        "normalization_contract": _TAX_IDENTITY_NORMALIZATION_CONTRACT,
        "hmac_contract": _TAX_IDENTITY_HMAC_CONTRACT,
        "final": True,
        "name": "provider_group_tax_identity",
    }


def _validate_tax_identity_artifact_file(
    path: Path,
    *,
    metadata: Mapping[str, Any],
) -> None:
    """Authenticate fixed-record bytes against validated tax metadata."""

    policy_bytes = str(metadata["token_policy_id"]).encode("ascii")
    expected_bytes = (
        13
        + len(policy_bytes)
        + (int(metadata["row_count"]) * _TAX_IDENTITY_RECORD_BYTES)
    )
    if (
        metadata["byte_count"] != expected_bytes
        or path.stat().st_size != metadata["byte_count"]
        or _sha256_file(path) != metadata["sha256"]
    ):
        raise RuntimeError("V4 provider tax identity artifact authentication failed")
    with path.open("rb") as artifact_file:
        header = artifact_file.read(13 + len(policy_bytes))
    if (
        len(header) != 13 + len(policy_bytes)
        or header[:8] != b"PTG2TAX1"
        or int.from_bytes(header[8:10], "little") != _TAX_IDENTITY_VERSION
        or int.from_bytes(header[10:12], "little") != _TAX_IDENTITY_RECORD_BYTES
        or header[12] != len(policy_bytes)
        or header[13:] != policy_bytes
    ):
        raise RuntimeError("V4 provider tax identity artifact header is invalid")


def _tax_identity_artifact_manifest(
    raw_entry: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    """Authenticate one fixed-record scanner tax-identity artifact."""

    path_text = raw_entry.get("path")
    if not isinstance(path_text, str) or not path_text.strip():
        raise RuntimeError("V4 provider tax identity artifact lacks a path")
    path, _path_metadata = _regular_file_without_symlink(
        path_text,
        label="provider tax identity artifact",
    )
    metadata = _validated_tax_identity_artifact_metadata(raw_entry)
    _validate_tax_identity_artifact_file(path, metadata=metadata)
    for name in ("source_shard_id", "shard_id"):
        metadata_value = raw_entry.get(name)
        if metadata_value is not None:
            if not isinstance(metadata_value, str):
                raise RuntimeError(
                    f"V4 provider tax identity artifact has invalid {name}"
                )
            metadata[name] = metadata_value
    return {"path": str(path), "metadata": metadata}, int(metadata["byte_count"])


def _build_v4_graph_manifest_shards(
    graph_artifact_entries: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    """Authenticate complete factor and source-scope bundles."""

    artifact_by_shard: dict[str, dict[str, Mapping[str, Any]]] = {}
    for raw_entry in graph_artifact_entries:
        if not isinstance(raw_entry, Mapping):
            continue
        artifact_name = str(
            raw_entry.get("name") or raw_entry.get("kind") or ""
        ).strip()
        field_name = _ARTIFACT_FIELD_BY_NAME.get(artifact_name)
        if field_name is None:
            continue
        shard_id = str(
            raw_entry.get("source_shard_id") or raw_entry.get("shard_id") or ""
        ).strip()
        if not shard_id:
            raise RuntimeError(f"V4 graph factor {artifact_name!r} lacks a shard ID")
        shard = artifact_by_shard.setdefault(shard_id, {})
        if field_name in shard:
            raise RuntimeError(
                f"V4 graph shard {shard_id!r} repeats factor {artifact_name!r}"
            )
        shard[field_name] = raw_entry
    if not artifact_by_shard:
        raise RuntimeError("V4 graph compilation has no factor artifacts")

    manifest_shards: list[dict[str, Any]] = []
    input_byte_count = 0
    for shard_id, fields in sorted(artifact_by_shard.items()):
        missing = sorted(_REQUIRED_SHARD_FIELDS - fields.keys())
        if missing:
            raise RuntimeError(
                f"V4 graph shard {shard_id!r} is incomplete: missing {', '.join(missing)}"
            )
        shard_manifest_by_field: dict[str, Any] = {"shard_id": shard_id}
        for field_name in sorted(_REQUIRED_SHARD_FIELDS - {_NPI_SCOPE_FIELD}):
            artifact, artifact_bytes = (
                _tax_identity_artifact_manifest(fields[field_name])
                if field_name == _TAX_IDENTITY_FIELD
                else _artifact_manifest(fields[field_name])
            )
            shard_manifest_by_field[field_name] = artifact
            input_byte_count += artifact_bytes
        shard_manifest_by_field[_NPI_SCOPE_FIELD] = _npi_scope_artifact_manifest(
            fields[_NPI_SCOPE_FIELD],
            reciprocal=shard_manifest_by_field["provider_npi_group"],
            shard_id=shard_id,
        )
        manifest_shards.append(shard_manifest_by_field)

    _tax_manifest_expectation({"shards": manifest_shards})
    return manifest_shards, input_byte_count


def _validated_npi_scope_input(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Authenticate the complete NPI-scope prepass result."""

    expected_fields = {
        "format",
        "row_count",
        "source_owner_count",
        "input_byte_count",
        "input_sha256",
        "output_byte_count",
        "output_sha256",
        "output_path",
    }
    if set(raw) != expected_fields:
        raise RuntimeError("V4 NPI scope prepass fields changed")
    path, path_metadata = _regular_file_without_symlink(
        str(raw.get("output_path") or ""),
        label="NPI scope prepass output",
    )
    scope_by_field = {
        "format": raw.get("format"),
        "row_count": _strict_nonnegative_int(
            raw.get("row_count"), label="NPI scope row_count"
        ),
        "source_owner_count": _strict_nonnegative_int(
            raw.get("source_owner_count"), label="NPI scope source_owner_count"
        ),
        "input_byte_count": _strict_nonnegative_int(
            raw.get("input_byte_count"), label="NPI scope input_byte_count"
        ),
        "input_sha256": _strict_sha256(
            raw.get("input_sha256"), label="NPI scope input sha256"
        ),
        "output_byte_count": _strict_nonnegative_int(
            raw.get("output_byte_count"), label="NPI scope output_byte_count"
        ),
        "output_sha256": _strict_sha256(
            raw.get("output_sha256"), label="NPI scope output sha256"
        ),
        "output_path": str(path),
    }
    if (
        scope_by_field["format"] != PTG2_V4_NPI_SCOPE_FORMAT
        or path_metadata.st_size != scope_by_field["output_byte_count"]
        or scope_by_field["output_byte_count"]
        != len(_PG_COPY_HEADER) + scope_by_field["row_count"] * 22 + 2
        or _sha256_file(path) != scope_by_field["output_sha256"]
    ):
        raise RuntimeError("V4 NPI scope prepass output changed")
    return scope_by_field


def _validated_taxonomy_member_artifact(
    raw: Any,
) -> tuple[Path, int, str]:
    """Authenticate the prepared taxonomy member vector."""

    if not isinstance(raw, Mapping) or set(raw) != {
        "path",
        "byte_count",
        "sha256",
    }:
        raise RuntimeError("V4 inferred-taxonomy member artifact is invalid")
    member_path, member_metadata = _regular_file_without_symlink(
        str(raw.get("path") or ""),
        label="inferred-taxonomy member artifact",
    )
    member_bytes = _strict_nonnegative_int(
        raw.get("byte_count"), label="taxonomy member byte_count"
    )
    member_sha256 = _strict_sha256(raw.get("sha256"), label="taxonomy member sha256")
    if (
        member_metadata.st_size != member_bytes
        or member_bytes % 4
        or _sha256_file(member_path) != member_sha256
    ):
        raise RuntimeError("V4 inferred-taxonomy member artifact changed")
    return member_path, member_bytes, member_sha256


def _normalize_taxonomy_rule_slice(
    raw: Any,
    *,
    expected_offset: int,
    previous_digest: str,
) -> dict[str, Any]:
    """Normalize one strictly ordered taxonomy rule slice."""

    expected_fields = {
        "rule_digest",
        "catalog_digest",
        "member_count",
        "member_offset_bytes",
        "member_byte_count",
    }
    if not isinstance(raw, Mapping) or set(raw) != expected_fields:
        raise RuntimeError("V4 inferred-taxonomy rule input changed")
    rule_digest = _strict_sha256(raw.get("rule_digest"), label="taxonomy rule digest")
    rule_by_field = {
        "rule_digest": rule_digest,
        "catalog_digest": _strict_sha256(
            raw.get("catalog_digest"),
            label="taxonomy catalog digest",
        ),
        "member_count": _strict_nonnegative_int(
            raw.get("member_count"),
            label="taxonomy member_count",
        ),
        "member_offset_bytes": _strict_nonnegative_int(
            raw.get("member_offset_bytes"),
            label="taxonomy member_offset_bytes",
        ),
        "member_byte_count": _strict_nonnegative_int(
            raw.get("member_byte_count"),
            label="taxonomy member_byte_count",
        ),
    }
    if (
        rule_digest <= previous_digest
        or rule_by_field["member_offset_bytes"] != expected_offset
        or rule_by_field["member_byte_count"] != rule_by_field["member_count"] * 4
    ):
        raise RuntimeError("V4 inferred-taxonomy rules are not strict")
    return rule_by_field


def _authenticate_taxonomy_rule_bundle(
    raw: Any,
) -> tuple[list[dict[str, Any]], int]:
    """Authenticate all rule slices and their contiguous member span."""

    if not isinstance(raw, list) or not raw:
        raise RuntimeError("V4 inferred-taxonomy rules are incomplete")
    rules: list[dict[str, Any]] = []
    expected_offset = 0
    previous_digest = ""
    for rule_raw in raw:
        rule_by_field = _normalize_taxonomy_rule_slice(
            rule_raw,
            expected_offset=expected_offset,
            previous_digest=previous_digest,
        )
        expected_offset += rule_by_field["member_byte_count"]
        previous_digest = rule_by_field["rule_digest"]
        rules.append(rule_by_field)
    return rules, expected_offset


def _validated_inferred_taxonomy_input(
    raw: Mapping[str, Any],
    *,
    npi_scope_sha256: str,
) -> dict[str, Any]:
    """Authenticate the complete inferred-taxonomy compiler input."""

    expected_fields = {
        "contract",
        "catalog_contract",
        "vector_format",
        "npi_scope_sha256",
        "rule_set_digest",
        "members",
        "rules",
    }
    if set(raw) != expected_fields:
        raise RuntimeError("V4 inferred-taxonomy input fields changed")
    member_path, member_bytes, member_sha256 = _validated_taxonomy_member_artifact(
        raw.get("members")
    )
    rules, expected_offset = _authenticate_taxonomy_rule_bundle(raw.get("rules"))
    if (
        raw.get("contract") != PTG2_V4_INFERRED_TAXONOMY_INPUT_CONTRACT
        or raw.get("catalog_contract") != PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT
        or raw.get("vector_format") != PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT
        or raw.get("npi_scope_sha256") != npi_scope_sha256
        or expected_offset != member_bytes
    ):
        raise RuntimeError("V4 inferred-taxonomy input contract changed")
    return {
        "contract": raw["contract"],
        "catalog_contract": raw["catalog_contract"],
        "vector_format": raw["vector_format"],
        "npi_scope_sha256": _strict_sha256(
            raw["npi_scope_sha256"], label="taxonomy scope sha256"
        ),
        "rule_set_digest": _strict_sha256(
            raw.get("rule_set_digest"), label="taxonomy rule-set digest"
        ),
        "members": {
            "path": str(member_path),
            "byte_count": member_bytes,
            "sha256": member_sha256,
        },
        "rules": rules,
    }


def build_v4_graph_compiler_manifest(
    *,
    graph_artifact_entries: Iterable[Mapping[str, Any]],
    provider_set_key_map_path: str | Path,
    npi_scope: Mapping[str, Any],
    inferred_taxonomy: Mapping[str, Any],
    output_directory: str | Path,
    options: Mapping[str, int] | None = None,
) -> tuple[dict[str, Any], int]:
    """Build a deterministic complete-shard manifest from scanner artifacts."""

    manifest_shards, input_byte_count = _build_v4_graph_manifest_shards(
        graph_artifact_entries
    )
    provider_map, provider_map_metadata = _regular_file_without_symlink(
        provider_set_key_map_path,
        label="authoritative provider-set map",
    )
    if provider_map_metadata.st_size <= 0:
        raise RuntimeError(
            "V4 graph compilation requires an authoritative provider-set map"
        )
    output = Path(output_directory).resolve()
    normalized_options_by_name: dict[str, int] = {}
    for name, option_value in sorted((options or {}).items()):
        if name not in set(PTG2_V4_GRAPH_ENCODING_OPTION_NAMES) | {
            "max_estimated_model_bytes",
            "max_factor_edges",
        }:
            raise RuntimeError(f"V4 graph compiler has unknown option {name!r}")
        normalized_options_by_name[name] = _strict_nonnegative_int(
            option_value, label=f"option {name}"
        )
    normalized_scope = _validated_npi_scope_input(npi_scope)
    normalized_taxonomy = _validated_inferred_taxonomy_input(
        inferred_taxonomy,
        npi_scope_sha256=str(normalized_scope["output_sha256"]),
    )
    return (
        {
            "shards": manifest_shards,
            "provider_set_key_map_path": str(provider_map),
            "npi_scope": normalized_scope,
            "inferred_taxonomy": normalized_taxonomy,
            "output_directory": str(output),
            "options": normalized_options_by_name,
        },
        input_byte_count,
    )


def _manifest_factor_counts(manifest: Mapping[str, Any]) -> tuple[int, int]:
    edges = 0
    owners = 0
    for shard in manifest["shards"]:
        for field_name in sorted(_REQUIRED_MEMBERSHIP_SHARD_FIELDS):
            metadata = shard[field_name]["metadata"]
            edges += int(metadata["member_count"])
            owners += int(metadata["owner_count"])
        tax_metadata = shard[_TAX_IDENTITY_FIELD]["metadata"]
        edges += int(tax_metadata["row_count"])
        owners += int(tax_metadata["provider_group_count"])
    return edges, owners


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def _checkpoint_binding(
    *, manifest_bytes: bytes, provider_set_key_map_path: Path
) -> tuple[str, str]:
    provider_map_sha256 = _sha256_file(provider_set_key_map_path)
    hasher = hashlib.sha256()
    hasher.update(b"PTG2V4CHECKPOINT\x01")
    hasher.update(PTG2_V4_GRAPH_SUMMARY_FORMAT.encode("ascii"))
    hasher.update(b"\0")
    hasher.update(manifest_bytes)
    hasher.update(b"\0")
    hasher.update(bytes.fromhex(provider_map_sha256))
    return hasher.hexdigest(), provider_map_sha256


def _checkpoint_payload(
    *,
    compilation: V4GraphCompilationResult,
    binding_sha256: str,
    provider_map_sha256: str,
    options: Mapping[str, int],
    input_contracts: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "format": PTG2_V4_GRAPH_CHECKPOINT_FORMAT,
        "complete": True,
        "compiler_contract": PTG2_V4_GRAPH_SUMMARY_FORMAT,
        "shared_format_version": PTG2_V4_SHARED_FORMAT_VERSION,
        "binding_sha256": binding_sha256,
        "provider_set_key_map_sha256": provider_map_sha256,
        "options": dict(sorted(options.items())),
        "npi_scope": input_contracts["npi_scope"],
        "inferred_taxonomy": input_contracts["inferred_taxonomy"],
        "summary_sha256": _sha256_file(compilation.summary_path),
        "selected_layout": compilation.selected_layout,
        "block_count": compilation.block_count,
        "selected_encoded_bytes": compilation.selected_encoded_bytes,
        "tax_identity": compilation.summary["tax_identity"],
        "output_artifacts": [
            {
                "name": artifact.name,
                "byte_count": artifact.byte_count,
                "sha256": artifact.sha256,
                "row_count": artifact.row_count,
            }
            for artifact in compilation.output_artifacts
        ],
    }


def _write_checkpoint(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("xb") as target:
        target.write(_canonical_json_bytes(payload))
        target.write(b"\n")
        target.flush()
        os.fsync(target.fileno())
    os.replace(temporary, path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _validate_checkpoint(
    checkpoint: Any,
    *,
    validated_result: V4GraphCompilationResult,
    binding_sha256: str,
    provider_map_sha256: str,
    options: Mapping[str, int],
    input_contracts: Mapping[str, Any],
) -> None:
    if not isinstance(checkpoint, dict):
        raise RuntimeError("V4 graph checkpoint is not an object")
    expected_by_name = {
        "format": PTG2_V4_GRAPH_CHECKPOINT_FORMAT,
        "complete": True,
        "compiler_contract": PTG2_V4_GRAPH_SUMMARY_FORMAT,
        "shared_format_version": PTG2_V4_SHARED_FORMAT_VERSION,
        "binding_sha256": binding_sha256,
        "provider_set_key_map_sha256": provider_map_sha256,
        "selected_layout": validated_result.selected_layout,
        "block_count": validated_result.block_count,
        "selected_encoded_bytes": validated_result.selected_encoded_bytes,
    }
    for name, expected in expected_by_name.items():
        if checkpoint.get(name) != expected:
            raise RuntimeError(f"V4 graph checkpoint changed {name}")
    if checkpoint.get("options") != dict(sorted(options.items())):
        raise RuntimeError("V4 graph checkpoint changed compiler options")
    for field_name in ("npi_scope", "inferred_taxonomy"):
        if checkpoint.get(field_name) != input_contracts[field_name]:
            raise RuntimeError(f"V4 graph checkpoint changed {field_name} input")
    if checkpoint.get("summary_sha256") != _sha256_file(validated_result.summary_path):
        raise RuntimeError("V4 graph checkpoint changed summary bytes")
    if checkpoint.get("tax_identity") != validated_result.summary.get("tax_identity"):
        raise RuntimeError("V4 graph checkpoint changed tax identity binding")
    expected_artifacts = [
        {
            "name": artifact.name,
            "byte_count": artifact.byte_count,
            "sha256": artifact.sha256,
            "row_count": artifact.row_count,
        }
        for artifact in validated_result.output_artifacts
    ]
    if checkpoint.get("output_artifacts") != expected_artifacts:
        raise RuntimeError("V4 graph checkpoint changed output artifacts")


async def _emit_compile_progress(**payload: Any) -> None:
    context = current_live_progress_context()
    progress_by_field = {
        **context,
        "phase": "publishing: provider graph compile",
        "stage_id": "ptg2_v4_provider_graph_compile",
        "stage_ordinal": 5,
        "unit": "factor_edges",
        "denominator_state": "known",
        "source": "ptg2-v4-provider-graph-compile",
        "confidence": "live",
        **payload,
    }
    try:
        await asyncio.wait_for(
            asyncio.to_thread(write_live_progress, **progress_by_field),
            timeout=0.75,
        )
    except Exception:
        return


async def _emit_npi_scope_progress(
    *,
    done: int,
    total: int,
    stage_pct: float,
    elapsed_seconds: float,
    message: str,
) -> None:
    """Publish one exact authenticated-NPI-scope progress boundary."""

    await _emit_compile_progress(
        phase="publishing: provider graph NPI scope extraction",
        stage_id="ptg2_v4_provider_graph_npi_scope",
        source="ptg2-v4-provider-graph-npi-scope",
        pct=90.0 + stage_pct / 100.0,
        stage_pct=stage_pct,
        phase_pct=stage_pct,
        done=done,
        total=total,
        unit="npi_rows",
        elapsed_seconds=elapsed_seconds,
        checkpoint_reused=False,
        message=message,
    )


async def _publish_compiler_progress_state(
    state: _CompilerProgressState,
    *,
    emit_lock: asyncio.Lock,
    input_bytes: int,
    input_factor_edges: int,
    input_factor_owners: int,
    checkpoint_reused: bool,
    heartbeat: bool = False,
) -> None:
    global_pct = 92.0 + 3.0 * (state.phase_pct / 100.0)
    message = (
        f"provider graph compile {state.phase}; "
        f"{state.done}/{state.total} {state.unit}, "
        f"elapsed={state.elapsed_ms / 1000.0:.1f}s"
    )
    if heartbeat:
        message += "; active"
    async with emit_lock:
        await _emit_compile_progress(
            pct=global_pct,
            stage_pct=state.phase_pct,
            phase_pct=state.phase_pct,
            done=state.done,
            total=state.total,
            unit=state.unit,
            elapsed_seconds=state.elapsed_ms / 1000.0,
            input_bytes=input_bytes,
            input_factor_edges=input_factor_edges,
            input_factor_owners=input_factor_owners,
            checkpoint_reused=checkpoint_reused,
            compiler_phase=state.phase,
            compiler_progress_seq=state.seq,
            compiler_terminal=state.terminal,
            message=message,
        )


async def _consume_compiler_stderr(
    stream: asyncio.StreamReader,
    diagnostic_output: Any,
    *,
    state: _CompilerProgressState,
    emit_lock: asyncio.Lock,
    input_bytes: int,
    input_factor_edges: int,
    input_factor_owners: int,
) -> None:
    while True:
        line = await stream.readline()
        if not line:
            diagnostic_output.flush()
            return
        if len(line) > PTG2_V4_PROGRESS_MAX_LINE_BYTES:
            diagnostic_output.write(line)
            continue
        if not line.startswith(PTG2_V4_PROGRESS_PREFIX):
            diagnostic_output.write(line)
            continue
        progress_bytes = line[len(PTG2_V4_PROGRESS_PREFIX) :].strip()
        try:
            decoded = _load_json_bytes(progress_bytes, label="progress event")
        except RuntimeError:
            diagnostic_output.write(line)
            continue
        if not state.is_accepted(decoded):
            diagnostic_output.write(line)
            continue
        await _publish_compiler_progress_state(
            state,
            emit_lock=emit_lock,
            input_bytes=input_bytes,
            input_factor_edges=input_factor_edges,
            input_factor_owners=input_factor_owners,
            checkpoint_reused=False,
        )


def _resolve_v4_graph_compiler_binary() -> Path | None:
    configured = os.getenv(PTG2_V4_GRAPH_COMPILER_BIN_ENV, "").strip()
    if configured:
        candidate = Path(configured).expanduser().resolve()
        return (
            candidate if candidate.is_file() and os.access(candidate, os.X_OK) else None
        )
    root = Path(__file__).resolve().parents[2]
    for profile in ("release", "debug"):
        candidate = (
            root
            / "support"
            / "ptg2_scanner"
            / "target"
            / profile
            / "ptg2_provider_graph_v4"
        )
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate
    return None


def _read_bounded(path: Path, maximum_bytes: int, *, label: str) -> bytes:
    byte_count = path.stat().st_size
    if byte_count > maximum_bytes:
        raise RuntimeError(
            f"V4 graph compiler {label} is {byte_count} bytes; "
            f"maximum is {maximum_bytes} bytes"
        )
    return path.read_bytes()


def _read_error_tail(path: Path) -> str:
    with path.open("rb") as source:
        source.seek(0, os.SEEK_END)
        size = source.tell()
        source.seek(max(0, size - PTG2_V4_GRAPH_ERROR_TAIL_BYTES))
        return source.read().decode("utf-8", errors="replace")


def _consume_pg_binary_field(
    copy_file: Any,
    *,
    path: Path,
    file_bytes: int,
    field_index: int,
    validate_shared_version: bool,
    nullable_field_indices: frozenset[int],
) -> None:
    """Validate and consume one PostgreSQL binary COPY field."""

    length_bytes = copy_file.read(4)
    if len(length_bytes) != 4:
        raise RuntimeError(f"V4 graph compiler output truncates COPY field: {path}")
    field_bytes = struct.unpack(">i", length_bytes)[0]
    if field_bytes < 0:
        if field_bytes == -1 and field_index in nullable_field_indices:
            return
        raise RuntimeError(
            f"V4 graph compiler output contains invalid NULL COPY field: {path}"
        )
    if validate_shared_version and field_index == 1:
        if field_bytes != 2:
            raise RuntimeError("V4 graph block has invalid format-version width")
        version_bytes = copy_file.read(2)
        if (
            len(version_bytes) != 2
            or struct.unpack(">h", version_bytes)[0] != PTG2_V4_SHARED_FORMAT_VERSION
        ):
            raise RuntimeError("V4 graph block changed the shared CAS wire version")
        return
    copy_file.seek(field_bytes, os.SEEK_CUR)
    if copy_file.tell() > file_bytes:
        raise RuntimeError(f"V4 graph compiler output truncates COPY field: {path}")


def _count_pg_binary_rows(
    path: Path,
    *,
    expected_field_count: int,
    validate_shared_version: bool = False,
    nullable_field_indices: frozenset[int] = frozenset(),
) -> int:
    row_count = 0
    file_bytes = path.stat().st_size
    with path.open("rb") as copy_file:
        if copy_file.read(len(_PG_COPY_HEADER)) != _PG_COPY_HEADER:
            raise RuntimeError(
                f"V4 graph compiler output has invalid COPY header: {path}"
            )
        while True:
            field_count_bytes = copy_file.read(2)
            if len(field_count_bytes) != 2:
                raise RuntimeError(
                    f"V4 graph compiler output truncates COPY rows: {path}"
                )
            field_count = struct.unpack(">h", field_count_bytes)[0]
            if field_count == -1:
                if copy_file.read(1):
                    raise RuntimeError(
                        f"V4 graph compiler output has trailing COPY bytes: {path}"
                    )
                return row_count
            if field_count != expected_field_count:
                raise RuntimeError(
                    f"V4 graph compiler output has wrong COPY width: {path}"
                )
            for field_index in range(field_count):
                _consume_pg_binary_field(
                    copy_file,
                    path=path,
                    file_bytes=file_bytes,
                    field_index=field_index,
                    validate_shared_version=validate_shared_version,
                    nullable_field_indices=nullable_field_indices,
                )
            row_count += 1


def _iter_pg_binary_rows(
    path: Path,
    *,
    expected_field_count: int,
    nullable_field_indices: frozenset[int] = frozenset(),
):
    with path.open("rb") as copy_file:
        if copy_file.read(len(_PG_COPY_HEADER)) != _PG_COPY_HEADER:
            raise RuntimeError(
                f"V4 graph compiler output has invalid COPY header: {path}"
            )
        while True:
            width = copy_file.read(2)
            if len(width) != 2:
                raise RuntimeError(
                    f"V4 graph compiler output truncates COPY rows: {path}"
                )
            field_count = struct.unpack(">h", width)[0]
            if field_count == -1:
                if copy_file.read(1):
                    raise RuntimeError(
                        f"V4 graph compiler output has trailing COPY bytes: {path}"
                    )
                return
            if field_count != expected_field_count:
                raise RuntimeError(
                    f"V4 graph compiler output has wrong COPY width: {path}"
                )
            fields: list[bytes | None] = []
            for field_index in range(field_count):
                width = copy_file.read(4)
                if len(width) != 4:
                    raise RuntimeError(
                        f"V4 graph compiler output truncates COPY field: {path}"
                    )
                field_bytes = struct.unpack(">i", width)[0]
                if field_bytes == -1 and field_index in nullable_field_indices:
                    fields.append(None)
                    continue
                if field_bytes < 0:
                    raise RuntimeError(
                        f"V4 graph compiler output contains invalid NULL COPY field: {path}"
                    )
                field = copy_file.read(field_bytes)
                if len(field) != field_bytes:
                    raise RuntimeError(
                        f"V4 graph compiler output truncates COPY field: {path}"
                    )
                fields.append(field)
            yield tuple(fields)


def _validate_tax_token_dictionary(
    token_path: Path,
    *,
    token_count: int,
    content_hasher: Any,
) -> None:
    """Validate dense full-HMAC dictionary rows and extend content binding."""

    content_hasher.update(struct.pack(">Q", token_count))
    previous_hmac: bytes | None = None
    observed_tokens = 0
    for copy_fields in _iter_pg_binary_rows(token_path, expected_field_count=3):
        key, candidate, full_hmac = copy_fields
        if (
            key is None
            or candidate is None
            or full_hmac is None
            or len(key) != 4
            or struct.unpack(">i", key)[0] != observed_tokens
            or len(candidate) != 16
            or len(full_hmac) != 32
            or candidate != full_hmac[:16]
            or (previous_hmac is not None and full_hmac <= previous_hmac)
        ):
            raise RuntimeError(
                "V4 graph compiler tax identity dictionary is not canonical"
            )
        content_hasher.update(full_hmac)
        previous_hmac = full_hmac
        observed_tokens += 1
    if observed_tokens != token_count:
        raise RuntimeError("V4 graph compiler tax identity dictionary count changed")


def _validated_tax_group_copy_fields(
    copy_fields: tuple[bytes | None, ...],
    *,
    previous_group: bytes | None,
    summary: Mapping[str, Any],
) -> tuple[bytes, str, bytes | None, bytes]:
    """Validate one canonical provider-group tax projection row."""

    source_shard_count = int(summary["source_shard_count"])
    source_bitmap_bytes = int(summary["source_bitmap_bytes"])
    token_count = int(summary["tax_identity_count"])
    group, state_bytes, key, bitmap = copy_fields
    if group is None or state_bytes is None or bitmap is None:
        raise RuntimeError(
            "V4 graph compiler provider tax identity rows contain NULL fields"
        )
    try:
        state = state_bytes.decode("ascii")
    except UnicodeDecodeError as exc:
        raise RuntimeError("V4 graph compiler tax identity state is invalid") from exc
    if (
        len(group) != 16
        or (previous_group is not None and group <= previous_group)
        or state not in _TAX_STATE_CODE_BY_NAME
        or len(bitmap) != source_bitmap_bytes
        or not any(bitmap)
    ):
        raise RuntimeError(
            "V4 graph compiler provider tax identity rows are not canonical"
        )
    valid_last_bits = source_shard_count % 8
    if valid_last_bits and bitmap[-1] & ~((1 << valid_last_bits) - 1):
        raise RuntimeError(
            "V4 graph compiler tax identity bitmap has out-of-range bits"
        )
    if state == "matched_ein":
        if (
            key is None
            or len(key) != 4
            or not 0 <= struct.unpack(">i", key)[0] < token_count
        ):
            raise RuntimeError("V4 graph compiler matched tax identity key is invalid")
    elif key is not None:
        raise RuntimeError("V4 graph compiler unavailable tax identity has a key")
    return group, state, key, bitmap


def _validate_tax_group_projection(
    group_path: Path,
    *,
    summary: Mapping[str, Any],
    content_hasher: Any,
) -> None:
    """Validate group tax projection rows and extend content binding."""

    group_count = int(summary["provider_group_count"])
    content_hasher.update(struct.pack(">Q", group_count))
    observed_groups = 0
    previous_group: bytes | None = None
    count_by_state = dict.fromkeys(_TAX_STATE_CODE_BY_NAME, 0)
    for copy_fields in _iter_pg_binary_rows(
        group_path,
        expected_field_count=4,
        nullable_field_indices=frozenset({2}),
    ):
        group, state, key, bitmap = _validated_tax_group_copy_fields(
            copy_fields,
            previous_group=previous_group,
            summary=summary,
        )
        content_hasher.update(group)
        content_hasher.update(bytes((_TAX_STATE_CODE_BY_NAME[state],)))
        if key is None:
            content_hasher.update(b"\0")
        else:
            content_hasher.update(b"\1")
            content_hasher.update(key)
        content_hasher.update(struct.pack(">I", len(bitmap)))
        content_hasher.update(bitmap)
        count_by_state[state] += 1
        previous_group = group
        observed_groups += 1
    if (
        observed_groups != group_count
        or count_by_state["matched_ein"] != summary["matched_ein_count"]
        or count_by_state["missing"] != summary["missing_count"]
        or count_by_state["malformed"] != summary["malformed_count"]
        or count_by_state["unsupported_type"] != summary["unsupported_type_count"]
    ):
        raise RuntimeError("V4 graph compiler tax identity content binding changed")


def _validate_tax_identity_copy_outputs(
    *,
    token_path: Path,
    group_path: Path,
    summary: Mapping[str, Any],
) -> None:
    """Independently authenticate both tax COPY outputs against the summary."""

    content_hasher = hashlib.sha256()
    content_hasher.update(b"PTG2V4TAXCONTENT\x01")
    content_hasher.update(bytes.fromhex(str(summary["token_policy_descriptor_sha256"])))
    content_hasher.update(bytes.fromhex(str(summary["source_ordinal_map_digest"])))
    _validate_tax_token_dictionary(
        token_path,
        token_count=int(summary["tax_identity_count"]),
        content_hasher=content_hasher,
    )
    _validate_tax_group_projection(
        group_path,
        summary=summary,
        content_hasher=content_hasher,
    )
    if content_hasher.hexdigest() != summary["content_digest"]:
        raise RuntimeError("V4 graph compiler tax identity content binding changed")


def _read_prefix_override_metadata(
    path: Path,
    *,
    prefix_target: int,
) -> dict[int, tuple[int, bytes]]:
    metadata_by_provider_set: dict[int, tuple[int, bytes]] = {}
    previous_provider_set_key: int | None = None
    with path.open("rb") as copy_file:
        if copy_file.read(len(_PG_COPY_HEADER)) != _PG_COPY_HEADER:
            raise RuntimeError("V4 graph prefix metadata has invalid COPY header")
        while True:
            field_count_bytes = copy_file.read(2)
            if len(field_count_bytes) != 2:
                raise RuntimeError("V4 graph prefix metadata truncates COPY rows")
            field_count = struct.unpack(">h", field_count_bytes)[0]
            if field_count == -1:
                if copy_file.read(1):
                    raise RuntimeError("V4 graph prefix metadata has trailing bytes")
                return metadata_by_provider_set
            if field_count != 3:
                raise RuntimeError("V4 graph prefix metadata has invalid row width")
            fields: list[bytes] = []
            for expected_width in (4, 4, 32):
                width_bytes = copy_file.read(4)
                if len(width_bytes) != 4:
                    raise RuntimeError("V4 graph prefix metadata truncates field width")
                width = struct.unpack(">i", width_bytes)[0]
                if width != expected_width:
                    raise RuntimeError(
                        "V4 graph prefix metadata has invalid field width"
                    )
                field = copy_file.read(width)
                if len(field) != width:
                    raise RuntimeError("V4 graph prefix metadata truncates field")
                fields.append(field)
            provider_set_key = struct.unpack(">i", fields[0])[0]
            member_count = struct.unpack(">i", fields[1])[0]
            if (
                provider_set_key < 0
                or member_count < 0
                or member_count > prefix_target
                or (
                    previous_provider_set_key is not None
                    and provider_set_key <= previous_provider_set_key
                )
            ):
                raise RuntimeError("V4 graph prefix metadata is not canonical")
            metadata_by_provider_set[provider_set_key] = (member_count, fields[2])
            previous_provider_set_key = provider_set_key


def _summary_path(summary: Mapping[str, Any], field: str, expected: Path) -> Path:
    value = summary.get(field)
    if not isinstance(value, str):
        raise RuntimeError(f"V4 graph compiler has invalid {field}")
    path = Path(value)
    if (
        not path.is_absolute()
        or path.is_symlink()
        or path.resolve() != expected.resolve()
    ):
        raise RuntimeError(f"V4 graph compiler has unexpected {field}")
    if not path.is_file():
        raise RuntimeError(f"V4 graph compiler output is unavailable: {path}")
    return path


def _validate_reference_manifest(path: Path, expected_rows: int) -> None:
    observed = 0
    previous_coordinate: tuple[str, int, int] | None = None
    with path.open("rb") as reference_stream:
        for line in reference_stream:
            if len(line) > 64 * 1024:
                raise RuntimeError("V4 graph reference record exceeds 64 KiB")
            reference_by_field = _load_json_bytes(line, label="reference record")
            if not isinstance(reference_by_field, dict):
                raise RuntimeError("V4 graph reference record is not an object")
            object_kind = reference_by_field.get("object_kind")
            block_key = _strict_nonnegative_int(
                reference_by_field.get("block_key"), label="block_key"
            )
            fragment = _strict_nonnegative_int(
                reference_by_field.get("fragment_no"), label="fragment_no"
            )
            if not isinstance(object_kind, str) or not object_kind.startswith("v4_"):
                raise RuntimeError("V4 graph reference has invalid object_kind")
            coordinate = (object_kind, block_key, fragment)
            if previous_coordinate is not None and coordinate <= previous_coordinate:
                raise RuntimeError(
                    "V4 graph references are not in strict coordinate order"
                )
            previous_coordinate = coordinate
            _strict_sha256(reference_by_field.get("hash"), label="block hash")
            if reference_by_field.get("codec") != "none":
                raise RuntimeError(
                    "V4 graph compiler unexpectedly compressed a graph block"
                )
            observed += 1
    if observed != expected_rows:
        raise RuntimeError("V4 graph reference row count disagrees with summary")


def _length_prefixed_sha256(domain: bytes, fields: Iterable[bytes]) -> str:
    hasher = hashlib.sha256()
    hasher.update(domain)
    for field in fields:
        hasher.update(struct.pack(">I", len(field)))
        hasher.update(field)
    return hasher.hexdigest()


def _tax_policy_descriptor_sha256(token_policy_id: str) -> str:
    return _length_prefixed_sha256(
        _TAX_POLICY_DESCRIPTOR_HASH_DOMAIN,
        (
            token_policy_id.encode("ascii"),
            _TAX_IDENTITY_NORMALIZATION_CONTRACT.encode("ascii"),
            _TAX_IDENTITY_HMAC_CONTRACT.encode("ascii"),
            _TAX_IDENTITY_CANDIDATE_PREFIX_CONTRACT.encode("ascii"),
            _TAX_IDENTITY_AUTHORITY_CONTRACT.encode("ascii"),
        ),
    )


def _tax_source_ordinal_digest(source_shard_ids: Iterable[str]) -> str:
    source_ids = tuple(source_shard_ids)
    hasher = hashlib.sha256()
    hasher.update(_TAX_SOURCE_ORDINAL_HASH_DOMAIN)
    hasher.update(struct.pack(">I", len(source_ids)))
    for ordinal, shard_id in enumerate(source_ids):
        encoded = shard_id.encode("utf-8")
        hasher.update(struct.pack(">I", len(encoded)))
        hasher.update(encoded)
        hasher.update(struct.pack(">I", ordinal))
    return hasher.hexdigest()


def _tax_manifest_expectation(manifest: Mapping[str, Any]) -> dict[str, Any]:
    source_shard_ids = tuple(str(shard["shard_id"]) for shard in manifest["shards"])
    if not source_shard_ids or tuple(sorted(source_shard_ids)) != source_shard_ids:
        raise RuntimeError("V4 tax identity shard order is not canonical")
    policies = {
        str(shard[_TAX_IDENTITY_FIELD]["metadata"]["token_policy_id"])
        for shard in manifest["shards"]
    }
    if len(policies) != 1:
        raise RuntimeError(
            "V4 provider tax identity token policy differs across shards"
        )
    source_bitmap_bytes = (len(source_shard_ids) + 7) // 8
    group_occurrence_count = sum(
        int(shard[_TAX_IDENTITY_FIELD]["metadata"]["provider_group_count"])
        for shard in manifest["shards"]
    )
    matched_ein_occurrence_count = sum(
        int(shard[_TAX_IDENTITY_FIELD]["metadata"]["matched_ein_count"])
        for shard in manifest["shards"]
    )
    return {
        "token_policy_id": policies.pop(),
        "source_shard_ids": source_shard_ids,
        "merge_bitmap_upper_bound_bytes": (
            group_occurrence_count * source_bitmap_bytes
        ),
        "source_ordinal_upper_bound_bytes": (
            len(source_shard_ids) * _TAX_SOURCE_ORDINAL_FIXED_UPPER_BOUND_BYTES
            + sum(
                len(shard_id.encode("utf-8")) * _TAX_SOURCE_IDENTITY_COPY_UPPER_BOUND
                for shard_id in source_shard_ids
            )
        ),
        "projection_upper_bound_bytes": (
            group_occurrence_count
            * (_TAX_IDENTITY_GROUP_ENTRY_UPPER_BOUND_BYTES + source_bitmap_bytes)
            + matched_ein_occurrence_count
            * _TAX_IDENTITY_DICTIONARY_ENTRY_UPPER_BOUND_BYTES
        ),
    }


def _validate_tax_summary_contract(raw: Mapping[str, Any]) -> str:
    """Validate immutable tax projection contract fields."""

    expected_fields = {
        "contract",
        "token_policy_id",
        "token_policy_descriptor_sha256",
        "normalization_contract",
        "hmac_contract",
        "candidate_prefix_contract",
        "authority_contract",
        "source_ordinal_contract",
        "source_ordinal_map",
        "source_ordinal_map_digest",
        "source_shard_count",
        "source_bitmap_bytes",
        "provider_group_count",
        "tax_identity_count",
        "matched_ein_count",
        "missing_count",
        "malformed_count",
        "unsupported_type_count",
        "content_digest",
    }
    if set(raw) != expected_fields:
        raise RuntimeError("V4 graph compiler tax identity summary shape changed")
    token_policy_id = raw.get("token_policy_id")
    if (
        raw.get("contract") != _TAX_IDENTITY_PROJECTION_CONTRACT
        or not isinstance(token_policy_id, str)
        or len(token_policy_id.encode("ascii", errors="ignore")) != len(token_policy_id)
        or len(token_policy_id.encode("ascii")) > 55
        or _TAX_POLICY_ID.fullmatch(token_policy_id) is None
        or raw.get("normalization_contract") != _TAX_IDENTITY_NORMALIZATION_CONTRACT
        or raw.get("hmac_contract") != _TAX_IDENTITY_HMAC_CONTRACT
        or raw.get("candidate_prefix_contract")
        != _TAX_IDENTITY_CANDIDATE_PREFIX_CONTRACT
        or raw.get("authority_contract") != _TAX_IDENTITY_AUTHORITY_CONTRACT
        or raw.get("source_ordinal_contract") != _TAX_SOURCE_ORDINAL_CONTRACT
        or raw.get("token_policy_descriptor_sha256")
        != _tax_policy_descriptor_sha256(token_policy_id)
    ):
        raise RuntimeError("V4 graph compiler tax identity contract changed")
    return token_policy_id


def _validate_tax_source_binding(
    raw: Mapping[str, Any],
    *,
    token_policy_id: str,
    expected: Mapping[str, Any] | None,
) -> None:
    """Validate deterministic source ordinals and optional input binding."""

    source_ordinal_map = raw.get("source_ordinal_map")
    if not isinstance(source_ordinal_map, list) or not source_ordinal_map:
        raise RuntimeError("V4 graph compiler tax identity source map is invalid")
    source_shard_ids: list[str] = []
    for ordinal, entry in enumerate(source_ordinal_map):
        if (
            not isinstance(entry, dict)
            or set(entry) != {"shard_id", "ordinal"}
            or entry.get("ordinal") != ordinal
            or not isinstance(entry.get("shard_id"), str)
            or not entry["shard_id"]
        ):
            raise RuntimeError("V4 graph compiler tax identity source map is invalid")
        source_shard_ids.append(entry["shard_id"])
    if source_shard_ids != sorted(set(source_shard_ids)):
        raise RuntimeError("V4 graph compiler tax identity source map is not canonical")
    source_shard_count = _strict_nonnegative_int(
        raw.get("source_shard_count"), label="tax source_shard_count"
    )
    source_bitmap_bytes = _strict_nonnegative_int(
        raw.get("source_bitmap_bytes"), label="tax source_bitmap_bytes"
    )
    if (
        source_shard_count != len(source_shard_ids)
        or source_bitmap_bytes != (source_shard_count + 7) // 8
        or raw.get("source_ordinal_map_digest")
        != _tax_source_ordinal_digest(source_shard_ids)
    ):
        raise RuntimeError("V4 graph compiler tax identity source binding changed")
    if expected is not None and (
        token_policy_id != expected.get("token_policy_id")
        or tuple(source_shard_ids) != tuple(expected.get("source_shard_ids") or ())
    ):
        raise RuntimeError("V4 graph compiler tax identity input binding changed")


def _validate_tax_summary_counts(raw: Mapping[str, Any]) -> None:
    """Validate state totals and both authenticated summary digests."""

    count_by_name = {
        name: _strict_nonnegative_int(raw.get(name), label=f"tax {name}")
        for name in (
            "provider_group_count",
            "tax_identity_count",
            "matched_ein_count",
            "missing_count",
            "malformed_count",
            "unsupported_type_count",
        )
    }
    if (
        sum(
            count_by_name[name]
            for name in (
                "matched_ein_count",
                "missing_count",
                "malformed_count",
                "unsupported_type_count",
            )
        )
        != count_by_name["provider_group_count"]
        or count_by_name["tax_identity_count"] > count_by_name["matched_ein_count"]
    ):
        raise RuntimeError("V4 graph compiler tax identity counts are inconsistent")
    _strict_sha256(raw.get("source_ordinal_map_digest"), label="tax source map digest")
    _strict_sha256(raw.get("content_digest"), label="tax content digest")


def _validate_tax_identity_summary(
    raw: Any,
    *,
    expected: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Authenticate tax summary shape, contracts, counts, and input binding."""

    if not isinstance(raw, dict):
        raise RuntimeError("V4 graph compiler has invalid tax identity summary")
    token_policy_id = _validate_tax_summary_contract(raw)
    _validate_tax_source_binding(
        raw,
        token_policy_id=token_policy_id,
        expected=expected,
    )
    _validate_tax_summary_counts(raw)
    return dict(raw)


def _normalized_observe_value(name: str, observed_value: Any) -> Any:
    optional_key_fields = {
        "npi_prefix_worst_provider_set_key",
        "npi_prefix_worst_online_provider_set_key",
    }
    digest_fields = {
        "npi_prefix_worst_member_digest",
        "npi_prefix_worst_online_member_digest",
    }
    boolean_fields = {
        "npi_prefix_worst_provider_set_uses_override",
        "npi_prefix_worst_uses_component_fallback",
        "npi_prefix_worst_online_groups_to_target_exact",
        "npi_prefix_worst_online_uses_component_fallback",
    }
    if name in optional_key_fields and observed_value is None:
        return None
    if name in digest_fields:
        return (
            None
            if observed_value is None
            else _strict_sha256(observed_value, label=f"observe.{name}")
        )
    if name in boolean_fields:
        if not isinstance(observed_value, bool):
            raise RuntimeError(f"V4 graph compiler has invalid observe.{name}")
        return observed_value
    return _strict_nonnegative_int(observed_value, label=f"observe.{name}")


def _normalize_observe_counters(observe_raw: Any) -> dict[str, Any]:
    if not isinstance(observe_raw, dict):
        raise RuntimeError("V4 graph compiler has invalid observe counters")
    return {
        name: _normalized_observe_value(name, observed_value)
        for name, observed_value in observe_raw.items()
    }


def _validate_compiler_summary(
    summary: Any,
    *,
    output_directory: Path,
    expectation: _CompilerSummaryExpectation,
    allow_checkpoint: bool = False,
) -> V4GraphCompilationResult:
    """Authenticate compiler geometry, outputs, and admission evidence."""

    if not isinstance(summary, dict):
        raise RuntimeError("V4 graph compiler summary is not an object")
    if summary.get("format") != PTG2_V4_GRAPH_SUMMARY_FORMAT:
        raise RuntimeError("V4 graph compiler summary has incompatible format")
    selected_layout = summary.get("selected_layout")
    if selected_layout not in {"direct", "pattern"}:
        raise RuntimeError("V4 graph compiler summary has invalid selected_layout")
    direct_bytes = _strict_nonnegative_int(
        summary.get("direct_complete_encoded_bytes"), label="direct encoded bytes"
    )
    pattern_bytes = _strict_nonnegative_int(
        summary.get("pattern_complete_encoded_bytes"), label="pattern encoded bytes"
    )
    selected_bytes = _strict_nonnegative_int(
        summary.get("selected_encoded_bytes"), label="selected encoded bytes"
    )
    direct_graph_bytes = _strict_nonnegative_int(
        summary.get("direct_graph_encoded_bytes"),
        label="direct graph encoded bytes",
    )
    pattern_graph_bytes = _strict_nonnegative_int(
        summary.get("pattern_graph_encoded_bytes"),
        label="pattern graph encoded bytes",
    )
    direct_mapping_bytes = _strict_nonnegative_int(
        summary.get("direct_mapping_persistence_encoded_bytes"),
        label="direct mapping persistence encoded bytes",
    )
    pattern_mapping_bytes = _strict_nonnegative_int(
        summary.get("pattern_mapping_persistence_encoded_bytes"),
        label="pattern mapping persistence encoded bytes",
    )
    direct_taxonomy_bytes = _strict_nonnegative_int(
        summary.get("direct_inferred_taxonomy_encoded_bytes"),
        label="direct inferred taxonomy encoded bytes",
    )
    pattern_taxonomy_bytes = _strict_nonnegative_int(
        summary.get("pattern_inferred_taxonomy_encoded_bytes"),
        label="pattern inferred taxonomy encoded bytes",
    )
    for layout_name, mapping_bytes in (
        ("direct", direct_mapping_bytes),
        ("pattern", pattern_mapping_bytes),
    ):
        geometry_by_field = {
            name: _strict_nonnegative_int(
                summary.get(f"{layout_name}_{name}"),
                label=f"{layout_name} {name.replace('_', ' ')}",
            )
            for name in (
                "map_payload_encoded_bytes",
                "map_coordinate_count",
                "map_pack_count",
                "map_object_kind_count",
            )
        }
        coordinate_count = geometry_by_field["map_coordinate_count"]
        pack_count = geometry_by_field["map_pack_count"]
        object_kind_count = geometry_by_field["map_object_kind_count"]
        if (
            geometry_by_field["map_payload_encoded_bytes"]
            != coordinate_count * 52 + pack_count * 80
            or geometry_by_field["map_payload_encoded_bytes"] > mapping_bytes
            or bool(coordinate_count) != bool(pack_count)
            or bool(pack_count) != bool(object_kind_count)
            or object_kind_count > pack_count
            or pack_count > coordinate_count
        ):
            raise RuntimeError(
                f"V4 graph {layout_name} packed-map geometry is inconsistent"
            )
    selected_graph_bytes = _strict_nonnegative_int(
        summary.get("selected_graph_encoded_bytes"),
        label="selected graph encoded bytes",
    )
    common_bytes = _strict_nonnegative_int(
        summary.get("common_encoded_bytes"),
        label="common encoded bytes",
    )
    if (
        direct_bytes
        != direct_graph_bytes + direct_mapping_bytes + direct_taxonomy_bytes
        or pattern_bytes
        != pattern_graph_bytes + pattern_mapping_bytes + pattern_taxonomy_bytes
        or common_bytes > min(direct_graph_bytes, pattern_graph_bytes)
    ):
        raise RuntimeError("V4 graph persistent candidate byte counts disagree")
    input_byte_count = _strict_nonnegative_int(
        summary.get("input_byte_count"), label="input_byte_count"
    )
    if input_byte_count != expectation.input_bytes:
        raise RuntimeError("V4 graph compiler input byte count changed")
    _strict_sha256(summary.get("input_sha256"), label="input digest")
    block_count = _strict_nonnegative_int(
        summary.get("block_count"), label="block_count"
    )
    observe_raw = summary.get("observe")
    observe_by_name = _normalize_observe_counters(observe_raw)
    tax_identity_by_name = _validate_tax_identity_summary(
        summary.get("tax_identity"),
        expected=expectation.tax_identity,
    )
    max_set_patterns_per_set = _strict_nonnegative_int(
        summary.get("max_set_patterns_per_set"),
        label="max_set_patterns_per_set",
    )
    if max_set_patterns_per_set != expectation.options.get("max_set_patterns_per_set"):
        raise RuntimeError("V4 graph pattern serving-degree limit changed")
    max_set_components_per_fallback_set = _strict_nonnegative_int(
        summary.get("max_set_components_per_fallback_set"),
        label="max_set_components_per_fallback_set",
    )
    if max_set_components_per_fallback_set != expectation.options.get(
        "max_set_components_per_fallback_set"
    ):
        raise RuntimeError("V4 graph component fallback-degree limit changed")
    for option_name in (
        "max_online_group_keys_per_set",
        "max_online_source_owners_per_set",
        "max_online_source_members_per_set",
        "max_online_source_pages_per_set",
        "max_online_source_bytes_per_set",
        "online_group_npi_batch_size",
        "max_online_group_npi_members_per_set",
        "max_online_group_npi_locator_pages_per_set",
        "max_online_group_npi_member_pages_per_set",
        "max_online_group_npi_bytes_per_set",
        "max_online_group_npi_batches_per_set",
        "provider_expansion_rate_page_rows",
        "max_online_provider_expansion_rate_rows",
        "max_online_provider_expansion_provider_sets",
        "max_online_provider_expansion_graph_batches",
        "npi_prefix_target",
        "max_npi_prefix_override_owners",
        "max_npi_prefix_override_bytes",
        "max_online_inferred_taxonomy_candidates",
        "max_online_candidate_pattern_projection_members",
    ):
        observed_option = _strict_nonnegative_int(
            summary.get(option_name), label=option_name
        )
        if observed_option != expectation.options.get(option_name):
            raise RuntimeError(f"V4 graph compiler option {option_name} changed")
    maximum_patterns_per_set = _strict_nonnegative_int(
        observe_raw.get("maximum_patterns_per_set"),
        label="observe.maximum_patterns_per_set",
    )
    maximum_components_per_set = _strict_nonnegative_int(
        observe_raw.get("maximum_components_per_set"),
        label="observe.maximum_components_per_set",
    )
    pattern_overflow_set_count = _strict_nonnegative_int(
        observe_raw.get("pattern_overflow_set_count"),
        label="observe.pattern_overflow_set_count",
    )
    maximum_components_per_pattern_overflow_set = _strict_nonnegative_int(
        observe_raw.get("maximum_components_per_pattern_overflow_set"),
        label="observe.maximum_components_per_pattern_overflow_set",
    )
    component_over_cap_set_count = _strict_nonnegative_int(
        observe_raw.get("pattern_component_over_cap_set_count"),
        label="observe.pattern_component_over_cap_set_count",
    )
    component_over_cap_prefix_covered_set_count = _strict_nonnegative_int(
        observe_raw.get("pattern_component_over_cap_prefix_covered_set_count"),
        label="observe.pattern_component_over_cap_prefix_covered_set_count",
    )
    unsafe_pattern_component_set_count = _strict_nonnegative_int(
        observe_raw.get("unsafe_pattern_component_set_count"),
        label="observe.unsafe_pattern_component_set_count",
    )
    if (
        maximum_components_per_pattern_overflow_set > maximum_components_per_set
        or unsafe_pattern_component_set_count > pattern_overflow_set_count
        or component_over_cap_set_count > pattern_overflow_set_count
        or component_over_cap_prefix_covered_set_count > component_over_cap_set_count
        or unsafe_pattern_component_set_count
        != component_over_cap_set_count - component_over_cap_prefix_covered_set_count
        or (
            maximum_patterns_per_set <= max_set_patterns_per_set
            and (
                pattern_overflow_set_count
                or maximum_components_per_pattern_overflow_set
                or component_over_cap_set_count
                or component_over_cap_prefix_covered_set_count
                or unsafe_pattern_component_set_count
            )
        )
        or (
            maximum_patterns_per_set > max_set_patterns_per_set
            and not pattern_overflow_set_count
        )
        or bool(pattern_overflow_set_count)
        != bool(maximum_components_per_pattern_overflow_set)
        or (
            maximum_components_per_pattern_overflow_set
            <= max_set_components_per_fallback_set
            and component_over_cap_set_count
        )
    ):
        raise RuntimeError("V4 graph component fallback diagnostics are inconsistent")
    is_component_fallback_eligible = unsafe_pattern_component_set_count == 0
    (
        worst_provider_set_key,
        worst_online_provider_set_key,
        worst_provider_set_uses_override,
    ) = _validate_prefix_diagnostics(
        observe_by_name,
        expectation.options,
        complete_direct_coverage=selected_layout == "direct",
    )
    _validate_npi_pattern_diagnostics(observe_by_name)
    direct_prefix_eligible = summary.get("direct_layout_complete_prefix_eligible")
    if not isinstance(direct_prefix_eligible, bool):
        raise RuntimeError("V4 graph direct prefix eligibility is invalid")
    direct_prefix_projection_bytes = _strict_nonnegative_int(
        summary.get("direct_complete_prefix_projection_encoded_bytes"),
        label="direct complete prefix projection encoded bytes",
    )
    is_direct_prefix_expected = direct_prefix_projection_bytes <= int(
        expectation.options["max_npi_prefix_override_bytes"]
    ) and direct_prefix_projection_bytes <= int(
        expectation.options["max_estimated_model_bytes"]
    )
    if direct_prefix_eligible is not is_direct_prefix_expected:
        raise RuntimeError("V4 graph direct prefix eligibility changed")
    sparse_owner_count = _strict_nonnegative_int(
        summary.get("pattern_sparse_prefix_owner_count"),
        label="pattern sparse prefix owner count",
    )
    sparse_member_count = _strict_nonnegative_int(
        summary.get("pattern_sparse_prefix_member_count"),
        label="pattern sparse prefix member count",
    )
    sparse_raw_bytes = _strict_nonnegative_int(
        summary.get("pattern_sparse_prefix_raw_bytes"),
        label="pattern sparse prefix raw bytes",
    )
    sparse_projection_bytes = _strict_nonnegative_int(
        summary.get("pattern_sparse_prefix_projection_encoded_bytes"),
        label="pattern sparse prefix projection encoded bytes",
    )
    sparse_prefix_eligible = summary.get("pattern_layout_sparse_prefix_eligible")
    if not isinstance(sparse_prefix_eligible, bool):
        raise RuntimeError("V4 graph sparse prefix eligibility is invalid")
    is_sparse_prefix_expected = sparse_owner_count <= int(
        expectation.options["max_npi_prefix_override_owners"]
    ) and sparse_raw_bytes <= int(expectation.options["max_npi_prefix_override_bytes"])
    if (
        sparse_raw_bytes != sparse_member_count * 4
        or sparse_member_count
        > sparse_owner_count * int(expectation.options["npi_prefix_target"])
        or sparse_projection_bytes < sparse_raw_bytes
        or sparse_prefix_eligible is not is_sparse_prefix_expected
        or (
            selected_layout == "pattern"
            and (
                sparse_owner_count
                != _observe_counter(
                    observe_by_name,
                    "npi_prefix_override_owner_count",
                )
                or sparse_member_count
                != _observe_counter(
                    observe_by_name,
                    "npi_prefix_override_member_count",
                )
                or sparse_raw_bytes
                != _observe_counter(
                    observe_by_name,
                    "npi_prefix_override_raw_bytes",
                )
            )
        )
    ):
        raise RuntimeError("V4 graph sparse prefix evidence is inconsistent")
    is_expected_pattern_serving_eligible = (
        is_component_fallback_eligible and sparse_prefix_eligible
    )
    if (
        summary.get("pattern_layout_serving_degree_eligible")
        is not is_expected_pattern_serving_eligible
    ):
        raise RuntimeError("V4 graph pattern serving-degree decision changed")
    is_direct_layout_eligible = direct_prefix_eligible and _is_summary_field_enabled(
        summary,
        "direct_inferred_taxonomy_eligible",
    )
    is_pattern_layout_eligible = (
        is_expected_pattern_serving_eligible
        and _is_summary_field_enabled(
            summary,
            "pattern_inferred_taxonomy_eligible",
        )
    )
    if pattern_bytes < direct_bytes and is_pattern_layout_eligible:
        expected_layout = "pattern"
    elif is_direct_layout_eligible:
        expected_layout = "direct"
    elif is_pattern_layout_eligible:
        expected_layout = "pattern"
    else:
        raise RuntimeError("V4 graph has no bounded representation")
    selected_base_bytes = (
        pattern_bytes if expected_layout == "pattern" else direct_bytes
    )
    expected_selected_graph_bytes = (
        pattern_graph_bytes if expected_layout == "pattern" else direct_graph_bytes
    )
    if (
        selected_layout != expected_layout
        or selected_bytes != selected_base_bytes
        or selected_graph_bytes != expected_selected_graph_bytes
    ):
        raise RuntimeError("V4 graph compiler violated adaptive-layout choice")
    resource_raw = summary.get("resource_admission")
    if not isinstance(resource_raw, dict):
        raise RuntimeError("V4 graph compiler has invalid resource admission summary")
    resource_admission_by_name = dict(resource_raw)
    for name in (
        "input_factor_bytes",
        "provider_set_key_map_bytes",
        "factor_edge_count",
        "factor_owner_count",
        "tax_identity_merge_bitmap_upper_bound_bytes",
        "tax_identity_source_ordinal_upper_bound_bytes",
        "tax_identity_projection_upper_bound_bytes",
        "tax_identity_projection_bytes",
        "estimated_peak_bytes",
        "max_estimated_model_bytes",
        "max_factor_edges",
    ):
        resource_admission_by_name[name] = _strict_nonnegative_int(
            resource_raw.get(name), label=f"resource_admission.{name}"
        )
    if resource_admission_by_name["input_factor_bytes"] != expectation.input_bytes:
        raise RuntimeError("V4 graph resource input byte count changed")
    if resource_admission_by_name["factor_edge_count"] != expectation.factor_edges:
        raise RuntimeError("V4 graph resource factor edge count changed")
    if resource_admission_by_name["factor_owner_count"] != expectation.factor_owners:
        raise RuntimeError("V4 graph resource factor owner count changed")
    if (
        expectation.tax_identity is None
        or resource_admission_by_name["tax_identity_merge_bitmap_upper_bound_bytes"]
        != expectation.tax_identity["merge_bitmap_upper_bound_bytes"]
    ):
        raise RuntimeError("V4 graph tax identity merge bitmap admission changed")
    if (
        resource_admission_by_name["tax_identity_source_ordinal_upper_bound_bytes"]
        != expectation.tax_identity["source_ordinal_upper_bound_bytes"]
    ):
        raise RuntimeError("V4 graph tax identity source ordinal admission changed")
    if (
        resource_admission_by_name["tax_identity_projection_upper_bound_bytes"]
        != expectation.tax_identity["projection_upper_bound_bytes"]
    ):
        raise RuntimeError("V4 graph tax identity projection admission changed")
    if resource_admission_by_name[
        "max_estimated_model_bytes"
    ] != expectation.options.get("max_estimated_model_bytes"):
        raise RuntimeError("V4 graph resource model byte limit changed")
    if resource_admission_by_name["max_factor_edges"] != expectation.options.get(
        "max_factor_edges"
    ):
        raise RuntimeError("V4 graph resource factor edge limit changed")
    if not isinstance(resource_raw.get("formula"), str):
        raise RuntimeError("V4 graph resource admission formula is missing")
    v4_adaptive_layout_decision_from_summary(summary)
    relations = summary.get("relation_summaries")
    heavy_bitmaps = summary.get("heavy_bitmaps")
    if not isinstance(relations, list) or not all(
        isinstance(relation_summary, dict) for relation_summary in relations
    ):
        raise RuntimeError("V4 graph compiler has invalid relation summaries")
    if not isinstance(heavy_bitmaps, list) or not all(
        isinstance(bitmap_summary, dict) for bitmap_summary in heavy_bitmaps
    ):
        raise RuntimeError("V4 graph compiler has invalid heavy bitmap summaries")
    relation_by_name: dict[str, dict[str, Any]] = {}
    replaced_members_by_relation: dict[str, int] = {}
    for raw_relation in relations:
        relation = raw_relation.get("relation")
        if (
            not isinstance(relation, str)
            or not relation
            or relation in relation_by_name
        ):
            raise RuntimeError("V4 graph compiler has invalid relation identity")
        owner_base = _strict_nonnegative_int(
            raw_relation.get("owner_base"), label=f"{relation} owner_base"
        )
        owner_count = _strict_nonnegative_int(
            raw_relation.get("owner_count"), label=f"{relation} owner_count"
        )
        logical_member_count = _strict_nonnegative_int(
            raw_relation.get("logical_member_count"),
            label=f"{relation} logical_member_count",
        )
        vector_member_count = _strict_nonnegative_int(
            raw_relation.get("vector_member_count"),
            label=f"{relation} vector_member_count",
        )
        member_width = _strict_nonnegative_int(
            raw_relation.get("member_width"), label=f"{relation} member_width"
        )
        raw_vector_bytes = _strict_nonnegative_int(
            raw_relation.get("raw_vector_bytes"), label=f"{relation} raw_vector_bytes"
        )
        if (
            owner_base + owner_count > 0x1_0000_0000
            or vector_member_count > logical_member_count
            or member_width != 4
            or raw_vector_bytes != vector_member_count * member_width
        ):
            raise RuntimeError("V4 graph compiler relation counts are inconsistent")
        relation_by_name[relation] = dict(raw_relation)
        replaced_members_by_relation[relation] = 0

    observed_heavy_owners: set[tuple[str, int]] = set()
    for raw_bitmap in heavy_bitmaps:
        relation = raw_bitmap.get("relation")
        if not isinstance(relation, str) or relation not in relation_by_name:
            raise RuntimeError("V4 graph compiler bitmap has an invalid relation")
        owner_key = _strict_nonnegative_int(
            raw_bitmap.get("owner_key"), label="bitmap owner_key"
        )
        member_count = _strict_nonnegative_int(
            raw_bitmap.get("member_count"), label="bitmap member_count"
        )
        relation_summary = relation_by_name[relation]
        owner_base = int(relation_summary["owner_base"])
        owner_count = int(relation_summary["owner_count"])
        owner_identity = (relation, owner_key)
        if (
            owner_identity in observed_heavy_owners
            or not owner_base <= owner_key < owner_base + owner_count
            or member_count <= 0
        ):
            raise RuntimeError("V4 graph compiler bitmap owner is inconsistent")
        observed_heavy_owners.add(owner_identity)
        replaced_members_by_relation[relation] += member_count
    for relation, raw_relation in relation_by_name.items():
        if replaced_members_by_relation[relation] != int(
            raw_relation["logical_member_count"]
        ) - int(raw_relation["vector_member_count"]):
            raise RuntimeError(
                "V4 graph compiler logical/vector counts disagree with bitmap owners"
            )
    override_relation = relation_by_name.get("set_npi_prefix_override")
    set_component_relation = relation_by_name.get("set_components")
    if override_relation is None or set_component_relation is None:
        raise RuntimeError("V4 graph compiler omitted NPI-prefix relation geometry")
    override_owner_base = int(override_relation["owner_base"])
    override_owner_count = int(override_relation["owner_count"])
    if (
        override_owner_base != int(set_component_relation["owner_base"])
        or override_owner_count
        != _observe_counter(observe_by_name, "provider_set_count")
        or int(override_relation["logical_member_count"])
        != _observe_counter(observe_by_name, "npi_prefix_override_member_count")
        or int(override_relation["vector_member_count"])
        != int(override_relation["logical_member_count"])
        or any(
            relation == "set_npi_prefix_override"
            for relation, _owner_key in observed_heavy_owners
        )
        or (
            worst_provider_set_key is not None
            and not override_owner_base
            <= worst_provider_set_key
            < override_owner_base + override_owner_count
        )
        or (
            worst_online_provider_set_key is not None
            and not override_owner_base
            <= worst_online_provider_set_key
            < override_owner_base + override_owner_count
        )
    ):
        raise RuntimeError("V4 graph NPI-prefix relation geometry is inconsistent")
    relation_blocks = sum(
        _strict_nonnegative_int(
            relation_summary.get("member_block_count"),
            label="member blocks",
        )
        + _strict_nonnegative_int(
            relation_summary.get("locator_block_count"),
            label="locator blocks",
        )
        for relation_summary in relations
    )
    bitmap_blocks = sum(
        _strict_nonnegative_int(
            bitmap_summary.get("block_count"),
            label="bitmap blocks",
        )
        for bitmap_summary in heavy_bitmaps
    )
    if relation_blocks + bitmap_blocks != block_count:
        raise RuntimeError("V4 graph compiler block counts disagree")

    expected_path_by_field = {
        "block_copy_path": output_directory / "v4-graph-blocks.copy",
        "reference_manifest_path": output_directory / "v4-graph-references.jsonl",
        "group_copy_path": output_directory / "v4-provider-groups.copy",
        "component_copy_path": output_directory / "v4-provider-components.copy",
        "npi_copy_path": output_directory / "v4-npi-scope.copy",
        "provider_set_audit_npi_copy_path": output_directory
        / "v4-provider-set-audit-npi.copy",
        "provider_set_npi_prefix_override_copy_path": output_directory
        / "v4-provider-set-npi-prefix-overrides.copy",
        "provider_tax_identity_copy_path": output_directory
        / "v4-provider-tax-identities.copy",
        "provider_group_tax_identity_copy_path": output_directory
        / "v4-provider-group-tax-identities.copy",
        "inferred_taxonomy_copy_path": output_directory
        / "v4-inferred-taxonomy-candidates.copy",
        "summary_path": output_directory / "v4-summary.json",
    }
    path_by_field = {
        field: _summary_path(summary, field, expected)
        for field, expected in expected_path_by_field.items()
    }
    raw_pattern_path = summary.get("pattern_copy_path")
    if selected_layout == "pattern":
        pattern_copy_path = _summary_path(
            summary, "pattern_copy_path", output_directory / "v4-patterns.copy"
        )
    elif raw_pattern_path is not None:
        raise RuntimeError(
            "V4 direct layout unexpectedly published a pattern dictionary"
        )
    else:
        pattern_copy_path = None

    artifacts_raw = summary.get("output_artifacts")
    if not isinstance(artifacts_raw, list):
        raise RuntimeError("V4 graph compiler has invalid output artifacts")
    output_artifacts: list[V4GraphOutputArtifact] = []
    observed_names: set[str] = set()
    for raw in artifacts_raw:
        if not isinstance(raw, dict):
            raise RuntimeError("V4 graph compiler output artifact is invalid")
        name = raw.get("name")
        if (
            not isinstance(name, str)
            or name not in _OUTPUT_FILE_BY_NAME
            or name in observed_names
        ):
            raise RuntimeError("V4 graph compiler output artifact has invalid name")
        observed_names.add(name)
        filename, field_count = _OUTPUT_FILE_BY_NAME[name]
        path = _summary_path(raw, "path", output_directory / filename)
        byte_count = _strict_nonnegative_int(
            raw.get("byte_count"), label="output bytes"
        )
        row_count = _strict_nonnegative_int(raw.get("row_count"), label="output rows")
        digest = _strict_sha256(raw.get("sha256"), label="output sha256")
        if path.stat().st_size != byte_count or _sha256_file(path) != digest:
            raise RuntimeError(
                f"V4 graph compiler output authentication failed: {path}"
            )
        if field_count is not None:
            observed_rows = _count_pg_binary_rows(
                path,
                expected_field_count=field_count,
                validate_shared_version=name == "graph_blocks",
                nullable_field_indices=(
                    frozenset({2})
                    if name == "provider_group_tax_identities"
                    else (
                        frozenset({8, 9})
                        if name == "inferred_taxonomy_candidates"
                        else frozenset()
                    )
                ),
            )
            if observed_rows != row_count:
                raise RuntimeError(f"V4 graph compiler COPY row count changed: {path}")
        output_artifacts.append(
            V4GraphOutputArtifact(name, path, byte_count, digest, row_count)
        )
    expected_artifact_names = {
        "graph_blocks",
        "graph_references",
        "provider_groups",
        "provider_components",
        "npi_scope",
        "provider_set_audit_npi",
        "provider_set_npi_prefix_overrides",
        "provider_tax_identities",
        "provider_group_tax_identities",
        "inferred_taxonomy_candidates",
    }
    if selected_layout == "pattern":
        expected_artifact_names.add("patterns")
    if observed_names != expected_artifact_names:
        raise RuntimeError("V4 graph compiler output artifact set is incomplete")
    artifact_by_name = {artifact.name: artifact for artifact in output_artifacts}
    if expectation.taxonomy_rule_count is not None and artifact_by_name[
        "inferred_taxonomy_candidates"
    ].row_count != int(expectation.taxonomy_rule_count):
        raise RuntimeError("V4 graph inferred-taxonomy candidate rule count changed")
    if artifact_by_name["graph_blocks"].row_count != block_count:
        raise RuntimeError("V4 graph block row count disagrees with summary")
    if artifact_by_name["graph_references"].row_count != block_count:
        raise RuntimeError("V4 graph reference row count disagrees with summary")
    _validate_reference_manifest(path_by_field["reference_manifest_path"], block_count)
    expected_dictionary_rows = {
        "provider_groups": observe_by_name.get("group_count"),
        "provider_components": observe_by_name.get("component_count"),
        "npi_scope": observe_by_name.get("npi_count"),
        "provider_set_audit_npi": observe_by_name.get("provider_set_audit_npi_count"),
        "provider_set_npi_prefix_overrides": observe_by_name.get(
            "npi_prefix_override_owner_count"
        ),
        "provider_tax_identities": tax_identity_by_name.get("tax_identity_count"),
        "provider_group_tax_identities": tax_identity_by_name.get(
            "provider_group_count"
        ),
        "inferred_taxonomy_candidates": None,
        "patterns": observe_by_name.get("pattern_count"),
    }
    if tax_identity_by_name["provider_group_count"] != observe_by_name.get(
        "group_count"
    ):
        raise RuntimeError(
            "V4 graph provider tax identity group count disagrees with graph"
        )
    for name, expected_rows in expected_dictionary_rows.items():
        if (
            expected_rows is not None
            and name in artifact_by_name
            and artifact_by_name[name].row_count != expected_rows
        ):
            raise RuntimeError(
                f"V4 graph {name} row count disagrees with observe counters"
            )
    _validate_tax_identity_copy_outputs(
        token_path=path_by_field["provider_tax_identity_copy_path"],
        group_path=path_by_field["provider_group_tax_identity_copy_path"],
        summary=tax_identity_by_name,
    )
    prefix_metadata = _read_prefix_override_metadata(
        path_by_field["provider_set_npi_prefix_override_copy_path"],
        prefix_target=int(expectation.options["npi_prefix_target"]),
    )
    if (
        len(prefix_metadata)
        != _observe_counter(observe_by_name, "npi_prefix_override_owner_count")
        or (
            worst_provider_set_key is not None
            and (worst_provider_set_key in prefix_metadata)
            != worst_provider_set_uses_override
        )
        or (
            worst_provider_set_key is not None
            and worst_provider_set_uses_override
            and prefix_metadata.get(worst_provider_set_key)
            != (
                _observe_counter(observe_by_name, "npi_prefix_worst_member_count"),
                bytes.fromhex(str(observe_by_name["npi_prefix_worst_member_digest"])),
            )
        )
        or (
            worst_online_provider_set_key is not None
            and worst_online_provider_set_key in prefix_metadata
        )
    ):
        raise RuntimeError("V4 graph prefix metadata disagrees with diagnostics")
    database_bytes = sum(
        artifact.byte_count
        for artifact in output_artifacts
        if artifact.name != "graph_references"
    )
    selected_taxonomy_bytes = (
        pattern_taxonomy_bytes
        if selected_layout == "pattern"
        else direct_taxonomy_bytes
    )
    if database_bytes != selected_graph_bytes + selected_taxonomy_bytes:
        raise RuntimeError(
            "V4 graph compiler selected graph bytes disagree with outputs"
        )
    expected_file_names = {artifact.path.name for artifact in output_artifacts} | {
        "v4-summary.json",
        PTG2_V4_GRAPH_SCRATCH_OWNER_NAME,
    }
    if allow_checkpoint:
        expected_file_names.add(PTG2_V4_GRAPH_CHECKPOINT_NAME)
    if {path.name for path in output_directory.iterdir()} != expected_file_names:
        raise RuntimeError("V4 graph compiler output directory has unexpected files")
    return V4GraphCompilationResult(
        scratch_directory=output_directory,
        summary_path=path_by_field["summary_path"],
        block_copy_path=path_by_field["block_copy_path"],
        reference_manifest_path=path_by_field["reference_manifest_path"],
        group_copy_path=path_by_field["group_copy_path"],
        component_copy_path=path_by_field["component_copy_path"],
        npi_copy_path=path_by_field["npi_copy_path"],
        provider_set_audit_npi_copy_path=path_by_field[
            "provider_set_audit_npi_copy_path"
        ],
        provider_set_npi_prefix_override_copy_path=path_by_field[
            "provider_set_npi_prefix_override_copy_path"
        ],
        provider_tax_identity_copy_path=path_by_field[
            "provider_tax_identity_copy_path"
        ],
        provider_group_tax_identity_copy_path=path_by_field[
            "provider_group_tax_identity_copy_path"
        ],
        pattern_copy_path=pattern_copy_path,
        inferred_taxonomy_copy_path=path_by_field["inferred_taxonomy_copy_path"],
        selected_layout=selected_layout,
        direct_complete_encoded_bytes=direct_bytes,
        pattern_complete_encoded_bytes=pattern_bytes,
        selected_encoded_bytes=selected_bytes,
        block_count=block_count,
        relation_summaries=tuple(relations),
        heavy_bitmaps=tuple(heavy_bitmaps),
        observe=observe_by_name,
        output_artifacts=tuple(output_artifacts),
        resource_admission=resource_admission_by_name,
        checkpoint_reused=False,
        summary=summary,
    )


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=2.0)
        return
    except TimeoutError:
        if process.returncode is not None:
            return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        process.kill()
    await process.wait()


def _source_shard_id(raw_entry: Mapping[str, Any]) -> str:
    return str(
        raw_entry.get("source_shard_id") or raw_entry.get("shard_id") or ""
    ).strip()


class _AuthenticatedReadStream:
    """Hash every byte consumed from one pinned reciprocal descriptor."""

    def __init__(self, source: Any) -> None:
        self._source = source
        self._digest = hashlib.sha256()
        self.byte_count = 0

    def read(self, size: int = -1) -> bytes:
        """Read and authenticate one sequential chunk."""

        chunk = self._source.read(size)
        self._digest.update(chunk)
        self.byte_count += len(chunk)
        return chunk

    def drain(self) -> None:
        """Authenticate the unread membership payload without retaining it."""

        while self.read(1024 * 1024):
            continue

    def hexdigest(self) -> str:
        """Return the digest of all bytes read so far."""

        return self._digest.hexdigest()


def _is_same_file_identity(
    first: os.stat_result,
    second: os.stat_result,
) -> bool:
    """Return whether two observations identify unchanged regular bytes."""

    return (
        stat.S_ISREG(first.st_mode)
        and stat.S_ISREG(second.st_mode)
        and first.st_dev == second.st_dev
        and first.st_ino == second.st_ino
        and first.st_size == second.st_size
        and first.st_mtime_ns == second.st_mtime_ns
    )


def _open_reciprocal_descriptor(path: Path) -> int:
    """Open one reciprocal input without following a replacement symlink."""

    return os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
    )


@contextlib.contextmanager
def _open_authenticated_reciprocal(
    reciprocal: Mapping[str, Any],
):
    """Hold and authenticate one dense reciprocal descriptor through extraction."""

    reciprocal_path = Path(str(reciprocal["path"]))
    reciprocal_contract = reciprocal["metadata"]
    expected_bytes = int(reciprocal_contract["byte_count"])
    expected_sha256 = _strict_sha256(
        reciprocal_contract["sha256"],
        label="provider NPI reciprocal sha256",
    )
    try:
        path_metadata = reciprocal_path.lstat()
        descriptor = _open_reciprocal_descriptor(reciprocal_path)
    except OSError as exc:
        raise RuntimeError(
            "V4 provider NPI scope reciprocal graph is unavailable"
        ) from exc
    reciprocal_file = os.fdopen(descriptor, "rb")
    try:
        opened_metadata = os.fstat(reciprocal_file.fileno())
        if (
            not _is_same_file_identity(path_metadata, opened_metadata)
            or opened_metadata.st_size != expected_bytes
        ):
            raise RuntimeError(
                "V4 provider NPI scope reciprocal graph changed before extraction"
            )
        authenticated_stream = _AuthenticatedReadStream(reciprocal_file)
        yield authenticated_stream
        authenticated_stream.drain()
        final_metadata = os.fstat(reciprocal_file.fileno())
        try:
            current_metadata = reciprocal_path.lstat()
        except OSError as exc:
            raise RuntimeError(
                "V4 provider NPI scope reciprocal graph changed during extraction"
            ) from exc
        if not _is_same_file_identity(
            opened_metadata, final_metadata
        ) or not _is_same_file_identity(opened_metadata, current_metadata):
            raise RuntimeError(
                "V4 provider NPI scope reciprocal graph changed during extraction"
            )
        if (
            authenticated_stream.byte_count != expected_bytes
            or authenticated_stream.hexdigest() != expected_sha256
        ):
            raise RuntimeError(
                "V4 provider NPI scope reciprocal graph changed before extraction"
            )
    finally:
        reciprocal_file.close()


def _write_npi_scope_from_reciprocal(
    scope_path: Path,
    *,
    reciprocal: Mapping[str, Any],
) -> None:
    """Rebuild one temporary source scope from its authenticated dense owner index."""

    descriptor = reciprocal["metadata"]
    if (
        descriptor.get("record_format") != PTG2_V4_DENSE_MEMBERSHIP_FORMAT
        or descriptor.get("member_global_count") is None
    ):
        raise RuntimeError(
            "V4 provider NPI scope reciprocal graph must use the dense format"
        )
    owner_count = int(descriptor["owner_count"])
    member_count = int(descriptor["member_count"])
    global_count = int(descriptor["member_global_count"])
    partial_path = scope_path.with_suffix(scope_path.suffix + ".partial")
    if os.path.lexists(scope_path) or os.path.lexists(partial_path):
        raise RuntimeError("V4 provider NPI source-scope output already exists")
    is_partial_owned = False
    is_scope_owned = False
    try:
        with _open_authenticated_reciprocal(
            reciprocal
        ) as reciprocal_file, partial_path.open("xb") as scope_file:
            is_partial_owned = True
            _validate_reciprocal_header(
                reciprocal_file,
                owner_count=owner_count,
                global_count=global_count,
            )
            _write_npi_scope_rows(
                reciprocal_file,
                scope_file,
                owner_count=owner_count,
                member_count=member_count,
            )
            scope_file.flush()
            os.fsync(scope_file.fileno())
        os.link(partial_path, scope_path, follow_symlinks=False)
        is_scope_owned = True
        partial_path.unlink()
        is_partial_owned = False
    except BaseException:
        if is_partial_owned:
            partial_path.unlink(missing_ok=True)
        if is_scope_owned:
            scope_path.unlink(missing_ok=True)
        raise


def _validate_reciprocal_header(
    reciprocal_file: Any,
    *,
    owner_count: int,
    global_count: int,
) -> None:
    header = reciprocal_file.read(28)
    if (
        len(header) != 28
        or header[:8] != b"PTG2MNDS"
        or int.from_bytes(header[8:12], "little") != 1
        or int.from_bytes(header[12:20], "little") != owner_count
        or int.from_bytes(header[20:28], "little") != global_count
    ):
        raise RuntimeError("V4 provider NPI scope reciprocal header changed")


def _write_npi_scope_rows(
    reciprocal_file: Any,
    scope_file: Any,
    *,
    owner_count: int,
    member_count: int,
) -> None:
    scope_file.write(_PG_COPY_HEADER)
    previous_npi = 0
    expected_offset = 0
    remaining_owners = owner_count
    zero_owner = bytes(8)
    owner_head = struct.Struct(">8sQ")
    owner_tail = struct.Struct("<QI")
    scope_row = struct.Struct(">hIq")
    while remaining_owners:
        chunk_owner_count = min(4_096, remaining_owners)
        owners = reciprocal_file.read(chunk_owner_count * 28)
        if len(owners) != chunk_owner_count * 28:
            raise RuntimeError("V4 provider NPI scope reciprocal index is truncated")
        scope_rows = bytearray(chunk_owner_count * scope_row.size)
        for owner_index in range(chunk_owner_count):
            owner_offset = owner_index * 28
            owner_prefix, npi = owner_head.unpack_from(owners, owner_offset)
            source_member_offset, owner_members = owner_tail.unpack_from(
                owners, owner_offset + owner_head.size
            )
            if (
                owner_prefix != zero_owner
                or not 1_000_000_000 <= npi <= 9_999_999_999
                or npi <= previous_npi
                or source_member_offset != expected_offset
                or owner_members <= 0
            ):
                raise RuntimeError("V4 provider NPI scope reciprocal index changed")
            scope_row.pack_into(scope_rows, owner_index * scope_row.size, 1, 8, npi)
            expected_offset += owner_members
            previous_npi = npi
        scope_file.write(scope_rows)
        remaining_owners -= chunk_owner_count
    if expected_offset != member_count:
        raise RuntimeError("V4 provider NPI scope reciprocal member count changed")
    scope_file.write(struct.pack(">h", -1))


def _npi_scope_shards(
    entries: Iterable[Mapping[str, Any]],
) -> dict[str, dict[str, dict[str, Any]]]:
    scope_by_shard: dict[str, dict[str, dict[str, Any]]] = {}
    for entry in entries:
        factor_name = str(entry.get("name") or entry.get("kind") or "").strip()
        if factor_name not in {"provider_npi_group", "provider_npi_scope"}:
            continue
        shard_id = _source_shard_id(entry)
        if not shard_id:
            raise RuntimeError(f"V4 graph factor {factor_name!r} lacks a shard ID")
        shard_by_factor = scope_by_shard.setdefault(shard_id, {})
        if factor_name in shard_by_factor:
            raise RuntimeError(
                f"V4 graph shard {shard_id!r} repeats factor " f"{factor_name!r}"
            )
        shard_by_factor[factor_name] = dict(entry)
    expected_factors = {"provider_npi_group", "provider_npi_scope"}
    if not scope_by_shard or any(
        set(shard_by_factor) != expected_factors
        for shard_by_factor in scope_by_shard.values()
    ):
        raise RuntimeError("V4 provider NPI source-scope bundle is incomplete")
    return scope_by_shard


def _rebuilt_npi_scopes(
    scope_by_shard: Mapping[str, Mapping[str, Mapping[str, Any]]],
    *,
    scratch_directory: Path,
) -> dict[str, dict[str, Any]]:
    rebuilt_by_shard: dict[str, dict[str, Any]] = {}
    for index, (shard_id, shard_by_factor) in enumerate(sorted(scope_by_shard.items())):
        reciprocal, _byte_count = _artifact_manifest(
            shard_by_factor["provider_npi_group"]
        )
        scope_path = scratch_directory / f"scope-{index:06d}.copy"
        _write_npi_scope_from_reciprocal(
            scope_path,
            reciprocal=reciprocal,
        )
        scope_by_field = {
            **shard_by_factor["provider_npi_scope"],
            "path": str(scope_path),
        }
        _npi_scope_artifact_manifest(
            scope_by_field,
            reciprocal=reciprocal,
            shard_id=shard_id,
        )
        rebuilt_by_shard[shard_id] = scope_by_field
    return rebuilt_by_shard


def _normalized_source_npi_scope_entries(
    graph_artifact_entries: Iterable[Mapping[str, Any]],
    *,
    scratch_directory: Path,
) -> tuple[Mapping[str, Any], ...]:
    """Regenerate deterministic source scopes and authenticate their scanner bindings."""

    entries = tuple(dict(entry) for entry in graph_artifact_entries)
    scope_by_shard = _npi_scope_shards(entries)
    try:
        scratch_directory.mkdir(mode=0o700)
    except FileExistsError as exc:
        raise RuntimeError(
            "V4 provider NPI source-scope scratch already exists"
        ) from exc
    try:
        rebuilt_by_shard = _rebuilt_npi_scopes(
            scope_by_shard,
            scratch_directory=scratch_directory,
        )
    except BaseException:
        shutil.rmtree(scratch_directory, ignore_errors=True)
        raise
    return tuple(
        (
            rebuilt_by_shard[_source_shard_id(entry)]
            if str(entry.get("name") or entry.get("kind") or "").strip()
            == "provider_npi_scope"
            else entry
        )
        for entry in entries
    )


@dataclass(frozen=True)
class _NpiScopeRunFiles:
    """Run-owned manifest and bounded subprocess output paths."""

    manifest_path: Path
    stdout_path: Path
    stderr_path: Path

    def cleanup(self) -> None:
        """Remove only these run-owned metadata files."""

        self.manifest_path.unlink(missing_ok=True)
        self.stdout_path.unlink(missing_ok=True)
        self.stderr_path.unlink(missing_ok=True)


def _npi_scope_run_files(output_path: Path) -> _NpiScopeRunFiles:
    scratch_prefix = (
        output_path.parent / f".{output_path.name}.prepass-{uuid.uuid4().hex}"
    )
    return _NpiScopeRunFiles(
        manifest_path=scratch_prefix.with_suffix(".manifest.json"),
        stdout_path=scratch_prefix.with_suffix(".stdout.json"),
        stderr_path=scratch_prefix.with_suffix(".stderr.log"),
    )


def _write_npi_scope_manifest(
    run_files: _NpiScopeRunFiles,
    *,
    shards: Iterable[Mapping[str, Any]],
    output_path: Path,
) -> None:
    manifest_by_field = {
        "shards": tuple(shards),
        "output_path": str(output_path),
    }
    with run_files.manifest_path.open("xb") as manifest_file:
        manifest_file.write(_canonical_json_bytes(manifest_by_field))
        manifest_file.write(b"\n")
        manifest_file.flush()
        os.fsync(manifest_file.fileno())


async def _wait_npi_scope_process(
    process: asyncio.subprocess.Process,
    *,
    started_at: float,
) -> int:
    """Wait with live heartbeats while preserving cancellation."""

    while True:
        try:
            return await asyncio.wait_for(
                asyncio.shield(process.wait()),
                timeout=PTG2_V4_GRAPH_HEARTBEAT_SECONDS,
            )
        except TimeoutError:
            await _emit_npi_scope_progress(
                done=0,
                total=1,
                stage_pct=50.0,
                elapsed_seconds=time.monotonic() - started_at,
                message="extracting authenticated V4 NPI scope; active",
            )


async def _run_npi_scope_process(
    binary_path: Path,
    run_files: _NpiScopeRunFiles,
    *,
    started_at: float,
) -> None:
    """Run one scope prepass and terminate it on every interrupted path."""

    process: asyncio.subprocess.Process | None = None
    await _emit_npi_scope_progress(
        done=0,
        total=1,
        stage_pct=0.0,
        elapsed_seconds=0.0,
        message="extracting authenticated V4 NPI scope; starting",
    )
    try:
        with run_files.stdout_path.open(
            "xb"
        ) as stdout_file, run_files.stderr_path.open("xb") as stderr_file:
            process = await asyncio.create_subprocess_exec(
                str(binary_path),
                "--extract-npi-scope",
                str(run_files.manifest_path),
                stdout=stdout_file,
                stderr=stderr_file,
                start_new_session=True,
            )
            return_code = await _wait_npi_scope_process(
                process,
                started_at=started_at,
            )
    except BaseException:
        if process is not None and process.returncode is None:
            await _terminate_process(process)
        raise
    if return_code != 0:
        raise RuntimeError(
            "V4 NPI scope preparation failed: "
            + _read_error_tail(run_files.stderr_path)
        )


def _read_npi_scope_summary(
    summary_path: Path,
    *,
    output_path: Path,
) -> dict[str, Any]:
    summary_by_field = _load_json_bytes(
        _read_bounded(
            summary_path,
            PTG2_V4_GRAPH_CHECKPOINT_MAX_BYTES,
            label="NPI scope summary",
        ),
        label="NPI scope summary",
    )
    if not isinstance(summary_by_field, Mapping):
        raise RuntimeError("V4 NPI scope summary is not an object")
    scope_by_field = _validated_npi_scope_input(summary_by_field)
    if scope_by_field["output_path"] != str(output_path):
        raise RuntimeError("V4 NPI scope output identity changed")
    return scope_by_field


async def _completed_npi_scope_preparation(
    run_files: _NpiScopeRunFiles,
    *,
    output_path: Path,
    graph_artifact_entries: tuple[dict[str, Any], ...],
    source_scope_directory: Path,
    started_at: float,
) -> V4GraphNpiScopePreparation:
    """Authenticate and expose one completed NPI-scope extraction."""

    scope_by_field = _read_npi_scope_summary(
        run_files.stdout_path,
        output_path=output_path,
    )
    row_count = int(scope_by_field["row_count"])
    await _emit_npi_scope_progress(
        done=row_count,
        total=row_count,
        stage_pct=100.0,
        elapsed_seconds=time.monotonic() - started_at,
        message=(
            "authenticated V4 NPI scope extraction complete; "
            f"rows={row_count}, "
            f"bytes={int(scope_by_field['output_byte_count'])}"
        ),
    )
    return V4GraphNpiScopePreparation(
        copy_path=output_path,
        manifest=scope_by_field,
        graph_artifact_entries=graph_artifact_entries,
        source_scope_directory=source_scope_directory,
    )


async def prepare_v4_npi_scope(
    *,
    graph_artifact_entries: Iterable[Mapping[str, Any]],
    output_path: str | Path,
    binary_path: str | Path | None = None,
) -> V4GraphNpiScopePreparation:
    """Extract and authenticate the exact dense NPI catalog before DB lookup."""

    binary = (
        Path(binary_path).resolve()
        if binary_path is not None
        else _resolve_v4_graph_compiler_binary()
    )
    if binary is None or not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError("V4 NPI scope preparation requires ptg2_provider_graph_v4")
    output, output_exists = _private_scratch_child(
        output_path,
        label="NPI scope output",
    )
    if output_exists:
        raise RuntimeError("V4 NPI scope output path is unavailable")
    source_scope_directory = output.parent / f".{output.name}.source-scopes"
    normalized_artifacts = _normalized_source_npi_scope_entries(
        graph_artifact_entries,
        scratch_directory=source_scope_directory,
    )
    shards, _input_factor_bytes = _build_v4_graph_manifest_shards(normalized_artifacts)
    run_files = _npi_scope_run_files(output)
    started_at = time.monotonic()
    try:
        _write_npi_scope_manifest(
            run_files,
            shards=shards,
            output_path=output,
        )
        await _run_npi_scope_process(
            binary,
            run_files,
            started_at=started_at,
        )
        return await _completed_npi_scope_preparation(
            run_files,
            output_path=output,
            graph_artifact_entries=normalized_artifacts,
            source_scope_directory=source_scope_directory,
            started_at=started_at,
        )
    except BaseException:
        output.unlink(missing_ok=True)
        shutil.rmtree(source_scope_directory, ignore_errors=True)
        raise
    finally:
        run_files.cleanup()


prepare_provider_graph_v4_npi_scope_rust = prepare_v4_npi_scope


async def compile_provider_graph_v4_rust(
    *,
    graph_artifact_entries: Iterable[Mapping[str, Any]],
    provider_set_key_map_path: str | Path,
    npi_scope: V4GraphNpiScopePreparation,
    inferred_taxonomy: Mapping[str, Any],
    output_directory: str | Path,
    options: Mapping[str, int] | None = None,
    binary_path: str | Path | None = None,
) -> V4GraphCompilationResult:
    """Run the standalone compiler and authenticate every returned artifact."""

    binary = (
        Path(binary_path).resolve()
        if binary_path is not None
        else _resolve_v4_graph_compiler_binary()
    )
    if binary is None or not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError(
            "V4 graph compilation requires ptg2_provider_graph_v4; build it with "
            "`cargo build --release --bin ptg2_provider_graph_v4 --manifest-path "
            "support/ptg2_scanner/Cargo.toml`"
        )
    output, output_exists = _private_scratch_child(
        output_directory,
        label="graph compiler output",
    )
    if output_exists:
        try:
            output_metadata = output.lstat()
        except OSError as exc:
            raise RuntimeError("V4 graph compiler output changed") from exc
        if not stat.S_ISDIR(output_metadata.st_mode):
            raise RuntimeError(
                f"V4 graph compiler output is not a safe directory: {output}"
            )
        _validate_compiler_output_owner(output)
    effective_options = _effective_compiler_options(options)
    manifest, expected_input_bytes = build_v4_graph_compiler_manifest(
        graph_artifact_entries=graph_artifact_entries,
        provider_set_key_map_path=provider_set_key_map_path,
        npi_scope=npi_scope.manifest,
        inferred_taxonomy=inferred_taxonomy,
        output_directory=output,
        options=effective_options,
    )
    expected_factor_edges, expected_factor_owners = _manifest_factor_counts(manifest)
    expected_tax_identity = _tax_manifest_expectation(manifest)
    manifest_bytes = _canonical_json_bytes(manifest)
    provider_map = Path(manifest["provider_set_key_map_path"])
    binding_sha256, provider_map_sha256 = _checkpoint_binding(
        manifest_bytes=manifest_bytes,
        provider_set_key_map_path=provider_map,
    )
    checkpoint_path = output / PTG2_V4_GRAPH_CHECKPOINT_NAME

    if output_exists:
        try:
            checkpoint = _load_json_bytes(
                _read_bounded(
                    checkpoint_path,
                    PTG2_V4_GRAPH_CHECKPOINT_MAX_BYTES,
                    label="completion checkpoint",
                ),
                label="completion checkpoint",
            )
            summary_path = output / "v4-summary.json"
            file_summary = _load_json_bytes(
                _read_bounded(
                    summary_path,
                    PTG2_V4_GRAPH_SUMMARY_MAX_BYTES,
                    label="summary file",
                ),
                label="summary file",
            )
            reused = _validate_compiler_summary(
                file_summary,
                output_directory=output,
                expectation=_CompilerSummaryExpectation(
                    input_bytes=expected_input_bytes,
                    factor_edges=expected_factor_edges,
                    factor_owners=expected_factor_owners,
                    options=effective_options,
                    tax_identity=expected_tax_identity,
                    taxonomy_rule_count=len(manifest["inferred_taxonomy"]["rules"]),
                ),
                allow_checkpoint=True,
            )
            _validate_checkpoint(
                checkpoint,
                validated_result=reused,
                binding_sha256=binding_sha256,
                provider_map_sha256=provider_map_sha256,
                options=effective_options,
                input_contracts=manifest,
            )
            reused = replace(reused, checkpoint_reused=True)
            await _emit_compile_progress(
                pct=95.0,
                stage_pct=100.0,
                done=expected_factor_edges,
                total=expected_factor_edges,
                unit="factor_edges",
                phase_pct=100.0,
                elapsed_seconds=0.0,
                input_bytes=expected_input_bytes,
                input_factor_edges=expected_factor_edges,
                input_factor_owners=expected_factor_owners,
                checkpoint_reused=True,
                selected_layout=reused.selected_layout,
                block_count=reused.block_count,
                selected_encoded_bytes=reused.selected_encoded_bytes,
                message=(
                    "provider graph compile reused complete checkpoint; "
                    f"layout={reused.selected_layout}, blocks={reused.block_count}, "
                    f"bytes={reused.selected_encoded_bytes}"
                ),
            )
            return reused
        except (OSError, RuntimeError, KeyError, TypeError, ValueError):
            shutil.rmtree(output)
            _create_compiler_output(output)
    else:
        _create_compiler_output(output)

    scratch_token = uuid.uuid4().hex
    manifest_path = output.parent / f".{output.name}.{scratch_token}.v4-manifest.json"
    stdout_path = output.parent / f".{output.name}.{scratch_token}.v4-stdout.json"
    stderr_path = output.parent / f".{output.name}.{scratch_token}.v4-stderr.log"
    with manifest_path.open("xb") as manifest_output:
        manifest_output.write(manifest_bytes)
        manifest_output.flush()
        os.fsync(manifest_output.fileno())
    process: asyncio.subprocess.Process | None = None
    stderr_task: asyncio.Task[None] | None = None
    progress_state = _CompilerProgressState()
    progress_emit_lock = asyncio.Lock()
    started_at = time.monotonic()
    try:
        await _emit_compile_progress(
            pct=92.0,
            stage_pct=0.0,
            done=0,
            total=expected_factor_edges,
            unit="factor_edges",
            phase_pct=0.0,
            elapsed_seconds=0.0,
            input_bytes=expected_input_bytes,
            input_factor_edges=expected_factor_edges,
            input_factor_owners=expected_factor_owners,
            checkpoint_reused=False,
            message=(
                "compiling provider graph factors; "
                f"edges={expected_factor_edges}, bytes={expected_input_bytes}"
            ),
        )
        with stdout_path.open("xb") as stdout_output, stderr_path.open(
            "xb"
        ) as stderr_output:
            process = await asyncio.create_subprocess_exec(
                str(binary),
                str(manifest_path),
                stdout=stdout_output,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
            if process.stderr is None:
                raise RuntimeError(
                    "V4 graph compiler did not expose its progress stream"
                )
            stderr_task = asyncio.create_task(
                _consume_compiler_stderr(
                    process.stderr,
                    stderr_output,
                    state=progress_state,
                    emit_lock=progress_emit_lock,
                    input_bytes=expected_input_bytes,
                    input_factor_edges=expected_factor_edges,
                    input_factor_owners=expected_factor_owners,
                )
            )
            try:
                while True:
                    try:
                        return_code = await asyncio.wait_for(
                            asyncio.shield(process.wait()),
                            timeout=PTG2_V4_GRAPH_HEARTBEAT_SECONDS,
                        )
                        break
                    except TimeoutError:
                        elapsed = time.monotonic() - started_at
                        heartbeat_state = replace(
                            progress_state,
                            elapsed_ms=max(
                                progress_state.elapsed_ms,
                                int(elapsed * 1000),
                            ),
                        )
                        if not progress_state.seq:
                            heartbeat_state = replace(
                                heartbeat_state,
                                done=0,
                                total=expected_factor_edges,
                                unit="factor_edges",
                            )
                        await _publish_compiler_progress_state(
                            heartbeat_state,
                            emit_lock=progress_emit_lock,
                            input_bytes=expected_input_bytes,
                            input_factor_edges=expected_factor_edges,
                            input_factor_owners=expected_factor_owners,
                            checkpoint_reused=False,
                            heartbeat=True,
                        )
            except asyncio.CancelledError:
                await _terminate_process(process)
                if stderr_task is not None:
                    try:
                        await asyncio.wait_for(stderr_task, timeout=1.0)
                    except (TimeoutError, asyncio.CancelledError):
                        stderr_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await stderr_task
                raise
            if stderr_task is not None:
                await stderr_task
        if return_code != 0:
            error_tail = _read_error_tail(stderr_path)
            error_type = (
                V4GraphResourceAdmissionError
                if "resource_admission:" in error_tail
                else RuntimeError
            )
            raise error_type(
                f"V4 graph compiler exited with status {return_code}: {error_tail}"
            )
        if not progress_state.terminal:
            raise RuntimeError(
                "V4 graph compiler exited without a terminal progress event"
            )
        stdout_summary = _load_json_bytes(
            _read_bounded(
                stdout_path,
                PTG2_V4_GRAPH_SUMMARY_MAX_BYTES,
                label="stdout summary",
            ),
            label="stdout summary",
        )
        stdout_summary_sha256 = hashlib.sha256(
            _canonical_json_bytes(stdout_summary)
        ).digest()
        del stdout_summary
        summary_path = output / "v4-summary.json"
        if not summary_path.is_file() or summary_path.is_symlink():
            raise RuntimeError("V4 graph compiler did not publish its summary")
        file_summary = _load_json_bytes(
            _read_bounded(
                summary_path,
                PTG2_V4_GRAPH_SUMMARY_MAX_BYTES,
                label="summary file",
            ),
            label="summary file",
        )
        if (
            stdout_summary_sha256
            != hashlib.sha256(_canonical_json_bytes(file_summary)).digest()
        ):
            raise RuntimeError("V4 graph compiler stdout and summary file disagree")
        compilation_result = _validate_compiler_summary(
            file_summary,
            output_directory=output,
            expectation=_CompilerSummaryExpectation(
                input_bytes=expected_input_bytes,
                factor_edges=expected_factor_edges,
                factor_owners=expected_factor_owners,
                options=effective_options,
                tax_identity=expected_tax_identity,
                taxonomy_rule_count=len(manifest["inferred_taxonomy"]["rules"]),
            ),
        )
        checkpoint = _checkpoint_payload(
            compilation=compilation_result,
            binding_sha256=binding_sha256,
            provider_map_sha256=provider_map_sha256,
            options=effective_options,
            input_contracts=manifest,
        )
        _write_checkpoint(checkpoint_path, checkpoint)
        elapsed = time.monotonic() - started_at
        await _emit_compile_progress(
            pct=95.0,
            stage_pct=100.0,
            done=progress_state.done,
            total=progress_state.total,
            unit=progress_state.unit,
            phase_pct=100.0,
            elapsed_seconds=elapsed,
            input_bytes=expected_input_bytes,
            input_factor_edges=expected_factor_edges,
            input_factor_owners=expected_factor_owners,
            checkpoint_reused=False,
            selected_layout=compilation_result.selected_layout,
            block_count=compilation_result.block_count,
            selected_encoded_bytes=compilation_result.selected_encoded_bytes,
            compiler_counters=dict(compilation_result.observe),
            message=(
                "provider graph compile complete; "
                f"layout={compilation_result.selected_layout}, "
                f"blocks={compilation_result.block_count}, "
                f"bytes={compilation_result.selected_encoded_bytes}, "
                f"elapsed={elapsed:.1f}s"
            ),
        )
        return compilation_result
    except BaseException:
        if process is not None and process.returncode is None:
            await _terminate_process(process)
        if stderr_task is not None and not stderr_task.done():
            try:
                await asyncio.wait_for(stderr_task, timeout=1.0)
            except (TimeoutError, asyncio.CancelledError):
                stderr_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await stderr_task
        shutil.rmtree(output, ignore_errors=True)
        raise
    finally:
        manifest_path.unlink(missing_ok=True)
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)


__all__ = [
    "PTG2_V4_GRAPH_COMPILER_BIN_ENV",
    "V4GraphCompilationResult",
    "V4GraphNpiScopePreparation",
    "V4GraphOutputArtifact",
    "V4GraphResourceAdmissionError",
    "build_v4_graph_compiler_manifest",
    "compile_provider_graph_v4_rust",
    "prepare_provider_graph_v4_npi_scope_rust",
]
