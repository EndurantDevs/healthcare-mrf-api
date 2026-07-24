# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validated subprocess bridge for the adaptive Rust V4 graph compiler."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import shutil
import signal
import struct
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Iterable, Mapping

from process.ptg_parts.live_progress import current_live_progress_context, write_live_progress


PTG2_V4_GRAPH_COMPILER_BIN_ENV = "HLTHPRT_PTG2_PROVIDER_GRAPH_V4_BIN"
PTG2_V4_GRAPH_SUMMARY_FORMAT = "ptg2_provider_graph_v4_factor_adaptive_v1"
# Dense-owner metadata is bounded separately from the multi-gigabyte graph model.
PTG2_V4_GRAPH_SUMMARY_MAX_BYTES = 256 * 1024 * 1024
PTG2_V4_GRAPH_ERROR_TAIL_BYTES = 8 * 1024
PTG2_V4_SHARED_FORMAT_VERSION = 2
PTG2_V4_GRAPH_CHECKPOINT_FORMAT = "ptg2_provider_graph_v4_checkpoint_v1"
PTG2_V4_GRAPH_CHECKPOINT_NAME = "v4-complete.json"
PTG2_V4_GRAPH_CHECKPOINT_MAX_BYTES = 512 * 1024
PTG2_V4_GRAPH_HEARTBEAT_SECONDS = 4.0
PTG2_V4_PROGRESS_PREFIX = b"PTG2_V4_PROGRESS\t"
PTG2_V4_PROGRESS_VERSION = 1
PTG2_V4_PROGRESS_MAX_LINE_BYTES = 16 * 1024
PTG2_V4_GRAPH_MAX_MODEL_BYTES_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_ESTIMATED_MODEL_BYTES"
)
PTG2_V4_GRAPH_MAX_FACTOR_EDGES_ENV = "HLTHPRT_PTG2_V4_GRAPH_MAX_FACTOR_EDGES"
PTG2_V4_GRAPH_MEMBER_PAGE_BYTES_ENV = "HLTHPRT_PTG2_V4_GRAPH_MEMBER_PAGE_BYTES"
PTG2_V4_GRAPH_LOCATOR_PAGE_BYTES_ENV = "HLTHPRT_PTG2_V4_GRAPH_LOCATOR_PAGE_BYTES"
PTG2_V4_GRAPH_HEAVY_OWNER_THRESHOLD_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_HEAVY_OWNER_MEMBER_THRESHOLD"
)
PTG2_V4_GRAPH_HEAVY_MIN_SAVINGS_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_HEAVY_BITMAP_MINIMUM_SAVINGS_BYTES"
)
PTG2_V4_GRAPH_MAX_SET_PATTERNS_ENV = (
    "HLTHPRT_PTG2_V4_GRAPH_MAX_SET_PATTERNS_PER_SET"
)
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
)

_PG_COPY_HEADER = b"PGCOPY\n\xff\r\n\0" + struct.pack(">II", 0, 0)
_PG_COPY_TRAILER = struct.pack(">h", -1)
_ARTIFACT_FIELD_BY_NAME = {
    "provider_set_component": "provider_set_component",
    "provider_component_group": "provider_component_group",
    "provider_group_npi": "provider_group_npi",
    "provider_npi_group": "provider_npi_group",
}
_REQUIRED_SHARD_FIELDS = frozenset(_ARTIFACT_FIELD_BY_NAME.values())
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
    pattern_copy_path: Path | None
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
            version = _strict_nonnegative_int(raw.get("version"), label="progress version")
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
        dimension: _observe_counter(
            observe, f"maximum_online_source_{dimension}_work"
        )
        for dimension in ("owner", "member", "page", "byte")
    }
    source_limits_by_dimension = {
        "owner": int(options["max_online_source_owners_per_set"]),
        "member": int(options["max_online_source_members_per_set"]),
        "page": int(options["max_online_source_pages_per_set"]),
        "byte": int(options["max_online_source_bytes_per_set"]),
    }
    has_physical_overflow = any(
        source_maxima_by_dimension[dimension]
        > source_limits_by_dimension[dimension]
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
) -> bool:
    """Return whether aggregate simulation, override, or cap evidence drifts."""

    unsafe_union_lower_bound = max(
        counts.group_unsafe_sets,
        counts.physical_unsafe_sets,
    )
    unsafe_union_upper_bound = (
        counts.group_unsafe_sets + counts.physical_unsafe_sets
    )
    return (
        unsafe_union_lower_bound > counts.simulated_sets
        or counts.override_owners < unsafe_union_lower_bound
        or counts.override_owners > unsafe_union_upper_bound
        or counts.simulated_sets != counts.provider_sets
        or counts.override_owners
        > int(options["max_npi_prefix_override_owners"])
        or counts.override_members
        > counts.override_owners * int(options["npi_prefix_target"])
        or counts.override_raw_bytes != counts.override_members * 4
        or counts.override_raw_bytes
        > int(options["max_npi_prefix_override_bytes"])
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
        dimension: _observe_counter(
            observe, f"{field_prefix}_{dimension}_work"
        )
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
        uses_override=observe.get(
            "npi_prefix_worst_provider_set_uses_override"
        ),
        uses_component=observe.get(
            "npi_prefix_worst_uses_component_fallback"
        ),
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
            raise RuntimeError("V4 graph empty worst-owner diagnostics are inconsistent")
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
        is_exact=observe.get(
            "npi_prefix_worst_online_groups_to_target_exact"
        ),
        uses_component=observe.get(
            "npi_prefix_worst_online_uses_component_fallback"
        ),
        member_count=_observe_counter(
            observe,
            "npi_prefix_worst_online_member_count",
        ),
        member_digest=observe.get(
            "npi_prefix_worst_online_member_digest"
        ),
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
            raise RuntimeError("V4 graph empty worst-online diagnostics are inconsistent")
    elif (
        isinstance(online_owner.key, bool)
        or not isinstance(online_owner.key, int)
        or online_owner.groups_to_target > online_owner.group_work_bound
        or online_owner.member_count > int(options["npi_prefix_target"])
        or not isinstance(online_owner.member_digest, str)
        or online_owner.group_work_bound
        > int(options["max_online_group_keys_per_set"])
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
) -> tuple[int | None, int | None, bool]:
    """Cross-check bounded prefix totals and both recorded canary owners."""

    context = _validate_prefix_totals(observe, options)
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
    actual_pattern_visits = _observe_counter(
        observe, "group_set_expansion_edge_visits"
    )
    logical_pattern_visits = _observe_counter(
        observe, "group_set_incidence_count"
    )
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
        raise RuntimeError(f"V4 graph compiler environment {name} is not an integer") from exc
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
    return {
        name: int(effective[name])
        for name in PTG2_V4_GRAPH_ENCODING_OPTION_NAMES
    }


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


def _artifact_manifest(raw_entry: Mapping[str, Any]) -> tuple[dict[str, Any], int]:
    path_text = raw_entry.get("path")
    if not isinstance(path_text, str) or not path_text.strip():
        raise RuntimeError("V4 graph factor artifact lacks a path")
    path = Path(path_text).resolve()
    if not path.is_file() or path.is_symlink():
        raise RuntimeError(f"V4 graph factor artifact is unavailable: {path}")
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
    if byte_count <= 0 or path.stat().st_size != byte_count:
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


def build_v4_graph_compiler_manifest(
    *,
    graph_artifact_entries: Iterable[Mapping[str, Any]],
    provider_set_key_map_path: str | Path,
    output_directory: str | Path,
    options: Mapping[str, int] | None = None,
) -> tuple[dict[str, Any], int]:
    """Build a deterministic complete-shard manifest from scanner artifacts."""

    artifact_by_shard: dict[str, dict[str, Mapping[str, Any]]] = {}
    for raw_entry in graph_artifact_entries:
        if not isinstance(raw_entry, Mapping):
            continue
        artifact_name = str(raw_entry.get("name") or raw_entry.get("kind") or "").strip()
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
        for field_name in sorted(_REQUIRED_SHARD_FIELDS):
            artifact, artifact_bytes = _artifact_manifest(fields[field_name])
            shard_manifest_by_field[field_name] = artifact
            input_byte_count += artifact_bytes
        manifest_shards.append(shard_manifest_by_field)

    provider_map = Path(provider_set_key_map_path).resolve()
    if not provider_map.is_file() or provider_map.is_symlink() or provider_map.stat().st_size <= 0:
        raise RuntimeError("V4 graph compilation requires an authoritative provider-set map")
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
    return (
        {
            "shards": manifest_shards,
            "provider_set_key_map_path": str(provider_map),
            "output_directory": str(output),
            "options": normalized_options_by_name,
        },
        input_byte_count,
    )


def _manifest_factor_counts(manifest: Mapping[str, Any]) -> tuple[int, int]:
    edges = 0
    owners = 0
    for shard in manifest["shards"]:
        for field_name in sorted(_REQUIRED_SHARD_FIELDS):
            metadata = shard[field_name]["metadata"]
            edges += int(metadata["member_count"])
            owners += int(metadata["owner_count"])
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
    result: V4GraphCompilationResult,
    binding_sha256: str,
    provider_map_sha256: str,
    options: Mapping[str, int],
) -> dict[str, Any]:
    return {
        "format": PTG2_V4_GRAPH_CHECKPOINT_FORMAT,
        "complete": True,
        "compiler_contract": PTG2_V4_GRAPH_SUMMARY_FORMAT,
        "shared_format_version": PTG2_V4_SHARED_FORMAT_VERSION,
        "binding_sha256": binding_sha256,
        "provider_set_key_map_sha256": provider_map_sha256,
        "options": dict(sorted(options.items())),
        "summary_sha256": _sha256_file(result.summary_path),
        "selected_layout": result.selected_layout,
        "block_count": result.block_count,
        "selected_encoded_bytes": result.selected_encoded_bytes,
        "output_artifacts": [
            {
                "name": artifact.name,
                "byte_count": artifact.byte_count,
                "sha256": artifact.sha256,
                "row_count": artifact.row_count,
            }
            for artifact in result.output_artifacts
        ],
    }


def _write_checkpoint(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.unlink(missing_ok=True)
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
    if checkpoint.get("summary_sha256") != _sha256_file(
        validated_result.summary_path
    ):
        raise RuntimeError("V4 graph checkpoint changed summary bytes")
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
        "stage_id": "publishing: provider graph compile",
        "unit": "factor_edges",
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
        return candidate if candidate.is_file() and os.access(candidate, os.X_OK) else None
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


def _count_pg_binary_rows(
    path: Path, *, expected_field_count: int, validate_shared_version: bool = False
) -> int:
    row_count = 0
    with path.open("rb") as copy_file:
        if copy_file.read(len(_PG_COPY_HEADER)) != _PG_COPY_HEADER:
            raise RuntimeError(f"V4 graph compiler output has invalid COPY header: {path}")
        while True:
            field_count_bytes = copy_file.read(2)
            if len(field_count_bytes) != 2:
                raise RuntimeError(f"V4 graph compiler output truncates COPY rows: {path}")
            field_count = struct.unpack(">h", field_count_bytes)[0]
            if field_count == -1:
                if copy_file.read(1):
                    raise RuntimeError(f"V4 graph compiler output has trailing COPY bytes: {path}")
                return row_count
            if field_count != expected_field_count:
                raise RuntimeError(f"V4 graph compiler output has wrong COPY width: {path}")
            for field_index in range(field_count):
                length_bytes = copy_file.read(4)
                if len(length_bytes) != 4:
                    raise RuntimeError(f"V4 graph compiler output truncates COPY field: {path}")
                field_bytes = struct.unpack(">i", length_bytes)[0]
                if field_bytes < 0:
                    raise RuntimeError(f"V4 graph compiler output contains NULL COPY field: {path}")
                if validate_shared_version and field_index == 1:
                    if field_bytes != 2:
                        raise RuntimeError("V4 graph block has invalid format-version width")
                    version_bytes = copy_file.read(2)
                    if struct.unpack(">h", version_bytes)[0] != PTG2_V4_SHARED_FORMAT_VERSION:
                        raise RuntimeError("V4 graph block changed the shared CAS wire version")
                    continue
                if copy_file.seek(field_bytes, os.SEEK_CUR) < 0:
                    raise RuntimeError(f"V4 graph compiler output truncates COPY field: {path}")
                if copy_file.tell() > path.stat().st_size:
                    raise RuntimeError(f"V4 graph compiler output truncates COPY field: {path}")
            row_count += 1


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
                    raise RuntimeError("V4 graph prefix metadata has invalid field width")
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
    if not path.is_absolute() or path.is_symlink() or path.resolve() != expected.resolve():
        raise RuntimeError(f"V4 graph compiler has unexpected {field}")
    if not path.is_file():
        raise RuntimeError(f"V4 graph compiler output is unavailable: {path}")
    return path


def _validate_reference_manifest(path: Path, expected_rows: int) -> None:
    observed = 0
    previous_coordinate: tuple[str, int, int] | None = None
    with path.open("rb") as source:
        for line in source:
            if len(line) > 64 * 1024:
                raise RuntimeError("V4 graph reference record exceeds 64 KiB")
            value = _load_json_bytes(line, label="reference record")
            if not isinstance(value, dict):
                raise RuntimeError("V4 graph reference record is not an object")
            object_kind = value.get("object_kind")
            block_key = _strict_nonnegative_int(value.get("block_key"), label="block_key")
            fragment = _strict_nonnegative_int(
                value.get("fragment_no"), label="fragment_no"
            )
            if not isinstance(object_kind, str) or not object_kind.startswith("v4_"):
                raise RuntimeError("V4 graph reference has invalid object_kind")
            coordinate = (object_kind, block_key, fragment)
            if previous_coordinate is not None and coordinate <= previous_coordinate:
                raise RuntimeError(
                    "V4 graph references are not in strict coordinate order"
                )
            previous_coordinate = coordinate
            _strict_sha256(value.get("hash"), label="block hash")
            if value.get("codec") != "none":
                raise RuntimeError("V4 graph compiler unexpectedly compressed a graph block")
            observed += 1
    if observed != expected_rows:
        raise RuntimeError("V4 graph reference row count disagrees with summary")


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
    expected_input_bytes: int,
    expected_factor_edges: int,
    expected_options: Mapping[str, int],
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
    input_byte_count = _strict_nonnegative_int(
        summary.get("input_byte_count"), label="input_byte_count"
    )
    if input_byte_count != expected_input_bytes:
        raise RuntimeError("V4 graph compiler input byte count changed")
    _strict_sha256(summary.get("input_sha256"), label="input digest")
    block_count = _strict_nonnegative_int(summary.get("block_count"), label="block_count")
    observe_raw = summary.get("observe")
    observe_by_name = _normalize_observe_counters(observe_raw)
    max_set_patterns_per_set = _strict_nonnegative_int(
        summary.get("max_set_patterns_per_set"),
        label="max_set_patterns_per_set",
    )
    if max_set_patterns_per_set != expected_options.get(
        "max_set_patterns_per_set"
    ):
        raise RuntimeError("V4 graph pattern serving-degree limit changed")
    max_set_components_per_fallback_set = _strict_nonnegative_int(
        summary.get("max_set_components_per_fallback_set"),
        label="max_set_components_per_fallback_set",
    )
    if max_set_components_per_fallback_set != expected_options.get(
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
    ):
        observed_option = _strict_nonnegative_int(
            summary.get(option_name), label=option_name
        )
        if observed_option != expected_options.get(option_name):
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
    unsafe_pattern_component_set_count = _strict_nonnegative_int(
        observe_raw.get("unsafe_pattern_component_set_count"),
        label="observe.unsafe_pattern_component_set_count",
    )
    is_overflow_component_safe = (
        maximum_components_per_pattern_overflow_set
        <= max_set_components_per_fallback_set
    )
    has_no_unsafe_component_fallback = unsafe_pattern_component_set_count == 0
    if (
        maximum_components_per_pattern_overflow_set > maximum_components_per_set
        or unsafe_pattern_component_set_count > pattern_overflow_set_count
        or (
            maximum_patterns_per_set <= max_set_patterns_per_set
            and (
                pattern_overflow_set_count
                or maximum_components_per_pattern_overflow_set
                or unsafe_pattern_component_set_count
            )
        )
        or (
            maximum_patterns_per_set > max_set_patterns_per_set
            and not pattern_overflow_set_count
        )
        or bool(pattern_overflow_set_count)
        != bool(maximum_components_per_pattern_overflow_set)
        or is_overflow_component_safe != has_no_unsafe_component_fallback
    ):
        raise RuntimeError("V4 graph component fallback diagnostics are inconsistent")
    is_expected_pattern_eligible = unsafe_pattern_component_set_count == 0
    if (
        summary.get("pattern_layout_serving_degree_eligible")
        is not is_expected_pattern_eligible
    ):
        raise RuntimeError("V4 graph pattern serving-degree decision changed")
    (
        worst_provider_set_key,
        worst_online_provider_set_key,
        worst_provider_set_uses_override,
    ) = _validate_prefix_diagnostics(observe_by_name, expected_options)
    _validate_npi_pattern_diagnostics(observe_by_name)
    expected_layout = (
        "pattern"
        if pattern_bytes < direct_bytes and is_expected_pattern_eligible
        else "direct"
    )
    selected_base_bytes = (
        pattern_bytes if expected_layout == "pattern" else direct_bytes
    )
    if selected_layout != expected_layout or selected_bytes > selected_base_bytes:
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
        "estimated_peak_bytes",
        "max_estimated_model_bytes",
        "max_factor_edges",
    ):
        resource_admission_by_name[name] = _strict_nonnegative_int(
            resource_raw.get(name), label=f"resource_admission.{name}"
        )
    if resource_admission_by_name["input_factor_bytes"] != expected_input_bytes:
        raise RuntimeError("V4 graph resource input byte count changed")
    if resource_admission_by_name["factor_edge_count"] != expected_factor_edges:
        raise RuntimeError("V4 graph resource factor edge count changed")
    if resource_admission_by_name[
        "max_estimated_model_bytes"
    ] != expected_options.get(
        "max_estimated_model_bytes"
    ):
        raise RuntimeError("V4 graph resource model byte limit changed")
    if resource_admission_by_name["max_factor_edges"] != expected_options.get(
        "max_factor_edges"
    ):
        raise RuntimeError("V4 graph resource factor edge limit changed")
    if not isinstance(resource_raw.get("formula"), str):
        raise RuntimeError("V4 graph resource admission formula is missing")
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
        or override_owner_count != _observe_counter(observe_by_name, "provider_set_count")
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
        raise RuntimeError("V4 direct layout unexpectedly published a pattern dictionary")
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
        if not isinstance(name, str) or name not in _OUTPUT_FILE_BY_NAME or name in observed_names:
            raise RuntimeError("V4 graph compiler output artifact has invalid name")
        observed_names.add(name)
        filename, field_count = _OUTPUT_FILE_BY_NAME[name]
        path = _summary_path(raw, "path", output_directory / filename)
        byte_count = _strict_nonnegative_int(raw.get("byte_count"), label="output bytes")
        row_count = _strict_nonnegative_int(raw.get("row_count"), label="output rows")
        digest = _strict_sha256(raw.get("sha256"), label="output sha256")
        if path.stat().st_size != byte_count or _sha256_file(path) != digest:
            raise RuntimeError(f"V4 graph compiler output authentication failed: {path}")
        if field_count is not None:
            observed_rows = _count_pg_binary_rows(
                path,
                expected_field_count=field_count,
                validate_shared_version=name == "graph_blocks",
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
    }
    if selected_layout == "pattern":
        expected_artifact_names.add("patterns")
    if observed_names != expected_artifact_names:
        raise RuntimeError("V4 graph compiler output artifact set is incomplete")
    artifact_by_name = {artifact.name: artifact for artifact in output_artifacts}
    if artifact_by_name["graph_blocks"].row_count != block_count:
        raise RuntimeError("V4 graph block row count disagrees with summary")
    if artifact_by_name["graph_references"].row_count != block_count:
        raise RuntimeError("V4 graph reference row count disagrees with summary")
    _validate_reference_manifest(
        path_by_field["reference_manifest_path"], block_count
    )
    expected_dictionary_rows = {
        "provider_groups": observe_by_name.get("group_count"),
        "provider_components": observe_by_name.get("component_count"),
        "npi_scope": observe_by_name.get("npi_count"),
        "provider_set_audit_npi": observe_by_name.get(
            "provider_set_audit_npi_count"
        ),
        "provider_set_npi_prefix_overrides": observe_by_name.get(
            "npi_prefix_override_owner_count"
        ),
        "patterns": observe_by_name.get("pattern_count"),
    }
    for name, expected_rows in expected_dictionary_rows.items():
        if name in artifact_by_name and artifact_by_name[name].row_count != expected_rows:
            raise RuntimeError(f"V4 graph {name} row count disagrees with observe counters")
    prefix_metadata = _read_prefix_override_metadata(
        path_by_field["provider_set_npi_prefix_override_copy_path"],
        prefix_target=int(expected_options["npi_prefix_target"]),
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
    if database_bytes != selected_bytes:
        raise RuntimeError("V4 graph compiler selected byte count disagrees with outputs")
    expected_file_names = {
        artifact.path.name for artifact in output_artifacts
    } | {"v4-summary.json"}
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
        pattern_copy_path=pattern_copy_path,
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


async def compile_provider_graph_v4_rust(
    *,
    graph_artifact_entries: Iterable[Mapping[str, Any]],
    provider_set_key_map_path: str | Path,
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
    output = Path(output_directory).resolve()
    if not output.parent.is_dir():
        raise RuntimeError("V4 graph compiler output parent is unavailable")
    effective_options = _effective_compiler_options(options)
    manifest, expected_input_bytes = build_v4_graph_compiler_manifest(
        graph_artifact_entries=graph_artifact_entries,
        provider_set_key_map_path=provider_set_key_map_path,
        output_directory=output,
        options=effective_options,
    )
    expected_factor_edges, expected_factor_owners = _manifest_factor_counts(manifest)
    manifest_bytes = _canonical_json_bytes(manifest)
    provider_map = Path(manifest["provider_set_key_map_path"])
    binding_sha256, provider_map_sha256 = _checkpoint_binding(
        manifest_bytes=manifest_bytes,
        provider_set_key_map_path=provider_map,
    )
    checkpoint_path = output / PTG2_V4_GRAPH_CHECKPOINT_NAME

    if output.exists():
        if output.is_symlink() or not output.is_dir():
            raise RuntimeError(f"V4 graph compiler output is not a safe directory: {output}")
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
                expected_input_bytes=expected_input_bytes,
                expected_factor_edges=expected_factor_edges,
                expected_options=effective_options,
                allow_checkpoint=True,
            )
            _validate_checkpoint(
                checkpoint,
                validated_result=reused,
                binding_sha256=binding_sha256,
                provider_map_sha256=provider_map_sha256,
                options=effective_options,
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

    manifest_path = output.parent / f".{output.name}.v4-manifest.json"
    stdout_path = output.parent / f".{output.name}.v4-stdout.json"
    stderr_path = output.parent / f".{output.name}.v4-stderr.log"
    for path in (manifest_path, stdout_path, stderr_path):
        if path.exists():
            if path.is_symlink() or not path.is_file():
                raise RuntimeError(f"V4 graph compiler scratch path is unsafe: {path}")
            path.unlink()
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
        with stdout_path.open("xb") as stdout_output, stderr_path.open("xb") as stderr_output:
            process = await asyncio.create_subprocess_exec(
                str(binary),
                str(manifest_path),
                stdout=stdout_output,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
            if process.stderr is None:
                raise RuntimeError("V4 graph compiler did not expose its progress stream")
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
            raise RuntimeError("V4 graph compiler exited without a terminal progress event")
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
        if stdout_summary_sha256 != hashlib.sha256(
            _canonical_json_bytes(file_summary)
        ).digest():
            raise RuntimeError("V4 graph compiler stdout and summary file disagree")
        compilation_result = _validate_compiler_summary(
            file_summary,
            output_directory=output,
            expected_input_bytes=expected_input_bytes,
            expected_factor_edges=expected_factor_edges,
            expected_options=effective_options,
        )
        checkpoint = _checkpoint_payload(
            result=compilation_result,
            binding_sha256=binding_sha256,
            provider_map_sha256=provider_map_sha256,
            options=effective_options,
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
    "V4GraphOutputArtifact",
    "V4GraphResourceAdmissionError",
    "build_v4_graph_compiler_manifest",
    "compile_provider_graph_v4_rust",
]
