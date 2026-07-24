from __future__ import annotations

from copy import deepcopy
import hashlib
from pathlib import Path
from typing import Any

from process.ptg_parts import ptg2_v4_graph_compiler as compiler


EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
_DIRECT_RELATIONS = ("set_components", "set_npi_prefix_override")
_EMPTY_OBSERVE_NAMES = """
maximum_patterns_per_set maximum_components_per_set pattern_overflow_set_count
maximum_components_per_pattern_overflow_set unsafe_pattern_component_set_count
provider_set_count npi_prefix_group_unsafe_set_count
npi_prefix_physical_unsafe_set_count npi_prefix_simulated_set_count
npi_prefix_override_owner_count npi_prefix_override_member_count
npi_prefix_override_raw_bytes npi_prefix_group_merge_member_visits
npi_prefix_worst_groups_to_target npi_prefix_worst_member_count
npi_prefix_worst_online_groups_to_target npi_prefix_worst_online_member_count
npi_prefix_worst_online_group_work_bound maximum_patterns_per_npi
group_set_expansion_edge_visits group_set_incidence_count group_count
component_count npi_count provider_set_audit_npi_count pattern_count
""".split()
_COPY_ARTIFACT_NAMES = (
    "graph_blocks",
    "provider_groups",
    "provider_components",
    "npi_scope",
    "provider_set_audit_npi",
    "provider_set_npi_prefix_overrides",
)


def empty_observe_fixture() -> dict[str, Any]:
    """Build internally consistent diagnostics for an empty graph."""

    observed_by_name: dict[str, Any] = {
        name: 0 for name in _EMPTY_OBSERVE_NAMES
    }
    for suffix in ("p50", "p95", "p99", "max"):
        observed_by_name[f"npi_prefix_groups_to_target_{suffix}"] = 0
    for suffix in ("p50", "p95", "p99"):
        observed_by_name[f"npi_patterns_per_npi_{suffix}"] = 0
    for prefix in (
        "maximum_online_source",
        "npi_prefix_worst_source",
        "npi_prefix_worst_online_source",
    ):
        for dimension in ("owner", "member", "page", "byte"):
            observed_by_name[f"{prefix}_{dimension}_work"] = 0
    for prefix in (
        "maximum_online_group_npi",
        "npi_prefix_worst_group_npi",
        "npi_prefix_worst_online_group_npi",
    ):
        for dimension in ("member", "locator_page", "member_page", "byte", "batch"):
            observed_by_name[f"{prefix}_{dimension}_work"] = 0
    observed_by_name.update(
        {
            "npi_prefix_worst_provider_set_key": None,
            "npi_prefix_worst_member_digest": None,
            "npi_prefix_worst_online_provider_set_key": None,
            "npi_prefix_worst_online_member_digest": None,
            "npi_prefix_worst_provider_set_uses_override": False,
            "npi_prefix_worst_uses_component_fallback": False,
            "npi_prefix_worst_online_groups_to_target_exact": False,
            "npi_prefix_worst_online_uses_component_fallback": False,
        }
    )
    return observed_by_name


def empty_relation_fixture() -> list[dict[str, int | str]]:
    """Build the two zero-geometry relations required by validation."""

    return [
        {
            "relation": relation,
            "owner_base": 0,
            "owner_count": 0,
            "logical_member_count": 0,
            "vector_member_count": 0,
            "member_width": 4,
            "raw_vector_bytes": 0,
            "member_block_count": 0,
            "locator_block_count": 0,
        }
        for relation in _DIRECT_RELATIONS
    ]


def write_empty_artifacts(
    output_directory: Path,
) -> dict[str, dict[str, Any]]:
    """Write authenticated zero-row COPY files and an empty manifest."""

    payload = compiler._PG_COPY_HEADER + compiler._PG_COPY_TRAILER
    artifact_by_name: dict[str, dict[str, Any]] = {}
    for name in _COPY_ARTIFACT_NAMES:
        filename = compiler._OUTPUT_FILE_BY_NAME[name][0]
        path = output_directory / filename
        path.write_bytes(payload)
        artifact_by_name[name] = {
            "name": name,
            "path": str(path),
            "byte_count": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "row_count": 0,
        }
    reference_path = output_directory / "v4-graph-references.jsonl"
    reference_path.write_bytes(b"")
    artifact_by_name["graph_references"] = {
        "name": "graph_references",
        "path": str(reference_path),
        "byte_count": 0,
        "sha256": EMPTY_SHA256,
        "row_count": 0,
    }
    return artifact_by_name


def heavy_bitmap_block_mismatch_fixture(
    summary_by_name: dict[str, Any],
) -> dict[str, Any]:
    """Return plausible heavy-owner geometry with its output block omitted."""

    changed = deepcopy(summary_by_name)
    member_count = int(changed["heavy_owner_member_threshold"])
    set_components = changed["relation_summaries"][0]
    set_components["owner_count"] = 1
    set_components["logical_member_count"] = member_count
    changed["heavy_bitmaps"] = [
        {
            "relation": "set_components",
            "owner_key": 0,
            "member_count": member_count,
            "block_count": 1,
        }
    ]
    changed["block_count"] = 1
    return changed


def pattern_summary_fixture(
    summary_by_name: dict[str, Any],
    output_directory: Path,
) -> dict[str, Any]:
    """Return valid pattern-layout evidence with an empty dictionary."""

    changed = deepcopy(summary_by_name)
    pattern_payload = compiler._PG_COPY_HEADER + compiler._PG_COPY_TRAILER
    pattern_path = output_directory / "v4-patterns.copy"
    pattern_path.write_bytes(pattern_payload)
    changed["output_artifacts"].append(
        {
            "name": "patterns",
            "path": str(pattern_path),
            "byte_count": len(pattern_payload),
            "sha256": hashlib.sha256(pattern_payload).hexdigest(),
            "row_count": 0,
        }
    )
    selected_bytes = int(changed["selected_encoded_bytes"]) + len(pattern_payload)
    changed.update(
        {
            "selected_layout": "pattern",
            "direct_complete_encoded_bytes": selected_bytes + 1,
            "pattern_complete_encoded_bytes": selected_bytes,
            "selected_encoded_bytes": selected_bytes,
            "pattern_copy_path": str(pattern_path),
        }
    )
    return changed
