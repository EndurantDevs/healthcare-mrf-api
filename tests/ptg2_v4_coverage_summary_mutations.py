from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Any

import pytest

from process.ptg_parts import ptg2_v4_graph_compiler as compiler
from tests.ptg2_v4_summary_fixture_support import (
    EMPTY_SHA256,
    empty_observe_fixture,
    empty_relation_fixture,
    heavy_bitmap_block_mismatch_fixture,
    pattern_summary_fixture,
    write_empty_artifacts,
)
_OUTPUT_ARTIFACT_NAMES = (
    "graph_blocks",
    "graph_references",
    "provider_groups",
    "provider_components",
    "npi_scope",
    "provider_set_audit_npi",
    "provider_set_npi_prefix_overrides",
)


def _selected_encoded_bytes(
    artifact_by_name: dict[str, dict[str, Any]],
) -> int:
    return sum(
        int(artifact["byte_count"])
        for name, artifact in artifact_by_name.items()
        if name != "graph_references"
    )


def _summary_path_fields(
    output_directory: Path,
    artifact_by_name: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return {
        "block_copy_path": artifact_by_name["graph_blocks"]["path"],
        "reference_manifest_path": artifact_by_name["graph_references"]["path"],
        "group_copy_path": artifact_by_name["provider_groups"]["path"],
        "component_copy_path": artifact_by_name["provider_components"]["path"],
        "npi_copy_path": artifact_by_name["npi_scope"]["path"],
        "provider_set_audit_npi_copy_path": artifact_by_name[
            "provider_set_audit_npi"
        ]["path"],
        "provider_set_npi_prefix_override_copy_path": artifact_by_name[
            "provider_set_npi_prefix_overrides"
        ]["path"],
        "pattern_copy_path": None,
        "summary_path": str(output_directory / "v4-summary.json"),
    }


def _resource_fixture(
    option_by_name: dict[str, int],
    *,
    input_bytes: int,
    factor_edges: int,
) -> dict[str, Any]:
    return {
        "formula": "authenticated test fixture",
        "input_factor_bytes": input_bytes,
        "provider_set_key_map_bytes": 0,
        "factor_edge_count": factor_edges,
        "factor_owner_count": 0,
        "estimated_peak_bytes": 0,
        "max_estimated_model_bytes": option_by_name["max_estimated_model_bytes"],
        "max_factor_edges": option_by_name["max_factor_edges"],
    }


def _valid_summary_fixture(
    output_directory: Path,
    option_by_name: dict[str, int],
) -> dict[str, Any]:
    """Build valid compiler evidence without requiring the Rust executable."""

    output_directory.mkdir()
    artifact_by_name = write_empty_artifacts(output_directory)
    selected_encoded_bytes = _selected_encoded_bytes(artifact_by_name)
    input_bytes = 0
    factor_edges = 0
    summary_by_name: dict[str, Any] = {
        "format": compiler.PTG2_V4_GRAPH_SUMMARY_FORMAT,
        "selected_layout": "direct",
        "pattern_layout_serving_degree_eligible": True,
        "direct_complete_encoded_bytes": selected_encoded_bytes,
        "pattern_complete_encoded_bytes": selected_encoded_bytes + 1,
        "common_encoded_bytes": selected_encoded_bytes,
        "selected_encoded_bytes": selected_encoded_bytes,
        "input_byte_count": input_bytes,
        "input_sha256": EMPTY_SHA256,
        "block_count": 0,
        "block_copy_bytes": artifact_by_name["graph_blocks"]["byte_count"],
        "observe": empty_observe_fixture(),
        "relation_summaries": empty_relation_fixture(),
        "heavy_bitmaps": [],
        "output_artifacts": [
            artifact_by_name[name] for name in _OUTPUT_ARTIFACT_NAMES
        ],
        "resource_admission": _resource_fixture(
            option_by_name,
            input_bytes=input_bytes,
            factor_edges=factor_edges,
        ),
    }
    summary_by_name.update(_summary_path_fields(output_directory, artifact_by_name))
    summary_by_name.update(
        {
            name: option_by_name[name]
            for name in compiler.PTG2_V4_GRAPH_ENCODING_OPTION_NAMES
        }
    )
    (output_directory / "v4-summary.json").write_text(
        json.dumps(summary_by_name, indent=2, sort_keys=True) + "\n"
    )
    return summary_by_name


def _mutated(summary: dict[str, Any], path: tuple[Any, ...], value: Any):
    changed = deepcopy(summary)
    target: Any = changed
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value
    return changed


def _reject(summary: Any, arguments: dict[str, Any], message: str) -> None:
    with pytest.raises(RuntimeError, match=message):
        compiler._validate_compiler_summary(summary, **arguments)


def _header_mutations(
    summary: dict[str, Any],
    option_by_name: dict[str, int],
) -> tuple[tuple[Any, str], ...]:
    return (
        (None, "summary is not an object"),
        (_mutated(summary, ("format",), "wrong"), "incompatible format"),
        (_mutated(summary, ("selected_layout",), "wrong"), "invalid selected_layout"),
        (
            _mutated(
                summary,
                ("input_byte_count",),
                summary["input_byte_count"] + 1,
            ),
            "input byte count changed",
        ),
        (
            _mutated(
                summary,
                ("max_set_patterns_per_set",),
                option_by_name["max_set_patterns_per_set"] + 1,
            ),
            "pattern serving-degree limit changed",
        ),
        (
            _mutated(
                summary,
                ("selected_encoded_bytes",),
                summary["direct_complete_encoded_bytes"] + 1,
            ),
            "adaptive-layout choice",
        ),
    )


def _resource_mutations(
    summary: dict[str, Any],
    option_by_name: dict[str, int],
) -> tuple[tuple[Any, str], ...]:
    return (
        (
            _mutated(summary, ("resource_admission",), None),
            "invalid resource admission",
        ),
        (
            _mutated(
                summary,
                ("resource_admission", "input_factor_bytes"),
                summary["resource_admission"]["input_factor_bytes"] + 1,
            ),
            "resource input byte count changed",
        ),
        (
            _mutated(
                summary,
                ("resource_admission", "factor_edge_count"),
                summary["resource_admission"]["factor_edge_count"] + 1,
            ),
            "resource factor edge count changed",
        ),
        (
            _mutated(
                summary,
                ("resource_admission", "max_estimated_model_bytes"),
                option_by_name["max_estimated_model_bytes"] + 1,
            ),
            "resource model byte limit changed",
        ),
        (
            _mutated(
                summary,
                ("resource_admission", "max_factor_edges"),
                option_by_name["max_factor_edges"] + 1,
            ),
            "resource factor edge limit changed",
        ),
        (
            _mutated(summary, ("resource_admission", "formula"), None),
            "admission formula is missing",
        ),
    )


def _relation_shape_mutations(
    summary: dict[str, Any],
) -> tuple[tuple[Any, str], ...]:
    return (
        (
            _mutated(summary, ("relation_summaries",), None),
            "invalid relation summaries",
        ),
        (
            _mutated(summary, ("heavy_bitmaps",), None),
            "invalid heavy bitmap summaries",
        ),
        (
            _mutated(summary, ("relation_summaries", 0, "relation"), ""),
            "invalid relation identity",
        ),
        (
            _mutated(summary, ("relation_summaries", 0, "member_width"), 8),
            "relation counts are inconsistent",
        ),
        (
            _mutated(
                summary,
                ("heavy_bitmaps",),
                [{"relation": "missing"}],
            ),
            "bitmap has an invalid relation",
        ),
        (
            _mutated(
                summary,
                ("heavy_bitmaps",),
                [{"relation": "set_components", "owner_key": 1, "member_count": 0}],
            ),
            "bitmap owner is inconsistent",
        ),
    )


def _relation_geometry_mutations(
    summary: dict[str, Any],
) -> tuple[tuple[Any, str], ...]:
    relations = summary["relation_summaries"]
    override_index = next(
        index
        for index, relation in enumerate(relations)
        if relation["relation"] == "set_npi_prefix_override"
    )
    return (
        (
            _mutated(
                summary,
                ("relation_summaries", 0, "logical_member_count"),
                relations[0]["logical_member_count"] + 1,
            ),
            "logical/vector counts disagree",
        ),
        (
            _mutated(
                summary,
                ("relation_summaries",),
                [
                    relation
                    for relation in relations
                    if relation["relation"] != "set_npi_prefix_override"
                ],
            ),
            "omitted NPI-prefix relation geometry",
        ),
        (
            _mutated(
                summary,
                ("relation_summaries", override_index, "owner_base"),
                relations[override_index]["owner_base"] + 1,
            ),
            "NPI-prefix relation geometry is inconsistent",
        ),
        (
            _mutated(summary, ("block_count",), summary["block_count"] + 1),
            "block counts disagree",
        ),
        (
            _mutated(summary, ("pattern_copy_path",), "unexpected"),
            "unexpectedly published a pattern dictionary",
        ),
    )


def _artifact_mutations(
    summary: dict[str, Any],
) -> tuple[tuple[Any, str], ...]:
    output_artifacts = summary["output_artifacts"]
    return (
        (
            _mutated(summary, ("output_artifacts",), None),
            "invalid output artifacts",
        ),
        (
            _mutated(
                summary,
                ("output_artifacts",),
                [None, *output_artifacts[1:]],
            ),
            "output artifact is invalid",
        ),
        (
            _mutated(summary, ("output_artifacts", 0, "name"), "wrong"),
            "output artifact has invalid name",
        ),
        (
            _mutated(
                summary,
                ("output_artifacts", 0, "byte_count"),
                output_artifacts[0]["byte_count"] + 1,
            ),
            "output authentication failed",
        ),
        (
            _mutated(
                summary,
                ("output_artifacts", 0, "row_count"),
                output_artifacts[0]["row_count"] + 1,
            ),
            "COPY row count changed",
        ),
        (
            _mutated(
                summary,
                ("output_artifacts",),
                output_artifacts[:-1],
            ),
            "artifact set is incomplete",
        ),
    )


def _output_consistency_mutations(
    summary: dict[str, Any],
) -> tuple[tuple[Any, str], ...]:
    output_artifacts = summary["output_artifacts"]
    changed_relation_blocks = deepcopy(summary)
    changed_relation_blocks["block_count"] += 1
    changed_relation_blocks["relation_summaries"][0]["member_block_count"] += 1
    return (
        (
            changed_relation_blocks,
            "graph block row count disagrees",
        ),
        (
            _mutated(
                summary,
                ("output_artifacts", 1, "row_count"),
                output_artifacts[1]["row_count"] + 1,
            ),
            "graph reference row count disagrees",
        ),
        (
            _mutated(
                summary,
                ("selected_encoded_bytes",),
                summary["selected_encoded_bytes"] - 1,
            ),
            "selected byte count disagrees",
        ),
    )


def _relation_mutations(
    summary: dict[str, Any],
) -> tuple[tuple[Any, str], ...]:
    return (
        *_relation_shape_mutations(summary),
        *_relation_geometry_mutations(summary),
    )


def _output_mutations(
    summary: dict[str, Any],
) -> tuple[tuple[Any, str], ...]:
    return (
        *_artifact_mutations(summary),
        *_output_consistency_mutations(summary),
    )


def _summary_mutations(
    summary: dict[str, Any],
    option_by_name: dict[str, int],
) -> tuple[tuple[Any, str], ...]:
    return (
        *_header_mutations(summary, option_by_name),
        *_resource_mutations(summary, option_by_name),
        *_relation_mutations(summary),
        *_output_mutations(summary),
    )


def test_compiler_summary_authentication_branch_matrix(tmp_path: Path) -> None:
    output = tmp_path / "compiled"
    option_by_name = compiler._effective_compiler_options(None)
    summary = _valid_summary_fixture(output, option_by_name)
    validation_arguments_by_name = {
        "output_directory": output,
        "expected_input_bytes": int(summary["input_byte_count"]),
        "expected_factor_edges": int(
            summary["resource_admission"]["factor_edge_count"]
        ),
        "expected_options": option_by_name,
        "allow_checkpoint": False,
    }

    for changed, message in _summary_mutations(summary, option_by_name):
        _reject(changed, validation_arguments_by_name, message)

    _reject(
        heavy_bitmap_block_mismatch_fixture(summary),
        validation_arguments_by_name,
        "graph block row count disagrees",
    )
    extra_file = output / "unexpected"
    extra_file.write_bytes(b"x")
    _reject(
        summary,
        validation_arguments_by_name,
        "output directory has unexpected files",
    )
    extra_file.unlink()
    validated = compiler._validate_compiler_summary(
        pattern_summary_fixture(summary, output),
        **validation_arguments_by_name,
    )
    validated.cleanup()
    assert not output.exists()


def test_compiler_progress_rejects_post_terminal_and_backward_events() -> None:
    progress_event_by_field = {
        "version": compiler.PTG2_V4_PROGRESS_VERSION,
        "seq": 1,
        "phase": "complete",
        "done": 1,
        "total": 1,
        "unit": "stage",
        "elapsed_ms": 1,
        "terminal": True,
    }
    terminal_state = compiler._CompilerProgressState()
    assert terminal_state.is_accepted(progress_event_by_field)
    assert not terminal_state.is_accepted(
        {**progress_event_by_field, "seq": 2}
    )

    backward_state = compiler._CompilerProgressState(
        seq=1,
        phase_index=3,
        phase="derive_patterns",
    )
    assert not backward_state.is_accepted(
        {
            **progress_event_by_field,
            "seq": 2,
            "phase": "resource_admission",
            "done": 0,
            "terminal": False,
        }
    )

    pct_state = compiler._CompilerProgressState(phase_pct=99.0)
    assert not pct_state.is_accepted(
        {
            **progress_event_by_field,
            "phase": "resource_admission",
            "done": 0,
            "terminal": False,
        }
    )
