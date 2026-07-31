"""Shared validation arguments for synthetic V4 compiler summaries."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

from process.ptg_parts import ptg2_v4_graph_compiler as compiler


def packed_summary_validation(
    arguments_by_name: dict[str, Any],
) -> dict[str, Any]:
    """Pack fixture expectations into the production validation contract."""

    copied_arguments_by_name = dict(arguments_by_name)
    expectation = compiler._CompilerSummaryExpectation(
        input_bytes=copied_arguments_by_name.pop("expected_input_bytes"),
        factor_edges=copied_arguments_by_name.pop("expected_factor_edges"),
        factor_owners=copied_arguments_by_name.pop("expected_factor_owners"),
        options=copied_arguments_by_name.pop("expected_options"),
        tax_identity=copied_arguments_by_name.pop(
            "expected_tax_identity",
            None,
        ),
        taxonomy_rule_count=copied_arguments_by_name.pop(
            "expected_taxonomy_rule_count",
            None,
        ),
    )
    return {**copied_arguments_by_name, "expectation": expectation}


def summary_validation_fixture(
    summary_by_field: dict[str, Any],
    output_directory: Path,
    option_by_name: dict[str, int],
) -> dict[str, Any]:
    """Return validation expectations for one synthetic compiler summary."""

    tax_identity = summary_by_field["tax_identity"]
    return {
        "output_directory": output_directory,
        "expected_input_bytes": int(summary_by_field["input_byte_count"]),
        "expected_factor_edges": int(
            summary_by_field["resource_admission"]["factor_edge_count"]
        ),
        "expected_factor_owners": int(
            summary_by_field["resource_admission"]["factor_owner_count"]
        ),
        "expected_options": option_by_name,
        "expected_tax_identity": {
            "token_policy_id": tax_identity["token_policy_id"],
            "source_shard_ids": tuple(
                source_entry["shard_id"]
                for source_entry in tax_identity["source_ordinal_map"]
            ),
            "merge_bitmap_upper_bound_bytes": 0,
            "source_ordinal_upper_bound_bytes": 268,
            "projection_upper_bound_bytes": 0,
        },
        "allow_checkpoint": False,
    }


def taxonomy_rejected_summary(
    summary_by_field: dict[str, Any],
    layout_name: str,
) -> dict[str, Any]:
    """Return valid evidence for one taxonomy-ineligible layout."""

    changed = deepcopy(summary_by_field)
    changed[f"{layout_name}_inferred_taxonomy_eligible"] = False
    changed[f"{layout_name}_inferred_taxonomy_rejection_reason"] = (
        "pattern_projection_cap_exceeded"
    )
    changed[f"{layout_name}_inferred_taxonomy_rejection_rule_digest"] = "f" * 64
    changed[f"{layout_name}_inferred_taxonomy_rejection_observed_count"] = 2
    changed[f"{layout_name}_inferred_taxonomy_rejection_cap"] = 1
    return changed
