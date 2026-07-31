from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable

import pytest

from process.ptg_parts import ptg2_v4_graph_compiler as compiler


_BASE_ADAPTIVE_SUMMARY_BY_FIELD = {
    **{
        option_name: option_value
        for option_name, option_value in compiler._effective_compiler_options(
            None
        ).items()
        if option_name in compiler.PTG2_V4_GRAPH_ENCODING_OPTION_NAMES
    },
    "selected_layout": "direct",
    "selected_encoded_bytes": 100,
    "direct_layout_complete_prefix_eligible": True,
    "pattern_layout_sparse_prefix_eligible": True,
    "pattern_layout_serving_degree_eligible": True,
    "direct_complete_prefix_projection_encoded_bytes": 10,
    "pattern_sparse_prefix_owner_count": 0,
    "pattern_sparse_prefix_member_count": 0,
    "pattern_sparse_prefix_raw_bytes": 0,
    "pattern_sparse_prefix_projection_encoded_bytes": 10,
    "direct_graph_encoded_bytes": 90,
    "direct_mapping_persistence_encoded_bytes": 10,
    "direct_map_payload_encoded_bytes": 0,
    "direct_map_coordinate_count": 0,
    "direct_map_pack_count": 0,
    "direct_map_object_kind_count": 0,
    "direct_complete_encoded_bytes": 100,
    "pattern_graph_encoded_bytes": 90,
    "pattern_mapping_persistence_encoded_bytes": 10,
    "pattern_map_payload_encoded_bytes": 0,
    "pattern_map_coordinate_count": 0,
    "pattern_map_pack_count": 0,
    "pattern_map_object_kind_count": 0,
    "pattern_complete_encoded_bytes": 100,
    "direct_inferred_taxonomy_encoded_bytes": 0,
    "pattern_inferred_taxonomy_encoded_bytes": 0,
    "direct_inferred_taxonomy_eligible": True,
    "pattern_inferred_taxonomy_eligible": True,
    **{
        f"{layout_name}_inferred_taxonomy_rejection_{field_name}": None
        for layout_name in ("direct", "pattern")
        for field_name in (
            "reason",
            "rule_digest",
            "observed_count",
            "cap",
        )
    },
    "resource_admission": {
        "max_estimated_model_bytes": 8 * 1024 * 1024 * 1024,
        "max_factor_edges": 1_000_000,
    },
    "tax_identity": {"source_ordinal_map_digest": "1" * 64},
    "observe": {"unsafe_pattern_component_set_count": 0},
}


def _layout_eligibility_mutations(
    summary: dict[str, Any],
    mutate: Callable[[dict[str, Any], tuple[Any, ...], Any], dict[str, Any]],
) -> tuple[tuple[Any, str], ...]:
    return (
        (
            mutate(summary, ("direct_layout_complete_prefix_eligible",), "yes"),
            "direct prefix eligibility is invalid",
        ),
        (
            mutate(summary, ("direct_layout_complete_prefix_eligible",), False),
            "direct prefix eligibility changed",
        ),
        (
            mutate(summary, ("pattern_layout_sparse_prefix_eligible",), "yes"),
            "sparse prefix eligibility is invalid",
        ),
        (
            mutate(summary, ("pattern_layout_sparse_prefix_eligible",), False),
            "sparse prefix evidence is inconsistent",
        ),
        (
            mutate(summary, ("pattern_layout_serving_degree_eligible",), False),
            "pattern serving-degree decision changed",
        ),
    )


def _layout_storage_mutations(
    summary: dict[str, Any],
    mutate: Callable[[dict[str, Any], tuple[Any, ...], Any], dict[str, Any]],
) -> tuple[tuple[Any, str], ...]:
    return (
        (
            mutate(summary, ("pattern_sparse_prefix_raw_bytes",), 4),
            "sparse prefix evidence is inconsistent",
        ),
        (
            mutate(
                summary,
                ("direct_mapping_persistence_encoded_bytes",),
                summary["direct_mapping_persistence_encoded_bytes"] + 1,
            ),
            "persistent candidate byte counts disagree",
        ),
        (
            mutate(
                summary,
                ("direct_map_payload_encoded_bytes",),
                summary["direct_map_payload_encoded_bytes"] + 1,
            ),
            "direct packed-map geometry is inconsistent",
        ),
        (
            mutate(
                summary,
                ("direct_map_pack_count",),
                summary["direct_map_pack_count"] + 1,
            ),
            "direct packed-map geometry is inconsistent",
        ),
        (
            mutate(
                summary,
                ("selected_graph_encoded_bytes",),
                summary["selected_graph_encoded_bytes"] + 1,
            ),
            "adaptive-layout choice",
        ),
    )


def _layout_diagnostic_mutations(
    summary: dict[str, Any],
    mutate: Callable[[dict[str, Any], tuple[Any, ...], Any], dict[str, Any]],
) -> tuple[tuple[Any, str], ...]:
    return (
        (
            mutate(
                summary,
                ("observe", "pattern_component_over_cap_set_count"),
                1,
            ),
            "component fallback diagnostics are inconsistent",
        ),
        (
            mutate(
                summary,
                (
                    "observe",
                    "pattern_component_over_cap_prefix_covered_set_count",
                ),
                1,
            ),
            "component fallback diagnostics are inconsistent",
        ),
    )


def layout_header_mutations(
    summary: dict[str, Any],
    mutate: Callable[[dict[str, Any], tuple[Any, ...], Any], dict[str, Any]],
) -> tuple[tuple[Any, str], ...]:
    """Return fail-closed mutations for adaptive layout evidence."""

    return (
        *_layout_eligibility_mutations(summary, mutate),
        *_layout_storage_mutations(summary, mutate),
        *_layout_diagnostic_mutations(summary, mutate),
    )


def test_adaptive_layout_evidence_ignores_source_identity() -> None:
    """Source-only provenance changes cannot alter adaptive layout evidence."""

    renamed_summary_by_field = {
        **_BASE_ADAPTIVE_SUMMARY_BY_FIELD,
        "source_metadata": {"label": "renamed-source-with-different-length"},
        "tax_identity": {"source_ordinal_map_digest": "2" * 64},
    }
    initial_evidence_by_field = compiler.v4_adaptive_layout_decision_from_summary(
        _BASE_ADAPTIVE_SUMMARY_BY_FIELD
    )
    renamed_evidence_by_field = compiler.v4_adaptive_layout_decision_from_summary(
        renamed_summary_by_field
    )

    assert initial_evidence_by_field == renamed_evidence_by_field
    assert initial_evidence_by_field["decision_digest"] == (
        renamed_evidence_by_field["decision_digest"]
    )
    assert (
        _BASE_ADAPTIVE_SUMMARY_BY_FIELD["tax_identity"]
        != renamed_summary_by_field["tax_identity"]
    )


def test_adaptive_layout_evidence_uses_complete_candidate_costs() -> None:
    """Shape costs select the lowest eligible candidate and seal both totals."""

    direct_evidence_by_field = compiler.v4_adaptive_layout_decision_from_summary(
        _BASE_ADAPTIVE_SUMMARY_BY_FIELD
    )
    changed_loser_by_field = deepcopy(direct_evidence_by_field)
    changed_loser_by_field["pattern"]["graph_encoded_bytes"] += 1
    changed_loser_by_field["pattern"]["complete_persistent_encoded_bytes"] += 1
    changed_loser_by_field["decision_digest"] = (
        compiler._adaptive_layout_evidence_digest(
            {
                field_name: field_value
                for field_name, field_value in changed_loser_by_field.items()
                if field_name != "decision_digest"
            }
        )
    )

    assert direct_evidence_by_field["selected_representation"] == "direct_v1"
    assert (
        compiler.validate_v4_adaptive_layout_decision(changed_loser_by_field)[
            "selected_representation"
        ]
        == "direct_v1"
    )
    assert (
        changed_loser_by_field["decision_digest"]
        != direct_evidence_by_field["decision_digest"]
    )


def _pattern_evidence_by_field() -> dict[str, Any]:
    """Return valid shape evidence whose lower complete cost selects pattern."""

    pattern_summary_by_field = {
        **_BASE_ADAPTIVE_SUMMARY_BY_FIELD,
        "selected_layout": "pattern",
        "selected_encoded_bytes": 89,
        "pattern_graph_encoded_bytes": 79,
        "pattern_complete_encoded_bytes": 89,
    }
    return compiler.v4_adaptive_layout_decision_from_summary(pattern_summary_by_field)


def test_adaptive_layout_rejects_cost_digest_and_eligibility_tampering() -> None:
    """Candidate cost, decision digest, and eligibility remain authenticated."""

    pattern_evidence_by_field = _pattern_evidence_by_field()
    changed_cost_by_field = deepcopy(pattern_evidence_by_field)
    changed_cost_by_field["pattern"]["complete_persistent_encoded_bytes"] += 1
    with pytest.raises(RuntimeError, match="cost is inconsistent"):
        compiler.validate_v4_adaptive_layout_decision(changed_cost_by_field)

    changed_digest_by_field = deepcopy(pattern_evidence_by_field)
    changed_digest_by_field["decision_digest"] = "0" * 64
    with pytest.raises(RuntimeError, match="digest changed"):
        compiler.validate_v4_adaptive_layout_decision(changed_digest_by_field)

    changed_eligibility_by_field = deepcopy(pattern_evidence_by_field)
    changed_eligibility_by_field["pattern"]["unsafe_component_set_count"] = 1
    changed_eligibility_by_field["decision_digest"] = (
        compiler._adaptive_layout_evidence_digest(
            {
                field_name: field_value
                for field_name, field_value in changed_eligibility_by_field.items()
                if field_name != "decision_digest"
            }
        )
    )
    with pytest.raises(RuntimeError, match="pattern eligibility is inconsistent"):
        compiler.validate_v4_adaptive_layout_decision(changed_eligibility_by_field)


def test_adaptive_layout_rejects_direct_prefix_cap_drift() -> None:
    """A post-decision prefix cap change cannot preserve direct eligibility."""

    evidence_by_field = compiler.v4_adaptive_layout_decision_from_summary(
        _BASE_ADAPTIVE_SUMMARY_BY_FIELD
    )
    evidence_by_field["compiler_options"]["max_npi_prefix_override_bytes"] = 9
    evidence_by_field["decision_digest"] = compiler._adaptive_layout_evidence_digest(
        {
            field_name: field_value
            for field_name, field_value in evidence_by_field.items()
            if field_name != "decision_digest"
        }
    )

    with pytest.raises(RuntimeError, match="direct eligibility is inconsistent"):
        compiler.validate_v4_adaptive_layout_decision(evidence_by_field)


@pytest.mark.parametrize(
    "option_name",
    (
        "member_page_bytes",
        "heavy_owner_member_threshold",
        "heavy_bitmap_minimum_savings_bytes",
        "max_online_source_members_per_set",
        "max_online_group_npi_batches_per_set",
        "max_online_provider_expansion_graph_batches",
        "max_estimated_model_bytes",
        "max_factor_edges",
    ),
)
def test_adaptive_layout_digest_binds_every_compiler_option(
    option_name: str,
) -> None:
    """Changing any sealed compiler option invalidates the decision digest."""

    evidence_by_field = compiler.v4_adaptive_layout_decision_from_summary(
        _BASE_ADAPTIVE_SUMMARY_BY_FIELD
    )
    evidence_by_field["compiler_options"][option_name] += 1

    with pytest.raises(RuntimeError, match="decision digest changed"):
        compiler.validate_v4_adaptive_layout_decision(evidence_by_field)
