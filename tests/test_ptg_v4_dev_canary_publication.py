# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS,
)
from scripts import ptg_v4_dev_canary_acceptance as acceptance
from scripts import ptg_v4_dev_canary_publication as publication
from scripts.ptg_v4_dev_canary_budget import (
    ELAPSED_FIXED_SECONDS,
    SECONDS_PER_INPUT_GIB,
    SECONDS_PER_MILLION_COMPONENT_FACTS,
)


def _provider_graph_diagnostic_evidence() -> dict[str, object]:
    """Build exact prefix, diagnostic, and sealed resource evidence."""

    fields_by_name: dict[str, object] = {
        field_name: 0 for field_name in PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS
    }
    fields_by_name.update(
        {
            "override_owner_count": 1,
            "override_member_count": 2,
            "worst_provider_set_key": 7,
            "worst_uses_override": True,
            "worst_member_count": 2,
            "worst_member_digest": "a" * 64,
            "worst_online_provider_set_key": 9,
            "worst_online_member_count": 2,
            "worst_online_member_digest": "b" * 64,
        }
    )
    return {
        "row_count": 1,
        "fields": fields_by_name,
        "resources": {
            "compressed_acquisition_bytes": 1_000_000,
            "input_factor_bytes": 2_000_000,
            "factor_edge_count": 3_000_000,
            "empty_npi_tin_only_normalization_count": 0,
        },
        "prefix": {
            "owner_count": 1,
            "member_count": 2,
            "all_rows_valid": True,
            "selected_owners": [
                {
                    "provider_set_key": 7,
                    "member_count": 2,
                    "member_digest": "a" * 64,
                }
            ],
        },
    }


def _prefix_relation() -> list[dict[str, object]]:
    """Return the exact sparse-prefix vector geometry fixture."""

    return [
        {
            "relation": "set_npi_prefix_override",
            "logical_member_count": 2,
            "vector_member_count": 2,
        }
    ]


def test_publication_reconciles_diagnostic_prefix_and_vector() -> None:
    failures: list[str] = []

    publication._validate_provider_graph_diagnostic(
        _provider_graph_diagnostic_evidence(),
        exact_counts={
            "diagnostic_count": 1,
            "prefix_owner_count": 1,
            "prefix_member_count": 2,
        },
        relations=_prefix_relation(),
        failures=failures,
    )

    assert failures == []


def test_publication_rejects_diagnostic_prefix_count_drift() -> None:
    failures: list[str] = []

    publication._validate_provider_graph_diagnostic(
        _provider_graph_diagnostic_evidence(),
        exact_counts={
            "diagnostic_count": 1,
            "prefix_owner_count": 1,
            "prefix_member_count": 3,
        },
        relations=_prefix_relation(),
        failures=failures,
    )

    assert "NPI-prefix metadata totals differ" in failures[0]


def test_publication_rejects_negative_empty_npi_resource() -> None:
    evidence = _provider_graph_diagnostic_evidence()
    evidence["resources"]["empty_npi_tin_only_normalization_count"] = -1
    failures: list[str] = []

    publication._validate_provider_graph_diagnostic(
        evidence,
        exact_counts={
            "diagnostic_count": 1,
            "prefix_owner_count": 1,
            "prefix_member_count": 2,
        },
        relations=_prefix_relation(),
        failures=failures,
    )

    assert any(
        "sealed resource admission is invalid" in failure
        for failure in failures
    )


def test_elapsed_budget_uses_sealed_inputs_and_source_controlled_policy() -> None:
    database_evidence_by_field = {
        "provider_graph_diagnostic": _provider_graph_diagnostic_evidence()
    }

    budget = acceptance._elapsed_budget(database_evidence_by_field)

    assert budget.compressed_input_bytes == 1_000_000
    assert budget.component_fact_count == 3_000_000
    assert budget.fixed_seconds == ELAPSED_FIXED_SECONDS
    assert budget.seconds_per_input_gib == SECONDS_PER_INPUT_GIB
    assert (
        budget.seconds_per_million_component_facts
        == SECONDS_PER_MILLION_COMPONENT_FACTS
    )
