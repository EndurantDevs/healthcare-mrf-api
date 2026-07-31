# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import hashlib
from types import SimpleNamespace

import pytest

from process.ptg_parts.ptg2_shared_source_set import (
    PTG2_V3_SOURCE_SET_CONTRACT,
)
from process.ptg_parts.canonical import canonical_json_dumps
from process.ptg_parts import ptg2_v4_graph_compiler as graph_compiler
from scripts import ptg_v4_dev_canary_acceptance as acceptance
from scripts import ptg_v4_dev_canary_publication as publication
from scripts import ptg_v4_dev_canary_storage_budget as storage_policy
from scripts.ptg_v4_dev_canary_cli import build_parser
from scripts.ptg_v4_dev_canary_publication import (
    STORAGE_EVIDENCE_CONTRACT,
    WHOLE_SNAPSHOT_PHYSICAL_RELATIONS,
)
from scripts.ptg_v4_dev_canary_storage_budget import (
    STORAGE_BUDGET_POLICY_DIGEST,
    STORAGE_CANARY_CASES,
    UNAPPROVED_STORAGE_CEILING_FAILURE,
    PhysicalStorageApproval,
    StorageCanaryCase,
    storage_budget,
)
from scripts.ptg_v4_dev_canary_retained_artifacts import (
    RETAINED_RAW_ARTIFACT_STORAGE_CONTRACT,
)
from scripts.ptg_v4_dev_canary_storage_sql import _ownership_predicates
from scripts.ptg_v4_dev_canary_support import CanaryConfigurationError


def _layout_candidate(
    candidate_name: str,
    encoded_bytes: int,
) -> dict[str, object]:
    common_by_field = {
        "eligible": True,
        "graph_encoded_bytes": encoded_bytes - 200,
        "mapping_persistence_encoded_bytes": 200,
        "map_payload_encoded_bytes": 132,
        "map_coordinate_count": 1,
        "map_pack_count": 1,
        "map_object_kind_count": 1,
        "inferred_taxonomy_encoded_bytes": 0,
        "inferred_taxonomy_eligible": True,
        "inferred_taxonomy_rejection_reason": None,
        "inferred_taxonomy_rejection_rule_digest": None,
        "inferred_taxonomy_rejection_observed_count": None,
        "inferred_taxonomy_rejection_cap": None,
        "complete_persistent_encoded_bytes": encoded_bytes,
    }
    if candidate_name == "direct":
        return {
            **common_by_field,
            "complete_prefix_eligible": True,
            "complete_prefix_projection_encoded_bytes": 10,
        }
    return {
        **common_by_field,
        "component_fallback_eligible": True,
        "unsafe_component_set_count": 0,
        "sparse_prefix_eligible": True,
        "sparse_prefix_owner_count": 0,
        "sparse_prefix_member_count": 0,
        "sparse_prefix_raw_bytes": 0,
        "sparse_prefix_projection_encoded_bytes": 10,
    }


def _adaptive_layout_evidence(representation: str) -> dict[str, object]:
    direct_bytes = 300 if representation == "direct_v1" else 301
    pattern_bytes = 299 if representation == "pattern_v1" else 301
    decision_by_field = {
        "contract": graph_compiler.PTG2_V4_ADAPTIVE_LAYOUT_DECISION_CONTRACT,
        "cost_contract": graph_compiler.PTG2_V4_ADAPTIVE_LAYOUT_COST_CONTRACT,
        "selection_policy": (graph_compiler.PTG2_V4_ADAPTIVE_LAYOUT_SELECTION_POLICY),
        "compiler_options": graph_compiler._effective_compiler_options(None),
        "selected_representation": representation,
        "selected_encoded_bytes": (
            direct_bytes if representation == "direct_v1" else pattern_bytes
        ),
        "direct": _layout_candidate("direct", direct_bytes),
        "pattern": _layout_candidate("pattern", pattern_bytes),
    }
    return {
        **decision_by_field,
        "decision_digest": graph_compiler._adaptive_layout_evidence_digest(
            decision_by_field
        ),
    }


def _source_set_evidence(case: StorageCanaryCase) -> dict[str, object]:
    return {
        "contract": PTG2_V3_SOURCE_SET_CONTRACT,
        "source_count": case.source_count,
        "raw_container_sha256_digest": case.source_set_digest,
    }


def _snapshot_evidence(
    case: StorageCanaryCase,
    *,
    logical_bytes: int,
    representation: str,
) -> dict[str, object]:
    return {
        "snapshot_id": f"ptg2:v4:{case.case_name}",
        "import_run_id": f"run_{case.case_name}",
        "snapshot_status": "published",
        "layout_state": "sealed",
        "layout_generation": "shared_blocks_v4",
        "layout_logical_byte_count": logical_bytes,
        "layout_manifest": {
            "serving_index": {
                "serving_binary": {
                    "provider_graph_v4": {
                        "representation": representation,
                        "adaptive_layout": _adaptive_layout_evidence(representation),
                    }
                }
            }
        },
    }


def _database_evidence(
    case: StorageCanaryCase,
    *,
    factor_edge_count: int,
    v4_logical_bytes: int = 123_456_789,
    representation: str = "direct_v1",
) -> dict[str, object]:
    """Build sealed snapshot, source-set, and compiler resource evidence."""

    source_set_by_field = _source_set_evidence(case)
    return {
        "snapshot": _snapshot_evidence(
            case,
            logical_bytes=v4_logical_bytes,
            representation=representation,
        ),
        "root": {
            "state": "complete",
            "representation": representation,
            "logical_byte_count": v4_logical_bytes,
        },
        "exact_counts": {"map_logical_byte_count": v4_logical_bytes},
        "reference_equivalence": {
            "v4_snapshot_id": f"ptg2:v4:{case.case_name}",
            "reference_snapshot_id": case.reference_snapshot_id,
            "same_raw_sources": True,
            "same_source_trace_sets": True,
            "v4_source_set": dict(source_set_by_field),
            "reference_source_set": dict(source_set_by_field),
            "reference_snapshot": {
                "snapshot_id": case.reference_snapshot_id,
                "snapshot_status": "published",
                "layout_state": "sealed",
                "layout_generation": "shared_blocks_v3",
                "layout_logical_byte_count": (case.base_layout_logical_bytes),
            },
        },
        "provider_graph_diagnostic": {
            "resources": {
                "compressed_acquisition_bytes": 8_000_000_000,
                "input_factor_bytes": 1_000_000_000,
                "factor_edge_count": factor_edge_count,
                "empty_npi_tin_only_normalization_count": 0,
            }
        },
    }


@pytest.mark.parametrize("case", STORAGE_CANARY_CASES)
def test_storage_budget_keeps_each_initial_canary_measurement_only(
    case: StorageCanaryCase,
) -> None:
    factor_edge_count = case.base_layout_logical_bytes // 8

    budget = storage_budget(
        _database_evidence(case, factor_edge_count=factor_edge_count)
    )

    assert budget.case == case
    assert budget.is_promotion_approved is False
    assert budget.maximum_graph_physical_storage_bytes is None
    assert budget.maximum_snapshot_physical_storage_bytes is None
    report = budget.report(graph_gate_bytes=123, snapshot_gate_bytes=456)
    assert report["policy_digest"] == STORAGE_BUDGET_POLICY_DIGEST
    assert report["promotion_state"] == "measurement_only_pending_review"
    assert report["base_layout_logical_bytes"] == case.base_layout_logical_bytes
    assert report["v4_factored_layout_logical_bytes"] == 123_456_789
    assert report["encoded_persistent_projection_contract"] == (
        "encoded_persistent_projection_v1"
    )
    assert report["encoded_persistent_projection_bytes"] == 300
    assert report["graph_physical_minus_encoded_projection_bytes"] == -177
    assert report["graph_physical_to_encoded_projection_basis_points"] == 4_100
    assert report["graph_projection_drift_within_budget"] is True
    assert report["graph_gate_bytes"] == 123
    assert report["snapshot_gate_bytes"] == 456


@pytest.mark.parametrize("representation", ("direct_v1", "pattern_v1"))
def test_storage_budget_derives_layout_from_sealed_shape_decision(
    representation: str,
) -> None:
    """The same source roster accepts either layout when measured shape selects it."""

    case = STORAGE_CANARY_CASES[0]
    budget = storage_budget(
        _database_evidence(
            case,
            factor_edge_count=100,
            representation=representation,
        )
    )

    assert budget.case == case


def test_storage_budget_accepts_distinct_factored_bytes() -> None:
    case = STORAGE_CANARY_CASES[1]
    v4_logical_bytes = case.base_layout_logical_bytes // 17
    assert v4_logical_bytes != case.base_layout_logical_bytes

    budget = storage_budget(
        _database_evidence(
            case,
            factor_edge_count=10,
            v4_logical_bytes=v4_logical_bytes,
        )
    )

    assert budget.case.base_layout_logical_bytes == case.base_layout_logical_bytes
    assert budget.v4_factored_layout_logical_bytes == v4_logical_bytes
    assert budget.is_promotion_approved is False


def test_sealed_factor_scale_cannot_self_approve_a_storage_ceiling() -> None:
    case = STORAGE_CANARY_CASES[0]

    budget = storage_budget(_database_evidence(case, factor_edge_count=10**12))

    assert budget.maximum_graph_physical_storage_bytes is None
    assert budget.maximum_snapshot_physical_storage_bytes is None


@pytest.mark.parametrize(
    ("field_path", "replacement", "message"),
    [
        (
            ("reference_equivalence", "reference_source_set", "source_count"),
            99,
            "sealed source set differs",
        ),
        (
            ("reference_equivalence", "same_raw_sources"),
            False,
            "sealed source set differs",
        ),
        (
            (
                "reference_equivalence",
                "reference_snapshot",
                "layout_logical_byte_count",
            ),
            1,
            "storage baseline differs",
        ),
        (
            ("snapshot", "layout_logical_byte_count"),
            1,
            "factored layout evidence",
        ),
        (
            ("root", "state"),
            "building",
            "factored layout evidence",
        ),
        (
            ("root", "representation"),
            "pattern_v1",
            "representation differs from compiler decision",
        ),
        (
            ("exact_counts", "map_logical_byte_count"),
            1,
            "factored layout evidence",
        ),
        (
            ("snapshot", "import_run_id"),
            "",
            "V4 import run id is missing",
        ),
    ],
)
def test_storage_budget_rejects_unsealed_or_changed_bindings(
    field_path: tuple[str, ...],
    replacement: object,
    message: str,
) -> None:
    evidence = _database_evidence(STORAGE_CANARY_CASES[1], factor_edge_count=10)
    cursor = evidence
    for field_name in field_path[:-1]:
        cursor = cursor[field_name]
    cursor[field_path[-1]] = replacement

    with pytest.raises(CanaryConfigurationError, match=message):
        storage_budget(evidence)


def test_accept_cli_has_no_operator_storage_or_representation_override() -> None:
    parser = build_parser()
    subparsers = next(
        action
        for action in parser._actions
        if action.__class__.__name__ == "_SubParsersAction"
    )
    accept_parser = subparsers.choices["accept"]

    assert "--maximum-graph-storage-bytes" not in (accept_parser._option_string_actions)
    assert "--maximum-snapshot-storage-bytes" not in (
        accept_parser._option_string_actions
    )
    assert "--expected-representation" not in (accept_parser._option_string_actions)


def test_acceptance_passes_only_derived_storage_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = STORAGE_CANARY_CASES[2]
    evidence = _database_evidence(case, factor_edge_count=123)
    captured_by_field: dict[str, object] = {}

    def fake_evaluate(
        database_evidence_by_field,
        *,
        storage_budget,
        measurement_image_identity,
        expected_root_counts,
        expected_relation_counts,
    ):
        captured_by_field["evidence"] = database_evidence_by_field
        captured_by_field["budget"] = storage_budget
        captured_by_field["image_identity"] = measurement_image_identity
        captured_by_field["root_counts"] = expected_root_counts
        captured_by_field["relation_counts"] = expected_relation_counts
        return {"passed": True}

    monkeypatch.setattr(acceptance, "evaluate_v4_evidence", fake_evaluate)
    arguments = SimpleNamespace(
        expect_root_count=[],
        expect_relation_count=[],
    )

    report = acceptance._evaluate_publication(
        arguments,
        deepcopy(evidence),
        measurement_image_identity="sha256:measured-image",
    )

    assert report == {"passed": True}
    assert captured_by_field["evidence"] == evidence
    assert captured_by_field["budget"].case.case_name == "reference_extreme_478"
    assert captured_by_field["image_identity"] == "sha256:measured-image"
    assert captured_by_field["root_counts"] == {}
    assert captured_by_field["relation_counts"] == {}
