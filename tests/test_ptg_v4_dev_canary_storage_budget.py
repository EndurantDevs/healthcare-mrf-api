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


def _database_evidence(
    case: StorageCanaryCase,
    *,
    factor_edge_count: int,
    v4_logical_bytes: int = 123_456_789,
) -> dict[str, object]:
    """Build sealed snapshot, source-set, and compiler resource evidence."""

    source_set_by_field = {
        "contract": PTG2_V3_SOURCE_SET_CONTRACT,
        "source_count": case.source_count,
        "raw_container_sha256_digest": case.source_set_digest,
    }
    return {
        "snapshot": {
            "snapshot_id": f"ptg2:v4:{case.case_name}",
            "import_run_id": f"run_{case.case_name}",
            "snapshot_status": "published",
            "layout_state": "sealed",
            "layout_generation": "shared_blocks_v4",
            "layout_logical_byte_count": v4_logical_bytes,
        },
        "root": {
            "state": "complete",
            "representation": case.expected_representation,
            "logical_byte_count": v4_logical_bytes,
        },
        "exact_counts": {
            "map_logical_byte_count": v4_logical_bytes,
        },
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
                "layout_logical_byte_count": (
                    case.base_layout_logical_bytes
                ),
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
    assert report["graph_gate_bytes"] == 123
    assert report["snapshot_gate_bytes"] == 456


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

    budget = storage_budget(
        _database_evidence(case, factor_edge_count=10**12)
    )

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
            ("reference_equivalence", "reference_snapshot", "layout_logical_byte_count"),
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
            "direct_v1",
            "factored layout evidence",
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

    assert "--maximum-graph-storage-bytes" not in (
        accept_parser._option_string_actions
    )
    assert "--maximum-snapshot-storage-bytes" not in (
        accept_parser._option_string_actions
    )
    assert "--expected-representation" not in (
        accept_parser._option_string_actions
    )


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


def _physical_storage_evidence(
    *,
    graph_gate_bytes: int,
    snapshot_gate_bytes: int,
) -> dict[str, object]:
    """Build fully shaped physical evidence around exact measured gate bytes."""

    retained_raw_by_field = _retained_raw_storage_evidence()
    return {
        "contract": STORAGE_EVIDENCE_CONTRACT,
        "relations": [
            {
                "relation": relation_name,
                "exists": True,
                "total_bytes": 1,
                "attributed_bytes": 1,
            }
            for relation_name in WHOLE_SNAPSHOT_PHYSICAL_RELATIONS
        ],
        "baseline_captured": True,
        "allocation_reconciled": True,
        "missing_required_object_kinds": [],
        "graph_gate_bytes": graph_gate_bytes,
        "snapshot_gate_bytes": snapshot_gate_bytes,
        "retained_raw_artifact_physical_bytes": 2_000,
        "retained_raw_artifacts": retained_raw_by_field,
        "storage_claim_scope": (
            "whole_snapshot_v4_graph_and_retained_raw"
        ),
        "cas": {
            "reference_source": (
                "direct_rows_plus_authenticated_v4_map_payloads"
            ),
            "reference_population": "published_sealed_layout_keys",
            "distinct_referenced_block_count": 1,
            "new_during_import_block_count": 1,
            "preexisting_reused_block_count": 0,
            "shared_block_count": 0,
        },
    }


def _retained_raw_storage_evidence() -> dict[str, object]:
    """Build exact retained compressed-file evidence for storage gates."""

    retained_artifacts = [
        {
            "ordinal": ordinal,
            "source_file_version_id": f"version-{ordinal}",
            "raw_sha256": f"{ordinal:064x}",
            "raw_byte_count": 4_000_000_000,
            "physical_allocated_bytes": 1_000,
            "source_version_reference_count": 1,
            "artifact_manifest_count": 1,
        }
        for ordinal in (1, 2)
    ]
    retained_raw_by_field = {
        "contract": RETAINED_RAW_ARTIFACT_STORAGE_CONTRACT,
        "snapshot_id": "ptg2:v4:provider_fragmented_391",
        "frozen_rate_file_set_sha256": "a" * 64,
        "source_file_version_count": 2,
        "distinct_artifact_count": 2,
        "referenced_raw_bytes": 8_000_000_000,
        "referenced_physical_bytes": 2_000,
        "all_files_verified": True,
        "attribution": "full_referenced_physical_bytes_conservative",
        "artifacts": retained_artifacts,
    }
    retained_raw_by_field["evidence_sha256"] = hashlib.sha256(
        canonical_json_dumps(retained_raw_by_field).encode("utf-8")
    ).hexdigest()
    return retained_raw_by_field


def test_tax_identity_sidecars_are_snapshot_owned_graph_storage() -> None:
    """Keep every token-only sidecar inside both physical storage gates."""

    tax_relations = {
        "ptg2_provider_tax_identity_manifest",
        "ptg2_provider_tax_identity",
        "ptg2_provider_group_tax_identity",
    }
    assert tax_relations <= publication.REQUIRED_PHYSICAL_RELATIONS
    assert tax_relations <= WHOLE_SNAPSHOT_PHYSICAL_RELATIONS
    assert (
        "ptg2_provider_tax_identity_legacy_layout"
        in WHOLE_SNAPSHOT_PHYSICAL_RELATIONS
    )
    for relation_name in tax_relations:
        assert _ownership_predicates(
            relation_name,
            "ptg2:ignored",
            501,
        ) == (
            '"snapshot_key" = $1::bigint',
            '"snapshot_key" IS NOT NULL',
            501,
        )


def _physical_storage_approval(
    *,
    case: StorageCanaryCase,
) -> PhysicalStorageApproval:
    approval = PhysicalStorageApproval(
        measurement_reference_snapshot_id=case.reference_snapshot_id,
        measurement_snapshot_id="ptg2:v4:measured",
        measurement_import_run_id="run_measured",
        measurement_image_identity="sha256:measured-image",
        measurement_evidence_sha256="",
        measured_graph_gate_bytes=1_000,
        measured_snapshot_gate_bytes=2_000,
        tolerance_basis_points=200,
        approved_graph_physical_storage_bytes=1_020,
        approved_snapshot_physical_storage_bytes=2_040,
    )
    return replace(
        approval,
        measurement_evidence_sha256=(
            storage_policy.physical_storage_measurement_evidence_sha256(
                approval
            )
        ),
    )


def test_unapproved_measurement_reports_bytes_but_blocks_promotion() -> None:
    case = STORAGE_CANARY_CASES[1]
    budget = storage_budget(_database_evidence(case, factor_edge_count=10))
    failures: list[str] = []

    publication._validate_physical_storage(
        _physical_storage_evidence(
            graph_gate_bytes=1_000,
            snapshot_gate_bytes=2_000,
        ),
        budget,
        failures,
    )

    assert failures == [UNAPPROVED_STORAGE_CEILING_FAILURE]
    report = budget.report(
        graph_gate_bytes=1_000,
        snapshot_gate_bytes=2_000,
        measurement_image_identity="sha256:measured-image",
    )
    assert report["graph_gate_bytes"] == 1_000
    assert report["snapshot_gate_bytes"] == 2_000
    assert report["promotion_approved"] is False
    assert report["measurement_evidence"] == {
        "contract": storage_policy.STORAGE_MEASUREMENT_EVIDENCE_CONTRACT,
        "measurement_reference_snapshot_id": case.reference_snapshot_id,
        "measurement_snapshot_id": f"ptg2:v4:{case.case_name}",
        "measurement_import_run_id": f"run_{case.case_name}",
        "measurement_image_identity": "sha256:measured-image",
        "measured_graph_gate_bytes": 1_000,
        "measured_snapshot_gate_bytes": 2_000,
    }
    measured_approval = replace(
        _physical_storage_approval(case=case),
        measurement_snapshot_id=f"ptg2:v4:{case.case_name}",
        measurement_import_run_id=f"run_{case.case_name}",
    )
    measured_approval = replace(
        measured_approval,
        measurement_evidence_sha256=(
            storage_policy.physical_storage_measurement_evidence_sha256(
                measured_approval
            )
        ),
    )
    assert report["measurement_evidence_sha256"] == (
        measured_approval.measurement_evidence_sha256
    )


def test_checked_in_absolute_ceiling_requires_exact_reviewed_tolerance() -> None:
    measured_case = STORAGE_CANARY_CASES[1]
    approval = _physical_storage_approval(case=measured_case)
    storage_policy._validate_storage_approval(measured_case, approval)
    approved_case = replace(
        measured_case,
        physical_storage_approval=approval,
    )
    unapproved_budget = storage_budget(
        _database_evidence(measured_case, factor_edge_count=10)
    )
    approved_budget = replace(unapproved_budget, case=approved_case)
    failures: list[str] = []

    publication._validate_physical_storage(
        _physical_storage_evidence(
            graph_gate_bytes=1_020,
            snapshot_gate_bytes=2_040,
        ),
        approved_budget,
        failures,
    )

    assert failures == []
    assert approved_budget.is_promotion_approved is True
