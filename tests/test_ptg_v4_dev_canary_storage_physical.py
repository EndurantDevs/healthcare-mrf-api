"""Physical-storage assertions for the V4 development canaries."""

from __future__ import annotations

from dataclasses import replace
import hashlib

import pytest

from process.ptg_parts.canonical import canonical_json_dumps
from process.ptg_parts.ptg2_shared_gc import (
    PTG2_PROVIDER_TAX_IDENTITY_SOURCE_TABLE_NAMES,
)
from scripts import ptg_v4_dev_canary_publication as publication
from scripts import ptg_v4_dev_canary_storage_budget as storage_policy
from scripts.ptg_v4_dev_canary_publication import (
    STORAGE_EVIDENCE_CONTRACT,
    WHOLE_SNAPSHOT_PHYSICAL_RELATIONS,
)
from scripts.ptg_v4_dev_canary_retained_artifacts import (
    RETAINED_RAW_ARTIFACT_STORAGE_CONTRACT,
)
from scripts.ptg_v4_dev_canary_storage_budget import (
    STORAGE_CANARY_CASES,
    UNAPPROVED_STORAGE_CEILING_FAILURE,
    PhysicalStorageApproval,
    StorageCanaryCase,
    storage_budget,
)
from scripts.ptg_v4_dev_canary_storage import relation_size_rows
from scripts.ptg_v4_dev_canary_storage_sql import _ownership_predicates
from tests.test_ptg_v4_dev_canary_storage_budget import _database_evidence


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


def _physical_storage_evidence(
    *,
    graph_gate_bytes: int,
    snapshot_gate_bytes: int,
) -> dict[str, object]:
    """Build fully shaped physical evidence around exact measured gate bytes."""

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
        "retained_raw_artifacts": _retained_raw_storage_evidence(),
        "storage_claim_scope": "whole_snapshot_v4_graph_and_retained_raw",
        "cas": {
            "reference_source": ("direct_rows_plus_authenticated_v4_map_payloads"),
            "reference_population": "published_sealed_layout_keys",
            "distinct_referenced_block_count": 1,
            "new_during_import_block_count": 1,
            "preexisting_reused_block_count": 0,
            "shared_block_count": 0,
        },
    }


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
        "ptg2_provider_tax_identity_legacy_layout" in WHOLE_SNAPSHOT_PHYSICAL_RELATIONS
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


def test_source_local_tax_identity_relations_count_once_in_snapshot_scope() -> None:
    """Measure every source projection once without changing the graph gate."""

    expected_relations = {
        "ptg2_provider_tax_identity_source_manifest",
        "ptg2_provider_tax_identity_source_binding",
        "ptg2_provider_group_tax_identity_source",
    }
    relation_names = sorted(WHOLE_SNAPSHOT_PHYSICAL_RELATIONS)

    assert set(PTG2_PROVIDER_TAX_IDENTITY_SOURCE_TABLE_NAMES) == expected_relations
    assert expected_relations <= WHOLE_SNAPSHOT_PHYSICAL_RELATIONS
    assert expected_relations.isdisjoint(publication.REQUIRED_PHYSICAL_RELATIONS)
    assert [
        relation_name
        for relation_name in relation_names
        if relation_name in expected_relations
    ] == sorted(expected_relations)
    for relation_name in expected_relations:
        assert _ownership_predicates(
            relation_name,
            "ptg2:ignored",
            501,
        ) == (
            '"snapshot_key" = $1::bigint',
            '"snapshot_key" IS NOT NULL',
            501,
        )


class _SourceRelationSizeConnection:
    def __init__(self) -> None:
        self.size_targets: list[tuple[object, ...]] = []
        self.existence_targets: list[tuple[object, ...]] = []

    async def fetchval(self, query: str, *parameters: object) -> object:
        if "pg_total_relation_size" in query:
            self.size_targets.append(parameters)
            return 8_192
        self.existence_targets.append(parameters)
        return True


@pytest.mark.asyncio
async def test_source_local_storage_uses_exact_schema_and_table_names() -> None:
    """Keep source projection measurement schema-qualified and duplicate-free."""

    connection = _SourceRelationSizeConnection()
    relation_names = sorted(PTG2_PROVIDER_TAX_IDENTITY_SOURCE_TABLE_NAMES)

    rows = await relation_size_rows(
        connection,
        "billing_canary",
        relation_names,
    )

    expected_targets = [
        ("billing_canary", relation_name) for relation_name in relation_names
    ]
    assert connection.size_targets == expected_targets
    assert connection.existence_targets == expected_targets
    assert [row["relation"] for row in rows] == relation_names


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
            storage_policy.physical_storage_measurement_evidence_sha256(approval)
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
    assert report["encoded_persistent_projection_bytes"] == 300
    assert report["graph_physical_minus_encoded_projection_bytes"] == 700
    assert report["graph_physical_to_encoded_projection_basis_points"] == (33_333)
    assert report["graph_projection_drift_within_budget"] is True
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


def test_physical_storage_rejects_generic_encoded_projection_drift() -> None:
    case = STORAGE_CANARY_CASES[1]
    budget = storage_budget(_database_evidence(case, factor_edge_count=10))
    failures: list[str] = []

    publication._validate_physical_storage(
        _physical_storage_evidence(
            graph_gate_bytes=1_501,
            snapshot_gate_bytes=2_000,
        ),
        budget,
        failures,
    )

    assert "physical graph storage exceeds encoded-projection drift budget" in failures
    report = budget.report(graph_gate_bytes=1_501)
    assert report["graph_projection_drift_within_budget"] is False


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
