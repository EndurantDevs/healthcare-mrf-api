"""Immutable source-controlled policy for PTG V4 storage canaries."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass

from scripts.ptg_v4_dev_canary_measurement_evidence import (
    measurement_evidence_sha256,
    physical_storage_measurement_evidence,
)


STORAGE_BUDGET_CONTRACT = "ptg_v4_physical_storage_budget_v1"
STORAGE_BUDGET_POLICY = "ptg_v4_canary_storage_policy_v1"
UNAPPROVED_STORAGE_CEILING_FAILURE = (
    "source-controlled physical storage ceiling has not been approved "
    "from a measured V4 import"
)
APPROVED_TOLERANCE_BASIS_POINTS = 200
MAX_APPROVED_TOLERANCE_BASIS_POINTS = APPROVED_TOLERANCE_BASIS_POINTS
MAX_GRAPH_PHYSICAL_TO_ENCODED_PROJECTION_BASIS_POINTS = 50_000
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class PhysicalStorageApproval:
    """Reviewed absolute ceilings derived from one exact V4 measure run."""

    measurement_reference_snapshot_id: str
    measurement_snapshot_id: str
    measurement_import_run_id: str
    measurement_image_identity: str
    measurement_evidence_sha256: str
    measured_graph_gate_bytes: int
    measured_snapshot_gate_bytes: int
    tolerance_basis_points: int
    approved_graph_physical_storage_bytes: int
    approved_snapshot_physical_storage_bytes: int


@dataclass(frozen=True)
class StorageCanaryCase:
    """One reviewed source set and its immutable retained-layout baseline."""

    case_name: str
    reference_snapshot_id: str
    source_count: int
    source_set_digest: str
    base_layout_logical_bytes: int
    physical_storage_approval: PhysicalStorageApproval | None


STORAGE_CANARY_CASES = (
    StorageCanaryCase(
        case_name="direct_baseline_233",
        reference_snapshot_id="ptg2:202607:bbc0656036ca",
        source_count=1,
        source_set_digest=(
            "9d60244f638c39918f382130998fea3df82f0e78bd489a77995557b9dc9b5e6e"
        ),
        base_layout_logical_bytes=4_352_379_985,
        physical_storage_approval=None,
    ),
    StorageCanaryCase(
        case_name="provider_fragmented_391",
        reference_snapshot_id="ptg2:202607:8a2b4b34d0f9",
        source_count=1,
        source_set_digest=(
            "680b275944c3e19df52d196c69a8d79774faffd36cbe57c8086152853f631ebc"
        ),
        base_layout_logical_bytes=3_262_270_957,
        physical_storage_approval=None,
    ),
    StorageCanaryCase(
        case_name="reference_extreme_478",
        reference_snapshot_id="ptg2:202607:bc93867480ed",
        source_count=7,
        source_set_digest=(
            "390880da0d9f35b707f4e0d65a6c87721f31a9034397a86df110c0ddba01cd27"
        ),
        base_layout_logical_bytes=410_519_582,
        physical_storage_approval=None,
    ),
)
CASE_BY_REFERENCE_SNAPSHOT_ID = {
    case.reference_snapshot_id: case for case in STORAGE_CANARY_CASES
}


def physical_storage_measurement_evidence_sha256(
    approval: PhysicalStorageApproval,
) -> str:
    """Hash only the immutable first-pass storage measurement and provenance."""

    evidence_by_field = physical_storage_measurement_evidence(
        measurement_reference_snapshot_id=(approval.measurement_reference_snapshot_id),
        measurement_snapshot_id=approval.measurement_snapshot_id,
        measurement_import_run_id=approval.measurement_import_run_id,
        measurement_image_identity=approval.measurement_image_identity,
        measured_graph_gate_bytes=approval.measured_graph_gate_bytes,
        measured_snapshot_gate_bytes=approval.measured_snapshot_gate_bytes,
    )
    return measurement_evidence_sha256(evidence_by_field)


def _ceiling_with_tolerance(
    measured_bytes: int,
    tolerance_basis_points: int,
) -> int:
    """Apply a reviewed basis-point tolerance using exact integer rounding."""

    return (measured_bytes * (10_000 + tolerance_basis_points) + 9_999) // 10_000


def _validate_storage_approval(
    case: StorageCanaryCase,
    approval: PhysicalStorageApproval | None,
) -> None:
    """Reject incomplete or non-reproducible checked-in approvals."""

    if approval is None:
        return
    tolerance = approval.tolerance_basis_points
    measured_values = (
        approval.measured_graph_gate_bytes,
        approval.measured_snapshot_gate_bytes,
    )
    approved_values = (
        approval.approved_graph_physical_storage_bytes,
        approval.approved_snapshot_physical_storage_bytes,
    )
    expected_approved_values = tuple(
        _ceiling_with_tolerance(measured_value, tolerance)
        for measured_value in measured_values
    )
    if (
        approval.measurement_reference_snapshot_id != case.reference_snapshot_id
        or not approval.measurement_snapshot_id.strip()
        or not approval.measurement_import_run_id.strip()
        or not approval.measurement_image_identity.strip()
        or approval.measurement_evidence_sha256
        != physical_storage_measurement_evidence_sha256(approval)
        or any(measured_value <= 0 for measured_value in measured_values)
        or any(approved_value <= 0 for approved_value in approved_values)
        or tolerance != APPROVED_TOLERANCE_BASIS_POINTS
        or approved_values != expected_approved_values
    ):
        raise RuntimeError(
            "PTG V4 physical storage approval is incomplete or inconsistent"
        )


def _storage_policy_document() -> dict[str, object]:
    """Return the canonical source-controlled policy document."""

    case_names = {case.case_name for case in STORAGE_CANARY_CASES}
    for case in STORAGE_CANARY_CASES:
        if (
            not case.case_name
            or not case.reference_snapshot_id
            or case.source_count <= 0
            or case.base_layout_logical_bytes <= 0
            or not _SHA256_PATTERN.fullmatch(case.source_set_digest)
        ):
            raise RuntimeError("PTG V4 source-controlled storage case is invalid")
        _validate_storage_approval(case, case.physical_storage_approval)
    if len(case_names) != len(STORAGE_CANARY_CASES) or len(
        CASE_BY_REFERENCE_SNAPSHOT_ID
    ) != len(STORAGE_CANARY_CASES):
        raise RuntimeError("PTG V4 source-controlled storage cases are duplicated")
    return {
        "policy": STORAGE_BUDGET_POLICY,
        "required_approved_tolerance_basis_points": (APPROVED_TOLERANCE_BASIS_POINTS),
        "maximum_graph_physical_to_encoded_projection_basis_points": (
            MAX_GRAPH_PHYSICAL_TO_ENCODED_PROJECTION_BASIS_POINTS
        ),
        "cases": [asdict(case) for case in STORAGE_CANARY_CASES],
    }


STORAGE_BUDGET_POLICY_DIGEST = hashlib.sha256(
    json.dumps(
        _storage_policy_document(),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()
