"""Immutable physical-storage budgets for the PTG V4 dev canary."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Mapping

from process.ptg_parts.ptg2_shared_source_set import (
    PTG2_V3_SOURCE_SET_CONTRACT,
)
from scripts.ptg_v4_dev_canary_budget import sealed_resource_admission
from scripts.ptg_v4_dev_canary_measurement_evidence import (
    STORAGE_MEASUREMENT_EVIDENCE_CONTRACT,
    measurement_evidence_sha256,
    physical_storage_measurement_evidence,
    physical_storage_measurement_report,
)
from scripts.ptg_v4_dev_canary_support import CanaryConfigurationError


STORAGE_BUDGET_CONTRACT = "ptg_v4_physical_storage_budget_v1"
STORAGE_BUDGET_POLICY = "ptg_v4_canary_storage_policy_v1"
UNAPPROVED_STORAGE_CEILING_FAILURE = (
    "source-controlled physical storage ceiling has not been approved "
    "from a measured V4 import"
)
APPROVED_TOLERANCE_BASIS_POINTS = 200
MAX_APPROVED_TOLERANCE_BASIS_POINTS = APPROVED_TOLERANCE_BASIS_POINTS
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
    expected_representation: str
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
        expected_representation="direct_v1",
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
        expected_representation="pattern_v1",
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
        expected_representation="pattern_v1",
        physical_storage_approval=None,
    ),
)
_CASE_BY_REFERENCE_SNAPSHOT_ID = {
    case.reference_snapshot_id: case for case in STORAGE_CANARY_CASES
}


@dataclass(frozen=True)
class StorageBudget:
    """A source-controlled ceiling bound to one sealed import and source set."""

    case: StorageCanaryCase
    snapshot_id: str
    import_run_id: str
    v4_factored_layout_logical_bytes: int
    compressed_acquisition_bytes: int
    input_factor_bytes: int
    factor_edge_count: int

    @property
    def promotion_approved(self) -> bool:
        """Return whether reviewed absolute physical ceilings exist."""

        return self.case.physical_storage_approval is not None

    @property
    def maximum_graph_physical_storage_bytes(self) -> int | None:
        """Return the reviewed absolute graph ceiling, if approved."""

        approval = self.case.physical_storage_approval
        return (
            approval.approved_graph_physical_storage_bytes
            if approval is not None
            else None
        )

    @property
    def maximum_snapshot_physical_storage_bytes(self) -> int | None:
        """Return the reviewed absolute whole-snapshot ceiling, if approved."""

        approval = self.case.physical_storage_approval
        return (
            approval.approved_snapshot_physical_storage_bytes
            if approval is not None
            else None
        )

    def report(
        self,
        *,
        graph_gate_bytes: int | None = None,
        snapshot_gate_bytes: int | None = None,
        measurement_image_identity: str | None = None,
    ) -> dict[str, Any]:
        """Return the exact immutable policy, bindings, and derived ceilings."""

        approval = self.case.physical_storage_approval
        return {
            "contract": STORAGE_BUDGET_CONTRACT,
            "policy": STORAGE_BUDGET_POLICY,
            "policy_digest": STORAGE_BUDGET_POLICY_DIGEST,
            "promotion_approved": self.promotion_approved,
            "promotion_state": (
                "approved_absolute_ceiling"
                if self.promotion_approved
                else "measurement_only_pending_review"
            ),
            "case_name": self.case.case_name,
            "reference_snapshot_id": self.case.reference_snapshot_id,
            "source_count": self.case.source_count,
            "source_set_digest": self.case.source_set_digest,
            "snapshot_id": self.snapshot_id,
            "import_run_id": self.import_run_id,
            "expected_representation": self.case.expected_representation,
            "base_layout_logical_bytes": self.case.base_layout_logical_bytes,
            "v4_factored_layout_logical_bytes": (
                self.v4_factored_layout_logical_bytes
            ),
            "compressed_acquisition_bytes": self.compressed_acquisition_bytes,
            "input_factor_bytes": self.input_factor_bytes,
            "factor_edge_count": self.factor_edge_count,
            "graph_gate_bytes": graph_gate_bytes,
            "snapshot_gate_bytes": snapshot_gate_bytes,
            **physical_storage_measurement_report(
                measurement_reference_snapshot_id=(
                    self.case.reference_snapshot_id
                ),
                measurement_snapshot_id=self.snapshot_id,
                measurement_import_run_id=self.import_run_id,
                measurement_image_identity=measurement_image_identity,
                measured_graph_gate_bytes=graph_gate_bytes,
                measured_snapshot_gate_bytes=snapshot_gate_bytes,
            ),
            "physical_storage_approval": (
                asdict(approval) if approval is not None else None
            ),
            "maximum_graph_physical_storage_bytes": (
                self.maximum_graph_physical_storage_bytes
            ),
            "maximum_snapshot_physical_storage_bytes": (
                self.maximum_snapshot_physical_storage_bytes
            ),
        }


def storage_budget(
    database_evidence_by_field: Mapping[str, Any],
) -> StorageBudget:
    """Derive a non-overridable ceiling from sealed DB/import evidence."""

    snapshot = _mapping(
        database_evidence_by_field.get("snapshot"),
        label="published V4 snapshot evidence",
    )
    root = _mapping(
        database_evidence_by_field.get("root"),
        label="completed V4 snapshot-map root evidence",
    )
    exact_counts = _mapping(
        database_evidence_by_field.get("exact_counts"),
        label="exact V4 snapshot count evidence",
    )
    equivalence = _mapping(
        database_evidence_by_field.get("reference_equivalence"),
        label="reference-equivalence evidence",
    )
    reference_snapshot_id = _required_text(
        equivalence.get("reference_snapshot_id"),
        label="reference snapshot id",
    )
    case = _CASE_BY_REFERENCE_SNAPSHOT_ID.get(reference_snapshot_id)
    if case is None:
        raise CanaryConfigurationError(
            "sealed source set has no source-controlled storage canary case"
        )
    snapshot_id, import_run_id = _validate_snapshot_binding(
        snapshot,
        equivalence,
        case,
    )
    v4_factored_layout_logical_bytes = _validate_v4_factored_layout(
        snapshot,
        root,
        exact_counts,
        case,
    )
    _validate_source_set_binding(equivalence, case)
    resources = sealed_resource_admission(database_evidence_by_field)
    return StorageBudget(
        case=case,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        v4_factored_layout_logical_bytes=v4_factored_layout_logical_bytes,
        compressed_acquisition_bytes=resources["compressed_acquisition_bytes"],
        input_factor_bytes=resources["input_factor_bytes"],
        factor_edge_count=resources["factor_edge_count"],
    )


def _validate_snapshot_binding(
    snapshot: Mapping[str, Any],
    equivalence: Mapping[str, Any],
    case: StorageCanaryCase,
) -> tuple[str, str]:
    """Bind the policy to one sealed V4 import and immutable V3 baseline."""

    snapshot_id = _required_text(
        snapshot.get("snapshot_id"),
        label="V4 snapshot id",
    )
    import_run_id = _required_text(
        snapshot.get("import_run_id"),
        label="V4 import run id",
    )
    reference_snapshot = _mapping(
        equivalence.get("reference_snapshot"),
        label="reference snapshot evidence",
    )
    if (
        equivalence.get("v4_snapshot_id") != snapshot_id
        or reference_snapshot.get("snapshot_id") != case.reference_snapshot_id
        or reference_snapshot.get("snapshot_status") != "published"
        or reference_snapshot.get("layout_state") != "sealed"
        or reference_snapshot.get("layout_generation") != "shared_blocks_v3"
        or snapshot.get("snapshot_status") != "published"
        or snapshot.get("layout_state") != "sealed"
        or snapshot.get("layout_generation") != "shared_blocks_v4"
        or _strict_nonnegative_int(
            reference_snapshot.get("layout_logical_byte_count"),
            label="reference layout logical byte count",
        )
        != case.base_layout_logical_bytes
    ):
        raise CanaryConfigurationError(
            "sealed snapshot storage baseline differs from source control"
        )
    return snapshot_id, import_run_id


def _validate_v4_factored_layout(
    snapshot: Mapping[str, Any],
    root: Mapping[str, Any],
    exact_counts: Mapping[str, Any],
    case: StorageCanaryCase,
) -> int:
    """Validate and return the measured logical size of the factored V4 graph."""

    layout_logical_bytes = _strict_positive_int(
        snapshot.get("layout_logical_byte_count"),
        label="V4 factored layout logical byte count",
    )
    root_logical_bytes = _strict_positive_int(
        root.get("logical_byte_count"),
        label="V4 snapshot-map root logical byte count",
    )
    exact_logical_bytes = _strict_positive_int(
        exact_counts.get("map_logical_byte_count"),
        label="exact V4 snapshot-map logical byte count",
    )
    if (
        root.get("state") != "complete"
        or root.get("representation") != case.expected_representation
        or layout_logical_bytes != root_logical_bytes
        or root_logical_bytes != exact_logical_bytes
    ):
        raise CanaryConfigurationError(
            "sealed V4 factored layout evidence is incomplete or inconsistent"
        )
    return layout_logical_bytes


def _validate_source_set_binding(
    equivalence: Mapping[str, Any],
    case: StorageCanaryCase,
) -> None:
    """Require both snapshots to identify the reviewed raw-container set."""

    expected_source_set = {
        "contract": PTG2_V3_SOURCE_SET_CONTRACT,
        "source_count": case.source_count,
        "raw_container_sha256_digest": case.source_set_digest,
    }
    if (
        equivalence.get("same_raw_sources") is not True
        or equivalence.get("same_source_trace_sets") is not True
        or equivalence.get("v4_source_set") != expected_source_set
        or equivalence.get("reference_source_set") != expected_source_set
    ):
        raise CanaryConfigurationError(
            "sealed source set differs from the source-controlled storage case"
        )


def _mapping(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise CanaryConfigurationError(f"{label} is missing")
    return dict(value)


def _required_text(value: Any, *, label: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise CanaryConfigurationError(f"{label} is missing")
    return normalized


def _strict_nonnegative_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool):
        raise CanaryConfigurationError(f"{label} is invalid")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise CanaryConfigurationError(f"{label} is invalid") from exc
    if normalized < 0:
        raise CanaryConfigurationError(f"{label} is invalid")
    return normalized


def _strict_positive_int(value: Any, *, label: str) -> int:
    normalized = _strict_nonnegative_int(value, label=label)
    if normalized == 0:
        raise CanaryConfigurationError(f"{label} is invalid")
    return normalized


def _policy_document() -> dict[str, Any]:
    """Return the canonical source-controlled policy document."""

    case_names = {case.case_name for case in STORAGE_CANARY_CASES}
    for case in STORAGE_CANARY_CASES:
        if (
            not case.case_name
            or not case.reference_snapshot_id
            or case.source_count <= 0
            or case.base_layout_logical_bytes <= 0
            or not _SHA256_PATTERN.fullmatch(case.source_set_digest)
            or case.expected_representation not in {"direct_v1", "pattern_v1"}
        ):
            raise RuntimeError("PTG V4 source-controlled storage case is invalid")
        _validate_storage_approval(case, case.physical_storage_approval)
    if (
        len(case_names) != len(STORAGE_CANARY_CASES)
        or len(_CASE_BY_REFERENCE_SNAPSHOT_ID) != len(STORAGE_CANARY_CASES)
    ):
        raise RuntimeError("PTG V4 source-controlled storage cases are duplicated")
    return {
        "policy": STORAGE_BUDGET_POLICY,
        "required_approved_tolerance_basis_points": (
            APPROVED_TOLERANCE_BASIS_POINTS
        ),
        "cases": [asdict(case) for case in STORAGE_CANARY_CASES],
    }


def physical_storage_measurement_evidence_sha256(
    approval: PhysicalStorageApproval,
) -> str:
    """Hash only the immutable first-pass storage measurement and provenance."""

    evidence_by_field = physical_storage_measurement_evidence(
        measurement_reference_snapshot_id=(
            approval.measurement_reference_snapshot_id
        ),
        measurement_snapshot_id=approval.measurement_snapshot_id,
        measurement_import_run_id=approval.measurement_import_run_id,
        measurement_image_identity=approval.measurement_image_identity,
        measured_graph_gate_bytes=approval.measured_graph_gate_bytes,
        measured_snapshot_gate_bytes=approval.measured_snapshot_gate_bytes,
    )
    return measurement_evidence_sha256(evidence_by_field)


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
        approval.measurement_reference_snapshot_id
        != case.reference_snapshot_id
        or not approval.measurement_snapshot_id.strip()
        or not approval.measurement_import_run_id.strip()
        or not approval.measurement_image_identity.strip()
        or approval.measurement_evidence_sha256
        != physical_storage_measurement_evidence_sha256(approval)
        or any(value <= 0 for value in measured_values)
        or any(value <= 0 for value in approved_values)
        or tolerance != APPROVED_TOLERANCE_BASIS_POINTS
        or approved_values != expected_approved_values
    ):
        raise RuntimeError(
            "PTG V4 physical storage approval is incomplete or inconsistent"
        )


def _ceiling_with_tolerance(measured_bytes: int, tolerance_basis_points: int) -> int:
    """Apply a reviewed basis-point tolerance using exact integer rounding."""

    return (
        measured_bytes * (10_000 + tolerance_basis_points) + 9_999
    ) // 10_000


STORAGE_BUDGET_POLICY_DIGEST = hashlib.sha256(
    json.dumps(
        _policy_document(),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()


__all__ = [
    "APPROVED_TOLERANCE_BASIS_POINTS",
    "MAX_APPROVED_TOLERANCE_BASIS_POINTS",
    "STORAGE_BUDGET_CONTRACT",
    "STORAGE_BUDGET_POLICY",
    "STORAGE_BUDGET_POLICY_DIGEST",
    "STORAGE_CANARY_CASES",
    "STORAGE_MEASUREMENT_EVIDENCE_CONTRACT",
    "UNAPPROVED_STORAGE_CEILING_FAILURE",
    "PhysicalStorageApproval",
    "StorageBudget",
    "StorageCanaryCase",
    "physical_storage_measurement_evidence_sha256",
    "storage_budget",
]
