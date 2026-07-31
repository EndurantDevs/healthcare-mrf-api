"""Immutable physical-storage budgets for the PTG V4 dev canary."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

from process.ptg_parts.ptg2_shared_source_set import (
    PTG2_V3_SOURCE_SET_CONTRACT,
)
from process.ptg_parts.ptg2_v4_graph_compiler import (
    PTG2_V4_ADAPTIVE_LAYOUT_COST_CONTRACT,
    validate_v4_adaptive_layout_decision,
)
from scripts.ptg_v4_dev_canary_budget import sealed_resource_admission
from scripts.ptg_v4_dev_canary_measurement_evidence import (
    STORAGE_MEASUREMENT_EVIDENCE_CONTRACT,
    physical_storage_measurement_report,
)
from scripts.ptg_v4_dev_canary_storage_policy import (
    APPROVED_TOLERANCE_BASIS_POINTS,
    CASE_BY_REFERENCE_SNAPSHOT_ID,
    MAX_APPROVED_TOLERANCE_BASIS_POINTS,
    MAX_GRAPH_PHYSICAL_TO_ENCODED_PROJECTION_BASIS_POINTS,
    STORAGE_BUDGET_CONTRACT,
    STORAGE_BUDGET_POLICY,
    STORAGE_BUDGET_POLICY_DIGEST,
    STORAGE_CANARY_CASES,
    UNAPPROVED_STORAGE_CEILING_FAILURE,
    PhysicalStorageApproval,
    StorageCanaryCase,
    _validate_storage_approval,
    physical_storage_measurement_evidence_sha256,
)
from scripts.ptg_v4_dev_canary_support import CanaryConfigurationError


_CASE_BY_REFERENCE_SNAPSHOT_ID = {**CASE_BY_REFERENCE_SNAPSHOT_ID}


@dataclass(frozen=True)
class StorageBudget:
    """A source-controlled ceiling bound to one sealed import and source set."""

    case: StorageCanaryCase
    snapshot_id: str
    import_run_id: str
    v4_factored_layout_logical_bytes: int
    encoded_persistent_projection_bytes: int
    compressed_acquisition_bytes: int
    input_factor_bytes: int
    factor_edge_count: int

    @property
    def is_promotion_approved(self) -> bool:
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

        return {
            **_storage_identity_report(self),
            **_projection_storage_report(self, graph_gate_bytes),
            "graph_gate_bytes": graph_gate_bytes,
            "snapshot_gate_bytes": snapshot_gate_bytes,
            **physical_storage_measurement_report(
                measurement_reference_snapshot_id=(self.case.reference_snapshot_id),
                measurement_snapshot_id=self.snapshot_id,
                measurement_import_run_id=self.import_run_id,
                measurement_image_identity=measurement_image_identity,
                measured_graph_gate_bytes=graph_gate_bytes,
                measured_snapshot_gate_bytes=snapshot_gate_bytes,
            ),
            **_storage_approval_report(self),
        }


def _storage_identity_report(budget: StorageBudget) -> dict[str, Any]:
    return {
        "contract": STORAGE_BUDGET_CONTRACT,
        "policy": STORAGE_BUDGET_POLICY,
        "policy_digest": STORAGE_BUDGET_POLICY_DIGEST,
        "promotion_approved": budget.is_promotion_approved,
        "promotion_state": (
            "approved_absolute_ceiling"
            if budget.is_promotion_approved
            else "measurement_only_pending_review"
        ),
        "case_name": budget.case.case_name,
        "reference_snapshot_id": budget.case.reference_snapshot_id,
        "source_count": budget.case.source_count,
        "source_set_digest": budget.case.source_set_digest,
        "snapshot_id": budget.snapshot_id,
        "import_run_id": budget.import_run_id,
        "base_layout_logical_bytes": budget.case.base_layout_logical_bytes,
        "v4_factored_layout_logical_bytes": (budget.v4_factored_layout_logical_bytes),
        "compressed_acquisition_bytes": budget.compressed_acquisition_bytes,
        "input_factor_bytes": budget.input_factor_bytes,
        "factor_edge_count": budget.factor_edge_count,
    }


def _projection_storage_report(
    budget: StorageBudget,
    graph_gate_bytes: int | None,
) -> dict[str, Any]:
    residual = (
        graph_gate_bytes - budget.encoded_persistent_projection_bytes
        if graph_gate_bytes is not None
        else None
    )
    basis_points = (
        graph_gate_bytes * 10_000 // budget.encoded_persistent_projection_bytes
        if graph_gate_bytes is not None
        else None
    )
    return {
        "encoded_persistent_projection_contract": (
            PTG2_V4_ADAPTIVE_LAYOUT_COST_CONTRACT
        ),
        "encoded_persistent_projection_bytes": (
            budget.encoded_persistent_projection_bytes
        ),
        "graph_physical_minus_encoded_projection_bytes": residual,
        "graph_physical_to_encoded_projection_basis_points": basis_points,
        "maximum_graph_physical_to_encoded_projection_basis_points": (
            MAX_GRAPH_PHYSICAL_TO_ENCODED_PROJECTION_BASIS_POINTS
        ),
        "graph_projection_drift_within_budget": (
            basis_points <= MAX_GRAPH_PHYSICAL_TO_ENCODED_PROJECTION_BASIS_POINTS
            if basis_points is not None
            else None
        ),
    }


def _storage_approval_report(budget: StorageBudget) -> dict[str, Any]:
    approval = budget.case.physical_storage_approval
    return {
        "physical_storage_approval": (
            asdict(approval) if approval is not None else None
        ),
        "maximum_graph_physical_storage_bytes": (
            budget.maximum_graph_physical_storage_bytes
        ),
        "maximum_snapshot_physical_storage_bytes": (
            budget.maximum_snapshot_physical_storage_bytes
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
    (
        v4_factored_layout_logical_bytes,
        encoded_persistent_projection_bytes,
    ) = _validate_v4_factored_layout(
        snapshot,
        root,
        exact_counts,
    )
    _validate_source_set_binding(equivalence, case)
    resources = sealed_resource_admission(database_evidence_by_field)
    return StorageBudget(
        case=case,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        v4_factored_layout_logical_bytes=v4_factored_layout_logical_bytes,
        encoded_persistent_projection_bytes=(encoded_persistent_projection_bytes),
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


def _adaptive_layout_from_snapshot(
    snapshot: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    layout_manifest = _mapping(
        snapshot.get("layout_manifest"),
        label="sealed V4 layout manifest",
    )
    serving_index = _mapping(
        layout_manifest.get("serving_index"),
        label="sealed V4 serving index",
    )
    serving_binary = _mapping(
        serving_index.get("serving_binary"),
        label="sealed V4 serving binary",
    )
    provider_graph = _mapping(
        serving_binary.get("provider_graph_v4"),
        label="sealed V4 provider graph",
    )
    try:
        adaptive_layout_map = validate_v4_adaptive_layout_decision(
            provider_graph.get("adaptive_layout")
        )
    except RuntimeError as exc:
        raise CanaryConfigurationError(
            "sealed V4 adaptive layout decision is invalid"
        ) from exc
    return provider_graph, adaptive_layout_map


def _validate_v4_factored_layout(
    snapshot: Mapping[str, Any],
    root: Mapping[str, Any],
    exact_counts: Mapping[str, Any],
) -> tuple[int, int]:
    """Return sealed logical bytes and the selected encoded projection cost."""

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
        or layout_logical_bytes != root_logical_bytes
        or root_logical_bytes != exact_logical_bytes
    ):
        raise CanaryConfigurationError(
            "sealed V4 factored layout evidence is incomplete or inconsistent"
        )
    provider_graph, adaptive_layout_map = _adaptive_layout_from_snapshot(snapshot)
    if (
        root.get("representation") != adaptive_layout_map["selected_representation"]
        or provider_graph.get("representation")
        != adaptive_layout_map["selected_representation"]
    ):
        raise CanaryConfigurationError(
            "sealed V4 representation differs from compiler decision"
        )
    return (
        layout_logical_bytes,
        adaptive_layout_map["selected_encoded_bytes"],
    )


def _validate_source_set_binding(
    equivalence: Mapping[str, Any],
    case: StorageCanaryCase,
) -> None:
    """Require both snapshots to identify the reviewed raw-container set."""

    expected_source_set_by_field = {
        "contract": PTG2_V3_SOURCE_SET_CONTRACT,
        "source_count": case.source_count,
        "raw_container_sha256_digest": case.source_set_digest,
    }
    if (
        equivalence.get("same_raw_sources") is not True
        or equivalence.get("same_source_trace_sets") is not True
        or equivalence.get("v4_source_set") != expected_source_set_by_field
        or equivalence.get("reference_source_set") != expected_source_set_by_field
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
