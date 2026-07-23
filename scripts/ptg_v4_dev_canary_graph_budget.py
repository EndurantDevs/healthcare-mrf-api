"""Source-controlled graph-read budgets for the PTG V4 dev canary."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

from scripts.ptg_v4_dev_canary_graph_evidence import (
    GRAPH_IO_FIELDS,
    INTERNAL_HARD_MAXIMUMS,
    INTERNAL_MAXIMUM_DATABASE_BLOCKS,
    INTERNAL_MAXIMUM_DATABASE_BYTES,
    INTERNAL_MAXIMUM_GRAPH_PAGES,
    INTERNAL_MAXIMUM_LOCATOR_PAGES,
    INTERNAL_MAXIMUM_LOGICAL_LOOKUPS,
    INTERNAL_MAXIMUM_MEMBER_PAGES,
    INTERNAL_MAXIMUM_SOURCE_PAGES,
    collect_graph_read_measurement,
)
from scripts.ptg_v4_dev_canary_measurement_evidence import (
    GRAPH_READ_MEASUREMENT_EVIDENCE_CONTRACT,
    graph_read_measurement_evidence,
    measurement_evidence_sha256,
)

GRAPH_READ_BUDGET_CONTRACT = "ptg_v4_graph_read_budget_v1"
GRAPH_READ_BUDGET_POLICY = "ptg_v4_canary_graph_read_policy_v1"
UNAPPROVED_GRAPH_READ_CEILING_FAILURE = (
    "source-controlled graph-read ceiling is unapproved"
)
APPROVED_TOLERANCE_BASIS_POINTS = 200
MAX_APPROVED_TOLERANCE_BASIS_POINTS = APPROVED_TOLERANCE_BASIS_POINTS


@dataclass(frozen=True)
class GraphReadApproval:
    """Reviewed ceilings derived from one exact V4 measurement run."""

    measurement_reference_snapshot_id: str
    measurement_snapshot_id: str
    measurement_import_run_id: str
    measurement_image_identity: str
    measurement_evidence_sha256: str
    measured_public_database_bytes: int
    measured_public_database_blocks: int
    measured_public_logical_lookups: int
    measured_internal_database_bytes: int
    measured_internal_database_blocks: int
    measured_internal_logical_lookups: int
    tolerance_basis_points: int
    approved_public_database_bytes: int
    approved_public_database_blocks: int
    approved_public_logical_lookups: int
    approved_internal_database_bytes: int
    approved_internal_database_blocks: int
    approved_internal_logical_lookups: int


@dataclass(frozen=True)
class GraphReadCanaryCase:
    """One reviewed reference snapshot and its optional measured approval."""

    case_name: str
    reference_snapshot_id: str
    graph_read_approval: GraphReadApproval | None


GRAPH_READ_CANARY_CASES = (
    GraphReadCanaryCase(
        case_name="direct_baseline_233",
        reference_snapshot_id="ptg2:202607:bbc0656036ca",
        graph_read_approval=None,
    ),
    GraphReadCanaryCase(
        case_name="provider_fragmented_391",
        reference_snapshot_id="ptg2:202607:8a2b4b34d0f9",
        graph_read_approval=None,
    ),
    GraphReadCanaryCase(
        case_name="reference_extreme_478",
        reference_snapshot_id="ptg2:202607:bc93867480ed",
        graph_read_approval=None,
    ),
)
_CASE_BY_REFERENCE_SNAPSHOT_ID = {
    case.reference_snapshot_id: case for case in GRAPH_READ_CANARY_CASES
}


def evaluate_graph_read_budget(
    *,
    reference_snapshot_id: str,
    snapshot_id: str,
    import_run_id: str,
    image_identity: str,
    public_cold_samples: Sequence[Mapping[str, Any]],
    public_warm_graph_evidence: Mapping[str, Any],
    internal_owner_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    """Aggregate measured reads and apply only reviewed source-controlled caps."""

    case = _CASE_BY_REFERENCE_SNAPSHOT_ID.get(reference_snapshot_id)
    if case is None:
        raise ValueError(
            "reference snapshot has no source-controlled graph-read canary case"
        )
    provenance_by_field = _graph_budget_provenance(
        reference_snapshot_id=reference_snapshot_id,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        image_identity=image_identity,
    )
    public_by_field, internal_by_field, failures = (
        collect_graph_read_measurement(
            public_cold_samples=public_cold_samples,
            public_warm_graph_evidence=public_warm_graph_evidence,
            internal_owner_evidence=internal_owner_evidence,
            provenance=provenance_by_field,
        )
    )
    approval = case.graph_read_approval
    if approval is None:
        failures.append(UNAPPROVED_GRAPH_READ_CEILING_FAILURE)
    else:
        _validate_graph_read_approval(case, approval)
        _append_approval_failures(
            approval,
            public_by_field["maximums"],
            internal_by_field["maximums"],
            failures,
        )
    return _graph_budget_report(
        case,
        approval=approval,
        provenance_by_field=provenance_by_field,
        public_by_field=public_by_field,
        internal_by_field=internal_by_field,
        failures=failures,
    )


def _graph_budget_provenance(
    *,
    reference_snapshot_id: str,
    snapshot_id: str,
    import_run_id: str,
    image_identity: str,
) -> dict[str, str]:
    """Validate and return the immutable current-run provenance."""

    return {
        "reference_snapshot_id": _required_text(
            reference_snapshot_id,
            label="reference snapshot id",
        ),
        "snapshot_id": _required_text(snapshot_id, label="V4 snapshot id"),
        "import_run_id": _required_text(import_run_id, label="V4 import run id"),
        "image_identity": _required_text(
            image_identity,
            label="V4 image identity",
        ),
    }


def _graph_budget_report(
    case: GraphReadCanaryCase,
    *,
    approval: GraphReadApproval | None,
    provenance_by_field: Mapping[str, str],
    public_by_field: Mapping[str, Any],
    internal_by_field: Mapping[str, Any],
    failures: Sequence[str],
) -> dict[str, Any]:
    """Shape the reviewable current measurement and policy decision."""

    measurement_evidence_by_field = (
        graph_read_measurement_evidence(
            measurement_reference_snapshot_id=provenance_by_field[
                "reference_snapshot_id"
            ],
            measurement_snapshot_id=provenance_by_field["snapshot_id"],
            measurement_import_run_id=provenance_by_field["import_run_id"],
            measurement_image_identity=provenance_by_field["image_identity"],
            measured_public_by_field=public_by_field["maximums"],
            measured_internal_by_field=internal_by_field["maximums"],
        )
    )
    return {
        "contract": GRAPH_READ_BUDGET_CONTRACT,
        "policy": GRAPH_READ_BUDGET_POLICY,
        "policy_digest": GRAPH_READ_BUDGET_POLICY_DIGEST,
        "case_name": case.case_name,
        **provenance_by_field,
        "promotion_approved": approval is not None,
        "promotion_state": (
            "approved_measured_ceiling"
            if approval is not None
            else "measurement_only_pending_review"
        ),
        "public": dict(public_by_field),
        "internal": dict(internal_by_field),
        "measurement_evidence": measurement_evidence_by_field,
        "measurement_evidence_sha256": measurement_evidence_sha256(
            measurement_evidence_by_field
        ),
        "graph_read_approval": (
            asdict(approval) if approval is not None else None
        ),
        "passed": not failures,
        "failures": sorted(set(failures)),
    }


def _append_approval_failures(
    approval: GraphReadApproval,
    public_maximums: Mapping[str, int],
    internal_maximums: Mapping[str, int],
    failures: list[str],
) -> None:
    public_approved = _approval_values(approval, prefix="approved_public")
    internal_approved = _approval_values(approval, prefix="approved_internal")
    for label, observed, approved in (
        ("public", public_maximums, public_approved),
        ("internal", internal_maximums, internal_approved),
    ):
        if any(
            int(observed.get(field_name) or 0) > approved[field_name]
            for field_name in GRAPH_IO_FIELDS
        ):
            failures.append(
                f"{label} graph reads exceed their source-controlled maximum"
            )


def _required_text(value: Any, *, label: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{label} is missing")
    return normalized


def _approval_values(
    approval: GraphReadApproval,
    *,
    prefix: str,
) -> dict[str, int]:
    return {
        field_name: int(getattr(approval, f"{prefix}_{field_name}"))
        for field_name in GRAPH_IO_FIELDS
    }


def graph_read_measurement_evidence_sha256(
    approval: GraphReadApproval,
) -> str:
    """Hash only the immutable first-pass graph measurement and provenance."""

    evidence_by_field = graph_read_measurement_evidence(
        measurement_reference_snapshot_id=(
            approval.measurement_reference_snapshot_id
        ),
        measurement_snapshot_id=approval.measurement_snapshot_id,
        measurement_import_run_id=approval.measurement_import_run_id,
        measurement_image_identity=approval.measurement_image_identity,
        measured_public_by_field=_approval_values(
            approval,
            prefix="measured_public",
        ),
        measured_internal_by_field=_approval_values(
            approval,
            prefix="measured_internal",
        ),
    )
    return measurement_evidence_sha256(evidence_by_field)


def _validate_graph_read_approval(
    case: GraphReadCanaryCase,
    approval: GraphReadApproval | None,
) -> None:
    """Reject incomplete or inflated checked-in approvals."""

    if approval is None:
        return
    tolerance = int(approval.tolerance_basis_points)
    measured_public = _approval_values(approval, prefix="measured_public")
    measured_internal = _approval_values(approval, prefix="measured_internal")
    approved_public = _approval_values(approval, prefix="approved_public")
    approved_internal = _approval_values(approval, prefix="approved_internal")
    if (
        approval.measurement_reference_snapshot_id
        != case.reference_snapshot_id
        or not approval.measurement_snapshot_id.strip()
        or not approval.measurement_import_run_id.strip()
        or not approval.measurement_image_identity.strip()
        or approval.measurement_evidence_sha256
        != graph_read_measurement_evidence_sha256(approval)
        or tolerance != APPROVED_TOLERANCE_BASIS_POINTS
        or any(
            measured_count <= 0
            for measured_count in measured_public.values()
        )
        or any(
            measured_count <= 0
            for measured_count in measured_internal.values()
        )
        or approved_public
        != _values_with_tolerance(measured_public, tolerance)
        or approved_internal
        != _values_with_tolerance(measured_internal, tolerance)
        or any(
            approved_internal[field_name]
            > INTERNAL_HARD_MAXIMUMS[field_name]
            for field_name in GRAPH_IO_FIELDS
        )
    ):
        raise RuntimeError(
            "PTG V4 graph-read approval is incomplete or inconsistent"
        )


def _values_with_tolerance(
    measured: Mapping[str, int],
    tolerance_basis_points: int,
) -> dict[str, int]:
    return {
        field_name: _ceiling_with_tolerance(
            int(measured[field_name]),
            tolerance_basis_points,
        )
        for field_name in GRAPH_IO_FIELDS
    }


def _ceiling_with_tolerance(value: int, tolerance_basis_points: int) -> int:
    return (value * (10_000 + tolerance_basis_points) + 9_999) // 10_000


def _policy_document() -> dict[str, Any]:
    case_names = {case.case_name for case in GRAPH_READ_CANARY_CASES}
    reference_ids = {
        case.reference_snapshot_id for case in GRAPH_READ_CANARY_CASES
    }
    for case in GRAPH_READ_CANARY_CASES:
        if not case.case_name or not case.reference_snapshot_id:
            raise RuntimeError("PTG V4 graph-read canary case is invalid")
        _validate_graph_read_approval(case, case.graph_read_approval)
    if (
        len(case_names) != len(GRAPH_READ_CANARY_CASES)
        or len(reference_ids) != len(GRAPH_READ_CANARY_CASES)
        or len(_CASE_BY_REFERENCE_SNAPSHOT_ID)
        != len(GRAPH_READ_CANARY_CASES)
    ):
        raise RuntimeError("PTG V4 graph-read canary cases are duplicated")
    return {
        "policy": GRAPH_READ_BUDGET_POLICY,
        "required_approved_tolerance_basis_points": (
            APPROVED_TOLERANCE_BASIS_POINTS
        ),
        "internal_hard_maximums": INTERNAL_HARD_MAXIMUMS,
        "cases": [asdict(case) for case in GRAPH_READ_CANARY_CASES],
    }


GRAPH_READ_BUDGET_POLICY_DIGEST = hashlib.sha256(
    json.dumps(
        _policy_document(),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()


__all__ = [
    "APPROVED_TOLERANCE_BASIS_POINTS",
    "GRAPH_IO_FIELDS",
    "GRAPH_READ_BUDGET_CONTRACT",
    "GRAPH_READ_MEASUREMENT_EVIDENCE_CONTRACT",
    "GRAPH_READ_BUDGET_POLICY",
    "GRAPH_READ_BUDGET_POLICY_DIGEST",
    "GRAPH_READ_CANARY_CASES",
    "INTERNAL_HARD_MAXIMUMS",
    "INTERNAL_MAXIMUM_DATABASE_BLOCKS",
    "INTERNAL_MAXIMUM_DATABASE_BYTES",
    "INTERNAL_MAXIMUM_GRAPH_PAGES",
    "INTERNAL_MAXIMUM_LOCATOR_PAGES",
    "INTERNAL_MAXIMUM_LOGICAL_LOOKUPS",
    "INTERNAL_MAXIMUM_MEMBER_PAGES",
    "INTERNAL_MAXIMUM_SOURCE_PAGES",
    "MAX_APPROVED_TOLERANCE_BASIS_POINTS",
    "UNAPPROVED_GRAPH_READ_CEILING_FAILURE",
    "GraphReadApproval",
    "GraphReadCanaryCase",
    "evaluate_graph_read_budget",
    "graph_read_measurement_evidence_sha256",
]
