"""Canonical first-pass measurement evidence for PTG V4 approvals."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping


GRAPH_READ_MEASUREMENT_EVIDENCE_CONTRACT = (
    "ptg_v4_graph_read_measurement_evidence_v1"
)
STORAGE_MEASUREMENT_EVIDENCE_CONTRACT = (
    "ptg_v4_physical_storage_measurement_evidence_v1"
)


def graph_read_measurement_evidence(
    *,
    measurement_reference_snapshot_id: str,
    measurement_snapshot_id: str,
    measurement_import_run_id: str,
    measurement_image_identity: str,
    measured_public_by_field: Mapping[str, int],
    measured_internal_by_field: Mapping[str, int],
) -> dict[str, Any]:
    """Return copyable graph evidence from one immutable measurement run."""

    graph_fields = (
        "database_bytes",
        "database_blocks",
        "logical_lookups",
    )
    return {
        "contract": GRAPH_READ_MEASUREMENT_EVIDENCE_CONTRACT,
        "measurement_reference_snapshot_id": (
            measurement_reference_snapshot_id
        ),
        "measurement_snapshot_id": measurement_snapshot_id,
        "measurement_import_run_id": measurement_import_run_id,
        "measurement_image_identity": measurement_image_identity,
        **{
            f"measured_public_{field_name}": int(
                measured_public_by_field[field_name]
            )
            for field_name in graph_fields
        },
        **{
            f"measured_internal_{field_name}": int(
                measured_internal_by_field[field_name]
            )
            for field_name in graph_fields
        },
    }


def physical_storage_measurement_evidence(
    *,
    measurement_reference_snapshot_id: str,
    measurement_snapshot_id: str,
    measurement_import_run_id: str,
    measurement_image_identity: str,
    measured_graph_gate_bytes: int,
    measured_snapshot_gate_bytes: int,
) -> dict[str, Any]:
    """Return copyable storage evidence from one immutable measurement run."""

    return {
        "contract": STORAGE_MEASUREMENT_EVIDENCE_CONTRACT,
        "measurement_reference_snapshot_id": (
            measurement_reference_snapshot_id
        ),
        "measurement_snapshot_id": measurement_snapshot_id,
        "measurement_import_run_id": measurement_import_run_id,
        "measurement_image_identity": measurement_image_identity,
        "measured_graph_gate_bytes": measured_graph_gate_bytes,
        "measured_snapshot_gate_bytes": measured_snapshot_gate_bytes,
    }


def physical_storage_measurement_report(
    *,
    measurement_reference_snapshot_id: str,
    measurement_snapshot_id: str,
    measurement_import_run_id: str,
    measurement_image_identity: str | None,
    measured_graph_gate_bytes: int | None,
    measured_snapshot_gate_bytes: int | None,
) -> dict[str, Any]:
    """Return copyable storage evidence and its digest when measurement exists."""

    normalized_image_identity = str(measurement_image_identity or "").strip()
    has_measurement = (
        measured_graph_gate_bytes is not None
        and measured_snapshot_gate_bytes is not None
        and bool(normalized_image_identity)
    )
    if not has_measurement:
        return {
            "measurement_evidence": None,
            "measurement_evidence_sha256": None,
        }
    evidence_by_field = physical_storage_measurement_evidence(
        measurement_reference_snapshot_id=measurement_reference_snapshot_id,
        measurement_snapshot_id=measurement_snapshot_id,
        measurement_import_run_id=measurement_import_run_id,
        measurement_image_identity=normalized_image_identity,
        measured_graph_gate_bytes=int(measured_graph_gate_bytes),
        measured_snapshot_gate_bytes=int(measured_snapshot_gate_bytes),
    )
    return {
        "measurement_evidence": evidence_by_field,
        "measurement_evidence_sha256": measurement_evidence_sha256(
            evidence_by_field
        ),
    }


def measurement_evidence_sha256(
    evidence_by_field: Mapping[str, Any],
) -> str:
    """Return the stable digest used by a checked-in approval."""

    return hashlib.sha256(
        json.dumps(
            evidence_by_field,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


__all__ = [
    "GRAPH_READ_MEASUREMENT_EVIDENCE_CONTRACT",
    "STORAGE_MEASUREMENT_EVIDENCE_CONTRACT",
    "graph_read_measurement_evidence",
    "measurement_evidence_sha256",
    "physical_storage_measurement_evidence",
    "physical_storage_measurement_report",
]
