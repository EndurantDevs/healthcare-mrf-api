"""Receipt construction and atomic persistence for packed-finalizer ABBA."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Mapping

from process.ptg_parts.ptg2_shared_blocks import SharedMappingDigestSummary


RECEIPT_CONTRACT = "ptg2_packed_finalizer_abba_receipt_v3"


def _completed_arm_receipt(
    arm_receipt: dict[str, Any],
    *,
    fixture_seconds: float,
    prepare_seconds: float,
    mapping_count: int,
) -> dict[str, Any]:
    lifecycle_seconds = (
        prepare_seconds + arm_receipt["publication_plus_summary_seconds"]
    )
    arm_receipt.update(
        {
            "schema_fixture_seconds": fixture_seconds,
            "layout_prepare_seconds": prepare_seconds,
            "prepare_plus_publication_plus_summary_seconds": lifecycle_seconds,
            "prepare_plus_publication_plus_summary_rows_per_second": (
                mapping_count / lifecycle_seconds
            ),
        }
    )
    return arm_receipt


def _summary_receipt(summary: SharedMappingDigestSummary) -> dict[str, Any]:
    return {
        "mapping_digest": summary.mapping_digest.hex(),
        "mapping_count": int(summary.mapping_count),
        "unique_block_count": int(summary.unique_block_count),
        "entry_count": int(summary.entry_count),
        "logical_byte_count": int(summary.logical_byte_count),
        "canonical_byte_count": int(summary.canonical_byte_count),
        "object_kinds": list(summary.object_kinds),
        "packed_mapping_digest": (
            summary.packed_mapping_digest.hex()
            if summary.packed_mapping_digest is not None
            else None
        ),
        "packed_mapping_count": int(summary.packed_mapping_count),
        "relational_mapping_digest": (
            summary.relational_mapping_digest.hex()
            if summary.relational_mapping_digest is not None
            else None
        ),
        "relational_mapping_count": int(summary.relational_mapping_count),
    }


def _native_summary_receipt(summary: Any) -> dict[str, Any]:
    return {
        "mapping_count": int(summary.mapping_count),
        "unique_block_count": int(summary.unique_block_count),
        "entry_count": int(summary.entry_count),
        "logical_byte_count": int(summary.logical_byte_count),
        "object_kinds": list(summary.object_kinds),
        "packed_mapping_digest": summary.packed_mapping_digest.hex(),
        "packed_mapping_count": int(summary.packed_mapping_count),
        "packed_canonical_byte_count": int(summary.packed_canonical_byte_count),
        "relational_mapping_digest": summary.relational_mapping_digest.hex(),
        "relational_mapping_count": int(summary.relational_mapping_count),
    }


def _jsonable(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def initial_receipt(
    shape: Any,
    source_identity_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the fail-closed receipt skeleton for one benchmark run."""

    return {
        "contract": RECEIPT_CONTRACT,
        "status": "incomplete",
        "accepted": False,
        "classification": shape.classification,
        "shape": shape.as_dict(),
        "shape_sha256": shape.sha256(),
        "source": dict(source_identity_by_field),
        "source_boundaries": [],
        "baseline": {
            "source_identity": "current_source",
            "candidate_compatibility_arm": "packed=False",
        },
        "arms": [],
        "cleanup": {
            "schemas": {},
            "work_directories": {},
            "artifact_directory_removed": False,
            "local_root_removed": False,
        },
        "failure_residue": {},
        "acceptance_scope": (
            "representative_source_candidate"
            if shape.is_release_eligible
            else "synthetic_non_representative_mechanism_only"
        ),
        "release_blockers": (
            (
                "runtime failure probes not executed by this receipt",
            )
            if shape.is_release_eligible
            else (
                "no authenticated production per-kind split",
                "synthetic allocation is non-representative",
            )
        ),
    }


def write_receipt(path: Path, receipt_by_field: Mapping[str, Any]) -> None:
    """Atomically persist a terminal or failed benchmark receipt."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary_path.write_text(
        json.dumps(receipt_by_field, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary_path.replace(path)


__all__ = ("RECEIPT_CONTRACT", "initial_receipt", "write_receipt")
