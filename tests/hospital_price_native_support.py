# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixture builders for native hospital-price parser tests."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any


_PG_BINARY_COPY_HEADER = b"PGCOPY\n\xff\r\n\0" + b"\0" * 8
_PG_BINARY_COPY_TRAILER = b"\xff\xff"


def _packed_root_dict(fact_count: int) -> dict[str, int]:
    payer_key_count = 2 if fact_count else 0
    payer_page_count = 2 if fact_count else 0
    code_ref_count = 4
    return {
        "service_count": 3,
        "charge_count": 4,
        "fact_count": fact_count,
        "code_selector_key_count": 2,
        "payer_plan_selector_key_count": payer_key_count,
        "code_selector_ref_count": code_ref_count,
        "payer_plan_selector_ref_count": fact_count,
        "code_selector_page_count": 2,
        "payer_plan_selector_page_count": payer_page_count,
        "service_block_count": 1,
        "fact_block_count": 1 if fact_count else 0,
        "code_selector_block_count": 2,
        "payer_plan_selector_block_count": payer_page_count,
        "selector_spool_bytes": 13 * (code_ref_count + fact_count),
        "peak_scratch_bytes": 39 * (code_ref_count + fact_count),
    }


def _row_counts_by_kind(packed_root_dict: dict[str, int]) -> dict[str, int]:
    return {
        "mrf": 1,
        "location": 1,
        "npi": 1,
        "license": 1,
        "contract_provision": 0,
        "modifier": 0,
        "modifier_payer": 0,
        "service_block": packed_root_dict["service_block_count"],
        "fact_block": packed_root_dict["fact_block_count"],
        "selector_page": (
            packed_root_dict["code_selector_page_count"]
            + packed_root_dict["payer_plan_selector_page_count"]
        ),
    }


def _copy_bytes(native_module: Any, kind: str, row_count: int) -> bytes:
    if kind not in native_module.HOSPITAL_MRF_BINARY_COPY_KINDS:
        return f"{kind}\n".encode() if row_count else b""
    binary_record = b"\0\r" + b"\xff\xff\xff\xff" * 13
    return (
        _PG_BINARY_COPY_HEADER
        + binary_record * row_count
        + _PG_BINARY_COPY_TRAILER
    )


def packed_summary(
    native_module: Any,
    output_directory: Path,
    *,
    fact_count: int = 5,
    max_output_bytes: int = 4096,
) -> dict[str, Any]:
    """Build a valid packed parser summary and its exact COPY files."""

    packed_root_dict = _packed_root_dict(fact_count)
    row_counts_by_kind = _row_counts_by_kind(packed_root_dict)
    artifact_dicts = []
    for kind in native_module.HOSPITAL_MRF_COPY_COLUMNS:
        row_count = row_counts_by_kind[kind]
        copy_bytes = _copy_bytes(native_module, kind, row_count)
        copy_path = output_directory / f"{kind}.copy"
        copy_path.write_bytes(copy_bytes)
        artifact_dicts.append({
            "kind": kind,
            "path": str(copy_path),
            "rows": row_count,
            "bytes": len(copy_bytes),
            "sha256": hashlib.sha256(copy_bytes).hexdigest(),
        })
    return {
        "contract": "hospital-mrf-copy-v3-packed-v1",
        "version_id": "a" * 64,
        "schema_version": "3.0.0",
        "schema_revision": native_module.HOSPITAL_MRF_SCHEMA_REVISION,
        "format": "json",
        "compressed_input_bytes": 123,
        "max_fanout_rows": 100_000,
        "max_decompressed_bytes": 2048,
        "max_output_bytes": max_output_bytes,
        "artifacts": artifact_dicts,
        "root": packed_root_dict,
    }
