# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read authenticated source-witness bundles emitted by the Rust scanner."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from process.ptg_parts.ptg2_source_witness_codec import decode_record
from process.ptg_parts.ptg2_source_witness_contract import (
    CompressedSourceWitnessRecord,
    PTG2_V3_SOURCE_WITNESS_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_MAX_BUNDLE_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES,
    PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET,
    PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
    PTG2_V3_SOURCE_WITNESS_SELECTION,
    PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
    PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY,
    SOURCE_BUNDLE_MAGIC,
)
from process.ptg_parts.ptg2_source_witness_primitives import (
    nonnegative_int,
    read_u32,
    sha256_hex,
)
from process.ptg_parts.ptg2_source_witness_selection import (
    local_source_witness_targets,
)


def _json_object(raw_json: bytes, *, field_name: str) -> dict[str, Any]:
    try:
        decoded_json = json.loads(raw_json)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"strict V3 source witness {field_name} is invalid JSON"
        ) from exc
    if not isinstance(decoded_json, dict):
        raise RuntimeError(
            f"strict V3 source witness {field_name} must be an object"
        )
    return decoded_json


def _compressed_record(
    compressed_bytes: bytes,
    raw_source_sha256: str,
) -> CompressedSourceWitnessRecord:
    decoded_record = decode_record(compressed_bytes, raw_source_sha256)
    return CompressedSourceWitnessRecord(
        kind=decoded_record.kind,
        priority=decoded_record.priority,
        tie_breaker=decoded_record.tie_breaker,
        raw_source_sha256=raw_source_sha256,
        compressed=compressed_bytes,
    )


def _authenticated_bundle_payload(bundle_entry: Mapping[str, Any]) -> bytes:
    bundle_path = Path(str(bundle_entry.get("path") or ""))
    if not bundle_path.is_file() or bundle_path.is_symlink():
        raise RuntimeError("strict V3 source witness bundle is missing")
    bundle_size = bundle_path.stat().st_size
    if bundle_size <= 0 or bundle_size > PTG2_V3_SOURCE_WITNESS_MAX_BUNDLE_BYTES:
        raise RuntimeError("strict V3 source witness bundle size is invalid")
    bundle_payload = bundle_path.read_bytes()
    if len(bundle_payload) != bundle_size:
        raise RuntimeError("strict V3 source witness bundle changed while reading")
    if hashlib.sha256(bundle_payload).hexdigest() != sha256_hex(
        bundle_entry.get("sha256"),
        field_name="bundle digest",
    ):
        raise RuntimeError("strict V3 source witness bundle digest does not match")
    if bundle_entry.get("byte_count") != bundle_size:
        raise RuntimeError("strict V3 source witness bundle byte count does not match")
    if not bundle_payload.startswith(SOURCE_BUNDLE_MAGIC):
        raise RuntimeError("strict V3 source witness bundle magic is invalid")
    return bundle_payload


def _bundle_records(
    bundle_payload: bytes,
    *,
    record_count: int,
    record_offset: int,
    raw_source_sha256: str,
) -> tuple[list[CompressedSourceWitnessRecord], int]:
    compressed_records: list[CompressedSourceWitnessRecord] = []
    for _record_index in range(record_count):
        record_length, record_offset = read_u32(
            bundle_payload,
            record_offset,
            field_name="record length",
        )
        record_end = record_offset + record_length
        if (
            record_length <= 0
            or record_length > PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES
            or record_end > len(bundle_payload)
        ):
            raise RuntimeError("strict V3 source witness record framing is invalid")
        compressed_records.append(
            _compressed_record(
                bundle_payload[record_offset:record_end],
                raw_source_sha256,
            )
        )
        record_offset = record_end
    return compressed_records, record_offset


def _validate_bundle_header(
    bundle_header: Mapping[str, Any],
    bundle_entry: Mapping[str, Any],
) -> None:
    required_value_by_field = {
        "contract": PTG2_V3_SOURCE_WITNESS_CONTRACT,
        "selection_method": PTG2_V3_SOURCE_WITNESS_SELECTION,
        "format_version": 2,
        "occurrence_target": PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET,
        "total_target": PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
        "provider_quota": PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
        "unqueryable_rate_policy": PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY,
    }
    if any(
        bundle_header.get(field_name) != expected
        for field_name, expected in required_value_by_field.items()
    ):
        raise RuntimeError("strict V3 source witness bundle contract is invalid")
    if sha256_hex(
        bundle_header.get("raw_source_sha256"),
        field_name="raw source digest",
    ) != sha256_hex(
        bundle_entry.get("raw_source_sha256"),
        field_name="entry raw source digest",
    ):
        raise RuntimeError("strict V3 source witness source digest changed")


def _validate_local_coverage(
    bundle_header: Mapping[str, Any],
    compressed_records: list[CompressedSourceWitnessRecord],
) -> None:
    """Require each scanner bundle to carry its exact local bottom-k cohorts."""

    occurrence_metrics = bundle_header.get("rate_occurrence")
    provider_metrics = bundle_header.get("provider_reference")
    if not isinstance(occurrence_metrics, Mapping) or not isinstance(
        provider_metrics, Mapping
    ):
        raise RuntimeError("strict V3 source witness cohort metrics are invalid")
    emitted_rate_rows = nonnegative_int(
        occurrence_metrics,
        "emitted_rate_row_count",
        error_field_name="emitted rate row count",
    )
    unqueryable_rate_rows = nonnegative_int(
        occurrence_metrics,
        "unqueryable_rate_row_count",
        error_field_name="unqueryable rate row count",
    )
    if unqueryable_rate_rows > emitted_rate_rows:
        raise RuntimeError("strict V3 source witness unqueryable rate count is invalid")
    occurrence_target, provider_target, _total_target = local_source_witness_targets(
        occurrence_metrics,
        provider_metrics,
    )
    observed_count_by_kind = {
        "rate_occurrence": sum(
            witness_record.kind == "rate_occurrence"
            for witness_record in compressed_records
        ),
        "provider_reference": sum(
            witness_record.kind == "provider_reference"
            for witness_record in compressed_records
        ),
    }
    for cohort_name, cohort_metrics, expected_count in (
        ("rate_occurrence", occurrence_metrics, occurrence_target),
        ("provider_reference", provider_metrics, provider_target),
    ):
        selected_count = nonnegative_int(
            cohort_metrics,
            "selected_count",
            error_field_name=f"{cohort_name} selected count",
        )
        if (
            selected_count != expected_count
            or observed_count_by_kind[cohort_name] != expected_count
        ):
            raise RuntimeError(
                f"strict V3 source witness {cohort_name} coverage is incomplete"
            )


def read_scanner_bundle(
    bundle_entry: Mapping[str, Any],
) -> tuple[dict[str, Any], list[CompressedSourceWitnessRecord]]:
    """Read one authenticated scanner bundle and validate local completeness."""

    bundle_payload = _authenticated_bundle_payload(bundle_entry)
    header_length, header_offset = read_u32(
        bundle_payload,
        len(SOURCE_BUNDLE_MAGIC),
        field_name="header",
    )
    header_end = header_offset + header_length
    if header_end > len(bundle_payload):
        raise RuntimeError("strict V3 source witness bundle header is truncated")
    bundle_header = _json_object(
        bundle_payload[header_offset:header_end],
        field_name="bundle header",
    )
    _validate_bundle_header(bundle_header, bundle_entry)
    raw_source_sha256 = sha256_hex(
        bundle_header.get("raw_source_sha256"),
        field_name="raw source digest",
    )
    record_count, record_offset = read_u32(
        bundle_payload,
        header_end,
        field_name="record count",
    )
    compressed_records, record_offset = _bundle_records(
        bundle_payload,
        record_count=record_count,
        record_offset=record_offset,
        raw_source_sha256=raw_source_sha256,
    )
    if record_offset != len(bundle_payload) or bundle_entry.get("row_count") != len(
        compressed_records
    ):
        raise RuntimeError(
            "strict V3 source witness bundle record count does not match"
        )
    _validate_local_coverage(bundle_header, compressed_records)
    return bundle_header, compressed_records


__all__ = ["read_scanner_bundle"]
