# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Decode bounded Rust scanner evidence without trusting scanner summaries."""

from __future__ import annotations

import hashlib
import json
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from process.ptg_parts.ptg2_source_witness_contract import (
    CompressedSourceWitnessRecord,
    PTG2_V3_SOURCE_WITNESS_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_FILE_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES,
    PTG2_V3_SOURCE_WITNESS_RECORD_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_SELECTION,
    PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
    PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY,
    SOURCE_BUNDLE_MAGIC,
    SOURCE_RECORD_MAGIC,
    SourceWitnessRecord,
)
from process.ptg_parts.ptg2_source_witness_primitives import (
    U32,
    nonnegative_int,
    read_u32,
    sha256_hex,
)


@dataclass(frozen=True)
class _RecordFields:
    witness_kind: str
    priority: int
    tie_breaker: str
    procedure: dict[str, Any] | None
    provider_evidence: dict[str, Any] | None
    expected_evidence: dict[str, Any]


def _decompress_record(compressed_record: bytes) -> bytes:
    decompressor = zlib.decompressobj()
    try:
        decoded_record = decompressor.decompress(
            compressed_record,
            PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES + 1,
        )
        if (
            len(decoded_record) > PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES
            or decompressor.unconsumed_tail
        ):
            raise RuntimeError(
                "strict V3 source witness record exceeds or violates its zlib framing"
            )
        decoded_record += decompressor.flush(
            PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES
            - len(decoded_record)
            + 1
        )
    except zlib.error as exc:
        raise RuntimeError(
            "strict V3 source witness record has invalid zlib framing"
        ) from exc
    if (
        len(decoded_record) > PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES
        or decompressor.unconsumed_tail
        or not decompressor.eof
        or decompressor.unused_data
    ):
        raise RuntimeError(
            "strict V3 source witness record exceeds or violates its zlib framing"
        )
    return decoded_record


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


def _coordinate(record_metadata: Mapping[str, Any]) -> tuple[int, int, int, int]:
    coordinate_mapping = record_metadata.get("coordinate")
    if not isinstance(coordinate_mapping, Mapping):
        raise RuntimeError("strict V3 source witness coordinate is invalid")
    coordinate_fields = (
        "object_ordinal",
        "rate_ordinal",
        "price_ordinal",
        "provider_ordinal",
    )
    coordinate_values = tuple(coordinate_mapping.get(name) for name in coordinate_fields)
    if any(type(value) is not int or value < 0 for value in coordinate_values):
        raise RuntimeError("strict V3 source witness coordinate is invalid")
    return (
        int(coordinate_values[0]),
        int(coordinate_values[1]),
        int(coordinate_values[2]),
        int(coordinate_values[3]),
    )


def _optional_mapping(raw_value: Any, *, field_name: str) -> dict[str, Any] | None:
    if raw_value is None:
        return None
    if not isinstance(raw_value, Mapping):
        raise RuntimeError(f"strict V3 source witness {field_name} is invalid")
    return dict(raw_value)


def _framed_raw_tokens(
    decoded_record: bytes,
    metadata_end: int,
) -> tuple[bytes, bytes | None]:
    raw_length, raw_offset = read_u32(
        decoded_record,
        metadata_end,
        field_name="raw JSON token",
    )
    raw_end = raw_offset + raw_length
    if raw_length <= 0 or raw_end > len(decoded_record):
        raise RuntimeError("strict V3 source witness raw JSON token is invalid")
    linked_length, linked_offset = read_u32(
        decoded_record,
        raw_end,
        field_name="linked provider token",
    )
    linked_end = linked_offset + linked_length
    if linked_end != len(decoded_record):
        raise RuntimeError("strict V3 source witness linked provider token is invalid")
    linked_provider_json = (
        decoded_record[linked_offset:linked_end] if linked_length else None
    )
    return decoded_record[raw_offset:raw_end], linked_provider_json


def _decoded_record_metadata(
    compressed_record: bytes,
) -> tuple[bytes, dict[str, Any], int]:
    decoded_record = _decompress_record(compressed_record)
    if not decoded_record.startswith(SOURCE_RECORD_MAGIC):
        raise RuntimeError("strict V3 source witness record magic is invalid")
    metadata_length, metadata_offset = read_u32(
        decoded_record,
        len(SOURCE_RECORD_MAGIC),
        field_name="record metadata",
    )
    metadata_end = metadata_offset + metadata_length
    if metadata_end > len(decoded_record):
        raise RuntimeError("strict V3 source witness record metadata is truncated")
    record_metadata = _json_object(
        decoded_record[metadata_offset:metadata_end],
        field_name="record metadata",
    )
    return decoded_record, record_metadata, metadata_end


def _verified_raw_evidence(
    decoded_record: bytes,
    metadata_end: int,
    record_metadata: Mapping[str, Any],
) -> tuple[bytes, bytes | None, str, str | None]:
    raw_json, linked_provider_json = _framed_raw_tokens(
        decoded_record,
        metadata_end,
    )
    raw_sha256 = sha256_hex(
        record_metadata.get("raw_sha256"),
        field_name="raw record digest",
    )
    if hashlib.sha256(raw_json).hexdigest() != raw_sha256:
        raise RuntimeError("strict V3 source witness raw token digest is invalid")
    linked_provider_sha256 = record_metadata.get("linked_provider_sha256")
    if linked_provider_sha256 is not None:
        linked_provider_sha256 = sha256_hex(
            linked_provider_sha256,
            field_name="linked provider digest",
        )
    if (linked_provider_json is None) != (linked_provider_sha256 is None):
        raise RuntimeError("strict V3 linked provider evidence is incomplete")
    if linked_provider_json is not None and (
        hashlib.sha256(linked_provider_json).hexdigest() != linked_provider_sha256
    ):
        raise RuntimeError("strict V3 linked provider digest is invalid")
    return raw_json, linked_provider_json, raw_sha256, linked_provider_sha256


def _record_fields(record_metadata: Mapping[str, Any]) -> _RecordFields:
    if record_metadata.get("contract") != PTG2_V3_SOURCE_WITNESS_RECORD_CONTRACT:
        raise RuntimeError("strict V3 source witness record contract is invalid")
    witness_kind = str(record_metadata.get("kind") or "")
    if witness_kind not in {"rate_occurrence", "provider_reference"}:
        raise RuntimeError("strict V3 source witness record kind is invalid")
    priority_text = str(record_metadata.get("priority") or "")
    if len(priority_text) != 16 or any(
        character not in "0123456789abcdef" for character in priority_text
    ):
        raise RuntimeError("strict V3 source witness priority is invalid")
    expected_evidence = _optional_mapping(
        record_metadata.get("expected"),
        field_name="expected evidence",
    )
    if expected_evidence is None:
        raise RuntimeError("strict V3 source witness expected evidence is missing")
    return _RecordFields(
        witness_kind=witness_kind,
        priority=int(priority_text, 16),
        tie_breaker=sha256_hex(
            record_metadata.get("tie_breaker"),
            field_name="record tie breaker",
        ),
        procedure=_optional_mapping(
            record_metadata.get("procedure"),
            field_name="procedure",
        ),
        provider_evidence=_optional_mapping(
            record_metadata.get("provider_evidence"),
            field_name="provider evidence",
        ),
        expected_evidence=expected_evidence,
    )


def decode_record(
    compressed_record: bytes,
    raw_source_sha256: str,
) -> SourceWitnessRecord:
    """Decode and authenticate one compressed scanner witness record."""

    decoded_record, record_metadata, metadata_end = _decoded_record_metadata(
        compressed_record
    )
    record_fields = _record_fields(record_metadata)
    (
        raw_json,
        linked_provider_json,
        raw_sha256,
        linked_provider_sha256,
    ) = _verified_raw_evidence(
        decoded_record,
        metadata_end,
        record_metadata,
    )
    _validate_record_shape(
        witness_kind=record_fields.witness_kind,
        procedure=record_fields.procedure,
        provider_evidence=record_fields.provider_evidence,
        linked_provider_json=linked_provider_json,
    )
    return SourceWitnessRecord(
        kind=record_fields.witness_kind,
        priority=record_fields.priority,
        tie_breaker=record_fields.tie_breaker,
        coordinate=_coordinate(record_metadata),
        raw_source_sha256=raw_source_sha256,
        raw_sha256=raw_sha256,
        linked_provider_sha256=linked_provider_sha256,
        procedure=record_fields.procedure,
        provider_evidence=record_fields.provider_evidence,
        expected=record_fields.expected_evidence,
        raw_json=raw_json,
        linked_provider_json=linked_provider_json,
    )


def _validate_record_shape(
    *,
    witness_kind: str,
    procedure: Mapping[str, Any] | None,
    provider_evidence: Mapping[str, Any] | None,
    linked_provider_json: bytes | None,
) -> None:
    if witness_kind == "provider_reference":
        if procedure is not None or provider_evidence is not None or linked_provider_json:
            raise RuntimeError("strict V3 provider witness shape is invalid")
        return
    if procedure is None or provider_evidence is None:
        raise RuntimeError("strict V3 occurrence witness shape is incomplete")
    source_kind = provider_evidence.get("source_kind")
    if source_kind == "provider_reference" and linked_provider_json is None:
        raise RuntimeError("strict V3 referenced occurrence lacks raw provider evidence")
    if source_kind == "inline_provider_group" and linked_provider_json is not None:
        raise RuntimeError("strict V3 inline occurrence has unexpected provider evidence")
    if source_kind not in {"provider_reference", "inline_provider_group"}:
        raise RuntimeError("strict V3 occurrence provider source is invalid")


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
    if bundle_size <= 0 or bundle_size > PTG2_V3_SOURCE_WITNESS_MAX_FILE_BYTES:
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
        raise RuntimeError("strict V3 source witness bundle record count does not match")
    _validate_local_coverage(bundle_header, compressed_records)
    return bundle_header, compressed_records


def _validate_bundle_header(
    bundle_header: Mapping[str, Any],
    bundle_entry: Mapping[str, Any],
) -> None:
    if (
        bundle_header.get("contract") != PTG2_V3_SOURCE_WITNESS_CONTRACT
        or bundle_header.get("selection_method")
        != PTG2_V3_SOURCE_WITNESS_SELECTION
        or bundle_header.get("format_version") != 2
        or bundle_header.get("total_target")
        != PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET
        or bundle_header.get("unqueryable_rate_policy")
        != PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY
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
    observed_count_by_cohort = {
        "rate_occurrence": sum(
            witness_record.kind == "rate_occurrence"
            for witness_record in compressed_records
        ),
        "provider_reference": sum(
            witness_record.kind == "provider_reference"
            for witness_record in compressed_records
        ),
    }
    for cohort_name, cohort_metrics in (
        ("rate_occurrence", occurrence_metrics),
        ("provider_reference", provider_metrics),
    ):
        population_count = nonnegative_int(
            cohort_metrics,
            "population_count",
            error_field_name=f"{cohort_name} population",
        )
        selected_count = nonnegative_int(
            cohort_metrics,
            "selected_count",
            error_field_name=f"{cohort_name} selected count",
        )
        expected_count = min(population_count, PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET)
        if (
            selected_count != expected_count
            or observed_count_by_cohort[cohort_name] != expected_count
        ):
            raise RuntimeError(
                f"strict V3 source witness {cohort_name} coverage is incomplete"
            )


__all__ = [
    "U32",
    "decode_record",
    "nonnegative_int",
    "read_scanner_bundle",
    "read_u32",
    "sha256_hex",
]
