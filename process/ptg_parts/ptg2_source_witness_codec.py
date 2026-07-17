# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Decode bounded Rust scanner evidence without trusting scanner summaries."""

from __future__ import annotations

import hashlib
import json
import zlib
from dataclasses import dataclass
from typing import Any, Mapping

from process.ptg_parts.ptg2_source_witness_contract import (
    PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES,
    PTG2_V3_SOURCE_WITNESS_RECORD_CONTRACT,
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
    if raw_end > len(decoded_record):
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
    *,
    evidence_by_sha256: Mapping[str, bytes] | None = None,
) -> tuple[bytes, bytes | None, str, str | None]:
    raw_json, linked_provider_json = _framed_raw_tokens(
        decoded_record,
        metadata_end,
    )
    raw_sha256 = sha256_hex(
        record_metadata.get("raw_sha256"),
        field_name="raw record digest",
    )
    if not raw_json and evidence_by_sha256 is not None:
        raw_json = evidence_by_sha256.get(raw_sha256, b"")
    if hashlib.sha256(raw_json).hexdigest() != raw_sha256:
        raise RuntimeError("strict V3 source witness raw token digest is invalid")
    linked_provider_sha256 = record_metadata.get("linked_provider_sha256")
    if linked_provider_sha256 is not None:
        linked_provider_sha256 = sha256_hex(
            linked_provider_sha256,
            field_name="linked provider digest",
        )
    if (
        linked_provider_json is None
        and linked_provider_sha256 is not None
        and evidence_by_sha256 is not None
    ):
        linked_provider_json = evidence_by_sha256.get(
            linked_provider_sha256
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


def _decode_record(
    compressed_record: bytes,
    raw_source_sha256: str,
    *,
    evidence_by_sha256: Mapping[str, bytes] | None,
) -> SourceWitnessRecord:
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
        evidence_by_sha256=evidence_by_sha256,
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


def decode_record(
    compressed_record: bytes,
    raw_source_sha256: str,
) -> SourceWitnessRecord:
    """Decode and authenticate one compressed scanner witness record."""

    return _decode_record(
        compressed_record,
        raw_source_sha256,
        evidence_by_sha256=None,
    )


def decode_persisted_record(
    compressed_record: bytes,
    raw_source_sha256: str,
    *,
    evidence_by_sha256: Mapping[str, bytes],
) -> SourceWitnessRecord:
    """Decode one persisted record backed by the authenticated evidence dictionary."""

    return _decode_record(
        compressed_record,
        raw_source_sha256,
        evidence_by_sha256=evidence_by_sha256,
    )


def externalize_source_evidence_record(
    compressed_record: bytes,
    raw_source_sha256: str,
) -> tuple[bytes, dict[str, bytes]]:
    """Move exact source tokens into a shared authenticated evidence dictionary."""

    decoded = decode_record(compressed_record, raw_source_sha256)
    decoded_record, _record_metadata, metadata_end = _decoded_record_metadata(
        compressed_record
    )
    externalized_record = b"".join(
        (
            decoded_record[:metadata_end],
            U32.pack(0),
            U32.pack(0),
        )
    )
    evidence_by_sha256 = {decoded.raw_sha256: decoded.raw_json}
    if decoded.linked_provider_sha256 is not None:
        linked_provider_json = decoded.linked_provider_json
        if linked_provider_json is None:
            raise RuntimeError("strict V3 linked provider evidence is incomplete")
        existing_evidence = evidence_by_sha256.setdefault(
            decoded.linked_provider_sha256,
            linked_provider_json,
        )
        if existing_evidence != linked_provider_json:
            raise RuntimeError("strict V3 source evidence digest is inconsistent")
    return zlib.compress(externalized_record, level=1), evidence_by_sha256


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


__all__ = [
    "U32",
    "decode_record",
    "decode_persisted_record",
    "externalize_source_evidence_record",
    "nonnegative_int",
    "read_u32",
    "sha256_hex",
]
