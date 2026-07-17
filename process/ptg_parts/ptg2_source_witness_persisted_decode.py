# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Decode compact immutable PTG V3 source-witness payloads."""

from __future__ import annotations

import hashlib
import json
import zlib
from typing import Any, Mapping, Sequence

from process.ptg_parts.ptg2_source_witness_codec import decode_persisted_record
from process.ptg_parts.ptg2_source_witness_contract import (
    LoadedSourceWitness,
    PERSISTED_PAYLOAD_MAGIC,
    PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_FILE_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES,
    PTG2_V3_SOURCE_WITNESS_PAYLOAD_COMPRESSION,
    PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
    PTG2_V3_SOURCE_WITNESS_SELECTION,
    PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
    PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY,
    SourceWitnessRecord,
    source_witness_targets,
)
from process.ptg_parts.ptg2_source_witness_primitives import (
    nonnegative_int,
    read_u32,
    sha256_hex,
)
from process.ptg_parts.ptg2_source_witness_selection import source_set_digest


def _read_header(witness_payload: bytes) -> tuple[dict[str, Any], int]:
    header_length, header_offset = read_u32(
        witness_payload,
        len(PERSISTED_PAYLOAD_MAGIC),
        field_name="persisted header",
    )
    header_end = header_offset + header_length
    if header_end > len(witness_payload):
        raise RuntimeError("strict V3 persisted source witness header is truncated")
    try:
        header = json.loads(witness_payload[header_offset:header_end])
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            "strict V3 persisted source witness header is invalid"
        ) from exc
    if not isinstance(header, dict):
        raise RuntimeError("strict V3 persisted source witness header is invalid")
    return header, header_end


def _validate_header_contract(header: Mapping[str, Any]) -> None:
    required_value_by_field = {
        "contract": PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
        "format_version": 3,
        "selection_method": PTG2_V3_SOURCE_WITNESS_SELECTION,
        "population_semantics": "queryable_emitted_price_provider_occurrence_v1",
        "unqueryable_rate_policy": PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY,
        "total_target": PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
        "provider_quota": PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
    }
    if any(
        header.get(field_name) != expected
        for field_name, expected in required_value_by_field.items()
    ):
        raise RuntimeError("strict V3 persisted source witness contract is invalid")


def _validate_header_scope(
    header: Mapping[str, Any],
    expected_sources: Sequence[str],
) -> None:
    if (
        nonnegative_int(header, "source_count", error_field_name="source count")
        != len(expected_sources)
        or header.get("source_set_digest") != source_set_digest(expected_sources)
    ):
        raise RuntimeError("strict V3 persisted source witness source set is invalid")
    emitted_rate_rows = nonnegative_int(
        header,
        "emitted_rate_row_count",
        error_field_name="emitted rate row count",
    )
    unqueryable_rate_rows = nonnegative_int(
        header,
        "unqueryable_rate_row_count",
        error_field_name="unqueryable rate row count",
    )
    if emitted_rate_rows <= 0 or unqueryable_rate_rows > emitted_rate_rows:
        raise RuntimeError("strict V3 persisted source witness rate policy is invalid")
    dictionary_count = nonnegative_int(
        header,
        "linked_provider_dictionary_count",
        error_field_name="linked provider dictionary count",
    )
    record_count = nonnegative_int(
        header,
        "record_count",
        error_field_name="record count",
    )
    if dictionary_count > record_count:
        raise RuntimeError(
            "strict V3 persisted source witness dictionary count is invalid"
        )


def _persisted_header(
    witness_payload: bytes,
    *,
    expected_sources: Sequence[str],
) -> tuple[dict[str, Any], int]:
    """Read and validate the immutable payload header."""

    header, header_end = _read_header(witness_payload)
    _validate_header_contract(header)
    _validate_header_scope(header, expected_sources)
    for field_name in (
        "linked_provider_dictionary_raw_bytes",
        "linked_provider_dictionary_stored_bytes",
    ):
        nonnegative_int(header, field_name, error_field_name=field_name.replace("_", " "))
    return header, header_end


def _decompress_linked_provider(compressed_provider: bytes) -> bytes:
    decompressor = zlib.decompressobj()
    try:
        raw_provider = decompressor.decompress(
            compressed_provider,
            PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES + 1,
        )
        if (
            len(raw_provider) > PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES
            or decompressor.unconsumed_tail
        ):
            raise RuntimeError(
                "strict V3 linked provider dictionary entry exceeds its decode budget"
            )
        raw_provider += decompressor.flush(
            PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES
            - len(raw_provider)
            + 1
        )
    except zlib.error as exc:
        raise RuntimeError(
            "strict V3 linked provider dictionary entry has invalid zlib framing"
        ) from exc
    if (
        len(raw_provider) > PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES
        or decompressor.unconsumed_tail
        or not decompressor.eof
        or decompressor.unused_data
    ):
        raise RuntimeError(
            "strict V3 linked provider dictionary entry violates its zlib framing"
        )
    return raw_provider


def _read_dictionary_entry(
    witness_payload: bytes,
    dictionary_offset: int,
    previous_sha256: str,
) -> tuple[str, bytes, int, int]:
    digest_end = dictionary_offset + 32
    if digest_end > len(witness_payload):
        raise RuntimeError(
            "strict V3 linked provider dictionary digest is truncated"
        )
    linked_provider_sha256 = witness_payload[dictionary_offset:digest_end].hex()
    if linked_provider_sha256 <= previous_sha256:
        raise RuntimeError("strict V3 linked provider dictionary order is invalid")
    provider_length, compressed_offset = read_u32(
        witness_payload,
        digest_end,
        field_name="linked provider dictionary record",
    )
    compressed_end = compressed_offset + provider_length
    if (
        provider_length <= 0
        or provider_length > PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES
        or compressed_end > len(witness_payload)
    ):
        raise RuntimeError("strict V3 linked provider dictionary record is invalid")
    raw_provider_json = _decompress_linked_provider(
        witness_payload[compressed_offset:compressed_end]
    )
    if hashlib.sha256(raw_provider_json).hexdigest() != linked_provider_sha256:
        raise RuntimeError("strict V3 linked provider dictionary digest is invalid")
    try:
        provider_object = json.loads(raw_provider_json)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            "strict V3 linked provider dictionary JSON is invalid"
        ) from exc
    if not isinstance(provider_object, dict):
        raise RuntimeError(
            "strict V3 linked provider dictionary JSON must be an object"
        )
    return linked_provider_sha256, raw_provider_json, compressed_end, provider_length


def _validate_dictionary_metrics(
    header: Mapping[str, Any],
    *,
    raw_byte_count: int,
    stored_byte_count: int,
) -> None:
    expected_raw_bytes = nonnegative_int(
        header,
        "linked_provider_dictionary_raw_bytes",
        error_field_name="linked provider dictionary raw bytes",
    )
    expected_stored_bytes = nonnegative_int(
        header,
        "linked_provider_dictionary_stored_bytes",
        error_field_name="linked provider dictionary stored bytes",
    )
    if raw_byte_count != expected_raw_bytes or stored_byte_count != expected_stored_bytes:
        raise RuntimeError(
            "strict V3 linked provider dictionary byte counts do not match"
        )


def _decode_linked_provider_dictionary(
    witness_payload: bytes,
    *,
    dictionary_offset: int,
    header: Mapping[str, Any],
) -> tuple[dict[str, bytes], int]:
    """Decode and authenticate the shared linked-provider dictionary."""

    dictionary_count, dictionary_offset = read_u32(
        witness_payload,
        dictionary_offset,
        field_name="linked provider dictionary count",
    )
    expected_count = nonnegative_int(
        header,
        "linked_provider_dictionary_count",
        error_field_name="linked provider dictionary count",
    )
    if dictionary_count != expected_count or dictionary_count > PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET:
        raise RuntimeError(
            "strict V3 linked provider dictionary count does not match"
        )
    linked_provider_by_sha256: dict[str, bytes] = {}
    raw_byte_count = stored_byte_count = 0
    previous_sha256 = ""
    for _dictionary_index in range(dictionary_count):
        linked_sha256, raw_json, dictionary_offset, stored_bytes = (
            _read_dictionary_entry(
                witness_payload,
                dictionary_offset,
                previous_sha256,
            )
        )
        linked_provider_by_sha256[linked_sha256] = raw_json
        raw_byte_count += len(raw_json)
        stored_byte_count += stored_bytes
        previous_sha256 = linked_sha256
    _validate_dictionary_metrics(
        header,
        raw_byte_count=raw_byte_count,
        stored_byte_count=stored_byte_count,
    )
    return linked_provider_by_sha256, dictionary_offset


def _read_persisted_record(
    witness_payload: bytes,
    *,
    record_offset: int,
    expected_sources: set[str],
    linked_provider_by_sha256: Mapping[str, bytes],
) -> tuple[SourceWitnessRecord, bytes, str, int]:
    source_end = record_offset + 32
    if source_end > len(witness_payload):
        raise RuntimeError("strict V3 persisted source digest is truncated")
    raw_source_sha256 = witness_payload[record_offset:source_end].hex()
    if raw_source_sha256 not in expected_sources:
        raise RuntimeError("strict V3 persisted witness references an unknown source")
    record_length, compressed_offset = read_u32(
        witness_payload,
        source_end,
        field_name="persisted record length",
    )
    compressed_end = compressed_offset + record_length
    if (
        record_length <= 0
        or record_length > PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES
        or compressed_end > len(witness_payload)
    ):
        raise RuntimeError("strict V3 persisted record framing is invalid")
    compressed_record = witness_payload[compressed_offset:compressed_end]
    witness_record = decode_persisted_record(
        compressed_record,
        raw_source_sha256,
        linked_provider_by_sha256=linked_provider_by_sha256,
    )
    return witness_record, compressed_record, raw_source_sha256, compressed_end


def _decode_persisted_records(
    witness_payload: bytes,
    *,
    record_offset: int,
    expected_sources: set[str],
    linked_provider_by_sha256: Mapping[str, bytes],
) -> tuple[list[SourceWitnessRecord], str]:
    record_count, record_offset = read_u32(
        witness_payload,
        record_offset,
        field_name="persisted record count",
    )
    if record_count <= 0 or record_count > PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET:
        raise RuntimeError("strict V3 persisted source witness record count is invalid")
    witness_records: list[SourceWitnessRecord] = []
    sample_hasher = hashlib.sha256()
    for _record_index in range(record_count):
        witness_record, compressed_record, source_sha256, record_offset = (
            _read_persisted_record(
                witness_payload,
                record_offset=record_offset,
                expected_sources=expected_sources,
                linked_provider_by_sha256=linked_provider_by_sha256,
            )
        )
        witness_records.append(witness_record)
        sample_hasher.update(bytes.fromhex(source_sha256))
        sample_hasher.update(compressed_record)
    if record_offset != len(witness_payload):
        raise RuntimeError("strict V3 persisted source witness has trailing bytes")
    used_linked_provider_digests = {
        witness_record.linked_provider_sha256
        for witness_record in witness_records
        if witness_record.linked_provider_sha256 is not None
    }
    if used_linked_provider_digests != set(linked_provider_by_sha256):
        raise RuntimeError(
            "strict V3 linked provider dictionary coverage is inconsistent"
        )
    return witness_records, sample_hasher.hexdigest()


def _validate_persisted_counts(
    header: Mapping[str, Any],
    witness_records: Sequence[SourceWitnessRecord],
) -> None:
    occurrence_population = nonnegative_int(
        header,
        "queryable_occurrence_population_count",
        error_field_name="queryable occurrence population",
    )
    provider_population = nonnegative_int(
        header,
        "provider_population_count",
        error_field_name="provider population",
    )
    expected_counts = source_witness_targets(
        occurrence_population=occurrence_population,
        provider_population=provider_population,
    )
    decoded_counts = (
        sum(
            witness_record.kind == "rate_occurrence"
            for witness_record in witness_records
        ),
        sum(
            witness_record.kind == "provider_reference"
            for witness_record in witness_records
        ),
        len(witness_records),
    )
    header_counts = (
        nonnegative_int(
            header,
            "occurrence_witness_count",
            error_field_name="occurrence witness count",
        ),
        nonnegative_int(
            header,
            "provider_witness_count",
            error_field_name="provider witness count",
        ),
        nonnegative_int(header, "record_count", error_field_name="record count"),
    )
    if header_counts != expected_counts or decoded_counts != header_counts:
        raise RuntimeError("strict V3 persisted source witness coverage is incomplete")


def decode_persisted_source_witness(
    witness_payload: bytes,
    *,
    expected_raw_source_sha256: Sequence[str],
    expected_metadata: Mapping[str, Any] | None = None,
) -> LoadedSourceWitness:
    """Decode a PostgreSQL payload and fail on any count, digest, or contract drift."""

    payload_bytes = bytes(witness_payload)
    if (
        not payload_bytes
        or len(payload_bytes) > PTG2_V3_SOURCE_WITNESS_MAX_FILE_BYTES
        or not payload_bytes.startswith(PERSISTED_PAYLOAD_MAGIC)
    ):
        raise RuntimeError("strict V3 persisted source witness framing is invalid")
    expected_sources = sorted(
        sha256_hex(source_digest, field_name="expected raw source digest")
        for source_digest in expected_raw_source_sha256
    )
    if not expected_sources or len(expected_sources) != len(set(expected_sources)):
        raise RuntimeError("strict V3 persisted source witness source set is invalid")
    header, dictionary_offset = _persisted_header(
        payload_bytes,
        expected_sources=expected_sources,
    )
    linked_provider_map, record_offset = _decode_linked_provider_dictionary(
        payload_bytes,
        dictionary_offset=dictionary_offset,
        header=header,
    )
    witness_records, sample_digest = _decode_persisted_records(
        payload_bytes,
        record_offset=record_offset,
        expected_sources=set(expected_sources),
        linked_provider_by_sha256=linked_provider_map,
    )
    _validate_persisted_counts(header, witness_records)
    if header.get("sample_digest") != sample_digest:
        raise RuntimeError("strict V3 persisted source witness digest is inconsistent")
    metadata_by_field = {
        **header,
        "payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
        "payload_bytes": len(payload_bytes),
        "compression": PTG2_V3_SOURCE_WITNESS_PAYLOAD_COMPRESSION,
    }
    if expected_metadata is not None and dict(expected_metadata) != metadata_by_field:
        raise RuntimeError("strict V3 source witness manifest fields changed")
    return LoadedSourceWitness(
        metadata=metadata_by_field,
        records=tuple(witness_records),
    )


__all__ = ["decode_persisted_source_witness"]
