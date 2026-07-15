# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Select and publish exact-budget source witnesses for immutable PTG V3."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from process.ptg_parts.ptg2_source_witness_codec import (
    decode_record,
    read_scanner_bundle,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    CompressedSourceWitnessRecord,
    LoadedSourceWitness,
    PERSISTED_PAYLOAD_MAGIC,
    PTG2_V3_SOURCE_WITNESS_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_MAX_FILE_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES,
    PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
    PTG2_V3_SOURCE_WITNESS_RECORD_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_SELECTION,
    PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
    PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY,
    SourceWitnessRecord,
    source_witness_targets,
)
from process.ptg_parts.ptg2_source_witness_primitives import (
    U32,
    nonnegative_int,
    read_u32,
    sha256_hex,
)
from process.ptg_parts.ptg2_source_witness_selection import (
    select_source_witness_records,
    source_population,
    source_set_digest,
)


@dataclass(frozen=True)
class _PayloadSummary:
    source_count: int
    source_digest: str
    occurrence_population: int
    provider_population: int
    emitted_rate_rows: int
    unqueryable_rate_rows: int
    occurrence_count: int
    provider_count: int
    record_count: int
    sample_digest: str


def _payload_header(summary: _PayloadSummary) -> dict[str, Any]:
    return {
        "contract": PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
        "format_version": 2,
        "selection_method": PTG2_V3_SOURCE_WITNESS_SELECTION,
        "population_semantics": "queryable_emitted_price_provider_occurrence_v1",
        "unqueryable_rate_policy": PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY,
        "source_count": summary.source_count,
        "source_set_digest": summary.source_digest,
        "total_target": PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
        "provider_quota": PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
        "queryable_occurrence_population_count": summary.occurrence_population,
        "provider_population_count": summary.provider_population,
        "emitted_rate_row_count": summary.emitted_rate_rows,
        "unqueryable_rate_row_count": summary.unqueryable_rate_rows,
        "occurrence_witness_count": summary.occurrence_count,
        "provider_witness_count": summary.provider_count,
        "record_count": summary.record_count,
        "sample_digest": summary.sample_digest,
    }


def _encode_persisted_payload(
    header: Mapping[str, Any],
    selected_records: Sequence[CompressedSourceWitnessRecord],
) -> bytes:
    header_bytes = json.dumps(
        dict(header),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    witness_payload = bytearray(PERSISTED_PAYLOAD_MAGIC)
    witness_payload.extend(U32.pack(len(header_bytes)))
    witness_payload.extend(header_bytes)
    witness_payload.extend(U32.pack(len(selected_records)))
    for witness_record in selected_records:
        witness_payload.extend(bytes.fromhex(witness_record.raw_source_sha256))
        witness_payload.extend(U32.pack(len(witness_record.compressed)))
        witness_payload.extend(witness_record.compressed)
        if len(witness_payload) > PTG2_V3_SOURCE_WITNESS_MAX_FILE_BYTES:
            raise RuntimeError(
                "strict V3 persisted source witness exceeds its aggregate payload budget"
            )
    return bytes(witness_payload)


def _read_source_bundles(
    bundle_entries: Sequence[Mapping[str, Any]],
    expected_sources: Sequence[str],
) -> tuple[list[dict[str, Any]], list[CompressedSourceWitnessRecord]]:
    if not bundle_entries or len(bundle_entries) != len(expected_sources):
        raise RuntimeError("strict V3 source witness coverage is incomplete")
    bundle_headers: list[dict[str, Any]] = []
    candidate_records: list[CompressedSourceWitnessRecord] = []
    for bundle_entry in bundle_entries:
        bundle_header, bundle_records = read_scanner_bundle(bundle_entry)
        bundle_headers.append(bundle_header)
        candidate_records.extend(bundle_records)
    observed_sources = sorted(
        sha256_hex(
            bundle_header.get("raw_source_sha256"),
            field_name="observed raw source digest",
        )
        for bundle_header in bundle_headers
    )
    if observed_sources != list(expected_sources):
        raise RuntimeError("strict V3 source witness set does not match imported sources")
    return bundle_headers, candidate_records


def build_persisted_source_witness(
    bundle_entries: Sequence[Mapping[str, Any]],
    *,
    expected_raw_source_sha256: Sequence[str],
) -> tuple[bytes, dict[str, Any]]:
    """Merge per-source candidates into one exact-budget persisted payload."""

    expected_sources = sorted(
        sha256_hex(source_digest, field_name="expected raw source digest")
        for source_digest in expected_raw_source_sha256
    )
    bundle_headers, candidate_records = _read_source_bundles(
        bundle_entries,
        expected_sources,
    )
    population = source_population(bundle_headers)
    selected_records, occurrence_count, provider_count, record_count = (
        select_source_witness_records(
            candidate_records,
            occurrence_population=population.occurrence_count,
            provider_population=population.provider_count,
        )
    )
    if occurrence_count <= 0:
        raise RuntimeError("strict V3 source witness has no queryable occurrence evidence")
    sample_hasher = hashlib.sha256()
    for witness_record in selected_records:
        sample_hasher.update(bytes.fromhex(witness_record.raw_source_sha256))
        sample_hasher.update(witness_record.compressed)
    header = _payload_header(
        _PayloadSummary(
            source_count=len(expected_sources),
            source_digest=source_set_digest(expected_sources),
            occurrence_population=population.occurrence_count,
            provider_population=population.provider_count,
            emitted_rate_rows=population.emitted_rate_rows,
            unqueryable_rate_rows=population.unqueryable_rate_rows,
            occurrence_count=occurrence_count,
            provider_count=provider_count,
            record_count=record_count,
            sample_digest=sample_hasher.hexdigest(),
        )
    )
    witness_payload = _encode_persisted_payload(header, selected_records)
    witness_metadata_by_field = {
        **header,
        "payload_sha256": hashlib.sha256(witness_payload).hexdigest(),
        "payload_bytes": len(witness_payload),
        "compression": "per_record_zlib",
    }
    return witness_payload, witness_metadata_by_field


def _persisted_header(
    witness_payload: bytes,
    *,
    expected_sources: Sequence[str],
) -> tuple[dict[str, Any], int]:
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
        raise RuntimeError("strict V3 persisted source witness header is invalid") from exc
    if (
        not isinstance(header, dict)
        or header.get("contract") != PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT
        or header.get("format_version") != 2
        or header.get("selection_method") != PTG2_V3_SOURCE_WITNESS_SELECTION
        or header.get("population_semantics")
        != "queryable_emitted_price_provider_occurrence_v1"
        or header.get("unqueryable_rate_policy")
        != PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY
        or header.get("total_target") != PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET
        or header.get("provider_quota") != PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA
    ):
        raise RuntimeError("strict V3 persisted source witness contract is invalid")
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
    return header, header_end


def _decode_persisted_records(
    witness_payload: bytes,
    *,
    record_offset: int,
    expected_sources: set[str],
) -> tuple[list[SourceWitnessRecord], str]:
    record_count, record_offset = read_u32(
        witness_payload,
        record_offset,
        field_name="persisted record count",
    )
    if record_count <= 0 or record_count > PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET:
        raise RuntimeError("strict V3 persisted source witness record count is invalid")
    decoded_records: list[SourceWitnessRecord] = []
    sample_hasher = hashlib.sha256()
    for _record_index in range(record_count):
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
        decoded_records.append(decode_record(compressed_record, raw_source_sha256))
        sample_hasher.update(bytes.fromhex(raw_source_sha256))
        sample_hasher.update(compressed_record)
        record_offset = compressed_end
    if record_offset != len(witness_payload):
        raise RuntimeError("strict V3 persisted source witness has trailing bytes")
    return decoded_records, sample_hasher.hexdigest()


def _validate_persisted_counts(
    header: Mapping[str, Any],
    decoded_records: Sequence[SourceWitnessRecord],
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
    expected_occurrences, expected_providers, expected_total = source_witness_targets(
        occurrence_population=occurrence_population,
        provider_population=provider_population,
    )
    occurrence_count = sum(
        witness_record.kind == "rate_occurrence"
        for witness_record in decoded_records
    )
    provider_count = sum(
        witness_record.kind == "provider_reference"
        for witness_record in decoded_records
    )
    observed_counts = (
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
    if observed_counts != (
        expected_occurrences,
        expected_providers,
        expected_total,
    ) or (occurrence_count, provider_count, len(decoded_records)) != observed_counts:
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
    header, record_offset = _persisted_header(
        payload_bytes,
        expected_sources=expected_sources,
    )
    decoded_records, sample_digest = _decode_persisted_records(
        payload_bytes,
        record_offset=record_offset,
        expected_sources=set(expected_sources),
    )
    _validate_persisted_counts(header, decoded_records)
    if header.get("sample_digest") != sample_digest:
        raise RuntimeError("strict V3 persisted source witness digest is inconsistent")
    witness_metadata_by_field = {
        **header,
        "payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
        "payload_bytes": len(payload_bytes),
        "compression": "per_record_zlib",
    }
    if (
        expected_metadata is not None
        and dict(expected_metadata) != witness_metadata_by_field
    ):
        raise RuntimeError("strict V3 source witness manifest fields changed")
    return LoadedSourceWitness(
        metadata=witness_metadata_by_field,
        records=tuple(decoded_records),
    )


__all__ = [
    "LoadedSourceWitness",
    "PTG2_V3_SOURCE_WITNESS_CONTRACT",
    "PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT",
    "PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA",
    "PTG2_V3_SOURCE_WITNESS_RECORD_CONTRACT",
    "PTG2_V3_SOURCE_WITNESS_SELECTION",
    "PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET",
    "PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY",
    "SourceWitnessRecord",
    "build_persisted_source_witness",
    "decode_persisted_source_witness",
    "source_set_digest",
]
