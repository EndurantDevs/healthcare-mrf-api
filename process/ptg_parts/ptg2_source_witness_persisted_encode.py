# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Encode compact immutable PTG V3 source-witness payloads."""

from __future__ import annotations

import hashlib
import json
import zlib
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from process.ptg_parts.ptg2_source_witness_codec import (
    externalize_source_evidence_record,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    CompressedSourceWitnessRecord,
    PERSISTED_PAYLOAD_MAGIC,
    PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES,
    PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET,
    PTG2_V3_SOURCE_WITNESS_PAYLOAD_COMPRESSION,
    PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
    PTG2_V3_SOURCE_WITNESS_SELECTION,
    PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
    PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY,
    WitnessPayloadLimitError,
)
from process.ptg_parts.ptg2_source_witness_primitives import U32


@dataclass(frozen=True)
class SourceWitnessPayloadCounts:
    """Describe source populations and selected witness counts."""

    source_count: int
    source_digest: str
    occurrence_population: int
    provider_population: int
    emitted_rate_rows: int
    unqueryable_rate_rows: int
    occurrence_count: int
    provider_count: int
    record_count: int


@dataclass(frozen=True)
class _PersistedRecord:
    raw_source_sha256: str
    compressed: bytes


@dataclass(frozen=True)
class _EvidenceEntry:
    sha256: str
    raw_byte_count: int
    compressed: bytes


def _insert_evidence(
    evidence_by_sha256: dict[str, _EvidenceEntry],
    evidence_sha256: str,
    raw_json: bytes,
) -> None:
    if hashlib.sha256(raw_json).hexdigest() != evidence_sha256:
        raise RuntimeError("strict V3 source evidence digest is inconsistent")
    existing_evidence = evidence_by_sha256.get(evidence_sha256)
    if existing_evidence is not None:
        if zlib.decompress(existing_evidence.compressed) != raw_json:
            raise RuntimeError("strict V3 source evidence digest is inconsistent")
        return
    compressed = zlib.compress(raw_json, level=6)
    if not compressed or len(compressed) > PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES:
        raise RuntimeError(
            "strict V3 evidence dictionary entry exceeds its payload budget"
        )
    evidence_by_sha256[evidence_sha256] = _EvidenceEntry(
        sha256=evidence_sha256,
        raw_byte_count=len(raw_json),
        compressed=compressed,
    )


def _externalize_source_evidence(
    selected_records: Sequence[CompressedSourceWitnessRecord],
) -> tuple[list[_PersistedRecord], list[_EvidenceEntry]]:
    persisted_records: list[_PersistedRecord] = []
    evidence_by_sha256: dict[str, _EvidenceEntry] = {}
    for selected_record in selected_records:
        persisted_record, record_evidence = externalize_source_evidence_record(
            selected_record.compressed,
            selected_record.raw_source_sha256,
        )
        persisted_records.append(
            _PersistedRecord(
                raw_source_sha256=selected_record.raw_source_sha256,
                compressed=persisted_record,
            )
        )
        for evidence_sha256, raw_json in record_evidence.items():
            _insert_evidence(
                evidence_by_sha256,
                evidence_sha256,
                raw_json,
            )
    return (
        persisted_records,
        [evidence_by_sha256[digest] for digest in sorted(evidence_by_sha256)],
    )


def _payload_header(
    counts: SourceWitnessPayloadCounts,
    persisted_records: Sequence[_PersistedRecord],
    evidence_entries: Sequence[_EvidenceEntry],
) -> dict[str, Any]:
    sample_hasher = hashlib.sha256()
    for witness_record in persisted_records:
        sample_hasher.update(bytes.fromhex(witness_record.raw_source_sha256))
        sample_hasher.update(witness_record.compressed)
    return {
        "contract": PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
        "format_version": 5,
        "selection_method": PTG2_V3_SOURCE_WITNESS_SELECTION,
        "population_semantics": "queryable_emitted_price_provider_occurrence_v1",
        "unqueryable_rate_policy": PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY,
        "source_count": counts.source_count,
        "source_set_digest": counts.source_digest,
        "occurrence_target": PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET,
        "total_target": PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
        "provider_quota": PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
        "queryable_occurrence_population_count": counts.occurrence_population,
        "provider_population_count": counts.provider_population,
        "emitted_rate_row_count": counts.emitted_rate_rows,
        "unqueryable_rate_row_count": counts.unqueryable_rate_rows,
        "occurrence_witness_count": counts.occurrence_count,
        "provider_witness_count": counts.provider_count,
        "record_count": counts.record_count,
        "sample_digest": sample_hasher.hexdigest(),
        "evidence_dictionary_count": len(evidence_entries),
        "evidence_dictionary_raw_bytes": sum(
            evidence.raw_byte_count for evidence in evidence_entries
        ),
        "evidence_dictionary_stored_bytes": sum(
            len(evidence.compressed) for evidence in evidence_entries
        ),
    }


def _append_payload_part(witness_payload: bytearray, payload_part: bytes) -> None:
    witness_payload.extend(payload_part)
    if len(witness_payload) > PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES:
        raise WitnessPayloadLimitError(
            "strict V3 persisted source witness exceeds its 512 MiB logical "
            "payload safety bound"
        )


def _encode_payload(
    header: Mapping[str, Any],
    evidence_entries: Sequence[_EvidenceEntry],
    persisted_records: Sequence[_PersistedRecord],
) -> bytes:
    header_bytes = json.dumps(
        dict(header),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    witness_payload = bytearray(PERSISTED_PAYLOAD_MAGIC)
    _append_payload_part(witness_payload, U32.pack(len(header_bytes)))
    _append_payload_part(witness_payload, header_bytes)
    _append_payload_part(witness_payload, U32.pack(len(evidence_entries)))
    for evidence in evidence_entries:
        _append_payload_part(
            witness_payload,
            bytes.fromhex(evidence.sha256),
        )
        _append_payload_part(
            witness_payload,
            U32.pack(len(evidence.compressed)),
        )
        _append_payload_part(witness_payload, evidence.compressed)
    _append_payload_part(witness_payload, U32.pack(len(persisted_records)))
    for witness_record in persisted_records:
        _append_payload_part(
            witness_payload,
            bytes.fromhex(witness_record.raw_source_sha256),
        )
        _append_payload_part(
            witness_payload,
            U32.pack(len(witness_record.compressed)),
        )
        _append_payload_part(witness_payload, witness_record.compressed)
    return bytes(witness_payload)


def encode_persisted_source_witness(
    selected_records: Sequence[CompressedSourceWitnessRecord],
    counts: SourceWitnessPayloadCounts,
) -> tuple[bytes, dict[str, Any]]:
    """Encode selected scanner records with exact source-evidence deduplication."""

    persisted_records, evidence_entries = _externalize_source_evidence(
        selected_records
    )
    header = _payload_header(
        counts,
        persisted_records,
        evidence_entries,
    )
    witness_payload = _encode_payload(
        header,
        evidence_entries,
        persisted_records,
    )
    return witness_payload, {
        **header,
        "payload_sha256": hashlib.sha256(witness_payload).hexdigest(),
        "payload_bytes": len(witness_payload),
        "compression": PTG2_V3_SOURCE_WITNESS_PAYLOAD_COMPRESSION,
    }


__all__ = [
    "SourceWitnessPayloadCounts",
    "encode_persisted_source_witness",
]
