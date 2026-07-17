# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Encode compact immutable PTG V3 source-witness payloads."""

from __future__ import annotations

import hashlib
import json
import zlib
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from process.ptg_parts.ptg2_source_witness_codec import (
    externalize_linked_provider_record,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    CompressedSourceWitnessRecord,
    PERSISTED_PAYLOAD_MAGIC,
    PTG2_V3_SOURCE_WITNESS_MAX_FILE_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES,
    PTG2_V3_SOURCE_WITNESS_PAYLOAD_COMPRESSION,
    PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
    PTG2_V3_SOURCE_WITNESS_SELECTION,
    PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
    PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY,
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
class _LinkedProviderEntry:
    sha256: str
    raw_json: bytes
    compressed: bytes


def _linked_provider_entries(
    linked_provider_raw_by_sha256: Mapping[str, bytes],
) -> list[_LinkedProviderEntry]:
    linked_provider_entries = [
        _LinkedProviderEntry(
            sha256=linked_provider_sha256,
            raw_json=linked_provider_raw_by_sha256[linked_provider_sha256],
            compressed=zlib.compress(
                linked_provider_raw_by_sha256[linked_provider_sha256],
                level=1,
            ),
        )
        for linked_provider_sha256 in sorted(linked_provider_raw_by_sha256)
    ]
    if any(
        not linked_provider.compressed
        or len(linked_provider.compressed)
        > PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES
        for linked_provider in linked_provider_entries
    ):
        raise RuntimeError(
            "strict V3 linked provider dictionary entry exceeds its payload budget"
        )
    return linked_provider_entries


def _externalize_linked_provider_evidence(
    selected_records: Sequence[CompressedSourceWitnessRecord],
) -> tuple[list[_PersistedRecord], list[_LinkedProviderEntry]]:
    persisted_records: list[_PersistedRecord] = []
    linked_provider_raw_by_sha256: dict[str, bytes] = {}
    for selected_record in selected_records:
        persisted_record, linked_sha256, linked_json = (
            externalize_linked_provider_record(
                selected_record.compressed,
                selected_record.raw_source_sha256,
            )
        )
        persisted_records.append(
            _PersistedRecord(
                raw_source_sha256=selected_record.raw_source_sha256,
                compressed=persisted_record,
            )
        )
        if linked_sha256 is None:
            continue
        if linked_json is None:
            raise RuntimeError("strict V3 linked provider evidence is incomplete")
        existing_linked_json = linked_provider_raw_by_sha256.setdefault(
            linked_sha256,
            linked_json,
        )
        if existing_linked_json != linked_json:
            raise RuntimeError("strict V3 linked provider digest is inconsistent")
    return (
        persisted_records,
        _linked_provider_entries(linked_provider_raw_by_sha256),
    )


def _payload_header(
    counts: SourceWitnessPayloadCounts,
    persisted_records: Sequence[_PersistedRecord],
    linked_provider_entries: Sequence[_LinkedProviderEntry],
) -> dict[str, Any]:
    sample_hasher = hashlib.sha256()
    for witness_record in persisted_records:
        sample_hasher.update(bytes.fromhex(witness_record.raw_source_sha256))
        sample_hasher.update(witness_record.compressed)
    return {
        "contract": PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
        "format_version": 3,
        "selection_method": PTG2_V3_SOURCE_WITNESS_SELECTION,
        "population_semantics": "queryable_emitted_price_provider_occurrence_v1",
        "unqueryable_rate_policy": PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY,
        "source_count": counts.source_count,
        "source_set_digest": counts.source_digest,
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
        "linked_provider_dictionary_count": len(linked_provider_entries),
        "linked_provider_dictionary_raw_bytes": sum(
            len(linked_provider.raw_json)
            for linked_provider in linked_provider_entries
        ),
        "linked_provider_dictionary_stored_bytes": sum(
            len(linked_provider.compressed)
            for linked_provider in linked_provider_entries
        ),
    }


def _append_payload_part(witness_payload: bytearray, payload_part: bytes) -> None:
    witness_payload.extend(payload_part)
    if len(witness_payload) > PTG2_V3_SOURCE_WITNESS_MAX_FILE_BYTES:
        raise RuntimeError(
            "strict V3 persisted source witness exceeds its aggregate payload budget"
        )


def _encode_payload(
    header: Mapping[str, Any],
    linked_provider_entries: Sequence[_LinkedProviderEntry],
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
    _append_payload_part(witness_payload, U32.pack(len(linked_provider_entries)))
    for linked_provider in linked_provider_entries:
        _append_payload_part(
            witness_payload,
            bytes.fromhex(linked_provider.sha256),
        )
        _append_payload_part(
            witness_payload,
            U32.pack(len(linked_provider.compressed)),
        )
        _append_payload_part(witness_payload, linked_provider.compressed)
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
    """Encode selected scanner records with linked-provider deduplication."""

    persisted_records, linked_provider_entries = (
        _externalize_linked_provider_evidence(selected_records)
    )
    header = _payload_header(
        counts,
        persisted_records,
        linked_provider_entries,
    )
    witness_payload = _encode_payload(
        header,
        linked_provider_entries,
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
