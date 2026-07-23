# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stream exact selected source witnesses into the bounded persisted payload."""

from __future__ import annotations

import hashlib
import json
import tempfile
import zlib
from dataclasses import dataclass, field
from typing import BinaryIO, Mapping, Sequence

from process.ptg_parts.ptg2_source_witness_bundle import _decompress_evidence
from process.ptg_parts.ptg2_source_witness_codec import (
    decode_record_locator_fields,
    externalize_source_evidence_record,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    CompressedSourceWitnessRecord,
    PERSISTED_PAYLOAD_MAGIC,
    PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_DECODED_TOTAL_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES,
    PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET,
    PTG2_V3_SOURCE_WITNESS_PAYLOAD_COMPRESSION,
    PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
    PTG2_V3_SOURCE_WITNESS_SELECTION,
    PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
    PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY,
    SourceWitnessCandidate,
    SourceWitnessBundleIdentity,
    SourceWitnessEvidenceLocator,
    SourceWitnessRecordLocator,
    WitnessPayloadLimitError,
)
from process.ptg_parts.ptg2_source_witness_locator_materialize import (
    _is_record_locator_match,
    _materialization_bundle_file,
    _read_locator_payload,
)
from process.ptg_parts.ptg2_source_witness_persisted_encode import (
    SourceWitnessPayloadCounts,
)
from process.ptg_parts.ptg2_source_witness_primitives import U32


@dataclass(frozen=True)
class _StagedPayload:
    offset: int
    length: int


@dataclass(frozen=True)
class _StagedEvidence:
    sha256: str
    raw_byte_count: int
    compressed_sha256: str
    payload: _StagedPayload


@dataclass(frozen=True)
class _StagedRecord:
    raw_source_sha256: str
    payload: _StagedPayload


@dataclass
class _StreamingBudget:
    """Track unique decoded evidence and staged payload bytes incrementally."""

    decoded_byte_count_by_sha256: dict[str, int] = field(default_factory=dict)
    decoded_total_bytes: int = 0
    # The payload body always contains one evidence count and one record count.
    stored_body_bytes: int = U32.size * 2

    def register_decoded_evidence(
        self,
        evidence_sha256: str,
        raw_byte_count: int,
    ) -> None:
        """Account for one unique decoded evidence entry before decompression."""

        if (
            raw_byte_count <= 0
            or raw_byte_count > PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES
        ):
            raise _payload_limit_error(
                "evidence exceeds its 64 MiB per-entry decoded safety bound"
            )
        existing_count = self.decoded_byte_count_by_sha256.get(evidence_sha256)
        if existing_count is not None:
            if existing_count != raw_byte_count:
                raise RuntimeError(
                    "strict V3 source witness evidence locator is inconsistent"
                )
            return
        projected = self.decoded_total_bytes + raw_byte_count
        if projected > PTG2_V3_SOURCE_WITNESS_MAX_DECODED_TOTAL_BYTES:
            raise _payload_limit_error(
                "evidence exceeds its 512 MiB aggregate decoded safety bound"
            )
        self.decoded_byte_count_by_sha256[evidence_sha256] = raw_byte_count
        self.decoded_total_bytes = projected

    def reserve_stored_entry(self, stored_byte_count: int) -> None:
        """Reserve one framed entry under the aggregate stored-byte ceiling."""

        projected = self.stored_body_bytes + 32 + U32.size + stored_byte_count
        if projected > PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES:
            raise _payload_limit_error(
                "exceeds its 512 MiB logical payload safety bound"
            )
        self.stored_body_bytes = projected


def _payload_limit_error(detail: str) -> WitnessPayloadLimitError:
    return WitnessPayloadLimitError(
        f"strict V3 source witness {detail}; the exact audit sample is retained"
    )


def _preflight_selected_evidence(
    selected_records: Sequence[SourceWitnessCandidate],
) -> _StreamingBudget:
    budget = _StreamingBudget()
    for selected_record in selected_records:
        if isinstance(selected_record, SourceWitnessRecordLocator):
            for evidence_sha256, evidence_locator in (
                selected_record.evidence_by_sha256.items()
            ):
                budget.register_decoded_evidence(
                    evidence_sha256,
                    evidence_locator.raw_byte_count,
                )
            continue
        if selected_record.evidence_by_sha256 is not None:
            for evidence_sha256, raw_evidence in (
                selected_record.evidence_by_sha256.items()
            ):
                budget.register_decoded_evidence(
                    evidence_sha256,
                    len(raw_evidence),
                )
    return budget


def _append_stage(stage_file: BinaryIO, payload: bytes) -> _StagedPayload:
    if not payload or len(payload) > PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES:
        raise _payload_limit_error("entry exceeds its 8 MiB stored safety bound")
    stage_file.seek(0, 2)
    offset = stage_file.tell()
    stage_file.write(payload)
    return _StagedPayload(offset=offset, length=len(payload))


def _read_stage(stage_file: BinaryIO, payload: _StagedPayload) -> bytes:
    stage_file.seek(payload.offset)
    staged_payload = stage_file.read(payload.length)
    if len(staged_payload) != payload.length:
        raise RuntimeError("strict V3 source witness staging file is truncated")
    return staged_payload


def _stage_evidence(
    stage_file: BinaryIO,
    staged_evidence_by_sha256: dict[str, _StagedEvidence],
    budget: _StreamingBudget,
    *,
    evidence_sha256: str,
    raw_evidence: bytes,
) -> None:
    if hashlib.sha256(raw_evidence).hexdigest() != evidence_sha256:
        raise RuntimeError("strict V3 source witness evidence digest is invalid")
    budget.register_decoded_evidence(evidence_sha256, len(raw_evidence))
    compressed_evidence = zlib.compress(raw_evidence, level=6)
    compressed_sha256 = hashlib.sha256(compressed_evidence).hexdigest()
    existing = staged_evidence_by_sha256.get(evidence_sha256)
    if existing is not None:
        if (
            existing.raw_byte_count != len(raw_evidence)
            or existing.payload.length != len(compressed_evidence)
            or existing.compressed_sha256 != compressed_sha256
        ):
            raise RuntimeError("strict V3 source evidence digest is inconsistent")
        return
    budget.reserve_stored_entry(len(compressed_evidence))
    staged_payload = _append_stage(stage_file, compressed_evidence)
    staged_evidence_by_sha256[evidence_sha256] = _StagedEvidence(
        sha256=evidence_sha256,
        raw_byte_count=len(raw_evidence),
        compressed_sha256=compressed_sha256,
        payload=staged_payload,
    )


def _stage_locator_evidence(
    stage_file: BinaryIO,
    staged_evidence_by_sha256: dict[str, _StagedEvidence],
    budget: _StreamingBudget,
    bundle_file: BinaryIO,
    record_locator: SourceWitnessRecordLocator,
    evidence_locator: SourceWitnessEvidenceLocator,
) -> None:
    compressed_evidence = _read_locator_payload(
        bundle_file,
        record_locator.bundle,
        offset=evidence_locator.offset,
        length=evidence_locator.length,
        field_name="evidence",
    )
    raw_evidence = _decompress_evidence(
        compressed_evidence,
        evidence_locator.raw_byte_count,
    )
    _stage_evidence(
        stage_file,
        staged_evidence_by_sha256,
        budget,
        evidence_sha256=evidence_locator.sha256,
        raw_evidence=raw_evidence,
    )


def _stage_locator_record(
    stage_file: BinaryIO,
    record_locator: SourceWitnessRecordLocator,
    bundle_file: BinaryIO,
    budget: _StreamingBudget,
) -> _StagedRecord:
    compressed_record = _read_locator_payload(
        bundle_file,
        record_locator.bundle,
        offset=record_locator.offset,
        length=record_locator.length,
        field_name="record",
    )
    if hashlib.sha256(compressed_record).hexdigest() != (
        record_locator.compressed_sha256
    ):
        raise RuntimeError("strict V3 source witness bundle changed before materialization")
    record_fields = decode_record_locator_fields(compressed_record)
    if not _is_record_locator_match(
        record_locator,
        kind=record_fields.kind,
        priority=record_fields.priority,
        tie_breaker=record_fields.tie_breaker,
        raw_sha256=record_fields.raw_sha256,
        linked_provider_sha256=record_fields.linked_provider_sha256,
    ):
        raise RuntimeError("strict V3 source witness record locator is inconsistent")
    budget.reserve_stored_entry(len(compressed_record))
    return _StagedRecord(
        raw_source_sha256=record_locator.raw_source_sha256,
        payload=_append_stage(stage_file, compressed_record),
    )


def _stage_locator_bundles(
    stage_file: BinaryIO,
    selected_records: Sequence[SourceWitnessCandidate],
    staged_record_by_index: list[_StagedRecord | None],
    staged_evidence_by_sha256: dict[str, _StagedEvidence],
    budget: _StreamingBudget,
) -> None:
    locator_indices_by_bundle: dict[
        SourceWitnessBundleIdentity,
        list[tuple[int, SourceWitnessRecordLocator]],
    ] = {}
    for selected_index, selected_record in enumerate(selected_records):
        if isinstance(selected_record, SourceWitnessRecordLocator):
            locator_indices_by_bundle.setdefault(selected_record.bundle, []).append(
                (selected_index, selected_record)
            )
    for bundle_identity, indexed_locators in locator_indices_by_bundle.items():
        with _materialization_bundle_file(bundle_identity) as bundle_file:
            validated_evidence_locators: set[tuple[str, int, int, int]] = set()
            for selected_index, record_locator in indexed_locators:
                staged_record_by_index[selected_index] = _stage_locator_record(
                    stage_file,
                    record_locator,
                    bundle_file,
                    budget,
                )
                for evidence_locator in record_locator.evidence_by_sha256.values():
                    evidence_key = (
                        evidence_locator.sha256,
                        evidence_locator.raw_byte_count,
                        evidence_locator.offset,
                        evidence_locator.length,
                    )
                    if evidence_key in validated_evidence_locators:
                        continue
                    _stage_locator_evidence(
                        stage_file,
                        staged_evidence_by_sha256,
                        budget,
                        bundle_file,
                        record_locator,
                        evidence_locator,
                    )
                    validated_evidence_locators.add(evidence_key)


def _stage_legacy_record(
    stage_file: BinaryIO,
    selected_record: CompressedSourceWitnessRecord,
    staged_evidence_by_sha256: dict[str, _StagedEvidence],
    budget: _StreamingBudget,
) -> _StagedRecord:
    if selected_record.evidence_by_sha256 is None:
        compressed_record, evidence_by_sha256 = externalize_source_evidence_record(
            selected_record.compressed,
            selected_record.raw_source_sha256,
        )
    else:
        compressed_record = selected_record.compressed
        evidence_by_sha256 = dict(selected_record.evidence_by_sha256)
    for evidence_sha256, raw_evidence in evidence_by_sha256.items():
        _stage_evidence(
            stage_file,
            staged_evidence_by_sha256,
            budget,
            evidence_sha256=evidence_sha256,
            raw_evidence=raw_evidence,
        )
    budget.reserve_stored_entry(len(compressed_record))
    return _StagedRecord(
        raw_source_sha256=selected_record.raw_source_sha256,
        payload=_append_stage(stage_file, compressed_record),
    )


def _payload_header(
    counts: SourceWitnessPayloadCounts,
    staged_records: Sequence[_StagedRecord],
    staged_evidence: Sequence[_StagedEvidence],
    stage_file: BinaryIO,
) -> dict[str, object]:
    sample_hasher = hashlib.sha256()
    for staged_record in staged_records:
        sample_hasher.update(bytes.fromhex(staged_record.raw_source_sha256))
        sample_hasher.update(_read_stage(stage_file, staged_record.payload))
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
        "evidence_dictionary_count": len(staged_evidence),
        "evidence_dictionary_raw_bytes": sum(
            evidence.raw_byte_count for evidence in staged_evidence
        ),
        "evidence_dictionary_stored_bytes": sum(
            evidence.payload.length for evidence in staged_evidence
        ),
    }


def _write_bounded(
    output_file: BinaryIO,
    payload_part: bytes,
    written_byte_count: int,
) -> int:
    projected = written_byte_count + len(payload_part)
    if projected > PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES:
        raise _payload_limit_error("exceeds its 512 MiB logical payload safety bound")
    output_file.write(payload_part)
    return projected


def _copy_staged_payload(
    stage_file: BinaryIO,
    output_file: BinaryIO,
    staged_payload: _StagedPayload,
    written_byte_count: int,
) -> int:
    stage_file.seek(staged_payload.offset)
    remaining_bytes = staged_payload.length
    while remaining_bytes:
        payload_part = stage_file.read(min(1024 * 1024, remaining_bytes))
        if not payload_part:
            raise RuntimeError("strict V3 source witness staging file is truncated")
        written_byte_count = _write_bounded(
            output_file,
            payload_part,
            written_byte_count,
        )
        remaining_bytes -= len(payload_part)
    return written_byte_count


def _encode_staged_payload(
    header: Mapping[str, object],
    staged_records: Sequence[_StagedRecord],
    staged_evidence: Sequence[_StagedEvidence],
    stage_file: BinaryIO,
) -> bytes:
    """Assemble the exact bounded payload from disk-backed staged entries."""

    header_bytes = json.dumps(
        dict(header),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    projected_payload_bytes = (
        len(PERSISTED_PAYLOAD_MAGIC)
        + U32.size
        + len(header_bytes)
        + U32.size * 2
        + sum(32 + U32.size + evidence.payload.length for evidence in staged_evidence)
        + sum(
            32 + U32.size + staged_record.payload.length
            for staged_record in staged_records
        )
    )
    if projected_payload_bytes > PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES:
        raise _payload_limit_error("exceeds its 512 MiB logical payload safety bound")
    with tempfile.TemporaryFile(mode="w+b") as output_file:
        written_byte_count = _write_bounded(
            output_file,
            PERSISTED_PAYLOAD_MAGIC,
            0,
        )
        for payload_part in (
            U32.pack(len(header_bytes)),
            header_bytes,
            U32.pack(len(staged_evidence)),
        ):
            written_byte_count = _write_bounded(
                output_file,
                payload_part,
                written_byte_count,
            )
        for evidence in staged_evidence:
            for payload_part in (
                bytes.fromhex(evidence.sha256),
                U32.pack(evidence.payload.length),
            ):
                written_byte_count = _write_bounded(
                    output_file,
                    payload_part,
                    written_byte_count,
                )
            written_byte_count = _copy_staged_payload(
                stage_file,
                output_file,
                evidence.payload,
                written_byte_count,
            )
        written_byte_count = _write_bounded(
            output_file,
            U32.pack(len(staged_records)),
            written_byte_count,
        )
        for staged_record in staged_records:
            for payload_part in (
                bytes.fromhex(staged_record.raw_source_sha256),
                U32.pack(staged_record.payload.length),
            ):
                written_byte_count = _write_bounded(
                    output_file,
                    payload_part,
                    written_byte_count,
                )
            written_byte_count = _copy_staged_payload(
                stage_file,
                output_file,
                staged_record.payload,
                written_byte_count,
            )
        if written_byte_count != projected_payload_bytes:
            raise RuntimeError("strict V3 source witness staging byte count changed")
        output_file.seek(0)
        witness_payload = output_file.read(projected_payload_bytes + 1)
    if len(witness_payload) != projected_payload_bytes:
        raise RuntimeError("strict V3 source witness staging file is truncated")
    return witness_payload


def encode_persisted_source_witness_candidates(
    selected_records: Sequence[SourceWitnessCandidate],
    counts: SourceWitnessPayloadCounts,
) -> tuple[bytes, dict[str, object]]:
    """Encode exact selected records with bounded one-evidence-at-a-time decoding."""

    budget = _preflight_selected_evidence(selected_records)
    staged_record_by_index: list[_StagedRecord | None] = [None] * len(selected_records)
    staged_evidence_by_sha256: dict[str, _StagedEvidence] = {}
    with tempfile.TemporaryFile(mode="w+b") as stage_file:
        _stage_locator_bundles(
            stage_file,
            selected_records,
            staged_record_by_index,
            staged_evidence_by_sha256,
            budget,
        )
        for selected_index, selected_record in enumerate(selected_records):
            if isinstance(selected_record, CompressedSourceWitnessRecord):
                staged_record_by_index[selected_index] = _stage_legacy_record(
                    stage_file,
                    selected_record,
                    staged_evidence_by_sha256,
                    budget,
                )
        if any(staged_record is None for staged_record in staged_record_by_index):
            raise RuntimeError("strict V3 source witness staging is incomplete")
        staged_records = tuple(
            staged_record
            for staged_record in staged_record_by_index
            if staged_record is not None
        )
        staged_evidence_entries = tuple(
            staged_evidence_by_sha256[evidence_sha256]
            for evidence_sha256 in sorted(staged_evidence_by_sha256)
        )
        header = _payload_header(
            counts,
            staged_records,
            staged_evidence_entries,
            stage_file,
        )
        witness_payload = _encode_staged_payload(
            header,
            staged_records,
            staged_evidence_entries,
            stage_file,
        )
    return witness_payload, {
        **header,
        "payload_sha256": hashlib.sha256(witness_payload).hexdigest(),
        "payload_bytes": len(witness_payload),
        "compression": PTG2_V3_SOURCE_WITNESS_PAYLOAD_COMPRESSION,
    }


__all__ = ["encode_persisted_source_witness_candidates"]
