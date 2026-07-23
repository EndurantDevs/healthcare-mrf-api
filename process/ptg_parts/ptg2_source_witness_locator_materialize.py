# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Materialize exact evidence only for globally selected witness locators."""

from __future__ import annotations

import hashlib
import os
import stat
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Iterator, Mapping, Sequence

from process.ptg_parts.ptg2_source_witness_bundle import _decompress_evidence
from process.ptg_parts.ptg2_source_witness_codec import (
    decode_persisted_record,
    decode_record_locator_fields,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    CompressedSourceWitnessRecord,
    PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES,
    SourceWitnessBundleIdentity,
    SourceWitnessCandidate,
    SourceWitnessEvidenceLocator,
    SourceWitnessRecordLocator,
)
from process.ptg_parts.ptg2_source_witness_locator_reader import (
    _file_identity_fields,
    _open_bundle_file,
)


def _validate_materialization_identity(
    file_stat: os.stat_result,
    bundle_identity: SourceWitnessBundleIdentity,
) -> None:
    if (
        not stat.S_ISREG(file_stat.st_mode)
        or file_stat.st_dev != bundle_identity.device
        or file_stat.st_ino != bundle_identity.inode
        or file_stat.st_size != bundle_identity.byte_count
        or file_stat.st_mtime_ns != bundle_identity.mtime_ns
    ):
        raise RuntimeError(
            "strict V3 source witness bundle changed before materialization"
        )


@contextmanager
def _materialization_bundle_file(
    bundle_identity: SourceWitnessBundleIdentity,
) -> Iterator[BinaryIO]:
    bundle_path = Path(bundle_identity.path)
    if not bundle_path.is_file() or bundle_path.is_symlink():
        raise RuntimeError(
            "strict V3 source witness bundle changed before materialization"
        )
    try:
        bundle_file = _open_bundle_file(bundle_path)
    except RuntimeError as exc:
        raise RuntimeError(
            "strict V3 source witness bundle changed before materialization"
        ) from exc
    with bundle_file:
        initial_stat = os.fstat(bundle_file.fileno())
        _validate_materialization_identity(initial_stat, bundle_identity)
        try:
            yield bundle_file
        finally:
            final_stat = os.fstat(bundle_file.fileno())
            if _file_identity_fields(final_stat) != _file_identity_fields(
                initial_stat
            ):
                raise RuntimeError(
                    "strict V3 source witness bundle changed during materialization"
                )


def _read_locator_payload(
    bundle_file: BinaryIO,
    bundle_identity: SourceWitnessBundleIdentity,
    *,
    offset: int,
    length: int,
    field_name: str,
) -> bytes:
    if (
        type(offset) is not int
        or type(length) is not int
        or offset < 0
        or length <= 0
        or length > PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES
        or offset + length > bundle_identity.byte_count
    ):
        raise RuntimeError(f"strict V3 source witness {field_name} locator is invalid")
    bundle_file.seek(offset)
    locator_payload = bundle_file.read(length)
    if len(locator_payload) != length:
        raise RuntimeError(f"strict V3 source witness {field_name} is truncated")
    return locator_payload


def _is_record_locator_match(
    locator: SourceWitnessRecordLocator,
    *,
    kind: str,
    priority: int,
    tie_breaker: str,
    raw_sha256: str,
    linked_provider_sha256: str | None,
) -> bool:
    return (
        locator.kind == kind
        and locator.priority == priority
        and locator.tie_breaker == tie_breaker
        and locator.raw_sha256 == raw_sha256
        and locator.linked_provider_sha256 == linked_provider_sha256
    )


def _evidence_locator_map(
    indexed_locators: Sequence[tuple[int, SourceWitnessRecordLocator]],
) -> dict[str, SourceWitnessEvidenceLocator]:
    evidence_locator_by_sha256: dict[str, SourceWitnessEvidenceLocator] = {}
    for _selected_index, record_locator in indexed_locators:
        for evidence_sha256, evidence_locator in (
            record_locator.evidence_by_sha256.items()
        ):
            if evidence_sha256 != evidence_locator.sha256:
                raise RuntimeError(
                    "strict V3 source witness evidence locator is invalid"
                )
            existing_locator = evidence_locator_by_sha256.setdefault(
                evidence_sha256,
                evidence_locator,
            )
            if existing_locator != evidence_locator:
                raise RuntimeError(
                    "strict V3 source witness evidence locator is inconsistent"
                )
    return evidence_locator_by_sha256


def _materialized_evidence_map(
    bundle_file: BinaryIO,
    bundle_identity: SourceWitnessBundleIdentity,
    evidence_locator_by_sha256: Mapping[str, SourceWitnessEvidenceLocator],
) -> dict[str, bytes]:
    evidence_by_sha256: dict[str, bytes] = {}
    for evidence_sha256, evidence_locator in evidence_locator_by_sha256.items():
        compressed_evidence = _read_locator_payload(
            bundle_file,
            bundle_identity,
            offset=evidence_locator.offset,
            length=evidence_locator.length,
            field_name="evidence",
        )
        raw_evidence = _decompress_evidence(
            compressed_evidence,
            evidence_locator.raw_byte_count,
        )
        if hashlib.sha256(raw_evidence).hexdigest() != evidence_sha256:
            raise RuntimeError("strict V3 source witness evidence digest is invalid")
        evidence_by_sha256[evidence_sha256] = raw_evidence
    return evidence_by_sha256


def _materialize_record_locator(
    bundle_file: BinaryIO,
    bundle_identity: SourceWitnessBundleIdentity,
    record_locator: SourceWitnessRecordLocator,
    evidence_by_sha256: Mapping[str, bytes],
) -> CompressedSourceWitnessRecord:
    compressed_record = _read_locator_payload(
        bundle_file,
        bundle_identity,
        offset=record_locator.offset,
        length=record_locator.length,
        field_name="record",
    )
    if (
        hashlib.sha256(compressed_record).hexdigest()
        != record_locator.compressed_sha256
    ):
        raise RuntimeError(
            "strict V3 source witness bundle changed before materialization"
        )
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
    record_evidence_by_sha256 = {
        evidence_sha256: evidence_by_sha256[evidence_sha256]
        for evidence_sha256 in record_locator.evidence_by_sha256
    }
    decoded_record = decode_persisted_record(
        compressed_record,
        record_locator.raw_source_sha256,
        evidence_by_sha256=record_evidence_by_sha256,
    )
    if not _is_record_locator_match(
        record_locator,
        kind=decoded_record.kind,
        priority=decoded_record.priority,
        tie_breaker=decoded_record.tie_breaker,
        raw_sha256=decoded_record.raw_sha256,
        linked_provider_sha256=decoded_record.linked_provider_sha256,
    ):
        raise RuntimeError("strict V3 source witness record locator is inconsistent")
    return CompressedSourceWitnessRecord(
        kind=record_locator.kind,
        priority=record_locator.priority,
        tie_breaker=record_locator.tie_breaker,
        raw_source_sha256=record_locator.raw_source_sha256,
        compressed=compressed_record,
        evidence_by_sha256=record_evidence_by_sha256,
    )


def _materialize_bundle_locators(
    bundle_identity: SourceWitnessBundleIdentity,
    indexed_locators: Sequence[tuple[int, SourceWitnessRecordLocator]],
    materialized_records: list[CompressedSourceWitnessRecord | None],
) -> None:
    with _materialization_bundle_file(bundle_identity) as bundle_file:
        evidence_locator_by_sha256 = _evidence_locator_map(indexed_locators)
        evidence_by_sha256 = _materialized_evidence_map(
            bundle_file,
            bundle_identity,
            evidence_locator_by_sha256,
        )
        for selected_index, record_locator in indexed_locators:
            materialized_records[selected_index] = _materialize_record_locator(
                bundle_file,
                bundle_identity,
                record_locator,
                evidence_by_sha256,
            )


def materialize_source_witness_locators(
    selected_records: Sequence[SourceWitnessCandidate],
) -> list[CompressedSourceWitnessRecord]:
    """Materialize exact source tokens only for globally selected locators."""

    materialized_records: list[CompressedSourceWitnessRecord | None] = [
        None
    ] * len(selected_records)
    locator_indices_by_bundle: dict[
        SourceWitnessBundleIdentity,
        list[tuple[int, SourceWitnessRecordLocator]],
    ] = {}
    for selected_index, selected_record in enumerate(selected_records):
        if isinstance(selected_record, CompressedSourceWitnessRecord):
            materialized_records[selected_index] = selected_record
            continue
        locator_indices_by_bundle.setdefault(selected_record.bundle, []).append(
            (selected_index, selected_record)
        )
    for bundle_identity, indexed_locators in locator_indices_by_bundle.items():
        _materialize_bundle_locators(
            bundle_identity,
            indexed_locators,
            materialized_records,
        )
    if any(witness_record is None for witness_record in materialized_records):
        raise RuntimeError("strict V3 source witness materialization is incomplete")
    return [
        witness_record
        for witness_record in materialized_records
        if isinstance(witness_record, CompressedSourceWitnessRecord)
    ]


__all__ = ["materialize_source_witness_locators"]
