# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate scanner bundles and retain bounded source-witness locators."""

from __future__ import annotations

import hashlib
import os
import stat
from contextlib import contextmanager
from pathlib import Path
from typing import Any, BinaryIO, Iterator, Mapping

from process.ptg_parts.ptg2_source_witness_bundle import (
    _json_object,
    _validate_bundle_header,
    _validate_local_coverage,
    read_scanner_bundle,
)
from process.ptg_parts.ptg2_source_witness_codec import (
    decode_record_locator_fields,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    PTG2_V3_SOURCE_WITNESS_MAX_BUNDLE_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES,
    PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
    SOURCE_BUNDLE_MAGIC,
    SOURCE_DICTIONARY_BUNDLE_MAGIC,
    SourceWitnessBundleIdentity,
    SourceWitnessCandidate,
    SourceWitnessEvidenceLocator,
    SourceWitnessRecordLocator,
)
from process.ptg_parts.ptg2_source_witness_primitives import U32, sha256_hex


def _file_identity_fields(file_stat: os.stat_result) -> tuple[int, int, int, int]:
    return (
        file_stat.st_dev,
        file_stat.st_ino,
        file_stat.st_size,
        file_stat.st_mtime_ns,
    )


def _open_bundle_file(bundle_path: Path) -> BinaryIO:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        file_descriptor = os.open(bundle_path, flags)
    except OSError as exc:
        raise RuntimeError("strict V3 source witness bundle is missing") from exc
    try:
        return os.fdopen(file_descriptor, "rb")
    except BaseException:
        os.close(file_descriptor)
        raise


def _validate_bundle_stat(
    bundle_entry: Mapping[str, Any],
    initial_stat: os.stat_result,
) -> None:
    bundle_size = initial_stat.st_size
    if (
        not stat.S_ISREG(initial_stat.st_mode)
        or bundle_size <= 0
        or bundle_size > PTG2_V3_SOURCE_WITNESS_MAX_BUNDLE_BYTES
    ):
        raise RuntimeError("strict V3 source witness bundle size is invalid")
    if bundle_entry.get("byte_count") != bundle_size:
        raise RuntimeError("strict V3 source witness bundle byte count does not match")


def _stream_authenticated_digest(
    bundle_file: BinaryIO,
    *,
    bundle_size: int,
) -> str:
    digest = hashlib.sha256()
    remaining_bytes = bundle_size
    while remaining_bytes:
        payload_part = bundle_file.read(min(1024 * 1024, remaining_bytes))
        if not payload_part:
            raise RuntimeError("strict V3 source witness bundle changed while reading")
        digest.update(payload_part)
        remaining_bytes -= len(payload_part)
    if bundle_file.read(1):
        raise RuntimeError("strict V3 source witness bundle changed while reading")
    return digest.hexdigest()


def _bundle_identity(
    bundle_path: Path,
    expected_digest: str,
    initial_stat: os.stat_result,
) -> SourceWitnessBundleIdentity:
    return SourceWitnessBundleIdentity(
        path=str(bundle_path),
        sha256=expected_digest,
        byte_count=initial_stat.st_size,
        device=initial_stat.st_dev,
        inode=initial_stat.st_ino,
        mtime_ns=initial_stat.st_mtime_ns,
    )


@contextmanager
def _authenticated_bundle_file(
    bundle_entry: Mapping[str, Any],
) -> Iterator[tuple[BinaryIO, SourceWitnessBundleIdentity, bytes]]:
    """Authenticate one bundle with bounded reads and retain its file identity."""

    bundle_path = Path(str(bundle_entry.get("path") or ""))
    if not bundle_path.is_file() or bundle_path.is_symlink():
        raise RuntimeError("strict V3 source witness bundle is missing")
    expected_digest = sha256_hex(
        bundle_entry.get("sha256"),
        field_name="bundle digest",
    )
    with _open_bundle_file(bundle_path) as bundle_file:
        initial_stat = os.fstat(bundle_file.fileno())
        _validate_bundle_stat(bundle_entry, initial_stat)
        observed_digest = _stream_authenticated_digest(
            bundle_file,
            bundle_size=initial_stat.st_size,
        )
        authenticated_stat = os.fstat(bundle_file.fileno())
        if _file_identity_fields(authenticated_stat) != _file_identity_fields(
            initial_stat
        ):
            raise RuntimeError("strict V3 source witness bundle changed while reading")
        if observed_digest != expected_digest:
            raise RuntimeError("strict V3 source witness bundle digest does not match")

        bundle_file.seek(0)
        bundle_magic = bundle_file.read(len(SOURCE_DICTIONARY_BUNDLE_MAGIC))
        if bundle_magic not in {SOURCE_BUNDLE_MAGIC, SOURCE_DICTIONARY_BUNDLE_MAGIC}:
            raise RuntimeError("strict V3 source witness bundle magic is invalid")
        bundle_file.seek(0)
        try:
            yield (
                bundle_file,
                _bundle_identity(bundle_path, expected_digest, initial_stat),
                bundle_magic,
            )
        finally:
            bundle_file.seek(0)
            final_digest = _stream_authenticated_digest(
                bundle_file,
                bundle_size=initial_stat.st_size,
            )
            final_stat = os.fstat(bundle_file.fileno())
            if (
                final_digest != expected_digest
                or _file_identity_fields(final_stat)
                != _file_identity_fields(initial_stat)
            ):
                raise RuntimeError(
                    "strict V3 source witness bundle changed while reading"
                )


def _read_exact_file(
    bundle_file: BinaryIO,
    byte_count: int,
    *,
    field_name: str,
) -> bytes:
    if byte_count < 0:
        raise RuntimeError(f"strict V3 source witness {field_name} is invalid")
    payload = bundle_file.read(byte_count)
    if len(payload) != byte_count:
        raise RuntimeError(f"strict V3 source witness {field_name} is truncated")
    return payload


def _read_u32_file(bundle_file: BinaryIO, *, field_name: str) -> int:
    return U32.unpack(
        _read_exact_file(bundle_file, U32.size, field_name=field_name)
    )[0]


def _validated_entry_row_count(bundle_entry: Mapping[str, Any]) -> int:
    row_count = bundle_entry.get("row_count")
    if (
        type(row_count) is not int
        or row_count < 0
        or row_count > PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET
    ):
        raise RuntimeError("strict V3 source witness bundle row count is invalid")
    return row_count


def _read_dictionary_evidence_locators(
    bundle_file: BinaryIO,
    *,
    bundle_identity: SourceWitnessBundleIdentity,
    maximum_evidence_count: int,
) -> dict[str, SourceWitnessEvidenceLocator]:
    evidence_count = _read_u32_file(
        bundle_file,
        field_name="evidence dictionary count",
    )
    if evidence_count > maximum_evidence_count:
        raise RuntimeError(
            "strict V3 source witness evidence dictionary count is invalid"
        )
    evidence_locator_by_sha256: dict[str, SourceWitnessEvidenceLocator] = {}
    for _evidence_index in range(evidence_count):
        evidence_sha256 = _read_exact_file(
            bundle_file,
            32,
            field_name="evidence digest",
        ).hex()
        raw_byte_count = _read_u32_file(
            bundle_file,
            field_name="evidence raw length",
        )
        compressed_byte_count = _read_u32_file(
            bundle_file,
            field_name="evidence compressed length",
        )
        compressed_offset = bundle_file.tell()
        compressed_end = compressed_offset + compressed_byte_count
        if (
            raw_byte_count <= 0
            or raw_byte_count > PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES
            or compressed_byte_count <= 0
            or compressed_byte_count > PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES
            or compressed_end > bundle_identity.byte_count
            or evidence_sha256 in evidence_locator_by_sha256
        ):
            raise RuntimeError(
                "strict V3 source witness evidence dictionary framing is invalid"
            )
        evidence_locator_by_sha256[evidence_sha256] = SourceWitnessEvidenceLocator(
            sha256=evidence_sha256,
            raw_byte_count=raw_byte_count,
            offset=compressed_offset,
            length=compressed_byte_count,
        )
        bundle_file.seek(compressed_byte_count, os.SEEK_CUR)
    return evidence_locator_by_sha256


def _record_evidence_locator_map(
    *,
    raw_sha256: str,
    linked_provider_sha256: str | None,
    evidence_locator_by_sha256: Mapping[str, SourceWitnessEvidenceLocator],
) -> dict[str, SourceWitnessEvidenceLocator]:
    referenced_evidence_by_sha256: dict[str, SourceWitnessEvidenceLocator] = {}
    for evidence_sha256 in (raw_sha256, linked_provider_sha256):
        if evidence_sha256 is None:
            continue
        evidence_locator = evidence_locator_by_sha256.get(evidence_sha256)
        if evidence_locator is None:
            raise RuntimeError("strict V3 persisted source evidence is missing")
        referenced_evidence_by_sha256[evidence_sha256] = evidence_locator
    return referenced_evidence_by_sha256


def _read_dictionary_record_locators(
    bundle_file: BinaryIO,
    *,
    bundle_identity: SourceWitnessBundleIdentity,
    raw_source_sha256: str,
    expected_record_count: int,
    evidence_locator_by_sha256: Mapping[str, SourceWitnessEvidenceLocator],
) -> list[SourceWitnessRecordLocator]:
    record_count = _read_u32_file(bundle_file, field_name="record count")
    if record_count != expected_record_count:
        raise RuntimeError(
            "strict V3 source witness bundle record count does not match"
        )
    record_locators: list[SourceWitnessRecordLocator] = []
    for _record_index in range(record_count):
        compressed_byte_count = _read_u32_file(
            bundle_file,
            field_name="record length",
        )
        compressed_offset = bundle_file.tell()
        compressed_end = compressed_offset + compressed_byte_count
        if (
            compressed_byte_count <= 0
            or compressed_byte_count > PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES
            or compressed_end > bundle_identity.byte_count
        ):
            raise RuntimeError("strict V3 source witness record framing is invalid")
        compressed_record = _read_exact_file(
            bundle_file,
            compressed_byte_count,
            field_name="record",
        )
        record_fields = decode_record_locator_fields(compressed_record)
        record_locators.append(
            SourceWitnessRecordLocator(
                kind=record_fields.kind,
                priority=record_fields.priority,
                tie_breaker=record_fields.tie_breaker,
                raw_source_sha256=raw_source_sha256,
                raw_sha256=record_fields.raw_sha256,
                linked_provider_sha256=record_fields.linked_provider_sha256,
                bundle=bundle_identity,
                offset=compressed_offset,
                length=compressed_byte_count,
                compressed_sha256=hashlib.sha256(compressed_record).hexdigest(),
                evidence_by_sha256=_record_evidence_locator_map(
                    raw_sha256=record_fields.raw_sha256,
                    linked_provider_sha256=record_fields.linked_provider_sha256,
                    evidence_locator_by_sha256=evidence_locator_by_sha256,
                ),
            )
        )
    return record_locators


def _read_current_dictionary_bundle(
    bundle_file: BinaryIO,
    bundle_identity: SourceWitnessBundleIdentity,
    bundle_entry: Mapping[str, Any],
) -> tuple[dict[str, Any], list[SourceWitnessCandidate]]:
    _read_exact_file(
        bundle_file,
        len(SOURCE_DICTIONARY_BUNDLE_MAGIC),
        field_name="bundle magic",
    )
    header_length = _read_u32_file(bundle_file, field_name="header")
    if (
        header_length <= 0
        or header_length > PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES
        or bundle_file.tell() + header_length > bundle_identity.byte_count
    ):
        raise RuntimeError("strict V3 source witness bundle header is truncated")
    bundle_header = _json_object(
        _read_exact_file(bundle_file, header_length, field_name="bundle header"),
        field_name="bundle header",
    )
    _validate_bundle_header(bundle_header, bundle_entry, format_version=3)
    raw_source_sha256 = sha256_hex(
        bundle_header.get("raw_source_sha256"),
        field_name="raw source digest",
    )
    expected_record_count = _validated_entry_row_count(bundle_entry)
    evidence_locator_by_sha256 = _read_dictionary_evidence_locators(
        bundle_file,
        bundle_identity=bundle_identity,
        maximum_evidence_count=expected_record_count * 2,
    )
    record_locators = _read_dictionary_record_locators(
        bundle_file,
        bundle_identity=bundle_identity,
        raw_source_sha256=raw_source_sha256,
        expected_record_count=expected_record_count,
        evidence_locator_by_sha256=evidence_locator_by_sha256,
    )
    if bundle_file.tell() != bundle_identity.byte_count:
        raise RuntimeError("strict V3 source witness bundle record count does not match")
    _validate_local_coverage(bundle_header, record_locators)
    return bundle_header, record_locators


def read_scanner_bundle_locators(
    bundle_entry: Mapping[str, Any],
) -> tuple[dict[str, Any], list[SourceWitnessCandidate]]:
    """Authenticate one bundle and retain fixed-size current-format locators."""

    is_legacy_bundle = False
    with _authenticated_bundle_file(bundle_entry) as (
        bundle_file,
        bundle_identity,
        bundle_magic,
    ):
        is_legacy_bundle = bundle_magic == SOURCE_BUNDLE_MAGIC
        if not is_legacy_bundle:
            return _read_current_dictionary_bundle(
                bundle_file,
                bundle_identity,
                bundle_entry,
            )
    if is_legacy_bundle:
        return read_scanner_bundle(bundle_entry)
    raise RuntimeError("strict V3 source witness bundle magic is invalid")


__all__ = ["read_scanner_bundle_locators"]
