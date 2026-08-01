# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Sparse independent raw-byte proof for redacted UHC provider tombstones."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import stat
from typing import Any, Callable

from process.uhc_provider_quarantine_contract import (
    UHC_PROVIDER_QUARANTINE_MAX_COUNT,
    UhcProviderQuarantine,
)
from process.uhc_provider_quarantine_record import (
    UhcProviderQuarantineRecordCensus,
    UhcProviderQuarantineRecordError,
    combine_provider_quarantine_census,
    validate_checksum_invalid_provider_record,
)
from process.uhc_retained_range_manifest import load_verified_range_manifest
from process.uhc_retained_types import (
    RawRangeProof,
    UHCRetainedAdmissionError,
    UhcProviderQuarantineRawSource,
)


_MANIFEST_MAX_BYTES = _RAW_READ_BYTES = 1024 * 1024
_IGNORABLE_OUTSIDE_OBJECT = frozenset(b" \t\r\n,[]")


class UhcProviderQuarantineRawError(ValueError):
    """Reject a tombstone not proven by its exact admitted source record."""


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value_by_key: dict[str, Any] = {}
    for key, value in pairs:
        if key in value_by_key:
            raise ValueError("duplicate key")
        value_by_key[key] = value
    return value_by_key


def _file_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
        metadata.st_mode,
        metadata.st_nlink,
    )


def _is_safe_regular_file(metadata: os.stat_result, expected_bytes: int) -> bool:
    return (
        stat.S_ISREG(metadata.st_mode)
        and metadata.st_nlink == 1
        and metadata.st_mode & 0o022 == 0
        and metadata.st_size == expected_bytes
    )


def _open_readonly(path: Path) -> int:
    try:
        return os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as error:
        raise UhcProviderQuarantineRawError(
            "retained UHC quarantine source file is unavailable"
        ) from error


def _read_manifest_identity(
    manifest_path: Path,
    expected_sha256: str,
    expected_producer_build_id: str,
) -> int:
    """Read an immutable manifest and bind its producer to the build."""
    descriptor = _open_readonly(manifest_path)
    try:
        before = os.fstat(descriptor)
        is_bounded = 0 < before.st_size <= _MANIFEST_MAX_BYTES
        if not is_bounded or not _is_safe_regular_file(before, before.st_size):
            raise UhcProviderQuarantineRawError(
                "retained UHC quarantine manifest is unsafe"
            )
        encoded = bytearray()
        while len(encoded) < before.st_size:
            chunk = os.read(
                descriptor,
                min(_RAW_READ_BYTES, before.st_size - len(encoded)),
            )
            if not chunk:
                raise UhcProviderQuarantineRawError(
                    "retained UHC quarantine manifest ended early"
                )
            encoded.extend(chunk)
        if (
            _file_identity(os.fstat(descriptor)) != _file_identity(before)
            or hashlib.sha256(encoded).hexdigest() != expected_sha256
        ):
            raise UhcProviderQuarantineRawError(
                "retained UHC quarantine manifest identity changed"
            )
        try:
            manifest = json.loads(
                encoded,
                object_pairs_hook=_reject_duplicate_keys,
            )
        except (UnicodeDecodeError, ValueError) as error:
            raise UhcProviderQuarantineRawError(
                "retained UHC quarantine manifest JSON is invalid"
            ) from error
        producer_build_id = (
            manifest.get("producer_build_id")
            if isinstance(manifest, dict)
            else None
        )
        if (
            not isinstance(producer_build_id, str)
            or producer_build_id != expected_producer_build_id
            or len(producer_build_id) > 256
            or not producer_build_id.isascii()
            or not producer_build_id.isprintable()
        ):
            raise UhcProviderQuarantineRawError(
                "retained UHC quarantine manifest producer is invalid"
            )
        return len(encoded)
    finally:
        os.close(descriptor)


class _JsonObjectFramer:
    """Frame exact top-level JSON object bytes across bounded chunks."""

    def __init__(
        self,
        observe: Callable[[bytes], None],
        max_record_bytes: int,
    ) -> None:
        self._observe = observe
        self._max_record_bytes = max_record_bytes
        self._record = bytearray()
        self._depth = 0
        self._in_string = False
        self._escaped = False

    def feed(self, chunk: bytes) -> None:
        """Consume one bounded raw byte chunk without changing byte identity."""

        for byte in chunk:
            if self._depth == 0:
                self._consume_outside_byte(byte)
                continue
            self._consume_record_byte(byte)

    def _consume_outside_byte(self, byte: int) -> None:
        if byte in _IGNORABLE_OUTSIDE_OBJECT:
            return
        if byte != ord("{"):
            raise UhcProviderQuarantineRawError(
                "retained UHC quarantine range framing is invalid"
            )
        self._record.append(byte)
        self._depth = 1

    def _consume_record_byte(self, byte: int) -> None:
        self._record.append(byte)
        if len(self._record) > self._max_record_bytes:
            raise UhcProviderQuarantineRawError(
                "retained UHC quarantine record exceeds its byte bound"
            )
        if self._in_string:
            self._consume_string_byte(byte)
            return
        self._consume_structure_byte(byte)

    def _consume_string_byte(self, byte: int) -> None:
        if self._escaped:
            self._escaped = False
        elif byte == ord("\\"):
            self._escaped = True
        elif byte == ord('"'):
            self._in_string = False

    def _consume_structure_byte(self, byte: int) -> None:
        if byte == ord('"'):
            self._in_string = True
            return
        if byte == ord("{"):
            self._depth += 1
            return
        if byte != ord("}"):
            return
        self._depth -= 1
        if self._depth == 0:
            self._observe(bytes(self._record))
            self._record.clear()

    def finish(self) -> None:
        """Reject an incomplete object or string at the range boundary."""

        if self._depth or self._record or self._in_string or self._escaped:
            raise UhcProviderQuarantineRawError(
                "retained UHC quarantine range framing is incomplete"
            )


def _validate_target_record(
    record: bytes,
    quarantine: UhcProviderQuarantine,
) -> UhcProviderQuarantineRecordCensus:
    if hashlib.sha256(record).hexdigest() != quarantine.record_sha256:
        raise UhcProviderQuarantineRawError(
            "retained UHC quarantine record hash does not match"
        )
    try:
        decoded = json.loads(record, object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, ValueError) as error:
        raise UhcProviderQuarantineRawError(
            "retained UHC quarantine record JSON is invalid"
        ) from error
    try:
        return validate_checksum_invalid_provider_record(decoded)
    except UhcProviderQuarantineRecordError as error:
        raise UhcProviderQuarantineRawError(
            "retained UHC quarantine is not checksum-invalid-only"
        ) from error


class _RangeRecordVerifier:
    """Bind framed records to one admitted range and sparse target set."""

    def __init__(
        self,
        raw_range: RawRangeProof,
        targets: dict[int, UhcProviderQuarantine],
        observed_census_by_occurrence: dict[
            int,
            UhcProviderQuarantineRecordCensus,
        ],
    ) -> None:
        self.raw_range = raw_range
        self.targets = targets
        self.observed_census_by_occurrence = observed_census_by_occurrence
        self.canonical_digest = hashlib.sha256()
        self.canonical_byte_count = 0
        self.record_index = 0

    def observe(self, record: bytes) -> None:
        """Accumulate exact canonical proof and validate sparse targets."""

        occurrence = self.raw_range.record_start + self.record_index
        canonical = record.replace(b"\r", b"").replace(b"\n", b"") + b"\n"
        self.canonical_digest.update(canonical)
        self.canonical_byte_count += len(canonical)
        if occurrence in self.targets:
            self.observed_census_by_occurrence[occurrence] = (
                _validate_target_record(record, self.targets[occurrence])
            )
        self.record_index += 1

    def assert_complete(self, raw_digest: Any) -> None:
        """Require exact record count and raw plus canonical range digests."""

        if (
            self.record_index != self.raw_range.record_count
            or raw_digest.hexdigest() != self.raw_range.raw_sha256
            or self.canonical_digest.hexdigest()
            != self.raw_range.canonical_sha256
            or self.canonical_byte_count
            != self.raw_range.canonical_byte_count
        ):
            raise UhcProviderQuarantineRawError(
                "retained UHC quarantine range proof changed"
            )


def _verify_range(
    descriptor: int,
    raw_range: RawRangeProof,
    quarantine_by_occurrence: dict[int, UhcProviderQuarantine],
    observed_census_by_occurrence: dict[
        int,
        UhcProviderQuarantineRecordCensus,
    ],
    max_record_bytes: int,
) -> None:
    raw_digest = hashlib.sha256()
    verifier = _RangeRecordVerifier(
        raw_range,
        quarantine_by_occurrence,
        observed_census_by_occurrence,
    )
    framer = _JsonObjectFramer(verifier.observe, max_record_bytes)
    offset = raw_range.raw_byte_start
    while offset < raw_range.raw_byte_end:
        requested = min(_RAW_READ_BYTES, raw_range.raw_byte_end - offset)
        try:
            chunk = os.pread(descriptor, requested, offset)
        except OSError as error:
            raise UhcProviderQuarantineRawError(
                "retained UHC quarantine range read failed"
            ) from error
        if not chunk:
            raise UhcProviderQuarantineRawError(
                "retained UHC quarantine range ended early"
            )
        raw_digest.update(chunk)
        framer.feed(chunk)
        offset += len(chunk)
    framer.finish()
    verifier.assert_complete(raw_digest)


@dataclass(frozen=True)
class _RawVerificationRequest:
    """Exact retained identity plus sparse quarantine verification bounds."""

    source: UhcProviderQuarantineRawSource
    quarantines: tuple[UhcProviderQuarantine, ...]
    max_record_bytes: int

    def __getattr__(self, field_name: str) -> Any:
        """Expose immutable source fields without copying their identity."""

        return getattr(self.source, field_name)


def _quarantine_by_occurrence(
    request: _RawVerificationRequest,
) -> dict[int, UhcProviderQuarantine]:
    if (
        isinstance(request.max_record_bytes, bool)
        or not isinstance(request.max_record_bytes, int)
        or not 1 <= request.max_record_bytes <= 64 * 1024 * 1024
    ):
        raise UhcProviderQuarantineRawError(
            "retained UHC quarantine record bound is invalid"
        )
    if len(request.quarantines) > UHC_PROVIDER_QUARANTINE_MAX_COUNT:
        raise UhcProviderQuarantineRawError(
            "retained UHC quarantine exceeds sparse verification bound"
        )
    if any(
        quarantine.source_file_id != request.source_file_id
        for quarantine in request.quarantines
    ):
        raise UhcProviderQuarantineRawError(
            "retained UHC quarantine source identity changed"
        )
    quarantine_by_occurrence = {
        quarantine.occurrence_ordinal: quarantine
        for quarantine in request.quarantines
    }
    if len(quarantine_by_occurrence) != len(request.quarantines):
        raise UhcProviderQuarantineRawError(
            "retained UHC quarantine occurrence is duplicated"
        )
    return quarantine_by_occurrence


def _load_range_by_ordinal(
    request: _RawVerificationRequest,
) -> dict[int, RawRangeProof]:
    manifest_bytes = _read_manifest_identity(
        request.manifest_path,
        request.manifest_sha256,
        request.raw_producer_build_id,
    )
    try:
        raw_artifact, raw_range_proofs = load_verified_range_manifest(
            raw_path=request.raw_path,
            manifest_path=request.manifest_path,
            expected_artifact_sha256=request.artifact_sha256,
            expected_artifact_bytes=request.artifact_byte_count,
            expected_manifest_sha256=request.manifest_sha256,
            expected_manifest_bytes=manifest_bytes,
            expected_range_count=request.range_count,
            producer_build_id=request.raw_producer_build_id,
            verify_raw_bytes=False,
        )
    except UHCRetainedAdmissionError as error:
        raise UhcProviderQuarantineRawError(
            "retained UHC quarantine manifest proof is invalid"
        ) from error
    if (
        raw_artifact.contract_version != request.raw_contract_version
        or raw_artifact.producer_build_id != request.raw_producer_build_id
        or raw_artifact.record_count != request.record_count
        or raw_artifact.range_set_sha256 != request.range_set_sha256
    ):
        raise UhcProviderQuarantineRawError(
            "retained UHC quarantine admitted layout changed"
        )
    return {
        raw_range.range_ordinal: raw_range
        for raw_range in raw_range_proofs
    }


def _quarantine_by_range(
    request: _RawVerificationRequest,
    range_by_ordinal: dict[int, RawRangeProof],
) -> dict[int, dict[int, UhcProviderQuarantine]]:
    quarantine_by_range: dict[int, dict[int, UhcProviderQuarantine]] = {}
    for quarantine in request.quarantines:
        raw_range = range_by_ordinal.get(quarantine.range_ordinal)
        if raw_range is None or not (
            raw_range.record_start
            <= quarantine.occurrence_ordinal
            < raw_range.record_end
        ):
            raise UhcProviderQuarantineRawError(
                "retained UHC quarantine range lineage changed"
            )
        quarantine_by_range.setdefault(quarantine.range_ordinal, {})[
            quarantine.occurrence_ordinal
        ] = quarantine
    return quarantine_by_range


def _verify_raw_ranges(
    request: _RawVerificationRequest,
    range_by_ordinal: dict[int, RawRangeProof],
    quarantine_by_range: dict[int, dict[int, UhcProviderQuarantine]],
    quarantine_by_occurrence: dict[int, UhcProviderQuarantine],
) -> UhcProviderQuarantineRecordCensus:
    descriptor = _open_readonly(request.raw_path)
    try:
        before = os.fstat(descriptor)
        if not _is_safe_regular_file(before, request.artifact_byte_count):
            raise UhcProviderQuarantineRawError(
                "retained UHC quarantine raw artifact is unsafe"
            )
        observed_census_by_occurrence: dict[
            int,
            UhcProviderQuarantineRecordCensus,
        ] = {}
        for range_ordinal in sorted(quarantine_by_range):
            _verify_range(
                descriptor,
                range_by_ordinal[range_ordinal],
                quarantine_by_range[range_ordinal],
                observed_census_by_occurrence,
                request.max_record_bytes,
            )
        if (
            set(observed_census_by_occurrence)
            != set(quarantine_by_occurrence)
            or _file_identity(os.fstat(descriptor)) != _file_identity(before)
        ):
            raise UhcProviderQuarantineRawError(
                "retained UHC quarantine source proof is incomplete"
            )
    finally:
        os.close(descriptor)
    return combine_provider_quarantine_census(
        observed_census_by_occurrence[occurrence]
        for occurrence in sorted(observed_census_by_occurrence)
    )


def verify_provider_quarantine_source_records(
    source: UhcProviderQuarantineRawSource,
    quarantines: tuple[UhcProviderQuarantine, ...],
    max_record_bytes: int,
) -> UhcProviderQuarantineRecordCensus:
    """Re-read only affected verified ranges and prove checksum-only rejection."""

    if type(source) is not UhcProviderQuarantineRawSource:
        raise UhcProviderQuarantineRawError(
            "retained UHC quarantine source contract is invalid"
        )
    request = _RawVerificationRequest(
        source=source,
        quarantines=quarantines,
        max_record_bytes=max_record_bytes,
    )
    quarantine_by_occurrence = _quarantine_by_occurrence(request)
    if not quarantine_by_occurrence:
        return UhcProviderQuarantineRecordCensus()
    range_by_ordinal = _load_range_by_ordinal(request)
    quarantine_by_range = _quarantine_by_range(request, range_by_ordinal)
    return _verify_raw_ranges(
        request,
        range_by_ordinal,
        quarantine_by_range,
        quarantine_by_occurrence,
    )
