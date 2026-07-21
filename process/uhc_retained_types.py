# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared immutable proofs and validation helpers for retained UHC data."""

from __future__ import annotations

import collections
import hashlib
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator


_SAFE_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_RAW_READ_CHUNK_BYTES = 1024 * 1024
_MIN_RAW_SHARD_COUNT = 4
_MAX_RAW_SHARD_COUNT = 256


class UHCRetainedAdmissionError(RuntimeError):
    """Raised when retained bytes or their immutable proofs violate the contract."""


@dataclass(frozen=True)
class RetainedRawArtifactProof:
    """One native-verified immutable raw file and its committed range manifest."""

    path: str
    sha256: str
    byte_count: int
    record_count: int
    contract_version: int
    range_count: int
    producer_build_id: str
    range_set_sha256: str
    canonical_byte_count: int
    manifest_path: str
    manifest_sha256: str
    manifest_byte_count: int


@dataclass(frozen=True)
class RawRangeProof:
    """One logical, range-aware replay partition over the retained raw file."""

    artifact_sha256: str
    contract_version: int
    range_count: int
    range_ordinal: int
    raw_byte_start: int
    raw_byte_end: int
    raw_sha256: str
    raw_byte_count: int
    record_start: int
    record_end: int
    record_count: int
    canonical_sha256: str
    canonical_byte_count: int
    path: str


@dataclass(frozen=True)
class VerifiedRetainedSource:
    """Opaque native-attested source returned by the retention boundary."""

    raw_artifact: RetainedRawArtifactProof
    ranges: tuple[RawRangeProof, ...]
    raw_reused: bool
    manifest_reused: bool
    verifier_build_id: str
    timings_seconds: tuple[tuple[str, float], ...]
    attestation: object


@dataclass(frozen=True)
class ArtifactReferenceProof:
    """One source-owned immutable raw or versioned-manifest reference."""

    content_sha256: str
    artifact_kind: str
    layout_artifact_sha256: str | None
    contract_version: int
    range_count: int
    storage_uri: str


def _validate_sha256(expected_sha256: str) -> None:
    """Reject an unsafe digest before it can influence a path or lock name."""

    if not isinstance(expected_sha256, str) or not _SAFE_SHA256_RE.fullmatch(
        expected_sha256
    ):
        raise UHCRetainedAdmissionError("expected artifact SHA-256 is invalid")


def _validate_artifact_identity(expected_sha256: str, expected_bytes: int) -> None:
    """Reject unsafe content identities before any path or allocation is built."""

    _validate_sha256(expected_sha256)
    if (
        type(expected_bytes) is not int
        or expected_bytes <= 0
        or expected_bytes > 2**63 - 1
    ):
        raise UHCRetainedAdmissionError("expected artifact byte count is invalid")


def _validate_range_count(range_count: int) -> None:
    """Bound logical-range fan-out before names or allocations are built."""

    if type(range_count) is not int:
        raise UHCRetainedAdmissionError("UHC admission range count is invalid")
    if range_count < _MIN_RAW_SHARD_COUNT:
        raise UHCRetainedAdmissionError("UHC admission requires at least four ranges")
    if range_count > _MAX_RAW_SHARD_COUNT:
        raise UHCRetainedAdmissionError(
            f"UHC admission allows at most {_MAX_RAW_SHARD_COUNT} ranges"
        )


def _validate_contract_version(contract_version: int) -> None:
    """Require one positive database-safe immutable layout version."""

    if (
        type(contract_version) is not int
        or contract_version <= 0
        or contract_version > 2**31 - 1
    ):
        raise UHCRetainedAdmissionError("UHC shard contract version is invalid")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value_by_key: dict[str, Any] = {}
    for key, field_value in pairs:
        if key in value_by_key:
            raise UHCRetainedAdmissionError(f"duplicate JSON object key: {key!r}")
        value_by_key[key] = field_value
    return value_by_key


def _verified_artifact_chunks(
    path: Path,
    *,
    expected_sha256: str,
    expected_bytes: int,
) -> Iterator[bytes]:
    """Yield stable retained bytes and verify the complete stream at EOF."""

    _validate_artifact_identity(expected_sha256, expected_bytes)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise UHCRetainedAdmissionError("retained artifact is unavailable") from error
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_mode & 0o022
        ):
            raise UHCRetainedAdmissionError(
                "retained artifact permissions or link count are unsafe"
            )
        if before.st_size != expected_bytes:
            raise UHCRetainedAdmissionError(
                f"retained artifact byte mismatch: {before.st_size} != {expected_bytes}"
            )
        digest = hashlib.sha256()
        byte_count = 0
        while chunk := os.read(descriptor, _RAW_READ_CHUNK_BYTES):
            digest.update(chunk)
            byte_count += len(chunk)
            yield chunk
        after = os.fstat(descriptor)
        has_stable_identity = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
            before.st_mode,
            before.st_nlink,
        ) == (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
            after.st_mode,
            after.st_nlink,
        )
        if not has_stable_identity:
            raise UHCRetainedAdmissionError("retained artifact changed while hashing")
        if byte_count != expected_bytes or digest.hexdigest() != expected_sha256:
            raise UHCRetainedAdmissionError("retained artifact proof does not match")
    finally:
        os.close(descriptor)


def _verify_artifact(path: Path, expected_sha256: str, expected_bytes: int) -> None:
    collections.deque(
        _verified_artifact_chunks(
            path,
            expected_sha256=expected_sha256,
            expected_bytes=expected_bytes,
        ),
        maxlen=0,
    )
