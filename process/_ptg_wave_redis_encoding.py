# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical scalar validation and hashing for exact PTG Redis waves."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any


PTG_SMALL_WAVE_SLOT_COUNT = 12
PTG_SMALL_WAVE_SLOTS = tuple(range(PTG_SMALL_WAVE_SLOT_COUNT))
PTG_SMALL_WAVE_MAX_JOB_COUNT = 4096
PTG_SMALL_WAVE_FUNCTION = "ptg_control_start"
PTG_SMALL_WAVE_WORKER_CLASS = "process.PTGSmall"
PTG_SMALL_WAVE_QUEUE_PREFIX = "arq:PTGSmall:wave:"
PTG_SMALL_WAVE_PROTOCOL_IDENTITY = "healthporta.ptg-small.exact-wave.v1"
PTG_SMALL_WAVE_SERIALIZER_IDENTITY = "arq-0.28.process-msgpack.v1"
_PTG_SMALL_WAVE_KEY_PREFIX = "ptg:PTGSmall:wave:"
WAVE_SCHEMA_VERSION = 1
_JOB_COUNT_WIDTH = 4
_HEX_64 = re.compile(r"[0-9a-f]{64}\Z")
_PINNED_IMAGE = re.compile(r"\S+@sha256:[0-9a-f]{64}\Z")
_RUNTIME_IMAGE = re.compile(r"sha256:[0-9a-f]{64}\Z")


class PTGSmallWaveError(RuntimeError):
    """Base error for a rejected exact-wave operation."""


class PTGSmallWaveValidationError(PTGSmallWaveError):
    """The caller supplied a malformed manifest or slot identity."""


class PTGSmallWaveConflictError(PTGSmallWaveError):
    """Redis changed after WATCH, so no exact publication was attempted."""


class PTGSmallWaveAttestationError(PTGSmallWaveError):
    """Redis no longer contains the exact immutable wave definition."""


class PTGSmallWaveBarrierTimeout(PTGSmallWaveError):
    """A registered slot did not receive its matching release in time."""


class PTGSmallWaveCleanupActiveError(PTGSmallWaveError):
    """Terminal cleanup was refused because exact wave state is still active."""


def wave_queue_name(wave_id: str) -> str:
    """Return the dedicated ARQ queue for one verified wave digest."""

    require_wave_id(wave_id)
    return f"{PTG_SMALL_WAVE_QUEUE_PREFIX}{wave_id}"


def wave_ready_key(wave_id: str) -> str:
    """Return the exact ready-hash key for one verified wave digest."""

    require_wave_id(wave_id)
    return f"{_PTG_SMALL_WAVE_KEY_PREFIX}{wave_id}:ready"


def wave_release_key(wave_id: str) -> str:
    """Return the exact durable release key for one verified wave digest."""

    require_wave_id(wave_id)
    return f"{_PTG_SMALL_WAVE_KEY_PREFIX}{wave_id}:release"


def wave_release_channel(wave_id: str) -> str:
    """Return the exact release channel for one verified wave digest."""

    require_wave_id(wave_id)
    return f"{_PTG_SMALL_WAVE_KEY_PREFIX}{wave_id}:release-channel"


def require_wave_id(candidate: Any) -> str:
    """Validate and return a canonical 64-hex execution/wave digest."""

    if not isinstance(candidate, str) or _HEX_64.fullmatch(candidate) is None:
        raise PTGSmallWaveValidationError(
            "wave_id must be exactly 64 lowercase hexadecimal characters"
        )
    return candidate


def require_digest(label: str, candidate: Any) -> str:
    """Validate and return one canonical lowercase SHA-256 digest."""

    try:
        return require_wave_id(candidate)
    except PTGSmallWaveValidationError as exc:
        raise PTGSmallWaveValidationError(
            f"{label} must be a 64-character lowercase SHA-256 digest"
        ) from exc


def require_job_count(candidate: Any) -> int:
    """Validate and return a protocol-supported job count."""

    if (
        not isinstance(candidate, int)
        or isinstance(candidate, bool)
        or not 1 <= candidate <= PTG_SMALL_WAVE_MAX_JOB_COUNT
    ):
        raise PTGSmallWaveValidationError("job_count must be from 1 through 4096")
    return candidate


def encode_job_count(candidate: Any) -> str:
    """Encode a job count without changing release payload length."""

    return f"{require_job_count(candidate):0{_JOB_COUNT_WIDTH}d}"


def decode_job_count(candidate: Any) -> int:
    """Decode and validate the canonical fixed-width job count."""

    if (
        not isinstance(candidate, str)
        or len(candidate) != _JOB_COUNT_WIDTH
        or not candidate.isascii()
        or not candidate.isdigit()
    ):
        raise PTGSmallWaveAttestationError(
            "job_count must use canonical fixed-width encoding"
        )
    try:
        return require_job_count(int(candidate))
    except PTGSmallWaveValidationError as exc:
        raise PTGSmallWaveAttestationError(
            "job_count is outside the protocol range"
        ) from exc


def require_job_id(candidate: Any) -> str:
    """Validate and return one durable controller-supplied ARQ job ID."""

    if (
        not isinstance(candidate, str)
        or not candidate
        or len(candidate) > 512
        or candidate.strip() != candidate
    ):
        raise PTGSmallWaveValidationError(
            "job_id must be a non-empty trimmed string up to 512 characters"
        )
    return candidate


def require_protocol_identity(label: str, candidate: Any) -> str:
    """Validate and return one protocol or serializer identity."""

    if (
        not isinstance(candidate, str)
        or not candidate
        or len(candidate) > 128
        or candidate.strip() != candidate
    ):
        raise PTGSmallWaveValidationError(
            f"{label} must be a non-empty trimmed string up to 128 characters"
        )
    return candidate


def require_identity(label: str, candidate: Any) -> str:
    """Validate and return one concrete worker identity string."""

    if (
        not isinstance(candidate, str)
        or not candidate
        or len(candidate) > 512
        or candidate.strip() != candidate
    ):
        raise PTGSmallWaveValidationError(
            f"{label} must be a non-empty trimmed string up to 512 characters"
        )
    return candidate


def require_pinned_image_identity(candidate: Any) -> str:
    """Validate and return one registry image pinned by SHA-256 digest."""

    if not isinstance(candidate, str) or _PINNED_IMAGE.fullmatch(candidate) is None:
        raise PTGSmallWaveValidationError(
            "image_identity must be an image reference pinned by a sha256 digest"
        )
    return candidate


def require_runtime_image_identity(candidate: Any) -> str:
    """Validate a normalized containerStatus runtime image identity."""

    if not isinstance(candidate, str) or _RUNTIME_IMAGE.fullmatch(candidate) is None:
        raise PTGSmallWaveValidationError(
            "runtime_image_identity must be a canonical sha256 digest"
        )
    return candidate


def runtime_identity_digest(
    config_identity: Any,
    manifest_identity: Any,
    image_identity: Any,
    runtime_image_identity: Any,
) -> str:
    """Bind controller-verified worker, Kubernetes, and image identities."""

    identity_mapping = {
        "schema_version": WAVE_SCHEMA_VERSION,
        "config_identity": require_digest("config_identity", config_identity),
        "manifest_identity": require_digest("manifest_identity", manifest_identity),
        "image_identity": require_pinned_image_identity(image_identity),
        "runtime_image_identity": require_runtime_image_identity(
            runtime_image_identity
        ),
    }
    return sha256_hex(canonical_json_bytes(identity_mapping))


def sha256_hex(raw_bytes: bytes) -> str:
    """Return the lowercase SHA-256 digest for exact bytes."""

    return hashlib.sha256(raw_bytes).hexdigest()


def canonical_json_bytes(structure: Any) -> bytes:
    """Serialize a JSON-compatible structure into its canonical bytes."""

    return json.dumps(
        structure,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def as_optional_bytes(redis_scalar: Any) -> bytes | None:
    """Normalize one Redis scalar to bytes while preserving a missing key."""

    if redis_scalar is None:
        return None
    if isinstance(redis_scalar, bytes):
        return redis_scalar
    if isinstance(redis_scalar, str):
        return redis_scalar.encode("utf-8")
    raise PTGSmallWaveAttestationError("Redis returned a non-string value")


def as_text(redis_scalar: Any) -> str:
    """Normalize one required Redis scalar to UTF-8 text."""

    raw_bytes = as_optional_bytes(redis_scalar)
    if raw_bytes is None:
        raise PTGSmallWaveAttestationError("Redis returned a missing queue member")
    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PTGSmallWaveAttestationError(
            "Redis returned a non-UTF-8 queue member"
        ) from exc
