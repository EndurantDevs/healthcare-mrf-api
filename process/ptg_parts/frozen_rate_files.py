# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable bounded multipart rate-file contract for internal PTG dispatch."""

from __future__ import annotations

import hashlib
import hmac
import os
import re
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

from process.ptg_parts.artifacts import _is_strong_etag
from process.ptg_parts.canonical import canonical_json_dumps, canonicalize_url

FROZEN_RATE_FILE_SET_CONTRACT = "ptg_frozen_rate_file_set_v1"
FROZEN_RATE_FILE_PROOF_CONTRACT = "ptg_frozen_rate_file_proof_v1"
FROZEN_RATE_FILE_PROOF_SET_CONTRACT = (
    "ptg_frozen_rate_file_proof_set_v1"
)
FROZEN_RATE_FILE_SET_MIN_FILES = 2
FROZEN_RATE_FILE_SET_MAX_FILES = 128
FROZEN_RATE_FILE_SET_MAX_CANONICAL_BYTES = 256 * 1024
FROZEN_RATE_FILE_MAX_URL_BYTES = 4096
FROZEN_RATE_FILE_MAX_VALIDATOR_BYTES = 1024
FROZEN_RATE_FILE_TOTAL_MAX_BYTES_ENV = "HLTHPRT_PTG2_FROZEN_TOTAL_MAX_BYTES"
FROZEN_RATE_FILE_TOTAL_MAX_BYTES_DEFAULT = 512 * 1024 * 1024 * 1024
FROZEN_RATE_FILE_VERIFICATION_MODES = frozenset(
    {
        "downloaded",
        "length_last_modified",
        "strong_etag_length",
        "verified_local_sha256",
    }
)

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_ENGINE_ID_PATTERN = re.compile(
    r"(?:[0-9a-f]{16}|[0-9a-f]{32}|[0-9a-f]{64})"
)
_FROZEN_SOURCE_TYPE = "in_network"
_DESCRIPTOR_KEYS = frozenset(
    {
        "source_type",
        "canonical_url",
        "content_length",
        "etag",
        "last_modified",
        "raw_sha256",
        "logical_sha256",
        "logical_hash_deferred",
        "engine_source_identity_hash",
        "engine_source_file_version_id",
        "ordinal",
    }
)


class FrozenRateFileValidationError(ValueError):
    """Raised when an internal multipart envelope is not canonical and bounded."""


class FrozenRateFileMismatchError(RuntimeError):
    """Raised when acquired or processed evidence differs from the frozen set."""


def normalize_frozen_verification_mode(value: Any) -> str:
    """Return one explicit byte-verification mode accepted by frozen proof."""

    if (
        not isinstance(value, str)
        or value not in FROZEN_RATE_FILE_VERIFICATION_MODES
    ):
        raise FrozenRateFileMismatchError(
            "frozen rate file verification_mode is invalid"
        )
    return value


def frozen_observed_logical_sha256(
    descriptor: Mapping[str, Any],
) -> str:
    """Return the persisted logical identity for one frozen declaration."""

    if descriptor.get("logical_hash_deferred") is True:
        return str(descriptor.get("raw_sha256") or "")
    return str(descriptor.get("logical_sha256") or "")


def _canonical_https_url(raw_url: Any) -> str:
    if not isinstance(raw_url, str) or not raw_url:
        raise FrozenRateFileValidationError(
            "frozen rate file URL must be canonical query-free HTTPS"
        )
    if len(raw_url.encode("utf-8")) > FROZEN_RATE_FILE_MAX_URL_BYTES:
        raise FrozenRateFileValidationError(
            "frozen rate file URL must be canonical query-free HTTPS"
        )
    parsed = urlsplit(raw_url)
    try:
        parsed_port = parsed.port
    except ValueError as exc:
        raise FrozenRateFileValidationError(
            "frozen rate file URL must be canonical query-free HTTPS"
        ) from exc
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed_port not in {None, 443}
        or parsed.query
        or parsed.fragment
        or parsed.path in {"", "/"}
        or canonicalize_url(raw_url) != raw_url
    ):
        raise FrozenRateFileValidationError(
            "frozen rate file URL must be canonical query-free HTTPS"
        )
    return raw_url


def _positive_int(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FrozenRateFileValidationError(
            f"frozen rate file {field_name} must be a positive integer"
        )
    return value


def _lower_hex(value: Any, *, field_name: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise FrozenRateFileValidationError(
            f"frozen rate file {field_name} is invalid"
        )
    return value


def _optional_validator(value: Any, *, field_name: str) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value.encode("utf-8")) > FROZEN_RATE_FILE_MAX_VALIDATOR_BYTES
        or any(ord(character) < 32 for character in value)
    ):
        raise FrozenRateFileValidationError(
            f"frozen rate file {field_name} is invalid"
        )
    return value


def _normalize_descriptor(raw_descriptor: Any) -> dict[str, Any]:
    """Normalize one exact v1 descriptor after checking every evidence field."""

    if not isinstance(raw_descriptor, Mapping):
        raise FrozenRateFileValidationError(
            "each frozen rate file must be an object"
        )
    descriptor_keys = set(raw_descriptor)
    unexpected_keys = descriptor_keys - _DESCRIPTOR_KEYS
    missing_keys = _DESCRIPTOR_KEYS - descriptor_keys
    if unexpected_keys or missing_keys:
        raise FrozenRateFileValidationError(
            "frozen rate file descriptor fields do not match the v1 contract"
        )
    source_type = raw_descriptor.get("source_type")
    if source_type != _FROZEN_SOURCE_TYPE:
        raise FrozenRateFileValidationError(
            "frozen multipart supports only in_network source_type"
        )
    etag, last_modified = _normalized_http_validators(raw_descriptor)
    logical_sha256, logical_hash_deferred = _normalized_logical_hash(
        raw_descriptor
    )
    return {
        "source_type": source_type,
        "canonical_url": _canonical_https_url(
            raw_descriptor.get("canonical_url")
        ),
        "content_length": _positive_int(
            raw_descriptor.get("content_length"),
            field_name="content_length",
        ),
        "etag": etag,
        "last_modified": last_modified,
        "raw_sha256": _lower_hex(
            raw_descriptor.get("raw_sha256"),
            field_name="raw_sha256",
            pattern=_SHA256_PATTERN,
        ),
        "logical_sha256": logical_sha256,
        "logical_hash_deferred": logical_hash_deferred,
        "engine_source_identity_hash": _lower_hex(
            raw_descriptor.get("engine_source_identity_hash"),
            field_name="engine_source_identity_hash",
            pattern=_ENGINE_ID_PATTERN,
        ),
        "engine_source_file_version_id": _lower_hex(
            raw_descriptor.get("engine_source_file_version_id"),
            field_name="engine_source_file_version_id",
            pattern=_ENGINE_ID_PATTERN,
        ),
        "ordinal": _positive_int(
            raw_descriptor.get("ordinal"),
            field_name="ordinal",
        ),
    }


def _normalized_http_validators(
    raw_descriptor: Mapping[str, Any],
) -> tuple[str | None, str | None]:
    etag = _optional_validator(raw_descriptor.get("etag"), field_name="etag")
    if etag is not None and not _is_strong_etag(etag):
        raise FrozenRateFileValidationError(
            "frozen rate file ETag must be a strong quoted validator"
        )
    last_modified = _optional_validator(
        raw_descriptor.get("last_modified"),
        field_name="last_modified",
    )
    if etag is None and last_modified is None:
        raise FrozenRateFileValidationError(
            "frozen rate file requires a strong ETag or Last-Modified validator"
        )
    return etag, last_modified


def _normalized_logical_hash(
    raw_descriptor: Mapping[str, Any],
) -> tuple[str | None, bool]:
    logical_hash_deferred = raw_descriptor.get("logical_hash_deferred")
    if type(logical_hash_deferred) is not bool:
        raise FrozenRateFileValidationError(
            "frozen rate file logical_hash_deferred must be boolean"
        )
    logical_sha256 = raw_descriptor.get("logical_sha256")
    if logical_hash_deferred:
        if logical_sha256 is not None:
            raise FrozenRateFileValidationError(
                "deferred frozen logical hashes must not include logical_sha256"
            )
    else:
        logical_sha256 = _lower_hex(
            logical_sha256,
            field_name="logical_sha256",
            pattern=_SHA256_PATTERN,
        )
    return logical_sha256, logical_hash_deferred


def _normalized_descriptors(
    frozen_rate_files: Any,
) -> list[dict[str, Any]]:
    """Return one ordinal-sorted set after enforcing set-wide invariants."""

    if not isinstance(frozen_rate_files, list):
        raise FrozenRateFileValidationError(
            "frozen_rate_files must be an array"
        )
    file_count = len(frozen_rate_files)
    if not (
        FROZEN_RATE_FILE_SET_MIN_FILES
        <= file_count
        <= FROZEN_RATE_FILE_SET_MAX_FILES
    ):
        raise FrozenRateFileValidationError(
            "frozen_rate_files cardinality must be between 2 and 128"
        )
    normalized = sorted(
        (
            _normalize_descriptor(raw_descriptor)
            for raw_descriptor in frozen_rate_files
        ),
        key=lambda descriptor: descriptor["ordinal"],
    )
    _assert_frozen_set_relationships(normalized, file_count)
    _assert_frozen_set_byte_budget(normalized)
    return normalized


def _frozen_total_max_bytes() -> int:
    raw_limit = os.getenv(FROZEN_RATE_FILE_TOTAL_MAX_BYTES_ENV)
    if raw_limit is None or not raw_limit.strip():
        return FROZEN_RATE_FILE_TOTAL_MAX_BYTES_DEFAULT
    try:
        limit = int(raw_limit)
    except ValueError as exc:
        raise FrozenRateFileValidationError(
            f"{FROZEN_RATE_FILE_TOTAL_MAX_BYTES_ENV} must be a positive integer"
        ) from exc
    if limit <= 0:
        raise FrozenRateFileValidationError(
            f"{FROZEN_RATE_FILE_TOTAL_MAX_BYTES_ENV} must be a positive integer"
        )
    return limit


def _assert_frozen_set_byte_budget(
    normalized_files: Sequence[Mapping[str, Any]],
) -> None:
    aggregate_bytes = sum(
        int(descriptor["content_length"])
        for descriptor in normalized_files
    )
    if aggregate_bytes > _frozen_total_max_bytes():
        raise FrozenRateFileValidationError(
            "frozen rate file aggregate content length exceeds the configured "
            "byte budget"
        )


def _assert_frozen_set_relationships(
    normalized_files: Sequence[Mapping[str, Any]],
    file_count: int,
) -> None:
    if [descriptor["ordinal"] for descriptor in normalized_files] != list(
        range(1, file_count + 1)
    ):
        raise FrozenRateFileValidationError(
            "frozen rate file ordinals must be exactly 1..N"
        )
    origins = {
        (
            urlsplit(descriptor["canonical_url"]).hostname,
            urlsplit(descriptor["canonical_url"]).port or 443,
        )
        for descriptor in normalized_files
    }
    source_types = {
        descriptor["source_type"] for descriptor in normalized_files
    }
    if len(origins) != 1 or len(source_types) != 1:
        raise FrozenRateFileValidationError(
            "frozen rate files must share the same source type and HTTPS origin"
        )
    unique_fields = (
        "canonical_url",
        "raw_sha256",
        "engine_source_identity_hash",
        "engine_source_file_version_id",
    )
    for field_name in unique_fields:
        field_entries = [
            descriptor[field_name] for descriptor in normalized_files
        ]
        if len(set(field_entries)) != file_count:
            raise FrozenRateFileValidationError(
                f"frozen rate file {field_name} values must be unique"
            )
    logical_hashes = [
        descriptor["logical_sha256"]
        for descriptor in normalized_files
        if descriptor["logical_sha256"] is not None
    ]
    if len(set(logical_hashes)) != len(logical_hashes):
        raise FrozenRateFileValidationError(
            "non-deferred frozen logical SHA-256 values must be unique"
        )


def _canonical_set_bytes(
    normalized_files: Sequence[Mapping[str, Any]],
) -> bytes:
    return canonical_json_dumps(
        {
            "contract": FROZEN_RATE_FILE_SET_CONTRACT,
            "files": [dict(item) for item in normalized_files],
        }
    ).encode("utf-8")


def frozen_rate_file_set_sha256(frozen_rate_files: Any) -> str:
    """Return the versioned canonical digest for one valid frozen file set."""

    normalized = _normalized_descriptors(frozen_rate_files)
    canonical_bytes = _canonical_set_bytes(normalized)
    if len(canonical_bytes) > FROZEN_RATE_FILE_SET_MAX_CANONICAL_BYTES:
        raise FrozenRateFileValidationError(
            "frozen rate file set exceeds the canonical request-size cap"
        )
    return hashlib.sha256(canonical_bytes).hexdigest()


def canonical_frozen_rate_file_set_json(
    frozen_rate_files: Any,
) -> str:
    """Return the exact v1 digest preimage for cross-service parity checks."""

    normalized = _normalized_descriptors(frozen_rate_files)
    canonical_bytes = _canonical_set_bytes(normalized)
    if len(canonical_bytes) > FROZEN_RATE_FILE_SET_MAX_CANONICAL_BYTES:
        raise FrozenRateFileValidationError(
            "frozen rate file set exceeds the canonical request-size cap"
        )
    return canonical_bytes.decode("utf-8")


def normalize_frozen_rate_file_set(
    frozen_rate_files: Any,
    frozen_rate_file_set_sha256_value: Any,
) -> tuple[list[dict[str, Any]], str]:
    """Validate, order, bound, and authenticate one frozen multipart envelope."""

    normalized = _normalized_descriptors(frozen_rate_files)
    canonical_bytes = _canonical_set_bytes(normalized)
    if len(canonical_bytes) > FROZEN_RATE_FILE_SET_MAX_CANONICAL_BYTES:
        raise FrozenRateFileValidationError(
            "frozen rate file set exceeds the canonical request-size cap"
        )
    if (
        not isinstance(frozen_rate_file_set_sha256_value, str)
        or _SHA256_PATTERN.fullmatch(
            frozen_rate_file_set_sha256_value
        )
        is None
    ):
        raise FrozenRateFileValidationError(
            "frozen rate file set SHA-256 is invalid"
        )
    actual_digest = hashlib.sha256(canonical_bytes).hexdigest()
    if not hmac.compare_digest(
        frozen_rate_file_set_sha256_value,
        actual_digest,
    ):
        raise FrozenRateFileValidationError(
            "frozen rate file set SHA-256 does not match its canonical envelope"
        )
    return normalized, actual_digest


def frozen_rate_file_proof_sha256(proof_rows: Any) -> str:
    """Hash a complete ordinal-ordered proof set for candidate revalidation."""

    if not isinstance(proof_rows, list) or not proof_rows:
        raise FrozenRateFileValidationError(
            "frozen rate file proof must be a non-empty array"
        )
    normalized_rows: list[dict[str, Any]] = []
    for proof_row in proof_rows:
        if (
            not isinstance(proof_row, Mapping)
            or proof_row.get("contract")
            != FROZEN_RATE_FILE_PROOF_CONTRACT
        ):
            raise FrozenRateFileValidationError(
                "frozen rate file proof contract is invalid"
            )
        normalized_rows.append(dict(proof_row))
    normalized_rows.sort(key=lambda row: row.get("ordinal", 0))
    return hashlib.sha256(
        canonical_json_dumps(
            {
                "contract": FROZEN_RATE_FILE_PROOF_SET_CONTRACT,
                "proof": normalized_rows,
            }
        ).encode("utf-8")
    ).hexdigest()


from process.ptg_parts.frozen_rate_runtime import (
    assert_frozen_input_compatibility,
    bind_frozen_rate_set_to_scope,
    build_frozen_rate_jobs,
    validate_frozen_artifacts,
    validate_frozen_head,
    validate_frozen_processed_results,
)


__all__ = [
    "FROZEN_RATE_FILE_PROOF_CONTRACT",
    "FROZEN_RATE_FILE_PROOF_SET_CONTRACT",
    "FROZEN_RATE_FILE_SET_CONTRACT",
    "FROZEN_RATE_FILE_SET_MAX_CANONICAL_BYTES",
    "FROZEN_RATE_FILE_SET_MAX_FILES",
    "FROZEN_RATE_FILE_SET_MIN_FILES",
    "FROZEN_RATE_FILE_TOTAL_MAX_BYTES_DEFAULT",
    "FROZEN_RATE_FILE_TOTAL_MAX_BYTES_ENV",
    "FROZEN_RATE_FILE_VERIFICATION_MODES",
    "FrozenRateFileMismatchError",
    "FrozenRateFileValidationError",
    "frozen_observed_logical_sha256",
    "assert_frozen_input_compatibility",
    "bind_frozen_rate_set_to_scope",
    "build_frozen_rate_jobs",
    "canonical_frozen_rate_file_set_json",
    "frozen_rate_file_set_sha256",
    "frozen_rate_file_proof_sha256",
    "normalize_frozen_rate_file_set",
    "normalize_frozen_verification_mode",
    "validate_frozen_artifacts",
    "validate_frozen_head",
    "validate_frozen_processed_results",
]
