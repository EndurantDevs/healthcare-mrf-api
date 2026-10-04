# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Lossless flat recipes for shared, bounded source-token fragments."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from typing import Any

from process.ptg_parts.ptg2_source_witness_contract import (
    PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES,
    WitnessPayloadLimitError,
)
from process.ptg_parts.ptg2_source_witness_primitives import nonnegative_int, sha256_hex

SOURCE_WITNESS_FRAGMENT_BYTES = 4096
_RECIPE_FIELDS = frozenset({"raw_sha256", "raw_byte_count", "fragment_sha256"})


def _bounded_length(raw_byte_count: int) -> None:
    if raw_byte_count == 0:
        raise RuntimeError("source witness token byte count must be positive")
    if raw_byte_count > PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES:
        raise WitnessPayloadLimitError("source witness token exceeds its decoded byte bound")


def _canonical_digest(raw_digest: Any, *, field_name: str) -> str:
    if not isinstance(raw_digest, str):
        raise RuntimeError(f"source witness fragment recipe has invalid {field_name}")
    normalized_digest = sha256_hex(raw_digest, field_name=field_name)
    if raw_digest != normalized_digest:
        raise RuntimeError(f"source witness fragment recipe has invalid {field_name}")
    return normalized_digest


def _validated_recipe(recipe: Mapping[str, Any]) -> tuple[str, int, tuple[str, ...]]:
    if not isinstance(recipe, Mapping) or set(recipe) != _RECIPE_FIELDS:
        raise RuntimeError("source witness fragment recipe fields are invalid")
    raw_byte_count = nonnegative_int(recipe, "raw_byte_count", error_field_name="raw byte count")
    _bounded_length(raw_byte_count)
    raw_sha256 = _canonical_digest(recipe["raw_sha256"], field_name="raw digest")
    fragment_sha256 = recipe["fragment_sha256"]
    expected_fragment_count = (raw_byte_count + SOURCE_WITNESS_FRAGMENT_BYTES - 1) // SOURCE_WITNESS_FRAGMENT_BYTES
    if not isinstance(fragment_sha256, list) or len(fragment_sha256) != expected_fragment_count:
        raise RuntimeError("source witness fragment reference count is invalid")
    fragment_digests = tuple(
        _canonical_digest(raw_digest, field_name="fragment digest") for raw_digest in fragment_sha256
    )
    return raw_sha256, raw_byte_count, fragment_digests


def encode_fragment_recipe(
    raw_token: bytes,
    fragment_sink: Callable[[str, bytes], None],
) -> dict[str, Any]:
    """Stage exact chunks; the sink owns deduplication and aggregate budgets.

    Sink failures propagate without returning a recipe. No token is parsed or
    normalized, and repeated chunks are offered to the sink independently.
    """

    if not isinstance(raw_token, bytes):
        raise RuntimeError("source witness token must be bytes")
    raw_byte_count = len(raw_token)
    _bounded_length(raw_byte_count)
    fragment_digests = []
    for offset in range(0, raw_byte_count, SOURCE_WITNESS_FRAGMENT_BYTES):
        raw_fragment = raw_token[offset : offset + SOURCE_WITNESS_FRAGMENT_BYTES]
        fragment_digest = hashlib.sha256(raw_fragment).hexdigest()
        fragment_sink(fragment_digest, raw_fragment)
        fragment_digests.append(fragment_digest)
    return {
        "raw_sha256": hashlib.sha256(raw_token).hexdigest(),
        "raw_byte_count": raw_byte_count,
        "fragment_sha256": fragment_digests,
    }


def decode_fragment_recipe(
    recipe: Mapping[str, Any],
    fragment_lookup: Callable[[str], bytes],
) -> bytes:
    """Authenticate and reconstruct one token through bounded chunk lookups.

    All metadata is validated before lookup. Lookup failures and cancellation
    propagate; bytes are returned only after the complete token is verified.
    """

    raw_sha256, raw_byte_count, fragment_digests = _validated_recipe(recipe)
    decoded_token = bytearray()
    for index, fragment_digest in enumerate(fragment_digests):
        raw_fragment = fragment_lookup(fragment_digest)
        expected_length = min(SOURCE_WITNESS_FRAGMENT_BYTES, raw_byte_count - index * SOURCE_WITNESS_FRAGMENT_BYTES)
        if not isinstance(raw_fragment, bytes) or len(raw_fragment) != expected_length:
            raise RuntimeError("source witness fragment length is invalid")
        if hashlib.sha256(raw_fragment).hexdigest() != fragment_digest:
            raise RuntimeError("source witness fragment digest is invalid")
        decoded_token.extend(raw_fragment)
    if len(decoded_token) != raw_byte_count or hashlib.sha256(decoded_token).hexdigest() != raw_sha256:
        raise RuntimeError("source witness reconstructed token digest is invalid")
    return bytes(decoded_token)


__all__ = ["SOURCE_WITNESS_FRAGMENT_BYTES", "decode_fragment_recipe", "encode_fragment_recipe"]
