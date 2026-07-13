# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Decoder primitives for strict shared-block PTG V3 PostgreSQL reads."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from api.ptg2_db_sidecars import _decode_serving_binary_payload
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_serving_binary_v3 import (
    PTG2_V3_ATOM_KEY_24_BITS,
    PTG2_V3_ATOM_KEY_32_BITS,
    decode_price_memberships_for_keys,
    decode_provider_code_set,
    price_membership_entry_count,
    read_uvarint,
)


PTG2_SERVING_BINARY_V3_PROVIDER_SET_CODES_KIND = "provider_set_codes_v3"
PTG2_SERVING_BINARY_V3_PRICE_MEMBERSHIPS_KIND = "price_set_atom_memberships_v3"
PTG2_SERVING_BINARY_V3_ATOM_PAYLOAD_KIND = "price_atoms_v3"

PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN = 1024
PTG2_SERVING_BINARY_V3_PRICE_KEY_BLOCK_SPAN = 512
PTG2_SERVING_BINARY_V3_ATOM_KEY_BLOCK_SPAN = 512


def _requested_keys(source_keys: Iterable[int]) -> tuple[int, ...]:
    requested_key_values = tuple(sorted({int(source_key) for source_key in source_keys}))
    if requested_key_values and requested_key_values[0] < 0:
        raise PTG2ManifestArtifactError("PTG2 v3 serving keys cannot be negative")
    return requested_key_values


def _block_keys_for(requested_key_values: Iterable[int], block_span: int) -> tuple[int, ...]:
    return tuple(sorted({int(requested_key) // block_span for requested_key in requested_key_values}))


def _expected_atom_key_bits(atom_key_bits: int | None) -> int | None:
    if atom_key_bits is None:
        return None
    normalized_bits = int(atom_key_bits)
    if normalized_bits not in {PTG2_V3_ATOM_KEY_24_BITS, PTG2_V3_ATOM_KEY_32_BITS}:
        raise PTG2ManifestArtifactError("PTG2 v3 manifest atom_key_bits must be 24 or 32")
    return normalized_bits


def _effective_block_span(block_span: int | None, default_span: int) -> int:
    normalized_span = int(block_span) if block_span is not None else int(default_span)
    if normalized_span <= 0 or normalized_span > 1 << 24:
        raise PTG2ManifestArtifactError("PTG2 v3 manifest block span is invalid")
    return normalized_span


def _logical_block_bytes(
    fragment_rows_by_number: Mapping[int, Mapping[str, Any]],
    *,
    artifact_kind: str,
    block_key: int,
) -> tuple[bytes, int]:
    """Validate and concatenate all physical fragments of one logical block."""

    block_numbers = tuple(sorted(fragment_rows_by_number))
    if block_numbers != tuple(range(len(block_numbers))):
        raise PTG2ManifestArtifactError(
            f"PTG2 v3 {artifact_kind} block {block_key} has non-contiguous block fragments"
        )
    first_fragment = fragment_rows_by_number[0]
    entry_count = int(first_fragment.get("entry_count") or 0)
    if entry_count < 0 or any(
        int(fragment_rows_by_number[block_number].get("entry_count") or 0) != 0
        for block_number in block_numbers[1:]
    ):
        raise PTG2ManifestArtifactError(
            f"PTG2 v3 {artifact_kind} block {block_key} has invalid fragment entry counts"
        )
    try:
        block_bytes = b"".join(
            _decode_serving_binary_payload(fragment_rows_by_number[block_number])
            for block_number in block_numbers
        )
    except Exception as exc:
        raise PTG2ManifestArtifactError(
            f"PTG2 v3 {artifact_kind} block {block_key} payload is corrupt"
        ) from exc
    if not block_bytes:
        raise PTG2ManifestArtifactError(f"PTG2 v3 {artifact_kind} block {block_key} is empty")
    return block_bytes, entry_count


def _is_key_in_block(key: int, block_key: int, block_span: int) -> bool:
    block_start = block_key * block_span
    return block_start <= key < block_start + block_span


def _decode_provider_code_block(
    block_bytes: bytes,
    *,
    block_key: int,
    entry_count: int,
    requested_provider_set_keys: set[int],
) -> dict[int, tuple[int, ...]]:
    """Decode requested provider sets without expanding neighboring containers."""

    try:
        provider_count, cursor = read_uvarint(block_bytes, 0)
        block_start = block_key * PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN
        code_keys_by_provider_set: dict[int, tuple[int, ...]] = {}
        previous_provider_set_key: int | None = None
        for _provider_index in range(provider_count):
            provider_offset, cursor = read_uvarint(block_bytes, cursor)
            provider_set_key = block_start + provider_offset
            encoded_size, cursor = read_uvarint(block_bytes, cursor)
            code_bytes_end = cursor + encoded_size
            if code_bytes_end > len(block_bytes) or not _is_key_in_block(
                provider_set_key,
                block_key,
                PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN,
            ):
                raise ValueError("provider-set entry is outside its block")
            if previous_provider_set_key is not None and provider_set_key <= previous_provider_set_key:
                raise ValueError("provider-set entries are not strictly ordered")
            if provider_set_key in requested_provider_set_keys:
                code_keys_by_provider_set[provider_set_key] = decode_provider_code_set(
                    block_bytes[cursor:code_bytes_end]
                )
            previous_provider_set_key = provider_set_key
            cursor = code_bytes_end
        if cursor != len(block_bytes) or provider_count != entry_count:
            raise ValueError("provider-set block length or count is invalid")
        return code_keys_by_provider_set
    except Exception as exc:
        raise PTG2ManifestArtifactError(f"PTG2 v3 provider-set code block {block_key} is corrupt") from exc


def _membership_atom_key_bits(block_bytes: bytes) -> int:
    if len(block_bytes) < 2:
        raise ValueError("price-membership payload is truncated")
    key_bytes = int(block_bytes[1])
    if key_bytes == 3:
        return PTG2_V3_ATOM_KEY_24_BITS
    if key_bytes == 4:
        return PTG2_V3_ATOM_KEY_32_BITS
    raise ValueError("price-membership payload uses an invalid atom-key width")


def _decode_price_membership_block(
    block_bytes: bytes,
    *,
    block_key: int,
    entry_count: int,
    atom_key_bits: int | None,
    block_span: int,
    requested_price_keys: set[int],
) -> dict[int, tuple[int, ...]]:
    try:
        payload_key_bits = _membership_atom_key_bits(block_bytes)
        if atom_key_bits is not None and payload_key_bits != atom_key_bits:
            raise ValueError("membership payload atom-key width disagrees with manifest")
        if price_membership_entry_count(block_bytes) != entry_count:
            raise ValueError("price-membership entry count does not match payload")
        memberships_by_price_key = decode_price_memberships_for_keys(
            block_bytes,
            requested_price_keys,
        )
        if any(
            not _is_key_in_block(
                price_key,
                block_key,
                block_span,
            )
            for price_key in memberships_by_price_key
        ):
            raise ValueError("price membership is outside its block")
        return memberships_by_price_key
    except Exception as exc:
        raise PTG2ManifestArtifactError(f"PTG2 v3 price-membership block {block_key} is corrupt") from exc
