# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL readers for the PTG2 v3 serving-binary blocks.

V3 keeps the v2 forward serving blocks and moves only reverse code membership
and dense price atoms to explicit PostgreSQL artifact kinds.  A physical row
may hold one fragment of a logical block, so readers always concatenate an
ordered block before decoding it.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from api.ptg2_db_sidecars import (
    _decode_serving_binary_payload,
    _safe_qualified_table_name,
    _serving_binary_payload_rows_for_keys,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_serving_binary_v3 import (
    PTG2_V3_ATOM_KEY_24_BITS,
    PTG2_V3_ATOM_KEY_32_BITS,
    PTG2V3PriceAtomRecord,
    decode_price_atoms_for_offsets,
    decode_price_memberships_for_keys,
    decode_provider_code_set,
    price_atom_entry_count,
    price_membership_entry_count,
    read_uvarint,
)


PTG2_SERVING_BINARY_V3_PROVIDER_SET_CODES_KIND = "provider_set_codes_v3"
PTG2_SERVING_BINARY_V3_PRICE_MEMBERSHIPS_KIND = "price_set_atom_memberships_v3"
PTG2_SERVING_BINARY_V3_ATOM_PAYLOAD_KIND = "price_atoms_v3"

PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN = 1024
PTG2_SERVING_BINARY_V3_PRICE_KEY_BLOCK_SPAN = 512
PTG2_SERVING_BINARY_V3_ATOM_KEY_BLOCK_SPAN = 512
# Kept as a compatibility alias for early v3 reader callers.  Provider-code
# payloads are not block-addressed; provider-set keys are.
PTG2_SERVING_BINARY_V3_CODE_KEY_BLOCK_SPAN = PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN


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


async def _logical_blocks_by_key(
    session: Any,
    table_name: str,
    *,
    artifact_kind: str,
    block_keys: Iterable[int],
) -> dict[int, tuple[bytes, int]]:
    """Fetch requested blocks and return one decoded payload per logical block."""

    requested_block_keys = tuple(sorted({int(block_key) for block_key in block_keys}))
    if not requested_block_keys:
        return {}
    binary_block_rows = await _serving_binary_payload_rows_for_keys(
        session,
        table_name,
        artifact_kind=artifact_kind,
        block_keys=requested_block_keys,
    )
    fragment_rows_by_block_key: dict[int, dict[int, Mapping[str, Any]]] = {
        block_key: {} for block_key in requested_block_keys
    }
    for binary_block_row in binary_block_rows:
        block_key = int(binary_block_row.get("block_key") or 0)
        block_number = int(binary_block_row.get("block_no") or 0)
        if block_key not in fragment_rows_by_block_key or block_number < 0:
            raise PTG2ManifestArtifactError("PTG2 v3 query returned an invalid block fragment")
        if block_number in fragment_rows_by_block_key[block_key]:
            raise PTG2ManifestArtifactError(f"PTG2 v3 {artifact_kind} has duplicate block fragments")
        fragment_rows_by_block_key[block_key][block_number] = binary_block_row

    logical_blocks_by_key: dict[int, tuple[bytes, int]] = {}
    for block_key, fragment_rows_by_number in fragment_rows_by_block_key.items():
        if not fragment_rows_by_number:
            continue
        logical_blocks_by_key[block_key] = _logical_block_bytes(
            fragment_rows_by_number,
            artifact_kind=artifact_kind,
            block_key=block_key,
        )
    return logical_blocks_by_key


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


def _store_requested_value(
    values_by_key: dict[int, Any],
    *,
    key: int,
    value: Any,
    requested_key_set: set[int],
    artifact_kind: str,
) -> None:
    if key not in requested_key_set:
        return
    if key in values_by_key:
        raise PTG2ManifestArtifactError(f"PTG2 v3 {artifact_kind} contains a duplicate key: {key}")
    values_by_key[key] = value


def _decode_provider_code_block(
    block_bytes: bytes,
    *,
    block_key: int,
    entry_count: int,
) -> dict[int, tuple[int, ...]]:
    """Decode provider-set code containers embedded in one provider-key block."""

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


async def lookup_provider_code_keys_from_db(
    session: Any,
    table_name: str,
    provider_set_keys: Iterable[int],
) -> dict[int, tuple[int, ...]]:
    """Return requested provider-set keys mapped to their immutable code keys."""

    _safe_qualified_table_name(table_name)
    requested_key_values = _requested_keys(provider_set_keys)
    logical_blocks = await _logical_blocks_by_key(
        session,
        table_name,
        artifact_kind=PTG2_SERVING_BINARY_V3_PROVIDER_SET_CODES_KIND,
        block_keys=_block_keys_for(
            requested_key_values,
            PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN,
        ),
    )
    requested_key_set = set(requested_key_values)
    code_keys_by_provider_set: dict[int, tuple[int, ...]] = {}
    for block_key, (block_bytes, entry_count) in logical_blocks.items():
        for provider_set_key, code_keys in _decode_provider_code_block(
            block_bytes,
            block_key=block_key,
            entry_count=entry_count,
        ).items():
            _store_requested_value(
                code_keys_by_provider_set,
                key=provider_set_key,
                value=code_keys,
                requested_key_set=requested_key_set,
                artifact_kind=PTG2_SERVING_BINARY_V3_PROVIDER_SET_CODES_KIND,
            )
    return code_keys_by_provider_set


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


async def lookup_price_atom_memberships_from_db(
    session: Any,
    table_name: str,
    price_keys: Iterable[int],
    *,
    atom_key_bits: int | None = None,
    block_span: int | None = None,
) -> dict[int, tuple[int, ...]]:
    """Return requested price keys mapped to dense price-atom memberships."""

    _safe_qualified_table_name(table_name)
    expected_atom_key_bits = _expected_atom_key_bits(atom_key_bits)
    effective_block_span = _effective_block_span(
        block_span,
        PTG2_SERVING_BINARY_V3_PRICE_KEY_BLOCK_SPAN,
    )
    requested_key_values = _requested_keys(price_keys)
    logical_blocks = await _logical_blocks_by_key(
        session,
        table_name,
        artifact_kind=PTG2_SERVING_BINARY_V3_PRICE_MEMBERSHIPS_KIND,
        block_keys=_block_keys_for(requested_key_values, effective_block_span),
    )
    requested_key_set = set(requested_key_values)
    atom_keys_by_price_key: dict[int, tuple[int, ...]] = {}
    for block_key, (block_bytes, entry_count) in logical_blocks.items():
        for price_key, atom_keys in _decode_price_membership_block(
            block_bytes,
            block_key=block_key,
            entry_count=entry_count,
            atom_key_bits=expected_atom_key_bits,
            block_span=effective_block_span,
            requested_price_keys=requested_key_set,
        ).items():
            _store_requested_value(
                atom_keys_by_price_key,
                key=price_key,
                value=atom_keys,
                requested_key_set=requested_key_set,
                artifact_kind=PTG2_SERVING_BINARY_V3_PRICE_MEMBERSHIPS_KIND,
            )
    return atom_keys_by_price_key


async def lookup_price_atoms_from_db(
    session: Any,
    table_name: str,
    atom_keys: Iterable[int],
    *,
    atom_key_bits: int | None = None,
    block_span: int | None = None,
) -> dict[int, PTG2V3PriceAtomRecord]:
    """Return requested dense atom keys mapped to compact price-atom records."""

    _safe_qualified_table_name(table_name)
    expected_atom_key_bits = _expected_atom_key_bits(atom_key_bits)
    effective_block_span = _effective_block_span(
        block_span,
        PTG2_SERVING_BINARY_V3_ATOM_KEY_BLOCK_SPAN,
    )
    requested_key_values = _requested_keys(atom_keys)
    if expected_atom_key_bits is not None and requested_key_values and requested_key_values[-1] >= 1 << expected_atom_key_bits:
        raise PTG2ManifestArtifactError("PTG2 v3 atom key exceeds the manifest atom-key width")
    logical_blocks = await _logical_blocks_by_key(
        session,
        table_name,
        artifact_kind=PTG2_SERVING_BINARY_V3_ATOM_PAYLOAD_KIND,
        block_keys=_block_keys_for(requested_key_values, effective_block_span),
    )
    requested_key_set = set(requested_key_values)
    price_atoms_by_key: dict[int, PTG2V3PriceAtomRecord] = {}
    for block_key, (block_bytes, entry_count) in logical_blocks.items():
        try:
            encoded_atom_count = price_atom_entry_count(block_bytes)
        except Exception as exc:
            raise PTG2ManifestArtifactError(f"PTG2 v3 price-atom block {block_key} is corrupt") from exc
        if encoded_atom_count != entry_count or encoded_atom_count > effective_block_span:
            raise PTG2ManifestArtifactError("PTG2 v3 price-atom count does not match its block row")
        first_atom_key = block_key * effective_block_span
        requested_offsets = {
            atom_key - first_atom_key
            for atom_key in requested_key_set
            if _is_key_in_block(atom_key, block_key, effective_block_span)
        }
        try:
            price_atoms_by_offset = decode_price_atoms_for_offsets(block_bytes, requested_offsets)
        except Exception as exc:
            raise PTG2ManifestArtifactError(f"PTG2 v3 price-atom block {block_key} is corrupt") from exc
        for atom_offset, price_atom in price_atoms_by_offset.items():
            atom_key = first_atom_key + atom_offset
            _store_requested_value(
                price_atoms_by_key,
                key=atom_key,
                value=price_atom,
                requested_key_set=requested_key_set,
                artifact_kind=PTG2_SERVING_BINARY_V3_ATOM_PAYLOAD_KIND,
            )
    return price_atoms_by_key
