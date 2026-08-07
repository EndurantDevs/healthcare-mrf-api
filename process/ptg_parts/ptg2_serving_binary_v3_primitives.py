# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Primitive integer and selective-membership codecs for PTG2 v3."""

from collections.abc import Callable, Iterable, Sequence

from process.ptg_parts.ptg2_serving_binary_v3_types import (
    MembershipPayloadHeader as _MembershipPayloadHeader,
)


PTG2_V3_FORMAT_VERSION = 1
PTG2_V3_INDEXED_FORMAT_VERSION = 2
PTG2_V3_CHECKPOINT_INTERVAL = 32
PTG2_V3_ATOM_KEY_24_BITS = 24
PTG2_V3_ATOM_KEY_32_BITS = 32
PTG2_V3_MAX_24_BIT_KEY_COUNT = 1 << PTG2_V3_ATOM_KEY_24_BITS
PTG2_V3_MAX_32_BIT_KEY_COUNT = 1 << PTG2_V3_ATOM_KEY_32_BITS


class _PriceMembershipAtomLimitError(ValueError):
    """A selected membership exceeds its predecode atom budget."""


def append_uvarint(buffer: bytearray, value: int) -> None:
    """Append one non-negative uint64 using canonical unsigned LEB128."""

    normalized_value = int(value)
    if normalized_value < 0 or normalized_value > 0xFFFFFFFFFFFFFFFF:
        raise ValueError("uvarint must fit in uint64")
    while normalized_value >= 0x80:
        buffer.append((normalized_value & 0x7F) | 0x80)
        normalized_value >>= 7
    buffer.append(normalized_value)


def read_uvarint(encoded_payload: bytes | bytearray | memoryview, offset: int) -> tuple[int, int]:
    """Read one canonical uint64 LEB128 value and return its trailing offset."""

    cursor = int(offset)
    if cursor < 0 or cursor > len(encoded_payload):
        raise ValueError("uvarint offset is outside its payload")
    decoded_value = 0
    for byte_index in range(10):
        if cursor >= len(encoded_payload):
            raise ValueError("uvarint is truncated")
        current_byte = int(encoded_payload[cursor])
        cursor += 1
        if byte_index == 9 and current_byte > 1:
            raise ValueError("uvarint is too large")
        decoded_value |= (current_byte & 0x7F) << (byte_index * 7)
        if not current_byte & 0x80:
            if byte_index and current_byte == 0:
                raise ValueError("uvarint is not canonical")
            return decoded_value, cursor
    raise ValueError("uvarint is too large")


def _checkpoint_offset_bytes(record_offset: int) -> bytes:
    normalized_offset = int(record_offset)
    if normalized_offset < 0 or normalized_offset > 0xFFFFFFFF:
        raise ValueError("PTG2 v3 checkpoint offset must fit in uint32")
    return normalized_offset.to_bytes(4, "little")


def _read_checkpoint_offset(
    encoded_payload: bytes | bytearray | memoryview,
    cursor: int,
) -> tuple[int, int]:
    checkpoint_end = cursor + 4
    if checkpoint_end > len(encoded_payload):
        raise ValueError("PTG2 v3 checkpoint directory is truncated")
    return (
        int.from_bytes(encoded_payload[cursor:checkpoint_end], "little"),
        checkpoint_end,
    )


def _validate_checkpoint_shape(
    entry_count: int,
    interval: int,
    checkpoint_count: int,
) -> None:
    if interval <= 0 or interval > 4096:
        raise ValueError("PTG2 v3 checkpoint interval is invalid")
    expected_count = (entry_count + interval - 1) // interval
    if checkpoint_count != expected_count:
        raise ValueError("PTG2 v3 checkpoint count is invalid")


def _skip_optional_text(
    encoded_payload: bytes | bytearray | memoryview,
    offset: int,
) -> int:
    encoded_length, cursor = read_uvarint(encoded_payload, offset)
    if encoded_length == 0:
        return cursor
    text_end = cursor + encoded_length - 1
    if text_end > len(encoded_payload):
        raise ValueError("PTG2 v3 price-atom text is truncated")
    return text_end


def select_atom_key_bits(atom_count: int) -> int:
    """Select the snapshot-wide key width able to address every dense atom key."""

    normalized_count = int(atom_count)
    if normalized_count < 0:
        raise ValueError("atom_count cannot be negative")
    if normalized_count <= PTG2_V3_MAX_24_BIT_KEY_COUNT:
        return PTG2_V3_ATOM_KEY_24_BITS
    if normalized_count <= PTG2_V3_MAX_32_BIT_KEY_COUNT:
        return PTG2_V3_ATOM_KEY_32_BITS
    raise ValueError("PTG2 v3 supports at most 2^32 price atoms")


def encode_dense_keys(keys: Iterable[int], key_bits: int) -> bytes:
    """Encode dense atom keys as fixed-width little-endian integers."""

    key_bytes = _dense_key_bytes(key_bits)
    maximum_key = (1 << int(key_bits)) - 1
    encoded_keys = bytearray()
    for source_key in keys:
        normalized_key = int(source_key)
        if normalized_key < 0 or normalized_key > maximum_key:
            raise ValueError(f"dense key {normalized_key} does not fit in {key_bits} bits")
        encoded_keys.extend(normalized_key.to_bytes(key_bytes, "little"))
    return bytes(encoded_keys)


def decode_dense_keys(encoded_payload: bytes | bytearray | memoryview, key_bits: int) -> tuple[int, ...]:
    """Decode fixed-width little-endian dense atom keys."""

    key_bytes = _dense_key_bytes(key_bits)
    if len(encoded_payload) % key_bytes:
        raise ValueError("dense-key payload length is not aligned to its key width")
    return tuple(
        int.from_bytes(encoded_payload[offset : offset + key_bytes], "little")
        for offset in range(0, len(encoded_payload), key_bytes)
    )


def _key_bits_from_bytes(key_bytes: int) -> int:
    if key_bytes == 3:
        return PTG2_V3_ATOM_KEY_24_BITS
    if key_bytes == 4:
        return PTG2_V3_ATOM_KEY_32_BITS
    raise ValueError("PTG2 v3 dense-key payload must use three or four bytes")


def _dense_key_bytes(key_bits: int) -> int:
    if int(key_bits) == PTG2_V3_ATOM_KEY_24_BITS:
        return 3
    if int(key_bits) == PTG2_V3_ATOM_KEY_32_BITS:
        return 4
    raise ValueError("PTG2 v3 dense keys must use 24 or 32 bits")


def _price_membership_header(
    encoded_payload: bytes | bytearray | memoryview,
) -> _MembershipPayloadHeader:
    if len(encoded_payload) < 2:
        raise ValueError("unsupported PTG2 v3 price-membership payload version")
    version = int(encoded_payload[0])
    if version not in {PTG2_V3_FORMAT_VERSION, PTG2_V3_INDEXED_FORMAT_VERSION}:
        raise ValueError("unsupported PTG2 v3 price-membership payload version")
    atom_key_bits = _key_bits_from_bytes(int(encoded_payload[1]))
    entry_count, cursor = read_uvarint(encoded_payload, 2)
    if version == PTG2_V3_FORMAT_VERSION:
        return _MembershipPayloadHeader(version, atom_key_bits, entry_count, cursor, 0, ())
    checkpoint_interval, cursor = read_uvarint(encoded_payload, cursor)
    checkpoint_count, cursor = read_uvarint(encoded_payload, cursor)
    _validate_checkpoint_shape(entry_count, checkpoint_interval, checkpoint_count)
    raw_checkpoints: list[tuple[int | None, int]] = []
    for checkpoint_index in range(checkpoint_count):
        previous_key_plus_one, cursor = read_uvarint(encoded_payload, cursor)
        record_offset, cursor = _read_checkpoint_offset(encoded_payload, cursor)
        previous_key = None if previous_key_plus_one == 0 else previous_key_plus_one - 1
        if checkpoint_index and previous_key is None:
            raise ValueError("PTG2 v3 membership checkpoint key is invalid")
        raw_checkpoints.append((previous_key, record_offset))
    _validate_membership_checkpoints(raw_checkpoints, len(encoded_payload) - cursor)
    return _MembershipPayloadHeader(
        version,
        atom_key_bits,
        entry_count,
        cursor,
        checkpoint_interval,
        tuple(raw_checkpoints),
    )


def _validate_membership_checkpoints(
    checkpoints: Sequence[tuple[int | None, int]],
    records_size: int,
) -> None:
    if checkpoints and checkpoints[0] != (None, 0):
        raise ValueError("PTG2 v3 membership checkpoint directory must start at zero")
    previous_key: int | None = None
    previous_offset = -1
    for checkpoint_key, checkpoint_offset in checkpoints:
        if checkpoint_offset <= previous_offset or checkpoint_offset >= records_size:
            raise ValueError("PTG2 v3 membership checkpoint offsets are invalid")
        if previous_key is not None and (checkpoint_key is None or checkpoint_key <= previous_key):
            raise ValueError("PTG2 v3 membership checkpoint keys are invalid")
        previous_key = checkpoint_key
        previous_offset = checkpoint_offset


def _validate_memberships(
    memberships: Sequence[tuple[int, Sequence[int]]],
    atom_key_bits: int,
) -> None:
    previous_price_key: int | None = None
    maximum_atom_key = (1 << _dense_key_bytes(atom_key_bits) * 8) - 1
    for price_key, atom_keys in memberships:
        if price_key < 0:
            raise ValueError("price keys cannot be negative")
        if previous_price_key is not None and price_key <= previous_price_key:
            raise ValueError("price memberships must be strictly ordered by price key")
        if not atom_keys:
            raise ValueError("price membership cannot be empty")
        if any(atom_key < 0 or atom_key > maximum_atom_key for atom_key in atom_keys):
            raise ValueError("dense atom key does not fit in its encoded width")
        if any(left_key > right_key for left_key, right_key in zip(atom_keys, atom_keys[1:])):
            raise ValueError("price membership atom keys must be ordered")
        previous_price_key = price_key


def _validate_selected_atom_limit(maximum_selected_atom_count: int | None) -> None:
    if maximum_selected_atom_count is not None and (
        type(maximum_selected_atom_count) is not int
        or maximum_selected_atom_count < 0
    ):
        raise ValueError("maximum selected atom count must be a non-negative integer")


def decode_selected_price_memberships(
    encoded_payload: bytes | bytearray | memoryview,
    requested_keys: tuple[int, ...],
    header: _MembershipPayloadHeader,
    maximum_selected_atom_count: int | None,
    *,
    dense_key_decoder: Callable[
        [bytes | bytearray | memoryview, int], tuple[int, ...]
    ],
    full_decoder: Callable[
        [bytes | bytearray | memoryview, _MembershipPayloadHeader],
        dict[int, tuple[int, ...]],
    ],
) -> dict[int, tuple[int, ...]]:
    """Decode a validated membership selection without reparsing its header."""

    if header.version == PTG2_V3_FORMAT_VERSION:
        if maximum_selected_atom_count is not None:
            return _legacy_selected_price_memberships(
                encoded_payload,
                header,
                set(requested_keys),
                maximum_selected_atom_count,
                dense_key_decoder,
            )
        all_memberships = full_decoder(encoded_payload, header)
        return {
            price_key: all_memberships[price_key]
            for price_key in requested_keys
            if price_key in all_memberships
        }
    return _indexed_selected_price_memberships(
        encoded_payload,
        header,
        requested_keys,
        maximum_selected_atom_count,
        dense_key_decoder,
    )


def _indexed_selected_price_memberships(
    encoded_payload: bytes | bytearray | memoryview,
    header: _MembershipPayloadHeader,
    requested_keys: tuple[int, ...],
    maximum_selected_atom_count: int | None,
    dense_key_decoder: Callable[
        [bytes | bytearray | memoryview, int], tuple[int, ...]
    ],
) -> dict[int, tuple[int, ...]]:
    requested_by_checkpoint: dict[int, set[int]] = {}
    for price_key in requested_keys:
        checkpoint_index = _membership_checkpoint_index(header, price_key)
        requested_by_checkpoint.setdefault(checkpoint_index, set()).add(price_key)
    memberships_by_price_key: dict[int, tuple[int, ...]] = {}
    remaining_selected_atom_count = maximum_selected_atom_count
    for checkpoint_index, checkpoint_keys in requested_by_checkpoint.items():
        checkpoint_memberships = _price_memberships_for_checkpoint(
            encoded_payload,
            header,
            checkpoint_index,
            checkpoint_keys,
            remaining_selected_atom_count,
            dense_key_decoder,
        )
        memberships_by_price_key.update(checkpoint_memberships)
        if remaining_selected_atom_count is not None:
            remaining_selected_atom_count -= sum(
                len(atom_keys) for atom_keys in checkpoint_memberships.values()
            )
    return memberships_by_price_key


def _legacy_selected_price_memberships(
    encoded_payload: bytes | bytearray | memoryview,
    header: _MembershipPayloadHeader,
    requested_keys: set[int],
    maximum_selected_atom_count: int,
    dense_key_decoder: Callable[
        [bytes | bytearray | memoryview, int], tuple[int, ...]
    ],
) -> dict[int, tuple[int, ...]]:
    """Scan legacy v1 records while decoding only bounded selections."""

    key_bytes = _dense_key_bytes(header.atom_key_bits)
    cursor = header.records_offset
    memberships_by_price_key: dict[int, tuple[int, ...]] = {}
    previous_price_key: int | None = None
    selected_atom_count = 0
    for membership_index in range(header.entry_count):
        price_delta, cursor = read_uvarint(encoded_payload, cursor)
        if membership_index and price_delta == 0:
            raise ValueError("price memberships are not strictly ordered")
        price_key = (
            price_delta
            if previous_price_key is None
            else previous_price_key + price_delta
        )
        atom_count, cursor = read_uvarint(encoded_payload, cursor)
        if atom_count == 0:
            raise ValueError("price membership cannot be empty")
        atom_end = cursor + atom_count * key_bytes
        if atom_end > len(encoded_payload):
            raise ValueError("PTG2 v3 price-membership atom keys are truncated")
        if price_key in requested_keys:
            selected_atom_count = _bounded_selected_atom_count(
                selected_atom_count,
                atom_count,
                maximum_selected_atom_count,
            )
            atom_keys = dense_key_decoder(
                encoded_payload[cursor:atom_end],
                header.atom_key_bits,
            )
            if any(
                left_key > right_key
                for left_key, right_key in zip(atom_keys, atom_keys[1:])
            ):
                raise ValueError("price membership atom keys are not ordered")
            memberships_by_price_key[price_key] = atom_keys
        else:
            _validate_dense_key_order_without_tuple(
                encoded_payload,
                start=cursor,
                end=atom_end,
                key_bytes=key_bytes,
            )
        cursor = atom_end
        previous_price_key = price_key
    if cursor != len(encoded_payload):
        raise ValueError("PTG2 v3 price-membership payload has trailing bytes")
    return memberships_by_price_key


def _price_memberships_for_checkpoint(
    encoded_payload: bytes | bytearray | memoryview,
    header: _MembershipPayloadHeader,
    checkpoint_index: int,
    requested_keys: set[int],
    maximum_selected_atom_count: int | None,
    dense_key_decoder: Callable[
        [bytes | bytearray | memoryview, int], tuple[int, ...]
    ],
) -> dict[int, tuple[int, ...]]:
    previous_price_key, record_offset = header.checkpoints[checkpoint_index]
    cursor = header.records_offset + record_offset
    remaining_entries = header.entry_count - checkpoint_index * header.checkpoint_interval
    segment_entries = min(header.checkpoint_interval, remaining_entries)
    key_bytes = _dense_key_bytes(header.atom_key_bits)
    memberships_by_price_key: dict[int, tuple[int, ...]] = {}
    selected_atom_count = 0
    for segment_index in range(segment_entries):
        price_delta, cursor = read_uvarint(encoded_payload, cursor)
        if (checkpoint_index or segment_index) and price_delta == 0:
            raise ValueError("price memberships are not strictly ordered")
        price_key = price_delta if previous_price_key is None else previous_price_key + price_delta
        atom_count, cursor = read_uvarint(encoded_payload, cursor)
        if atom_count == 0:
            raise ValueError("price membership cannot be empty")
        atom_end = cursor + atom_count * key_bytes
        if atom_end > len(encoded_payload):
            raise ValueError("PTG2 v3 price-membership atom keys are truncated")
        if price_key in requested_keys:
            selected_atom_count = _bounded_selected_atom_count(
                selected_atom_count,
                atom_count,
                maximum_selected_atom_count,
            )
            atom_keys = dense_key_decoder(
                encoded_payload[cursor:atom_end],
                header.atom_key_bits,
            )
            if any(left_key > right_key for left_key, right_key in zip(atom_keys, atom_keys[1:])):
                raise ValueError("price membership atom keys are not ordered")
            memberships_by_price_key[price_key] = atom_keys
        cursor = atom_end
        previous_price_key = price_key
    expected_end = _checkpoint_segment_end(encoded_payload, header, checkpoint_index)
    if cursor != expected_end:
        raise ValueError("PTG2 v3 membership checkpoint offset is invalid")
    return memberships_by_price_key


def _bounded_selected_atom_count(
    selected_atom_count: int,
    atom_count: int,
    maximum_selected_atom_count: int | None,
) -> int:
    next_selected_atom_count = selected_atom_count + atom_count
    if (
        maximum_selected_atom_count is not None
        and next_selected_atom_count > maximum_selected_atom_count
    ):
        raise _PriceMembershipAtomLimitError(
            "selected price memberships exceed their atom limit"
        )
    return next_selected_atom_count


def _validate_dense_key_order_without_tuple(
    encoded_payload: bytes | bytearray | memoryview,
    *,
    start: int,
    end: int,
    key_bytes: int,
) -> None:
    """Validate one dense-key sequence without materializing its tuple."""

    payload_view = memoryview(encoded_payload)
    previous_atom_key: int | None = None
    for offset in range(start, end, key_bytes):
        atom_key = int.from_bytes(
            payload_view[offset : offset + key_bytes],
            "little",
        )
        if previous_atom_key is not None and previous_atom_key > atom_key:
            raise ValueError("price membership atom keys are not ordered")
        previous_atom_key = atom_key


def _membership_checkpoint_index(
    header: _MembershipPayloadHeader,
    price_key: int,
) -> int:
    selected_index = 0
    for checkpoint_index, (previous_key, _record_offset) in enumerate(header.checkpoints):
        if previous_key is not None and previous_key >= price_key:
            break
        selected_index = checkpoint_index
    return selected_index


def _checkpoint_segment_end(
    encoded_payload: bytes | bytearray | memoryview,
    header: _MembershipPayloadHeader,
    checkpoint_index: int,
) -> int:
    next_index = checkpoint_index + 1
    if next_index < len(header.checkpoints):
        return header.records_offset + header.checkpoints[next_index][1]
    return len(encoded_payload)
