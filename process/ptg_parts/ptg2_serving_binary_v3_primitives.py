# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Primitive integer codecs and key-width rules for PTG2 v3."""

from collections.abc import Iterable


PTG2_V3_FORMAT_VERSION = 1
PTG2_V3_INDEXED_FORMAT_VERSION = 2
PTG2_V3_CHECKPOINT_INTERVAL = 32
PTG2_V3_ATOM_KEY_24_BITS = 24
PTG2_V3_ATOM_KEY_32_BITS = 32
PTG2_V3_MAX_24_BIT_KEY_COUNT = 1 << PTG2_V3_ATOM_KEY_24_BITS
PTG2_V3_MAX_32_BIT_KEY_COUNT = 1 << PTG2_V3_ATOM_KEY_32_BITS


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
