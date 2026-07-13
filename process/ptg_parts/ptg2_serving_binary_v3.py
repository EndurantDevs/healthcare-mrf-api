# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict compact codecs for the canonical PostgreSQL PTG2 v3 artifacts."""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from process.ptg_parts.ptg2_serving_binary_v3_code_sets import (
    decode_provider_code_set,
    encode_provider_code_set,
)
from process.ptg_parts.ptg2_serving_binary_v3_primitives import (
    PTG2_V3_ATOM_KEY_24_BITS,
    PTG2_V3_ATOM_KEY_32_BITS,
    PTG2_V3_CHECKPOINT_INTERVAL,
    PTG2_V3_FORMAT_VERSION,
    PTG2_V3_INDEXED_FORMAT_VERSION,
    PTG2_V3_MAX_24_BIT_KEY_COUNT,
    PTG2_V3_MAX_32_BIT_KEY_COUNT,
    _dense_key_bytes,
    _key_bits_from_bytes,
    append_uvarint,
    decode_dense_keys,
    encode_dense_keys,
    read_uvarint,
    select_atom_key_bits,
)
from process.ptg_parts.ptg2_serving_binary_v3_types import (
    AtomPayloadHeader as _AtomPayloadHeader,
    MembershipPayloadHeader as _MembershipPayloadHeader,
    PTG2V3CodeSetStats,
    PTG2V3PriceAtomRecord,
)


def _checkpoint_offset_bytes(record_offset: int) -> bytes:
    normalized_offset = int(record_offset)
    if normalized_offset < 0 or normalized_offset > 0xFFFFFFFF:
        raise ValueError("PTG2 v3 checkpoint offset must fit in uint32")
    return normalized_offset.to_bytes(4, "little")


def _read_checkpoint_offset(payload: bytes | bytearray | memoryview, cursor: int) -> tuple[int, int]:
    checkpoint_end = cursor + 4
    if checkpoint_end > len(payload):
        raise ValueError("PTG2 v3 checkpoint directory is truncated")
    return int.from_bytes(payload[cursor:checkpoint_end], "little"), checkpoint_end


def encode_price_memberships(
    memberships: Iterable[tuple[int, Sequence[int]]],
    atom_key_bits: int,
) -> bytes:
    """Encode strictly ordered shared-price-key to dense-atom-key memberships."""

    normalized_memberships = tuple(
        (int(price_key), tuple(int(atom_key) for atom_key in atom_keys))
        for price_key, atom_keys in memberships
    )
    _validate_memberships(normalized_memberships, atom_key_bits)
    encoded_records = bytearray()
    checkpoints: list[tuple[int, int]] = []
    previous_price_key = 0
    for membership_index, (price_key, atom_keys) in enumerate(normalized_memberships):
        if membership_index % PTG2_V3_CHECKPOINT_INTERVAL == 0:
            previous_key_plus_one = previous_price_key + 1 if membership_index else 0
            checkpoints.append((previous_key_plus_one, len(encoded_records)))
        price_delta = price_key if membership_index == 0 else price_key - previous_price_key
        append_uvarint(encoded_records, price_delta)
        append_uvarint(encoded_records, len(atom_keys))
        encoded_records.extend(encode_dense_keys(atom_keys, atom_key_bits))
        previous_price_key = price_key
    encoded_memberships = bytearray((PTG2_V3_INDEXED_FORMAT_VERSION, _dense_key_bytes(atom_key_bits)))
    append_uvarint(encoded_memberships, len(normalized_memberships))
    append_uvarint(encoded_memberships, PTG2_V3_CHECKPOINT_INTERVAL)
    append_uvarint(encoded_memberships, len(checkpoints))
    for previous_key_plus_one, record_offset in checkpoints:
        append_uvarint(encoded_memberships, previous_key_plus_one)
        encoded_memberships.extend(_checkpoint_offset_bytes(record_offset))
    encoded_memberships.extend(encoded_records)
    return bytes(encoded_memberships)


def decode_price_memberships(encoded_payload: bytes | bytearray | memoryview) -> dict[int, tuple[int, ...]]:
    """Decode and validate shared-price-key to dense-atom-key memberships."""

    header = _price_membership_header(encoded_payload)
    key_bytes = _dense_key_bytes(header.atom_key_bits)
    cursor = header.records_offset
    memberships_by_price_key: dict[int, tuple[int, ...]] = {}
    previous_price_key: int | None = None
    for membership_index in range(header.entry_count):
        price_delta, cursor = read_uvarint(encoded_payload, cursor)
        if membership_index and price_delta == 0:
            raise ValueError("price memberships are not strictly ordered")
        price_key = price_delta if previous_price_key is None else previous_price_key + price_delta
        atom_count, cursor = read_uvarint(encoded_payload, cursor)
        if atom_count == 0:
            raise ValueError("price membership cannot be empty")
        atom_end = cursor + atom_count * key_bytes
        if atom_end > len(encoded_payload):
            raise ValueError("PTG2 v3 price-membership atom keys are truncated")
        atom_keys = decode_dense_keys(encoded_payload[cursor:atom_end], header.atom_key_bits)
        if any(left_key > right_key for left_key, right_key in zip(atom_keys, atom_keys[1:])):
            raise ValueError("price membership atom keys are not ordered")
        memberships_by_price_key[price_key] = atom_keys
        cursor = atom_end
        previous_price_key = price_key
    if cursor != len(encoded_payload):
        raise ValueError("PTG2 v3 price-membership payload has trailing bytes")
    return memberships_by_price_key


def price_membership_entry_count(encoded_payload: bytes | bytearray | memoryview) -> int:
    """Return the validated logical membership count without decoding records."""

    return _price_membership_header(encoded_payload).entry_count


def decode_price_memberships_for_keys(
    encoded_payload: bytes | bytearray | memoryview,
    requested_price_keys: Iterable[int],
) -> dict[int, tuple[int, ...]]:
    """Decode only requested memberships using v2 checkpoint directories."""

    requested_keys = tuple(sorted({int(price_key) for price_key in requested_price_keys}))
    if not requested_keys:
        return {}
    header = _price_membership_header(encoded_payload)
    if header.version == PTG2_V3_FORMAT_VERSION:
        all_memberships = decode_price_memberships(encoded_payload)
        return {price_key: all_memberships[price_key] for price_key in requested_keys if price_key in all_memberships}
    requested_by_checkpoint: dict[int, set[int]] = {}
    for price_key in requested_keys:
        checkpoint_index = _membership_checkpoint_index(header, price_key)
        requested_by_checkpoint.setdefault(checkpoint_index, set()).add(price_key)
    memberships_by_price_key: dict[int, tuple[int, ...]] = {}
    for checkpoint_index, checkpoint_keys in requested_by_checkpoint.items():
        memberships_by_price_key.update(
            _price_memberships_for_checkpoint(encoded_payload, header, checkpoint_index, checkpoint_keys)
        )
    return memberships_by_price_key


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


def _validate_checkpoint_shape(entry_count: int, interval: int, checkpoint_count: int) -> None:
    if interval <= 0 or interval > 4096:
        raise ValueError("PTG2 v3 checkpoint interval is invalid")
    expected_count = (entry_count + interval - 1) // interval
    if checkpoint_count != expected_count:
        raise ValueError("PTG2 v3 checkpoint count is invalid")


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


def _membership_checkpoint_index(header: _MembershipPayloadHeader, price_key: int) -> int:
    selected_index = 0
    for checkpoint_index, (previous_key, _record_offset) in enumerate(header.checkpoints):
        if previous_key is not None and previous_key >= price_key:
            break
        selected_index = checkpoint_index
    return selected_index


def _price_memberships_for_checkpoint(
    encoded_payload: bytes | bytearray | memoryview,
    header: _MembershipPayloadHeader,
    checkpoint_index: int,
    requested_keys: set[int],
) -> dict[int, tuple[int, ...]]:
    previous_price_key, record_offset = header.checkpoints[checkpoint_index]
    cursor = header.records_offset + record_offset
    remaining_entries = header.entry_count - checkpoint_index * header.checkpoint_interval
    segment_entries = min(header.checkpoint_interval, remaining_entries)
    key_bytes = _dense_key_bytes(header.atom_key_bits)
    memberships_by_price_key: dict[int, tuple[int, ...]] = {}
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
            atom_keys = decode_dense_keys(encoded_payload[cursor:atom_end], header.atom_key_bits)
            if any(left_key > right_key for left_key, right_key in zip(atom_keys, atom_keys[1:])):
                raise ValueError("price membership atom keys are not ordered")
            memberships_by_price_key[price_key] = atom_keys
        cursor = atom_end
        previous_price_key = price_key
    expected_end = _checkpoint_segment_end(encoded_payload, header, checkpoint_index)
    if cursor != expected_end:
        raise ValueError("PTG2 v3 membership checkpoint offset is invalid")
    return memberships_by_price_key


def _checkpoint_segment_end(
    encoded_payload: bytes | bytearray | memoryview,
    header: _MembershipPayloadHeader,
    checkpoint_index: int,
) -> int:
    next_index = checkpoint_index + 1
    if next_index < len(header.checkpoints):
        return header.records_offset + header.checkpoints[next_index][1]
    return len(encoded_payload)


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


def encode_price_atoms(price_atoms: Iterable[PTG2V3PriceAtomRecord]) -> bytes:
    """Encode negotiated rates and lean dictionary keys by dense atom order."""

    normalized_atoms = tuple(price_atoms)
    attribute_count = len(normalized_atoms[0].attribute_keys) if normalized_atoms else 0
    if any(len(price_atom.attribute_keys) != attribute_count for price_atom in normalized_atoms):
        raise ValueError("all PTG2 v3 price atoms must have the same attribute-key count")
    encoded_records = bytearray()
    checkpoint_offsets: list[int] = []
    for atom_index, price_atom in enumerate(normalized_atoms):
        if atom_index % PTG2_V3_CHECKPOINT_INTERVAL == 0:
            checkpoint_offsets.append(len(encoded_records))
        _append_optional_text(encoded_records, price_atom.negotiated_rate)
        for attribute_key in price_atom.attribute_keys:
            if attribute_key is not None and int(attribute_key) < 0:
                raise ValueError("price-atom attribute keys cannot be negative")
            append_uvarint(encoded_records, 0 if attribute_key is None else int(attribute_key) + 1)
    encoded_atoms = bytearray((PTG2_V3_INDEXED_FORMAT_VERSION,))
    append_uvarint(encoded_atoms, attribute_count)
    append_uvarint(encoded_atoms, len(normalized_atoms))
    append_uvarint(encoded_atoms, PTG2_V3_CHECKPOINT_INTERVAL)
    append_uvarint(encoded_atoms, len(checkpoint_offsets))
    for checkpoint_offset in checkpoint_offsets:
        encoded_atoms.extend(_checkpoint_offset_bytes(checkpoint_offset))
    encoded_atoms.extend(encoded_records)
    return bytes(encoded_atoms)


def decode_price_atoms(payload: bytes | bytearray | memoryview) -> tuple[PTG2V3PriceAtomRecord, ...]:
    """Decode dense-order negotiated rates and lean dictionary keys."""

    header = _price_atom_header(payload)
    cursor = header.records_offset
    price_atoms: list[PTG2V3PriceAtomRecord] = []
    for _atom_index in range(header.entry_count):
        negotiated_rate, cursor = _read_optional_text(payload, cursor)
        attribute_keys: list[int | None] = []
        for _attribute_index in range(header.attribute_count):
            encoded_key, cursor = read_uvarint(payload, cursor)
            attribute_keys.append(None if encoded_key == 0 else encoded_key - 1)
        price_atoms.append(PTG2V3PriceAtomRecord(negotiated_rate, tuple(attribute_keys)))
    if cursor != len(payload):
        raise ValueError("PTG2 v3 price-atom payload has trailing bytes")
    return tuple(price_atoms)


def price_atom_entry_count(payload: bytes | bytearray | memoryview) -> int:
    """Return the validated logical atom count without decoding records."""

    return _price_atom_header(payload).entry_count


def decode_price_atoms_for_offsets(
    payload: bytes | bytearray | memoryview,
    requested_atom_offsets: Iterable[int],
) -> dict[int, PTG2V3PriceAtomRecord]:
    """Decode only requested dense offsets using v2 checkpoint directories."""

    requested_offsets = tuple(sorted({int(atom_offset) for atom_offset in requested_atom_offsets}))
    if not requested_offsets:
        return {}
    header = _price_atom_header(payload)
    valid_offsets = tuple(
        atom_offset for atom_offset in requested_offsets if 0 <= atom_offset < header.entry_count
    )
    if header.version == PTG2_V3_FORMAT_VERSION:
        all_atoms = decode_price_atoms(payload)
        return {atom_offset: all_atoms[atom_offset] for atom_offset in valid_offsets}
    offsets_by_checkpoint: dict[int, set[int]] = {}
    for atom_offset in valid_offsets:
        checkpoint_index = atom_offset // header.checkpoint_interval
        offsets_by_checkpoint.setdefault(checkpoint_index, set()).add(atom_offset)
    atoms_by_offset: dict[int, PTG2V3PriceAtomRecord] = {}
    for checkpoint_index, checkpoint_offsets in offsets_by_checkpoint.items():
        atoms_by_offset.update(
            _price_atoms_for_checkpoint(payload, header, checkpoint_index, checkpoint_offsets)
        )
    return atoms_by_offset


def _price_atom_header(payload: bytes | bytearray | memoryview) -> _AtomPayloadHeader:
    if not payload:
        raise ValueError("unsupported PTG2 v3 price-atom payload version")
    version = int(payload[0])
    if version not in {PTG2_V3_FORMAT_VERSION, PTG2_V3_INDEXED_FORMAT_VERSION}:
        raise ValueError("unsupported PTG2 v3 price-atom payload version")
    attribute_count, cursor = read_uvarint(payload, 1)
    entry_count, cursor = read_uvarint(payload, cursor)
    if version == PTG2_V3_FORMAT_VERSION:
        return _AtomPayloadHeader(version, attribute_count, entry_count, cursor, 0, ())
    checkpoint_interval, cursor = read_uvarint(payload, cursor)
    checkpoint_count, cursor = read_uvarint(payload, cursor)
    _validate_checkpoint_shape(entry_count, checkpoint_interval, checkpoint_count)
    checkpoint_offsets: list[int] = []
    for _checkpoint_index in range(checkpoint_count):
        checkpoint_offset, cursor = _read_checkpoint_offset(payload, cursor)
        checkpoint_offsets.append(checkpoint_offset)
    _validate_atom_checkpoints(checkpoint_offsets, len(payload) - cursor)
    return _AtomPayloadHeader(
        version,
        attribute_count,
        entry_count,
        cursor,
        checkpoint_interval,
        tuple(checkpoint_offsets),
    )


def _validate_atom_checkpoints(checkpoint_offsets: Sequence[int], records_size: int) -> None:
    if checkpoint_offsets and checkpoint_offsets[0] != 0:
        raise ValueError("PTG2 v3 atom checkpoint directory must start at zero")
    previous_offset = -1
    for checkpoint_offset in checkpoint_offsets:
        if checkpoint_offset <= previous_offset or checkpoint_offset >= records_size:
            raise ValueError("PTG2 v3 atom checkpoint offsets are invalid")
        previous_offset = checkpoint_offset


def _price_atoms_for_checkpoint(
    encoded_payload: bytes | bytearray | memoryview,
    header: _AtomPayloadHeader,
    checkpoint_index: int,
    requested_offsets: set[int],
) -> dict[int, PTG2V3PriceAtomRecord]:
    cursor = header.records_offset + header.checkpoint_offsets[checkpoint_index]
    first_atom_offset = checkpoint_index * header.checkpoint_interval
    segment_entries = min(header.checkpoint_interval, header.entry_count - first_atom_offset)
    atoms_by_offset: dict[int, PTG2V3PriceAtomRecord] = {}
    for local_offset in range(segment_entries):
        atom_offset = first_atom_offset + local_offset
        if atom_offset in requested_offsets:
            negotiated_rate, cursor = _read_optional_text(encoded_payload, cursor)
            attribute_keys: list[int | None] = []
            for _attribute_index in range(header.attribute_count):
                encoded_key, cursor = read_uvarint(encoded_payload, cursor)
                attribute_keys.append(None if encoded_key == 0 else encoded_key - 1)
            atoms_by_offset[atom_offset] = PTG2V3PriceAtomRecord(
                negotiated_rate,
                tuple(attribute_keys),
            )
            continue
        cursor = _skip_optional_text(encoded_payload, cursor)
        for _attribute_index in range(header.attribute_count):
            _encoded_key, cursor = read_uvarint(encoded_payload, cursor)
    expected_end = _atom_checkpoint_segment_end(encoded_payload, header, checkpoint_index)
    if cursor != expected_end:
        raise ValueError("PTG2 v3 atom checkpoint offset is invalid")
    return atoms_by_offset


def _atom_checkpoint_segment_end(
    payload: bytes | bytearray | memoryview,
    header: _AtomPayloadHeader,
    checkpoint_index: int,
) -> int:
    next_index = checkpoint_index + 1
    if next_index < len(header.checkpoint_offsets):
        return header.records_offset + header.checkpoint_offsets[next_index]
    return len(payload)


def _append_optional_text(buffer: bytearray, text_value: str | None) -> None:
    if text_value is None:
        append_uvarint(buffer, 0)
        return
    if not isinstance(text_value, str):
        raise ValueError("price-atom negotiated rates must be text or None")
    try:
        encoded_text = text_value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("price-atom text is not valid UTF-8") from exc
    append_uvarint(buffer, len(encoded_text) + 1)
    buffer.extend(encoded_text)


def _read_optional_text(
    payload: bytes | bytearray | memoryview,
    offset: int,
) -> tuple[str | None, int]:
    encoded_length, cursor = read_uvarint(payload, offset)
    if encoded_length == 0:
        return None, cursor
    text_end = cursor + encoded_length - 1
    if text_end > len(payload):
        raise ValueError("PTG2 v3 price-atom text is truncated")
    try:
        return bytes(payload[cursor:text_end]).decode("utf-8"), text_end
    except UnicodeDecodeError as exc:
        raise ValueError("PTG2 v3 price-atom text is not valid UTF-8") from exc


def _skip_optional_text(payload: bytes | bytearray | memoryview, offset: int) -> int:
    encoded_length, cursor = read_uvarint(payload, offset)
    if encoded_length == 0:
        return cursor
    text_end = cursor + encoded_length - 1
    if text_end > len(payload):
        raise ValueError("PTG2 v3 price-atom text is truncated")
    return text_end
