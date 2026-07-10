# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider-to-code container codec for PTG2 v3."""

from collections.abc import Iterable, Sequence

from process.ptg_parts.ptg2_serving_binary_v3_primitives import (
    PTG2_V3_FORMAT_VERSION,
    append_uvarint,
    read_uvarint,
)
from process.ptg_parts.ptg2_serving_binary_v3_types import PTG2V3CodeSetStats

_CODE_CONTAINER_SHIFT = 16
_CODE_CONTAINER_LOW_MASK = (1 << _CODE_CONTAINER_SHIFT) - 1
_CODE_CONTAINER_BITMAP_BYTES = 1 << (_CODE_CONTAINER_SHIFT - 3)
_CODE_CONTAINER_SPARSE = 1
_CODE_CONTAINER_RUNS = 2
_CODE_CONTAINER_BITMAP = 3


def encode_provider_code_set(code_keys: Iterable[int]) -> tuple[bytes, PTG2V3CodeSetStats]:
    """Encode strictly ordered code keys with the smallest container per high word."""

    normalized_keys = _strict_code_keys(code_keys)
    containers = _code_containers(normalized_keys)
    encoded_code_set = bytearray((PTG2_V3_FORMAT_VERSION,))
    append_uvarint(encoded_code_set, len(containers))
    previous_high = 0
    container_count_by_kind = {
        _CODE_CONTAINER_SPARSE: 0,
        _CODE_CONTAINER_RUNS: 0,
        _CODE_CONTAINER_BITMAP: 0,
    }
    for container_index, (high_bits, low_keys) in enumerate(containers):
        encoded_kind, encoded_body = _smallest_code_container(low_keys)
        high_delta = high_bits if container_index == 0 else high_bits - previous_high
        append_uvarint(encoded_code_set, high_delta)
        encoded_code_set.append(encoded_kind)
        append_uvarint(encoded_code_set, len(low_keys))
        append_uvarint(encoded_code_set, len(encoded_body))
        encoded_code_set.extend(encoded_body)
        previous_high = high_bits
        container_count_by_kind[encoded_kind] += 1
    return bytes(encoded_code_set), PTG2V3CodeSetStats(
        code_count=len(normalized_keys),
        container_count=len(containers),
        sparse_container_count=container_count_by_kind[_CODE_CONTAINER_SPARSE],
        run_container_count=container_count_by_kind[_CODE_CONTAINER_RUNS],
        bitmap_container_count=container_count_by_kind[_CODE_CONTAINER_BITMAP],
        encoded_bytes=len(encoded_code_set),
    )


def decode_provider_code_set(encoded_payload: bytes | bytearray | memoryview) -> tuple[int, ...]:
    """Decode one strict self-describing provider-to-code container payload."""

    if not encoded_payload or int(encoded_payload[0]) != PTG2_V3_FORMAT_VERSION:
        raise ValueError("unsupported PTG2 v3 provider-code payload version")
    container_count, cursor = read_uvarint(encoded_payload, 1)
    code_keys: list[int] = []
    previous_high: int | None = None
    for container_index in range(container_count):
        high_delta, cursor = read_uvarint(encoded_payload, cursor)
        if container_index and high_delta == 0:
            raise ValueError("provider-code containers are not strictly ordered")
        high_bits = high_delta if previous_high is None else previous_high + high_delta
        if high_bits > 0x7FFF:
            raise ValueError("provider-code container exceeds signed PostgreSQL integer keys")
        if cursor >= len(encoded_payload):
            raise ValueError("provider-code container kind is truncated")
        container_kind = int(encoded_payload[cursor])
        cursor += 1
        cardinality, cursor = read_uvarint(encoded_payload, cursor)
        if cardinality == 0:
            raise ValueError("provider-code container cannot be empty")
        body_bytes, cursor = read_uvarint(encoded_payload, cursor)
        body_end = cursor + body_bytes
        if body_end > len(encoded_payload):
            raise ValueError("provider-code container body is truncated")
        low_keys = _decode_code_container(
            container_kind,
            encoded_payload[cursor:body_end],
            cardinality,
        )
        code_keys.extend((high_bits << _CODE_CONTAINER_SHIFT) | low_key for low_key in low_keys)
        cursor = body_end
        previous_high = high_bits
    if cursor != len(encoded_payload):
        raise ValueError("provider-code payload has trailing bytes")
    if any(left_key >= right_key for left_key, right_key in zip(code_keys, code_keys[1:])):
        raise ValueError("provider-code payload is not strictly ordered")
    return tuple(code_keys)


def _strict_code_keys(code_keys: Iterable[int]) -> tuple[int, ...]:
    normalized_keys = tuple(int(code_key) for code_key in code_keys)
    if normalized_keys and normalized_keys[0] < 0:
        raise ValueError("code keys cannot be negative")
    if normalized_keys and normalized_keys[-1] > 0x7FFFFFFF:
        raise ValueError("code keys must fit in signed PostgreSQL integer keys")
    if any(left_key >= right_key for left_key, right_key in zip(normalized_keys, normalized_keys[1:])):
        raise ValueError("code keys must be strictly ordered")
    return normalized_keys


def _code_containers(code_keys: Sequence[int]) -> tuple[tuple[int, tuple[int, ...]], ...]:
    low_keys_by_high: dict[int, list[int]] = {}
    for code_key in code_keys:
        low_keys_by_high.setdefault(code_key >> _CODE_CONTAINER_SHIFT, []).append(
            code_key & _CODE_CONTAINER_LOW_MASK
        )
    return tuple(
        (high_bits, tuple(low_keys))
        for high_bits, low_keys in sorted(low_keys_by_high.items())
    )


def _smallest_code_container(low_keys: Sequence[int]) -> tuple[int, bytes]:
    candidates = (
        (_CODE_CONTAINER_SPARSE, _encode_sparse_code_container(low_keys)),
        (_CODE_CONTAINER_RUNS, _encode_run_code_container(low_keys)),
        (_CODE_CONTAINER_BITMAP, _encode_bitmap_code_container(low_keys)),
    )
    return min(candidates, key=lambda candidate: (len(candidate[1]), candidate[0]))


def _encode_sparse_code_container(low_keys: Sequence[int]) -> bytes:
    encoded_keys = bytearray()
    previous_low = 0
    for key_index, low_key in enumerate(low_keys):
        append_uvarint(encoded_keys, low_key if key_index == 0 else low_key - previous_low)
        previous_low = low_key
    return bytes(encoded_keys)


def _encode_run_code_container(low_keys: Sequence[int]) -> bytes:
    runs = _code_runs(low_keys)
    encoded_runs = bytearray()
    append_uvarint(encoded_runs, len(runs))
    previous_end = -1
    for run_start, run_end in runs:
        append_uvarint(encoded_runs, run_start - previous_end - 1)
        append_uvarint(encoded_runs, run_end - run_start)
        previous_end = run_end
    return bytes(encoded_runs)


def _code_runs(low_keys: Sequence[int]) -> tuple[tuple[int, int], ...]:
    if not low_keys:
        return ()
    runs: list[tuple[int, int]] = []
    run_start = int(low_keys[0])
    run_end = run_start
    for low_key in low_keys[1:]:
        if low_key == run_end + 1:
            run_end = int(low_key)
            continue
        runs.append((run_start, run_end))
        run_start = int(low_key)
        run_end = run_start
    runs.append((run_start, run_end))
    return tuple(runs)


def _encode_bitmap_code_container(low_keys: Sequence[int]) -> bytes:
    encoded_bitmap = bytearray(_CODE_CONTAINER_BITMAP_BYTES)
    for low_key in low_keys:
        encoded_bitmap[low_key >> 3] |= 1 << (low_key & 7)
    return bytes(encoded_bitmap)


def _decode_code_container(container_kind: int, body: memoryview, cardinality: int) -> tuple[int, ...]:
    if container_kind == _CODE_CONTAINER_SPARSE:
        low_keys = _decode_sparse_code_container(body, cardinality)
    elif container_kind == _CODE_CONTAINER_RUNS:
        low_keys = _decode_run_code_container(body)
    elif container_kind == _CODE_CONTAINER_BITMAP:
        low_keys = _decode_bitmap_code_container(body)
    else:
        raise ValueError(f"unknown provider-code container kind: {container_kind}")
    if len(low_keys) != cardinality:
        raise ValueError("provider-code container cardinality does not match its payload")
    expected_kind, expected_body = _smallest_code_container(low_keys)
    if (container_kind, bytes(body)) != (expected_kind, expected_body):
        raise ValueError("provider-code container is not canonical")
    return low_keys


def _decode_sparse_code_container(body: memoryview, cardinality: int) -> tuple[int, ...]:
    low_keys: list[int] = []
    cursor = 0
    previous_low: int | None = None
    for key_index in range(cardinality):
        encoded_low, cursor = read_uvarint(body, cursor)
        if key_index and encoded_low == 0:
            raise ValueError("sparse provider-code keys are not strictly ordered")
        low_key = encoded_low if previous_low is None else previous_low + encoded_low
        if low_key > _CODE_CONTAINER_LOW_MASK:
            raise ValueError("sparse provider-code key exceeds its 16-bit container")
        low_keys.append(low_key)
        previous_low = low_key
    if cursor != len(body):
        raise ValueError("sparse provider-code container has trailing bytes")
    return tuple(low_keys)


def _decode_run_code_container(body: memoryview) -> tuple[int, ...]:
    run_count, cursor = read_uvarint(body, 0)
    if run_count == 0:
        raise ValueError("run provider-code container cannot be empty")
    low_keys: list[int] = []
    previous_end = -1
    for run_index in range(run_count):
        start_delta, cursor = read_uvarint(body, cursor)
        if run_index and start_delta == 0:
            raise ValueError("run provider-code ranges are not canonical")
        run_length, cursor = read_uvarint(body, cursor)
        run_start = previous_end + 1 + start_delta
        run_end = run_start + run_length
        if run_end > _CODE_CONTAINER_LOW_MASK:
            raise ValueError("run provider-code key exceeds its 16-bit container")
        low_keys.extend(range(run_start, run_end + 1))
        previous_end = run_end
    if cursor != len(body):
        raise ValueError("run provider-code container has trailing bytes")
    return tuple(low_keys)


def _decode_bitmap_code_container(body: memoryview) -> tuple[int, ...]:
    if len(body) != _CODE_CONTAINER_BITMAP_BYTES:
        raise ValueError("bitmap provider-code container has an invalid byte count")
    return tuple(
        byte_index * 8 + bit_index
        for byte_index, current_byte in enumerate(body)
        if current_byte
        for bit_index in range(8)
        if int(current_byte) & (1 << bit_index)
    )
