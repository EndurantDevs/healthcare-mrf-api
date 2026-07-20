# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded requested-code intersections for provider membership containers."""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field

from process.ptg_parts.ptg2_serving_binary_v3_code_sets import (
    _CODE_CONTAINER_BITMAP,
    _CODE_CONTAINER_BITMAP_BYTES,
    _CODE_CONTAINER_LOW_MASK,
    _CODE_CONTAINER_RUNS,
    _CODE_CONTAINER_SHIFT,
    _CODE_CONTAINER_SPARSE,
    _strict_code_keys,
)
from process.ptg_parts.ptg2_serving_binary_v3_primitives import (
    PTG2_V3_FORMAT_VERSION,
    read_uvarint,
)


@dataclass(frozen=True)
class ProviderCodeSelection:
    """One normalized requested-code index reused across provider payloads."""

    requested_keys: tuple[int, ...]
    lows_by_high: Mapping[int, tuple[int, ...]]


def provider_code_selection(requested_code_keys: Iterable[int]) -> ProviderCodeSelection:
    """Normalize and partition requested codes once for repeated intersections."""

    requested_keys = _strict_code_keys(
        tuple(sorted({int(code_key) for code_key in requested_code_keys}))
    )
    mutable_lows_by_high: dict[int, list[int]] = {}
    for code_key in requested_keys:
        mutable_lows_by_high.setdefault(
            code_key >> _CODE_CONTAINER_SHIFT,
            [],
        ).append(code_key & _CODE_CONTAINER_LOW_MASK)
    return ProviderCodeSelection(
        requested_keys=requested_keys,
        lows_by_high={
            high_bits: tuple(low_keys)
            for high_bits, low_keys in mutable_lows_by_high.items()
        },
    )


def intersect_provider_code_set(
    encoded_payload: bytes | bytearray | memoryview,
    requested_code_keys: Iterable[int] | ProviderCodeSelection,
) -> tuple[int, ...]:
    """Validate one code set while retaining only requested memberships.

    The request index is reusable across provider payloads, and each encoded
    container is scanned without expanding its full semantic key sequence.
    """

    selection = (
        requested_code_keys
        if isinstance(requested_code_keys, ProviderCodeSelection)
        else provider_code_selection(requested_code_keys)
    )
    if not encoded_payload or int(encoded_payload[0]) != PTG2_V3_FORMAT_VERSION:
        raise ValueError("unsupported PTG2 v3 provider-code payload version")
    container_count, cursor = read_uvarint(encoded_payload, 1)
    if container_count == 0:
        raise ValueError("provider-code set cannot be empty")
    matched_code_keys: list[int] = []
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
        matched_low_keys = _intersect_code_container(
            container_kind,
            memoryview(encoded_payload)[cursor:body_end],
            cardinality,
            selection.lows_by_high.get(high_bits, ()),
        )
        matched_code_keys.extend(
            (high_bits << _CODE_CONTAINER_SHIFT) | low_key
            for low_key in matched_low_keys
        )
        cursor = body_end
        previous_high = high_bits
    if cursor != len(encoded_payload):
        raise ValueError("provider-code payload has trailing bytes")
    return tuple(matched_code_keys)


@dataclass
class _CodeContainerScan:
    """Validate ordered keys and derive canonical sizes without retaining them."""

    requested_keys: Sequence[int]
    cardinality: int = 0
    sparse_body_bytes: int = 0
    run_count: int = 0
    run_component_bytes: int = 0
    previous_key: int | None = None
    current_run_cardinality: int = 0
    requested_index: int = 0
    matched_keys: list[int] = field(default_factory=list)

    def add_key(self, low_key: int) -> None:
        """Consume one strictly ordered key without retaining the full sequence."""

        if low_key < 0 or low_key > _CODE_CONTAINER_LOW_MASK:
            raise ValueError("provider-code key exceeds its 16-bit container")
        if self.previous_key is not None and low_key <= self.previous_key:
            raise ValueError("provider-code keys are not strictly ordered")
        if self.previous_key is None:
            self.sparse_body_bytes += _uvarint_size(low_key)
            self._add_run(low_key, low_key, low_key)
        else:
            gap = low_key - self.previous_key
            self.sparse_body_bytes += _uvarint_size(gap)
            if gap == 1:
                self._extend_last_run()
            else:
                self._add_run(low_key, low_key, gap - 1)
        self.cardinality += 1
        self._match_range(low_key, low_key)
        self.previous_key = low_key

    def add_run(self, run_start: int, run_end: int, start_delta: int) -> None:
        """Consume one canonical run without expanding its member keys."""

        if run_start < 0 or run_end < run_start or run_end > _CODE_CONTAINER_LOW_MASK:
            raise ValueError("run provider-code key exceeds its 16-bit container")
        if self.previous_key is not None and run_start <= self.previous_key + 1:
            raise ValueError("run provider-code ranges are not canonical")
        run_cardinality = run_end - run_start + 1
        first_delta = run_start if self.previous_key is None else run_start - self.previous_key
        self.sparse_body_bytes += _uvarint_size(first_delta)
        self.sparse_body_bytes += run_cardinality - 1
        self.run_count += 1
        self.run_component_bytes += _uvarint_size(start_delta)
        self.run_component_bytes += _uvarint_size(run_end - run_start)
        self.current_run_cardinality = run_cardinality
        self.cardinality += run_cardinality
        self._match_range(run_start, run_end)
        self.previous_key = run_end

    def _add_run(self, run_start: int, run_end: int, start_delta: int) -> None:
        self.run_count += 1
        self.run_component_bytes += _uvarint_size(start_delta)
        self.run_component_bytes += _uvarint_size(run_end - run_start)
        self.current_run_cardinality = run_end - run_start + 1

    def _extend_last_run(self) -> None:
        if self.previous_key is None or self.current_run_cardinality < 1:
            raise ValueError("provider-code run state is missing")
        previous_length = self.current_run_cardinality - 1
        new_length = self.current_run_cardinality
        self.run_component_bytes += _uvarint_size(new_length) - _uvarint_size(
            previous_length
        )
        self.current_run_cardinality += 1

    def _match_range(self, range_start: int, range_end: int) -> None:
        while (
            self.requested_index < len(self.requested_keys)
            and int(self.requested_keys[self.requested_index]) < range_start
        ):
            self.requested_index += 1
        while (
            self.requested_index < len(self.requested_keys)
            and int(self.requested_keys[self.requested_index]) <= range_end
        ):
            self.matched_keys.append(int(self.requested_keys[self.requested_index]))
            self.requested_index += 1

    @property
    def runs_body_bytes(self) -> int:
        """Return the canonical run-container body size for scanned keys."""

        return _uvarint_size(self.run_count) + self.run_component_bytes


def _intersect_code_container(
    container_kind: int,
    body: memoryview,
    cardinality: int,
    requested_keys: Sequence[int],
) -> tuple[int, ...]:
    """Validate one canonical container and retain only requested low keys."""

    scan = _CodeContainerScan(requested_keys)
    if container_kind == _CODE_CONTAINER_SPARSE:
        _scan_sparse_code_container(body, cardinality, scan)
    elif container_kind == _CODE_CONTAINER_RUNS:
        _scan_run_code_container(body, scan)
    elif container_kind == _CODE_CONTAINER_BITMAP:
        _scan_bitmap_code_container(body, scan)
    else:
        raise ValueError(f"unknown provider-code container kind: {container_kind}")
    if scan.cardinality != cardinality:
        raise ValueError("provider-code container cardinality does not match its payload")
    expected_kind = min(
        (
            (scan.sparse_body_bytes, _CODE_CONTAINER_SPARSE),
            (scan.runs_body_bytes, _CODE_CONTAINER_RUNS),
            (_CODE_CONTAINER_BITMAP_BYTES, _CODE_CONTAINER_BITMAP),
        )
    )[1]
    if container_kind != expected_kind:
        raise ValueError("provider-code container is not canonical")
    return tuple(scan.matched_keys)


def _scan_sparse_code_container(
    body: memoryview,
    cardinality: int,
    scan: _CodeContainerScan,
) -> None:
    cursor = 0
    previous_low: int | None = None
    for key_index in range(cardinality):
        encoded_low, cursor = _read_canonical_uvarint(body, cursor)
        if key_index and encoded_low == 0:
            raise ValueError("sparse provider-code keys are not strictly ordered")
        low_key = encoded_low if previous_low is None else previous_low + encoded_low
        scan.add_key(low_key)
        previous_low = low_key
    if cursor != len(body):
        raise ValueError("sparse provider-code container has trailing bytes")


def _scan_run_code_container(body: memoryview, scan: _CodeContainerScan) -> None:
    run_count, cursor = _read_canonical_uvarint(body, 0)
    if run_count == 0:
        raise ValueError("run provider-code container cannot be empty")
    previous_end = -1
    for run_index in range(run_count):
        start_delta, cursor = _read_canonical_uvarint(body, cursor)
        if run_index and start_delta == 0:
            raise ValueError("run provider-code ranges are not canonical")
        run_length, cursor = _read_canonical_uvarint(body, cursor)
        run_start = previous_end + 1 + start_delta
        run_end = run_start + run_length
        scan.add_run(run_start, run_end, start_delta)
        previous_end = run_end
    if cursor != len(body):
        raise ValueError("run provider-code container has trailing bytes")


def _scan_bitmap_code_container(body: memoryview, scan: _CodeContainerScan) -> None:
    if len(body) != _CODE_CONTAINER_BITMAP_BYTES:
        raise ValueError("bitmap provider-code container has an invalid byte count")
    for byte_index, current_byte in enumerate(body):
        remaining_bits = int(current_byte)
        while remaining_bits:
            least_bit = remaining_bits & -remaining_bits
            scan.add_key(byte_index * 8 + least_bit.bit_length() - 1)
            remaining_bits ^= least_bit


def _read_canonical_uvarint(
    payload: bytes | bytearray | memoryview,
    cursor: int,
) -> tuple[int, int]:
    value, next_cursor = read_uvarint(payload, cursor)
    if next_cursor - cursor != _uvarint_size(value):
        raise ValueError("provider-code container uvarint is not canonical")
    return value, next_cursor


def _uvarint_size(value: int) -> int:
    if value < 0:
        raise ValueError("provider-code uvarint cannot be negative")
    return max(1, (int(value).bit_length() + 6) // 7)
