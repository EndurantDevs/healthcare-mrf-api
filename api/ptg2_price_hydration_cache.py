# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded process-local cache for validated PTG2 price hydration."""

from __future__ import annotations

import copy
import os
from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping


_DEFAULT_MAX_BYTES = 8 * 1024 * 1024
_MAX_CONFIGURED_BYTES = 16 * 1024 * 1024
_MAX_BYTES_ENV = "HLTHPRT_PTG2_PRICE_HYDRATION_CACHE_BYTES"


def configured_price_hydration_cache_bytes() -> int:
    """Return a bounded cache size; zero explicitly disables retention."""

    raw_value = os.getenv(_MAX_BYTES_ENV)
    if raw_value is None:
        return _DEFAULT_MAX_BYTES
    try:
        configured_bytes = int(str(raw_value).strip())
    except (TypeError, ValueError):
        return _DEFAULT_MAX_BYTES
    return min(max(configured_bytes, 0), _MAX_CONFIGURED_BYTES)


@dataclass(frozen=True)
class PriceHydrationLayout:
    """Immutable coordinates that determine one price payload."""

    shared_snapshot_key: int
    storage_generation: str
    atom_key_bits: int
    price_key_block_span: int
    atom_key_block_span: int
    constant_values_fingerprint: str


@dataclass(frozen=True)
class PriceHydrationCacheMetrics:
    """Low-cost cache counters for diagnostics and Prometheus projection."""

    hits: int
    misses: int
    admissions: int
    evictions: int
    rejected_batches: int
    retained_bytes: int
    entries: int
    maximum_bytes: int


_PriceRows = list[dict[str, Any]]
_EntryKey = tuple[PriceHydrationLayout, tuple[int, ...]]


@dataclass(frozen=True)
class _CacheEntry:
    rows: _PriceRows
    retained_bytes: int


class PriceHydrationCache:
    """LRU of private native price rows with atomic batch admission."""

    def __init__(self, maximum_bytes: int) -> None:
        self._maximum_bytes = max(int(maximum_bytes), 0)
        self._retained_bytes = 0
        self._entries: OrderedDict[_EntryKey, _CacheEntry] = OrderedDict()
        self._hits = 0
        self._misses = 0
        self._admissions = 0
        self._evictions = 0
        self._rejected_batches = 0

    def get_many(
        self,
        layout: PriceHydrationLayout,
        price_keys: tuple[int, ...],
    ) -> tuple[dict[int, _PriceRows], tuple[int, ...]]:
        """Return request-local copies and the exact ordered miss set."""

        cached_rows_by_key: dict[int, _PriceRows] = {}
        missing_keys: list[int] = []
        for price_key in price_keys:
            entry_key = _entry_key(layout, price_key)
            cached_entry = self._entries.get(entry_key)
            if cached_entry is None:
                self._misses += 1
                missing_keys.append(price_key)
                continue
            self._hits += 1
            self._entries.move_to_end(entry_key)
            cached_rows_by_key[price_key] = _copy_private_rows(cached_entry.rows)
        return cached_rows_by_key, tuple(missing_keys)

    def admit_many(
        self,
        layout: PriceHydrationLayout,
        rows_by_price_key: Mapping[int, _PriceRows],
    ) -> None:
        """Privately copy and atomically retain one fully validated batch."""

        if not rows_by_price_key or self._maximum_bytes == 0:
            return
        if self._batch_weight(layout, rows_by_price_key) > self._maximum_bytes:
            self._rejected_batches += 1
            return
        prepared_entries = self._prepared_entries(layout, rows_by_price_key)
        prepared_bytes = sum(entry.retained_bytes for entry in prepared_entries.values())
        if prepared_bytes > self._maximum_bytes:
            self._rejected_batches += 1
            return
        replaced_bytes = sum(
            self._entries[entry_key].retained_bytes
            for entry_key in prepared_entries
            if entry_key in self._entries
        )
        retained_after_replacement = self._retained_bytes - replaced_bytes
        eviction_keys = self._evictions_for(
            retained_after_replacement + prepared_bytes,
            frozenset(prepared_entries),
        )
        for entry_key in prepared_entries:
            previous = self._entries.pop(entry_key, None)
            if previous is not None:
                self._retained_bytes -= previous.retained_bytes
        for eviction_key in eviction_keys:
            evicted_entry = self._entries.pop(eviction_key)
            self._retained_bytes -= evicted_entry.retained_bytes
            self._evictions += 1
        for entry_key, prepared_entry in prepared_entries.items():
            self._entries[entry_key] = prepared_entry
            self._retained_bytes += prepared_entry.retained_bytes
        self._admissions += len(prepared_entries)

    def metrics(self) -> PriceHydrationCacheMetrics:
        """Return an immutable view without walking retained payloads."""

        return PriceHydrationCacheMetrics(
            hits=self._hits,
            misses=self._misses,
            admissions=self._admissions,
            evictions=self._evictions,
            rejected_batches=self._rejected_batches,
            retained_bytes=self._retained_bytes,
            entries=len(self._entries),
            maximum_bytes=self._maximum_bytes,
        )

    def clear(self) -> None:
        """Release all retained payloads without resetting lifetime counters."""

        self._entries.clear()
        self._retained_bytes = 0

    def _prepared_entries(
        self,
        layout: PriceHydrationLayout,
        rows_by_price_key: Mapping[int, _PriceRows],
    ) -> OrderedDict[_EntryKey, _CacheEntry]:
        prepared: OrderedDict[_EntryKey, _CacheEntry] = OrderedDict()
        for price_key in sorted({int(key) for key in rows_by_price_key}):
            private_rows = _copy_private_rows(rows_by_price_key[price_key])
            entry_key = _entry_key(layout, price_key)
            retained_bytes = _retained_weight((entry_key, private_rows), set())
            prepared[entry_key] = _CacheEntry(private_rows, retained_bytes)
        return prepared

    def _batch_weight(
        self,
        layout: PriceHydrationLayout,
        rows_by_price_key: Mapping[int, _PriceRows],
    ) -> int:
        return sum(
            _retained_weight(
                (_entry_key(layout, price_key), rows_by_price_key[price_key]),
                set(),
            )
            for price_key in sorted({int(key) for key in rows_by_price_key})
        )

    def _evictions_for(
        self,
        desired_bytes: int,
        replacement_keys: frozenset[_EntryKey],
    ) -> tuple[_EntryKey, ...]:
        eviction_keys: list[_EntryKey] = []
        remaining_bytes = desired_bytes
        for entry_key, entry in self._entries.items():
            if remaining_bytes <= self._maximum_bytes:
                break
            if entry_key in replacement_keys:
                continue
            remaining_bytes -= entry.retained_bytes
            eviction_keys.append(entry_key)
        return tuple(eviction_keys)


def _entry_key(layout: PriceHydrationLayout, price_key: int) -> _EntryKey:
    """Represent each normalized distinct key as a one-key batch."""

    return layout, (int(price_key),)


def _copy_private_rows(rows: _PriceRows) -> _PriceRows:
    """Copy native rows so callers never retain a cache-owned reference."""

    return copy.deepcopy(rows)


def _retained_weight(value: Any, seen_ids: set[int]) -> int:
    value_id = id(value)
    if value_id in seen_ids:
        return 0
    seen_ids.add(value_id)
    if isinstance(value, Mapping):
        return 64 + 32 * len(value) + sum(
            _retained_weight(key, seen_ids) + _retained_weight(item, seen_ids)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return 56 + 8 * len(value) + sum(
            _retained_weight(item, seen_ids) for item in value
        )
    if isinstance(value, (set, frozenset)):
        return 216 + 32 * len(value) + sum(
            _retained_weight(item, seen_ids) for item in value
        )
    if isinstance(value, str):
        return 49 + len(value.encode("utf-8"))
    if isinstance(value, bytes):
        return 33 + len(value)
    if value is None or isinstance(value, (bool, int, float, Decimal)):
        return 32
    return 64 + len(str(value).encode("utf-8"))


PRICE_HYDRATION_CACHE = PriceHydrationCache(
    configured_price_hydration_cache_bytes()
)


def price_hydration_cache_metrics() -> PriceHydrationCacheMetrics:
    """Return current process-local cache counters."""

    return PRICE_HYDRATION_CACHE.metrics()
