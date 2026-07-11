# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider-reference caches used by PTG2 import paths."""

from __future__ import annotations

import datetime
import json
import sqlite3
from collections import OrderedDict
from decimal import Decimal
from pathlib import Path
from typing import Any

from process.ptg_parts.config import PTG2_PROVIDER_CACHE_MEMORY_REFS_ENV, _env_int


def _normalize_provider_ref(value: Any) -> Any:
    if isinstance(value, str) and value.isdigit():
        try:
            return int(value)
        except ValueError:
            return value
    return value


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        normalized = value.normalize()
        if normalized == normalized.to_integral():
            return format(normalized.quantize(Decimal(1)), "f")
        return format(normalized, "f")
    return str(value)


class PTG2ProviderReferenceCache:
    def __init__(self, path: str | Path, initial: dict[Any, list[dict[str, Any]]] | None = None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path))
        self.conn.execute("PRAGMA journal_mode=OFF")
        self.conn.execute("PRAGMA synchronous=OFF")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS provider_ref "
            "(provider_group_ref TEXT PRIMARY KEY, groups_json TEXT NOT NULL)"
        )
        self.provider_hashes: set[int] = set()
        self.memory_limit = max(_env_int(PTG2_PROVIDER_CACHE_MEMORY_REFS_ENV, 65536), 0)
        self.memory_cache: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
        self.get_count = 0
        self.sqlite_hit_count = 0
        self.memory_hit_count = 0
        self.miss_count = 0
        if initial:
            for ref, groups in initial.items():
                self.put(ref, groups)

    @staticmethod
    def key(ref: Any) -> str:
        """Return the normalized string key for a provider-group reference."""
        return str(_normalize_provider_ref(ref))

    def put(self, ref: Any, groups: list[dict[str, Any]]) -> None:
        """Store nonempty provider groups under a reference in SQLite and memory."""
        if ref is None or not groups:
            return
        cache_key = self.key(ref)
        for group in groups:
            if isinstance(group, dict) and "__hash__" in group:
                self.provider_hashes.add(group["__hash__"])
        self._remember(cache_key, groups)
        payload = json.dumps(groups, sort_keys=True, default=_json_default)
        self.conn.execute(
            "INSERT OR REPLACE INTO provider_ref (provider_group_ref, groups_json) VALUES (?, ?)",
            (cache_key, payload),
        )

    def get(self, ref: Any) -> list[dict[str, Any]]:
        """Return provider groups for a reference while recording cache statistics."""
        if ref is None:
            return []
        self.get_count += 1
        cache_key = self.key(ref)
        cached = self.memory_cache.get(cache_key)
        if cached is not None:
            self.memory_hit_count += 1
            self.memory_cache.move_to_end(cache_key)
            return cached
        row = self.conn.execute(
            "SELECT groups_json FROM provider_ref WHERE provider_group_ref = ?",
            (cache_key,),
        ).fetchone()
        if not row:
            self.miss_count += 1
            return []
        groups = json.loads(row[0])
        self.sqlite_hit_count += 1
        self._remember(cache_key, groups)
        return groups

    def _remember(self, cache_key: str, groups: list[dict[str, Any]]) -> None:
        if self.memory_limit <= 0:
            return
        self.memory_cache[cache_key] = groups
        self.memory_cache.move_to_end(cache_key)
        while len(self.memory_cache) > self.memory_limit:
            self.memory_cache.popitem(last=False)

    def stats(self) -> dict[str, int]:
        """Return current lookup, hit, miss, and memory-cache counters."""
        return {
            "provider_cache_gets": self.get_count,
            "provider_cache_memory_hits": self.memory_hit_count,
            "provider_cache_sqlite_hits": self.sqlite_hit_count,
            "provider_cache_misses": self.miss_count,
            "provider_cache_memory_size": len(self.memory_cache),
            "provider_cache_memory_limit": self.memory_limit,
        }

    def commit(self) -> None:
        """Commit pending provider-reference changes to SQLite."""
        self.conn.commit()

    def close(self) -> None:
        """Commit pending changes and close the SQLite cache connection."""
        self.conn.commit()
        self.conn.close()


class PTG2InMemoryProviderReferenceCache:
    def __init__(self, initial: dict[Any, list[dict[str, Any]]] | None = None):
        self.refs: dict[str, list[dict[str, Any]]] = {}
        self.provider_hashes: set[int] = set()
        self.get_count = 0
        self.hit_count = 0
        self.miss_count = 0
        if initial:
            for ref, groups in initial.items():
                self.put(ref, groups)

    @staticmethod
    def key(ref: Any) -> str:
        """Return the normalized string key for a provider-group reference."""
        return str(_normalize_provider_ref(ref))

    def put(self, ref: Any, groups: list[dict[str, Any]]) -> None:
        """Store nonempty provider groups under an in-memory reference key."""
        if ref is None or not groups:
            return
        cache_key = self.key(ref)
        for group in groups:
            if isinstance(group, dict) and "__hash__" in group:
                self.provider_hashes.add(group["__hash__"])
        self.refs[cache_key] = groups

    def get(self, ref: Any) -> list[dict[str, Any]]:
        """Return provider groups for a reference while recording cache statistics."""
        if ref is None:
            return []
        self.get_count += 1
        groups = self.refs.get(self.key(ref))
        if groups is None:
            self.miss_count += 1
            return []
        self.hit_count += 1
        return groups

    def stats(self) -> dict[str, int]:
        """Return current lookup, hit, miss, and cache-size counters."""
        return {
            "provider_cache_gets": self.get_count,
            "provider_cache_memory_hits": self.hit_count,
            "provider_cache_sqlite_hits": 0,
            "provider_cache_misses": self.miss_count,
            "provider_cache_memory_size": len(self.refs),
            "provider_cache_memory_limit": len(self.refs),
        }

    def commit(self) -> None:
        """Provide the persistent-cache commit interface without doing work."""
        return None

    def close(self) -> None:
        """Provide the persistent-cache close interface without doing work."""
        return None


def _provider_cache_get(provider_cache: Any, ref: Any) -> list[dict[str, Any]]:
    if isinstance(provider_cache, (PTG2ProviderReferenceCache, PTG2InMemoryProviderReferenceCache)):
        return provider_cache.get(ref)
    return provider_cache.get(ref) or provider_cache.get(_normalize_provider_ref(ref)) or []


def _provider_cache_put(provider_cache: Any, ref: Any, groups: list[dict[str, Any]]) -> None:
    if isinstance(provider_cache, (PTG2ProviderReferenceCache, PTG2InMemoryProviderReferenceCache)):
        provider_cache.put(ref, groups)
    else:
        provider_cache.setdefault(ref, []).extend(groups)


def _provider_combo_cache_key(provider_refs: list[Any] | tuple[Any, ...]) -> tuple[str, ...]:
    return tuple(sorted({str(_normalize_provider_ref(ref)) for ref in provider_refs if ref is not None}))


def _provider_combo_cache_get(
    cache: OrderedDict[tuple[str, ...], dict[str, Any]],
    key: tuple[str, ...],
    stats: dict[str, int],
) -> dict[str, Any] | None:
    stats["provider_combo_cache_gets"] += 1
    cached = cache.get(key)
    if cached is None:
        stats["provider_combo_cache_misses"] += 1
        return None
    stats["provider_combo_cache_hits"] += 1
    cache.move_to_end(key)
    return cached


def _provider_combo_cache_put(
    cache: OrderedDict[tuple[str, ...], dict[str, Any]],
    key: tuple[str, ...],
    value: dict[str, Any],
    stats: dict[str, int],
    limit: int,
) -> None:
    if limit <= 0 or not key:
        return
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > limit:
        cache.popitem(last=False)
    stats["provider_combo_cache_size"] = len(cache)
    stats["provider_combo_cache_limit"] = limit


def _provider_cache_hashes(provider_cache: Any) -> set[int]:
    if isinstance(provider_cache, (PTG2ProviderReferenceCache, PTG2InMemoryProviderReferenceCache)):
        return set(provider_cache.provider_hashes)
    return {
        entry["__hash__"]
        for groups in provider_cache.values()
        for entry in groups
        if isinstance(entry, dict) and "__hash__" in entry
    }
