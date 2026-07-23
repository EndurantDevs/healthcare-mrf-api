# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Small thread-safe Redis test double for live-progress Lua contracts."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any


class AtomicLiveProgressRedis:
    def __init__(
        self,
        storage: dict[str, Any] | None = None,
        *,
        on_progress_write: Callable[[str, int, Any], None] | None = None,
        before_get: Callable[[str], None] | None = None,
        before_set: Callable[[str], None] | None = None,
        shared_lock: threading.Lock | None = None,
    ) -> None:
        self.storage = storage if storage is not None else {}
        self.on_progress_write = on_progress_write
        self.before_get = before_get
        self.before_set = before_set
        self._lock = shared_lock or threading.Lock()

    def get(self, key: str) -> Any:
        if self.before_get is not None:
            self.before_get(key)
        with self._lock:
            return self.storage.get(key)

    def set(
        self,
        key: str,
        value: Any,
        *,
        nx: bool = False,
        px: int | None = None,
    ):
        del px
        if self.before_set is not None:
            self.before_set(key)
        with self._lock:
            if nx and key in self.storage:
                return 0
            self.storage[key] = value
            return 1

    def eval(
        self,
        script: str,
        _key_count: int,
        key: str,
        *arguments: Any,
    ) -> int:
        with self._lock:
            if "SETEX" in script:
                expected_missing, expected, ttl, candidate = arguments
                current = self.storage.get(key)
                if str(expected_missing) == "1":
                    if current is not None:
                        return 0
                elif current is None or current != expected:
                    return 0
                self.storage[key] = candidate
                if self.on_progress_write is not None:
                    self.on_progress_write(key, int(ttl), candidate)
                return 1
            token = arguments[0]
            if self.storage.get(key) == token:
                del self.storage[key]
                return 1
            return 0
