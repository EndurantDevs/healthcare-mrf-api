# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""API-worker heap lifecycle controls for stable low-latency serving."""

from __future__ import annotations

import gc
import logging
import os
from dataclasses import dataclass
from typing import Any


logger = logging.getLogger(__name__)
_FREEZE_ENV = "HLTHPRT_API_WORKER_GC_FREEZE_ENABLED"
_FALSE_VALUES = frozenset({"0", "false", "no", "off", "disabled"})


@dataclass
class _WorkerMemoryState:
    is_heap_frozen: bool = False
    freeze_successes: int = 0
    freeze_failures: int = 0
    explicit_collections: int = 0


_WORKER_MEMORY_STATE = _WorkerMemoryState()


@dataclass(frozen=True)
class WorkerMemoryMetrics:
    """Immutable lifecycle counters for one API worker process."""

    is_enabled: bool
    is_frozen: bool
    freeze_successes: int
    freeze_failures: int
    explicit_collections: int
    permanent_objects: int


def is_worker_gc_freeze_enabled() -> bool:
    """Default on for API workers, with an immediate rollback switch."""

    raw_value = str(os.getenv(_FREEZE_ENV, "1")).strip().lower()
    return raw_value not in _FALSE_VALUES


def freeze_api_worker_heap() -> None:
    """Collect and freeze initialized worker objects while leaving GC enabled."""

    if not is_worker_gc_freeze_enabled() or _WORKER_MEMORY_STATE.is_heap_frozen:
        return
    if not gc.isenabled():
        gc.enable()
    try:
        gc.collect()
        _WORKER_MEMORY_STATE.explicit_collections += 1
        gc.freeze()
    except Exception:
        _WORKER_MEMORY_STATE.freeze_failures += 1
        _rollback_failed_freeze()
        logger.warning("API worker heap freeze failed; continuing unfrozen", exc_info=True)
        return
    _WORKER_MEMORY_STATE.is_heap_frozen = True
    _WORKER_MEMORY_STATE.freeze_successes += 1


def unfreeze_api_worker_heap() -> None:
    """Release this worker's frozen generation before orderly shutdown."""

    if not _WORKER_MEMORY_STATE.is_heap_frozen:
        return
    gc.unfreeze()
    _WORKER_MEMORY_STATE.is_heap_frozen = False
    if not gc.isenabled():
        gc.enable()
    gc.collect()
    _WORKER_MEMORY_STATE.explicit_collections += 1


def worker_memory_metrics() -> WorkerMemoryMetrics:
    """Return low-cost GC lifecycle state for internal metrics."""

    get_freeze_count = getattr(gc, "get_freeze_count", None)
    permanent_objects = int(get_freeze_count()) if get_freeze_count else 0
    return WorkerMemoryMetrics(
        is_enabled=is_worker_gc_freeze_enabled(),
        is_frozen=_WORKER_MEMORY_STATE.is_heap_frozen,
        freeze_successes=_WORKER_MEMORY_STATE.freeze_successes,
        freeze_failures=_WORKER_MEMORY_STATE.freeze_failures,
        explicit_collections=_WORKER_MEMORY_STATE.explicit_collections,
        permanent_objects=permanent_objects,
    )


def register_worker_memory_lifecycle(app: Any) -> None:
    """Install per-worker Sanic listeners without affecting CLI processes."""

    app.listener("after_server_start")(_after_server_start)
    app.listener("before_server_stop")(_before_server_stop)


async def _after_server_start(_app: Any) -> None:
    freeze_api_worker_heap()


async def _before_server_stop(_app: Any) -> None:
    unfreeze_api_worker_heap()


def _rollback_failed_freeze() -> None:
    """Return to normal collection if freeze partially changed the heap."""

    try:
        gc.unfreeze()
    except Exception:
        logger.warning("API worker heap freeze rollback could not unfreeze", exc_info=True)
    _WORKER_MEMORY_STATE.is_heap_frozen = False
    if not gc.isenabled():
        gc.enable()
    gc.collect()
    _WORKER_MEMORY_STATE.explicit_collections += 1
