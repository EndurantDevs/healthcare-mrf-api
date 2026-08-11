"""Lifecycle fence adapter for source-snapshot control operations."""

from __future__ import annotations

from typing import Any

from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_lifecycle_lock


async def lock_source_pointer_gc(session: Any, *, source_key: str) -> None:
    """Hold the shared GC fence for one source-control transaction."""

    del source_key
    await acquire_ptg2_lifecycle_lock(session)
