# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared transaction lock for PTG snapshot lifecycle mutations."""

from __future__ import annotations

from typing import Any

from db.connection import db


PTG2_SOURCE_POINTER_GC_LOCK_KEY = "ptg2_source_pointer_gc_v1"


async def acquire_ptg2_lifecycle_lock(session: Any) -> None:
    """Serialize candidate, pointer, attestation, and garbage-collection changes."""

    await session.execute(
        db.text("SELECT pg_advisory_xact_lock(hashtext(:publish_lock_key))"),
        {"publish_lock_key": PTG2_SOURCE_POINTER_GC_LOCK_KEY},
    )


__all__ = ["PTG2_SOURCE_POINTER_GC_LOCK_KEY", "acquire_ptg2_lifecycle_lock"]
