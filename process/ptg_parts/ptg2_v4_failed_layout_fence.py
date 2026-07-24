# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Writable-attempt fence for failed PTG V4 physical-layout recovery."""

from __future__ import annotations

from typing import Any

from db.connection import db
from process.ptg_parts.ptg2_shared_gc import abandon_owned_v4_layout
from process.ptg_parts.ptg2_v4_stale_metadata_fence import (
    StaleMetadataFenceError,
    lock_writable_snapshot,
)


class PTG2V4RecoveryConflict(RuntimeError):
    """Exact failed-layout recovery safety gates no longer hold."""


async def abandon_writable_v4_layout(
    connection: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    import_run_id: str,
    snapshot_key: int,
    build_token: str,
) -> Any:
    """Fence the logical attempt before releasing its physical ownership."""

    try:
        await lock_writable_snapshot(
            connection,
            db,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            internal_run_id=import_run_id,
        )
    except StaleMetadataFenceError as exc:
        raise PTG2V4RecoveryConflict(
            "failed PTG V4 layout recovery requires an active writable "
            "attempt fence"
        ) from exc
    return await abandon_owned_v4_layout(
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        build_token=build_token,
        executor=connection,
    )


__all__ = [
    "PTG2V4RecoveryConflict",
    "abandon_writable_v4_layout",
]
