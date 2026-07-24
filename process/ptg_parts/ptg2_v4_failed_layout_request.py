# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Input normalization for exact PTG V4 failed-layout recovery."""

from __future__ import annotations

from typing import Any

from process.ptg_parts.ptg2_lifecycle_lock import (
    PTG2_SOURCE_POINTER_GC_LOCK_KEY,
)


def normalize_recovery_request(
    *,
    snapshot_id: str,
    import_run_id: str,
    snapshot_key: int,
) -> tuple[str, str, int]:
    """Normalize one exact logical and physical recovery target."""

    normalized_snapshot_id = str(snapshot_id or "").strip()
    normalized_run_id = str(import_run_id or "").strip()
    normalized_snapshot_key = int(snapshot_key)
    if (
        not normalized_snapshot_id
        or not normalized_run_id
        or normalized_snapshot_key <= 0
    ):
        raise ValueError(
            "snapshot_id, import_run_id, and a positive snapshot_key are required"
        )
    return normalized_snapshot_id, normalized_run_id, normalized_snapshot_key


def normalize_plan_digest(expected_plan_digest: str) -> str:
    """Require one lowercase SHA-256 recovery-plan digest."""

    normalized_digest = str(expected_plan_digest or "").strip().lower()
    try:
        decoded_digest = bytes.fromhex(normalized_digest)
    except ValueError as exc:
        raise ValueError(
            "expected_plan_digest must contain 64 hexadecimal characters"
        ) from exc
    if len(decoded_digest) != 32:
        raise ValueError(
            "expected_plan_digest must contain 64 hexadecimal characters"
        )
    return normalized_digest


async def lock_recovery_pointer_state(executor: Any) -> None:
    """Serialize recovery with every PTG source-pointer transition."""

    await executor.status(
        "SELECT pg_advisory_xact_lock(hashtext(:lock_key))",
        lock_key=PTG2_SOURCE_POINTER_GC_LOCK_KEY,
    )


__all__ = [
    "lock_recovery_pointer_state",
    "normalize_plan_digest",
    "normalize_recovery_request",
]
