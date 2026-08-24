# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Writable-attempt fence for failed PTG V4 physical-layout recovery."""

from __future__ import annotations

import json
from typing import Any, Mapping

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_gc import (
    PTG2V4AbandonmentCallbacks,
    abandon_owned_v4_layout,
)
from process.ptg_parts.ptg2_v4_failed_layout_state import row_mapping
from process.ptg_parts.ptg2_v4_stale_metadata_json import canonical_json_digest
from process.ptg_parts.ptg2_v4_stale_metadata_fence import (
    StaleMetadataFenceError,
    lock_writable_snapshot,
)


class PTG2V4RecoveryConflict(RuntimeError):
    """Exact failed-layout recovery safety gates no longer hold."""


async def load_recovery_attempt_fence(
    executor: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    import_run_id: str,
    lock_row: bool = False,
) -> dict[str, Any]:
    """Load the exact immutable attempt authority for one recovery pair."""

    lock_clause = " FOR UPDATE" if lock_row else ""
    rows = await executor.all(
        f"""
        SELECT snapshot_id, internal_run_id, fence_nonce, state,
               target_digest, plan_digest, marker_digest, marker,
               created_at, reconciled_at
          FROM {_quote_ident(schema_name)}.ptg2_v4_attempt_fence
         WHERE snapshot_id = :snapshot_id
           AND internal_run_id = :import_run_id
        {lock_clause}
        """,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
    )
    if len(rows) != 1:
        return {}
    return row_mapping(rows[0])


def require_active_recovery_fence(
    fence_by_field: dict[str, Any],
    *,
    snapshot_id: str,
    import_run_id: str,
    expected_fence_nonce: str | None = None,
    expected_fence_created_at: Any | None = None,
) -> dict[str, Any]:
    """Require the exact pristine active fence and immutable row identity."""

    fence_nonce = str(fence_by_field.get("fence_nonce") or "")
    if (
        fence_by_field.get("snapshot_id") != snapshot_id
        or fence_by_field.get("internal_run_id") != import_run_id
        or fence_by_field.get("state") != "active"
        or not fence_nonce
        or fence_by_field.get("created_at") is None
        or any(
            fence_by_field.get(field_name) is not None
            for field_name in (
                "target_digest",
                "plan_digest",
                "marker_digest",
                "marker",
                "reconciled_at",
            )
        )
        or (expected_fence_nonce is not None and fence_nonce != expected_fence_nonce)
        or (
            expected_fence_created_at is not None
            and fence_by_field.get("created_at") != expected_fence_created_at
        )
    ):
        raise PTG2V4RecoveryConflict(
            "failed PTG V4 layout recovery attempt fence changed"
        )
    return {**fence_by_field, "fence_nonce": fence_nonce}


async def lock_active_recovery_fence(
    connection: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    import_run_id: str,
    expected_fence_nonce: str | None = None,
    expected_fence_created_at: Any | None = None,
) -> dict[str, Any]:
    """Lock and validate the exact active recovery fence."""

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
            "failed PTG V4 layout recovery requires an active writable " "attempt fence"
        ) from exc
    fence_by_field = await load_recovery_attempt_fence(
        connection,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        lock_row=True,
    )
    return require_active_recovery_fence(
        fence_by_field,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        expected_fence_nonce=expected_fence_nonce,
        expected_fence_created_at=expected_fence_created_at,
    )


async def seal_recovery_attempt_fence(
    connection: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    import_run_id: str,
    expected_fence_nonce: str,
    expected_fence_created_at: Any,
    marker_by_field: dict[str, Any],
) -> None:
    """Seal the exact active attempt with the recovery audit marker."""

    marker_json = json.dumps(
        marker_by_field,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    updated_rows = await connection.all(
        f"""
        UPDATE {_quote_ident(schema_name)}.ptg2_v4_attempt_fence
           SET state = 'reconciled',
               target_digest = :target_digest,
               plan_digest = :plan_digest,
               marker_digest = :marker_digest,
               marker = CAST(:marker_json AS jsonb),
               reconciled_at = transaction_timestamp()
         WHERE snapshot_id = :snapshot_id
           AND internal_run_id = :import_run_id
           AND fence_nonce = CAST(:expected_fence_nonce AS uuid)
           AND created_at = :expected_fence_created_at
           AND state = 'active'
           AND target_digest IS NULL
           AND plan_digest IS NULL
           AND marker_digest IS NULL
           AND marker IS NULL
           AND reconciled_at IS NULL
        RETURNING snapshot_id
        """,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        expected_fence_nonce=expected_fence_nonce,
        expected_fence_created_at=expected_fence_created_at,
        target_digest=str(marker_by_field["target_digest"]),
        plan_digest=str(marker_by_field["plan_digest"]),
        marker_digest=canonical_json_digest(marker_by_field),
        marker_json=marker_json,
    )
    if len(updated_rows) != 1:
        raise PTG2V4RecoveryConflict(
            "failed PTG V4 layout recovery attempt fence changed"
        )


async def abandon_writable_v4_layout(
    *,
    schema_name: str,
    snapshot_id: str,
    import_run_id: str,
    snapshot_key: int,
    build_token: str,
    expected_fence_by_field: Mapping[str, Any],
    step_guard: Any | None = None,
    finalize_callback: Any | None = None,
) -> Any:
    """Fence every independently committed physical cleanup step."""

    async def guarded_step(connection: Any) -> None:
        """Recheck the exact attempt authority before one cleanup step."""

        await lock_active_recovery_fence(
            connection,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            import_run_id=import_run_id,
            expected_fence_nonce=str(expected_fence_by_field["fence_nonce"]),
            expected_fence_created_at=expected_fence_by_field["created_at"],
        )
        if step_guard is not None:
            await step_guard(connection)

    return await abandon_owned_v4_layout(
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        build_token=build_token,
        callbacks=PTG2V4AbandonmentCallbacks(
            step_guard=guarded_step,
            finalize=finalize_callback,
        ),
    )


__all__ = [
    "PTG2V4RecoveryConflict",
    "abandon_writable_v4_layout",
    "load_recovery_attempt_fence",
    "lock_active_recovery_fence",
    "require_active_recovery_fence",
    "seal_recovery_attempt_fence",
]
