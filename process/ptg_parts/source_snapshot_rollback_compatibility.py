"""Best-effort projection of source rollback into the legacy singleton."""

from __future__ import annotations

import logging

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_lifecycle_lock import (
    PTG2LifecycleLockDeferred,
    acquire_ptg2_source_lifecycle_lock,
    is_retryable_lifecycle_database_error,
)


_GLOBAL_POINTER_LOCK_IDENTITY = "legacy_global_snapshot_pointer"
_GLOBAL_POINTER_LOCK_TIMEOUT = "50ms"
_GLOBAL_POINTER_STATEMENT_TIMEOUT = "500ms"
logger = logging.getLogger(__name__)


async def attempt_legacy_global_pointer_rollback(
    *,
    schema_name: str,
    snapshot_id: str,
    expected_current_snapshot_id: str,
) -> str:
    """Best-effort legacy singleton rollback after source rollback commits."""

    try:
        async with db.transaction() as session:
            await acquire_ptg2_source_lifecycle_lock(
                session,
                source_key=_GLOBAL_POINTER_LOCK_IDENTITY,
                lock_timeout=_GLOBAL_POINTER_LOCK_TIMEOUT,
                statement_timeout=_GLOBAL_POINTER_STATEMENT_TIMEOUT,
            )
            compatibility_result = await session.execute(
                db.text(
                    f"""
                    UPDATE {_quote_ident(schema_name)}.ptg2_current_snapshot
                       SET snapshot_id = :snapshot_id,
                           previous_snapshot_id = :expected_current_snapshot_id,
                           updated_at = transaction_timestamp()
                     WHERE slot = 'current'
                       AND snapshot_id = :expected_current_snapshot_id
                       AND previous_snapshot_id = :snapshot_id
                    RETURNING slot
                    """
                ),
                {
                    "snapshot_id": snapshot_id,
                    "expected_current_snapshot_id": expected_current_snapshot_id,
                },
            )
            has_rolled_back_pointer = (
                compatibility_result.one_or_none() is not None
            )
    except Exception as exc:
        if not isinstance(exc, PTG2LifecycleLockDeferred) and not (
            is_retryable_lifecycle_database_error(exc)
        ):
            logger.warning(
                "Deferred legacy PTG global rollback for %s: %s",
                snapshot_id,
                exc,
            )
        return "deferred"
    return "reversed" if has_rolled_back_pointer else "unchanged"
