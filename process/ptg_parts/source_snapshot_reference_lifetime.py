# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-scoped lifetime admission before snapshot-reference row mutations."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_lifecycle_lock import (
    PTG2_LIFECYCLE_LOCK_TIMEOUT,
    PTG2_LIFECYCLE_STATEMENT_TIMEOUT,
    PTG2LifecycleLockDeferred,
    acquire_ptg2_source_lifecycle_lock,
    is_retryable_lifecycle_database_error,
    lifecycle_database_sqlstate,
)
from process.ptg_parts.snapshot_tables import _normalize_source_key


class SnapshotReferenceIdentityError(RuntimeError):
    """The source or run behind a snapshot reference is missing or changed."""


async def _snapshot_source_identity(
    session: Any, schema_name: str, snapshot_id: str, *, lock: bool = False
) -> tuple[str, str, str]:
    identity_result = await session.execute(
        text(
            "SELECT snapshot.snapshot_id,snapshot.import_run_id,"
            "snapshot.manifest::jsonb #>> '{serving_index,source_key}' AS source_key,"
            "snapshot.manifest::jsonb #> '{serving_index,source_key}' IS NOT NULL AS source_present,"
            "jsonb_typeof(snapshot.manifest::jsonb #> '{serving_index,source_key}')='string' AS source_is_text,"
            "run.options::jsonb->>'source_key' AS run_source_key,"
            "jsonb_typeof(run.options::jsonb->'source_key')='string' AS run_source_is_text "
            f"FROM {_quote_ident(schema_name)}.ptg2_snapshot AS snapshot "
            f"JOIN {_quote_ident(schema_name)}.ptg2_import_run AS run USING(import_run_id) "
            "WHERE snapshot.snapshot_id=:snapshot_id" + (" FOR SHARE OF snapshot,run" if lock else "")
        ),
        {"snapshot_id": snapshot_id},
    )
    identity_row = identity_result.one_or_none()
    identity_by_field = dict(getattr(identity_row, "_mapping", identity_row)) if identity_row is not None else {}
    source_key = identity_by_field.get("run_source_key")
    run_id = identity_by_field.get("import_run_id")
    if (
        identity_by_field.get("snapshot_id") != snapshot_id
        or not isinstance(run_id, str)
        or not run_id
        or identity_by_field.get("run_source_is_text") is not True
        or not isinstance(source_key, str)
        or not source_key
        or _normalize_source_key(source_key) != source_key
        or (
            identity_by_field.get("source_present")
            and (
                identity_by_field.get("source_is_text") is not True or identity_by_field.get("source_key") != source_key
            )
        )
    ):
        raise SnapshotReferenceIdentityError("snapshot reference source identity is unavailable")
    return snapshot_id, run_id, source_key


async def acquire_snapshot_reference_lifetime(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    internal_run_id: str | None = None,
    lock_timeout: str = PTG2_LIFECYCLE_LOCK_TIMEOUT,
    statement_timeout: str = PTG2_LIFECYCLE_STATEMENT_TIMEOUT,
) -> str:
    """Authenticate the source before its fence, then refuse a changed observation.

    Call before acquiring snapshot or control row locks, in the same transaction
    as the reference mutation. The global shared fence also keeps GC outside it.
    """
    in_transaction = getattr(session, "in_transaction", None)
    if not callable(in_transaction) or not in_transaction():
        raise RuntimeError("snapshot reference lifetime requires a caller transaction")
    before = await _snapshot_source_identity(session, schema_name, snapshot_id)
    if internal_run_id is not None and before[1] != str(internal_run_id):
        raise SnapshotReferenceIdentityError("snapshot reference run identity changed")
    await acquire_ptg2_source_lifecycle_lock(
        session,
        source_key=before[2],
        lock_timeout=lock_timeout,
        statement_timeout=statement_timeout,
    )
    try:
        after = await _snapshot_source_identity(session, schema_name, snapshot_id, lock=True)
    except Exception as error:
        if lifecycle_database_sqlstate(error) == "40001":
            raise SnapshotReferenceIdentityError("snapshot reference source identity changed") from error
        if is_retryable_lifecycle_database_error(error):
            raise PTG2LifecycleLockDeferred("snapshot reference header lock is busy; retry") from error
        raise
    if after != before:
        raise SnapshotReferenceIdentityError("snapshot reference source identity changed")
    return before[2]


__all__ = ["SnapshotReferenceIdentityError", "acquire_snapshot_reference_lifetime"]
