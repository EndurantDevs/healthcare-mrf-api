# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Database reads and compare-and-swap writes for stale V4 metadata."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from typing import Any, Mapping

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_v4_attempt_registry import manifest_stage_table_names
from process.ptg_parts.ptg2_v4_stale_metadata_attachments import (
    attachment_count_query,
    optional_attachment_probe,
    present_optional_attachment_names,
)
from process.ptg_parts.ptg2_v4_stale_metadata_json import json_mapping
from process.ptg_parts.ptg2_v4_stale_metadata_types import (
    PTG2V4StaleMetadataConflict,
    PTG2V4StaleMetadataContext,
    PTG2V4StaleMetadataWrite,
    PTG2_V4_STALE_METADATA_ERROR,
    PTG2_V4_STALE_METADATA_MARKER,
)


def _row_mapping(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    if isinstance(row, dict):
        return dict(row)
    return dict(getattr(row, "_mapping", row))


def _snapshot_query(schema_name: str, *, should_lock_row: bool) -> str:
    lock_clause = " FOR UPDATE" if should_lock_row else ""
    return f"""
        SELECT snapshot_id, import_run_id, status, created_at,
               validated_at, published_at, previous_snapshot_id, manifest,
               manifest IS NULL AS manifest_is_sql_null
          FROM {_quote_ident(schema_name)}.ptg2_snapshot
         WHERE snapshot_id = :snapshot_id
        {lock_clause}
    """


def _internal_run_query(
    schema_name: str,
    *,
    should_lock_row: bool,
) -> str:
    lock_clause = " FOR UPDATE" if should_lock_row else ""
    return f"""
        SELECT import_run_id,
               status,
               started_at,
               finished_at,
               heartbeat_at,
               options,
               report,
               report IS NULL AS report_is_sql_null,
               error
          FROM {_quote_ident(schema_name)}.ptg2_import_run
         WHERE import_run_id = :internal_run_id
        {lock_clause}
    """


def _stage_parameters(
    *,
    schema_name: str,
    snapshot_id: str,
    internal_run_by_field: Mapping[str, Any] | None,
) -> dict[str, Any]:
    options_by_field = json_mapping(
        (internal_run_by_field or {}).get("options")
    )
    source_key = str(options_by_field.get("source_key") or "").strip()
    table_names = (
        manifest_stage_table_names(source_key, snapshot_id)
        if source_key
        else ()
    )
    parameter_by_name = {
        f"manifest_stage_{index}": (
            f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"
        )
        for index, table_name in enumerate(table_names)
    }
    parameter_by_name["manifest_stage_identity_missing"] = (
        0 if source_key else 1
    )
    return parameter_by_name


def _attempt_fence_query(
    schema_name: str,
    *,
    should_lock_row: bool,
) -> str:
    lock_clause = " FOR UPDATE" if should_lock_row else ""
    return f"""
        SELECT snapshot_id, internal_run_id, fence_nonce, state, target_digest,
               plan_digest, marker_digest, marker,
               marker IS NULL AS marker_is_sql_null,
               created_at, reconciled_at
          FROM {_quote_ident(schema_name)}.ptg2_v4_attempt_fence
         WHERE snapshot_id = :snapshot_id
           AND internal_run_id = :internal_run_id
        {lock_clause}
    """


async def _first_record(
    session: Any,
    database: Any,
    statement: str,
    parameter_by_name: Mapping[str, Any],
) -> dict[str, Any] | None:
    query_result = await session.execute(
        database.text(statement),
        dict(parameter_by_name),
    )
    return _row_mapping(query_result.first())


async def _database_timestamp(session: Any, database: Any) -> dt.datetime:
    timestamp_result = await session.execute(
        database.text(
            "SELECT timezone('UTC', transaction_timestamp()) AS observed_at"
        )
    )
    timestamp_row = _row_mapping(timestamp_result.first()) or {}
    observed_at = timestamp_row.get("observed_at")
    if not isinstance(observed_at, dt.datetime):
        raise RuntimeError("database did not return a reconciliation timestamp")
    return observed_at


async def _attachment_counts(
    session: Any,
    database: Any,
    *,
    schema_name: str,
    parameter_by_name: Mapping[str, Any],
    stage_table_count: int,
) -> dict[str, int]:
    optional_query, optional_parameter_by_name = optional_attachment_probe(
        schema_name
    )
    optional_presence_by_name = (
        await _first_record(
            session,
            database,
            optional_query,
            optional_parameter_by_name,
        )
        or {}
    )
    present_optional_names = present_optional_attachment_names(
        optional_presence_by_name
    )
    count_by_name = (
        await _first_record(
            session,
            database,
            attachment_count_query(
                schema_name,
                stage_table_count=stage_table_count,
                present_optional_names=present_optional_names,
            ),
            parameter_by_name,
        )
        or {}
    )
    return {
        str(name): int(attachment_count or 0)
        for name, attachment_count in count_by_name.items()
    }


async def _load_exact_attempt_rows(
    session: Any,
    database: Any,
    *,
    schema_name: str,
    parameter_by_name: Mapping[str, Any],
    should_lock_rows: bool,
) -> tuple[
    dict[str, Any] | None,
    dict[str, Any] | None,
    dict[str, Any] | None,
]:
    """Load snapshot, run, then fence in the required lock order."""

    snapshot_by_field = await _first_record(
        session,
        database,
        _snapshot_query(schema_name, should_lock_row=should_lock_rows),
        parameter_by_name,
    )
    internal_run_by_field = await _first_record(
        session,
        database,
        _internal_run_query(schema_name, should_lock_row=should_lock_rows),
        parameter_by_name,
    )
    attempt_fence_by_field = await _first_record(
        session,
        database,
        _attempt_fence_query(schema_name, should_lock_row=should_lock_rows),
        parameter_by_name,
    )
    return (
        snapshot_by_field,
        internal_run_by_field,
        attempt_fence_by_field,
    )


async def load_stale_metadata_context(
    session: Any,
    database: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    internal_run_id: str,
    should_lock_rows: bool,
) -> PTG2V4StaleMetadataContext:
    """Load one consistent exact-pair view and all no-payload guards."""

    observed_at = await _database_timestamp(session, database)
    parameter_by_name = {
        "snapshot_id": snapshot_id,
        "internal_run_id": internal_run_id,
    }
    (
        snapshot_by_field,
        internal_run_by_field,
        attempt_fence_by_field,
    ) = await _load_exact_attempt_rows(
        session,
        database,
        schema_name=schema_name,
        parameter_by_name=parameter_by_name,
        should_lock_rows=should_lock_rows,
    )
    stage_parameter_by_name = _stage_parameters(
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        internal_run_by_field=internal_run_by_field,
    )
    parameter_by_name.update(stage_parameter_by_name)
    attachment_count_by_name = await _attachment_counts(
        session,
        database,
        schema_name=schema_name,
        parameter_by_name=parameter_by_name,
        stage_table_count=len(stage_parameter_by_name) - 1,
    )
    return PTG2V4StaleMetadataContext(
        observed_at=observed_at,
        snapshot_by_field=snapshot_by_field,
        internal_run_by_field=internal_run_by_field,
        attempt_fence_by_field=attempt_fence_by_field,
        attachment_count_by_name=attachment_count_by_name,
    )


async def _apply_stale_snapshot_row(
    session: Any,
    database: Any,
    *,
    stale_write: PTG2V4StaleMetadataWrite,
    marker_json: str,
) -> None:
    """Transition only the reviewed empty-manifest snapshot row."""

    schema = _quote_ident(stale_write.schema_name)
    snapshot_by_field = stale_write.context.snapshot_by_field or {}
    snapshot_result = await session.execute(
        database.text(
            f"""
            UPDATE {schema}.ptg2_snapshot
               SET status = 'failed',
                   manifest = jsonb_build_object(
                       '{PTG2_V4_STALE_METADATA_MARKER}',
                       CAST(:marker_json AS jsonb)
                   )::json
             WHERE snapshot_id = :snapshot_id
               AND import_run_id = :internal_run_id
               AND status = 'building'
               AND created_at IS NOT DISTINCT FROM :snapshot_created_at
               AND validated_at IS NOT DISTINCT FROM :expected_validated_at
               AND published_at IS NOT DISTINCT FROM :expected_published_at
               AND previous_snapshot_id IS NOT DISTINCT FROM
                   :expected_previous_snapshot_id
               AND COALESCE(manifest::jsonb, '{{}}'::jsonb) = '{{}}'::jsonb
            RETURNING snapshot_id
            """
        ),
        {
            "marker_json": marker_json,
            "snapshot_id": stale_write.snapshot_id,
            "internal_run_id": stale_write.internal_run_id,
            "snapshot_created_at": snapshot_by_field.get("created_at"),
            "expected_validated_at": snapshot_by_field.get("validated_at"),
            "expected_published_at": snapshot_by_field.get("published_at"),
            "expected_previous_snapshot_id": snapshot_by_field.get(
                "previous_snapshot_id"
            ),
        },
    )
    if snapshot_result.first() is None:
        raise PTG2V4StaleMetadataConflict(
            "snapshot changed after stale-build planning"
        )


def _internal_run_update_query(schema_name: str) -> str:
    schema = _quote_ident(schema_name)
    return f"""
        UPDATE {schema}.ptg2_import_run
           SET status = 'failed',
               finished_at = timezone('UTC', transaction_timestamp()),
               heartbeat_at = timezone('UTC', transaction_timestamp()),
               report = jsonb_set(
                   COALESCE(report::jsonb, '{{}}'::jsonb),
                   '{{{PTG2_V4_STALE_METADATA_MARKER}}}',
                   CAST(:marker_json AS jsonb),
                   true
               )::json,
               error = :reconciliation_error
         WHERE import_run_id = :internal_run_id
           AND status = :expected_internal_run_status
           AND started_at IS NOT DISTINCT FROM :expected_started_at
           AND finished_at IS NOT DISTINCT FROM :expected_finished_at
           AND heartbeat_at IS NOT DISTINCT FROM :expected_heartbeat_at
           AND error IS NOT DISTINCT FROM :expected_error
           AND COALESCE(options->>'storage_generation', '')
               = :v4_storage_generation
           AND COALESCE(
                   heartbeat_at,
                   started_at,
                   :snapshot_created_at
               ) < timezone('UTC', transaction_timestamp())
                   - (:stale_after_seconds * INTERVAL '1 second')
           AND NOT (
               COALESCE(report::jsonb, '{{}}'::jsonb)
                   ? '{PTG2_V4_STALE_METADATA_MARKER}'
           )
        RETURNING import_run_id
    """


async def _apply_stale_internal_run(
    session: Any,
    database: Any,
    *,
    stale_write: PTG2V4StaleMetadataWrite,
    marker_json: str,
) -> None:
    """Transition only the reviewed stale V4 internal-run row."""

    snapshot_by_field = stale_write.context.snapshot_by_field or {}
    internal_run_by_field = stale_write.context.internal_run_by_field or {}
    internal_run_result = await session.execute(
        database.text(_internal_run_update_query(stale_write.schema_name)),
        {
            "marker_json": marker_json,
            "reconciliation_error": PTG2_V4_STALE_METADATA_ERROR,
            "internal_run_id": stale_write.internal_run_id,
            "expected_internal_run_status": internal_run_by_field.get("status"),
            "expected_started_at": internal_run_by_field.get("started_at"),
            "expected_finished_at": internal_run_by_field.get("finished_at"),
            "expected_heartbeat_at": internal_run_by_field.get("heartbeat_at"),
            "expected_error": internal_run_by_field.get("error"),
            "snapshot_created_at": snapshot_by_field.get("created_at"),
            "v4_storage_generation": stale_write.v4_storage_generation,
            "stale_after_seconds": stale_write.stale_after_seconds,
        },
    )
    if internal_run_result.first() is None:
        raise PTG2V4StaleMetadataConflict(
            "internal run changed after stale-build planning"
        )


async def _seal_attempt_fence(
    session: Any,
    database: Any,
    *,
    stale_write: PTG2V4StaleMetadataWrite,
    marker_json: str,
) -> None:
    """Persist the immutable audit authority for the dual-row marker."""
    marker_by_field = stale_write.marker_by_field
    fence_by_field = stale_write.context.attempt_fence_by_field or {}
    marker_digest = hashlib.sha256(marker_json.encode("utf-8")).hexdigest()
    fence_result = await session.execute(
        database.text(
            f"""
            UPDATE {_quote_ident(stale_write.schema_name)}.ptg2_v4_attempt_fence
               SET state = 'reconciled',
                   target_digest = :target_digest,
                   plan_digest = :plan_digest,
                   marker_digest = :marker_digest,
                   marker = CAST(:marker_json AS jsonb),
                   reconciled_at = transaction_timestamp()
             WHERE snapshot_id = :snapshot_id
               AND internal_run_id = :internal_run_id
               AND fence_nonce = :expected_fence_nonce
               AND created_at IS NOT DISTINCT FROM :expected_fence_created_at
               AND state = 'active'
               AND target_digest IS NULL
               AND plan_digest IS NULL
               AND marker_digest IS NULL
               AND marker IS NULL
               AND reconciled_at IS NULL
            RETURNING snapshot_id
            """
        ),
        {
            "snapshot_id": stale_write.snapshot_id,
            "internal_run_id": stale_write.internal_run_id,
            "expected_fence_nonce": fence_by_field.get("fence_nonce"),
            "expected_fence_created_at": fence_by_field.get("created_at"),
            "target_digest": marker_by_field["target_digest"],
            "plan_digest": marker_by_field["plan_digest"],
            "marker_digest": marker_digest,
            "marker_json": marker_json,
        },
    )
    if fence_result.first() is None:
        raise PTG2V4StaleMetadataConflict(
            "attempt fence changed after stale-build planning"
        )


async def apply_stale_metadata_rows(
    session: Any,
    database: Any,
    *,
    stale_write: PTG2V4StaleMetadataWrite,
) -> None:
    """Apply the three CAS metadata updates without cleanup side effects."""

    marker_json = json.dumps(
        dict(stale_write.marker_by_field),
        sort_keys=True,
        separators=(",", ":"),
    )
    await _apply_stale_snapshot_row(
        session,
        database,
        stale_write=stale_write,
        marker_json=marker_json,
    )
    await _apply_stale_internal_run(
        session,
        database,
        stale_write=stale_write,
        marker_json=marker_json,
    )
    await _seal_attempt_fence(
        session,
        database,
        stale_write=stale_write,
        marker_json=marker_json,
    )
__all__ = ["apply_stale_metadata_rows", "load_stale_metadata_context"]
