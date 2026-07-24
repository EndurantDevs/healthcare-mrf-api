# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Targeted control helpers for source-scoped PTG2 snapshots."""

from __future__ import annotations

import os
from typing import Any

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_artifact_blobs import ensure_ptg2_artifact_blob_table
from process.ptg_parts.ptg2_shared_gc import release_unbound_ptg2_shared_layouts
from process.ptg_parts.snapshot_cleanup import is_shared_snapshot_control_manifest
from process.ptg_parts.source_snapshot_control_policy import (
    SUPPORTED_SHARED_SNAPSHOT_CONTROL_MESSAGE,
    manifest_dict,
    retirement_manifest_source_key,
    snapshot_remove_reasons,
)
from process.ptg_parts.source_snapshot_shared_layout import (
    bound_shared_layout_keys,
)
from process.ptg_parts.source_pointers import (
    PTG2_SOURCE_POINTER_GC_LOCK_KEY,
    PTG2SourcePointerConflict,
    activate_ptg2_source_candidate,
)


class SourceSnapshotConflict(ValueError):
    """Raised when a source snapshot pointer changed between plan and execute."""


class _TransactionExecutor:
    """Expose the current SQLAlchemy transaction through the shared-GC interface."""

    def __init__(self, session: Any):
        self._session = session

    async def all(self, statement: Any, **params: Any) -> list[Any]:
        """Execute a statement and return all result rows."""
        result = await self._session.execute(
            _text(statement) if isinstance(statement, str) else statement,
            params,
        )
        return result.all()

    async def status(self, statement: Any, **params: Any) -> int | None:
        """Execute a statement and return its affected row count."""
        result = await self._session.execute(
            _text(statement) if isinstance(statement, str) else statement,
            params,
        )
        return getattr(result, "rowcount", None)


def _schema_name() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _row_mapping(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    return dict(getattr(row, "_mapping", row))


async def promote_ptg2_source_snapshot(
    *,
    source_key: str,
    snapshot_id: str,
    expected_current_snapshot_id: str | None = None,
) -> dict[str, Any]:
    """Activate one audited strict-V3 candidate and all of its live pointers."""
    source_key = str(source_key or "").strip().lower()
    snapshot_id = str(snapshot_id or "").strip()
    if not source_key or not snapshot_id:
        raise ValueError("source_key and snapshot_id are required")
    try:
        result = await activate_ptg2_source_candidate(
            source_key=source_key,
            snapshot_id=snapshot_id,
            expected_current_snapshot_id=expected_current_snapshot_id,
        )
    except PTG2SourcePointerConflict as exc:
        raise SourceSnapshotConflict(str(exc)) from exc
    _clear_ptg2_snapshot_cache()
    return result


async def build_ptg2_source_snapshot_remove_plan(
    *,
    snapshot_id: str,
    source_key: str | None = None,
) -> dict[str, Any]:
    """Describe whether a source-scoped snapshot can be safely removed."""
    snapshot_id = str(snapshot_id or "").strip()
    source_key = str(source_key or "").strip() or None
    if not snapshot_id:
        raise ValueError("snapshot_id is required")
    schema = _schema_name()
    snapshot = await _snapshot_row(schema, snapshot_id)
    if not snapshot:
        return {
            "snapshot_id": snapshot_id,
            "source_key": source_key,
            "exists": False,
            "removable": True,
            "metadata_only": True,
            "tables": [],
            "artifact_manifest_ids": [],
            "current_references": {},
        }
    manifest = manifest_dict(snapshot.get("manifest"))
    serving_index = manifest.get("serving_index") if isinstance(manifest.get("serving_index"), dict) else {}
    storage_generation = str(
        serving_index.get("storage_generation") or ""
    ).strip().lower()
    if not is_shared_snapshot_control_manifest(serving_index):
        return {
            "snapshot_id": snapshot_id,
            "source_key": source_key,
            "exists": True,
            "removable": False,
            "reason": (
                f"{SUPPORTED_SHARED_SNAPSHOT_CONTROL_MESSAGE} can be removed"
            ),
            "metadata_only": True,
            "tables": [],
            "artifact_manifest_ids": [],
            "current_references": {},
            "storage_generation": storage_generation or None,
            "status": snapshot.get("status"),
            "import_month": str(snapshot.get("import_month") or ""),
        }
    manifest_source_key = str(serving_index.get("source_key") or "").strip() or None
    references = await _current_references(schema, snapshot_id)
    artifact_ids = await _artifact_manifest_ids(schema, snapshot_id)
    snapshot_status = str(snapshot.get("status") or "").strip().lower()
    reasons = snapshot_remove_reasons(
        source_key=source_key,
        manifest_source_key=manifest_source_key,
        snapshot_status=snapshot_status,
        references=references,
    )
    return {
        "snapshot_id": snapshot_id,
        "source_key": source_key or manifest_source_key,
        "exists": True,
        "removable": not reasons,
        "reason": "; ".join(reasons) if reasons else None,
        "metadata_only": False,
        "tables": [],
        "artifact_manifest_ids": artifact_ids,
        "current_references": references,
        "storage_generation": storage_generation,
        "shared_snapshot_key": serving_index.get("shared_snapshot_key"),
        "status": snapshot.get("status"),
        "import_month": str(snapshot.get("import_month") or ""),
    }


async def remove_ptg2_source_snapshot(
    *,
    snapshot_id: str,
    source_key: str | None = None,
) -> dict[str, Any]:
    """Remove an unreferenced source snapshot after validating its removal plan."""
    snapshot_id = str(snapshot_id or "").strip()
    source_key = str(source_key or "").strip() or None
    if not snapshot_id:
        raise ValueError("snapshot_id is required")
    schema = _schema_name()
    async with db.transaction() as session:
        await _lock_source_pointer_gc(session)
        plan = await build_ptg2_source_snapshot_remove_plan(snapshot_id=snapshot_id, source_key=source_key)
        if not plan.get("removable"):
            raise ValueError(str(plan.get("reason") or "snapshot is not removable"))
        if not plan.get("exists"):
            return {
                **plan,
                "executed": True,
                "deleted_tables": 0,
                "deleted_v3_snapshot_scopes": 0,
                "deleted_v3_snapshot_bindings": 0,
                "deleted_artifact_manifests": 0,
                "deleted_snapshots": 0,
                "released_shared_layouts": 0,
                "queued_shared_block_candidates": 0,
                "queued_shared_block_bytes": 0,
                "physical_cleanup": "not_applicable",
            }
        storage_generation = str(
            plan.get("storage_generation") or ""
        ).strip().lower()
        layout_keys = await bound_shared_layout_keys(
            session,
            schema=schema,
            snapshot_id=snapshot_id,
            expected_generation=storage_generation,
            expected_snapshot_key=plan.get("shared_snapshot_key"),
        )
        tables: list[str] = []
        deleted_v3_snapshot_scopes = await db.status(
            f"DELETE FROM {_quote_ident(schema)}.ptg2_v3_snapshot_scope WHERE snapshot_id = :snapshot_id",
            snapshot_id=snapshot_id,
        )
        deleted_v3_snapshot_bindings = await db.status(
            f"DELETE FROM {_quote_ident(schema)}.ptg2_v3_snapshot_binding WHERE snapshot_id = :snapshot_id",
            snapshot_id=snapshot_id,
        )
        artifact_ids = [
            str(artifact_id)
            for artifact_id in plan.get("artifact_manifest_ids") or []
        ]
        deleted_artifact_chunks = 0
        if artifact_ids:
            await ensure_ptg2_artifact_blob_table(schema)
            deleted_artifact_chunks = await db.status(
                f"DELETE FROM {_quote_ident(schema)}.ptg2_artifact_blob_chunk WHERE artifact_id = ANY(:artifact_ids)",
                artifact_ids=artifact_ids,
            )
        deleted_artifacts = await db.status(
            f"DELETE FROM {_quote_ident(schema)}.ptg2_artifact_manifest WHERE snapshot_id = :snapshot_id",
            snapshot_id=snapshot_id,
        )
        deleted_snapshots = await db.status(
            f"DELETE FROM {_quote_ident(schema)}.ptg2_snapshot WHERE snapshot_id = :snapshot_id",
            snapshot_id=snapshot_id,
        )
        shared_layout_release = None
        if int(deleted_v3_snapshot_bindings or 0) > 0 and layout_keys:
            shared_layout_release = await release_unbound_ptg2_shared_layouts(
                schema_name=schema,
                executor=_TransactionExecutor(session),
                require_shared=True,
                layout_keys=layout_keys,
            )
        released_shared_layouts = int(
            getattr(shared_layout_release, "logical_layout_count", 0) or 0
        )
        queued_shared_block_candidates = int(
            getattr(shared_layout_release, "candidate_hash_count", 0) or 0
        )
        queued_shared_block_bytes = int(
            getattr(shared_layout_release, "stored_bytes", 0) or 0
        )
    return {
        **plan,
        "executed": True,
        "deleted_tables": len(tables),
        "deleted_v3_snapshot_scopes": int(deleted_v3_snapshot_scopes or 0),
        "deleted_v3_snapshot_bindings": int(deleted_v3_snapshot_bindings or 0),
        "deleted_artifact_chunks": int(deleted_artifact_chunks or 0),
        "deleted_artifact_manifests": int(deleted_artifacts or 0),
        "deleted_snapshots": int(deleted_snapshots or 0),
        "released_shared_layouts": released_shared_layouts,
        "queued_shared_block_candidates": queued_shared_block_candidates,
        "queued_shared_block_bytes": queued_shared_block_bytes,
        "physical_cleanup": (
            "released"
            if released_shared_layouts
            else ("deferred" if layout_keys else "not_applicable")
        ),
    }


async def retire_ptg2_source_snapshot(
    *,
    snapshot_id: str,
    source_key: str | None = None,
) -> dict[str, Any]:
    """Retire one source-scoped PTG2 snapshot and delete its serving artifacts."""
    snapshot_id = str(snapshot_id or "").strip()
    source_key = str(source_key or "").strip() or None
    if not snapshot_id:
        raise ValueError("snapshot_id is required")
    schema = _schema_name()
    async with db.transaction() as session:
        await _lock_source_pointer_gc(session)
        snapshot = await _snapshot_row(schema, snapshot_id)
        manifest_source_key = retirement_manifest_source_key(snapshot, source_key)
        before = await _current_references(schema, snapshot_id)
        if before.get("global_slots"):
            raise ValueError("snapshot is referenced by current global pointer")
        if any(
            before.get(reference_name)
            for reference_name in (
                "previous_global_slots",
                "previous_source_keys",
                "previous_plan_source_keys",
            )
        ):
            raise ValueError("snapshot is referenced by a previous snapshot pointer")
        query_param_map: dict[str, Any] = {"snapshot_id": snapshot_id}
        source_filter = ""
        if source_key:
            query_param_map["source_key"] = source_key
            source_filter = " AND source_key = :source_key"
        deleted_plan_pointers = await db.status(
            f"""
            DELETE FROM {_quote_ident(schema)}.ptg2_current_plan_source
             WHERE snapshot_id = :snapshot_id{source_filter}
            """,
            **query_param_map,
        )
        deleted_source_pointers = await db.status(
            f"""
            DELETE FROM {_quote_ident(schema)}.ptg2_current_source_snapshot
             WHERE snapshot_id = :snapshot_id{source_filter}
            """,
            **query_param_map,
        )
        after = await _current_references(schema, snapshot_id)
    _clear_ptg2_snapshot_cache()
    return {
        "snapshot_id": snapshot_id,
        "source_key": source_key or manifest_source_key,
        "exists": bool(snapshot),
        "retired": True,
        "deleted_plan_pointers": int(deleted_plan_pointers or 0),
        "deleted_source_pointers": int(deleted_source_pointers or 0),
        "previous_current_references": before,
        "current_references": after,
    }


async def _snapshot_row(schema: str, snapshot_id: str) -> dict[str, Any]:
    rows = await db.all(
        f"""
        SELECT snapshot_id, import_month, status, manifest
          FROM {_quote_ident(schema)}.ptg2_snapshot
         WHERE snapshot_id = :snapshot_id
         LIMIT 1
        """,
        snapshot_id=snapshot_id,
    )
    return _row_mapping(rows[0]) if rows else {}


async def _current_source_snapshot_state(schema: str, source_key: str) -> tuple[str | None, str | None]:
    row = await db.first(
        f"SELECT snapshot_id, previous_snapshot_id FROM {_quote_ident(schema)}.ptg2_current_source_snapshot "
        "WHERE source_key = :source_key LIMIT 1",
        source_key=source_key,
    )
    if row is None:
        return None, None
    return (str(row[0]) if row[0] else None, str(row[1]) if row[1] else None)


async def _snapshot_reference_rows(
    schema: str,
    snapshot_id: str,
    *,
    table: str,
    selected_fields: str,
    reference_field: str = "snapshot_id",
    order_fields: str | None = None,
) -> list[Any]:
    return await db.all(
        f"SELECT {selected_fields} FROM {_quote_ident(schema)}.{table} "
        f"WHERE {reference_field} = :snapshot_id "
        f"ORDER BY {order_fields or selected_fields}",
        snapshot_id=snapshot_id,
    )


async def _current_references(schema: str, snapshot_id: str) -> dict[str, list[str]]:
    """Return every current pointer or release pin retaining a snapshot."""

    global_rows = await _snapshot_reference_rows(
        schema, snapshot_id, table="ptg2_current_snapshot", selected_fields="slot"
    )
    source_rows = await _snapshot_reference_rows(
        schema, snapshot_id, table="ptg2_current_source_snapshot", selected_fields="source_key"
    )
    plan_rows = await _snapshot_reference_rows(
        schema, snapshot_id, table="ptg2_current_plan_source", selected_fields="plan_source_key"
    )
    previous_global_rows = await _snapshot_reference_rows(
        schema, snapshot_id, table="ptg2_current_snapshot", selected_fields="slot",
        reference_field="previous_snapshot_id",
    )
    previous_source_rows = await _snapshot_reference_rows(
        schema, snapshot_id, table="ptg2_current_source_snapshot", selected_fields="source_key",
        reference_field="previous_snapshot_id",
    )
    previous_plan_rows = await _snapshot_reference_rows(
        schema, snapshot_id, table="ptg2_current_plan_source", selected_fields="plan_source_key",
        reference_field="previous_snapshot_id",
    )
    pin_rows = await _snapshot_reference_rows(
        schema, snapshot_id, table="ptg2_snapshot_pin", selected_fields="owner_type, owner_id"
    )
    return {
        "global_slots": [
            str(_row_mapping(reference_row).get("slot"))
            for reference_row in global_rows
        ],
        "source_keys": [
            str(_row_mapping(reference_row).get("source_key"))
            for reference_row in source_rows
        ],
        "plan_source_keys": [
            str(_row_mapping(reference_row).get("plan_source_key"))
            for reference_row in plan_rows
        ],
        "previous_global_slots": [
            str(_row_mapping(reference_row).get("slot"))
            for reference_row in previous_global_rows
        ],
        "previous_source_keys": [
            str(_row_mapping(reference_row).get("source_key"))
            for reference_row in previous_source_rows
        ],
        "previous_plan_source_keys": [
            str(_row_mapping(reference_row).get("plan_source_key"))
            for reference_row in previous_plan_rows
        ],
        "plan_release_pins": [
            f"{_row_mapping(reference_row).get('owner_type')}:"
            f"{_row_mapping(reference_row).get('owner_id')}"
            for reference_row in pin_rows
        ],
    }


async def _artifact_manifest_ids(schema: str, snapshot_id: str) -> list[str]:
    rows = await db.all(
        f"""
        SELECT artifact_id
          FROM {_quote_ident(schema)}.ptg2_artifact_manifest
         WHERE snapshot_id = :snapshot_id
         ORDER BY artifact_id
        """,
        snapshot_id=snapshot_id,
    )
    return [str(_row_mapping(row).get("artifact_id")) for row in rows]


def _clear_ptg2_snapshot_cache() -> None:
    try:
        from api.ptg2_snapshot import _PTG2_SNAPSHOT_RESOLVE_CACHE
    except Exception:
        return
    _PTG2_SNAPSHOT_RESOLVE_CACHE.clear()


def _text(statement: str):
    from sqlalchemy import text

    return text(statement)


async def _lock_source_pointer_gc(session: Any) -> None:
    await session.execute(
        _text("SELECT pg_advisory_xact_lock(hashtext(:publish_lock_key))"),
        {"publish_lock_key": PTG2_SOURCE_POINTER_GC_LOCK_KEY},
    )
