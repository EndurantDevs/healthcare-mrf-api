# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Targeted control helpers for source-scoped PTG2 snapshots."""

from __future__ import annotations

import datetime
import json
import os
from typing import Any

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_artifact_blobs import ensure_ptg2_artifact_blob_table
from process.ptg_parts.ptg2_shared_gc import release_unbound_ptg2_shared_layouts
from process.ptg_parts.snapshot_cleanup import (
    _is_ptg2_snapshot_in_flight,
    is_strict_ptg2_v3_shared_blocks_manifest,
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
        result = await self._session.execute(
            _text(statement) if isinstance(statement, str) else statement,
            params,
        )
        return result.all()

    async def status(self, statement: Any, **params: Any) -> int | None:
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


def _manifest_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _snapshot_remove_reasons(
    *,
    source_key: str | None,
    manifest_source_key: str | None,
    snapshot_status: str,
    references: dict[str, list[str]],
) -> list[str]:
    """Describe every current or rollback reference that prevents removal."""

    reasons: list[str] = []
    if source_key and manifest_source_key and source_key != manifest_source_key:
        reasons.append("snapshot source_key does not match requested source_key")
    if _is_ptg2_snapshot_in_flight(snapshot_status):
        reasons.append(f"snapshot is in-flight (status: {snapshot_status})")
    label_by_reference_name = {
        "global_slots": "current global",
        "source_keys": "current source",
        "plan_source_keys": "current plan",
        "previous_global_slots": "previous global",
        "previous_source_keys": "previous source",
        "previous_plan_source_keys": "previous plan",
    }
    reasons.extend(
        f"snapshot is referenced by {label} pointer"
        for reference_name, label in label_by_reference_name.items()
        if references.get(reference_name)
    )
    return reasons


def _retirement_manifest_source_key(
    snapshot: dict[str, Any],
    requested_source_key: str | None,
) -> str | None:
    """Validate a snapshot's immutable identity before retiring its pointers."""

    if not snapshot:
        return None
    manifest_map = _manifest_dict(snapshot.get("manifest"))
    serving_index_map = (
        manifest_map.get("serving_index")
        if isinstance(manifest_map.get("serving_index"), dict)
        else {}
    )
    manifest_source_key = str(serving_index_map.get("source_key") or "").strip() or None
    if not is_strict_ptg2_v3_shared_blocks_manifest(serving_index_map):
        raise ValueError(
            "only postgres_binary_v3/shared_blocks_v3 snapshots can be retired"
        )
    snapshot_status = str(snapshot.get("status") or "").strip().lower()
    if _is_ptg2_snapshot_in_flight(snapshot_status):
        raise ValueError(f"snapshot is in-flight (status: {snapshot_status})")
    if requested_source_key and manifest_source_key and requested_source_key != manifest_source_key:
        raise ValueError("snapshot source_key does not match requested source_key")
    return manifest_source_key


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
    manifest = _manifest_dict(snapshot.get("manifest"))
    serving_index = manifest.get("serving_index") if isinstance(manifest.get("serving_index"), dict) else {}
    if not is_strict_ptg2_v3_shared_blocks_manifest(serving_index):
        return {
            "snapshot_id": snapshot_id,
            "source_key": source_key,
            "exists": True,
            "removable": False,
            "reason": "only postgres_binary_v3/shared_blocks_v3 snapshots can be removed",
            "metadata_only": True,
            "tables": [],
            "artifact_manifest_ids": [],
            "current_references": {},
            "status": snapshot.get("status"),
            "import_month": str(snapshot.get("import_month") or ""),
        }
    manifest_source_key = str(serving_index.get("source_key") or "").strip() or None
    references = await _current_references(schema, snapshot_id)
    artifact_ids = await _artifact_manifest_ids(schema, snapshot_id)
    snapshot_status = str(snapshot.get("status") or "").strip().lower()
    reasons = _snapshot_remove_reasons(
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
        "status": snapshot.get("status"),
        "import_month": str(snapshot.get("import_month") or ""),
    }


async def remove_ptg2_source_snapshot(
    *,
    snapshot_id: str,
    source_key: str | None = None,
) -> dict[str, Any]:
    """Remove an unreferenced source snapshot after validating its removal plan."""
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
            }
        tables: list[str] = []
        deleted_v3_snapshot_scopes = await db.status(
            f"DELETE FROM {_quote_ident(schema)}.ptg2_v3_snapshot_scope WHERE snapshot_id = :snapshot_id",
            snapshot_id=snapshot_id,
        )
        deleted_v3_snapshot_bindings = await db.status(
            f"DELETE FROM {_quote_ident(schema)}.ptg2_v3_snapshot_binding WHERE snapshot_id = :snapshot_id",
            snapshot_id=snapshot_id,
        )
        artifact_ids = [str(value) for value in plan.get("artifact_manifest_ids") or []]
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
        if int(deleted_v3_snapshot_bindings or 0) > 0:
            shared_layout_release = await release_unbound_ptg2_shared_layouts(
                schema_name=schema,
                executor=_TransactionExecutor(session),
                require_shared=True,
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
        "released_shared_layouts": int(
            getattr(shared_layout_release, "logical_layout_count", 0) or 0
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
        manifest_source_key = _retirement_manifest_source_key(snapshot, source_key)
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


async def _current_references(schema: str, snapshot_id: str) -> dict[str, list[str]]:
    global_rows = await db.all(
        f"SELECT slot FROM {_quote_ident(schema)}.ptg2_current_snapshot WHERE snapshot_id = :snapshot_id ORDER BY slot",
        snapshot_id=snapshot_id,
    )
    source_rows = await db.all(
        f"SELECT source_key FROM {_quote_ident(schema)}.ptg2_current_source_snapshot WHERE snapshot_id = :snapshot_id ORDER BY source_key",
        snapshot_id=snapshot_id,
    )
    plan_rows = await db.all(
        f"SELECT plan_source_key FROM {_quote_ident(schema)}.ptg2_current_plan_source WHERE snapshot_id = :snapshot_id ORDER BY plan_source_key",
        snapshot_id=snapshot_id,
    )
    previous_global_rows = await db.all(
        f"SELECT slot FROM {_quote_ident(schema)}.ptg2_current_snapshot WHERE previous_snapshot_id = :snapshot_id ORDER BY slot",
        snapshot_id=snapshot_id,
    )
    previous_source_rows = await db.all(
        f"SELECT source_key FROM {_quote_ident(schema)}.ptg2_current_source_snapshot WHERE previous_snapshot_id = :snapshot_id ORDER BY source_key",
        snapshot_id=snapshot_id,
    )
    previous_plan_rows = await db.all(
        f"SELECT plan_source_key FROM {_quote_ident(schema)}.ptg2_current_plan_source WHERE previous_snapshot_id = :snapshot_id ORDER BY plan_source_key",
        snapshot_id=snapshot_id,
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
