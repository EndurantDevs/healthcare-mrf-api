# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Targeted control helpers for source-scoped PTG2 snapshots."""

from __future__ import annotations

import datetime
import json
import os
from typing import Any

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.snapshot_cleanup import _drop_ptg2_snapshot_table_names, _snapshot_manifest_table_names
from process.ptg_parts.source_pointers import _source_plan_rows


class SourceSnapshotConflict(ValueError):
    """Raised when a source snapshot pointer changed between plan and execute."""


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


def _date_value(value: Any) -> datetime.date:
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, str) and value:
        return datetime.date.fromisoformat(value[:10])
    return datetime.date.today()


async def promote_ptg2_source_snapshot(
    *,
    source_key: str,
    snapshot_id: str,
    expected_current_snapshot_id: str | None = None,
) -> dict[str, Any]:
    source_key = str(source_key or "").strip()
    snapshot_id = str(snapshot_id or "").strip()
    if not source_key or not snapshot_id:
        raise ValueError("source_key and snapshot_id are required")
    schema = _schema_name()
    snapshot = await _snapshot_row(schema, snapshot_id)
    if not snapshot:
        raise ValueError("snapshot not found")
    if str(snapshot.get("status") or "").strip().lower() != "published":
        raise ValueError("snapshot is not published")
    manifest = _manifest_dict(snapshot.get("manifest"))
    serving_index = manifest.get("serving_index") if isinstance(manifest.get("serving_index"), dict) else {}
    manifest_source_key = str(serving_index.get("source_key") or "").strip()
    if manifest_source_key and manifest_source_key != source_key:
        raise ValueError("snapshot source_key does not match requested source_key")
    previous_snapshot_id = await _current_source_snapshot(schema, source_key)
    if expected_current_snapshot_id is not None and str(expected_current_snapshot_id or "") != str(previous_snapshot_id or ""):
        raise SourceSnapshotConflict("current source snapshot changed")
    import_month = _date_value(snapshot.get("import_month"))
    updated_at = datetime.datetime.now(datetime.timezone.utc)
    plan_rows = await _source_plan_rows(
        snapshot_id=snapshot_id,
        source_key=source_key,
        import_month=import_month,
        previous_snapshot_id=previous_snapshot_id,
        updated_at=updated_at,
        serving_index=serving_index,
    )
    if not plan_rows:
        raise ValueError("snapshot has no plan source rows")
    async with db.transaction() as session:
        await session.execute(
            _text(
                f"""
                INSERT INTO {_quote_ident(schema)}.ptg2_current_source_snapshot
                    (source_key, snapshot_id, previous_snapshot_id, import_month, updated_at)
                VALUES (:source_key, :snapshot_id, :previous_snapshot_id, :import_month, :updated_at)
                ON CONFLICT (source_key) DO UPDATE SET
                    snapshot_id = EXCLUDED.snapshot_id,
                    previous_snapshot_id = EXCLUDED.previous_snapshot_id,
                    import_month = EXCLUDED.import_month,
                    updated_at = EXCLUDED.updated_at
                """
            ),
            {
                "source_key": source_key,
                "snapshot_id": snapshot_id,
                "previous_snapshot_id": previous_snapshot_id,
                "import_month": import_month,
                "updated_at": updated_at,
            },
        )
        await session.execute(
            _text(f"DELETE FROM {_quote_ident(schema)}.ptg2_current_plan_source WHERE source_key = :source_key"),
            {"source_key": source_key},
        )
        for row in plan_rows:
            await session.execute(
                _text(
                    f"""
                    INSERT INTO {_quote_ident(schema)}.ptg2_current_plan_source
                        (plan_source_key, plan_id, plan_market_type, import_month, source_key, snapshot_id, previous_snapshot_id, updated_at)
                    VALUES
                        (:plan_source_key, :plan_id, :plan_market_type, :import_month, :source_key, :snapshot_id, :previous_snapshot_id, :updated_at)
                    ON CONFLICT (plan_source_key) DO UPDATE SET
                        plan_id = EXCLUDED.plan_id,
                        plan_market_type = EXCLUDED.plan_market_type,
                        import_month = EXCLUDED.import_month,
                        source_key = EXCLUDED.source_key,
                        snapshot_id = EXCLUDED.snapshot_id,
                        previous_snapshot_id = EXCLUDED.previous_snapshot_id,
                        updated_at = EXCLUDED.updated_at
                    """
                ),
                row,
            )
    _clear_ptg2_snapshot_cache()
    return {
        "source_key": source_key,
        "snapshot_id": snapshot_id,
        "previous_snapshot_id": previous_snapshot_id,
        "plan_source_count": len(plan_rows),
    }


async def build_ptg2_source_snapshot_remove_plan(
    *,
    snapshot_id: str,
    source_key: str | None = None,
) -> dict[str, Any]:
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
    manifest_source_key = str(serving_index.get("source_key") or "").strip() or None
    references = await _current_references(schema, snapshot_id)
    tables = _snapshot_manifest_table_names(serving_index)
    artifact_ids = await _artifact_manifest_ids(schema, snapshot_id)
    reasons: list[str] = []
    if source_key and manifest_source_key and source_key != manifest_source_key:
        reasons.append("snapshot source_key does not match requested source_key")
    if references.get("global_slots"):
        reasons.append("snapshot is referenced by current global pointer")
    if references.get("source_keys"):
        reasons.append("snapshot is referenced by current source pointer")
    if references.get("plan_source_keys"):
        reasons.append("snapshot is referenced by current plan pointer")
    return {
        "snapshot_id": snapshot_id,
        "source_key": source_key or manifest_source_key,
        "exists": True,
        "removable": not reasons,
        "reason": "; ".join(reasons) if reasons else None,
        "metadata_only": False,
        "tables": tables,
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
    plan = await build_ptg2_source_snapshot_remove_plan(snapshot_id=snapshot_id, source_key=source_key)
    if not plan.get("removable"):
        raise ValueError(str(plan.get("reason") or "snapshot is not removable"))
    if not plan.get("exists"):
        return {**plan, "executed": True, "deleted_tables": 0, "deleted_artifact_manifests": 0, "deleted_snapshots": 0}
    schema = _schema_name()
    tables = [str(value) for value in plan.get("tables") or []]
    await _drop_ptg2_snapshot_table_names(tables)
    deleted_artifacts = await db.status(
        f"DELETE FROM {_quote_ident(schema)}.ptg2_artifact_manifest WHERE snapshot_id = :snapshot_id",
        snapshot_id=snapshot_id,
    )
    deleted_snapshots = await db.status(
        f"DELETE FROM {_quote_ident(schema)}.ptg2_snapshot WHERE snapshot_id = :snapshot_id",
        snapshot_id=snapshot_id,
    )
    return {
        **plan,
        "executed": True,
        "deleted_tables": len(tables),
        "deleted_artifact_manifests": int(deleted_artifacts or 0),
        "deleted_snapshots": int(deleted_snapshots or 0),
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


async def _current_source_snapshot(schema: str, source_key: str) -> str | None:
    rows = await db.all(
        f"""
        SELECT snapshot_id
          FROM {_quote_ident(schema)}.ptg2_current_source_snapshot
         WHERE source_key = :source_key
         LIMIT 1
        """,
        source_key=source_key,
    )
    if not rows:
        return None
    value = _row_mapping(rows[0]).get("snapshot_id")
    return str(value) if value else None


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
    return {
        "global_slots": [str(_row_mapping(row).get("slot")) for row in global_rows],
        "source_keys": [str(_row_mapping(row).get("source_key")) for row in source_rows],
        "plan_source_keys": [str(_row_mapping(row).get("plan_source_key")) for row in plan_rows],
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
        from api.ptg2_snapshot import _PTG2_SNAPSHOT_RESOLVE_CACHE  # pylint: disable=import-outside-toplevel,protected-access
    except Exception:
        return
    _PTG2_SNAPSHOT_RESOLVE_CACHE.clear()


def _text(statement: str):
    from sqlalchemy import text  # pylint: disable=import-outside-toplevel

    return text(statement)
