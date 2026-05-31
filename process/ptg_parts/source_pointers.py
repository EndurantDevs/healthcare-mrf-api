# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Helpers for source-scoped PTG2 current-snapshot pointers."""

from __future__ import annotations

import datetime
import os
import sys
from typing import Any

from db.connection import db
from db.models import PTG2CurrentPlanSource, PTG2CurrentSourceSnapshot
from process.ptg_parts.canonical import semantic_hash
from process.ptg_parts.db_tables import _quote_ident, _table_exists


def _ptg2_plan_source_key(plan_id: str, plan_market_type: str | None, import_month: datetime.date) -> str:
    return semantic_hash(
        {
            "plan_id": plan_id,
            "plan_market_type": plan_market_type or "",
            "import_month": import_month.isoformat(),
        },
        domain="ptg2_current_plan_source",
    )[:32]


async def _current_source_snapshot_id(source_key: str) -> str | None:
    row = await (
        db.select(PTG2CurrentSourceSnapshot.__table__.c.snapshot_id)
        .where(PTG2CurrentSourceSnapshot.__table__.c.source_key == source_key)
        .first()
    )
    if row is None:
        return None
    return str(row[0]) if row[0] else None


async def _push_ptg2_objects_from_facade(rows: list[dict[str, Any]], cls, *, rewrite: bool = True) -> None:
    ptg_module = sys.modules.get("process.ptg")
    if ptg_module is None:
        raise RuntimeError("process.ptg facade is not loaded")
    await ptg_module._push_ptg2_objects(rows, cls, rewrite=rewrite)


async def _source_plan_rows(
    *,
    snapshot_id: str,
    source_key: str,
    import_month: datetime.date,
    previous_snapshot_id: str | None,
    updated_at: datetime.datetime,
    serving_index: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    rows = await db.all(
        f"""
        SELECT DISTINCT p.plan_id, COALESCE(p.plan_market_type, '') AS plan_market_type
          FROM {_quote_ident(schema_name)}.ptg2_plan_month pm
          JOIN {_quote_ident(schema_name)}.ptg2_plan p ON p.plan_hash = pm.plan_hash
         WHERE pm.snapshot_id = :snapshot_id
           AND p.plan_id IS NOT NULL
           AND p.plan_id <> ''
        """,
        snapshot_id=snapshot_id,
    )
    if not rows and serving_index and serving_index.get("table"):
        table_value = str(serving_index["table"])
        table_name = table_value.split(".", 1)[1] if "." in table_value else table_value
        if await _table_exists(schema_name, table_name):
            snapshot_filter = "" if serving_index.get("storage") == "manifest_snapshot" else "WHERE snapshot_id = :snapshot_id"
            plan_filter = "WHERE" if not snapshot_filter else "AND"
            rows = await db.all(
                f"""
                SELECT DISTINCT plan_id, '' AS plan_market_type
                  FROM {_quote_ident(schema_name)}.{_quote_ident(table_name)}
                 {snapshot_filter}
                 {plan_filter} plan_id IS NOT NULL
                   AND plan_id <> ''
                """,
                snapshot_id=snapshot_id,
            )
    result: list[dict[str, Any]] = []
    for row in rows:
        data = row if isinstance(row, dict) else row._mapping
        plan_id = str(data.get("plan_id") or "").strip()
        if not plan_id:
            continue
        plan_market_type = str(data.get("plan_market_type") or "").strip().lower()
        result.append(
            {
                "plan_source_key": _ptg2_plan_source_key(plan_id, plan_market_type, import_month),
                "plan_id": plan_id,
                "plan_market_type": plan_market_type,
                "import_month": import_month,
                "source_key": source_key,
                "snapshot_id": snapshot_id,
                "previous_snapshot_id": previous_snapshot_id,
                "updated_at": updated_at,
            }
        )
    return result


async def _publish_ptg2_source_pointers(
    *,
    source_key: str,
    snapshot_id: str,
    previous_snapshot_id: str | None,
    import_month: datetime.date,
    updated_at: datetime.datetime,
    serving_index: dict[str, Any] | None,
) -> None:
    await _push_ptg2_objects_from_facade(
        [
            {
                "source_key": source_key,
                "snapshot_id": snapshot_id,
                "previous_snapshot_id": previous_snapshot_id,
                "import_month": import_month,
                "updated_at": updated_at,
            }
        ],
        PTG2CurrentSourceSnapshot,
        rewrite=True,
    )
    plan_rows = await _source_plan_rows(
        snapshot_id=snapshot_id,
        source_key=source_key,
        import_month=import_month,
        previous_snapshot_id=previous_snapshot_id,
        updated_at=updated_at,
        serving_index=serving_index,
    )
    if plan_rows:
        await _push_ptg2_objects_from_facade(plan_rows, PTG2CurrentPlanSource, rewrite=True)
