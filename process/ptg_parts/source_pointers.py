# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Helpers for source-scoped PTG2 current-snapshot pointers."""

from __future__ import annotations

import datetime
import os
from typing import Any

from db.connection import db
from db.models import PTG2CurrentSourceSnapshot
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
            rows = await db.all(
                f"""
                SELECT DISTINCT plan_id, '' AS plan_market_type
                  FROM {_quote_ident(schema_name)}.{_quote_ident(table_name)}
                 WHERE snapshot_id = :snapshot_id
                   AND plan_id IS NOT NULL
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
