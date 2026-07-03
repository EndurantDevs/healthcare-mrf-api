# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Helpers for source-scoped PTG2 current-snapshot pointers."""

from __future__ import annotations

import datetime
import os
from typing import Any

from db.connection import db
from process.ptg_parts.canonical import semantic_hash
from process.ptg_parts.db_tables import _quote_ident, _table_exists


def _ptg2_plan_source_key(
    plan_id: str,
    plan_market_type: str | None,
    import_month: datetime.date,
    source_key: str = "",
) -> str:
    # source_key MUST be part of the pointer key: a plan served by multiple
    # network sources (e.g. a medical network plus a pharmacy carve-out) needs
    # one current-pointer row per source. Without it, whichever source
    # publishes last overwrites the others' rows via ON CONFLICT, and
    # current_source_snapshot_ids_for_plan only ever sees one network.
    payload = {
        "plan_id": plan_id,
        "plan_market_type": plan_market_type or "",
        "import_month": import_month.isoformat(),
    }
    if source_key:
        payload["source_key"] = source_key
    return semantic_hash(payload, domain="ptg2_current_plan_source")[:32]


async def _current_source_snapshot_id(source_key: str) -> str | None:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    row = await db.first(
        f"""
        SELECT snapshot_id
          FROM {_quote_ident(schema_name)}.ptg2_current_source_snapshot
         WHERE source_key = :source_key
         LIMIT 1
        """,
        source_key=source_key,
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
            if str(serving_index.get("serving_table_layout") or "").strip().lower() == "lean_provider_key_v1":
                code_count_value = str(serving_index.get("code_count_table") or "")
                code_count_table = code_count_value.split(".", 1)[1] if "." in code_count_value else code_count_value
                if code_count_table and await _table_exists(schema_name, code_count_table):
                    rows = await db.all(
                        f"""
                        SELECT DISTINCT plan_id, '' AS plan_market_type
                          FROM {_quote_ident(schema_name)}.{_quote_ident(code_count_table)}
                         WHERE plan_id IS NOT NULL
                           AND plan_id <> ''
                        """,
                        snapshot_id=snapshot_id,
                    )
            else:
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
                "plan_source_key": _ptg2_plan_source_key(
                    plan_id, plan_market_type, import_month, source_key
                ),
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
    plan_rows = await _source_plan_rows(
        snapshot_id=snapshot_id,
        source_key=source_key,
        import_month=import_month,
        previous_snapshot_id=previous_snapshot_id,
        updated_at=updated_at,
        serving_index=serving_index,
    )
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    async with db.transaction() as session:
        await session.execute(
            db.text(
                f"""
                INSERT INTO {_quote_ident(schema_name)}.ptg2_current_source_snapshot
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
            db.text(
                f"DELETE FROM {_quote_ident(schema_name)}.ptg2_current_plan_source WHERE source_key = :source_key"
            ),
            {"source_key": source_key},
        )
        for row in plan_rows:
            await session.execute(
                db.text(
                    f"""
                    INSERT INTO {_quote_ident(schema_name)}.ptg2_current_plan_source
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
