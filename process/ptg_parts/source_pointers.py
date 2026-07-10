# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Helpers for source-scoped PTG2 current-snapshot pointers."""

from __future__ import annotations

import datetime
import json
import os
from typing import Any

from db.connection import db
from process.ptg_parts.canonical import semantic_hash
from process.ptg_parts.db_tables import _quote_ident, _table_exists


PTG2_SOURCE_POINTER_GC_LOCK_KEY = "ptg2_source_pointer_gc_v1"

_GLOBAL_SNAPSHOT_POINTER_RECONCILIATION_SQL = """
WITH publish_lock AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(hashtext(:publish_lock_key))
), candidate_snapshot AS MATERIALIZED (
    SELECT candidate.snapshot_id, candidate.published_at
      FROM __SCHEMA__.ptg2_snapshot AS candidate,
           publish_lock
     WHERE candidate.snapshot_id = :snapshot_id
       AND candidate.status = 'published'
), updated_global_pointer AS (
    INSERT INTO __SCHEMA__.ptg2_current_snapshot AS current_pointer
        (slot, snapshot_id, previous_snapshot_id, updated_at)
    SELECT 'current', candidate_snapshot.snapshot_id, NULL, :updated_at
      FROM candidate_snapshot
    ON CONFLICT (slot) DO UPDATE SET
        snapshot_id = EXCLUDED.snapshot_id,
        previous_snapshot_id = CASE
            WHEN current_pointer.snapshot_id = EXCLUDED.snapshot_id
            THEN current_pointer.previous_snapshot_id
            ELSE current_pointer.snapshot_id
        END,
        updated_at = EXCLUDED.updated_at
    WHERE current_pointer.snapshot_id = EXCLUDED.snapshot_id
       OR NOT EXISTS (
            SELECT 1
              FROM __SCHEMA__.ptg2_snapshot AS incumbent
             WHERE incumbent.snapshot_id = current_pointer.snapshot_id
               AND incumbent.status = 'published'
               AND incumbent.published_at >= (
                    SELECT published_at FROM candidate_snapshot
               )
       )
    RETURNING snapshot_id
)
SELECT snapshot_id FROM updated_global_pointer
UNION ALL
SELECT current_pointer.snapshot_id
  FROM __SCHEMA__.ptg2_current_snapshot AS current_pointer,
       candidate_snapshot
 WHERE current_pointer.slot = 'current'
   AND NOT EXISTS (SELECT 1 FROM updated_global_pointer)
LIMIT 1
"""


class PTG2SourcePointerConflict(RuntimeError):
    """Raised when a source pointer changed after an import observed it."""


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
    if not rows:
        rows = await _source_plan_rows_from_import_catalog(snapshot_id=snapshot_id, source_key=source_key)
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


async def _source_plan_rows_from_import_catalog(*, snapshot_id: str, source_key: str) -> list[Any]:
    """Recover source-plan pointers for compact snapshots whose rate rows carry no plan id."""
    catalog_schema = "hp_import_control"
    if not (
        await _table_exists(catalog_schema, "source_file_import")
        and await _table_exists(catalog_schema, "discovered_plan_file")
        and await _table_exists(catalog_schema, "discovered_plan")
    ):
        return []
    return await db.all(
        f"""
        SELECT DISTINCT dp.plan_id, lower(COALESCE(dp.market_type, '')) AS plan_market_type
          FROM {_quote_ident(catalog_schema)}.source_file_import sfi
          JOIN {_quote_ident(catalog_schema)}.discovered_plan_file dpf
            ON dpf.source_file_id = sfi.source_file_id
           AND dpf.content_version = sfi.content_version
          JOIN {_quote_ident(catalog_schema)}.discovered_plan dp
            ON dp.discovered_plan_id = dpf.discovered_plan_id
         WHERE sfi.snapshot_id = :snapshot_id
           AND sfi.source_key = :source_key
           AND sfi.status = 'succeeded'
           AND dp.plan_id IS NOT NULL
           AND dp.plan_id <> ''
           AND dp.status = 'active'
        """,
        snapshot_id=snapshot_id,
        source_key=source_key,
    )


def _has_result_row(query_result: Any) -> bool:
    if query_result is None:
        return True
    return query_result.first() is not None


async def _acquire_source_pointer_gc_lock(session: Any) -> None:
    await session.execute(
        db.text("SELECT pg_advisory_xact_lock(hashtext(:publish_lock_key))"),
        {"publish_lock_key": PTG2_SOURCE_POINTER_GC_LOCK_KEY},
    )


async def _compare_and_swap_source_pointer(
    session: Any,
    *,
    schema_name: str,
    source_key: str,
    snapshot_id: str,
    previous_snapshot_id: str | None,
    import_month: datetime.date,
    updated_at: datetime.datetime,
) -> None:
    cas_query_result = await session.execute(
        db.text(
            f"""
            WITH publish_lock AS MATERIALIZED (
                SELECT pg_advisory_xact_lock(hashtext(:publish_lock_key))
            ), updated_pointer AS (
                UPDATE {_quote_ident(schema_name)}.ptg2_current_source_snapshot AS current_pointer
                   SET snapshot_id = :snapshot_id,
                       previous_snapshot_id = :previous_snapshot_id,
                       import_month = :import_month,
                       updated_at = :updated_at
                  FROM publish_lock
                 WHERE current_pointer.source_key = :source_key
                   AND (
                        current_pointer.snapshot_id IS NOT DISTINCT FROM :previous_snapshot_id
                        OR current_pointer.snapshot_id = :snapshot_id
                   )
                RETURNING current_pointer.snapshot_id
            ), inserted_pointer AS (
                INSERT INTO {_quote_ident(schema_name)}.ptg2_current_source_snapshot
                    (source_key, snapshot_id, previous_snapshot_id, import_month, updated_at)
                SELECT :source_key, :snapshot_id, :previous_snapshot_id, :import_month, :updated_at
                  FROM publish_lock
                 WHERE :previous_snapshot_id IS NULL
                ON CONFLICT (source_key) DO NOTHING
                RETURNING snapshot_id
            )
            SELECT snapshot_id FROM updated_pointer
            UNION ALL
            SELECT snapshot_id FROM inserted_pointer
            LIMIT 1
            """
        ),
        {
            "publish_lock_key": PTG2_SOURCE_POINTER_GC_LOCK_KEY,
            "source_key": source_key,
            "snapshot_id": snapshot_id,
            "previous_snapshot_id": previous_snapshot_id,
            "import_month": import_month,
            "updated_at": updated_at,
        },
    )
    if not _has_result_row(cas_query_result):
        raise PTG2SourcePointerConflict(
            f"PTG source pointer changed after import planning for {source_key}; "
            f"expected {previous_snapshot_id or '<none>'}"
        )


async def _publish_snapshot_in_pointer_transaction(
    session: Any,
    *,
    schema_name: str,
    snapshot_attributes: dict[str, Any] | None,
) -> None:
    if snapshot_attributes is None:
        return
    if snapshot_attributes.get("status") != "published":
        raise ValueError("Atomic source-pointer promotion requires a published snapshot row")
    publication_query_result = await session.execute(
        db.text(
            f"""
            WITH updated_snapshot AS (
                UPDATE {_quote_ident(schema_name)}.ptg2_snapshot
                   SET import_run_id = :import_run_id,
                       import_month = :import_month,
                       status = :status,
                       created_at = :created_at,
                       validated_at = :validated_at,
                       published_at = :published_at,
                       previous_snapshot_id = :previous_snapshot_id,
                       manifest = CAST(:manifest_json AS json)
                 WHERE snapshot_id = :snapshot_id
                   AND status IS DISTINCT FROM 'published'
                RETURNING status
            )
            SELECT status FROM updated_snapshot
            UNION ALL
            SELECT status
              FROM {_quote_ident(schema_name)}.ptg2_snapshot
             WHERE snapshot_id = :snapshot_id
               AND NOT EXISTS (SELECT 1 FROM updated_snapshot)
            LIMIT 1
            """
        ),
        {
            **snapshot_attributes,
            "manifest_json": json.dumps(
                snapshot_attributes.get("manifest") or {},
                default=str,
            ),
        },
    )
    if not _has_result_row(publication_query_result):
        raise RuntimeError(
            f"PTG snapshot {snapshot_attributes.get('snapshot_id')} disappeared "
            "during source-pointer promotion"
        )


async def _reconcile_global_snapshot_pointer(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    updated_at: datetime.datetime,
) -> None:
    """Advance a stale global pointer without replacing a newer publication."""
    reconciliation_sql = _GLOBAL_SNAPSHOT_POINTER_RECONCILIATION_SQL.replace(
        "__SCHEMA__",
        _quote_ident(schema_name),
    )
    reconciliation_result = await session.execute(
        db.text(reconciliation_sql),
        {
            "publish_lock_key": PTG2_SOURCE_POINTER_GC_LOCK_KEY,
            "snapshot_id": snapshot_id,
            "updated_at": updated_at,
        },
    )
    if not _has_result_row(reconciliation_result):
        raise RuntimeError(
            f"PTG snapshot {snapshot_id} was not available for global pointer reconciliation"
        )


async def _replace_source_plan_pointers(
    session: Any,
    *,
    schema_name: str,
    source_key: str,
    plan_pointer_entries: list[dict[str, Any]],
) -> None:
    await session.execute(
        db.text(
            f"DELETE FROM {_quote_ident(schema_name)}.ptg2_current_plan_source WHERE source_key = :source_key"
        ),
        {"source_key": source_key},
    )
    for plan_pointer_entry in plan_pointer_entries:
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
            plan_pointer_entry,
        )


async def _publish_ptg2_source_pointers(
    *,
    source_key: str,
    snapshot_id: str,
    previous_snapshot_id: str | None,
    import_month: datetime.date,
    updated_at: datetime.datetime,
    serving_index: dict[str, Any] | None,
    snapshot_attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    async with db.transaction() as session:
        await _acquire_source_pointer_gc_lock(session)
        plan_pointer_entries = await _source_plan_rows(
            snapshot_id=snapshot_id,
            source_key=source_key,
            import_month=import_month,
            previous_snapshot_id=previous_snapshot_id,
            updated_at=updated_at,
            serving_index=serving_index,
        )
        await _compare_and_swap_source_pointer(
            session,
            schema_name=schema_name,
            source_key=source_key,
            snapshot_id=snapshot_id,
            previous_snapshot_id=previous_snapshot_id,
            import_month=import_month,
            updated_at=updated_at,
        )
        await _publish_snapshot_in_pointer_transaction(
            session,
            schema_name=schema_name,
            snapshot_attributes=snapshot_attributes,
        )
        if snapshot_attributes is not None:
            await _reconcile_global_snapshot_pointer(
                session,
                schema_name=schema_name,
                snapshot_id=snapshot_id,
                updated_at=updated_at,
            )
        await _replace_source_plan_pointers(
            session,
            schema_name=schema_name,
            source_key=source_key,
            plan_pointer_entries=plan_pointer_entries,
        )
    return {
        "status": "promoted",
        "source_key": source_key,
        "snapshot_id": snapshot_id,
        "previous_snapshot_id": previous_snapshot_id,
        "global_pointer": "reconciled" if snapshot_attributes is not None else "not_requested",
    }


async def _publish_ptg2_global_snapshot_pointer(
    *,
    snapshot_attributes: dict[str, Any],
    updated_at: datetime.datetime,
) -> dict[str, Any]:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    snapshot_id = str(snapshot_attributes.get("snapshot_id") or "").strip()
    if not snapshot_id:
        raise ValueError("Global snapshot-pointer promotion requires a snapshot id")
    async with db.transaction() as session:
        await _acquire_source_pointer_gc_lock(session)
        await _publish_snapshot_in_pointer_transaction(
            session,
            schema_name=schema_name,
            snapshot_attributes=snapshot_attributes,
        )
        await _reconcile_global_snapshot_pointer(
            session,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            updated_at=updated_at,
        )
    return {
        "status": "promoted",
        "snapshot_id": snapshot_id,
        "global_pointer": "reconciled",
    }
