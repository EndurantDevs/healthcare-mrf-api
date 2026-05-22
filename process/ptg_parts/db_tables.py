# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Database table helpers shared by PTG import and publish paths."""

from __future__ import annotations

from db.connection import db


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


async def _table_exists(schema_name: str, table_name: str) -> bool:
    rows = await db.all(
        """
        SELECT EXISTS (
            SELECT 1
              FROM information_schema.tables
             WHERE table_schema = :schema_name
               AND table_name = :table_name
        ) AS table_exists
        """,
        schema_name=schema_name,
        table_name=table_name,
    )
    if not rows:
        return False
    row = rows[0]
    return bool(row.get("table_exists") if isinstance(row, dict) else getattr(row, "table_exists", False))


async def _table_has_rows(schema_name: str, table_name: str) -> bool:
    rows = await db.all(
        f"""
        SELECT EXISTS (
            SELECT 1
              FROM {_quote_ident(schema_name)}.{_quote_ident(table_name)}
             LIMIT 1
        ) AS has_rows
        """
    )
    if not rows:
        return False
    row = rows[0]
    return bool(row.get("has_rows") if isinstance(row, dict) else getattr(row, "has_rows", False))


async def _estimated_table_rows(schema_name: str, table_name: str) -> int:
    rows = await db.all(
        """
        SELECT GREATEST(c.reltuples, 0)::bigint AS row_estimate
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = :schema_name
           AND c.relname = :table_name
         LIMIT 1
        """,
        schema_name=schema_name,
        table_name=table_name,
    )
    if not rows:
        return 0
    row = rows[0]
    return int(row.get("row_estimate") if isinstance(row, dict) else getattr(row, "row_estimate", 0) or 0)


async def _exact_table_rows(schema_name: str, table_name: str) -> int:
    return int(
        await db.scalar(
            f"SELECT COUNT(*) FROM {_quote_ident(schema_name)}.{_quote_ident(table_name)}"
        )
        or 0
    )
