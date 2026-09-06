"""Physical PostgreSQL relation and partition size measurements."""

from __future__ import annotations

from typing import Any, Sequence

import asyncpg


async def relation_size_rows(
    connection: asyncpg.Connection,
    schema_name: str,
    relation_names: Sequence[str],
) -> list[dict[str, Any]]:
    """Measure relations plus every partition, including indexes."""

    relation_records = []
    for relation_name in relation_names:
        total_bytes = await connection.fetchval(
            """
            WITH RECURSIVE relation_tree(oid) AS (
                SELECT class.oid
                  FROM pg_class AS class
                  JOIN pg_namespace AS namespace
                    ON namespace.oid = class.relnamespace
                 WHERE namespace.nspname = $1 AND class.relname = $2
                UNION ALL
                SELECT inherits.inhrelid
                  FROM pg_inherits AS inherits
                  JOIN relation_tree ON relation_tree.oid = inherits.inhparent
            )
            SELECT COALESCE(SUM(pg_total_relation_size(oid)), 0)::bigint
              FROM relation_tree
            """,
            schema_name,
            relation_name,
        )
        exists = await connection.fetchval(
            "SELECT to_regclass(format('%I.%I', $1::text, $2::text)) IS NOT NULL",
            schema_name,
            relation_name,
        )
        relation_records.append(
            {
                "relation": relation_name,
                "exists": bool(exists),
                "total_bytes": int(total_bytes or 0),
            }
        )
    return relation_records
