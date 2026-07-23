"""Low-level read-only PostgreSQL storage queries for the PTG V4 canary."""

from __future__ import annotations

from typing import Any

import asyncpg


SNAPSHOT_ID_RELATIONS = frozenset(
    {
        "ptg2_v3_snapshot_scope",
        "ptg2_v3_snapshot_plan_scope",
        "ptg2_v3_snapshot_source",
    }
)
UNOWNED_RELATIONS = frozenset({"ptg2_v3_gc_candidate"})


async def owned_row_bytes(
    connection: asyncpg.Connection,
    schema_name: str,
    relation_name: str,
    snapshot_id: str,
    snapshot_key: int,
) -> dict[str, int]:
    """Measure global and snapshot-owned tuple bytes for an ordinary relation."""

    target_filter, owned_filter, ownership_value = _ownership_predicates(
        relation_name,
        snapshot_id,
        snapshot_key,
    )
    schema = quote_identifier(schema_name)
    relation = quote_identifier(relation_name)
    byte_record = await connection.fetchrow(
        f"""
        SELECT COALESCE(SUM(pg_column_size(item)), 0)::bigint AS global_row_bytes,
               COALESCE(SUM(pg_column_size(item))
                 FILTER (WHERE {target_filter}), 0)::bigint
                 AS snapshot_weighted_row_bytes,
               COUNT(*) FILTER (WHERE {target_filter})::bigint
                 AS snapshot_row_count,
               COALESCE(SUM(pg_column_size(item))
                 FILTER (WHERE {owned_filter}), 0)::bigint
                 AS allocated_all_snapshot_row_bytes,
               COALESCE(SUM(pg_column_size(item))
                 FILTER (WHERE NOT ({owned_filter})), 0)::bigint
                 AS unallocated_row_bytes
          FROM {schema}.{relation} AS item
        """,
        ownership_value,
    )
    bytes_by_field = {
        field_name: int(byte_count or 0)
        for field_name, byte_count in dict(byte_record).items()
    }
    bytes_by_field["row_reconciliation_delta_bytes"] = (
        bytes_by_field["global_row_bytes"]
        - bytes_by_field["allocated_all_snapshot_row_bytes"]
        - bytes_by_field["unallocated_row_bytes"]
    )
    return bytes_by_field


def _ownership_predicates(
    relation_name: str,
    snapshot_id: str,
    snapshot_key: int,
) -> tuple[str, str, Any]:
    """Return safe SQL ownership predicates and the matching bind value."""

    if relation_name in UNOWNED_RELATIONS:
        return "FALSE AND $1::text IS NULL", "FALSE", None
    if relation_name in SNAPSHOT_ID_RELATIONS:
        ownership_column = "snapshot_id"
        ownership_value: Any = snapshot_id
        cast_name = "text"
    else:
        ownership_column = "snapshot_key"
        ownership_value = snapshot_key
        cast_name = "bigint"
    quoted_column = quote_identifier(ownership_column)
    return (
        f"{quoted_column} = $1::{cast_name}",
        f"{quoted_column} IS NOT NULL",
        ownership_value,
    )


def quote_identifier(identifier: str) -> str:
    """Quote one PostgreSQL identifier after higher-level allowlist validation."""

    return '"' + str(identifier).replace('"', '""') + '"'
