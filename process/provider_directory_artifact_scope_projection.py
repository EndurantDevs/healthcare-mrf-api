# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Typed column expressions for Provider Directory artifact scope projections."""

from __future__ import annotations

from typing import Any

from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.sqltypes import JSON as SQLAlchemyJSON

PARSED_PAYLOAD_SQL = "parsed.payload_jsonb"
# Parse each payload once: OFFSET 0 stops the planner inlining the jsonb cast into every column.
PARSED_PAYLOAD_JOIN = "JOIN LATERAL (SELECT resource.payload_json::jsonb AS payload_jsonb OFFSET 0) AS parsed ON TRUE"


def payload_column_sql(column: Any, payload_sql: str = "resource.payload_json::jsonb") -> str:
    """Return one typed scope column read from the selected dataset resource."""
    column_name = column.name
    if column_name == "source_id":
        return "selected.source_id"
    if column_name == "resource_id":
        return "resource.resource_id"
    if column_name == "last_seen_run_id":
        return "selected.evidence_run_id"
    if column_name in {"observed_at", "updated_at"}:
        return "COALESCE(dataset.published_at, dataset.validated_at, dataset.created_at)"
    json_key = "'" + str(column_name).replace("'", "''") + "'"
    if isinstance(column.type, SQLAlchemyJSON):
        return f"CAST({payload_sql} -> {json_key} AS json)"
    column_type = column.type.compile(dialect=postgresql.dialect())
    return f"CAST({payload_sql} ->> {json_key} AS {column_type})"
