# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog and attachment evidence for legacy PTG V3 repair."""

from __future__ import annotations

from typing import Any, Mapping

from sqlalchemy import text

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_v4_attempt_registry import ATTEMPT_ATTACHMENTS
from process.ptg_parts.ptg_source_attempt_guard import canonical_digest


ALLOWED_ATTACHMENT_NAMES = frozenset(
    {
        "snapshot_scope",
        "snapshot_plan_scope",
        "snapshot_source",
        "plan_month",
    }
)


def _schema_table(schema_name: str, table_name: str) -> str:
    return f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"


async def has_relation(
    session: Any,
    schema_name: str,
    table_name: str,
) -> bool:
    """Return whether one exact relation exists in the target schema."""

    relation_check_result = await session.execute(
        text("SELECT to_regclass(:qualified_name) IS NOT NULL"),
        {"qualified_name": f"{schema_name}.{table_name}"},
    )
    return bool(relation_check_result.scalar_one())


def _attachment_where(attachment: Any) -> str:
    conditions = [
        f"{_quote_ident(column)} = :snapshot_id"
        for column in attachment.snapshot_columns
    ]
    conditions.extend(
        f"{_quote_ident(column)} = :internal_run_id"
        for column in attachment.run_columns
    )
    if not conditions:
        raise RuntimeError(
            f"attachment {attachment.name} has no attempt coordinate"
        )
    return " OR ".join(conditions)


async def _retained_rows(
    session: Any,
    *,
    table_name: str,
    where_clause: str,
    parameters_by_name: Mapping[str, Any],
) -> list[dict[str, Any]]:
    result = await session.execute(
        text(
            f"SELECT to_jsonb(attached_row) AS row_payload, "
            "xmin::text AS row_xmin "
            f"FROM {table_name} AS attached_row "
            f"WHERE {where_clause} "
            "ORDER BY to_jsonb(attached_row)::text"
        ),
        dict(parameters_by_name),
    )
    return [
        {"row": row.row_payload, "xmin": row.row_xmin}
        for row in result.all()
    ]


async def _catalog_row(
    session: Any,
    *,
    schema_name: str,
    attachment: Any,
) -> dict[str, Any] | None:
    catalog_query = await session.execute(
        text(
            """
            SELECT relation_record.oid::bigint AS relation_oid,
                   relation_record.relkind,
                   relation_record.relpersistence,
                   relation_record.relowner::bigint AS owner_oid,
                   COALESCE(
                       jsonb_agg(
                           jsonb_build_array(
                               attribute_record.attnum,
                               attribute_record.attname,
                               attribute_record.atttypid::bigint,
                               attribute_record.atttypmod,
                               attribute_record.attnotnull
                           ) ORDER BY attribute_record.attnum
                       ) FILTER (WHERE attribute_record.attnum IS NOT NULL),
                       '[]'::jsonb
                   ) AS columns
              FROM pg_class AS relation_record
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = relation_record.relnamespace
              LEFT JOIN pg_attribute AS attribute_record
                ON attribute_record.attrelid = relation_record.oid
               AND attribute_record.attnum > 0
               AND NOT attribute_record.attisdropped
             WHERE namespace_record.nspname = :schema_name
               AND relation_record.relname = :table_name
             GROUP BY relation_record.oid,
                      relation_record.relkind,
                      relation_record.relpersistence,
                      relation_record.relowner
            """
        ),
        {"schema_name": schema_name, "table_name": attachment.table_name},
    )
    catalog_record = catalog_query.mappings().one_or_none()
    if catalog_record is None:
        return None
    return {
        "attachment": attachment.name,
        "table": attachment.table_name,
        **dict(catalog_record),
    }


async def _one_attachment_evidence(
    session: Any,
    *,
    schema_name: str,
    attachment: Any,
    parameters_by_name: Mapping[str, Any],
) -> tuple[int, list[dict[str, Any]] | None, dict[str, Any] | None]:
    if not await has_relation(
        session,
        schema_name,
        attachment.table_name,
    ):
        missing_count = 0 if attachment.optional_relation else -1
        return missing_count, None, None
    table_name = _schema_table(schema_name, attachment.table_name)
    where_clause = _attachment_where(attachment)
    count_query = await session.execute(
        text(f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}"),
        dict(parameters_by_name),
    )
    retained_rows = None
    if attachment.name in ALLOWED_ATTACHMENT_NAMES:
        retained_rows = await _retained_rows(
            session,
            table_name=table_name,
            where_clause=where_clause,
            parameters_by_name=parameters_by_name,
        )
    catalog_row = await _catalog_row(
        session,
        schema_name=schema_name,
        attachment=attachment,
    )
    return int(count_query.scalar_one()), retained_rows, catalog_row


async def load_attachment_evidence(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    internal_run_id: str,
) -> tuple[dict[str, int], dict[str, Any], str]:
    """Load counts, retained rows, and relation catalog digest."""

    count_by_name: dict[str, int] = {}
    retained_rows_by_name: dict[str, Any] = {}
    catalog_rows: list[dict[str, Any]] = []
    parameters_by_name = {
        "snapshot_id": snapshot_id,
        "internal_run_id": internal_run_id,
    }
    for attachment in ATTEMPT_ATTACHMENTS:
        count, retained_rows, catalog_row = await _one_attachment_evidence(
            session,
            schema_name=schema_name,
            attachment=attachment,
            parameters_by_name=parameters_by_name,
        )
        count_by_name[attachment.name] = count
        if retained_rows is not None:
            retained_rows_by_name[attachment.name] = retained_rows
        if catalog_row is not None:
            catalog_rows.append(catalog_row)
    return (
        count_by_name,
        retained_rows_by_name,
        canonical_digest(catalog_rows),
    )


async def load_dynamic_relation_evidence(
    session: Any,
    *,
    schema_name: str,
    internal_run_id: str,
) -> dict[str, Any]:
    """Prove no run-suffixed dynamic relation remains."""

    suffix = (
        internal_run_id.removeprefix("ptg2:")
        if internal_run_id.startswith("ptg2:")
        else ""
    )
    if len(suffix) != 32 or any(
        character not in "0123456789abcdef" for character in suffix
    ):
        return {"suffix_valid": False, "relation_count": -1, "digest": ""}
    relation_query = await session.execute(
        text(
            """
            SELECT relation_record.relname,
                   relation_record.oid::bigint AS relation_oid,
                   relation_record.relkind,
                   relation_record.relpersistence
              FROM pg_class AS relation_record
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = relation_record.relnamespace
             WHERE namespace_record.nspname = :schema_name
               AND relation_record.relname ~ :suffix_pattern
             ORDER BY relation_record.relname
            """
        ),
        {
            "schema_name": schema_name,
            "suffix_pattern": f"_{suffix}(_|$)",
        },
    )
    relation_rows = [
        dict(relation_record)
        for relation_record in relation_query.mappings().all()
    ]
    return {
        "suffix_valid": True,
        "relation_count": len(relation_rows),
        "digest": canonical_digest(relation_rows),
    }


__all__ = [
    "ALLOWED_ATTACHMENT_NAMES",
    "load_attachment_evidence",
    "load_dynamic_relation_evidence",
    "has_relation",
]
