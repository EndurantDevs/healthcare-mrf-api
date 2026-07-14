# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL-backed expression comparison for migration adoption."""

from __future__ import annotations

import re
from typing import Any
import uuid

import sqlalchemy as sa


def _database_connection(operations: Any) -> Any | None:
    get_database_connection = getattr(operations, "get_bind", None)
    if not callable(get_database_connection):
        return None
    database_connection = get_database_connection()
    if not isinstance(database_connection, sa.engine.Connection):
        return None
    return database_connection


def _normalized_expression(expression: Any) -> str:
    """Normalize formatting while preserving SQL grouping, casts, and literals."""
    raw_expression = "" if expression is None else str(expression)
    normalized_segments = []
    for segment_number, expression_segment in enumerate(
        re.split(r"('(?:''|[^'])*'|\"(?:\"\"|[^\"])*\")", raw_expression)
    ):
        if segment_number % 2:
            normalized_segments.append(expression_segment)
            continue
        normalized_segments.append(re.sub(r"\s+", "", expression_segment.lower()))
    return "".join(normalized_segments)


def _quoted_identifier(database_connection: Any, identifier: str) -> str:
    return database_connection.dialect.identifier_preparer.quote(identifier)


def _fallback_column_expression(expected_column: sa.Column) -> str:
    if expected_column.computed is not None:
        return str(expected_column.computed.sqltext)
    if expected_column.server_default is not None:
        return str(expected_column.server_default.arg)
    return ""


def _create_expression_table(
    database_connection: Any,
    schema: str,
    table_name: str,
) -> tuple[str, str]:
    temporary_table = f"mrf_expression_{uuid.uuid4().hex}"
    quoted_temporary_table = _quoted_identifier(database_connection, temporary_table)
    quoted_source_table = (
        f"{_quoted_identifier(database_connection, schema)}."
        f"{_quoted_identifier(database_connection, table_name)}"
    )
    database_connection.execute(
        sa.text(
            f"CREATE TEMPORARY TABLE {quoted_temporary_table} "
            f"(LIKE {quoted_source_table} INCLUDING DEFAULTS INCLUDING GENERATED) "
            "ON COMMIT DROP"
        )
    )
    return temporary_table, quoted_temporary_table


def _install_expected_column_expression(
    database_connection: Any,
    quoted_temporary_table: str,
    expected_column: sa.Column,
) -> None:
    quoted_column = _quoted_identifier(database_connection, expected_column.name)
    if expected_column.computed is not None:
        column_type = expected_column.type.compile(dialect=database_connection.dialect)
        database_connection.execute(
            sa.text(
                f"ALTER TABLE {quoted_temporary_table} DROP COLUMN {quoted_column}"
            )
        )
        database_connection.execute(
            sa.text(
                f"ALTER TABLE {quoted_temporary_table} "
                f"ADD COLUMN {quoted_column} {column_type} GENERATED ALWAYS AS "
                f"({expected_column.computed.sqltext}) STORED"
            )
        )
        return
    database_connection.execute(
        sa.text(
            f"ALTER TABLE {quoted_temporary_table} "
            f"ALTER COLUMN {quoted_column} DROP DEFAULT"
        )
    )
    database_connection.execute(
        sa.text(
            f"ALTER TABLE {quoted_temporary_table} "
            f"ALTER COLUMN {quoted_column} SET DEFAULT "
            f"{expected_column.server_default.arg}"
        )
    )


def _read_column_expression(
    database_connection: Any,
    temporary_table: str,
    column_name: str,
) -> str | None:
    return database_connection.execute(
        sa.text(
            """
            SELECT pg_get_expr(default_record.adbin, default_record.adrelid)
              FROM pg_attrdef AS default_record
              JOIN pg_attribute AS attribute_record
                ON attribute_record.attrelid = default_record.adrelid
               AND attribute_record.attnum = default_record.adnum
             WHERE default_record.adrelid = to_regclass(:table_name)
               AND attribute_record.attname = :column_name
            """
        ),
        {"table_name": temporary_table, "column_name": column_name},
    ).scalar_one_or_none()


def _canonical_column_expression(
    operations: Any,
    schema: str,
    table_name: str,
    expected_column: sa.Column,
) -> str:
    """Ask PostgreSQL to canonicalize an expected default or generated expression."""
    database_connection = _database_connection(operations)
    if database_connection is None:
        return _fallback_column_expression(expected_column)
    temporary_table, quoted_temporary_table = _create_expression_table(
        database_connection,
        schema,
        table_name,
    )
    try:
        _install_expected_column_expression(
            database_connection,
            quoted_temporary_table,
            expected_column,
        )
        canonical_expression = _read_column_expression(
            database_connection,
            temporary_table,
            expected_column.name,
        )
        if canonical_expression is None:
            raise RuntimeError(
                "migration_expected_expression_missing:"
                f"{schema}.{table_name}.{expected_column.name}"
            )
        return str(canonical_expression)
    finally:
        database_connection.execute(
            sa.text(f"DROP TABLE IF EXISTS {quoted_temporary_table}")
        )
