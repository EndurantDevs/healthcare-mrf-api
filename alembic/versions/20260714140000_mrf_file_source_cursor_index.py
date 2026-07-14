"""Add the MRF discovery file export cursor index.

Revision ID: 20260714140000_mrf_file_source_cursor_index
Revises: 20260714130000_provider_directory_plan_index_shape_repair
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text

from db.migration_index_adoption import has_matching_index


revision = "20260714140000_mrf_file_source_cursor_index"
down_revision = "20260714130000_provider_directory_plan_index_shape_repair"
branch_labels = None
depends_on = None


INDEX_NAME = "mrf_file_source_cursor_idx"
TABLE_NAME = "mrf_file"


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _quoted_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _table_exists(bind, schema: str) -> bool:
    return bool(
        bind.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                      FROM pg_class AS table_record
                      JOIN pg_namespace AS table_namespace
                        ON table_namespace.oid = table_record.relnamespace
                     WHERE table_namespace.nspname = :schema
                       AND table_record.relname = :table_name
                       AND table_record.relkind IN ('r', 'p')
                )
                """
            ),
            {"schema": schema, "table_name": TABLE_NAME},
        ).scalar()
    )


def _offline_context():
    get_context = getattr(op, "get_context", None)
    if get_context is None:
        return None
    migration_context = get_context()
    return migration_context if migration_context.as_sql else None


def _create_index_sql(schema: str) -> str:
    return (
        f"CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        f"{_quoted_identifier(INDEX_NAME)} "
        f"ON {_quoted_identifier(schema)}.{_quoted_identifier(TABLE_NAME)} "
        f"({_quoted_identifier('source_id')}, "
        f"{_quoted_identifier('mrf_file_id')});"
    )


def _drop_index_sql(schema: str) -> str:
    return (
        "DROP INDEX CONCURRENTLY IF EXISTS "
        f"{_quoted_identifier(schema)}.{_quoted_identifier(INDEX_NAME)};"
    )


def _index_state(bind, schema: str) -> tuple[str, str, bool, bool, bool] | None:
    result = bind.execute(
        text(
            """
            SELECT table_namespace.nspname AS table_schema,
                   table_record.relname AS table_name,
                   index_meta.indisvalid,
                   index_meta.indisready,
                   index_meta.indislive
              FROM pg_class AS index_record
              JOIN pg_namespace AS index_namespace
                ON index_namespace.oid = index_record.relnamespace
              JOIN pg_index AS index_meta
                ON index_meta.indexrelid = index_record.oid
              JOIN pg_class AS table_record
                ON table_record.oid = index_meta.indrelid
              JOIN pg_namespace AS table_namespace
                ON table_namespace.oid = table_record.relnamespace
             WHERE index_namespace.nspname = :schema
               AND index_record.relname = :index_name
            """
        ),
        {"schema": schema, "index_name": INDEX_NAME},
    )
    row = result.first()
    if row is None:
        return None
    row_mapping = getattr(row, "_mapping", None)
    values = row_mapping if row_mapping is not None else row
    return (
        str(values["table_schema"] if row_mapping is not None else values[0]),
        str(values["table_name"] if row_mapping is not None else values[1]),
        bool(values["indisvalid"] if row_mapping is not None else values[2]),
        bool(values["indisready"] if row_mapping is not None else values[3]),
        bool(values["indislive"] if row_mapping is not None else values[4]),
    )


def upgrade() -> None:
    schema = _schema()
    offline_context = _offline_context()
    if offline_context is not None:
        # PostgreSQL cannot conditionally create a concurrent index in offline
        # SQL, and mrf_file is optional in fresh installations.
        return
    bind = op.get_bind()
    if not _table_exists(bind, schema):
        return
    index_state = _index_state(bind, schema)
    drop_invalid_index = False
    if index_state is not None:
        table_schema, table_name, is_valid, is_ready, is_live = index_state
        if (table_schema, table_name) != (schema, TABLE_NAME):
            raise RuntimeError(f"existing_schema_index_mismatch:{schema}.{INDEX_NAME}")
        if is_valid and is_ready and is_live:
            if has_matching_index(
                op,
                INDEX_NAME,
                TABLE_NAME,
                ("source_id", "mrf_file_id"),
                schema=schema,
            ):
                return
        else:
            drop_invalid_index = True
    with op.get_context().autocommit_block():
        if drop_invalid_index:
            bind.exec_driver_sql(_drop_index_sql(schema))
        bind.exec_driver_sql(_create_index_sql(schema))
    if not has_matching_index(
        op,
        INDEX_NAME,
        TABLE_NAME,
        ("source_id", "mrf_file_id"),
        schema=schema,
    ):
        raise RuntimeError(f"required_index_missing:{schema}.{INDEX_NAME}")


def downgrade() -> None:
    schema = _schema()
    offline_context = _offline_context()
    if offline_context is not None:
        with offline_context.autocommit_block():
            op.drop_index(
                INDEX_NAME,
                table_name=TABLE_NAME,
                schema=schema,
                if_exists=True,
                postgresql_concurrently=True,
            )
        return
    with op.get_context().autocommit_block():
        op.get_bind().exec_driver_sql(_drop_index_sql(schema))
