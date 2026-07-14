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
            text("SELECT to_regclass(:relation_name)"),
            {"relation_name": f"{schema}.{TABLE_NAME}"},
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


def upgrade() -> None:
    schema = _schema()
    offline_context = _offline_context()
    if offline_context is not None:
        with offline_context.autocommit_block():
            op.create_index(
                INDEX_NAME,
                TABLE_NAME,
                ["source_id", "mrf_file_id"],
                schema=schema,
                if_not_exists=True,
                postgresql_concurrently=True,
            )
        return
    bind = op.get_bind()
    if not _table_exists(bind, schema):
        return
    if has_matching_index(
        op,
        INDEX_NAME,
        TABLE_NAME,
        ("source_id", "mrf_file_id"),
        schema=schema,
    ):
        return
    with op.get_context().autocommit_block():
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
        op.get_bind().exec_driver_sql(
            "DROP INDEX CONCURRENTLY IF EXISTS "
            f"{_quoted_identifier(schema)}.{_quoted_identifier(INDEX_NAME)};"
        )
