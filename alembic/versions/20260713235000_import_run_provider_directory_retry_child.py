"""Enforce one direct Provider Directory retry child per run.

Revision ID: 20260713235000_import_run_provider_directory_retry_child
Revises: 20260713234000_provider_directory_plan_lookup_index
"""

from __future__ import annotations

import os

import sqlalchemy as sa
from alembic import op

from db.migration_index_adoption import create_index_if_missing


revision = "20260713235000_import_run_provider_directory_retry_child"
down_revision = "20260713234000_provider_directory_plan_lookup_index"
branch_labels = None
depends_on = None


INDEX_NAME = "import_run_provider_directory_retry_child_idx"
TABLE_NAME = "import_run"
INDEX_PREDICATE = "importer = 'provider-directory-fhir' AND retry_of_run_id IS NOT NULL"


def _schema() -> str:
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _table_exists(bind, schema: str) -> bool:
    return bool(
        bind.execute(
            sa.text("SELECT to_regclass(:name)"),
            {"name": f"{schema}.{TABLE_NAME}"},
        ).scalar()
    )


def _duplicate_retry_children(bind, schema: str) -> list[dict[str, object]]:
    result = bind.execute(
        sa.text(
            f"""
            SELECT retry_of_run_id, count(*) AS child_count
            FROM {_qt(schema, TABLE_NAME)}
            WHERE {INDEX_PREDICATE}
            GROUP BY retry_of_run_id
            HAVING count(*) > 1
            ORDER BY retry_of_run_id
            LIMIT 20
            """
        )
    )
    return [dict(row) for row in result.mappings().all()]


def _is_offline_mode() -> bool:
    get_context = getattr(op, "get_context", None)
    return bool(get_context and get_context().as_sql)


def upgrade():
    schema = _schema()
    if _is_offline_mode():
        op.create_index(
            INDEX_NAME,
            TABLE_NAME,
            ["retry_of_run_id"],
            schema=schema,
            unique=True,
            if_not_exists=True,
            postgresql_where=sa.text(INDEX_PREDICATE),
        )
        return
    bind = op.get_bind()
    if not _table_exists(bind, schema):
        return
    duplicates = _duplicate_retry_children(bind, schema)
    if duplicates:
        details = ", ".join(
            f"{row['retry_of_run_id']} ({row['child_count']} children)"
            for row in duplicates
        )
        raise RuntimeError(
            "Cannot create import_run Provider Directory retry-child unique index; "
            f"duplicate direct children exist: {details}. "
            "Resolve the duplicates explicitly before rerunning this migration; no data was deleted."
        )
    create_index_if_missing(
        op,
        INDEX_NAME,
        TABLE_NAME,
        ["retry_of_run_id"],
        schema=schema,
        unique=True,
        postgresql_where=sa.text(INDEX_PREDICATE),
    )


def downgrade():
    schema = _schema()
    if _is_offline_mode():
        op.drop_index(
            INDEX_NAME,
            table_name=TABLE_NAME,
            schema=schema,
            if_exists=True,
        )
        return
    if not _table_exists(op.get_bind(), schema):
        return
    op.drop_index(INDEX_NAME, table_name=TABLE_NAME, schema=schema)
