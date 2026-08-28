# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Scope active import-run idempotency keys by importer.

Revision ID: 20260829090000_import_run_idempotency_scope
Revises: 20260828120000_hospital_price_modifier_payer_identity
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text

from db.migration_index_adoption import has_matching_index


revision = "20260829090000_import_run_idempotency_scope"
down_revision = "20260828120000_hospital_price_modifier_payer_identity"
branch_labels = None
depends_on = None


INDEX_NAME = "import_run_importer_active_idempotency_idx"
LEGACY_INDEX_NAME = "import_run_active_idempotency_idx"
TABLE_NAME = "import_run"
INDEX_COLUMNS = ("importer", "idempotency_key")
LEGACY_INDEX_COLUMNS = ("idempotency_key",)
ACTIVE_PREDICATE = (
    "status IN ('queued', 'starting', 'running', 'finalizing', 'canceling')"
)


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, identifier: str) -> str:
    return f"{_q(schema)}.{_q(identifier)}"


def _create_index_sql(
    schema: str,
    index_name: str,
    columns: tuple[str, ...],
) -> str:
    column_sql = ", ".join(_q(column) for column in columns)
    return (
        f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {_q(index_name)} "
        f"ON {_qt(schema, TABLE_NAME)} ({column_sql}) "
        f"WHERE {ACTIVE_PREDICATE}"
    )


def _drop_index_sql(schema: str, index_name: str) -> str:
    return f"DROP INDEX CONCURRENTLY IF EXISTS {_qt(schema, index_name)}"


def _matches_index(
    schema: str,
    index_name: str,
    columns: tuple[str, ...],
) -> bool:
    return has_matching_index(
        op,
        index_name,
        TABLE_NAME,
        columns,
        schema=schema,
        unique=True,
        postgresql_where=text(ACTIVE_PREDICATE),
    )


def _target_matches(
    schema: str,
    index_name: str,
    columns: tuple[str, ...],
) -> bool:
    try:
        return _matches_index(schema, index_name, columns)
    except RuntimeError as exc:
        if str(exc) != f"existing_schema_index_invalid:{schema}.{index_name}":
            raise
        return False


def _replace_index(
    target_name: str,
    target_columns: tuple[str, ...],
    obsolete_name: str,
) -> None:
    schema = _schema()
    context = op.get_context()
    if context.as_sql:
        with context.autocommit_block():
            op.execute(text(_drop_index_sql(schema, target_name)))
            op.execute(text(_create_index_sql(schema, target_name, target_columns)))
            op.execute(text(_drop_index_sql(schema, obsolete_name)))
        return

    target_matches = _target_matches(schema, target_name, target_columns)
    if not target_matches:
        with context.autocommit_block():
            bind = op.get_bind()
            bind.exec_driver_sql(_drop_index_sql(schema, target_name))
            bind.exec_driver_sql(_create_index_sql(schema, target_name, target_columns))
        if not _matches_index(schema, target_name, target_columns):
            raise RuntimeError(f"required_index_missing:{schema}.{target_name}")
    with context.autocommit_block():
        op.get_bind().exec_driver_sql(_drop_index_sql(schema, obsolete_name))


def upgrade() -> None:
    _replace_index(INDEX_NAME, INDEX_COLUMNS, LEGACY_INDEX_NAME)


def downgrade() -> None:
    _replace_index(LEGACY_INDEX_NAME, LEGACY_INDEX_COLUMNS, INDEX_NAME)
