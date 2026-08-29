# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare importer-scoped active import-run idempotency.

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


def _replaceable_index_matches(
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


def _ensure_index(
    schema: str,
    index_name: str,
    columns: tuple[str, ...],
) -> None:
    if _replaceable_index_matches(
        schema,
        index_name,
        columns,
    ):
        return
    with op.get_context().autocommit_block():
        bind = op.get_bind()
        bind.exec_driver_sql(_drop_index_sql(schema, index_name))
        bind.exec_driver_sql(_create_index_sql(schema, index_name, columns))
    if not _matches_index(schema, index_name, columns):
        raise RuntimeError(f"required_index_missing:{schema}.{index_name}")


def _prepare_indexes() -> None:
    schema = _schema()
    context = op.get_context()
    if context.as_sql:
        with context.autocommit_block():
            op.execute(
                text(
                    _create_index_sql(
                        schema,
                        LEGACY_INDEX_NAME,
                        LEGACY_INDEX_COLUMNS,
                    )
                )
            )
            op.execute(text(_drop_index_sql(schema, INDEX_NAME)))
            op.execute(text(_create_index_sql(schema, INDEX_NAME, INDEX_COLUMNS)))
        return

    _ensure_index(
        schema,
        LEGACY_INDEX_NAME,
        LEGACY_INDEX_COLUMNS,
    )
    _ensure_index(
        schema,
        INDEX_NAME,
        INDEX_COLUMNS,
    )


def upgrade() -> None:
    _prepare_indexes()


def downgrade() -> None:
    schema = _schema()
    context = op.get_context()
    if context.as_sql:
        with context.autocommit_block():
            op.execute(
                text(
                    _create_index_sql(
                        schema,
                        LEGACY_INDEX_NAME,
                        LEGACY_INDEX_COLUMNS,
                    )
                )
            )
        return
    _ensure_index(
        schema,
        LEGACY_INDEX_NAME,
        LEGACY_INDEX_COLUMNS,
    )
    with context.autocommit_block():
        op.get_bind().exec_driver_sql(_drop_index_sql(schema, INDEX_NAME))
