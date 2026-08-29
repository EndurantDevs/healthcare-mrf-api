# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Activate importer-scoped active import-run idempotency.

Revision ID: 20260829100000_activate_import_run_idempotency_scope
Revises: 20260829090000_import_run_idempotency_scope
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text

from db.migration_index_adoption import has_matching_index


revision = "20260829100000_activate_import_run_idempotency_scope"
down_revision = "20260829090000_import_run_idempotency_scope"
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


def _create_legacy_index_sql(schema: str) -> str:
    return (
        f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {_q(LEGACY_INDEX_NAME)} "
        f"ON {_qt(schema, TABLE_NAME)} ({_q(LEGACY_INDEX_COLUMNS[0])}) "
        f"WHERE {ACTIVE_PREDICATE}"
    )


def _drop_legacy_index_sql(schema: str) -> str:
    return f"DROP INDEX CONCURRENTLY IF EXISTS {_qt(schema, LEGACY_INDEX_NAME)}"


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


def _named_index_state(
    schema: str,
    index_name: str,
) -> tuple[str | None, str | None, bool, bool, bool] | None:
    row = (
        op.get_bind()
        .execute(
            text(
                """
                SELECT table_namespace.nspname AS table_schema,
                       table_record.relname AS table_name,
                       index_state.indisvalid,
                       index_state.indisready,
                       index_state.indislive
                  FROM pg_class AS named_record
                  JOIN pg_namespace AS named_namespace
                    ON named_namespace.oid = named_record.relnamespace
                  LEFT JOIN pg_index AS index_state
                    ON index_state.indexrelid = named_record.oid
                  LEFT JOIN pg_class AS table_record
                    ON table_record.oid = index_state.indrelid
                  LEFT JOIN pg_namespace AS table_namespace
                    ON table_namespace.oid = table_record.relnamespace
                 WHERE named_namespace.nspname = :schema
                   AND named_record.relname = :index_name
                """
            ),
            {"schema": schema, "index_name": index_name},
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    return (
        row["table_schema"],
        row["table_name"],
        bool(row["indisvalid"]),
        bool(row["indisready"]),
        bool(row["indislive"]),
    )


def _require_target_index(
    schema: str,
    index_name: str,
) -> tuple[bool, bool, bool] | None:
    state = _named_index_state(schema, index_name)
    if state is None:
        return None
    table_schema, table_name, is_valid, is_ready, is_live = state
    if (table_schema, table_name) != (schema, TABLE_NAME):
        raise RuntimeError(f"existing_schema_index_mismatch:{schema}.{index_name}")
    return is_valid, is_ready, is_live


def _require_index(
    schema: str,
    index_name: str,
    columns: tuple[str, ...],
) -> None:
    state = _require_target_index(schema, index_name)
    if state is None or not all(state) or not _matches_index(
        schema,
        index_name,
        columns,
    ):
        raise RuntimeError(f"required_index_missing:{schema}.{index_name}")


def _legacy_index_needs_rebuild(schema: str) -> bool:
    state = _require_target_index(schema, LEGACY_INDEX_NAME)
    if state is None or not all(state):
        return True
    return not _matches_index(
        schema,
        LEGACY_INDEX_NAME,
        LEGACY_INDEX_COLUMNS,
    )


def _create_legacy_index(schema: str) -> None:
    context = op.get_context()
    try:
        with context.autocommit_block():
            op.get_bind().exec_driver_sql(_create_legacy_index_sql(schema))
    except Exception:
        state = _require_target_index(schema, LEGACY_INDEX_NAME)
        if state is not None and not all(state):
            with context.autocommit_block():
                op.get_bind().exec_driver_sql(_drop_legacy_index_sql(schema))
        raise


def upgrade() -> None:
    schema = _schema()
    context = op.get_context()
    if context.as_sql:
        raise RuntimeError("offline_activation_requires_live_index_validation")

    _require_index(schema, INDEX_NAME, INDEX_COLUMNS)
    legacy_state = _require_target_index(schema, LEGACY_INDEX_NAME)
    if legacy_state is None:
        return
    if all(legacy_state):
        _matches_index(schema, LEGACY_INDEX_NAME, LEGACY_INDEX_COLUMNS)
    with context.autocommit_block():
        op.get_bind().exec_driver_sql(_drop_legacy_index_sql(schema))


def downgrade() -> None:
    schema = _schema()
    context = op.get_context()
    if context.as_sql:
        with context.autocommit_block():
            op.execute(text(_create_legacy_index_sql(schema)))
        return

    if _legacy_index_needs_rebuild(schema):
        with context.autocommit_block():
            if _require_target_index(schema, LEGACY_INDEX_NAME) is not None:
                op.get_bind().exec_driver_sql(_drop_legacy_index_sql(schema))
        _create_legacy_index(schema)
    _require_index(schema, LEGACY_INDEX_NAME, LEGACY_INDEX_COLUMNS)
