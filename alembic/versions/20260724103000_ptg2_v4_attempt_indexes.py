"""Index every attempt coordinate not already covered by the PTG schema.

Revision ID: 20260724103000_ptg2_v4_attempt_indexes
Revises: 20260724104500_ptg2_v4_attempt_lock_order
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from db.migration_index_adoption import has_matching_index


revision = "20260724103000_ptg2_v4_attempt_indexes"
down_revision = "20260724104500_ptg2_v4_attempt_lock_order"
branch_labels = None
depends_on = None

_ATTEMPT_INDEXES = (
    ("ptg2_snapshot_attempt_run_idx", "ptg2_snapshot", "import_run_id"),
    ("ptg2_source_catalog_attempt_run_idx", "ptg2_source_catalog", "import_run_id"),
    ("ptg2_artifact_manifest_attempt_run_idx", "ptg2_artifact_manifest", "import_run_id"),
    ("ptg2_current_snapshot_attempt_previous_idx", "ptg2_current_snapshot", "previous_snapshot_id"),
    ("ptg2_current_source_attempt_previous_idx", "ptg2_current_source_snapshot", "previous_snapshot_id"),
    ("ptg2_current_plan_attempt_previous_idx", "ptg2_current_plan_source", "previous_snapshot_id"),
    ("ptg2_v4_attempt_stage_run_idx", "ptg2_v4_attempt_stage", "internal_run_id"),
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


def _create_index_sql(
    schema: str,
    index_name: str,
    table_name: str,
    column_name: str,
    *,
    concurrently: bool,
) -> str:
    concurrent_sql = " CONCURRENTLY" if concurrently else ""
    return (
        f"CREATE INDEX{concurrent_sql} IF NOT EXISTS {_q(index_name)} "
        f"ON {_q(schema)}.{_q(table_name)} ({_q(column_name)})"
    )


def _online_table_exists(
    operations,
    schema: str,
    table_name: str,
) -> bool:
    """Treat offline recorders as complete and inspect real online catalogs."""

    get_bind = getattr(operations, "get_bind", None)
    connection = get_bind() if callable(get_bind) else None
    if not isinstance(connection, sa.engine.Connection):
        return True
    return bool(
        connection.execute(
            sa.text("SELECT to_regclass(:qualified_name) IS NOT NULL"),
            {"qualified_name": f"{schema}.{table_name}"},
        ).scalar_one()
    )


def upgrade() -> None:
    """Adopt exact indexes or build them without blocking table writers."""

    schema = _schema()
    get_context = getattr(op, "get_context", None)
    if get_context is None:
        for index_name, table_name, column_name in _ATTEMPT_INDEXES:
            op.execute(
                _create_index_sql(
                    schema,
                    index_name,
                    table_name,
                    column_name,
                    concurrently=False,
                )
            )
        return
    migration_context = get_context()
    if migration_context.as_sql:
        with migration_context.autocommit_block():
            for index_name, table_name, column_name in _ATTEMPT_INDEXES:
                op.create_index(
                    index_name,
                    table_name,
                    [column_name],
                    schema=schema,
                    if_not_exists=True,
                    postgresql_concurrently=True,
                )
        return
    for index_name, table_name, column_name in _ATTEMPT_INDEXES:
        if not _online_table_exists(op, schema, table_name):
            continue
        if has_matching_index(
            op,
            index_name,
            table_name,
            (column_name,),
            schema=schema,
        ):
            continue
        with migration_context.autocommit_block():
            op.get_bind().exec_driver_sql(
                _create_index_sql(
                    schema,
                    index_name,
                    table_name,
                    column_name,
                    concurrently=True,
                )
            )
        if not has_matching_index(
            op,
            index_name,
            table_name,
            (column_name,),
            schema=schema,
        ):
            raise RuntimeError(f"required_index_missing:{schema}.{index_name}")


def downgrade() -> None:
    """Remove only indexes owned by this revision."""

    schema = _schema()
    get_context = getattr(op, "get_context", None)
    if get_context is None:
        for index_name, _table_name, _column_name in reversed(_ATTEMPT_INDEXES):
            op.execute(f"DROP INDEX IF EXISTS {_q(schema)}.{_q(index_name)}")
        return
    migration_context = get_context()
    if migration_context.as_sql:
        with migration_context.autocommit_block():
            for index_name, table_name, _column_name in reversed(_ATTEMPT_INDEXES):
                op.drop_index(
                    index_name,
                    table_name=table_name,
                    schema=schema,
                    if_exists=True,
                    postgresql_concurrently=True,
                )
        return
    with migration_context.autocommit_block():
        for index_name, _table_name, _column_name in reversed(_ATTEMPT_INDEXES):
            op.get_bind().exec_driver_sql(
                f"DROP INDEX CONCURRENTLY IF EXISTS "
                f"{_q(schema)}.{_q(index_name)}"
            )
