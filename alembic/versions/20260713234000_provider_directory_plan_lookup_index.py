"""Add the immutable Provider Directory plan lookup index.

Revision ID: 20260713234000_provider_directory_plan_lookup_index
Revises: 20260713233000_provider_directory_resource_identifiers
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text

from db.migration_index_adoption import has_matching_index


revision = "20260713234000_provider_directory_plan_lookup_index"
down_revision = "20260713233000_provider_directory_resource_identifiers"
branch_labels = None
depends_on = None


INDEX_NAME = "provider_directory_dataset_resource_plan_lookup_idx"
TABLE_NAME = "provider_directory_dataset_resource"


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _table_exists(bind, schema: str, table: str) -> bool:
    return bool(
        bind.execute(
            text("SELECT to_regclass(:name)"),
            {"name": f"{schema}.{table}"},
        ).scalar()
    )


def _offline_context():
    get_context = getattr(op, "get_context", None)
    if get_context is None:
        return None
    migration_context = get_context()
    return migration_context if migration_context.as_sql else None


def upgrade():
    schema = _schema()
    offline_context = _offline_context()
    if offline_context is not None:
        with offline_context.autocommit_block():
            op.create_index(
                INDEX_NAME,
                TABLE_NAME,
                ["dataset_id", "resource_id"],
                schema=schema,
                if_not_exists=True,
                postgresql_concurrently=True,
                postgresql_where=text("resource_type = 'InsurancePlan'"),
            )
        return
    bind = op.get_bind()
    if not _table_exists(bind, schema, TABLE_NAME):
        return
    if has_matching_index(
        op,
        INDEX_NAME,
        TABLE_NAME,
        ("dataset_id", "resource_id"),
        schema=schema,
        postgresql_where=text("resource_type = 'InsurancePlan'"),
    ):
        return
    with op.get_context().autocommit_block():
        bind.exec_driver_sql(
            f"""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS {_q(INDEX_NAME)}
            ON {_qt(schema, TABLE_NAME)} (dataset_id, resource_id)
            WHERE resource_type = 'InsurancePlan';
            """
        )
    if not has_matching_index(
        op,
        INDEX_NAME,
        TABLE_NAME,
        ("dataset_id", "resource_id"),
        schema=schema,
        postgresql_where=text("resource_type = 'InsurancePlan'"),
    ):
        raise RuntimeError(f"required_index_missing:{schema}.{INDEX_NAME}")


def downgrade():
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
            f"DROP INDEX CONCURRENTLY IF EXISTS {_q(schema)}.{_q(INDEX_NAME)};"
        )
