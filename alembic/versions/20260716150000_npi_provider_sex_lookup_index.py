"""Add the covering NPI provider-sex lookup index.

Revision ID: 20260716150000_npi_provider_sex_lookup_index
Revises: 20260717100000_nearby_geo_count_covering_index
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text

from db.migration_index_adoption import has_matching_index


revision = "20260716150000_npi_provider_sex_lookup_index"
down_revision = "20260717100000_nearby_geo_count_covering_index"
branch_labels = None
depends_on = None


INDEX_NAME = "npi_idx_npi_provider_sex_code"
TABLE_NAME = "npi"


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _offline_context():
    migration_context = op.get_context()
    return migration_context if migration_context.as_sql else None


def upgrade() -> None:
    schema = _schema()
    index_predicate = text("provider_sex_code IS NOT NULL")
    offline_context = _offline_context()
    if offline_context is not None:
        with offline_context.autocommit_block():
            op.create_index(
                INDEX_NAME,
                TABLE_NAME,
                ["npi", "provider_sex_code"],
                schema=schema,
                if_not_exists=True,
                postgresql_concurrently=True,
                postgresql_where=index_predicate,
            )
        return
    bind = op.get_bind()
    if not bind.execute(
        text("SELECT to_regclass(:table_name)"),
        {"table_name": f"{schema}.{TABLE_NAME}"},
    ).scalar():
        return
    if has_matching_index(
        op,
        INDEX_NAME,
        TABLE_NAME,
        ("npi", "provider_sex_code"),
        schema=schema,
        postgresql_where=index_predicate,
    ):
        return
    with op.get_context().autocommit_block():
        bind.exec_driver_sql(
            f"""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS {_q(INDEX_NAME)}
            ON {_qt(schema, TABLE_NAME)} (npi, provider_sex_code)
            WHERE provider_sex_code IS NOT NULL;
            """
        )
    if not has_matching_index(
        op,
        INDEX_NAME,
        TABLE_NAME,
        ("npi", "provider_sex_code"),
        schema=schema,
        postgresql_where=index_predicate,
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
            f"DROP INDEX CONCURRENTLY IF EXISTS {_q(schema)}.{_q(INDEX_NAME)};"
        )
