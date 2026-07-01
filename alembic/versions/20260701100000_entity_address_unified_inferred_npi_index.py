"""Add inferred NPI lookup index for unified address serving.

Revision ID: 20260701100000_entity_address_unified_inferred_npi_index
Revises: 20260701090000_provider_directory_address_overlay
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text


revision = "20260701100000_entity_address_unified_inferred_npi_index"
down_revision = "20260701090000_provider_directory_address_overlay"
branch_labels = None
depends_on = None


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


def upgrade():
    bind = op.get_bind()
    schema = _schema()
    if not _table_exists(bind, schema, "entity_address_unified"):
        return
    with op.get_context().autocommit_block():
        bind.exec_driver_sql(
            f"""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS entity_address_unified_idx_inferred_npi
            ON {_qt(schema, "entity_address_unified")} (inferred_npi)
            WHERE inferred_npi IS NOT NULL;
            """
        )


def downgrade():
    schema = _schema()
    with op.get_context().autocommit_block():
        op.get_bind().exec_driver_sql(
            f"DROP INDEX CONCURRENTLY IF EXISTS {_q(schema)}.entity_address_unified_idx_inferred_npi;"
        )
