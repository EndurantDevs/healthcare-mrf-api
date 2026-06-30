"""Add unified service-location phone indexes.

Revision ID: 20260630110000_entity_address_unified_service_phone_indexes
Revises: 20260629205000_retire_ptg_address_leftovers
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text


revision = "20260630110000_entity_address_unified_service_phone_indexes"
down_revision = "20260629205000_retire_ptg_address_leftovers"
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
            CREATE INDEX CONCURRENTLY IF NOT EXISTS entity_address_unified_idx_service_phone_digits_npi
            ON {_qt(schema, "entity_address_unified")}
            (
                regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g'),
                npi
            )
            WHERE type IN ('primary', 'secondary', 'practice', 'site');
            """
        )
        bind.exec_driver_sql(
            f"""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS entity_address_unified_idx_service_phone_number_npi
            ON {_qt(schema, "entity_address_unified")} (phone_number, npi)
            WHERE type IN ('primary', 'secondary', 'practice', 'site')
              AND phone_number IS NOT NULL
              AND phone_number <> '';
            """
        )


def downgrade():
    schema = _schema()
    with op.get_context().autocommit_block():
        op.get_bind().exec_driver_sql(
            f"DROP INDEX CONCURRENTLY IF EXISTS {_q(schema)}.entity_address_unified_idx_service_phone_number_npi;"
        )
        op.get_bind().exec_driver_sql(
            f"DROP INDEX CONCURRENTLY IF EXISTS {_q(schema)}.entity_address_unified_idx_service_phone_digits_npi;"
        )
