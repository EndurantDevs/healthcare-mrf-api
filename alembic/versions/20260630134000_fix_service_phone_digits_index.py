"""Constrain service phone index and add address-key lookup.

Revision ID: 20260630134000_fix_service_phone_digits_index
Revises: 20260630112000_provider_directory_role_affiliation_run_indexes
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text


revision = "20260630134000_fix_service_phone_digits_index"
down_revision = "20260630112000_provider_directory_role_affiliation_run_indexes"
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
            f"DROP INDEX CONCURRENTLY IF EXISTS {_q(schema)}.entity_address_unified_idx_service_phone_digits_npi;"
        )
        bind.exec_driver_sql(
            f"""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS entity_address_unified_idx_service_phone_digits_npi
            ON {_qt(schema, "entity_address_unified")}
            (
                regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g'),
                npi
            )
            WHERE type IN ('primary', 'secondary', 'practice', 'site')
              AND regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g') <> '';
            """
        )
        bind.exec_driver_sql(
            f"""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS entity_address_unified_idx_service_address_key_npi
            ON {_qt(schema, "entity_address_unified")} (address_key, npi)
            WHERE type IN ('primary', 'secondary', 'practice', 'site')
              AND address_key IS NOT NULL;
            """
        )


def downgrade():
    bind = op.get_bind()
    schema = _schema()
    with op.get_context().autocommit_block():
        bind.exec_driver_sql(
            f"DROP INDEX CONCURRENTLY IF EXISTS {_q(schema)}.entity_address_unified_idx_service_address_key_npi;"
        )
        bind.exec_driver_sql(
            f"DROP INDEX CONCURRENTLY IF EXISTS {_q(schema)}.entity_address_unified_idx_service_phone_digits_npi;"
        )
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
