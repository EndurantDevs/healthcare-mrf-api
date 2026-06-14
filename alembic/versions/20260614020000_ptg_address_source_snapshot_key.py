"""Scope ptg_address identity by source snapshot.

Revision ID: 20260614020000
Revises: 20260614010000_entity_address_unified_serving_projection
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text


revision = "20260614020000_ptg_address_source_snapshot_key"
down_revision = "20260614010000_entity_address_unified_serving_projection"
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
    if not _table_exists(bind, schema, "ptg_address"):
        return

    primary_key = bind.execute(
        text(
            """
            SELECT c.conname
              FROM pg_constraint c
              JOIN pg_class t ON t.oid = c.conrelid
              JOIN pg_namespace n ON n.oid = t.relnamespace
             WHERE n.nspname = :schema
               AND t.relname = 'ptg_address'
               AND c.contype = 'p'
            """
        ),
        {"schema": schema},
    ).scalar()
    bind.exec_driver_sql(f"DROP INDEX IF EXISTS {_q(schema)}.{_q('ptg_address_idx_primary')};")
    if primary_key:
        bind.exec_driver_sql(f"ALTER TABLE {_qt(schema, 'ptg_address')} DROP CONSTRAINT {_q(primary_key)};")
    bind.exec_driver_sql(
        f"""
        ALTER TABLE {_qt(schema, 'ptg_address')}
        ADD PRIMARY KEY (source_key, snapshot_id, location_key);
        """
    )


def downgrade():
    return None
