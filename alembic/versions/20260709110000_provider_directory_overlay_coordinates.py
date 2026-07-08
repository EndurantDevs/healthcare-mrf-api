"""Add coordinates to Provider Directory address overlay.

Revision ID: 20260709110000_provider_directory_overlay_coordinates
Revises: 20260706190000_entity_address_unified_service_premise_index
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260709110000_provider_directory_overlay_coordinates"
down_revision = "20260706190000_entity_address_unified_service_premise_index"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def upgrade():
    table = _qt(_schema(), "provider_directory_address_overlay")
    op.execute(
        f"""
        ALTER TABLE {table}
            ADD COLUMN IF NOT EXISTS lat numeric,
            ADD COLUMN IF NOT EXISTS long numeric;
        """
    )


def downgrade():
    table = _qt(_schema(), "provider_directory_address_overlay")
    op.execute(
        f"""
        ALTER TABLE {table}
            DROP COLUMN IF EXISTS lat,
            DROP COLUMN IF EXISTS long;
        """
    )
