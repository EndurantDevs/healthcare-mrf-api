"""Add the canonical NPI search taxonomy projection column.

Revision ID: 20260828090000_npi_search_taxonomy_projection
Revises: 20260827160000_hospital_price_selector_page_packing
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260828090000_npi_search_taxonomy_projection"
down_revision = "20260827160000_hospital_price_selector_page_packing"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def upgrade() -> None:
    # The current sealed relation stays on the legacy query. A later canonical
    # NPI publication fills the staged column and atomically promotes its GIN.
    # Serving stays default-off until pre-change NPI workers have drained and
    # a current-image canonical publication promotes the populated projection.
    op.execute(
        f"ALTER TABLE IF EXISTS {_qt(_schema(), 'npi')} "
        "ADD COLUMN IF NOT EXISTS search_taxonomy_codes varchar[] "
        "NOT NULL DEFAULT ARRAY[]::varchar[]"
    )


def downgrade() -> None:
    # Roll back readers and NPI import workers before removing their column.
    op.execute(
        f"ALTER TABLE IF EXISTS {_qt(_schema(), 'npi')} "
        "DROP COLUMN IF EXISTS search_taxonomy_codes"
    )
