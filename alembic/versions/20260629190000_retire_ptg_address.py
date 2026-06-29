"""Retire derived PTG address table.

Revision ID: 20260629190000_retire_ptg_address
Revises: 20260629160000_contact_number_canonical_columns
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260629190000_retire_ptg_address"
down_revision = "20260629160000_contact_number_canonical_columns"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def upgrade():
    schema = _schema()
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg_address')} CASCADE;")
    op.execute(
        f"ALTER TABLE IF EXISTS {_qt(schema, 'entity_address_unified')} "
        "DROP COLUMN IF EXISTS ptg_address_version;"
    )


def downgrade():
    # ptg_address was a derived cache from PTG snapshot data. Rebuild unified
    # addresses from address-bearing sources instead of recreating this table.
    # ptg_address_version was only a compatibility bridge for that retired cache.
    pass
