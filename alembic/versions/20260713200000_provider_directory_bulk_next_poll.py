"""Persist Provider Directory Bulk Data next-poll boundaries.

Revision ID: 20260713200000_provider_directory_bulk_next_poll
Revises: 20260713193000_address_canonical_incomplete_line2_unit
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa

from db.migration_adoption import add_column_if_missing


revision = "20260713200000_provider_directory_bulk_next_poll"
down_revision = "20260713193000_address_canonical_incomplete_line2_unit"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    add_column_if_missing(
        op,
        "provider_directory_bulk_acquisition_checkpoint",
        sa.Column("next_poll_at", sa.TIMESTAMP(), nullable=True),
        schema=_schema(),
    )


def downgrade():
    op.drop_column(
        "provider_directory_bulk_acquisition_checkpoint",
        "next_poll_at",
        schema=_schema(),
    )
