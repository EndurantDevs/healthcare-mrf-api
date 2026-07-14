"""Persist Provider Directory opaque-cursor completeness census state.

Revision ID: 20260714150000_provider_directory_pagination_census
Revises: 20260714140000_mrf_file_source_cursor_index
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from db.migration_adoption import add_column_if_missing


revision = "20260714150000_provider_directory_pagination_census"
down_revision = "20260714140000_mrf_file_source_cursor_index"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade() -> None:
    add_column_if_missing(
        op,
        "provider_directory_pagination_checkpoint",
        sa.Column(
            "completeness_json",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        schema=_schema(),
    )


def downgrade() -> None:
    op.drop_column(
        "provider_directory_pagination_checkpoint",
        "completeness_json",
        schema=_schema(),
    )
