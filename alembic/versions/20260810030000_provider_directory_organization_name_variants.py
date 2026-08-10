# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain every observed primary Organization name.

Revision ID: 20260810030000_provider_directory_organization_name_variants
Revises: 20260810020000_provider_directory_terminal_scope_binding
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa

from db.migration_adoption import add_column_if_missing


revision = "20260810030000_provider_directory_organization_name_variants"
down_revision = "20260810020000_provider_directory_terminal_scope_binding"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade() -> None:
    """Add nullable v4 semantic name-state storage."""

    add_column_if_missing(
        op,
        "provider_directory_organization",
        sa.Column("name_variants", sa.JSON(), nullable=True),
        schema=_schema(),
    )


def downgrade() -> None:
    """Remove v4 semantic name-state storage."""

    op.drop_column(
        "provider_directory_organization",
        "name_variants",
        schema=_schema(),
    )
