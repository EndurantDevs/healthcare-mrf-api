"""Retain reviewed Provider Directory Medicaid acceptance evidence.

Revision ID: 20260713230000_provider_directory_role_accepting_medicaid
Revises: 20260713220000_provider_directory_practitioner_derived_profile
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa


revision = "20260713230000_provider_directory_role_accepting_medicaid"
down_revision = "20260713220000_provider_directory_practitioner_derived_profile"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    op.add_column(
        "provider_directory_practitioner_role",
        sa.Column("accepting_medicaid", sa.Boolean(), nullable=True),
        schema=_schema(),
    )


def downgrade():
    op.drop_column(
        "provider_directory_practitioner_role",
        "accepting_medicaid",
        schema=_schema(),
    )
