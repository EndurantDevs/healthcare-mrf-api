"""Retain Provider Directory role, service, and affiliation identifiers.

Revision ID: 20260713233000_provider_directory_resource_identifiers
Revises: 20260713230000_provider_directory_role_accepting_medicaid
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa


revision = "20260713233000_provider_directory_resource_identifiers"
down_revision = "20260713230000_provider_directory_role_accepting_medicaid"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


RESOURCE_COLUMNS = {
    "provider_directory_practitioner_role": (
        sa.Column("identifiers", sa.JSON(), nullable=True),
    ),
    "provider_directory_healthcare_service": (
        sa.Column("identifiers", sa.JSON(), nullable=True),
        sa.Column("comment", sa.Text(), nullable=True),
    ),
    "provider_directory_organization_affiliation": (
        sa.Column("identifiers", sa.JSON(), nullable=True),
    ),
}


def upgrade():
    schema = _schema()
    for table_name, columns in RESOURCE_COLUMNS.items():
        for column in columns:
            op.add_column(table_name, column, schema=schema)


def downgrade():
    schema = _schema()
    for table_name, columns in reversed(tuple(RESOURCE_COLUMNS.items())):
        for column in reversed(columns):
            op.drop_column(table_name, column.name, schema=schema)
