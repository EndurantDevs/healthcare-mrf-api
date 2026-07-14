"""Add normalized HealthcareService provider context.

Revision ID: 20260712120000_provider_directory_healthcare_service_context
Revises: 20260711120000_provider_directory_affiliation_telecom
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa

from db.migration_adoption import add_column_if_missing


revision = "20260712120000_provider_directory_healthcare_service_context"
down_revision = "20260711120000_provider_directory_affiliation_telecom"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()
    add_column_if_missing(
        op,
        "provider_directory_healthcare_service",
        sa.Column("provided_by_ref", sa.Text(), nullable=True),
        schema=schema,
    )
    add_column_if_missing(
        op,
        "provider_directory_healthcare_service",
        sa.Column("accepting_patients", sa.JSON(), nullable=True),
        schema=schema,
    )
    add_column_if_missing(
        op,
        "provider_directory_healthcare_service",
        sa.Column("npi", sa.BigInteger(), nullable=True),
        schema=schema,
    )


def downgrade():
    schema = _schema()
    for column_name in ("npi", "accepting_patients", "provided_by_ref"):
        op.drop_column(
            "provider_directory_healthcare_service",
            column_name,
            schema=schema,
        )
