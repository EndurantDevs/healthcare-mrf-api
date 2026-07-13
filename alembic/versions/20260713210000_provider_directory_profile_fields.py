"""Retain reviewed Provider Directory profile fields.

Revision ID: 20260713210000_provider_directory_profile_fields
Revises: 20260713200000_provider_directory_bulk_next_poll
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa


revision = "20260713210000_provider_directory_profile_fields"
down_revision = "20260713200000_provider_directory_bulk_next_poll"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


PROFILE_COLUMNS = {
    "provider_directory_practitioner": (
        sa.Column("identifiers", sa.JSON(), nullable=True),
        sa.Column("names", sa.JSON(), nullable=True),
        sa.Column("administrative_gender", sa.String(length=32), nullable=True),
        sa.Column("addresses", sa.JSON(), nullable=True),
        sa.Column("qualifications", sa.JSON(), nullable=True),
        sa.Column("communications", sa.JSON(), nullable=True),
        sa.Column("photos", sa.JSON(), nullable=True),
    ),
    "provider_directory_organization": (
        sa.Column("identifiers", sa.JSON(), nullable=True),
        sa.Column("contacts", sa.JSON(), nullable=True),
        sa.Column("part_of_ref", sa.Text(), nullable=True),
    ),
    "provider_directory_location": (
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("physical_type_codes", sa.JSON(), nullable=True),
        sa.Column("managing_organization_ref", sa.Text(), nullable=True),
        sa.Column("addresses", sa.JSON(), nullable=True),
        sa.Column("hours_of_operation", sa.JSON(), nullable=True),
        sa.Column("availability_exceptions", sa.Text(), nullable=True),
        sa.Column("photos", sa.JSON(), nullable=True),
    ),
    "provider_directory_healthcare_service": (
        sa.Column("program_codes", sa.JSON(), nullable=True),
        sa.Column("characteristic_codes", sa.JSON(), nullable=True),
        sa.Column("communication_codes", sa.JSON(), nullable=True),
        sa.Column("referral_method_codes", sa.JSON(), nullable=True),
        sa.Column("service_provision_codes", sa.JSON(), nullable=True),
        sa.Column("eligibility", sa.JSON(), nullable=True),
        sa.Column("appointment_required", sa.Boolean(), nullable=True),
        sa.Column("available_time", sa.JSON(), nullable=True),
        sa.Column("not_available", sa.JSON(), nullable=True),
        sa.Column("availability_exceptions", sa.Text(), nullable=True),
        sa.Column("extra_details", sa.Text(), nullable=True),
        sa.Column("photos", sa.JSON(), nullable=True),
    ),
}


def upgrade():
    schema = _schema()
    for table_name, columns in PROFILE_COLUMNS.items():
        for column in columns:
            op.add_column(table_name, column, schema=schema)


def downgrade():
    schema = _schema()
    for table_name, columns in reversed(tuple(PROFILE_COLUMNS.items())):
        for column in reversed(columns):
            op.drop_column(table_name, column.name, schema=schema)
