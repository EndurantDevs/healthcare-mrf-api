"""Add Provider Directory FHIR completeness and provenance fields.

Revision ID: 20260713120000_provider_directory_fhir_completeness
Revises: 20260712120000_provider_directory_healthcare_service_context
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa


revision = "20260713120000_provider_directory_fhir_completeness"
down_revision = "20260712120000_provider_directory_healthcare_service_context"
branch_labels = None
depends_on = None


FHIR_RESOURCE_TABLES = (
    "provider_directory_insurance_plan",
    "provider_directory_practitioner",
    "provider_directory_organization",
    "provider_directory_location",
    "provider_directory_practitioner_role",
    "provider_directory_healthcare_service",
    "provider_directory_organization_affiliation",
    "provider_directory_endpoint",
    "provider_directory_canonical_resource",
)


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _provenance_columns() -> tuple[sa.Column, ...]:
    return (
        sa.Column("fhir_meta", sa.JSON(), nullable=True),
        sa.Column("fhir_self_url", sa.Text(), nullable=True),
        sa.Column("fhir_fetch_url", sa.Text(), nullable=True),
        sa.Column("fhir_fetch_mode", sa.String(length=32), nullable=True),
    )


def upgrade():
    schema = _schema()
    for table_name in FHIR_RESOURCE_TABLES:
        for column in _provenance_columns():
            op.add_column(table_name, column, schema=schema)

    for column in (
        sa.Column("product_identifiers", sa.JSON(), nullable=True),
        sa.Column("plan_backbones", sa.JSON(), nullable=True),
        sa.Column("coverage", sa.JSON(), nullable=True),
    ):
        op.add_column("provider_directory_insurance_plan", column, schema=schema)

    for column in (
        sa.Column("available_time", sa.JSON(), nullable=True),
        sa.Column("not_available", sa.JSON(), nullable=True),
        sa.Column("availability_exceptions", sa.Text(), nullable=True),
        sa.Column("new_patient_acceptance", sa.JSON(), nullable=True),
        sa.Column("telehealth", sa.JSON(), nullable=True),
    ):
        op.add_column("provider_directory_practitioner_role", column, schema=schema)


def downgrade():
    schema = _schema()
    for column_name in (
        "telehealth",
        "new_patient_acceptance",
        "availability_exceptions",
        "not_available",
        "available_time",
    ):
        op.drop_column(
            "provider_directory_practitioner_role",
            column_name,
            schema=schema,
        )
    for column_name in ("coverage", "plan_backbones", "product_identifiers"):
        op.drop_column(
            "provider_directory_insurance_plan",
            column_name,
            schema=schema,
        )
    for table_name in reversed(FHIR_RESOURCE_TABLES):
        for column_name in (
            "fhir_fetch_mode",
            "fhir_fetch_url",
            "fhir_self_url",
            "fhir_meta",
        ):
            op.drop_column(table_name, column_name, schema=schema)
