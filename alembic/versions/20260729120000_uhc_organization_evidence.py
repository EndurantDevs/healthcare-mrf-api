"""Retain UHC organization and payer-plan membership evidence.

Revision ID: 20260729120000_uhc_organization_evidence
Revises: 20260729110000_tin_npi_connector
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa

from db.migration_adoption import add_column_if_missing


revision = "20260729120000_uhc_organization_evidence"
down_revision = "20260729110000_tin_npi_connector"
branch_labels = None
depends_on = None


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


UHC_EVIDENCE_COLUMNS_BY_TABLE = {
    "provider_directory_organization": (
        sa.Column("tin_status", sa.String(length=64), nullable=True),
        sa.Column("source_lineage", sa.JSON(), nullable=True),
    ),
    "provider_directory_practitioner_role": (
        sa.Column("plan_scope", sa.JSON(), nullable=True),
        sa.Column("network_tier", sa.String(length=128), nullable=True),
        sa.Column("network_key_id", sa.String(length=64), nullable=True),
    ),
    "provider_directory_organization_affiliation": (
        sa.Column("insurance_plan_refs", sa.JSON(), nullable=True),
        sa.Column("plan_scope", sa.JSON(), nullable=True),
        sa.Column("network_tier", sa.String(length=128), nullable=True),
        sa.Column("network_key_id", sa.String(length=64), nullable=True),
        sa.Column("relationship_type", sa.String(length=64), nullable=True),
        sa.Column("ownership_status", sa.String(length=64), nullable=True),
        sa.Column("source_lineage", sa.JSON(), nullable=True),
    ),
}


def upgrade() -> None:
    """Add nullable typed fields before UHC canonical rehydration uses them."""
    schema = _schema()
    for table_name, columns in UHC_EVIDENCE_COLUMNS_BY_TABLE.items():
        for column in columns:
            add_column_if_missing(
                op,
                table_name,
                column,
                schema=schema,
            )


def downgrade() -> None:
    """Remove only the typed fields introduced by this revision."""
    schema = _schema()
    for table_name, columns in reversed(
        tuple(UHC_EVIDENCE_COLUMNS_BY_TABLE.items())
    ):
        for column in reversed(columns):
            op.drop_column(table_name, column.name, schema=schema)
