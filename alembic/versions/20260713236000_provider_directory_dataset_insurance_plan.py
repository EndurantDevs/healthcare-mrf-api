"""Add the immutable Provider Directory InsurancePlan dataset projection.

Revision ID: 20260713236000_provider_directory_dataset_insurance_plan
Revises: 20260713235000_import_run_provider_directory_retry_child
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from db.migration_adoption import create_table_or_validate


revision = "20260713236000_provider_directory_dataset_insurance_plan"
down_revision = "20260713235000_import_run_provider_directory_retry_child"
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
    create_table_or_validate(
        op,
        "provider_directory_dataset_insurance_plan",
        sa.Column("dataset_id", sa.String(length=96), nullable=False),
        sa.Column("resource_id", sa.String(length=256), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["dataset_id"],
            [f"{schema}.provider_directory_endpoint_dataset.dataset_id"],
            name="provider_directory_dataset_insurance_plan_dataset_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "dataset_id",
            "resource_id",
            name="provider_directory_dataset_insurance_plan_pkey",
        ),
        schema=schema,
    )
    projection_ref = _qt(schema, "provider_directory_dataset_insurance_plan")
    resource_ref = _qt(schema, "provider_directory_dataset_resource")
    op.execute(
        sa.text(
            f"""
            INSERT INTO {projection_ref} (
                dataset_id,
                resource_id,
                payload_hash,
                payload_json
            )
            SELECT dataset_id,
                   resource_id,
                   payload_hash,
                   payload_json
              FROM {resource_ref}
             WHERE resource_type = 'InsurancePlan'
            ON CONFLICT (dataset_id, resource_id) DO UPDATE
                    SET payload_hash = EXCLUDED.payload_hash,
                        payload_json = EXCLUDED.payload_json;
            """
        )
    )


def downgrade():
    op.drop_table("provider_directory_dataset_insurance_plan", schema=_schema())
