"""Add scalar lookup fields to the immutable InsurancePlan projection.

Revision ID: 20260713237000_provider_directory_plan_scalars
Revises: 20260713236000_provider_directory_dataset_insurance_plan
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa

from db.migration_adoption import add_column_if_missing
from db.migration_index_adoption import IndexDefinition, create_index_if_missing


revision = "20260713237000_provider_directory_plan_scalars"
down_revision = "20260713236000_provider_directory_dataset_insurance_plan"
branch_labels = None
depends_on = None


PLAN_TABLE = "provider_directory_dataset_insurance_plan"
ACTIVE_INDEX = "provider_directory_dataset_insurance_plan_active_lookup_idx"


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()
    add_column_if_missing(
        op,
        PLAN_TABLE,
        sa.Column(
            "plan_active",
            sa.Boolean(),
            sa.Computed(
                "COALESCE(NULLIF(lower(btrim(payload_json ->> 'status')), ''), "
                "'active') = 'active'",
                persisted=True,
            ),
        ),
        schema=schema,
    )
    add_column_if_missing(
        op,
        PLAN_TABLE,
        sa.Column(
            "plan_identifier",
            sa.Text(),
            sa.Computed(
                "NULLIF(btrim(payload_json ->> 'plan_identifier'), '')",
                persisted=True,
            ),
        ),
        schema=schema,
    )
    create_index_if_missing(
        op,
        ACTIVE_INDEX,
        PLAN_TABLE,
        ("dataset_id", "resource_id"),
        schema=schema,
        legacy_shapes=(
            IndexDefinition(
                key_columns=("dataset_id", "resource_id", "plan_identifier"),
                predicate="plan_active",
            ),
        ),
        postgresql_include=("plan_identifier",),
        postgresql_where=sa.text("plan_active"),
    )


def downgrade():
    schema = _schema()
    op.drop_index(ACTIVE_INDEX, table_name=PLAN_TABLE, schema=schema)
    op.drop_column(PLAN_TABLE, "plan_identifier", schema=schema)
    op.drop_column(PLAN_TABLE, "plan_active", schema=schema)
