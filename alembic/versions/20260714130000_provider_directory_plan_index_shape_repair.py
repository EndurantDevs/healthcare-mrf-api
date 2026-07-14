"""Repair legacy InsurancePlan active lookup index shapes.

Revision ID: 20260714130000_provider_directory_plan_index_shape_repair
Revises: 20260714120000_ptg2_v3_schema_gc_consistency
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa

from db.migration_index_adoption import IndexDefinition, create_index_if_missing


revision = "20260714130000_provider_directory_plan_index_shape_repair"
down_revision = "20260714120000_ptg2_v3_schema_gc_consistency"
branch_labels = None
depends_on = None


PLAN_TABLE = "provider_directory_dataset_insurance_plan"
ACTIVE_INDEX = "provider_directory_dataset_insurance_plan_active_lookup_idx"


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade() -> None:
    create_index_if_missing(
        op,
        ACTIVE_INDEX,
        PLAN_TABLE,
        ("dataset_id", "resource_id"),
        schema=_schema(),
        legacy_shapes=(
            IndexDefinition(
                key_columns=("dataset_id", "resource_id", "plan_identifier"),
                predicate="plan_active",
            ),
        ),
        postgresql_include=("plan_identifier",),
        postgresql_where=sa.text("plan_active"),
    )


def downgrade() -> None:
    # Both adjacent revisions intend the same final index. Reverting the repair
    # would reintroduce a known-bad shape, so downgrade preserves the safe DDL.
    return None
