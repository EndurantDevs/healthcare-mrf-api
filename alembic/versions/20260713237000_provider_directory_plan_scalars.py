"""Add scalar lookup fields to the immutable InsurancePlan projection.

Revision ID: 20260713237000_provider_directory_plan_scalars
Revises: 20260713236000_provider_directory_dataset_insurance_plan
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa


revision = "20260713237000_provider_directory_plan_scalars"
down_revision = "20260713236000_provider_directory_dataset_insurance_plan"
branch_labels = None
depends_on = None


PLAN_TABLE = "provider_directory_dataset_insurance_plan"
ACTIVE_INDEX = "provider_directory_dataset_insurance_plan_active_lookup_idx"


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _column_exists(column_name: str) -> bool:
    bind = op.get_bind()
    return bool(
        bind.execute(
            sa.text(
                """
                SELECT EXISTS (
                    SELECT 1
                      FROM pg_attribute
                     WHERE attrelid = to_regclass(:table_name)
                       AND attname = :column_name
                       AND attnum > 0
                       AND NOT attisdropped
                )
                """
            ),
            {
                "table_name": f"{_schema()}.{PLAN_TABLE}",
                "column_name": column_name,
            },
        ).scalar()
    )


def upgrade():
    schema = _schema()
    if not _column_exists("plan_active"):
        op.add_column(
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
    if not _column_exists("plan_identifier"):
        op.add_column(
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
    op.execute(
        sa.text(
            f"""
            CREATE INDEX IF NOT EXISTS {ACTIVE_INDEX}
                ON {schema}.{PLAN_TABLE} (dataset_id, resource_id)
                INCLUDE (plan_identifier)
             WHERE plan_active;
            """
        )
    )


def downgrade():
    schema = _schema()
    op.drop_index(ACTIVE_INDEX, table_name=PLAN_TABLE, schema=schema)
    op.drop_column(PLAN_TABLE, "plan_identifier", schema=schema)
    op.drop_column(PLAN_TABLE, "plan_active", schema=schema)
