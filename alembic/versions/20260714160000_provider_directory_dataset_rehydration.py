"""Add durable checkpoints for retained Provider Directory datasets.

Revision ID: 20260714160000_provider_directory_dataset_rehydration
Revises: 20260714150000_provider_directory_pagination_census
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from db.migration_adoption import create_table_or_validate
from db.migration_index_adoption import create_index_if_missing


revision = "20260714160000_provider_directory_dataset_rehydration"
down_revision = "20260714150000_provider_directory_pagination_census"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade() -> None:
    schema = _schema()
    create_table_or_validate(
        op,
        "provider_directory_dataset_rehydration_checkpoint",
        sa.Column("source_id", sa.String(64), nullable=False),
        sa.Column("dataset_id", sa.String(96), nullable=False),
        sa.Column("acquisition_root_run_id", sa.String(64), nullable=False),
        sa.Column("resource_type", sa.String(64), nullable=False),
        sa.Column("endpoint_id", sa.String(64), nullable=False),
        sa.Column("dataset_hash", sa.String(64), nullable=False),
        sa.Column("owner_run_id", sa.String(64), nullable=False),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("last_resource_id", sa.String(256)),
        sa.Column("expected_input_count", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("input_count", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("mapped_count", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("rejected_count", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("evidence_json", postgresql.JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("error", sa.Text()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime()),
        sa.ForeignKeyConstraint(
            ["source_id"],
            [f"{schema}.provider_directory_source.source_id"],
            name="provider_directory_dataset_rehydration_checkpoin_source_id_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["dataset_id"],
            [f"{schema}.provider_directory_endpoint_dataset.dataset_id"],
            name="provider_directory_dataset_rehydration_checkpoi_dataset_id_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["endpoint_id"],
            [f"{schema}.provider_directory_api_endpoint.endpoint_id"],
            name="provider_directory_dataset_rehydration_checkpo_endpoint_id_fkey",
        ),
        sa.PrimaryKeyConstraint("source_id", "dataset_id", "acquisition_root_run_id", "resource_type"),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_dataset_rehydrate_checkpoint_owner_idx",
        "provider_directory_dataset_rehydration_checkpoint",
        ["owner_run_id"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_dataset_rehydrate_checkpoint_state_idx",
        "provider_directory_dataset_rehydration_checkpoint",
        ["state", "updated_at"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_dataset_rehydrate_checkpoint_dataset_idx",
        "provider_directory_dataset_rehydration_checkpoint",
        ["dataset_id"],
        schema=schema,
    )


def downgrade() -> None:
    op.drop_table("provider_directory_dataset_rehydration_checkpoint", schema=_schema())
