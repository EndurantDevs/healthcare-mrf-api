"""Add resumable Provider Directory Profile build checkpoints.

Revision ID: 20260720120000_provider_directory_profile_build_checkpoint
Revises: 20260720100000_provider_directory_bulk_output_resume
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from db.migration_adoption import create_table_or_validate
from db.migration_index_adoption import create_index_if_missing


revision = "20260720120000_provider_directory_profile_build_checkpoint"
down_revision = "20260720100000_provider_directory_bulk_output_resume"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade() -> None:
    schema = _schema()
    table = "provider_directory_profile_build_checkpoint"
    create_table_or_validate(
        op,
        table,
        sa.Column("build_id", sa.String(64), nullable=False),
        sa.Column("strategy_version", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("owner_run_id", sa.String(64)),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("profile_as_of", sa.String(10), nullable=False),
        sa.Column("source_ids", postgresql.JSONB(), nullable=False),
        sa.Column("retained_source_ids", postgresql.JSONB(), nullable=False),
        sa.Column("dataset_ids", postgresql.JSONB(), nullable=False),
        sa.Column("evidence_stage", sa.String(63), nullable=False),
        sa.Column("profile_stage", sa.String(63), nullable=False),
        sa.Column("evidence_target_oid", sa.BigInteger()),
        sa.Column("profile_target_oid", sa.BigInteger()),
        sa.Column("has_existing_artifacts", sa.Boolean(), nullable=False),
        sa.Column(
            "evidence_next_batch",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
        sa.Column("evidence_total_batches", sa.Integer(), nullable=False),
        sa.Column(
            "profile_next_batch",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
        sa.Column("profile_total_batches", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime()),
        sa.PrimaryKeyConstraint("build_id"),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_profile_build_checkpoint_state_idx",
        table,
        ["state", "updated_at"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_profile_build_checkpoint_owner_idx",
        table,
        ["owner_run_id"],
        schema=schema,
    )


def downgrade() -> None:
    op.drop_table(
        "provider_directory_profile_build_checkpoint",
        schema=_schema(),
    )
