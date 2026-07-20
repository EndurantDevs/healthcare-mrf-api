"""Add resumable Provider Directory Profile build checkpoints.

Revision ID: 20260720120000_provider_directory_profile_build_checkpoint
Revises: 20260720110000_uhc_provider_file_catalog
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260720120000_provider_directory_profile_build_checkpoint"
down_revision = "20260720110000_uhc_provider_file_catalog"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade() -> None:
    schema = _schema()
    table = "provider_directory_profile_build_checkpoint"
    op.create_table(
        table,
        sa.Column("build_id", sa.String(64), nullable=False),
        sa.Column("strategy_version", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("resume_lineage_hash", sa.String(64), nullable=False),
        sa.Column("owner_run_id", sa.String(64)),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("profile_as_of", sa.String(10), nullable=False),
        sa.Column("source_ids", postgresql.JSONB(), nullable=False),
        sa.Column("retained_source_ids", postgresql.JSONB(), nullable=False),
        sa.Column("dataset_ids", postgresql.JSONB(), nullable=False),
        sa.Column("evidence_stage", sa.String(63), nullable=False),
        sa.Column("profile_stage", sa.String(63), nullable=False),
        sa.Column("evidence_stage_oid", sa.BigInteger(), nullable=False),
        sa.Column("profile_stage_oid", sa.BigInteger(), nullable=False),
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
        sa.CheckConstraint(
            "resume_lineage_hash ~ '^[0-9a-f]{64}$'",
            name="pd_profile_build_checkpoint_lineage_hash_check",
        ),
        sa.CheckConstraint(
            "evidence_stage_oid > 0 AND profile_stage_oid > 0 "
            "AND (evidence_target_oid IS NULL OR evidence_target_oid > 0) "
            "AND (profile_target_oid IS NULL OR profile_target_oid > 0)",
            name="pd_profile_build_checkpoint_oid_check",
        ),
        sa.CheckConstraint(
            "state IN ('building_evidence', 'evidence_complete', "
            "'building_profile', 'ready', 'failed')",
            name="pd_profile_build_checkpoint_state_check",
        ),
        sa.CheckConstraint(
            "evidence_total_batches >= 0 "
            "AND evidence_next_batch BETWEEN 0 AND evidence_total_batches "
            "AND profile_total_batches >= 0 "
            "AND profile_next_batch BETWEEN 0 AND profile_total_batches",
            name="pd_profile_build_checkpoint_batch_bounds_check",
        ),
        sa.CheckConstraint(
            "profile_next_batch = 0 "
            "OR evidence_next_batch = evidence_total_batches",
            name="pd_profile_build_checkpoint_phase_order_check",
        ),
        sa.CheckConstraint(
            "state = 'failed' "
            "OR (state = 'building_evidence' AND profile_next_batch = 0) "
            "OR (state = 'evidence_complete' "
            "AND evidence_next_batch = evidence_total_batches "
            "AND profile_next_batch = 0) "
            "OR (state = 'building_profile' "
            "AND evidence_next_batch = evidence_total_batches) "
            "OR (state = 'ready' "
            "AND evidence_next_batch = evidence_total_batches "
            "AND profile_next_batch = profile_total_batches)",
            name="pd_profile_build_checkpoint_state_progress_check",
        ),
        schema=schema,
    )
    op.create_index(
        "pd_profile_build_checkpoint_state_idx",
        table,
        ["state", "updated_at"],
        schema=schema,
    )
    op.create_index(
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
