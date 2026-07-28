"""Add resumable Provider Directory content-proof shards.

Revision ID: 20260728130000_provider_directory_content_proof_shards
Revises: 20260728120000_uhc_semantic_build_registry
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260728130000_provider_directory_content_proof_shards"
down_revision = "20260728120000_uhc_semantic_build_registry"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade() -> None:
    schema = _schema()
    table_name = "provider_directory_dataset_proof_shard"
    op.create_table(
        table_name,
        sa.Column("dataset_id", sa.String(length=96), nullable=False),
        sa.Column("shard_id", sa.String(length=64), nullable=False),
        sa.Column("endpoint_id", sa.String(length=64), nullable=False),
        sa.Column(
            "acquisition_root_run_id",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column("source_ids_json", postgresql.JSONB(), nullable=False),
        sa.Column("resource_count", sa.BigInteger(), nullable=False),
        sa.Column("resource_counts_json", postgresql.JSONB(), nullable=False),
        sa.Column("first_identity_json", postgresql.JSONB(), nullable=False),
        sa.Column("last_identity_json", postgresql.JSONB(), nullable=False),
        sa.Column("input_sha256", sa.String(length=64), nullable=False),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("artifact_byte_count", sa.BigInteger(), nullable=False),
        sa.Column("payload_bytes", sa.LargeBinary(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "shard_id ~ '^[0-9a-f]{64}$' "
            "AND input_sha256 ~ '^[0-9a-f]{64}$' "
            "AND artifact_sha256 ~ '^[0-9a-f]{64}$' "
            "AND endpoint_id <> '' AND acquisition_root_run_id <> '' "
            "AND resource_count > 0 AND artifact_byte_count > 0",
            name="provider_directory_dataset_proof_shard_identity_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(source_ids_json) = 'array' "
            "AND jsonb_array_length(source_ids_json) > 0 "
            "AND jsonb_typeof(resource_counts_json) = 'object' "
            "AND jsonb_typeof(first_identity_json) = 'array' "
            "AND jsonb_typeof(last_identity_json) = 'array'",
            name="provider_directory_dataset_proof_shard_json_check",
        ),
        sa.ForeignKeyConstraint(
            ["dataset_id"],
            [f"{schema}.provider_directory_endpoint_dataset.dataset_id"],
            name="provider_directory_dataset_proof_shard_dataset_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "dataset_id",
            "shard_id",
            name="provider_directory_dataset_proof_shard_pkey",
        ),
        schema=schema,
    )
    op.create_index(
        "provider_directory_dataset_proof_shard_root_idx",
        table_name,
        ["dataset_id", "acquisition_root_run_id", "shard_id"],
        schema=schema,
    )


def downgrade() -> None:
    op.drop_table(
        "provider_directory_dataset_proof_shard",
        schema=_schema(),
    )
