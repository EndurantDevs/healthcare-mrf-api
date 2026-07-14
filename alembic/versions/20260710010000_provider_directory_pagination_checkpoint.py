"""Add Provider Directory pagination checkpoints.

Revision ID: 20260710010000_provider_directory_pagination_checkpoint
Revises: 20260710003000_provider_directory_endpoint_datasets
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from db.migration_adoption import create_table_or_validate
from db.migration_index_adoption import create_index_if_missing


revision = "20260710010000_provider_directory_pagination_checkpoint"
down_revision = "20260710003000_provider_directory_endpoint_datasets"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()
    create_table_or_validate(
        op,
        "provider_directory_pagination_checkpoint",
        sa.Column("canonical_api_base", sa.Text(), nullable=False),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("source_scope_hash", sa.String(length=64), nullable=False),
        sa.Column("dataset_id", sa.String(length=96), nullable=True),
        sa.Column("source_ids", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("owner_run_id", sa.String(length=64), nullable=False),
        sa.Column("retry_of_run_id", sa.String(length=64), nullable=True),
        sa.Column("start_url_hash", sa.String(length=64), nullable=False),
        sa.Column("next_url", sa.Text(), nullable=True),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column(
            "pages_processed",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "rows_processed",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "recent_cursor_hashes",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), server_default=sa.func.now(), nullable=False),
        sa.Column("completed_at", sa.TIMESTAMP(), nullable=True),
        sa.PrimaryKeyConstraint(
            "canonical_api_base",
            "resource_type",
            "source_scope_hash",
            name="provider_directory_pagination_checkpoint_pkey",
        ),
        sa.ForeignKeyConstraint(
            ["dataset_id"],
            [f"{schema}.provider_directory_endpoint_dataset.dataset_id"],
            name="provider_directory_pagination_checkpoint_dataset_id_fkey",
        ),
        schema=schema,
        accepted_primary_keys=(
            (
                "canonical_api_base",
                "resource_type",
                "source_scope_hash",
            ),
            (
                "canonical_api_base",
                "resource_type",
                "source_scope_hash",
                "acquisition_root_run_id",
            ),
        ),
    )
    create_index_if_missing(
        op,
        "provider_directory_pagination_checkpoint_owner_idx",
        "provider_directory_pagination_checkpoint",
        ["owner_run_id"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "provider_directory_pagination_checkpoint_state_updated_idx",
        "provider_directory_pagination_checkpoint",
        ["state", "updated_at"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "provider_directory_pagination_checkpoint_dataset_idx",
        "provider_directory_pagination_checkpoint",
        ["dataset_id"],
        schema=schema,
    )


def downgrade():
    op.drop_table("provider_directory_pagination_checkpoint", schema=_schema())
