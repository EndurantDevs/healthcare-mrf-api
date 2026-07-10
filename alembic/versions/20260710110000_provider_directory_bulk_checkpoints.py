"""Add Provider Directory Bulk Data checkpoints.

Revision ID: 20260710110000_provider_directory_bulk_checkpoints
Revises: 20260710010000_provider_directory_pagination_checkpoint
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260710110000_provider_directory_bulk_checkpoints"
down_revision = "20260710010000_provider_directory_pagination_checkpoint"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()
    op.add_column(
        "provider_directory_endpoint_dataset",
        sa.Column("acquisition_root_run_id", sa.String(length=64), nullable=True),
        schema=schema,
    )
    endpoint_dataset = sa.table(
        "provider_directory_endpoint_dataset",
        sa.column("import_run_id", sa.String(length=64)),
        sa.column("acquisition_root_run_id", sa.String(length=64)),
        schema=schema,
    )
    op.execute(
        endpoint_dataset.update()
        .where(endpoint_dataset.c.acquisition_root_run_id.is_(None))
        .where(endpoint_dataset.c.import_run_id.is_not(None))
        .values(acquisition_root_run_id=endpoint_dataset.c.import_run_id)
    )
    op.create_index(
        "provider_directory_endpoint_dataset_acquisition_root_idx",
        "provider_directory_endpoint_dataset",
        ["endpoint_id", "acquisition_root_run_id"],
        unique=True,
        schema=schema,
        postgresql_where=sa.text("acquisition_root_run_id IS NOT NULL"),
    )

    op.add_column(
        "provider_directory_pagination_checkpoint",
        sa.Column("acquisition_root_run_id", sa.String(length=64), nullable=True),
        schema=schema,
    )
    pagination_checkpoint = sa.table(
        "provider_directory_pagination_checkpoint",
        sa.column("owner_run_id", sa.String(length=64)),
        sa.column("acquisition_root_run_id", sa.String(length=64)),
        schema=schema,
    )
    op.execute(
        pagination_checkpoint.update().values(
            acquisition_root_run_id=pagination_checkpoint.c.owner_run_id
        )
    )
    op.alter_column(
        "provider_directory_pagination_checkpoint",
        "acquisition_root_run_id",
        existing_type=sa.String(length=64),
        nullable=False,
        schema=schema,
    )

    op.create_table(
        "provider_directory_bulk_acquisition_checkpoint",
        sa.Column("checkpoint_id", sa.String(length=64), nullable=False),
        sa.Column("canonical_api_base", sa.Text(), nullable=False),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("source_scope_hash", sa.String(length=64), nullable=False),
        sa.Column("strategy_version", sa.String(length=64), nullable=False),
        sa.Column("acquisition_root_run_id", sa.String(length=64), nullable=False),
        sa.Column("owner_run_id", sa.String(length=64), nullable=False),
        sa.Column("retry_of_run_id", sa.String(length=64), nullable=True),
        sa.Column("endpoint_id", sa.String(length=64), nullable=False),
        sa.Column("dataset_id", sa.String(length=96), nullable=False),
        sa.Column("start_url_hash", sa.String(length=64), nullable=False),
        sa.Column("status_url_ciphertext", sa.Text(), nullable=True),
        sa.Column("status_url_hash", sa.String(length=64), nullable=True),
        sa.Column("manifest_hash", sa.String(length=64), nullable=True),
        sa.Column("manifest_ciphertext", sa.Text(), nullable=True),
        sa.Column("manifest_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("lease_expires_at", sa.TIMESTAMP(), nullable=True),
        sa.Column(
            "rows_written",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.func.now(), nullable=False),
        sa.Column("accepted_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("last_polled_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("manifest_received_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("completed_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("failed_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint(
            "checkpoint_id",
            name="provider_directory_bulk_acquisition_checkpoint_pkey",
        ),
        sa.UniqueConstraint(
            "canonical_api_base",
            "resource_type",
            "source_scope_hash",
            "strategy_version",
            "acquisition_root_run_id",
            "dataset_id",
            name="provider_directory_bulk_acquisition_identity_key",
        ),
        sa.ForeignKeyConstraint(
            ["endpoint_id"],
            [f"{schema}.provider_directory_api_endpoint.endpoint_id"],
            name="provider_directory_bulk_acquisition_endpoint_id_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["dataset_id"],
            [f"{schema}.provider_directory_endpoint_dataset.dataset_id"],
            name="provider_directory_bulk_acquisition_dataset_id_fkey",
        ),
        schema=schema,
    )
    op.create_index(
        "provider_directory_bulk_acquisition_dataset_idx",
        "provider_directory_bulk_acquisition_checkpoint",
        ["dataset_id"],
        schema=schema,
    )
    op.create_index(
        "provider_directory_bulk_acquisition_owner_idx",
        "provider_directory_bulk_acquisition_checkpoint",
        ["owner_run_id"],
        schema=schema,
    )
    op.create_index(
        "provider_directory_bulk_acquisition_root_idx",
        "provider_directory_bulk_acquisition_checkpoint",
        ["acquisition_root_run_id"],
        schema=schema,
    )
    op.create_index(
        "provider_directory_bulk_acquisition_state_updated_idx",
        "provider_directory_bulk_acquisition_checkpoint",
        ["state", "updated_at"],
        schema=schema,
    )

    op.create_table(
        "provider_directory_bulk_output_checkpoint",
        sa.Column("checkpoint_id", sa.String(length=64), nullable=False),
        sa.Column("output_id", sa.String(length=64), nullable=False),
        sa.Column("output_index", sa.Integer(), nullable=False),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("output_url_ciphertext", sa.Text(), nullable=True),
        sa.Column("output_url_hash", sa.String(length=64), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column(
            "rows_written",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.func.now(), nullable=False),
        sa.Column("started_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("completed_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint(
            "checkpoint_id",
            "output_id",
            name="provider_directory_bulk_output_checkpoint_pkey",
        ),
        sa.UniqueConstraint(
            "checkpoint_id",
            "output_index",
            name="provider_directory_bulk_output_index_key",
        ),
        sa.ForeignKeyConstraint(
            ["checkpoint_id"],
            [f"{schema}.provider_directory_bulk_acquisition_checkpoint.checkpoint_id"],
            name="provider_directory_bulk_output_checkpoint_id_fkey",
            ondelete="CASCADE",
        ),
        schema=schema,
    )
    op.create_index(
        "provider_directory_bulk_output_state_updated_idx",
        "provider_directory_bulk_output_checkpoint",
        ["state", "updated_at"],
        schema=schema,
    )


def downgrade():
    schema = _schema()
    op.drop_table("provider_directory_bulk_output_checkpoint", schema=schema)
    op.drop_table("provider_directory_bulk_acquisition_checkpoint", schema=schema)
    op.drop_column(
        "provider_directory_pagination_checkpoint",
        "acquisition_root_run_id",
        schema=schema,
    )
    op.drop_index(
        "provider_directory_endpoint_dataset_acquisition_root_idx",
        table_name="provider_directory_endpoint_dataset",
        schema=schema,
    )
    op.drop_column(
        "provider_directory_endpoint_dataset",
        "acquisition_root_run_id",
        schema=schema,
    )
