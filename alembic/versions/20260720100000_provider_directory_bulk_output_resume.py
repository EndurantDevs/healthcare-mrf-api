"""Persist resumable Provider Directory Bulk output validators.

Revision ID: 20260720100000_provider_directory_bulk_output_resume
Revises: 20260717180000_ptg2_v4_witness_capacity
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa

from db.migration_adoption import add_column_if_missing


revision = "20260720100000_provider_directory_bulk_output_resume"
down_revision = "20260717180000_ptg2_v4_witness_capacity"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade() -> None:
    schema = _schema()
    table = "provider_directory_bulk_output_checkpoint"
    columns = (
        sa.Column("content_length_bytes", sa.BigInteger(), nullable=True),
        sa.Column("etag_ciphertext", sa.Text(), nullable=True),
        sa.Column("etag_hash", sa.String(length=64), nullable=True),
        sa.Column(
            "committed_bytes",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("output_expires_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("validator_checked_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_error_at", sa.TIMESTAMP(), nullable=True),
    )
    for column in columns:
        add_column_if_missing(op, table, column, schema=schema)


def downgrade() -> None:
    schema = _schema()
    table = "provider_directory_bulk_output_checkpoint"
    for column_name in (
        "last_error_at",
        "last_error",
        "validator_checked_at",
        "output_expires_at",
        "committed_bytes",
        "etag_hash",
        "etag_ciphertext",
        "content_length_bytes",
    ):
        op.drop_column(table, column_name, schema=schema)
