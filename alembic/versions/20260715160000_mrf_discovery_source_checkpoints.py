"""Add retry-safe MRF discovery source checkpoints.

Revision ID: 20260715160000_mrf_discovery_source_checkpoints
Revises: 20260715120000_ptg2_v3_source_audit_witness
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from db.migration_adoption import create_table_or_validate
from db.migration_index_adoption import create_index_if_missing


revision = "20260715160000_mrf_discovery_source_checkpoints"
down_revision = "20260715120000_ptg2_v3_source_audit_witness"
branch_labels = None
depends_on = None


RETRY_INDEX_NAME = "import_run_mrf_discovery_retry_child_idx"


def _schema() -> str:
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()
    create_table_or_validate(
        op,
        "mrf_discovery_batch",
        sa.Column("root_run_id", sa.String(length=64), nullable=False),
        sa.Column("latest_run_id", sa.String(length=64), nullable=False),
        sa.Column("retry_of_run_id", sa.String(length=64), nullable=True),
        sa.Column("strategy_version", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("source_set_count", sa.Integer(), nullable=False),
        sa.Column("source_set_sha256", sa.String(length=64), nullable=False),
        sa.Column("source_payload_set_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "completed_source_count",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "failed_source_count",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "urls_checked", sa.Integer(), server_default=sa.text("0"), nullable=False
        ),
        sa.Column(
            "plans_discovered",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "files_discovered",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "bytes_streamed",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("lease_expires_at", sa.TIMESTAMP(), nullable=True),
        sa.Column(
            "started_at", sa.TIMESTAMP(), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.TIMESTAMP(), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("completed_at", sa.TIMESTAMP(), nullable=True),
        sa.PrimaryKeyConstraint(
            "root_run_id", name="mrf_discovery_batch_pkey"
        ),
        sa.CheckConstraint(
            "status IN ('running', 'failed', 'succeeded')",
            name="mrf_discovery_batch_status_check",
        ),
        sa.CheckConstraint(
            "source_set_count >= 0 AND completed_source_count >= 0 "
            "AND failed_source_count >= 0",
            name="mrf_discovery_batch_count_check",
        ),
        sa.CheckConstraint(
            "length(source_set_sha256) = 64 "
            "AND length(source_payload_set_sha256) = 64",
            name="mrf_discovery_batch_digest_check",
        ),
        schema=schema,
    )
    create_table_or_validate(
        op,
        "mrf_discovery_source_checkpoint",
        sa.Column("root_run_id", sa.String(length=64), nullable=False),
        sa.Column("source_id", sa.String(length=64), nullable=False),
        sa.Column("owner_run_id", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column(
            "source_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("source_payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("lease_expires_at", sa.TIMESTAMP(), nullable=True),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "urls_checked", sa.Integer(), server_default=sa.text("0"), nullable=False
        ),
        sa.Column(
            "plans_discovered",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "files_discovered",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "bytes_streamed",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "error", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
        sa.Column("started_at", sa.TIMESTAMP(), nullable=True),
        sa.Column(
            "updated_at", sa.TIMESTAMP(), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("completed_at", sa.TIMESTAMP(), nullable=True),
        sa.PrimaryKeyConstraint(
            "root_run_id",
            "source_id",
            name="mrf_discovery_source_checkpoint_pkey",
        ),
        sa.ForeignKeyConstraint(
            ["root_run_id"],
            [f"{schema}.mrf_discovery_batch.root_run_id"],
            name="mrf_discovery_source_checkpoint_batch_fkey",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'failed', 'succeeded')",
            name="mrf_discovery_source_checkpoint_status_check",
        ),
        sa.CheckConstraint(
            "attempt_count >= 0 AND urls_checked >= 0 "
            "AND plans_discovered >= 0 AND files_discovered >= 0 "
            "AND bytes_streamed >= 0",
            name="mrf_discovery_source_checkpoint_count_check",
        ),
        sa.CheckConstraint(
            "length(source_payload_sha256) = 64",
            name="mrf_discovery_source_checkpoint_digest_check",
        ),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "mrf_discovery_batch_latest_run_idx",
        "mrf_discovery_batch",
        ["latest_run_id"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "mrf_discovery_batch_status_idx",
        "mrf_discovery_batch",
        ["status", "updated_at"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "mrf_discovery_source_checkpoint_pending_idx",
        "mrf_discovery_source_checkpoint",
        ["root_run_id", "status", "source_id"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "mrf_discovery_source_checkpoint_owner_idx",
        "mrf_discovery_source_checkpoint",
        ["owner_run_id"],
        schema=schema,
    )
    op.execute(
        sa.text(
            f"""
            DO $migration$
            BEGIN
                IF EXISTS (
                    SELECT retry_of_run_id
                      FROM {schema}.import_run
                     WHERE importer = 'mrf-source-discovery'
                       AND retry_of_run_id IS NOT NULL
                     GROUP BY retry_of_run_id
                    HAVING count(*) > 1
                ) THEN
                    RAISE EXCEPTION 'mrf_discovery_duplicate_retry_children';
                END IF;
            END
            $migration$;
            """
        )
    )
    create_index_if_missing(
        op,
        RETRY_INDEX_NAME,
        "import_run",
        ["retry_of_run_id"],
        schema=schema,
        unique=True,
        postgresql_where=sa.text(
            "importer = 'mrf-source-discovery' AND retry_of_run_id IS NOT NULL"
        ),
    )


def downgrade():
    schema = _schema()
    op.drop_index(
        RETRY_INDEX_NAME,
        table_name="import_run",
        schema=schema,
    )
    op.drop_table("mrf_discovery_source_checkpoint", schema=schema)
    op.drop_table("mrf_discovery_batch", schema=schema)
