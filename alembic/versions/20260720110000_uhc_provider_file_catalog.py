"""Persist immutable UHC provider-file catalog history.

Revision ID: 20260720110000_uhc_provider_file_catalog
Revises: 20260720100000_provider_directory_bulk_output_resume
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from db.migration_index_adoption import create_index_if_missing


revision = "20260720110000_uhc_provider_file_catalog"
down_revision = "20260720100000_provider_directory_bulk_output_resume"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade() -> None:
    schema = _schema()
    op.create_table(
        "provider_directory_uhc_catalog_set",
        sa.Column("catalog_set_sha256", sa.String(length=64), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("families_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "collection_summary_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("file_count", sa.Integer(), nullable=False),
        sa.Column("provider_file_count", sa.Integer(), nullable=False),
        sa.Column("plan_reference_file_count", sa.Integer(), nullable=False),
        sa.Column("first_observed_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("last_observed_at", sa.TIMESTAMP(), nullable=False),
        sa.CheckConstraint(
            "catalog_set_sha256 ~ '^[0-9a-f]{64}$' AND schema_version=1",
            name="provider_directory_uhc_catalog_set_identity_check",
        ),
        sa.CheckConstraint(
            "file_count>=0 AND provider_file_count>=0 "
            "AND plan_reference_file_count>=0 "
            "AND file_count=provider_file_count+plan_reference_file_count",
            name="provider_directory_uhc_catalog_set_counts_check",
        ),
        sa.PrimaryKeyConstraint(
            "catalog_set_sha256",
            name="provider_directory_uhc_catalog_set_pkey",
        ),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "provider_directory_uhc_catalog_set_latest_idx",
        "provider_directory_uhc_catalog_set",
        ["last_observed_at"],
        schema=schema,
    )
    op.create_table(
        "provider_directory_uhc_catalog_file",
        sa.Column("catalog_set_sha256", sa.String(length=64), nullable=False),
        sa.Column("file_id", sa.String(length=64), nullable=False),
        sa.Column("family", sa.String(length=8), nullable=False),
        sa.Column("collection_kind", sa.String(length=32), nullable=False),
        sa.Column("file_name", sa.String(length=256), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("catalog_modified_at", sa.String(length=64), nullable=False),
        sa.Column("catalog_entry_sha256", sa.String(length=64), nullable=False),
        sa.Column("size_bytes", sa.BigInteger()),
        sa.Column("availability", sa.String(length=32), nullable=False),
        sa.Column("catalog_support", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.CheckConstraint(
            "family IN ('cs', 'ifp') AND collection_kind IN "
            "('provider_membership', 'plan_reference')",
            name="provider_directory_uhc_catalog_file_collection_check",
        ),
        sa.CheckConstraint(
            "file_id ~ '^[0-9a-f]{64}$' "
            "AND catalog_entry_sha256 ~ '^[0-9a-f]{64}$' "
            "AND (size_bytes IS NULL OR "
            "(size_bytes>=0 AND size_bytes<=9223372036854775807)) "
            "AND availability IN ('published', 'not_published_by_source') "
            "AND catalog_support IN ('cataloged', 'not_applicable')",
            name="provider_directory_uhc_catalog_file_values_check",
        ),
        sa.ForeignKeyConstraint(
            ["catalog_set_sha256"],
            [f"{schema}.provider_directory_uhc_catalog_set.catalog_set_sha256"],
            name="provider_directory_uhc_catalog_file_set_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "catalog_set_sha256",
            "file_id",
            name="provider_directory_uhc_catalog_file_pkey",
        ),
        sa.UniqueConstraint(
            "catalog_set_sha256",
            "family",
            "collection_kind",
            "file_name",
            name="provider_directory_uhc_catalog_file_logical_key",
        ),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "provider_directory_uhc_catalog_file_browse_idx",
        "provider_directory_uhc_catalog_file",
        ["catalog_set_sha256", "family", "collection_kind", "file_name"],
        schema=schema,
    )
    op.create_table(
        "provider_directory_uhc_catalog_raw_observation",
        sa.Column("raw_set_sha256", sa.String(length=64), nullable=False),
        sa.Column("catalog_set_sha256", sa.String(length=64), nullable=False),
        sa.Column("raw_documents_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("first_observed_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("last_observed_at", sa.TIMESTAMP(), nullable=False),
        sa.CheckConstraint(
            "raw_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND catalog_set_sha256 ~ '^[0-9a-f]{64}$'",
            name="provider_directory_uhc_catalog_raw_hashes_check",
        ),
        sa.ForeignKeyConstraint(
            ["catalog_set_sha256"],
            [f"{schema}.provider_directory_uhc_catalog_set.catalog_set_sha256"],
            name="provider_directory_uhc_catalog_raw_set_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "raw_set_sha256",
            name="provider_directory_uhc_catalog_raw_observation_pkey",
        ),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "provider_directory_uhc_catalog_raw_latest_idx",
        "provider_directory_uhc_catalog_raw_observation",
        ["last_observed_at"],
        schema=schema,
    )


def downgrade() -> None:
    schema = _schema()
    op.drop_table("provider_directory_uhc_catalog_raw_observation", schema=schema)
    op.drop_table("provider_directory_uhc_catalog_file", schema=schema)
    op.drop_table("provider_directory_uhc_catalog_set", schema=schema)
