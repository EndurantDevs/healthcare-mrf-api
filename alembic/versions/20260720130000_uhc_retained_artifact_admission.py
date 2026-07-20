"""Add immutable retained-file UHC admission state.

Revision ID: 20260720130000_uhc_retained_artifact_admission
Revises: 20260720120000_provider_directory_profile_build_checkpoint
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa

from db.migration_index_adoption import create_index_if_missing


revision = "20260720130000_uhc_retained_artifact_admission"
down_revision = "20260720120000_provider_directory_profile_build_checkpoint"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _timestamp(name: str, *, nullable: bool = False) -> sa.Column:
    return sa.Column(
        name,
        sa.TIMESTAMP(timezone=True),
        server_default=None if nullable else sa.func.now(),
        nullable=nullable,
    )


def upgrade() -> None:
    schema = _schema()
    op.create_table(
        "provider_directory_uhc_raw_artifact",
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("byte_count", sa.BigInteger(), nullable=False),
        sa.Column("storage_uri", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        _timestamp("verified_at"),
        _timestamp("created_at"),
        sa.CheckConstraint(
            "artifact_sha256 ~ '^[0-9a-f]{64}$' AND byte_count > 0 "
            "AND storage_uri <> ''",
            name="provider_directory_uhc_raw_artifact_proof_check",
        ),
        sa.CheckConstraint(
            "status IN ('verified', 'quarantined')",
            name="provider_directory_uhc_raw_artifact_status_check",
        ),
        sa.PrimaryKeyConstraint(
            "artifact_sha256",
            name="provider_directory_uhc_raw_artifact_pkey",
        ),
        schema=schema,
    )
    op.create_table(
        "provider_directory_uhc_raw_layout",
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("contract_version", sa.Integer(), nullable=False),
        sa.Column("range_count", sa.Integer(), nullable=False),
        sa.Column("record_count", sa.BigInteger(), nullable=False),
        sa.Column("contract_id", sa.String(length=128), nullable=False),
        sa.Column("canonicalization_id", sa.String(length=128), nullable=False),
        sa.Column("producer_build_id", sa.String(length=256), nullable=False),
        sa.Column("range_set_sha256", sa.String(length=64), nullable=False),
        sa.Column("canonical_byte_count", sa.BigInteger(), nullable=False),
        sa.Column("manifest_sha256", sa.String(length=64), nullable=False),
        sa.Column("manifest_byte_count", sa.BigInteger(), nullable=False),
        sa.Column("manifest_storage_uri", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        _timestamp("verified_at"),
        _timestamp("created_at"),
        sa.CheckConstraint(
            "contract_version > 0 AND range_count >= 4 AND range_count <= 256 "
            "AND record_count > 0 AND contract_id <> '' "
            "AND canonicalization_id <> '' AND producer_build_id <> '' "
            "AND range_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND canonical_byte_count > 0 "
            "AND manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND manifest_byte_count > 0 AND manifest_storage_uri <> ''",
            name="provider_directory_uhc_raw_layout_proof_check",
        ),
        sa.CheckConstraint(
            "status IN ('verified', 'quarantined')",
            name="provider_directory_uhc_raw_layout_status_check",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_sha256"],
            [f"{schema}.provider_directory_uhc_raw_artifact.artifact_sha256"],
            name="provider_directory_uhc_raw_layout_artifact_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "artifact_sha256",
            "contract_version",
            "range_count",
            name="provider_directory_uhc_raw_layout_pkey",
        ),
        sa.UniqueConstraint(
            "artifact_sha256",
            "contract_version",
            "range_count",
            "manifest_sha256",
            name="provider_directory_uhc_raw_layout_manifest_key",
        ),
        schema=schema,
    )
    op.create_table(
        "provider_directory_uhc_source_binding",
        sa.Column("catalog_set_sha256", sa.String(length=64), nullable=False),
        sa.Column("source_file_id", sa.String(length=64), nullable=False),
        sa.Column("family", sa.String(length=8), nullable=False),
        sa.Column("collection_kind", sa.String(length=32), nullable=False),
        sa.Column("file_name", sa.String(length=256), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("catalog_modified_at", sa.String(length=64), nullable=False),
        sa.Column("size_bytes", sa.BigInteger()),
        sa.Column("catalog_entry_sha256", sa.String(length=64), nullable=False),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        _timestamp("bound_at"),
        _timestamp("released_at", nullable=True),
        sa.CheckConstraint(
            "source_file_id ~ '^[0-9a-f]{64}$' "
            "AND catalog_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND catalog_entry_sha256 ~ '^[0-9a-f]{64}$'",
            name="provider_directory_uhc_source_binding_digest_check",
        ),
        sa.CheckConstraint(
            "family IN ('cs', 'ifp') "
            "AND collection_kind IN ('provider_membership', 'plan_reference') "
            "AND file_name <> '' AND source_url <> '' "
            "AND catalog_modified_at <> '' "
            "AND (size_bytes IS NULL OR size_bytes >= 0)",
            name="provider_directory_uhc_source_binding_values_check",
        ),
        sa.ForeignKeyConstraint(
            ["catalog_set_sha256", "source_file_id"],
            [
                f"{schema}.provider_directory_uhc_catalog_file.catalog_set_sha256",
                f"{schema}.provider_directory_uhc_catalog_file.file_id",
            ],
            name="provider_directory_uhc_source_binding_catalog_file_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_sha256"],
            [f"{schema}.provider_directory_uhc_raw_artifact.artifact_sha256"],
            name="provider_directory_uhc_source_binding_artifact_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "catalog_set_sha256",
            "source_file_id",
            name="provider_directory_uhc_source_binding_pkey",
        ),
        schema=schema,
    )
    op.create_table(
        "provider_directory_uhc_raw_range",
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("contract_version", sa.Integer(), nullable=False),
        sa.Column("range_count", sa.Integer(), nullable=False),
        sa.Column("range_ordinal", sa.Integer(), nullable=False),
        sa.Column("raw_byte_start", sa.BigInteger(), nullable=False),
        sa.Column("raw_byte_end", sa.BigInteger(), nullable=False),
        sa.Column("raw_byte_count", sa.BigInteger(), nullable=False),
        sa.Column("raw_sha256", sa.String(length=64), nullable=False),
        sa.Column("record_start", sa.BigInteger(), nullable=False),
        sa.Column("record_end", sa.BigInteger(), nullable=False),
        sa.Column("record_count", sa.BigInteger(), nullable=False),
        sa.Column("canonical_sha256", sa.String(length=64), nullable=False),
        sa.Column("canonical_byte_count", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        _timestamp("verified_at"),
        sa.CheckConstraint(
            "contract_version > 0 "
            "AND range_count >= 4 AND range_count <= 256 "
            "AND range_ordinal >= 0 AND range_ordinal < range_count "
            "AND raw_byte_start >= 0 AND raw_byte_end > raw_byte_start "
            "AND raw_byte_count = raw_byte_end - raw_byte_start "
            "AND raw_sha256 ~ '^[0-9a-f]{64}$' "
            "AND record_start >= 0 AND record_end > record_start "
            "AND record_count = record_end - record_start "
            "AND canonical_sha256 ~ '^[0-9a-f]{64}$' "
            "AND canonical_byte_count > 0",
            name="provider_directory_uhc_raw_range_proof_check",
        ),
        sa.CheckConstraint(
            "status IN ('verified', 'quarantined')",
            name="provider_directory_uhc_raw_range_status_check",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_sha256", "contract_version", "range_count"],
            [
                f"{schema}.provider_directory_uhc_raw_layout.artifact_sha256",
                f"{schema}.provider_directory_uhc_raw_layout.contract_version",
                f"{schema}.provider_directory_uhc_raw_layout.range_count",
            ],
            name="provider_directory_uhc_raw_range_layout_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "artifact_sha256",
            "contract_version",
            "range_count",
            "range_ordinal",
            name="provider_directory_uhc_raw_range_pkey",
        ),
        schema=schema,
    )
    op.create_table(
        "provider_directory_uhc_artifact_reference",
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("artifact_kind", sa.String(length=16), nullable=False),
        sa.Column("layout_artifact_sha256", sa.String(length=64)),
        sa.Column("contract_version", sa.Integer(), nullable=False),
        sa.Column("range_count", sa.Integer(), nullable=False),
        sa.Column("catalog_set_sha256", sa.String(length=64), nullable=False),
        sa.Column("source_file_id", sa.String(length=64), nullable=False),
        sa.Column("storage_uri", sa.Text(), nullable=False),
        _timestamp("created_at"),
        _timestamp("retain_until", nullable=True),
        _timestamp("released_at", nullable=True),
        sa.CheckConstraint(
            "content_sha256 ~ '^[0-9a-f]{64}$' "
            "AND (layout_artifact_sha256 IS NULL OR "
            "layout_artifact_sha256 ~ '^[0-9a-f]{64}$') "
            "AND catalog_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND source_file_id ~ '^[0-9a-f]{64}$' AND storage_uri <> ''",
            name="provider_directory_uhc_artifact_reference_proof_check",
        ),
        sa.CheckConstraint(
            "(artifact_kind = 'raw' AND layout_artifact_sha256 IS NULL "
            "AND contract_version = 0 "
            "AND range_count = 0) OR "
            "(artifact_kind = 'manifest' "
            "AND layout_artifact_sha256 IS NOT NULL "
            "AND contract_version > 0 "
            "AND range_count >= 4 AND range_count <= 256)",
            name="provider_directory_uhc_artifact_reference_kind_check",
        ),
        sa.ForeignKeyConstraint(
            [
                "layout_artifact_sha256",
                "contract_version",
                "range_count",
                "content_sha256",
            ],
            [
                f"{schema}.provider_directory_uhc_raw_layout.artifact_sha256",
                f"{schema}.provider_directory_uhc_raw_layout.contract_version",
                f"{schema}.provider_directory_uhc_raw_layout.range_count",
                f"{schema}.provider_directory_uhc_raw_layout.manifest_sha256",
            ],
            name="provider_directory_uhc_artifact_reference_layout_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["catalog_set_sha256", "source_file_id"],
            [
                f"{schema}.provider_directory_uhc_source_binding.catalog_set_sha256",
                f"{schema}.provider_directory_uhc_source_binding.source_file_id",
            ],
            name="provider_directory_uhc_artifact_reference_source_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "catalog_set_sha256",
            "source_file_id",
            "artifact_kind",
            "contract_version",
            "range_count",
            name="provider_directory_uhc_artifact_reference_pkey",
        ),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "provider_directory_uhc_artifact_reference_gc_idx",
        "provider_directory_uhc_artifact_reference",
        ["released_at", "retain_until", "artifact_kind"],
        schema=schema,
    )


def downgrade() -> None:
    schema = _schema()
    for table_name in (
        "provider_directory_uhc_artifact_reference",
        "provider_directory_uhc_raw_range",
        "provider_directory_uhc_source_binding",
        "provider_directory_uhc_raw_layout",
        "provider_directory_uhc_raw_artifact",
    ):
        op.drop_table(table_name, schema=schema)
