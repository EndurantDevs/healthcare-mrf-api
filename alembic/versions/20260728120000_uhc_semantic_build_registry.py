"""Add durable UHC semantic build and seal state.

Revision ID: 20260728120000_uhc_semantic_build_registry
Revises: 20260727140000_ptg2_frozen_rate_file_binding
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa

from db.migration_index_adoption import create_index_if_missing


revision = "20260728120000_uhc_semantic_build_registry"
down_revision = "20260727140000_ptg2_frozen_rate_file_binding"
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
        "provider_directory_uhc_semantic_build",
        sa.Column("semantic_build_id", sa.String(length=64), nullable=False),
        sa.Column("catalog_set_sha256", sa.String(length=64), nullable=False),
        sa.Column("source_file_id", sa.String(length=64), nullable=False),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("raw_contract_version", sa.Integer(), nullable=False),
        sa.Column("raw_range_count", sa.Integer(), nullable=False),
        sa.Column("collection_kind", sa.String(length=32), nullable=False),
        sa.Column("semantic_contract_id", sa.String(length=128), nullable=False),
        sa.Column("semantic_contract_version", sa.Integer(), nullable=False),
        sa.Column("copy_format_id", sa.String(length=128), nullable=False),
        sa.Column("encoder_sha256", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("lease_token", sa.String(length=64)),
        _timestamp("lease_expires_at", nullable=True),
        _timestamp("heartbeat_at", nullable=True),
        sa.Column("stage_schema", sa.String(length=63), nullable=False),
        sa.Column("stage_relation", sa.String(length=63), nullable=False),
        sa.Column("fact_count", sa.BigInteger()),
        sa.Column("evidence_count", sa.BigInteger()),
        sa.Column("fact_set_sha256", sa.String(length=64)),
        sa.Column("record_identity_set_sha256", sa.String(length=64)),
        sa.Column("evidence_identity_set_sha256", sa.String(length=64)),
        sa.Column("evidence_layout_set_sha256", sa.String(length=64)),
        sa.Column("verifier_sha256", sa.String(length=64)),
        sa.Column("counters_json", sa.JSON(), nullable=True),
        sa.Column("fact_blocks_json", sa.JSON(), nullable=True),
        sa.Column("evidence_ranges_json", sa.JSON(), nullable=True),
        sa.Column("failure_code", sa.String(length=128)),
        _timestamp("verified_at", nullable=True),
        _timestamp("sealed_at", nullable=True),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        sa.CheckConstraint(
            "semantic_build_id ~ '^[0-9a-f]{64}$' "
            "AND catalog_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND source_file_id ~ '^[0-9a-f]{64}$' "
            "AND artifact_sha256 ~ '^[0-9a-f]{64}$' "
            "AND encoder_sha256 ~ '^[0-9a-f]{64}$' "
            "AND raw_contract_version > 0 "
            "AND raw_range_count >= 4 AND raw_range_count <= 256 "
            "AND semantic_contract_version > 0 "
            "AND semantic_contract_id <> '' AND copy_format_id <> '' "
            "AND attempt_count > 0 AND stage_schema <> '' "
            "AND stage_relation <> ''",
            name="provider_directory_uhc_semantic_build_identity_check",
        ),
        sa.CheckConstraint(
            "collection_kind IN ('provider_membership', 'plan_reference') "
            "AND status IN ('building', 'sealed', 'quarantined')",
            name="provider_directory_uhc_semantic_build_state_check",
        ),
        sa.CheckConstraint(
            "(status = 'building' AND lease_token IS NOT NULL "
            "AND lease_token ~ '^[0-9a-f]{64}$' "
            "AND lease_expires_at IS NOT NULL AND heartbeat_at IS NOT NULL "
            "AND fact_count IS NULL AND evidence_count IS NULL "
            "AND fact_set_sha256 IS NULL "
            "AND record_identity_set_sha256 IS NULL "
            "AND evidence_identity_set_sha256 IS NULL "
            "AND evidence_layout_set_sha256 IS NULL "
            "AND verifier_sha256 IS NULL AND counters_json IS NULL "
            "AND fact_blocks_json IS NULL AND evidence_ranges_json IS NULL "
            "AND failure_code IS NULL AND verified_at IS NULL "
            "AND sealed_at IS NULL) OR "
            "(status = 'sealed' AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND fact_count > 0 "
            "AND evidence_count >= 0 "
            "AND fact_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND record_identity_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND evidence_identity_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND evidence_layout_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND verifier_sha256 ~ '^[0-9a-f]{64}$' "
            "AND counters_json IS NOT NULL "
            "AND fact_blocks_json IS NOT NULL "
            "AND evidence_ranges_json IS NOT NULL "
            "AND failure_code IS NULL AND verified_at IS NOT NULL "
            "AND sealed_at IS NOT NULL) OR "
            "(status = 'quarantined' AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND failure_code IS NOT NULL)",
            name="provider_directory_uhc_semantic_build_proof_state_check",
        ),
        sa.ForeignKeyConstraint(
            ["catalog_set_sha256", "source_file_id"],
            [
                f"{schema}.provider_directory_uhc_source_binding.catalog_set_sha256",
                f"{schema}.provider_directory_uhc_source_binding.source_file_id",
            ],
            name="provider_directory_uhc_semantic_build_source_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_sha256", "raw_contract_version", "raw_range_count"],
            [
                f"{schema}.provider_directory_uhc_raw_layout.artifact_sha256",
                f"{schema}.provider_directory_uhc_raw_layout.contract_version",
                f"{schema}.provider_directory_uhc_raw_layout.range_count",
            ],
            name="provider_directory_uhc_semantic_build_layout_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "semantic_build_id",
            name="provider_directory_uhc_semantic_build_pkey",
        ),
        sa.UniqueConstraint(
            "catalog_set_sha256",
            "source_file_id",
            "semantic_contract_id",
            "semantic_contract_version",
            "encoder_sha256",
            name="provider_directory_uhc_semantic_build_identity_key",
        ),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "provider_directory_uhc_semantic_build_lease_idx",
        "provider_directory_uhc_semantic_build",
        ["status", "lease_expires_at"],
        schema=schema,
    )


def downgrade() -> None:
    op.drop_table(
        "provider_directory_uhc_semantic_build",
        schema=_schema(),
    )
