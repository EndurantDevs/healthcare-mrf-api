"""Add source-neutral retained Provider Directory artifact acquisition.

Revision ID: 20260721160000_provider_directory_retained_artifact_acquisition
Revises: 20260721150000_ptg2_source_witness_parts
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa

from db.migration_adoption import create_table_or_validate
from db.migration_index_adoption import create_index_if_missing


revision = "20260721160000_provider_directory_retained_artifact_acquisition"
down_revision = "20260721150000_ptg2_source_witness_parts"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _timestamp(name: str, *, nullable: bool = False) -> sa.Column:
    return sa.Column(
        name,
        sa.TIMESTAMP(timezone=True),
        nullable=nullable,
        server_default=None if nullable else sa.func.now(),
    )


def upgrade() -> None:
    schema = _schema()
    create_table_or_validate(
        op,
        "provider_directory_retained_artifact_campaign",
        sa.Column("campaign_id", sa.String(length=64), nullable=False),
        sa.Column("contract_id", sa.String(length=128), nullable=False),
        sa.Column("adapter_id", sa.String(length=64), nullable=False),
        sa.Column("endpoint_id", sa.String(length=64), nullable=False),
        sa.Column("request_fence_id", sa.String(length=64), nullable=False),
        sa.Column(
            "credential_descriptor_sha256",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column("source_census_sha256", sa.String(length=64), nullable=False),
        sa.Column("identity_members_sha256", sa.String(length=64), nullable=False),
        sa.Column("identity_member_count", sa.Integer(), nullable=False),
        sa.Column("census_mode", sa.String(length=24), nullable=False),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column("expected_item_count", sa.Integer(), nullable=False),
        sa.Column("expected_stream_count", sa.Integer(), nullable=False),
        sa.Column("per_item_byte_budget", sa.BigInteger(), nullable=False),
        sa.Column("aggregate_byte_budget", sa.BigInteger(), nullable=False),
        sa.Column(
            "disk_reserved_bytes",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "observed_item_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "expected_byte_count",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "admitted_item_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "admitted_byte_count",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "admitted_record_count",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "terminal_zero_item_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "terminal_stream_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "failed_item_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "unaccounted_item_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("item_census_sha256", sa.String(length=64)),
        sa.Column("observation_set_sha256", sa.String(length=64)),
        sa.Column("campaign_sha256", sa.String(length=64)),
        sa.Column(
            "complete",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("safe_failure_code", sa.String(length=64)),
        sa.Column("lease_owner", sa.String(length=128)),
        sa.Column(
            "lease_epoch",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        _timestamp("lease_expires_at", nullable=True),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        _timestamp("sealed_at", nullable=True),
        _timestamp("released_at", nullable=True),
        sa.CheckConstraint(
            "campaign_id ~ '^[0-9a-f]{64}$' "
            "AND source_census_sha256 ~ '^[0-9a-f]{64}$' "
            "AND request_fence_id ~ '^[0-9a-f]{64}$' "
            "AND endpoint_id ~ '^[0-9a-f]{64}$' "
            "AND credential_descriptor_sha256 ~ '^[0-9a-f]{64}$' "
            "AND identity_members_sha256 ~ '^[0-9a-f]{64}$' "
            "AND identity_member_count >= 0 "
            "AND contract_id <> '' AND adapter_id <> ''",
            name="pd_retained_campaign_identity_check",
        ),
        sa.CheckConstraint(
            "census_mode IN ('fixed_catalog', 'ordered_streams') "
            "AND state IN ('planned', 'preflighting', 'downloading', 'sealing', "
            "'sealed_incomplete', 'complete', 'failed', 'released')",
            name="pd_retained_campaign_mode_state_check",
        ),
        sa.CheckConstraint(
            "((census_mode = 'fixed_catalog' AND expected_item_count > 0 "
            "AND expected_stream_count = 0) OR "
            "(census_mode = 'ordered_streams' AND expected_item_count >= 0 "
            "AND expected_stream_count > 0)) "
            "AND expected_item_count <= 100000 "
            "AND expected_stream_count <= 4096 "
            "AND ((census_mode = 'fixed_catalog' "
            "AND identity_member_count = expected_item_count) OR "
            "(census_mode = 'ordered_streams' "
            "AND identity_member_count = expected_stream_count)) "
            "AND per_item_byte_budget > 0 AND aggregate_byte_budget > 0 "
            "AND disk_reserved_bytes >= 0 AND observed_item_count >= 0 "
            "AND expected_byte_count >= 0 AND admitted_item_count >= 0 "
            "AND admitted_byte_count >= 0 AND admitted_record_count >= 0 "
            "AND terminal_zero_item_count >= 0 AND terminal_stream_count >= 0 "
            "AND failed_item_count >= 0 AND unaccounted_item_count >= 0 "
            "AND observed_item_count <= expected_item_count "
            "AND admitted_item_count <= expected_item_count "
            "AND terminal_stream_count <= expected_stream_count "
            "AND expected_byte_count <= aggregate_byte_budget "
            "AND admitted_byte_count <= aggregate_byte_budget "
            "AND lease_epoch >= 0 "
            "AND ((lease_owner IS NULL AND lease_expires_at IS NULL) OR "
            "(lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL))",
            name="pd_retained_campaign_counts_check",
        ),
        sa.CheckConstraint(
            "(item_census_sha256 IS NULL OR item_census_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (observation_set_sha256 IS NULL OR "
            "observation_set_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (campaign_sha256 IS NULL OR campaign_sha256 ~ '^[0-9a-f]{64}$')",
            name="pd_retained_campaign_digest_check",
        ),
        sa.CheckConstraint(
            "(complete = FALSE) OR "
            "(state = 'complete' AND sealed_at IS NOT NULL "
            "AND item_census_sha256 IS NOT NULL "
            "AND observation_set_sha256 IS NOT NULL "
            "AND campaign_sha256 IS NOT NULL "
            "AND failed_item_count = 0 AND unaccounted_item_count = 0 "
            "AND admitted_item_count + terminal_zero_item_count = expected_item_count "
            "AND ((census_mode = 'fixed_catalog' "
            "AND terminal_zero_item_count = 0) OR "
            "(census_mode = 'ordered_streams' "
            "AND terminal_stream_count = expected_stream_count)))",
            name="pd_retained_campaign_complete_check",
        ),
        sa.PrimaryKeyConstraint(
            "campaign_id",
            name="provider_directory_retained_artifact_campaign_pkey",
        ),
        sa.UniqueConstraint(
            "contract_id",
            "adapter_id",
            "endpoint_id",
            "credential_descriptor_sha256",
            "source_census_sha256",
            "per_item_byte_budget",
            "aggregate_byte_budget",
            name="pd_retained_campaign_source_census_key",
        ),
        schema=schema,
    )
    create_table_or_validate(
        op,
        "provider_directory_retained_artifact_campaign_stream",
        sa.Column("campaign_id", sa.String(length=64), nullable=False),
        sa.Column("stream_identity_sha256", sa.String(length=64), nullable=False),
        sa.Column("stream_ordinal", sa.Integer(), nullable=False),
        sa.Column("terminal_sequence_ordinal", sa.Integer()),
        sa.Column("terminal_proof_sha256", sa.String(length=64)),
        sa.Column(
            "complete",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        _timestamp("created_at"),
        _timestamp("completed_at", nullable=True),
        sa.CheckConstraint(
            "stream_identity_sha256 ~ '^[0-9a-f]{64}$' "
            "AND stream_ordinal >= 0 "
            "AND (terminal_sequence_ordinal IS NULL OR "
            "terminal_sequence_ordinal >= 0) "
            "AND (terminal_proof_sha256 IS NULL OR "
            "terminal_proof_sha256 ~ '^[0-9a-f]{64}$') "
            "AND ((complete = FALSE) OR "
            "(terminal_sequence_ordinal IS NOT NULL "
            "AND terminal_proof_sha256 IS NOT NULL "
            "AND completed_at IS NOT NULL))",
            name="pd_retained_campaign_stream_proof_check",
        ),
        sa.ForeignKeyConstraint(
            ["campaign_id"],
            [f"{schema}.provider_directory_retained_artifact_campaign.campaign_id"],
            name="pd_retained_campaign_stream_campaign_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "campaign_id",
            "stream_identity_sha256",
            name="provider_directory_retained_artifact_campaign_stream_pkey",
        ),
        sa.UniqueConstraint(
            "campaign_id",
            "stream_ordinal",
            name="pd_retained_campaign_stream_ordinal_key",
        ),
        schema=schema,
    )
    create_table_or_validate(
        op,
        "provider_directory_retained_artifact_endpoint_fence",
        sa.Column("request_fence_id", sa.String(length=64), nullable=False),
        _timestamp("next_request_at"),
        sa.Column(
            "fence_epoch",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        sa.CheckConstraint(
            "request_fence_id ~ '^[0-9a-f]{64}$' AND fence_epoch >= 0",
            name="pd_retained_endpoint_fence_values_check",
        ),
        sa.PrimaryKeyConstraint(
            "request_fence_id",
            name="provider_directory_retained_artifact_endpoint_fence_pkey",
        ),
        schema=schema,
    )
    create_table_or_validate(
        op,
        "provider_directory_retained_artifact",
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("artifact_byte_count", sa.BigInteger(), nullable=False),
        sa.Column("artifact_locator", sa.Text(), nullable=False),
        sa.Column("registry_status", sa.String(length=16), nullable=False),
        _timestamp("verified_at"),
        _timestamp("created_at"),
        _timestamp("released_at", nullable=True),
        sa.CheckConstraint(
            "artifact_sha256 ~ '^[0-9a-f]{64}$' "
            "AND artifact_byte_count > 0 AND artifact_locator <> ''",
            name="pd_retained_artifact_proof_check",
        ),
        sa.CheckConstraint(
            "registry_status IN ('verified', 'quarantined', 'released') "
            "AND ((registry_status = 'released') = (released_at IS NOT NULL))",
            name="pd_retained_artifact_status_check",
        ),
        sa.PrimaryKeyConstraint(
            "artifact_sha256",
            name="provider_directory_retained_artifact_pkey",
        ),
        sa.UniqueConstraint(
            "artifact_locator",
            name="pd_retained_artifact_locator_key",
        ),
        schema=schema,
    )
    create_table_or_validate(
        op,
        "provider_directory_retained_artifact_layout",
        sa.Column("layout_sha256", sa.String(length=64), nullable=False),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("artifact_record_count", sa.BigInteger(), nullable=False),
        sa.Column("layout_contract_id", sa.String(length=128), nullable=False),
        sa.Column("layout_contract_version", sa.Integer(), nullable=False),
        sa.Column("layout_range_count", sa.Integer(), nullable=False),
        sa.Column("range_set_sha256", sa.String(length=64), nullable=False),
        sa.Column("canonical_byte_count", sa.BigInteger(), nullable=False),
        sa.Column("manifest_sha256", sa.String(length=64), nullable=False),
        sa.Column("manifest_byte_count", sa.BigInteger(), nullable=False),
        sa.Column("manifest_locator", sa.Text(), nullable=False),
        sa.Column("producer_build_id", sa.String(length=256), nullable=False),
        sa.Column("registry_status", sa.String(length=16), nullable=False),
        _timestamp("verified_at"),
        _timestamp("created_at"),
        _timestamp("released_at", nullable=True),
        sa.CheckConstraint(
            "layout_sha256 ~ '^[0-9a-f]{64}$' "
            "AND artifact_sha256 ~ '^[0-9a-f]{64}$' "
            "AND range_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND artifact_record_count >= 0 AND layout_contract_id <> '' "
            "AND layout_contract_version > 0 AND layout_range_count > 0 "
            "AND canonical_byte_count > 0 AND manifest_byte_count > 0 "
            "AND manifest_locator <> '' AND producer_build_id <> ''",
            name="pd_retained_layout_proof_check",
        ),
        sa.CheckConstraint(
            "registry_status IN ('verified', 'quarantined', 'released') "
            "AND ((registry_status = 'released') = (released_at IS NOT NULL))",
            name="pd_retained_layout_status_check",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_sha256"],
            [f"{schema}.provider_directory_retained_artifact.artifact_sha256"],
            name="pd_retained_layout_artifact_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "layout_sha256",
            name="provider_directory_retained_artifact_layout_pkey",
        ),
        sa.UniqueConstraint(
            "layout_sha256",
            "artifact_sha256",
            "layout_contract_version",
            "layout_range_count",
            name="pd_retained_layout_range_identity_key",
        ),
        sa.UniqueConstraint(
            "layout_sha256",
            "artifact_sha256",
            name="pd_retained_layout_artifact_key",
        ),
        sa.UniqueConstraint(
            "artifact_sha256",
            "layout_contract_id",
            "layout_contract_version",
            "layout_range_count",
            name="pd_retained_layout_contract_key",
        ),
        schema=schema,
    )
    create_table_or_validate(
        op,
        "provider_directory_retained_artifact_campaign_item_identity",
        sa.Column("campaign_id", sa.String(length=64), nullable=False),
        sa.Column("source_item_id", sa.String(length=64), nullable=False),
        sa.Column("campaign_ordinal", sa.Integer(), nullable=False),
        sa.Column("planned_identity_sha256", sa.String(length=64), nullable=False),
        _timestamp("created_at"),
        sa.CheckConstraint(
            "source_item_id ~ '^[0-9a-f]{64}$' "
            "AND planned_identity_sha256 ~ '^[0-9a-f]{64}$' "
            "AND campaign_ordinal >= 0",
            name="pd_retained_item_identity_ledger_check",
        ),
        sa.ForeignKeyConstraint(
            ["campaign_id"],
            [f"{schema}.provider_directory_retained_artifact_campaign.campaign_id"],
            name="pd_retained_item_identity_campaign_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "campaign_id",
            "source_item_id",
            name="pd_retained_item_identity_pkey",
        ),
        sa.UniqueConstraint(
            "campaign_id",
            "campaign_ordinal",
            name="pd_retained_item_identity_campaign_ordinal_key",
        ),
        sa.UniqueConstraint(
            "campaign_id",
            "source_item_id",
            "campaign_ordinal",
            "planned_identity_sha256",
            name="pd_retained_item_identity_exact_key",
        ),
        schema=schema,
    )
    create_table_or_validate(
        op,
        "provider_directory_retained_artifact_campaign_item",
        sa.Column("campaign_id", sa.String(length=64), nullable=False),
        sa.Column("source_item_id", sa.String(length=64), nullable=False),
        sa.Column("campaign_ordinal", sa.Integer(), nullable=False),
        sa.Column("source_entry_sha256", sa.String(length=64), nullable=False),
        sa.Column("artifact_kind", sa.String(length=32), nullable=False),
        sa.Column("family", sa.String(length=64), nullable=False),
        sa.Column("collection_kind", sa.String(length=64), nullable=False),
        sa.Column("partition_metadata_sha256", sa.String(length=64), nullable=False),
        sa.Column("stream_identity_sha256", sa.String(length=64), nullable=False),
        sa.Column("sequence_ordinal", sa.Integer(), nullable=False),
        sa.Column("item_role", sa.String(length=16), nullable=False),
        sa.Column("locator_ciphertext", sa.Text()),
        sa.Column("locator_identity_hmac", sa.String(length=112)),
        sa.Column("locator_key_id", sa.String(length=32)),
        sa.Column("declared_byte_count", sa.BigInteger()),
        sa.Column("observed_byte_count", sa.BigInteger()),
        sa.Column("acquisition_mode", sa.String(length=32)),
        sa.Column("validator_kind", sa.String(length=32)),
        sa.Column("validator_ciphertext", sa.Text()),
        sa.Column("validator_key_id", sa.String(length=32)),
        sa.Column("validator_sha256", sa.String(length=64)),
        sa.Column("immutable_identity_sha256", sa.String(length=64)),
        sa.Column(
            "request_interval_ms",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        _timestamp("retry_not_before", nullable=True),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column(
            "committed_byte_count",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("safe_failure_code", sa.String(length=64)),
        sa.Column("terminal_proof_sha256", sa.String(length=64)),
        sa.Column("planned_identity_sha256", sa.String(length=64), nullable=False),
        sa.Column("downloaded_artifact_sha256", sa.String(length=64)),
        sa.Column("artifact_sha256", sa.String(length=64)),
        sa.Column("layout_sha256", sa.String(length=64)),
        sa.Column("lease_owner", sa.String(length=128)),
        sa.Column(
            "lease_epoch",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        _timestamp("lease_expires_at", nullable=True),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        _timestamp("admitted_at", nullable=True),
        sa.CheckConstraint(
            "source_item_id ~ '^[0-9a-f]{64}$' "
            "AND source_entry_sha256 ~ '^[0-9a-f]{64}$' "
            "AND partition_metadata_sha256 ~ '^[0-9a-f]{64}$' "
            "AND stream_identity_sha256 ~ '^[0-9a-f]{64}$' "
            "AND artifact_kind IN ('provider_file', 'bulk_ndjson', "
            "'fhir_bundle_page', 'fhir_bundle_block') "
            "AND family <> '' AND collection_kind <> '' "
            "AND campaign_ordinal >= 0 AND sequence_ordinal >= 0 "
            "AND item_role IN ('payload', 'terminal_zero')",
            name="pd_retained_item_identity_check",
        ),
        sa.CheckConstraint(
            "status IN ('expected', 'preflighted', 'downloading', 'downloaded', "
            "'admitted', 'terminal_zero', 'failed', 'unaccounted') "
            "AND (acquisition_mode IS NULL OR acquisition_mode IN "
            "('ranged_strong_validator', 'atomic_catalog_object', "
            "'producer_verified')) "
            "AND (validator_kind IS NULL OR validator_kind IN "
            "('strong_etag', 'catalog_object', 'producer_proof'))",
            name="pd_retained_item_state_check",
        ),
        sa.CheckConstraint(
            "(declared_byte_count IS NULL OR declared_byte_count >= 0) "
            "AND (observed_byte_count IS NULL OR observed_byte_count >= 0) "
            "AND committed_byte_count >= 0 "
            "AND (locator_identity_hmac IS NULL OR "
            "locator_identity_hmac ~ "
            "'^pdhmac3:[A-Za-z0-9][A-Za-z0-9_.-]{0,31}:[0-9a-f]{64}$') "
            "AND (locator_key_id IS NULL OR "
            "locator_key_id ~ '^[A-Za-z0-9][A-Za-z0-9_.-]{0,31}$') "
            "AND (validator_sha256 IS NULL OR validator_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (validator_ciphertext IS NULL OR "
            "validator_ciphertext LIKE 'pdart3:%') "
            "AND (validator_key_id IS NULL OR "
            "validator_key_id ~ '^[A-Za-z0-9][A-Za-z0-9_.-]{0,31}$') "
            "AND ((validator_ciphertext IS NULL AND validator_key_id IS NULL) OR "
            "(validator_ciphertext IS NOT NULL AND validator_key_id IS NOT NULL "
            "AND split_part(validator_ciphertext, ':', 2) = validator_key_id)) "
            "AND (immutable_identity_sha256 IS NULL OR "
            "immutable_identity_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (terminal_proof_sha256 IS NULL OR "
            "terminal_proof_sha256 ~ '^[0-9a-f]{64}$') "
            "AND planned_identity_sha256 ~ '^[0-9a-f]{64}$' "
            "AND (downloaded_artifact_sha256 IS NULL OR "
            "downloaded_artifact_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (artifact_sha256 IS NULL OR artifact_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (layout_sha256 IS NULL OR layout_sha256 ~ '^[0-9a-f]{64}$') "
            "AND request_interval_ms >= 0 "
            "AND request_interval_ms <= 86400000 "
            "AND lease_epoch >= 0 "
            "AND ((lease_owner IS NULL AND lease_expires_at IS NULL) OR "
            "(lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL))",
            name="pd_retained_item_values_check",
        ),
        sa.CheckConstraint(
            "(item_role = 'payload' AND locator_ciphertext IS NOT NULL "
            "AND locator_identity_hmac IS NOT NULL "
            "AND locator_key_id IS NOT NULL "
            "AND locator_ciphertext LIKE 'pdart3:%' "
            "AND split_part(locator_ciphertext, ':', 2) = locator_key_id "
            "AND split_part(locator_identity_hmac, ':', 2) = locator_key_id "
            "AND terminal_proof_sha256 IS NULL) OR "
            "(item_role = 'terminal_zero' AND artifact_sha256 IS NULL "
            "AND status IN ('expected', 'terminal_zero', 'failed', 'unaccounted'))",
            name="pd_retained_item_role_check",
        ),
        sa.CheckConstraint(
            "status <> 'preflighted' OR "
            "(observed_byte_count IS NOT NULL AND acquisition_mode IS NOT NULL "
            "AND validator_kind IS NOT NULL "
            "AND immutable_identity_sha256 IS NOT NULL)",
            name="pd_retained_item_preflight_check",
        ),
        sa.CheckConstraint(
            "status <> 'downloaded' OR "
            "(item_role = 'payload' AND downloaded_artifact_sha256 IS NOT NULL)",
            name="pd_retained_item_downloaded_check",
        ),
        sa.CheckConstraint(
            "status <> 'admitted' OR "
            "(item_role = 'payload' AND artifact_sha256 IS NOT NULL "
            "AND layout_sha256 IS NOT NULL "
            "AND admitted_at IS NOT NULL)",
            name="pd_retained_item_admitted_check",
        ),
        sa.CheckConstraint(
            "status <> 'terminal_zero' OR "
            "(item_role = 'terminal_zero' AND terminal_proof_sha256 IS NOT NULL)",
            name="pd_retained_item_zero_terminal_check",
        ),
        sa.ForeignKeyConstraint(
            ["campaign_id"],
            [f"{schema}.provider_directory_retained_artifact_campaign.campaign_id"],
            name="pd_retained_item_campaign_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            [
                "campaign_id",
                "source_item_id",
                "campaign_ordinal",
                "planned_identity_sha256",
            ],
            [
                f"{schema}.provider_directory_retained_artifact_campaign_item_identity.campaign_id",
                f"{schema}.provider_directory_retained_artifact_campaign_item_identity.source_item_id",
                f"{schema}.provider_directory_retained_artifact_campaign_item_identity.campaign_ordinal",
                f"{schema}.provider_directory_retained_artifact_campaign_item_identity.planned_identity_sha256",
            ],
            name="pd_retained_item_identity_ledger_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_sha256"],
            [f"{schema}.provider_directory_retained_artifact.artifact_sha256"],
            name="pd_retained_item_artifact_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["layout_sha256", "artifact_sha256"],
            [
                f"{schema}.provider_directory_retained_artifact_layout.layout_sha256",
                f"{schema}.provider_directory_retained_artifact_layout.artifact_sha256",
            ],
            name="pd_retained_item_layout_artifact_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "campaign_id",
            "source_item_id",
            name="provider_directory_retained_artifact_campaign_item_pkey",
        ),
        sa.UniqueConstraint(
            "campaign_id",
            "campaign_ordinal",
            name="pd_retained_item_campaign_ordinal_key",
        ),
        sa.UniqueConstraint(
            "campaign_id",
            "stream_identity_sha256",
            "sequence_ordinal",
            name="pd_retained_item_stream_sequence_key",
        ),
        schema=schema,
    )
    create_table_or_validate(
        op,
        "provider_directory_retained_artifact_download_range",
        sa.Column("campaign_id", sa.String(length=64), nullable=False),
        sa.Column("source_item_id", sa.String(length=64), nullable=False),
        sa.Column("range_ordinal", sa.Integer(), nullable=False),
        sa.Column("raw_byte_start", sa.BigInteger(), nullable=False),
        sa.Column("raw_byte_end", sa.BigInteger(), nullable=False),
        sa.Column("raw_byte_count", sa.BigInteger(), nullable=False),
        sa.Column("raw_sha256", sa.String(length=64), nullable=False),
        _timestamp("verified_at"),
        sa.CheckConstraint(
            "range_ordinal >= 0 AND raw_byte_start >= 0 "
            "AND raw_byte_end > raw_byte_start "
            "AND raw_byte_count = raw_byte_end - raw_byte_start "
            "AND raw_sha256 ~ '^[0-9a-f]{64}$'",
            name="pd_retained_download_range_check",
        ),
        sa.ForeignKeyConstraint(
            ["campaign_id", "source_item_id"],
            [
                f"{schema}.provider_directory_retained_artifact_campaign_item.campaign_id",
                f"{schema}.provider_directory_retained_artifact_campaign_item.source_item_id",
            ],
            name="pd_retained_download_range_item_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "campaign_id",
            "source_item_id",
            "range_ordinal",
            name="provider_directory_retained_artifact_download_range_pkey",
        ),
        schema=schema,
    )
    create_table_or_validate(
        op,
        "provider_directory_retained_artifact_range",
        sa.Column("layout_sha256", sa.String(length=64), nullable=False),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("layout_contract_version", sa.Integer(), nullable=False),
        sa.Column("layout_range_count", sa.Integer(), nullable=False),
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
        _timestamp("verified_at"),
        sa.CheckConstraint(
            "layout_sha256 ~ '^[0-9a-f]{64}$' "
            "AND artifact_sha256 ~ '^[0-9a-f]{64}$' "
            "AND layout_contract_version > 0 AND layout_range_count > 0 "
            "AND range_ordinal >= 0 AND range_ordinal < layout_range_count "
            "AND raw_byte_start >= 0 AND raw_byte_end > raw_byte_start "
            "AND raw_byte_count = raw_byte_end - raw_byte_start "
            "AND raw_sha256 ~ '^[0-9a-f]{64}$' "
            "AND record_start >= 0 AND record_end >= record_start "
            "AND record_count = record_end - record_start "
            "AND canonical_sha256 ~ '^[0-9a-f]{64}$' "
            "AND canonical_byte_count > 0",
            name="pd_retained_artifact_range_proof_check",
        ),
        sa.ForeignKeyConstraint(
            [
                "layout_sha256",
                "artifact_sha256",
                "layout_contract_version",
                "layout_range_count",
            ],
            [
                f"{schema}.provider_directory_retained_artifact_layout.layout_sha256",
                f"{schema}.provider_directory_retained_artifact_layout.artifact_sha256",
                f"{schema}.provider_directory_retained_artifact_layout.layout_contract_version",
                f"{schema}.provider_directory_retained_artifact_layout.layout_range_count",
            ],
            name="pd_retained_artifact_range_layout_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "layout_sha256",
            "range_ordinal",
            name="provider_directory_retained_artifact_range_pkey",
        ),
        schema=schema,
    )
    create_table_or_validate(
        op,
        "provider_directory_retained_artifact_consumer",
        sa.Column("campaign_id", sa.String(length=64), nullable=False),
        sa.Column("consumer_recipe_id", sa.String(length=128), nullable=False),
        sa.Column("claimed_campaign_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "claim_generation",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        _timestamp("claimed_at"),
        _timestamp("heartbeat_at"),
        _timestamp("released_at", nullable=True),
        sa.CheckConstraint(
            "consumer_recipe_id <> '' "
            "AND claimed_campaign_sha256 ~ '^[0-9a-f]{64}$' "
            "AND claim_generation > 0",
            name="pd_retained_consumer_identity_check",
        ),
        sa.ForeignKeyConstraint(
            ["campaign_id"],
            [f"{schema}.provider_directory_retained_artifact_campaign.campaign_id"],
            name="pd_retained_consumer_campaign_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "campaign_id",
            "consumer_recipe_id",
            name="provider_directory_retained_artifact_consumer_pkey",
        ),
        schema=schema,
    )
    create_table_or_validate(
        op,
        "provider_directory_retained_artifact_consumer_reference",
        sa.Column("campaign_id", sa.String(length=64), nullable=False),
        sa.Column("consumer_recipe_id", sa.String(length=128), nullable=False),
        sa.Column("source_item_id", sa.String(length=64), nullable=False),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("layout_sha256", sa.String(length=64), nullable=False),
        sa.Column("claim_generation", sa.BigInteger(), nullable=False),
        _timestamp("created_at"),
        _timestamp("released_at", nullable=True),
        sa.CheckConstraint(
            "artifact_sha256 ~ '^[0-9a-f]{64}$' "
            "AND layout_sha256 ~ '^[0-9a-f]{64}$' "
            "AND claim_generation > 0",
            name="pd_retained_consumer_ref_identity_check",
        ),
        sa.ForeignKeyConstraint(
            ["campaign_id", "consumer_recipe_id"],
            [
                f"{schema}.provider_directory_retained_artifact_consumer.campaign_id",
                f"{schema}.provider_directory_retained_artifact_consumer.consumer_recipe_id",
            ],
            name="pd_retained_consumer_ref_consumer_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["campaign_id", "source_item_id"],
            [
                f"{schema}.provider_directory_retained_artifact_campaign_item.campaign_id",
                f"{schema}.provider_directory_retained_artifact_campaign_item.source_item_id",
            ],
            name="pd_retained_consumer_ref_item_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_sha256"],
            [f"{schema}.provider_directory_retained_artifact.artifact_sha256"],
            name="pd_retained_consumer_ref_artifact_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["layout_sha256", "artifact_sha256"],
            [
                f"{schema}.provider_directory_retained_artifact_layout.layout_sha256",
                f"{schema}.provider_directory_retained_artifact_layout.artifact_sha256",
            ],
            name="pd_retained_consumer_ref_layout_artifact_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "campaign_id",
            "consumer_recipe_id",
            "source_item_id",
            name="provider_directory_retained_artifact_consumer_reference_pkey",
        ),
        schema=schema,
    )
    quoted_schema = op.get_bind().dialect.identifier_preparer.quote(schema)
    campaign_guard_function = "provider_directory_retained_campaign_identity_guard_v1"
    campaign_guard_trigger = "pd_retained_campaign_identity_guard"
    campaign_table = "provider_directory_retained_artifact_campaign"
    op.execute(
        sa.text(
            f"""CREATE OR REPLACE FUNCTION {quoted_schema}.{campaign_guard_function}()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                BEGIN
                    IF TG_OP = 'DELETE' THEN
                        IF OLD.state IS DISTINCT FROM 'released'
                           OR OLD.released_at IS NULL THEN
                            RAISE EXCEPTION 'retained_campaign_delete_before_release'
                                USING ERRCODE = '23514';
                        END IF;
                        RETURN OLD;
                    END IF;
                    IF ROW(
                        NEW.campaign_id,
                        NEW.contract_id,
                        NEW.adapter_id,
                        NEW.endpoint_id,
                        NEW.request_fence_id,
                        NEW.credential_descriptor_sha256,
                        NEW.source_census_sha256,
                        NEW.identity_members_sha256,
                        NEW.identity_member_count,
                        NEW.census_mode,
                        NEW.expected_stream_count,
                        NEW.per_item_byte_budget,
                        NEW.aggregate_byte_budget
                    ) IS DISTINCT FROM ROW(
                        OLD.campaign_id,
                        OLD.contract_id,
                        OLD.adapter_id,
                        OLD.endpoint_id,
                        OLD.request_fence_id,
                        OLD.credential_descriptor_sha256,
                        OLD.source_census_sha256,
                        OLD.identity_members_sha256,
                        OLD.identity_member_count,
                        OLD.census_mode,
                        OLD.expected_stream_count,
                        OLD.per_item_byte_budget,
                        OLD.aggregate_byte_budget
                    ) OR (
                        OLD.census_mode = 'fixed_catalog'
                        AND NEW.expected_item_count IS DISTINCT FROM
                            OLD.expected_item_count
                    ) OR (
                        OLD.census_mode = 'ordered_streams'
                        AND NEW.expected_item_count < OLD.expected_item_count
                    ) THEN
                        RAISE EXCEPTION 'retained_campaign_identity_immutable'
                            USING ERRCODE = '23514';
                    END IF;
                    RETURN NEW;
                END;
                $$;"""
        )
    )
    op.execute(
        sa.text(
            f"DROP TRIGGER IF EXISTS {campaign_guard_trigger} "
            f"ON {quoted_schema}.{campaign_table};"
        )
    )
    op.execute(
        sa.text(
            f"CREATE TRIGGER {campaign_guard_trigger} "
            f"BEFORE UPDATE OR DELETE ON {quoted_schema}.{campaign_table} "
            f"FOR EACH ROW EXECUTE FUNCTION "
            f"{quoted_schema}.{campaign_guard_function}();"
        )
    )
    item_guard_function = "provider_directory_retained_item_identity_immutable_v1"
    item_guard_trigger = "pd_retained_item_identity_immutable"
    item_table = "provider_directory_retained_artifact_campaign_item"
    op.execute(
        sa.text(
            f"""CREATE OR REPLACE FUNCTION {quoted_schema}.{item_guard_function}()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                DECLARE
                    campaign_state text;
                    campaign_released_at timestamptz;
                BEGIN
                    IF TG_OP = 'DELETE' THEN
                        SELECT state, released_at
                          INTO campaign_state, campaign_released_at
                          FROM {quoted_schema}.{campaign_table}
                         WHERE campaign_id = OLD.campaign_id;
                        IF campaign_state IS DISTINCT FROM 'released'
                           OR campaign_released_at IS NULL THEN
                            RAISE EXCEPTION 'retained_item_delete_before_release'
                                USING ERRCODE = '23514';
                        END IF;
                        RETURN OLD;
                    END IF;
                    IF ROW(
                        NEW.campaign_id,
                        NEW.source_item_id,
                        NEW.campaign_ordinal,
                        NEW.source_entry_sha256,
                        NEW.artifact_kind,
                        NEW.family,
                        NEW.collection_kind,
                        NEW.partition_metadata_sha256,
                        NEW.stream_identity_sha256,
                        NEW.sequence_ordinal,
                        NEW.item_role,
                        NEW.declared_byte_count,
                        NEW.terminal_proof_sha256,
                        NEW.planned_identity_sha256
                    ) IS DISTINCT FROM ROW(
                        OLD.campaign_id,
                        OLD.source_item_id,
                        OLD.campaign_ordinal,
                        OLD.source_entry_sha256,
                        OLD.artifact_kind,
                        OLD.family,
                        OLD.collection_kind,
                        OLD.partition_metadata_sha256,
                        OLD.stream_identity_sha256,
                        OLD.sequence_ordinal,
                        OLD.item_role,
                        OLD.declared_byte_count,
                        OLD.terminal_proof_sha256,
                        OLD.planned_identity_sha256
                    ) THEN
                        RAISE EXCEPTION 'retained_item_planned_identity_immutable'
                            USING ERRCODE = '23514';
                    END IF;
                    RETURN NEW;
                END;
                $$;"""
        )
    )
    op.execute(
        sa.text(
            f"DROP TRIGGER IF EXISTS {item_guard_trigger} "
            f"ON {quoted_schema}.{item_table};"
        )
    )
    op.execute(
        sa.text(
            f"CREATE TRIGGER {item_guard_trigger} "
            f"BEFORE UPDATE OR DELETE ON {quoted_schema}.{item_table} "
            f"FOR EACH ROW EXECUTE FUNCTION "
            f"{quoted_schema}.{item_guard_function}();"
        )
    )
    ledger_guard_function = "provider_directory_retained_item_ledger_guard_v1"
    ledger_guard_trigger = "pd_retained_item_ledger_guard"
    ledger_table = "provider_directory_retained_artifact_campaign_item_identity"
    op.execute(
        sa.text(
            f"""CREATE OR REPLACE FUNCTION {quoted_schema}.{ledger_guard_function}()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                DECLARE
                    campaign_state text;
                    campaign_released_at timestamptz;
                BEGIN
                    IF TG_OP = 'UPDATE' THEN
                        RAISE EXCEPTION 'retained_item_ledger_immutable'
                            USING ERRCODE = '23514';
                    END IF;
                    SELECT state, released_at
                      INTO campaign_state, campaign_released_at
                      FROM {quoted_schema}.{campaign_table}
                     WHERE campaign_id = OLD.campaign_id;
                    IF campaign_state IS DISTINCT FROM 'released'
                       OR campaign_released_at IS NULL THEN
                        RAISE EXCEPTION 'retained_item_ledger_delete_before_release'
                            USING ERRCODE = '23514';
                    END IF;
                    RETURN OLD;
                END;
                $$;"""
        )
    )
    op.execute(
        sa.text(
            f"DROP TRIGGER IF EXISTS {ledger_guard_trigger} "
            f"ON {quoted_schema}.{ledger_table};"
        )
    )
    op.execute(
        sa.text(
            f"CREATE TRIGGER {ledger_guard_trigger} "
            f"BEFORE UPDATE OR DELETE ON {quoted_schema}.{ledger_table} "
            f"FOR EACH ROW EXECUTE FUNCTION "
            f"{quoted_schema}.{ledger_guard_function}();"
        )
    )
    stream_guard_function = "provider_directory_retained_stream_identity_guard_v1"
    stream_guard_trigger = "pd_retained_stream_identity_guard"
    stream_table = "provider_directory_retained_artifact_campaign_stream"
    op.execute(
        sa.text(
            f"""CREATE OR REPLACE FUNCTION {quoted_schema}.{stream_guard_function}()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                DECLARE
                    campaign_state text;
                    campaign_released_at timestamptz;
                BEGIN
                    IF TG_OP = 'DELETE' THEN
                        SELECT state, released_at
                          INTO campaign_state, campaign_released_at
                          FROM {quoted_schema}.{campaign_table}
                         WHERE campaign_id = OLD.campaign_id;
                        IF campaign_state IS DISTINCT FROM 'released'
                           OR campaign_released_at IS NULL THEN
                            RAISE EXCEPTION 'retained_stream_delete_before_release'
                                USING ERRCODE = '23514';
                        END IF;
                        RETURN OLD;
                    END IF;
                    IF ROW(
                        NEW.campaign_id,
                        NEW.stream_identity_sha256,
                        NEW.stream_ordinal
                    ) IS DISTINCT FROM ROW(
                        OLD.campaign_id,
                        OLD.stream_identity_sha256,
                        OLD.stream_ordinal
                    ) THEN
                        RAISE EXCEPTION 'retained_stream_identity_immutable'
                            USING ERRCODE = '23514';
                    END IF;
                    RETURN NEW;
                END;
                $$;"""
        )
    )
    op.execute(
        sa.text(
            f"DROP TRIGGER IF EXISTS {stream_guard_trigger} "
            f"ON {quoted_schema}.{stream_table};"
        )
    )
    op.execute(
        sa.text(
            f"CREATE TRIGGER {stream_guard_trigger} "
            f"BEFORE UPDATE OR DELETE ON {quoted_schema}.{stream_table} "
            f"FOR EACH ROW EXECUTE FUNCTION "
            f"{quoted_schema}.{stream_guard_function}();"
        )
    )
    create_index_if_missing(
        op,
        "pd_retained_campaign_state_idx",
        "provider_directory_retained_artifact_campaign",
        ["state", "updated_at"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_retained_item_status_idx",
        "provider_directory_retained_artifact_campaign_item",
        ["campaign_id", "status", "campaign_ordinal"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_retained_item_artifact_idx",
        "provider_directory_retained_artifact_campaign_item",
        ["artifact_sha256"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_retained_item_locator_key_idx",
        "provider_directory_retained_artifact_campaign_item",
        ["locator_key_id"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_retained_item_validator_key_idx",
        "provider_directory_retained_artifact_campaign_item",
        ["validator_key_id"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_retained_item_adoption_idx",
        "provider_directory_retained_artifact_campaign_item",
        [
            "artifact_kind",
            "acquisition_mode",
            "validator_kind",
            "validator_sha256",
            "immutable_identity_sha256",
            "observed_byte_count",
        ],
        schema=schema,
        postgresql_include=[
            "artifact_sha256",
            "layout_sha256",
            "downloaded_artifact_sha256",
            "campaign_id",
            "source_item_id",
        ],
        postgresql_where=sa.text(
            "status = 'admitted' AND immutable_identity_sha256 IS NOT NULL "
            "AND observed_byte_count IS NOT NULL AND artifact_sha256 IS NOT NULL "
            "AND layout_sha256 IS NOT NULL AND (validator_sha256 IS NOT NULL OR "
            "(acquisition_mode = 'producer_verified' "
            "AND validator_kind = 'producer_proof' "
            "AND downloaded_artifact_sha256 = artifact_sha256 "
            "AND immutable_identity_sha256 = artifact_sha256))"
        ),
    )
    create_index_if_missing(
        op,
        "pd_retained_artifact_status_idx",
        "provider_directory_retained_artifact",
        ["registry_status", "verified_at"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_retained_consumer_ref_artifact_idx",
        "provider_directory_retained_artifact_consumer_reference",
        ["artifact_sha256", "released_at"],
        schema=schema,
    )


def downgrade() -> None:
    schema = _schema()
    quoted_schema = op.get_bind().dialect.identifier_preparer.quote(schema)
    op.execute(
        sa.text(
            "DROP TRIGGER IF EXISTS pd_retained_campaign_identity_guard "
            f"ON {quoted_schema}.provider_directory_retained_artifact_campaign;"
        )
    )
    op.execute(
        sa.text(
            "DROP TRIGGER IF EXISTS pd_retained_stream_identity_guard "
            f"ON {quoted_schema}.provider_directory_retained_artifact_campaign_stream;"
        )
    )
    op.execute(
        sa.text(
            "DROP TRIGGER IF EXISTS pd_retained_item_ledger_guard "
            f"ON {quoted_schema}."
            "provider_directory_retained_artifact_campaign_item_identity;"
        )
    )
    op.execute(
        sa.text(
            "DROP TRIGGER IF EXISTS pd_retained_item_identity_immutable "
            f"ON {quoted_schema}.provider_directory_retained_artifact_campaign_item;"
        )
    )
    op.execute(
        sa.text(
            f"DROP FUNCTION IF EXISTS {quoted_schema}."
            "provider_directory_retained_item_identity_immutable_v1();"
        )
    )
    op.execute(
        sa.text(
            f"DROP FUNCTION IF EXISTS {quoted_schema}."
            "provider_directory_retained_item_ledger_guard_v1();"
        )
    )
    op.execute(
        sa.text(
            f"DROP FUNCTION IF EXISTS {quoted_schema}."
            "provider_directory_retained_stream_identity_guard_v1();"
        )
    )
    op.execute(
        sa.text(
            f"DROP FUNCTION IF EXISTS {quoted_schema}."
            "provider_directory_retained_campaign_identity_guard_v1();"
        )
    )
    op.drop_table(
        "provider_directory_retained_artifact_consumer_reference",
        schema=schema,
    )
    op.drop_table("provider_directory_retained_artifact_consumer", schema=schema)
    op.drop_table("provider_directory_retained_artifact_range", schema=schema)
    op.drop_table(
        "provider_directory_retained_artifact_download_range",
        schema=schema,
    )
    op.drop_table(
        "provider_directory_retained_artifact_campaign_item",
        schema=schema,
    )
    op.drop_table(
        "provider_directory_retained_artifact_campaign_item_identity",
        schema=schema,
    )
    op.drop_table("provider_directory_retained_artifact_layout", schema=schema)
    op.drop_table("provider_directory_retained_artifact", schema=schema)
    op.drop_table(
        "provider_directory_retained_artifact_campaign_stream",
        schema=schema,
    )
    op.drop_table(
        "provider_directory_retained_artifact_endpoint_fence",
        schema=schema,
    )
    op.drop_table(
        "provider_directory_retained_artifact_campaign",
        schema=schema,
    )
