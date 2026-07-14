"""Add endpoint-scoped Provider Directory datasets.

Revision ID: 20260710003000_provider_directory_endpoint_datasets
Revises: 20260709110000_provider_directory_overlay_coordinates
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from db.migration_adoption import (
    add_column_if_missing,
    create_foreign_key_if_missing,
    create_table_or_validate,
)
from db.migration_index_adoption import create_index_if_missing


revision = "20260710003000_provider_directory_endpoint_datasets"
down_revision = "20260709110000_provider_directory_overlay_coordinates"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()
    create_table_or_validate(
        op,
        "provider_directory_api_endpoint",
        sa.Column("endpoint_id", sa.String(length=64), nullable=False),
        sa.Column("canonical_api_base", sa.Text(), nullable=False),
        sa.Column("credential_descriptor_hash", sa.String(length=64), nullable=False),
        sa.Column("endpoint_signature_hash", sa.String(length=64), nullable=False),
        sa.Column("credential_descriptor_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("endpoint_signature_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("first_seen_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("last_seen_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=True),
        sa.PrimaryKeyConstraint("endpoint_id", name="provider_directory_api_endpoint_pkey"),
        sa.UniqueConstraint(
            "canonical_api_base",
            "credential_descriptor_hash",
            "endpoint_signature_hash",
            name="provider_directory_api_endpoint_identity_key",
        ),
        schema=schema,
    )

    create_table_or_validate(
        op,
        "provider_directory_endpoint_dataset",
        sa.Column("dataset_id", sa.String(length=96), nullable=False),
        sa.Column("endpoint_id", sa.String(length=64), nullable=False),
        sa.Column("import_run_id", sa.String(length=64), nullable=True),
        sa.Column("previous_dataset_id", sa.String(length=96), nullable=True),
        sa.Column("dataset_hash", sa.String(length=64), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("is_current", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("resource_count", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("validated_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("published_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("superseded_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("publication_metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(
            ["endpoint_id"],
            [f"{schema}.provider_directory_api_endpoint.endpoint_id"],
            name="provider_directory_endpoint_dataset_endpoint_id_fkey",
        ),
        sa.PrimaryKeyConstraint("dataset_id", name="provider_directory_endpoint_dataset_pkey"),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "provider_directory_endpoint_dataset_endpoint_idx",
        "provider_directory_endpoint_dataset",
        ["endpoint_id"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "provider_directory_endpoint_dataset_status_idx",
        "provider_directory_endpoint_dataset",
        ["status"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "provider_directory_endpoint_dataset_current_idx",
        "provider_directory_endpoint_dataset",
        ["endpoint_id"],
        unique=True,
        schema=schema,
        postgresql_where=sa.text("is_current = true"),
    )
    create_index_if_missing(
        op,
        "provider_directory_endpoint_dataset_hash_idx",
        "provider_directory_endpoint_dataset",
        ["dataset_hash"],
        schema=schema,
    )

    create_table_or_validate(
        op,
        "provider_directory_dataset_resource",
        sa.Column("dataset_id", sa.String(length=96), nullable=False),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("resource_id", sa.String(length=256), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(
            ["dataset_id"],
            [f"{schema}.provider_directory_endpoint_dataset.dataset_id"],
            name="provider_directory_dataset_resource_dataset_id_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "dataset_id",
            "resource_type",
            "resource_id",
            name="provider_directory_dataset_resource_pkey",
        ),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "provider_directory_dataset_resource_type_id_idx",
        "provider_directory_dataset_resource",
        ["resource_type", "resource_id"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "provider_directory_dataset_resource_hash_idx",
        "provider_directory_dataset_resource",
        ["payload_hash"],
        schema=schema,
    )

    add_column_if_missing(
        op,
        "provider_directory_source",
        sa.Column("endpoint_id", sa.String(length=64), nullable=True),
        schema=schema,
    )
    create_foreign_key_if_missing(
        op,
        "provider_directory_source",
        sa.ForeignKeyConstraint(
            ["endpoint_id"],
            [f"{schema}.provider_directory_api_endpoint.endpoint_id"],
            name="provider_directory_source_endpoint_id_fkey",
            ondelete="SET NULL",
        ),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "provider_directory_source_endpoint_id_idx",
        "provider_directory_source",
        ["endpoint_id"],
        schema=schema,
    )


def downgrade():
    schema = _schema()
    op.drop_index(
        "provider_directory_source_endpoint_id_idx",
        table_name="provider_directory_source",
        schema=schema,
    )
    op.drop_constraint(
        "provider_directory_source_endpoint_id_fkey",
        "provider_directory_source",
        type_="foreignkey",
        schema=schema,
    )
    op.drop_column("provider_directory_source", "endpoint_id", schema=schema)
    op.drop_table("provider_directory_dataset_resource", schema=schema)
    op.drop_table("provider_directory_endpoint_dataset", schema=schema)
    op.drop_table("provider_directory_api_endpoint", schema=schema)
