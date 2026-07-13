"""Add immutable Provider Directory dataset serving relations.

Revision ID: 20260713213000_provider_directory_dataset_serving_relations
Revises: 20260713210000_provider_directory_profile_fields
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa


revision = "20260713213000_provider_directory_dataset_serving_relations"
down_revision = "20260713210000_provider_directory_profile_fields"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _dataset_foreign_key(
    schema: str,
    name: str,
) -> sa.ForeignKeyConstraint:
    return sa.ForeignKeyConstraint(
        ["dataset_id"],
        [f"{schema}.provider_directory_endpoint_dataset.dataset_id"],
        name=name,
        ondelete="CASCADE",
    )


def _create_network_plan_table(schema: str) -> None:
    op.create_table(
        "provider_directory_dataset_network_plan",
        sa.Column("dataset_id", sa.String(length=96), nullable=False),
        sa.Column("network_resource_id", sa.String(length=256), nullable=False),
        sa.Column(
            "insurance_plan_resource_id",
            sa.String(length=256),
            nullable=False,
        ),
        _dataset_foreign_key(
            schema,
            "provider_directory_dataset_network_plan_dataset_id_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "dataset_id",
            "network_resource_id",
            "insurance_plan_resource_id",
            name="provider_directory_dataset_network_plan_pkey",
        ),
        schema=schema,
    )
    op.create_index(
        "provider_directory_dataset_network_plan_reverse_lookup_idx",
        "provider_directory_dataset_network_plan",
        ["dataset_id", "insurance_plan_resource_id"],
        unique=False,
        schema=schema,
        postgresql_include=["network_resource_id"],
    )


def _create_affiliation_organization_table(schema: str) -> None:
    op.create_table(
        "provider_directory_dataset_affiliation_organization",
        sa.Column("dataset_id", sa.String(length=96), nullable=False),
        sa.Column(
            "participating_organization_resource_id",
            sa.String(length=256),
            nullable=False,
        ),
        sa.Column(
            "affiliation_resource_id",
            sa.String(length=256),
            nullable=False,
        ),
        _dataset_foreign_key(
            schema,
            "pd_dataset_affiliation_org_dataset_id_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "dataset_id",
            "participating_organization_resource_id",
            "affiliation_resource_id",
            name=(
                "provider_directory_dataset_affiliation_organization_pkey"
            ),
        ),
        schema=schema,
    )


def upgrade():
    schema = _schema()
    _create_network_plan_table(schema)
    _create_affiliation_organization_table(schema)


def downgrade():
    schema = _schema()
    op.drop_table(
        "provider_directory_dataset_affiliation_organization",
        schema=schema,
    )
    op.drop_table(
        "provider_directory_dataset_network_plan",
        schema=schema,
    )
