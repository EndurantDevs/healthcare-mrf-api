# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add stable insurance-network identities and exact plan evidence."""

from __future__ import annotations

import os

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260929020000_provider_directory_insurance_network_identity"
down_revision = "20260929010000_provider_directory_entity_identity"
branch_labels = None
depends_on = None

_NETWORK_TABLES = (
    "provider_directory_insurance_network_identity",
    "provider_directory_insurance_network_source_binding",
    "provider_directory_insurance_network_plan_evidence",
)


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _qualified(schema: str, table: str) -> str:
    return '"' + schema.replace('"', '""') + '"."' + table + '"'


def _create_identity(schema: str) -> None:
    op.create_table(
        "provider_directory_insurance_network_identity",
        sa.Column("network_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("network_id"),
        schema=schema,
    )


def _create_source_binding(schema: str) -> None:
    op.create_table(
        "provider_directory_insurance_network_source_binding",
        sa.Column("source_id", sa.String(64), nullable=False),
        sa.Column("resource_type", sa.String(16), nullable=False),
        sa.Column("resource_id", sa.String(256), nullable=False),
        sa.Column("network_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("source_id", "resource_id"),
        sa.CheckConstraint("resource_type = 'Organization'", name="pd_insurance_network_org_type_check"),
        sa.ForeignKeyConstraint(
            ("source_id", "resource_type", "resource_id"),
            (
                f"{schema}.provider_directory_entity_source_binding.source_id",
                f"{schema}.provider_directory_entity_source_binding.resource_type",
                f"{schema}.provider_directory_entity_source_binding.resource_id",
            ),
            ondelete="RESTRICT",
            name="pd_insurance_network_entity_binding_fkey",
        ),
        sa.ForeignKeyConstraint(
            ("network_id",),
            (f"{schema}.provider_directory_insurance_network_identity.network_id",),
            ondelete="RESTRICT",
            name="pd_insurance_network_identity_fkey",
        ),
        schema=schema,
    )
    op.create_index(
        "pd_insurance_network_binding_id_idx",
        "provider_directory_insurance_network_source_binding",
        ("network_id",),
        schema=schema,
    )


def _create_plan_evidence(schema: str) -> None:
    op.create_table(
        "provider_directory_insurance_network_plan_evidence",
        sa.Column("source_id", sa.String(64), nullable=False),
        sa.Column("release_id", sa.String(256), nullable=False),
        sa.Column("network_resource_type", sa.String(16), nullable=False),
        sa.Column("network_resource_id", sa.String(256), nullable=False),
        sa.Column("insurance_plan_resource_id", sa.String(256), nullable=False),
        sa.Column("network_refs", postgresql.JSONB(), nullable=False),
        sa.Column("owned_by_ref", sa.Text()),
        sa.Column("administered_by_ref", sa.Text()),
        sa.Column("plan_payload_sha256", sa.String(64), nullable=False),
        sa.Column("plan_payload_json", postgresql.JSONB(), nullable=False),
        sa.Column("observed_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("source_id", "release_id", "network_resource_id", "insurance_plan_resource_id"),
        sa.CheckConstraint("network_resource_type = 'Organization'", name="pd_insurance_network_plan_org_type_check"),
        sa.ForeignKeyConstraint(
            ("source_id", "network_resource_id"),
            (
                f"{schema}.provider_directory_insurance_network_source_binding.source_id",
                f"{schema}.provider_directory_insurance_network_source_binding.resource_id",
            ),
            ondelete="RESTRICT",
            name="pd_insurance_network_plan_binding_fkey",
        ),
        sa.ForeignKeyConstraint(
            ("source_id", "network_resource_type", "network_resource_id", "release_id"),
            (
                f"{schema}.provider_directory_entity_release_evidence.source_id",
                f"{schema}.provider_directory_entity_release_evidence.resource_type",
                f"{schema}.provider_directory_entity_release_evidence.resource_id",
                f"{schema}.provider_directory_entity_release_evidence.release_id",
            ),
            ondelete="RESTRICT",
            name="pd_insurance_network_plan_release_fkey",
        ),
        schema=schema,
    )
    op.create_index(
        "pd_insurance_network_plan_release_idx",
        "provider_directory_insurance_network_plan_evidence",
        ("source_id", "release_id", "insurance_plan_resource_id"),
        schema=schema,
    )


def upgrade() -> None:
    """Create network roles and exact plan evidence after entity bindings."""
    schema = _schema()
    _create_identity(schema)
    _create_source_binding(schema)
    _create_plan_evidence(schema)


def downgrade() -> None:
    """Remove only empty network tables so retained evidence cannot be lost."""
    schema = _schema()
    connection = op.get_bind()
    # Keep admission blocked until all emptiness checks and DDL finish in this transaction.
    locked_tables = ", ".join(_qualified(schema, table) for table in _NETWORK_TABLES)
    connection.exec_driver_sql(f"LOCK TABLE {locked_tables} IN ACCESS EXCLUSIVE MODE")
    for table in _NETWORK_TABLES:
        if connection.exec_driver_sql(f"SELECT EXISTS (SELECT 1 FROM {_qualified(schema, table)})").scalar_one():
            raise RuntimeError("provider_directory_insurance_network_downgrade_requires_empty_tables")
    op.drop_index(
        "pd_insurance_network_plan_release_idx",
        table_name="provider_directory_insurance_network_plan_evidence",
        schema=schema,
    )
    op.drop_table("provider_directory_insurance_network_plan_evidence", schema=schema)
    op.drop_index(
        "pd_insurance_network_binding_id_idx",
        table_name="provider_directory_insurance_network_source_binding",
        schema=schema,
    )
    op.drop_table("provider_directory_insurance_network_source_binding", schema=schema)
    op.drop_table("provider_directory_insurance_network_identity", schema=schema)
