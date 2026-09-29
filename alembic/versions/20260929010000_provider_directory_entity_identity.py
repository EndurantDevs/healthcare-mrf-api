# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add source-scoped organization and site identity with release evidence."""

from __future__ import annotations

import os

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260929010000_provider_directory_entity_identity"
down_revision = "20260929000000_cms_doctor_group_site"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _create_identity_tables(schema: str) -> None:
    op.create_table(
        "provider_directory_organization_identity",
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("organization_id"),
        schema=schema,
    )
    op.create_table(
        "provider_directory_site_identity",
        sa.Column("site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("site_id"),
        schema=schema,
    )


def _create_source_binding(schema: str) -> None:
    op.create_table(
        "provider_directory_entity_source_binding",
        sa.Column("source_id", sa.String(64), nullable=False),
        sa.Column("resource_type", sa.String(16), nullable=False),
        sa.Column("resource_id", sa.String(256), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True)),
        sa.Column("site_id", postgresql.UUID(as_uuid=True)),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("source_id", "resource_type", "resource_id"),
        sa.ForeignKeyConstraint(
            ("organization_id",),
            (f"{schema}.provider_directory_organization_identity.organization_id",),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ("site_id",),
            (f"{schema}.provider_directory_site_identity.site_id",),
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "(resource_type = 'Organization' AND organization_id IS NOT NULL AND site_id IS NULL) OR "
            "(resource_type = 'Location' AND site_id IS NOT NULL AND organization_id IS NULL)",
            name="provider_directory_entity_binding_kind_check",
        ),
        schema=schema,
    )
    op.create_index(
        "provider_directory_entity_binding_organization_idx",
        "provider_directory_entity_source_binding",
        ("organization_id",),
        schema=schema,
    )
    op.create_index(
        "provider_directory_entity_binding_site_idx",
        "provider_directory_entity_source_binding",
        ("site_id",),
        schema=schema,
    )


def _create_release_evidence(schema: str) -> None:
    op.create_table(
        "provider_directory_entity_release_evidence",
        sa.Column("source_id", sa.String(64), nullable=False),
        sa.Column("resource_type", sa.String(16), nullable=False),
        sa.Column("resource_id", sa.String(256), nullable=False),
        sa.Column("release_id", sa.String(256), nullable=False),
        sa.Column("payload_sha256", sa.String(64), nullable=False),
        sa.Column("payload_json", postgresql.JSONB(), nullable=False),
        sa.Column("observed_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("source_id", "resource_type", "resource_id", "release_id"),
        sa.ForeignKeyConstraint(
            ("source_id", "resource_type", "resource_id"),
            (
                f"{schema}.provider_directory_entity_source_binding.source_id",
                f"{schema}.provider_directory_entity_source_binding.resource_type",
                f"{schema}.provider_directory_entity_source_binding.resource_id",
            ),
            ondelete="RESTRICT",
            name="provider_directory_entity_evidence_binding_fkey",
        ),
        schema=schema,
    )
    op.create_index(
        "provider_directory_entity_evidence_release_idx",
        "provider_directory_entity_release_evidence",
        ("source_id", "release_id"),
        schema=schema,
    )


def upgrade() -> None:
    """Create empty identities and persistent source/release bindings."""
    schema = _schema()
    _create_identity_tables(schema)
    _create_source_binding(schema)
    _create_release_evidence(schema)
    op.create_table(
        "provider_directory_cms_doctors_group_binding",
        sa.Column("org_pac_id", sa.String(64), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("org_pac_id"),
        sa.UniqueConstraint("organization_id"),
        sa.ForeignKeyConstraint(
            ("organization_id",),
            (f"{schema}.provider_directory_organization_identity.organization_id",),
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "org_pac_id <> '' AND org_pac_id = btrim(org_pac_id)",
            name="provider_directory_cms_doctors_group_pac_check",
        ),
        schema=schema,
    )


def downgrade() -> None:
    """Remove only empty tables, preserving any observed source evidence."""
    schema = _schema()
    connection = op.get_bind()
    tables = (
        "provider_directory_cms_doctors_group_binding",
        "provider_directory_entity_release_evidence",
        "provider_directory_entity_source_binding",
        "provider_directory_organization_identity",
        "provider_directory_site_identity",
    )
    # Block concurrent observations until the empty check and drops commit.
    for table in tables:
        connection.exec_driver_sql(f"LOCK TABLE {_quote(schema)}.{_quote(table)} IN ACCESS EXCLUSIVE MODE")
    for table in tables:
        qualified = f"{_quote(schema)}.{_quote(table)}"
        if connection.exec_driver_sql(f"SELECT EXISTS (SELECT 1 FROM {qualified})").scalar_one():
            raise RuntimeError("provider_directory_entity_identity_downgrade_requires_empty_tables")
    op.drop_table("provider_directory_cms_doctors_group_binding", schema=schema)
    op.drop_index(
        "provider_directory_entity_evidence_release_idx",
        table_name="provider_directory_entity_release_evidence",
        schema=schema,
    )
    op.drop_table("provider_directory_entity_release_evidence", schema=schema)
    op.drop_index(
        "provider_directory_entity_binding_site_idx",
        table_name="provider_directory_entity_source_binding",
        schema=schema,
    )
    op.drop_index(
        "provider_directory_entity_binding_organization_idx",
        table_name="provider_directory_entity_source_binding",
        schema=schema,
    )
    op.drop_table("provider_directory_entity_source_binding", schema=schema)
    op.drop_table("provider_directory_site_identity", schema=schema)
    op.drop_table("provider_directory_organization_identity", schema=schema)
