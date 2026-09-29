# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind exact CMS Doctors practice-location IDs to stable site identities."""

from __future__ import annotations

import os

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260930060000_cms_doctors_site_binding"
down_revision = "20260930050000_cms_npd_relationship"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade() -> None:
    """Create the source-address to stable-site binding contract."""
    schema = _schema()
    op.create_table(
        "provider_directory_cms_doctors_site_binding",
        sa.Column("adrs_id", sa.String(256), nullable=False),
        sa.Column("site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("adrs_id"),
        sa.UniqueConstraint("site_id"),
        sa.CheckConstraint(
            "adrs_id <> '' AND adrs_id = btrim(adrs_id)",
            name="provider_directory_cms_doctors_site_address_check",
        ),
        sa.ForeignKeyConstraint(
            ("site_id",),
            (f"{schema}.provider_directory_site_identity.site_id",),
            ondelete="RESTRICT",
        ),
        schema=schema,
    )
    op.create_index("cms_doctor_group_site_idx_adrs", "cms_doctor_group_site", ["adrs_id"], schema=schema)


def downgrade() -> None:
    """Keep populated bindings unless an explicit retention plan exists."""
    schema = _schema()
    binding = f'"{schema}"."provider_directory_cms_doctors_site_binding"'
    op.execute(f"LOCK TABLE {binding} IN ACCESS EXCLUSIVE MODE")
    if op.get_bind().execute(sa.text(f"SELECT EXISTS (SELECT 1 FROM {binding})")).scalar_one():
        raise RuntimeError("CMS Doctors site bindings prevent downgrade")
    op.drop_index("cms_doctor_group_site_idx_adrs", table_name="cms_doctor_group_site", schema=schema)
    op.drop_table("provider_directory_cms_doctors_site_binding", schema=schema)
