"""Add telecom to Provider Directory OrganizationAffiliation.

Revision ID: 20260711120000_provider_directory_affiliation_telecom
Revises: 20260710143000_provider_directory_pagination_root_identity
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260711120000_provider_directory_affiliation_telecom"
down_revision = "20260710143000_provider_directory_pagination_root_identity"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def upgrade():
    table = _qt(_schema(), "provider_directory_organization_affiliation")
    op.execute(
        f"""
        ALTER TABLE {table}
            ADD COLUMN IF NOT EXISTS telecom jsonb;
        """
    )


def downgrade():
    table = _qt(_schema(), "provider_directory_organization_affiliation")
    op.execute(
        f"""
        ALTER TABLE {table}
            DROP COLUMN IF EXISTS telecom;
        """
    )
