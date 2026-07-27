"""Add immutable audited PTG predecessor-retirement evidence.

Revision ID: 20260727130000_ptg2_predecessor_retirement_audit
Revises: 20260727120000_provider_profile_facts
"""

from __future__ import annotations

from alembic import op

from db.migration_ptg2_predecessor_retirement_audit import (
    install_predecessor_retirement_audit,
    uninstall_predecessor_retirement_audit,
)
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema


revision = "20260727130000_ptg2_predecessor_retirement_audit"
down_revision = "20260727120000_provider_profile_facts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Install standalone immutable retirement evidence."""

    install_predecessor_retirement_audit(op, resolve_ptg2_schema())


def downgrade() -> None:
    """Remove the audit contract only when no evidence exists."""

    uninstall_predecessor_retirement_audit(op, resolve_ptg2_schema())
