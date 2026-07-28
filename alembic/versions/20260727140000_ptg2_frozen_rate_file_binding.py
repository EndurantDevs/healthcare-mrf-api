"""Add immutable frozen PTG source-file bindings.

Revision ID: 20260727140000_ptg2_frozen_rate_file_binding
Revises: 20260727130000_ptg2_predecessor_retirement_audit
"""

from __future__ import annotations

from alembic import op

from db.migration_ptg2_frozen_source_file_binding import (
    install_frozen_source_file_binding,
    uninstall_frozen_source_file_binding,
)
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema


revision = "20260727140000_ptg2_frozen_rate_file_binding"
down_revision = "20260727130000_ptg2_predecessor_retirement_audit"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Install immutable source-file binding evidence."""

    install_frozen_source_file_binding(op, resolve_ptg2_schema())


def downgrade() -> None:
    """Remove the binding contract only when no evidence exists."""

    uninstall_frozen_source_file_binding(op, resolve_ptg2_schema())
