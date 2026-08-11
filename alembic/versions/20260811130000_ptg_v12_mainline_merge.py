"""Merge PTG V12 and provider-directory V5 mainline heads.

Revision ID: 20260811130000_ptg_v12_mainline_merge
Revises: 20260811110000_ptg_v12_mainline_merge, 20260811120000_provider_directory_reviewed_subset_v5_http410_disposition
"""

from __future__ import annotations


revision = "20260811130000_ptg_v12_mainline_merge"
down_revision = (
    "20260811110000_ptg_v12_mainline_merge",
    "20260811120000_provider_directory_reviewed_subset_v5_http410_disposition",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join the independently applied migration branches."""
    return None


def downgrade() -> None:
    """Restore both independent branch heads without changing schema."""
    return None
