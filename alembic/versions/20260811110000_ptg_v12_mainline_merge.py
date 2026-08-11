"""Merge PTG receipt authority and address mainline migration heads.

Revision ID: 20260811110000_ptg_v12_mainline_merge
Revises: 20260810160000_ptg2_legacy_global_projection_queue, 20260811100000_address_numeric_grid_alias
"""

from __future__ import annotations


revision = "20260811110000_ptg_v12_mainline_merge"
down_revision = (
    "20260810160000_ptg2_legacy_global_projection_queue",
    "20260811100000_address_numeric_grid_alias",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join the two independently applied migration branches."""
    return None


def downgrade() -> None:
    """Restore both independent branch heads without changing schema."""
    return None
