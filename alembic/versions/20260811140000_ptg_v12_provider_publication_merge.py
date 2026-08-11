# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Merge PTG V12 and rooted provider-publication heads.

Revision ID: 20260811140000_ptg_v12_provider_publication_merge
Revises: 20260811130000_ptg_v12_mainline_merge, 20260811130000_provider_directory_exact_practitioner_resource_order_repair
"""

from __future__ import annotations


revision = "20260811140000_ptg_v12_provider_publication_merge"
down_revision = (
    "20260811130000_ptg_v12_mainline_merge",
    "20260811130000_provider_directory_exact_practitioner_resource_order_repair",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join the independently applied migration branches."""
    return None


def downgrade() -> None:
    """Restore both independent branch heads without changing schema."""
    return None
