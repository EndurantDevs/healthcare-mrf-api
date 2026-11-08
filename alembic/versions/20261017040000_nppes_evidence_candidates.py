# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare native NPPES candidate bindings without retiring live guards."""

from alembic import op
from db.migration_nppes_evidence_candidates import (
    downgrade_nppes_evidence_candidates,
    upgrade_nppes_evidence_candidates,
)
from db.plan_release_storage import plan_release_schema

revision = "20261017040000_nppes_evidence_candidates"
down_revision = "20261017030000_registry_ptg_office_recipe_selection"
branch_labels = None
depends_on = None


def upgrade():
    """Add only durable structure; authenticated custody activation is separate."""
    upgrade_nppes_evidence_candidates(op.get_bind(), plan_release_schema())


def downgrade():
    """Refuse destruction of retained source and resume bindings."""
    downgrade_nppes_evidence_candidates(op.get_bind(), plan_release_schema())
