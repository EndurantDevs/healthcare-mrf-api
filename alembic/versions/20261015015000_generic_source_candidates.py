# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare header-bound generic SOURCE candidates without changing live custody."""

from alembic import op
from db.migration_generic_source_candidates import (
    downgrade_generic_source_candidates,
    upgrade_generic_source_candidates,
)
from db.plan_release_storage import plan_release_schema

revision = "20261015015000_generic_source_candidates"
down_revision = "20261015010000_plan_release_binding_cohorts"
branch_labels = None
depends_on = None


def upgrade():
    """Prepare durable binding structure; native administrator cut is separate."""
    upgrade_generic_source_candidates(op.get_bind(), plan_release_schema())


def downgrade():
    """Refuse destructive downgrade of retained publication custody."""
    downgrade_generic_source_candidates(op.get_bind(), plan_release_schema())
