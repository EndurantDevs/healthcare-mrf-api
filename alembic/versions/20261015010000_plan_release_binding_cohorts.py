# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Enroll immutable plan-release cohorts without rewriting retained payload."""

import sqlalchemy as sa

from alembic import op
from db.migration_index_adoption import create_index_if_missing
from db.migration_plan_release_storage import downgrade_plan_release_storage, upgrade_plan_release_storage
from db.plan_release_storage import plan_release_schema

revision = "20261015010000_plan_release_binding_cohorts"
down_revision = "20261015020000_custom_import_query_projections"
branch_labels = None
depends_on = None


def upgrade():
    """Prepare structural roots; explicit administrator custody follows."""
    upgrade_plan_release_storage(op.get_bind(), plan_release_schema())
    _prepare_retry_census_index()


def _prepare_retry_census_index():
    """Keep complete terminal-lineage lookup indexed without historical uniqueness."""
    create_index_if_missing(
        op,
        "import_run_plan_pricing_retry_census_idx",
        "import_run",
        ("retry_of_run_id", "run_id"),
        schema=plan_release_schema(),
        postgresql_where=sa.text("importer = 'plan-pricing-projection' AND retry_of_run_id IS NOT NULL"),
    )


def downgrade():
    """Preserve published cohorts rather than silently deleting their rows."""
    downgrade_plan_release_storage(op.get_bind(), plan_release_schema())
