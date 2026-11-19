# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Record the current and previous typed CMS National Provider Directory releases."""

from alembic import op
from db.plan_release_storage import plan_release_schema
from process.cms_npd_typed_schema import release_table_statements

revision = "20261119090000_cms_npd_typed_release"
down_revision = "20261116090000_profile_capacity_logical_names"
branch_labels = None
depends_on = None


def upgrade():
    """Create the release pointer table; release schemas themselves are created by the importer."""
    for statement in release_table_statements(plan_release_schema()):
        op.execute(statement)


def downgrade():
    """Dropping the pointer table would orphan the loaded release schemas."""
    raise RuntimeError("Typed CMS NPD releases require an explicit retirement")
