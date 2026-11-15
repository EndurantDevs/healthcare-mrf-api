# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Install the existing named snapshot preparation lifecycle metadata."""

from alembic import op
from db.migration_named_preparation import upgrade_named_preparation_catalog

revision = "20261114130000_named_preparation_catalog"
down_revision = "20261114020000_custom_import_snapshot_metadata"
branch_labels = None
depends_on = None


def upgrade():
    """Create the named catalog without activating a publisher or worker."""
    upgrade_named_preparation_catalog(op.get_bind())


def downgrade():
    """Preserve admitted generations and consumed claim history."""
    raise RuntimeError("Named preparation history requires explicit retirement")
