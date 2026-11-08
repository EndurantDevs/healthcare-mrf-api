# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Join registry publication and independently applied raw-object storage."""

revision = "20261019010000_registry_raw_object_join"
down_revision = (
    "20261018040000_registry_aca_source_notifications",
    "20261017010000_custom_import_raw_objects",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join both completed branches without changing schema or retained data."""
    return None


def downgrade() -> None:
    """Restore both branch heads without changing schema or retained data."""
    return None
