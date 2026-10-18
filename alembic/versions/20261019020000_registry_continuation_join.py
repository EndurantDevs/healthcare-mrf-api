# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Join registry publication and retained build continuation histories."""

revision = "20261019020000_registry_continuation_join"
down_revision = (
    "20261019010000_registry_raw_object_join",
    "20261019010000_custom_import_build_continuation",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join both completed branches without changing schema or retained data."""
    return None


def downgrade() -> None:
    """Restore both branch heads without changing schema or retained data."""
    return None
