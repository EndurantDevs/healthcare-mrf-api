# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Join independently applied source snapshot and notification branches."""

revision = "20261017020000_source_registry_snapshot_join"
down_revision = (
    "20261016060000_uhc_official_cohort_snapshots",
    "20261017010000_registry_cms_source_notifications",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join the migration branches without changing schema or retained data."""
    return None


def downgrade() -> None:
    """Restore both independent branch heads without changing schema."""
    return None
