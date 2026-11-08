# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Join independent directory-retirement and pricing-control migrations."""

revision = "20261023220000_merge_directory_pricing_controls"
down_revision = (
    "20261023100000_provider_directory_retirement_parent_updates",
    "20261023110000_code_pricing_import_idempotency",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join both completed branches without changing schema or retained data."""
    return None


def downgrade() -> None:
    """Restore both branch heads without changing schema or retained data."""
    return None
