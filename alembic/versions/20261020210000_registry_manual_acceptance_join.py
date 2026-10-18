# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Join registry publication and manual build acceptance histories."""

revision = "20261020210000_registry_manual_acceptance_join"
down_revision = (
    "20261020160000_registry_staged_verification_join",
    "20261021110000_ptg_membership_import_idempotency",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join completed histories without changing schema or retained data."""
    return None


def downgrade() -> None:
    """Restore both parent heads without changing schema or retained data."""
    return None
