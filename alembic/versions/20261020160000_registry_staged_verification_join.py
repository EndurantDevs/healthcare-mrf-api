# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Join registry publication and staged build verification histories."""

revision = "20261020160000_registry_staged_verification_join"
down_revision = (
    "20261019150000_registry_source_check_idempotency",
    "20261020150000_custom_import_staged_verification",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join completed parent histories without changing schema or retained data."""
    return None


def downgrade() -> None:
    """Restore both parent heads without changing schema or retained data."""
    return None
