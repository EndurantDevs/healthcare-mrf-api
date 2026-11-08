# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Join independent company declarations and directory recovery migrations."""

revision = "20261025020000_company_affiliation_join"
down_revision = (
    "20261023100000_provider_directory_serving_recovery",
    "20261025010000_company_affiliation_states",
)
branch_labels = None
depends_on = None


def upgrade():
    """Join completed branches without changing schema, rows or grants."""
    return None


def downgrade():
    """Restore both branch heads without changing schema, rows or grants."""
    return None
