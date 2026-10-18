# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep source-check idempotency keys unique after completion."""

from alembic import op
from db.plan_release_storage import plan_release_schema, storage_identifier, storage_relation

revision = "20261019150000_registry_source_check_idempotency"
down_revision = "20261019140000_plan_pricing_attribution_idempotency"
branch_labels = None
depends_on = None

INDEX_NAME = "import_run_registry_source_check_idempotency_idx"


def upgrade():
    """Rebuild startup-created indexes without deleting conflicting historical owners."""
    op.execute(f"DROP INDEX IF EXISTS {storage_relation(plan_release_schema(), INDEX_NAME)}")
    op.execute(
        f"CREATE UNIQUE INDEX {storage_identifier(INDEX_NAME)} "
        f"ON {storage_relation(plan_release_schema(), 'import_run')} (importer, idempotency_key) "
        "WHERE importer = 'registry-source-check' AND idempotency_key IS NOT NULL"
    )


def downgrade():
    """Remove only the source-check all-status index."""
    op.execute(f"DROP INDEX {storage_relation(plan_release_schema(), INDEX_NAME)}")
