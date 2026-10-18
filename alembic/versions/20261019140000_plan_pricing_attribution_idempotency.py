# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep attribution idempotency keys unique after their run becomes terminal."""

from alembic import op
from db.plan_release_storage import plan_release_schema, storage_identifier, storage_relation

revision = "20261019140000_plan_pricing_attribution_idempotency"
down_revision = "20261019130000_cms_archive_transaction_guard"
branch_labels = None
depends_on = None

INDEX_NAME = "import_run_plan_pricing_idempotency_idx"
_LEGACY_IMPORTERS = (
    "plan-pricing-projection",
    "plan-pricing-prewarm",
    "plan-pricing-em-distance",
)


def _replace_index(importers):
    schema = plan_release_schema()
    predicate = ", ".join("'" + importer + "'" for importer in importers)
    # Transactional DDL restores the original index if existing duplicates refuse creation.
    op.execute(f"DROP INDEX IF EXISTS {storage_relation(schema, INDEX_NAME)}")
    op.execute(
        f"CREATE UNIQUE INDEX {storage_identifier(INDEX_NAME)} "
        f"ON {storage_relation(schema, 'import_run')} (importer, idempotency_key) "
        f"WHERE importer IN ({predicate}) AND idempotency_key IS NOT NULL"
    )


def upgrade():
    """Extend the existing all-status contract without deleting conflicting runs."""
    _replace_index((*_LEGACY_IMPORTERS, "plan-pricing-em-attribution"))


def downgrade():
    """Restore the preceding three-importer predicate without changing run data."""
    _replace_index(_LEGACY_IMPORTERS)
