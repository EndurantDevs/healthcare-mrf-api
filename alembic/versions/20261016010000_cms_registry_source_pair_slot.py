# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind one prepared retained pair to its immutable serving receipt."""

from alembic import op
from db.cms_registry_source_pair_slot import CHECK_NAME, CHECK_SQL, COLUMN, INDEX_NAME, TABLE
from db.plan_release_storage import plan_release_schema, storage_relation

revision = "20261016010000_cms_registry_source_pair_slot"
down_revision = "20261015015000_generic_source_candidates"
branch_labels = None
depends_on = None


def upgrade():
    """Add only native nullable structure; ownership admission is separate."""
    table = storage_relation(plan_release_schema(), TABLE)
    op.execute(f"ALTER TABLE {table} ADD COLUMN {COLUMN} jsonb")
    op.execute(f"ALTER TABLE {table} ADD CONSTRAINT {CHECK_NAME} CHECK ({CHECK_SQL})")
    op.execute(f"CREATE UNIQUE INDEX {INDEX_NAME} ON {table} (({COLUMN}->>'capture_id')) WHERE {COLUMN} IS NOT NULL")


def downgrade():
    """Never erase the only durable locator of a finalized retained pair."""
    table = storage_relation(plan_release_schema(), TABLE)
    if op.get_bind().exec_driver_sql("SHOW transaction_isolation").scalar() != "read committed":
        raise RuntimeError("cms_serving_registry_pair_downgrade_isolation_invalid")
    op.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    if op.get_bind().exec_driver_sql(f"SELECT EXISTS(SELECT FROM {table} WHERE {COLUMN} IS NOT NULL)").scalar():
        raise RuntimeError("cms_serving_registry_pair_slot_in_use")
    op.execute(f"ALTER TABLE {table} DROP COLUMN {COLUMN} RESTRICT")
