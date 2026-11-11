# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reserve CodeSets keys across active runs and mutable publication."""

from alembic import op
from db.migration_index_adoption import create_index_if_missing
from db.plan_release_storage import plan_release_schema, storage_identifier

revision = "20261111010000_code_sets_publication_idempotency"
down_revision = "20261108010000_custom_import_family_child_reference_aggregate"
branch_labels = None
depends_on = None

INDEX_NAME = "import_run_code_sets_publication_idempotency_idx"
INDEX_PREDICATE = (
    "importer = 'code-sets' AND idempotency_key IS NOT NULL AND "
    "(status IN ('queued', 'starting', 'running', 'finalizing', 'canceling') "
    "OR metrics->>'code_sets_mutable_publication' IS NOT NULL)"
)


def upgrade():
    """Refuse conflicting historical keys and adopt only the exact native index."""
    from sqlalchemy import text

    schema = plan_release_schema()
    previous_path = (
        f"{storage_identifier(schema)},public"
        if op.get_context().as_sql
        else op.get_bind().scalar(text("SHOW search_path"))
    )
    op.execute("SET LOCAL search_path TO pg_catalog,pg_temp")
    create_index_if_missing(
        op,
        INDEX_NAME,
        "import_run",
        ["importer", "idempotency_key"],
        schema=schema,
        unique=True,
        postgresql_where=text(INDEX_PREDICATE),
    )
    op.execute(text("SELECT pg_catalog.set_config('search_path',:previous,true)").bindparams(previous=previous_path))


def downgrade():
    """Release only the scoped publication reservation index."""
    op.drop_index(INDEX_NAME, table_name="import_run", schema=plan_release_schema())
