# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Accept portable recovery documents while retaining existing history unchanged."""

from alembic import op
from db.plan_release_storage import plan_release_schema, storage_relation

revision = "20261115120000_named_output_recovery_document"
down_revision = "20261115110000_network_retained_read_grant_ledger"
branch_labels = None
depends_on = None


def _statement(schema):
    table = storage_relation(schema, "provider_directory_serving_recovery")
    return f"""ALTER TABLE IF EXISTS {table}
      DROP CONSTRAINT pd_serving_recovery_document,
      ADD CONSTRAINT pd_serving_recovery_document CHECK (
        jsonb_typeof(document)='object' AND document->>'version' IN ('1','2')
        AND octet_length(document::text)<=2097152)"""


def _generation_statement(schema):
    table = storage_relation(schema, "entity_address_result_generation")
    return f"""ALTER TABLE IF EXISTS {table}
      DROP CONSTRAINT entity_address_result_generation_shape_check,
      ADD CONSTRAINT entity_address_result_generation_shape_check CHECK (
        singleton IS TRUE AND local_generation >= 0 AND (
          (origin_lineage_id IS NULL AND origin_generation IS NULL AND published_at IS NULL) OR
          (origin_lineage_id IS NOT NULL AND origin_generation IS NOT NULL
           AND origin_generation > 0 AND published_at IS NOT NULL)))"""


def _retire_dataset_plan_owner(schema):
    """Retire only the historical physical-owner gate; native grants protect the plan."""
    table = storage_relation(schema, "pd_dataset_migration_plan")
    function = storage_relation(schema, "guard_pd_dataset_migration_owner")
    return (
        f"DROP TRIGGER IF EXISTS pd_dataset_plan_owner ON {table} RESTRICT",
        f"DROP FUNCTION IF EXISTS {function}() RESTRICT",
    )


def upgrade():
    """Advance logical checks without rewriting saved rows, documents or hashes."""
    op.execute(_statement(plan_release_schema()))
    op.execute(_generation_statement(plan_release_schema()))
    for statement in _retire_dataset_plan_owner(plan_release_schema()):
        op.execute(statement)


def downgrade():
    """A predecessor cannot interpret a portable document already published."""
    raise RuntimeError("Recovery document history requires an explicit compatibility transition")
