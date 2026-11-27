# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retire the reviewed-subset source and dataset guard triggers.

Ordinary seed sync now moves a retired census source to its configured endpoint,
and these row guards refuse that move. The guard functions stay: other callers
still reference them.
"""

from alembic import op
from db.plan_release_storage import plan_release_schema, storage_relation

revision = "20261127120000_retire_reviewed_subset_source_guards"
down_revision = "20261126120000_stored_id_lookups_by_name"
branch_labels = None
depends_on = None

_DATASET = "provider_directory_endpoint_dataset"
_SOURCE = "provider_directory_source"
_DATASET_GUARD_COLUMNS = (
    "dataset_id, endpoint_id, import_run_id, acquisition_root_run_id, previous_dataset_id, dataset_hash, "
    "status, is_current, resource_count, created_at, validated_at, published_at, superseded_at, "
    "publication_metadata_json, completion_proof_required_version, completion_proof_json, completion_proof_sha256"
)
# (trigger, table, timing and events, function)
_TRIGGERS = (
    (
        "provider_directory_reviewed_subset_activation_dataset_guard",
        _DATASET,
        f"BEFORE INSERT OR UPDATE OF {_DATASET_GUARD_COLUMNS}",
        "guard_provider_directory_reviewed_subset_activation_dataset",
    ),
    (
        "provider_directory_reviewed_subset_dataset_truncate_guard",
        _DATASET,
        "BEFORE TRUNCATE",
        "guard_provider_directory_reviewed_subset_activation_dataset",
    ),
    (
        "provider_directory_reviewed_subset_activation_source_guard",
        _SOURCE,
        "AFTER INSERT OR DELETE OR UPDATE",
        "guard_provider_directory_reviewed_subset_activation_source",
    ),
    (
        "provider_directory_reviewed_subset_source_truncate_guard",
        _SOURCE,
        "BEFORE TRUNCATE",
        "guard_provider_directory_reviewed_subset_activation_source",
    ),
    (
        "provider_directory_subset_published_source_guard",
        _SOURCE,
        "AFTER INSERT OR DELETE OR UPDATE",
        "guard_provider_directory_subset_published_source",
    ),
    (
        "provider_directory_subset_published_source_truncate_guard",
        _SOURCE,
        "BEFORE TRUNCATE",
        "guard_provider_directory_subset_published_source",
    ),
)


def _create_statements(schema, trigger):
    name, table, events, function = trigger
    relation = storage_relation(schema, table)
    if events.startswith("AFTER"):
        create = f'CREATE CONSTRAINT TRIGGER "{name}" {events} ON {relation} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW'
    else:
        create = (
            f'CREATE TRIGGER "{name}" {events} ON {relation} FOR EACH {"STATEMENT" if "TRUNCATE" in events else "ROW"}'
        )
    return (
        f"{create} EXECUTE FUNCTION {storage_relation(schema, function)}()",
        f'ALTER TABLE {relation} ENABLE ALWAYS TRIGGER "{name}"',
    )


def upgrade():
    """Drop each guard trigger once; a database without them is already current."""
    schema = plan_release_schema()
    for name, table, _events, _function in _TRIGGERS:
        op.execute(f'DROP TRIGGER IF EXISTS "{name}" ON {storage_relation(schema, table)}')


def downgrade():
    """Restore the exact predecessor triggers over the retained guard functions."""
    schema = plan_release_schema()
    for trigger in _TRIGGERS:
        for statement in _create_statements(schema, trigger):
            op.execute(statement)
