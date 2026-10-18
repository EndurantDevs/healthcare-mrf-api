# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Structural preparation only; independent native administrator custody follows."""

import sqlalchemy as sa

from db.plan_release_storage import storage_relation


def upgrade_generic_source_candidates(connection, schema):
    """Add only the nullable internal binding; keep live guards and ACLs intact."""
    relation = storage_relation(schema, "provider_directory_endpoint_dataset")
    connection.execute(sa.text(f"ALTER TABLE {relation} ADD COLUMN IF NOT EXISTS candidate_storage_json jsonb"))
    valid = connection.scalar(
        sa.text(
            "SELECT count(*)=1 AND bool_and(atttypid='jsonb'::regtype AND NOT attnotnull AND NOT atthasdef) "
            "FROM pg_attribute WHERE attrelid=to_regclass(:relation) AND attname='candidate_storage_json' "
            "AND attnum>0 AND NOT attisdropped"
        ),
        {"relation": relation},
    )
    if valid is not True:
        raise RuntimeError("provider_directory_generic_source_binding_column_changed")


def downgrade_generic_source_candidates(connection, schema):
    # The binding is part of immutable publication custody and resume identity.
    """Refuse removal of durable immutable publication/resume custody."""
    raise RuntimeError("provider_directory_generic_source_forward_only")
