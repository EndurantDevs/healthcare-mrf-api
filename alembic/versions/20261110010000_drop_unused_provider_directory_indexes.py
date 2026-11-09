# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Drop provider directory secondary indexes that no read path uses."""

from __future__ import annotations

import os

from alembic import op

revision = "20261110010000_drop_unused_provider_directory_indexes"
down_revision = "20261108010000_custom_import_family_child_reference_aggregate"
branch_labels = None
depends_on = None

# Frozen with this revision: name, table, and the exact definition for downgrade.
INDEXES = (
    (
        "provider_directory_source_resource_canonical_idx",
        "provider_directory_source_resource",
        "USING btree (canonical_api_base, resource_type, resource_id)",
    ),
    (
        "provider_directory_canonical_resource_hash_idx",
        "provider_directory_canonical_resource",
        "USING btree (payload_hash)",
    ),
    (
        "provider_directory_role_practitioner_idx",
        "provider_directory_practitioner_role",
        "USING btree (practitioner_ref)",
    ),
    (
        "provider_directory_role_organization_idx",
        "provider_directory_practitioner_role",
        "USING btree (organization_ref)",
    ),
    (
        "provider_directory_role_location_refs_gin_idx",
        "provider_directory_practitioner_role",
        "USING gin ((location_refs::jsonb))",
    ),
    (
        "provider_directory_role_specialty_codes_gin_idx",
        "provider_directory_practitioner_role",
        "USING gin ((specialty_codes::jsonb))",
    ),
    (
        "provider_directory_role_code_codes_gin_idx",
        "provider_directory_practitioner_role",
        "USING gin ((code_codes::jsonb))",
    ),
)


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def upgrade():
    """Drop each frozen index without blocking readers or writers."""
    schema = _schema()
    # IF EXISTS keeps the revision convergent where an index was already removed.
    with op.get_context().autocommit_block():
        for name, _table, _definition in INDEXES:
            op.get_bind().exec_driver_sql(f"DROP INDEX CONCURRENTLY IF EXISTS {_q(schema)}.{_q(name)}")


def downgrade():
    """Rebuild each frozen index concurrently from its recorded definition."""
    schema = _schema()
    with op.get_context().autocommit_block():
        for name, table, definition in INDEXES:
            op.get_bind().exec_driver_sql(
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {_q(name)} ON {_q(schema)}.{_q(table)} {definition}"
            )
