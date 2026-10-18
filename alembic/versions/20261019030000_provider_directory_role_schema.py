# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Adopt declared PractitionerRole identifiers and search indexes without replacing data."""

import os

import sqlalchemy as sa

from alembic import op
from db.migration_adoption import add_column_if_missing
from db.migration_index_adoption import _create_temporary_index_table, _shape_from_catalog, _temporary_table_schema
from db.migration_index_catalog import _index_catalog_record

revision = "20261019030000_provider_directory_role_schema"
down_revision = "20261019020000_registry_continuation_join"
branch_labels = None
depends_on = None

_TABLE = "provider_directory_practitioner_role"
_COLUMNS = ("location_refs", "specialty_codes", "code_codes", "network_refs")


def _ensure_index(schema, column):
    """Adopt only a complete native match; create missing indexes without blocking readers."""
    bind = op.get_bind()
    quote = bind.dialect.identifier_preparer.quote
    relation_ref = quote(schema) + "." + quote(_TABLE)
    native_type = bind.execute(
        sa.text(
            "SELECT format_type(atttypid,atttypmod) FROM pg_attribute "
            "WHERE attrelid=to_regclass(:target) AND attname=:column AND NOT attisdropped"
        ),
        {"target": relation_ref, "column": column},
    ).scalar_one()
    if native_type not in {"json", "jsonb"}:
        raise RuntimeError("existing_schema_role_index_column_mismatch")
    expression = quote(column) if native_type == "jsonb" else "(" + quote(column) + "::jsonb)"
    name = "provider_directory_role_" + column + "_gin_idx"
    temporary = _create_temporary_index_table(bind, schema, _TABLE)
    try:
        bind.exec_driver_sql(
            f"CREATE INDEX {temporary.quoted_index} ON {temporary.quoted_table} USING gin ({expression})"
        )
        expected = _shape_from_catalog(
            _index_catalog_record(
                op, temporary.index_name, temporary.table_name, _temporary_table_schema(bind, temporary.table_name)
            )
        )
    finally:
        bind.exec_driver_sql(f"DROP TABLE {temporary.quoted_table}")
    current = _index_catalog_record(op, name, _TABLE, schema)
    if current is None:
        with op.get_context().autocommit_block():
            bind.exec_driver_sql(f"CREATE INDEX CONCURRENTLY {quote(name)} ON {relation_ref} USING gin ({expression})")
        current = _index_catalog_record(op, name, _TABLE, schema)
    if current is None or not all(current[key] for key in ("indisvalid", "indisready", "indislive")):
        raise RuntimeError("existing_schema_role_index_invalid")
    if _shape_from_catalog(current) != expected:
        raise RuntimeError("existing_schema_role_index_mismatch")


def upgrade() -> None:
    """Add missing declared fields or verify original runtime-created objects exactly."""
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    add_column_if_missing(op, _TABLE, sa.Column("npi", sa.BigInteger(), nullable=True), schema=schema)
    for column in _COLUMNS:
        _ensure_index(schema, column)


def downgrade() -> None:
    """Preserve adopted source identifiers, indexes and their existing values on downgrade."""
    return None
