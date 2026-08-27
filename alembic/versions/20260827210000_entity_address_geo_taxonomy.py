# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Add the combined entity-address geography and taxonomy GiST index.

Revision ID: 20260827210000_entity_address_geo_taxonomy
Revises: 20260828090000_npi_search_taxonomy_projection
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text

from db.migration_index_adoption import (
    _create_temporary_index_table,
    _shape_from_catalog,
    _temporary_table_schema,
)
from db.migration_index_catalog import _index_catalog_record


revision = "20260827210000_entity_address_geo_taxonomy"
down_revision = "20260828090000_npi_search_taxonomy_projection"
branch_labels = None
depends_on = None


INDEX_NAME = "entity_address_unified_idx_geo_taxonomy"
TABLE_NAME = "entity_address_unified"
INDEX_EXPRESSIONS = (
    "public.Geography(public.ST_MakePoint((long)::double precision, (lat)::double precision))",
    "taxonomy_array public.gist__intbig_ops",
)
INDEX_PREDICATE = (
    "type IN ('primary', 'secondary', 'practice', 'site') "
    "AND COALESCE(address_precision, '') <> 'city_zip' "
    "AND lat IS NOT NULL AND long IS NOT NULL"
)
ENSURE_EXTENSION_SQL = "CREATE EXTENSION IF NOT EXISTS intarray WITH SCHEMA public"


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, identifier: str) -> str:
    return f"{_q(schema)}.{_q(identifier)}"


def _create_index_sql(schema: str, *, concurrently: bool = True) -> str:
    concurrent_clause = " CONCURRENTLY" if concurrently else ""
    return (
        f"CREATE INDEX{concurrent_clause} IF NOT EXISTS {_q(INDEX_NAME)} "
        f"ON {_qt(schema, TABLE_NAME)} USING gist "
        f"({', '.join(INDEX_EXPRESSIONS)}) WHERE {INDEX_PREDICATE}"
    )


def _drop_index_sql(schema: str) -> str:
    return f"DROP INDEX CONCURRENTLY IF EXISTS {_qt(schema, INDEX_NAME)}"


def _analyze_sql(schema: str) -> str:
    return f"ANALYZE {_qt(schema, TABLE_NAME)}"


def _table_exists(schema: str) -> bool:
    return bool(
        op.get_bind()
        .execute(
            text("SELECT to_regclass(:table_name)"),
            {"table_name": f"{schema}.{TABLE_NAME}"},
        )
        .scalar()
    )


def _same_name_relation_exists(schema: str) -> bool:
    return bool(
        op.get_bind()
        .execute(
            text(
                """
                SELECT 1
                  FROM pg_class AS relation_record
                  JOIN pg_namespace AS namespace_record
                    ON namespace_record.oid = relation_record.relnamespace
                 WHERE namespace_record.nspname = :schema
                   AND relation_record.relname = :index_name
                """
            ),
            {"schema": schema, "index_name": INDEX_NAME},
        )
        .scalar()
    )


def _expected_index_shape(schema: str):
    bind = op.get_bind()
    temporary = _create_temporary_index_table(bind, schema, TABLE_NAME)
    bind.exec_driver_sql(
        f"CREATE INDEX {temporary.quoted_index} "
        f"ON {temporary.quoted_table} USING gist "
        f"({', '.join(INDEX_EXPRESSIONS)}) WHERE {INDEX_PREDICATE}"
    )
    record = _index_catalog_record(
        op,
        temporary.index_name,
        temporary.table_name,
        _temporary_table_schema(bind, temporary.table_name),
    )
    if record is None:
        raise RuntimeError("temporary_expected_index_missing")
    return _shape_from_catalog(record)


def _matching_index_record(schema: str, expected_shape):
    record = _index_catalog_record(op, INDEX_NAME, TABLE_NAME, schema)
    if record is None:
        if _same_name_relation_exists(schema):
            raise RuntimeError(f"existing_schema_index_mismatch:{schema}.{INDEX_NAME}")
        return None
    if not all(record[field] for field in ("indisvalid", "indisready", "indislive")):
        return False
    if _shape_from_catalog(record) != expected_shape:
        raise RuntimeError(f"existing_schema_index_mismatch:{schema}.{INDEX_NAME}")
    return record


def upgrade() -> None:
    schema = _schema()
    context = op.get_context()
    op.execute(ENSURE_EXTENSION_SQL)
    if context.as_sql:
        with context.autocommit_block():
            op.execute(_create_index_sql(schema))
        op.execute(_analyze_sql(schema))
        return
    if not _table_exists(schema):
        return
    expected_shape = _expected_index_shape(schema)
    existing_record = _matching_index_record(schema, expected_shape)
    if existing_record is False or existing_record is None:
        with context.autocommit_block():
            if existing_record is False:
                op.get_bind().exec_driver_sql(_drop_index_sql(schema))
            op.get_bind().exec_driver_sql(_create_index_sql(schema))
        if not _matching_index_record(schema, expected_shape):
            raise RuntimeError(f"required_index_missing:{schema}.{INDEX_NAME}")
    op.execute(_analyze_sql(schema))


def downgrade() -> None:
    schema = _schema()
    with op.get_context().autocommit_block():
        op.execute(_drop_index_sql(schema))
