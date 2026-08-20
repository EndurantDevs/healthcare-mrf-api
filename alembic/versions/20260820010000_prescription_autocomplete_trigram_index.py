"""Index prescription autocomplete contains-search expressions.

Revision ID: 20260820010000_prescription_autocomplete_trigram_index
Revises: 202608200001_ptg_v13_json_null_guard
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


revision = "20260820010000_prescription_autocomplete_trigram_index"
down_revision = "202608200001_ptg_v13_json_null_guard"
branch_labels = None
depends_on = None


INDEX_NAME = "pricing_provider_rx_autocomplete_trgm_idx"
STAGING_INDEX_NAME = "rx_ac_gin"
TABLE_NAME = "pricing_provider_prescription"
INDEX_EXPRESSIONS = (
    "lower(COALESCE(rx_name, '')) gin_trgm_ops",
    "lower(COALESCE(generic_name, '')) gin_trgm_ops",
    "lower(COALESCE(brand_name, '')) gin_trgm_ops",
    "lower(COALESCE(rx_code, '')) gin_trgm_ops",
)
INDEX_PREDICATE = "rx_code_system = 'HP_RX_CODE'"


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
        f"ON {_qt(schema, TABLE_NAME)} USING gin "
        f"({', '.join(INDEX_EXPRESSIONS)}) WHERE {INDEX_PREDICATE}"
    )


def _drop_index_sql(schema: str) -> str:
    return f"DROP INDEX CONCURRENTLY IF EXISTS {_qt(schema, INDEX_NAME)}"


def _expected_index_shape(schema: str):
    bind = op.get_bind()
    temporary = _create_temporary_index_table(bind, schema, TABLE_NAME)
    bind.exec_driver_sql(
        f"CREATE INDEX {temporary.quoted_index} "
        f"ON {temporary.quoted_table} USING gin "
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


def _table_exists(schema: str) -> bool:
    return bool(
        op.get_bind()
        .execute(
            text("SELECT to_regclass(:table_name)"),
            {"table_name": f"{schema}.{TABLE_NAME}"},
        )
        .scalar()
    )


def _matching_index_record(schema: str, expected_shape):
    record = _index_catalog_record(op, INDEX_NAME, TABLE_NAME, schema)
    if record is None:
        return None
    if not record["indisvalid"] or not record["indisready"]:
        return False
    if _shape_from_catalog(record) != expected_shape:
        raise RuntimeError(f"existing_schema_index_mismatch:{schema}.{INDEX_NAME}")
    return record


def upgrade() -> None:
    schema = _schema()
    context = op.get_context()
    if context.as_sql:
        with context.autocommit_block():
            op.execute(_create_index_sql(schema))
        return
    if not _table_exists(schema):
        return
    expected_shape = _expected_index_shape(schema)
    existing_record = _matching_index_record(schema, expected_shape)
    if existing_record is not None and existing_record is not False:
        return
    with context.autocommit_block():
        if existing_record is False:
            op.get_bind().exec_driver_sql(_drop_index_sql(schema))
        op.get_bind().exec_driver_sql(_create_index_sql(schema))
    if not _matching_index_record(schema, expected_shape):
        raise RuntimeError(f"required_index_missing:{schema}.{INDEX_NAME}")


def downgrade() -> None:
    schema = _schema()
    with op.get_context().autocommit_block():
        op.execute(_drop_index_sql(schema))
