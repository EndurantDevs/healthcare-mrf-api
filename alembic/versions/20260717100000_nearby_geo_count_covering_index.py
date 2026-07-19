"""Add the covering nearby-provider exact-count index.

Revision ID: 20260717100000_nearby_geo_count_covering_index
Revises: 20260716140000_ptg2_v3_code_identity_index
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text

from db.migration_index_adoption import has_matching_index


revision = "20260717100000_nearby_geo_count_covering_index"
down_revision = "20260716140000_ptg2_v3_code_identity_index"
branch_labels = None
depends_on = None


TABLE_NAME = "entity_address_unified"
INDEX_NAME = "entity_address_unified_idx_geo_bbox"
REPLACEMENT_INDEX_NAME = "entity_address_unified_idx_geo_bbox_replacement"
INDEX_COLUMNS = ("lat", "long")
INCLUDE_COLUMNS = ("npi", "address_key")
INDEX_PREDICATE = (
    "type IN ('primary', 'secondary', 'practice', 'site') "
    "AND COALESCE(address_precision, '') <> 'city_zip' "
    "AND lat IS NOT NULL AND long IS NOT NULL"
)


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, identifier: str) -> str:
    return f"{_q(schema)}.{_q(identifier)}"


def _offline_context():
    migration_context = op.get_context()
    return migration_context if migration_context.as_sql else None


def _create_replacement_sql(schema: str, include_columns: tuple[str, ...]) -> str:
    include_sql = (
        f" INCLUDE ({', '.join(_q(column) for column in include_columns)})"
        if include_columns
        else ""
    )
    return (
        f"CREATE INDEX CONCURRENTLY {_q(REPLACEMENT_INDEX_NAME)} "
        f"ON {_qt(schema, TABLE_NAME)} ({_q('lat')}, {_q('long')})"
        f"{include_sql} WHERE {INDEX_PREDICATE};"
    )


def _index_matches(
    index_name: str,
    include_columns: tuple[str, ...],
) -> bool:
    """Return whether an index matches, allowing only the known replaceable shapes."""

    schema = _schema()
    try:
        return has_matching_index(
            op,
            index_name,
            TABLE_NAME,
            INDEX_COLUMNS,
            schema=schema,
            postgresql_include=include_columns,
            postgresql_where=text(INDEX_PREDICATE),
        )
    except RuntimeError as exc:
        mismatch = f"existing_schema_index_mismatch:{schema}.{index_name}"
        invalid = f"existing_schema_index_invalid:{schema}.{index_name}"
        if str(exc) not in {mismatch, invalid}:
            raise
        if index_name == REPLACEMENT_INDEX_NAME:
            # This name is private to this migration. A stale or interrupted
            # replacement is safe to drop and rebuild concurrently.
            return False
        if str(exc) == invalid:
            raise

        alternate_include = () if include_columns else INCLUDE_COLUMNS
        if has_matching_index(
            op,
            index_name,
            TABLE_NAME,
            INDEX_COLUMNS,
            schema=schema,
            postgresql_include=alternate_include,
            postgresql_where=text(INDEX_PREDICATE),
        ):
            # Upgrade and downgrade each replace exactly the other known shape.
            return False
        raise


def _replace_index(include_columns: tuple[str, ...]) -> None:
    schema = _schema()
    offline_context = _offline_context()
    if offline_context is not None:
        with offline_context.autocommit_block():
            op.execute(
                text(
                    f"DROP INDEX CONCURRENTLY IF EXISTS "
                    f"{_qt(schema, REPLACEMENT_INDEX_NAME)};"
                )
            )
            op.execute(text(_create_replacement_sql(schema, include_columns)))
            op.execute(
                text(
                    f"DROP INDEX CONCURRENTLY IF EXISTS {_qt(schema, INDEX_NAME)};"
                )
            )
        op.execute(
            text(
                f"ALTER INDEX {_qt(schema, REPLACEMENT_INDEX_NAME)} "
                f"RENAME TO {_q(INDEX_NAME)};"
            )
        )
        return

    bind = op.get_bind()
    if not bind.execute(
        text("SELECT to_regclass(:table_name)"),
        {"table_name": f"{schema}.{TABLE_NAME}"},
    ).scalar():
        return
    if _index_matches(INDEX_NAME, include_columns):
        return

    replacement_matches = _index_matches(REPLACEMENT_INDEX_NAME, include_columns)
    with op.get_context().autocommit_block():
        if not replacement_matches:
            bind.exec_driver_sql(
                f"DROP INDEX CONCURRENTLY IF EXISTS "
                f"{_qt(schema, REPLACEMENT_INDEX_NAME)};"
            )
            bind.exec_driver_sql(_create_replacement_sql(schema, include_columns))

    if not _index_matches(REPLACEMENT_INDEX_NAME, include_columns):
        raise RuntimeError(
            f"required_replacement_index_missing:{schema}.{REPLACEMENT_INDEX_NAME}"
        )

    with op.get_context().autocommit_block():
        bind.exec_driver_sql(
            f"DROP INDEX CONCURRENTLY IF EXISTS {_qt(schema, INDEX_NAME)};"
        )
    bind.exec_driver_sql(
        f"ALTER INDEX {_qt(schema, REPLACEMENT_INDEX_NAME)} "
        f"RENAME TO {_q(INDEX_NAME)};"
    )
    if not _index_matches(INDEX_NAME, include_columns):
        raise RuntimeError(f"required_index_missing:{schema}.{INDEX_NAME}")


def upgrade() -> None:
    _replace_index(INCLUDE_COLUMNS)


def downgrade() -> None:
    _replace_index(())
