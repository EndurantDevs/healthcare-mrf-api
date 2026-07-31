"""Add the TIGER ZCTA ZIP lookup index required by spatial serving.

Revision ID: 20260731190000_tiger_zcta5_zip_index
Revises: 20260801010000_uhc_semantic_layout_identity
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

from db.migration_index_adoption import has_matching_index


revision = "20260731190000_tiger_zcta5_zip_index"
down_revision = "20260801140000_ptg2_legacy_v3_metadata_reconcile"
branch_labels = None
depends_on = None


REFERENCE_SCHEMA = "tiger"
TABLE_NAME = "zcta5"
INDEX_NAME = "zcta5_zcta5ce_idx"
INDEX_COLUMNS = ("zcta5ce",)


_USABLE_ZCTA_ZIP_INDEX_SQL = """
    SELECT EXISTS (
        SELECT 1
          FROM pg_index AS index_meta
          JOIN pg_class AS index_record
            ON index_record.oid = index_meta.indexrelid
          JOIN pg_am AS index_method
            ON index_method.oid = index_record.relam
          JOIN pg_attribute AS key_attribute
            ON key_attribute.attrelid = index_meta.indrelid
           AND key_attribute.attname = :column_name
           AND key_attribute.attnum > 0
           AND NOT key_attribute.attisdropped
         WHERE index_meta.indrelid = to_regclass(:table_name)
           AND index_record.relkind IN ('i', 'I')
           AND index_method.amname = 'btree'
           AND index_meta.indisvalid
           AND index_meta.indisready
           AND index_meta.indislive
           AND index_meta.indpred IS NULL
           AND index_meta.indnkeyatts >= 1
           AND index_meta.indkey[0] = key_attribute.attnum
    )
"""


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, identifier: str) -> str:
    return f"{_q(schema)}.{_q(identifier)}"


def _offline_context():
    migration_context = op.get_context()
    return migration_context if migration_context.as_sql else None


def _table_exists(bind) -> bool:
    return bool(
        bind.execute(
            text("SELECT to_regclass(:table_name)"),
            {"table_name": f"{REFERENCE_SCHEMA}.{TABLE_NAME}"},
        ).scalar()
    )


def _named_index_state(bind) -> tuple[str, str, bool, bool, bool] | None:
    result = bind.execute(
        text(
            """
            SELECT table_namespace.nspname AS table_schema,
                   table_record.relname AS table_name,
                   index_meta.indisvalid,
                   index_meta.indisready,
                   index_meta.indislive
              FROM pg_class AS index_record
              JOIN pg_namespace AS index_namespace
                ON index_namespace.oid = index_record.relnamespace
              JOIN pg_index AS index_meta
                ON index_meta.indexrelid = index_record.oid
              JOIN pg_class AS table_record
                ON table_record.oid = index_meta.indrelid
              JOIN pg_namespace AS table_namespace
                ON table_namespace.oid = table_record.relnamespace
             WHERE index_namespace.nspname = :schema
               AND index_record.relname = :index_name
            """
        ),
        {"schema": REFERENCE_SCHEMA, "index_name": INDEX_NAME},
    )
    row = result.first()
    if row is None:
        return None
    fields = getattr(row, "_mapping", row)
    return (
        str(fields["table_schema"]),
        str(fields["table_name"]),
        bool(fields["indisvalid"]),
        bool(fields["indisready"]),
        bool(fields["indislive"]),
    )


def _has_usable_zcta_zip_index(bind) -> bool:
    return bool(
        bind.execute(
            text(_USABLE_ZCTA_ZIP_INDEX_SQL),
            {
                "column_name": INDEX_COLUMNS[0],
                "table_name": f"{REFERENCE_SCHEMA}.{TABLE_NAME}",
            },
        ).scalar()
    )


def _create_index_sql() -> str:
    return (
        f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {_q(INDEX_NAME)} "
        f"ON {_qt(REFERENCE_SCHEMA, TABLE_NAME)} ({_q(INDEX_COLUMNS[0])});"
    )


def _drop_index_sql() -> str:
    return (
        "DROP INDEX CONCURRENTLY IF EXISTS "
        f"{_qt(REFERENCE_SCHEMA, INDEX_NAME)};"
    )


def upgrade() -> None:
    if _offline_context() is not None:
        # TIGER reference data is optional in offline/fresh installations, and
        # PostgreSQL cannot conditionally create a concurrent index offline.
        return
    bind = op.get_bind()
    if not _table_exists(bind):
        return

    index_state = _named_index_state(bind)
    drop_invalid_index = False
    if index_state is not None:
        table_schema, table_name, is_valid, is_ready, is_live = index_state
        if (table_schema, table_name) != (REFERENCE_SCHEMA, TABLE_NAME):
            raise RuntimeError(
                f"existing_schema_index_mismatch:{REFERENCE_SCHEMA}.{INDEX_NAME}"
            )
        if is_valid and is_ready and is_live:
            if has_matching_index(
                op,
                INDEX_NAME,
                TABLE_NAME,
                INDEX_COLUMNS,
                schema=REFERENCE_SCHEMA,
                postgresql_using="btree",
            ):
                return
        else:
            drop_invalid_index = True
    elif _has_usable_zcta_zip_index(bind):
        # Adopt an externally managed equivalent without creating a duplicate.
        return

    with op.get_context().autocommit_block():
        if drop_invalid_index:
            bind.exec_driver_sql(_drop_index_sql())
        bind.exec_driver_sql(_create_index_sql())
    if not has_matching_index(
        op,
        INDEX_NAME,
        TABLE_NAME,
        INDEX_COLUMNS,
        schema=REFERENCE_SCHEMA,
        postgresql_using="btree",
    ):
        raise RuntimeError(
            f"required_index_missing:{REFERENCE_SCHEMA}.{INDEX_NAME}"
        )


def downgrade() -> None:
    if _offline_context() is not None:
        with op.get_context().autocommit_block():
            op.drop_index(
                INDEX_NAME,
                table_name=TABLE_NAME,
                schema=REFERENCE_SCHEMA,
                if_exists=True,
                postgresql_concurrently=True,
            )
        return
    with op.get_context().autocommit_block():
        op.get_bind().exec_driver_sql(_drop_index_sql())
