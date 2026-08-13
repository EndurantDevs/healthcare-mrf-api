"""Index retained Provider Directory observations by NPI.

Revision ID: 20260813010000_provider_directory_observed_npi_index
Revises: 20260812030000_provider_directory_specialized_single_root_admission
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text


revision = "20260813010000_provider_directory_observed_npi_index"
down_revision = "20260812030000_provider_directory_specialized_single_root_admission"
branch_labels = None
depends_on = None


INDEX_NAME = "provider_directory_dataset_resource_observed_npi_idx"
TABLE_NAME = "provider_directory_dataset_resource"


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _index_record(schema: str):
    """Return the same-name PostgreSQL index catalog record, if present."""

    return op.get_bind().execute(
        text(
            """
            SELECT table_namespace.nspname AS table_schema,
                   table_record.relname AS table_name,
                   index_record.indisvalid AS is_valid,
                   index_record.indisready AS is_ready,
                   index_record.indislive AS is_live,
                   access_method.amname AS access_method,
                   index_record.indisunique AS is_unique,
                   index_record.indnkeyatts AS key_count,
                   index_record.indnatts AS attribute_count,
                   pg_get_indexdef(index_record.indexrelid, 1, true) AS key_one,
                   pg_get_indexdef(index_record.indexrelid, 2, true) AS key_two,
                   pg_get_indexdef(index_record.indexrelid, 3, true) AS key_three,
                   pg_get_expr(index_record.indpred, index_record.indrelid) AS predicate
              FROM pg_catalog.pg_index AS index_record
              JOIN pg_catalog.pg_class AS index_class
                ON index_class.oid = index_record.indexrelid
              JOIN pg_catalog.pg_namespace AS namespace
                ON namespace.oid = index_class.relnamespace
              JOIN pg_catalog.pg_am AS access_method
                ON access_method.oid = index_class.relam
              JOIN pg_catalog.pg_class AS table_record
                ON table_record.oid = index_record.indrelid
              JOIN pg_catalog.pg_namespace AS table_namespace
                ON table_namespace.oid = table_record.relnamespace
             WHERE namespace.nspname = :schema
               AND index_class.relname = :index_name
            """
        ),
        {"schema": schema, "index_name": INDEX_NAME},
    ).mappings().one_or_none()


def _index_is_expected(schema: str, row=None) -> bool:
    row = row if row is not None else _index_record(schema)
    if (
        row is None
        or (row["table_schema"], row["table_name"]) != (schema, TABLE_NAME)
        or not all(bool(row[field]) for field in ("is_valid", "is_ready", "is_live"))
        or row["access_method"] != "btree"
        or bool(row["is_unique"])
        or int(row["key_count"]) != 3
        or int(row["attribute_count"]) != 3
    ):
        return False
    return (
        row["key_one"]
        in {
            "(payload_json ->> 'npi'::text)",
            "(payload_json::jsonb ->> 'npi'::text)",
        }
        and row["key_two"] == "dataset_id"
        and row["key_three"] == "resource_type"
        and row["predicate"]
        == (
            "((resource_type)::text = ANY ((ARRAY["
            "'Practitioner'::character varying, "
            "'PractitionerRole'::character varying])::text[]))"
        )
    )


def _create_index_sql(schema: str) -> str:
    return f"""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS {_q(INDEX_NAME)}
        ON {_qt(schema, TABLE_NAME)} (
            ((payload_json::jsonb ->> 'npi')),
            dataset_id,
            resource_type
        )
        WHERE resource_type IN ('Practitioner', 'PractitionerRole');
    """


def _drop_index_sql(schema: str) -> str:
    return f"DROP INDEX CONCURRENTLY IF EXISTS {_qt(schema, INDEX_NAME)}"


def upgrade() -> None:
    schema = _schema()
    context = op.get_context()
    if context.as_sql:
        with context.autocommit_block():
            op.execute(_create_index_sql(schema))
        return
    row = _index_record(schema)
    drop_invalid_index = False
    if row is not None:
        if (row["table_schema"], row["table_name"]) != (schema, TABLE_NAME):
            raise RuntimeError(f"existing_schema_index_mismatch:{schema}.{INDEX_NAME}")
        if all(bool(row[field]) for field in ("is_valid", "is_ready", "is_live")):
            if _index_is_expected(schema, row):
                return
            raise RuntimeError(f"existing_schema_index_mismatch:{schema}.{INDEX_NAME}")
        drop_invalid_index = True
    with context.autocommit_block():
        if drop_invalid_index:
            op.get_bind().exec_driver_sql(_drop_index_sql(schema))
        op.get_bind().exec_driver_sql(_create_index_sql(schema))
    if not _index_is_expected(schema):
        raise RuntimeError(f"required_index_missing:{schema}.{INDEX_NAME}")


def downgrade() -> None:
    schema = _schema()
    context = op.get_context()
    with context.autocommit_block():
        op.execute(_drop_index_sql(schema))
