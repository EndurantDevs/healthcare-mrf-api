"""Index retained Provider Directory observations by NPI.

Revision ID: 20260813010000_provider_directory_observed_npi_index
Revises: 20260812030000_provider_directory_specialized_single_root_admission
"""

from __future__ import annotations

import os
import re

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


def _normalized_sql(value: object) -> str:
    return re.sub(r"\s+", "", str(value or "")).replace('"', "").lower()


def _index_is_expected(schema: str) -> bool:
    row = op.get_bind().execute(
        text(
            """
            SELECT index_record.indisvalid AS is_valid,
                   pg_get_indexdef(index_record.indexrelid, 1, true) AS key_one,
                   pg_get_indexdef(index_record.indexrelid, 2, true) AS key_two,
                   pg_get_indexdef(index_record.indexrelid, 3, true) AS key_three,
                   pg_get_expr(index_record.indpred, index_record.indrelid) AS predicate
              FROM pg_catalog.pg_index AS index_record
              JOIN pg_catalog.pg_class AS index_class
                ON index_class.oid = index_record.indexrelid
              JOIN pg_catalog.pg_namespace AS namespace
                ON namespace.oid = index_class.relnamespace
             WHERE namespace.nspname = :schema
               AND index_class.relname = :index_name
            """
        ),
        {"schema": schema, "index_name": INDEX_NAME},
    ).mappings().one_or_none()
    if row is None or row["is_valid"] is not True:
        return False
    key_one = _normalized_sql(row["key_one"])
    key_two = _normalized_sql(row["key_two"])
    key_three = _normalized_sql(row["key_three"])
    predicate = _normalized_sql(row["predicate"])
    return (
        "payload_json" in key_one
        and "jsonb" in key_one
        and "npi" in key_one
        and key_two == "dataset_id"
        and key_three == "resource_type"
        and "resource_type" in predicate
        and "practitioner" in predicate
        and "practitionerrole" in predicate
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


def upgrade() -> None:
    schema = _schema()
    context = op.get_context()
    if context.as_sql:
        with context.autocommit_block():
            op.execute(_create_index_sql(schema))
        return
    with context.autocommit_block():
        op.get_bind().exec_driver_sql(_create_index_sql(schema))
    if not _index_is_expected(schema):
        raise RuntimeError(f"required_index_missing:{schema}.{INDEX_NAME}")


def downgrade() -> None:
    schema = _schema()
    context = op.get_context()
    with context.autocommit_block():
        op.get_bind().exec_driver_sql(
            f"DROP INDEX CONCURRENTLY IF EXISTS {_q(schema)}.{_q(INDEX_NAME)};"
        )
