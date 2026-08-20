"""Materialize exact provider-backed prescription autocomplete rows.

Revision ID: 20260820140000_prescription_autocomplete_rollup
Revises: 20260820130000_site_intelligence_fast_paths
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text

from db.prescription_autocomplete_rollup_sql import (
    prescription_autocomplete_rollup_insert_sql,
)


revision = "20260820140000_prescription_autocomplete_rollup"
down_revision = "20260820130000_site_intelligence_fast_paths"
branch_labels = None
depends_on = None


TABLE_NAME = "pricing_provider_rx_rollup"
PROVIDER_TABLE = "pricing_provider_prescription"
TABLE_CONTRACT = {
    "year": ("integer", True, 1),
    "rx_code_system": ("character varying(32)", True, 2),
    "rx_code": ("character varying(64)", True, 3),
    "variant_id": ("bigint", True, 4),
    "rx_name": ("character varying", False, 0),
    "generic_name": ("character varying", False, 0),
    "brand_name": ("character varying", False, 0),
    "total_claims": ("double precision", False, 0),
    "total_drug_cost": ("numeric", False, 0),
    "total_benes": ("double precision", False, 0),
    "source_relation_fingerprint": ("character varying(128)", True, 0),
}
TABLE_COLUMNS = set(TABLE_CONTRACT)
PROVIDER_COLUMNS = {
    "year",
    "rx_code_system",
    "rx_code",
    "rx_name",
    "generic_name",
    "brand_name",
    "total_claims",
    "total_drug_cost",
    "total_benes",
}


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, identifier: str) -> str:
    return f"{_q(schema)}.{_q(identifier)}"


def _create_table_sql(schema: str) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, TABLE_NAME)} (
            year integer NOT NULL,
            rx_code_system varchar(32) NOT NULL,
            rx_code varchar(64) NOT NULL,
            variant_id bigint NOT NULL,
            rx_name varchar,
            generic_name varchar,
            brand_name varchar,
            total_claims double precision,
            total_drug_cost numeric,
            total_benes double precision,
            source_relation_fingerprint varchar(128) NOT NULL,
            PRIMARY KEY (year, rx_code_system, rx_code, variant_id)
        )
    """


def _table_columns(schema: str, table_name: str) -> set[str]:
    return {
        str(row[0])
        for row in op.get_bind().execute(
            text(
                """
                SELECT column_name
                  FROM information_schema.columns
                 WHERE table_schema = :schema
                   AND table_name = :table_name
                """
            ),
            {"schema": schema, "table_name": table_name},
        )
    }


def _table_contract(schema: str, table_name: str):
    rows = list(
        op.get_bind().execute(
            text(
                """
                SELECT relation.relkind::text,
                       attribute.attname,
                       pg_catalog.format_type(
                           attribute.atttypid,
                           attribute.atttypmod
                       ),
                       attribute.attnotnull,
                       COALESCE(
                           array_position(primary_key.conkey, attribute.attnum),
                           0
                       )
                  FROM pg_catalog.pg_class AS relation
                  JOIN pg_catalog.pg_namespace AS namespace
                    ON namespace.oid = relation.relnamespace
                  JOIN pg_catalog.pg_attribute AS attribute
                    ON attribute.attrelid = relation.oid
                   AND attribute.attnum > 0
                   AND NOT attribute.attisdropped
             LEFT JOIN pg_catalog.pg_constraint AS primary_key
                    ON primary_key.conrelid = relation.oid
                   AND primary_key.contype = 'p'
                 WHERE namespace.nspname = :schema
                   AND relation.relname = :table_name
                 ORDER BY attribute.attnum
                """
            ),
            {"schema": schema, "table_name": table_name},
        )
    )
    relation_kind = str(rows[0][0]) if rows else None
    column_contract = {
        str(row[1]): (str(row[2]), bool(row[3]), int(row[4])) for row in rows
    }
    return relation_kind, column_contract


def _backfill(schema: str) -> None:
    op.execute(f"TRUNCATE TABLE {_qt(schema, TABLE_NAME)}")
    op.execute(
        prescription_autocomplete_rollup_insert_sql(
            schema=schema,
            rollup_table=TABLE_NAME,
            provider_table=PROVIDER_TABLE,
        )
    )


def upgrade() -> None:
    """Create and backfill the provider-backed autocomplete rollup."""

    schema = _schema()
    op.execute(_create_table_sql(schema))
    if op.get_context().as_sql:
        return
    relation_kind, table_contract = _table_contract(schema, TABLE_NAME)
    if relation_kind not in {"r", "p"} or table_contract != TABLE_CONTRACT:
        raise RuntimeError(f"existing_schema_table_mismatch:{schema}.{TABLE_NAME}")
    if PROVIDER_COLUMNS.issubset(_table_columns(schema, PROVIDER_TABLE)):
        _backfill(schema)


def downgrade() -> None:
    """Remove the optional autocomplete rollup."""

    op.execute(f"DROP TABLE IF EXISTS {_qt(_schema(), TABLE_NAME)}")
