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
TABLE_COLUMNS = {
    "year",
    "rx_code_system",
    "rx_code",
    "variant_id",
    "rx_name",
    "generic_name",
    "brand_name",
    "total_claims",
    "total_drug_cost",
    "total_benes",
    "source_relation_fingerprint",
}
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
    if _table_columns(schema, TABLE_NAME) != TABLE_COLUMNS:
        raise RuntimeError(f"existing_schema_table_mismatch:{schema}.{TABLE_NAME}")
    if PROVIDER_COLUMNS.issubset(_table_columns(schema, PROVIDER_TABLE)):
        _backfill(schema)


def downgrade() -> None:
    """Remove the optional autocomplete rollup."""

    op.execute(f"DROP TABLE IF EXISTS {_qt(_schema(), TABLE_NAME)}")
