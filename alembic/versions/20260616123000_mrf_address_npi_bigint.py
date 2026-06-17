"""Widen MRF address NPI columns to bigint.

Revision ID: 20260616123000_mrf_address_npi_bigint
Revises: 20260616090000_openaddresses_geocode, 20260616110000_facility_anchor_parent_npi_overrides
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text


revision = "20260616123000_mrf_address_npi_bigint"
down_revision = (
    "20260616090000_openaddresses_geocode",
    "20260616110000_facility_anchor_parent_npi_overrides",
)
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _qualified_table(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def _address_tables_with_narrow_npi(bind, schema: str) -> list[str]:
    rows = bind.execute(
        text(
            """
            SELECT table_name
              FROM information_schema.columns
             WHERE table_schema = :schema
               AND column_name = 'npi'
               AND data_type <> 'bigint'
               AND (
                    table_name = 'mrf_address'
                    OR (
                        table_name LIKE 'mrf_address\\_%' ESCAPE '\\'
                        AND table_name NOT LIKE 'mrf_address_evidence%'
                    )
                    OR table_name = 'npi_address'
                    OR table_name LIKE 'npi_address\\_%' ESCAPE '\\'
               )
             ORDER BY table_name
            """
        ),
        {"schema": schema},
    )
    return [row[0] for row in rows]


def upgrade():
    bind = op.get_bind()
    schema = _schema()
    for table in _address_tables_with_narrow_npi(bind, schema):
        bind.exec_driver_sql(
            f"ALTER TABLE {_qualified_table(schema, table)} "
            f"ALTER COLUMN {_quote_ident('npi')} DROP DEFAULT;"
        )
        bind.exec_driver_sql(
            f"ALTER TABLE {_qualified_table(schema, table)} "
            f"ALTER COLUMN {_quote_ident('npi')} TYPE bigint USING {_quote_ident('npi')}::bigint;"
        )


def downgrade():
    # Do not narrow NPI columns back to integer. Valid 10-digit NPIs can exceed
    # signed 32-bit integer range.
    pass
