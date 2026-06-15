"""Add source identifiers to facility anchors.

Revision ID: 20260615190000
Revises: 20260615170000_address_archive_geo_source
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text


revision = "20260615190000_facility_anchor_source_npi"
down_revision = "20260615170000_address_archive_geo_source"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _table_exists(bind, schema: str, table: str) -> bool:
    return bool(
        bind.execute(
            text("SELECT to_regclass(:name)"),
            {"name": f"{schema}.{table}"},
        ).scalar()
    )


def _column_exists(bind, schema: str, table: str, column: str) -> bool:
    return bool(
        bind.execute(
            text(
                """
                SELECT 1
                  FROM information_schema.columns
                 WHERE table_schema = :schema
                   AND table_name = :table
                   AND column_name = :column
                """
            ),
            {"schema": schema, "table": table, "column": column},
        ).scalar()
    )


def upgrade():
    bind = op.get_bind()
    schema = _schema()
    table = "facility_anchor"
    if not _table_exists(bind, schema, table):
        return
    if not _column_exists(bind, schema, table, "npi"):
        op.execute(f"ALTER TABLE {_qt(schema, table)} ADD COLUMN npi bigint;")
    if not _column_exists(bind, schema, table, "medicare_ccn"):
        op.execute(f"ALTER TABLE {_qt(schema, table)} ADD COLUMN medicare_ccn varchar(32);")
    if not _column_exists(bind, schema, table, "telephone_number"):
        op.execute(f"ALTER TABLE {_qt(schema, table)} ADD COLUMN telephone_number varchar(32);")
    op.execute(
        f"CREATE INDEX IF NOT EXISTS facility_anchor_npi_idx "
        f"ON {_qt(schema, table)} (npi) WHERE npi IS NOT NULL;"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS facility_anchor_medicare_ccn_idx "
        f"ON {_qt(schema, table)} (medicare_ccn) WHERE medicare_ccn IS NOT NULL;"
    )


def downgrade():
    schema = _schema()
    table = "facility_anchor"
    op.execute(f"DROP INDEX IF EXISTS {_q(schema)}.facility_anchor_npi_idx;")
    op.execute(f"DROP INDEX IF EXISTS {_q(schema)}.facility_anchor_medicare_ccn_idx;")
    op.execute(f"ALTER TABLE {_qt(schema, table)} DROP COLUMN IF EXISTS telephone_number;")
    op.execute(f"ALTER TABLE {_qt(schema, table)} DROP COLUMN IF EXISTS medicare_ccn;")
    op.execute(f"ALTER TABLE {_qt(schema, table)} DROP COLUMN IF EXISTS npi;")
