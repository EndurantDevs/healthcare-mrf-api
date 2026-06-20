"""Add OpenAddresses ZIP restore provenance columns."""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text


revision = "20260620120000_openaddresses_zip_restore"
down_revision = "20260619165000_address_archive_completion_scope_index"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _table_exists(bind, schema: str, table: str) -> bool:
    return bool(
        bind.execute(
            text("SELECT to_regclass(:name)"),
            {"name": f"{schema}.{table}"},
        ).scalar()
    )


def upgrade():
    bind = op.get_bind()
    schema = _schema()
    if not _table_exists(bind, schema, "openaddresses_geocode"):
        return
    table = _qt(schema, "openaddresses_geocode")
    bind.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS zip5_source text;")
    bind.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS zip5_restored_at timestamptz;")


def downgrade():
    bind = op.get_bind()
    schema = _schema()
    if not _table_exists(bind, schema, "openaddresses_geocode"):
        return
    table = _qt(schema, "openaddresses_geocode")
    bind.exec_driver_sql(f"ALTER TABLE {table} DROP COLUMN IF EXISTS zip5_restored_at;")
    bind.exec_driver_sql(f"ALTER TABLE {table} DROP COLUMN IF EXISTS zip5_source;")
