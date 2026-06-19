"""Index address archive keys used by completion alias repair."""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text


revision = "20260619165000_address_archive_completion_scope_index"
down_revision = "20260618100000_address_canonical_current_rekey"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


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
    if not _table_exists(bind, schema, "address_archive_v2"):
        return
    archive = _qt(schema, "address_archive_v2")
    bind.exec_driver_sql(
        f"""
        CREATE INDEX IF NOT EXISTS address_archive_v2_completion_scope_idx
            ON {archive} (
                state_code,
                zip5,
                (COALESCE(country_code, 'US')),
                (COALESCE(unit_norm, ''))
            )
            WHERE address_key IS NOT NULL
              AND identity_key IS NOT NULL
              AND COALESCE(precision, split_part(identity_key, '|', 8)) = 'street'
              AND merged_into IS NULL
              AND state_code IS NOT NULL
              AND zip5 IS NOT NULL;
        """
    )


def downgrade():
    bind = op.get_bind()
    schema = _schema()
    bind.exec_driver_sql(
        f"DROP INDEX IF EXISTS {_quote_ident(schema)}.{_quote_ident('address_archive_v2_completion_scope_idx')};"
    )
