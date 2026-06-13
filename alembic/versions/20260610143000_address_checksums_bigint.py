"""Widen durable legacy address checksum columns to bigint.

Local production-like evidence on 2026-06-11:
`mrf.address_archive` contained 916,949 rows and 9 checksum values outside the
signed 32-bit integer range (min -2147478616, max 4150343390). Swap-published
tables are deliberately omitted here; they inherit BigInteger from the model on
their next rebuild instead of being rewritten under an ACCESS EXCLUSIVE lock.
"""

import os

from alembic import op


revision = "20260610143000_address_checksums_bigint"
down_revision = "20260610120000_add_import_run_table"
branch_labels = None
depends_on = None


ADDRESS_CHECKSUM_COLUMNS = {
    "address_archive": ("checksum",),
}


def _schema():
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _widen_column_sql(schema: str, table: str, column: str) -> str:
    schema_literal = schema.replace("'", "''")
    table_literal = table.replace("'", "''")
    column_literal = column.replace("'", "''")
    qualified_table = f"{_quote_ident(schema)}.{_quote_ident(table)}"
    quoted_column = _quote_ident(column)
    return f"""
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = '{schema_literal}'
           AND table_name = '{table_literal}'
           AND column_name = '{column_literal}'
           AND data_type <> 'bigint'
    ) THEN
        ALTER TABLE {qualified_table} ALTER COLUMN {quoted_column} DROP DEFAULT;
        ALTER TABLE {qualified_table}
            ALTER COLUMN {quoted_column} TYPE bigint USING {quoted_column}::bigint;
    END IF;
END $$;
"""


def upgrade():
    schema = _schema()
    bind = op.get_bind()
    for table, columns in ADDRESS_CHECKSUM_COLUMNS.items():
        for column in columns:
            bind.exec_driver_sql(_widen_column_sql(schema, table, column))


def downgrade():
    # Do not narrow address checksum columns back to integer. Production archive
    # data contains checksum values outside signed 32-bit integer range.
    pass
