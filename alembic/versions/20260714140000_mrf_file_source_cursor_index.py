"""Add the MRF discovery file export cursor index.

Revision ID: 20260714140000_mrf_file_source_cursor_index
Revises: 20260714130000_provider_directory_plan_index_shape_repair
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260714140000_mrf_file_source_cursor_index"
down_revision = "20260714130000_provider_directory_plan_index_shape_repair"
branch_labels = None
depends_on = None


INDEX_NAME = "mrf_file_source_cursor_idx"
TABLE_NAME = "mrf_file"


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _quoted_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _qualified_name(name: str) -> str:
    return f"{_quoted_identifier(_schema())}.{_quoted_identifier(name)}"


def upgrade() -> None:
    table_name = _qualified_name(TABLE_NAME)
    index_name = _qualified_name(INDEX_NAME)
    regclass_name = table_name.replace("'", "''")
    create_index = (
        f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} "
        f"({_quoted_identifier('source_id')}, {_quoted_identifier('mrf_file_id')})"
    ).replace("'", "''")
    op.execute(
        f"""
        DO $migration$
        BEGIN
            IF to_regclass('{regclass_name}') IS NOT NULL THEN
                EXECUTE '{create_index}';
            END IF;
        END
        $migration$;
        """
    )


def downgrade() -> None:
    op.execute(f"DROP INDEX IF EXISTS {_qualified_name(INDEX_NAME)}")
