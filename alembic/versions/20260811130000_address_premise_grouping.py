# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Persist Provider Directory premise keys for offline grouping.

Revision ID: 20260811130000_address_premise_grouping
Revises: 20260811120000_provider_directory_reviewed_subset_v5_http410_disposition
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260811130000_address_premise_grouping"
down_revision = (
    "20260811120000_provider_directory_reviewed_subset_v5_http410_disposition"
)
branch_labels = None
depends_on = None

_OVERLAY_TABLE = "provider_directory_address_overlay"
_PREMISE_INDEX = "provider_directory_address_overlay_npi_premise_key_idx"


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _column_sql(schema: str) -> str:
    return f"""
    ALTER TABLE IF EXISTS {_qt(schema, _OVERLAY_TABLE)}
        ADD COLUMN IF NOT EXISTS premise_key uuid;
    """


def _index_sql(schema: str) -> str:
    return f"""
    CREATE INDEX IF NOT EXISTS {_q(_PREMISE_INDEX)}
        ON {_qt(schema, _OVERLAY_TABLE)} (npi, premise_key)
     WHERE premise_key IS NOT NULL;
    """


def upgrade() -> None:
    schema = _schema()
    op.execute(_column_sql(schema))
    op.execute(_index_sql(schema))


def downgrade() -> None:
    schema = _schema()
    op.execute(f"DROP INDEX IF EXISTS {_qt(schema, _PREMISE_INDEX)};")
    op.execute(
        f"ALTER TABLE IF EXISTS {_qt(schema, _OVERLAY_TABLE)} "
        "DROP COLUMN IF EXISTS premise_key;"
    )
