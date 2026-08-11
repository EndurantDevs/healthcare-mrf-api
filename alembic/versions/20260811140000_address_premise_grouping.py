# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Persist Provider Directory premise keys for offline grouping.

Revision ID: 20260811140000_address_premise_grouping
Revises: 20260811130000_provider_directory_exact_practitioner_resource_order_repair
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260811140000_address_premise_grouping"
down_revision = (
    "20260811130000_provider_directory_exact_practitioner_resource_order_repair"
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


def _drop_index_sql(schema: str) -> str:
    return (
        "DROP INDEX CONCURRENTLY IF EXISTS "
        f"{_qt(schema, _PREMISE_INDEX)};"
    )


def upgrade() -> None:
    schema = _schema()
    op.execute("SET LOCAL lock_timeout = '5s';")
    # Existing overlays are large and contain no premise values until the
    # required full artifact rebuild. Building their empty partial index here
    # would put a full-table scan inside the deployment migration gate. The
    # overlay publisher instead builds the index on its unpublished stage and
    # promotes it together with the hydrated rows.
    op.execute(_column_sql(schema))


def downgrade() -> None:
    schema = _schema()
    with op.get_context().autocommit_block():
        op.execute(_drop_index_sql(schema))
    op.execute("SET LOCAL lock_timeout = '5s';")
    op.execute(
        f"ALTER TABLE IF EXISTS {_qt(schema, _OVERLAY_TABLE)} "
        "DROP COLUMN IF EXISTS premise_key;"
    )
