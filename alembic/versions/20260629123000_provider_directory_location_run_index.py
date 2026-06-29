"""Add Provider Directory Location run index."""

from __future__ import annotations

import os

from alembic import op


revision = "20260629123000_provider_directory_location_run_index"
down_revision = "20260629100000_provider_directory_canonical_resources"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def upgrade():
    schema = _schema()
    op.execute(
        f"CREATE INDEX IF NOT EXISTS provider_directory_location_run_idx "
        f"ON {_qt(schema, 'provider_directory_location')} (last_seen_run_id);"
    )


def downgrade():
    schema = _schema()
    op.execute(f"DROP INDEX IF EXISTS {_q(schema)}.provider_directory_location_run_idx;")
