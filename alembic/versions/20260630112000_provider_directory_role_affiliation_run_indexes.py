"""Add Provider Directory resource run/source indexes.

Revision ID: 20260630112000_provider_directory_role_affiliation_run_indexes
Revises: 20260630110000_entity_address_unified_service_phone_indexes
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text


revision = "20260630112000_provider_directory_role_affiliation_run_indexes"
down_revision = "20260630110000_entity_address_unified_service_phone_indexes"
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


def upgrade():
    bind = op.get_bind()
    schema = _schema()
    if _table_exists(bind, schema, "provider_directory_location"):
        op.execute(
            f"CREATE INDEX IF NOT EXISTS provider_directory_location_run_source_idx "
            f"ON {_qt(schema, 'provider_directory_location')} (last_seen_run_id, source_id);"
        )
    if _table_exists(bind, schema, "provider_directory_practitioner_role"):
        op.execute(
            f"CREATE INDEX IF NOT EXISTS provider_directory_role_run_source_idx "
            f"ON {_qt(schema, 'provider_directory_practitioner_role')} (last_seen_run_id, source_id);"
        )
    if _table_exists(bind, schema, "provider_directory_organization"):
        op.execute(
            f"CREATE INDEX IF NOT EXISTS provider_directory_organization_run_source_idx "
            f"ON {_qt(schema, 'provider_directory_organization')} (last_seen_run_id, source_id);"
        )
    if _table_exists(bind, schema, "provider_directory_organization_affiliation"):
        op.execute(
            f"CREATE INDEX IF NOT EXISTS provider_directory_affiliation_run_source_idx "
            f"ON {_qt(schema, 'provider_directory_organization_affiliation')} (last_seen_run_id, source_id);"
        )


def downgrade():
    schema = _schema()
    op.execute(f"DROP INDEX IF EXISTS {_q(schema)}.provider_directory_affiliation_run_source_idx;")
    op.execute(f"DROP INDEX IF EXISTS {_q(schema)}.provider_directory_organization_run_source_idx;")
    op.execute(f"DROP INDEX IF EXISTS {_q(schema)}.provider_directory_role_run_source_idx;")
    op.execute(f"DROP INDEX IF EXISTS {_q(schema)}.provider_directory_location_run_source_idx;")
