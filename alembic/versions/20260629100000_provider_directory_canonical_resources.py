# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add Provider Directory canonical resource tables."""

from __future__ import annotations

import os

from alembic import op


revision = "20260629100000_provider_directory_canonical_resources"
down_revision = "20260628170000_provider_directory_endpoint_resource"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def upgrade():
    schema = _schema()
    bind = op.get_bind()
    bind.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {_q(schema)};")
    bind.exec_driver_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "provider_directory_canonical_resource")} (
            canonical_api_base text NOT NULL,
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            resource_url text,
            payload_hash varchar(64),
            payload_json jsonb,
            first_seen_run_id varchar(64),
            last_seen_run_id varchar(64),
            observed_at timestamp,
            updated_at timestamp,
            PRIMARY KEY (canonical_api_base, resource_type, resource_id)
        );
        """
    )
    bind.exec_driver_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "provider_directory_source_resource")} (
            source_id varchar(64) NOT NULL,
            canonical_api_base text NOT NULL,
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            last_seen_run_id varchar(64),
            observed_at timestamp,
            updated_at timestamp,
            PRIMARY KEY (source_id, resource_type, resource_id)
        );
        """
    )
    statements = (
        f"CREATE INDEX IF NOT EXISTS provider_directory_canonical_resource_type_id_idx ON {_qt(schema, 'provider_directory_canonical_resource')} (resource_type, resource_id)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_canonical_resource_hash_idx ON {_qt(schema, 'provider_directory_canonical_resource')} (payload_hash)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_canonical_resource_run_idx ON {_qt(schema, 'provider_directory_canonical_resource')} (last_seen_run_id)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_source_resource_canonical_idx ON {_qt(schema, 'provider_directory_source_resource')} (canonical_api_base, resource_type, resource_id)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_source_resource_run_idx ON {_qt(schema, 'provider_directory_source_resource')} (last_seen_run_id)",
    )
    for statement in statements:
        bind.exec_driver_sql(statement + ";")


def downgrade():
    return None
