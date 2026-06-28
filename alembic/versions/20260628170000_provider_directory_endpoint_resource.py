# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

from alembic import op
from sqlalchemy import text


revision = "20260628170000_provider_directory_endpoint_resource"
down_revision = "20260628143000_provider_directory_projection_indexes"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _table_exists(bind, schema: str, table: str) -> bool:
    return bool(
        bind.execute(
            text("SELECT to_regclass(:name)"),
            {"name": f"{schema}.{table}"},
        ).scalar()
    )


def _add_column_if_table_exists(bind, schema: str, table: str, column: str, column_type: str) -> None:
    if not _table_exists(bind, schema, table):
        return
    bind.exec_driver_sql(
        f"""
        ALTER TABLE {_qt(schema, table)}
        ADD COLUMN IF NOT EXISTS {_q(column)} {column_type};
        """
    )


def upgrade():
    bind = op.get_bind()
    schema = _schema()
    bind.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {_q(schema)};")
    bind.exec_driver_sql(
        f"DROP INDEX IF EXISTS {_qt(schema, 'provider_directory_import_seen_source_idx')};"
    )
    for table in (
        "provider_directory_organization",
        "provider_directory_practitioner_role",
        "provider_directory_healthcare_service",
        "provider_directory_organization_affiliation",
    ):
        _add_column_if_table_exists(bind, schema, table, "endpoint_refs", "jsonb")

    bind.exec_driver_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "provider_directory_endpoint")} (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            resource_url text,
            status varchar(64),
            connection_type_system text,
            connection_type_code varchar(128),
            connection_type_display varchar(256),
            name varchar(512),
            managing_organization_ref text,
            contact jsonb,
            period_start varchar(64),
            period_end varchar(64),
            payload_type_codes jsonb,
            payload_mime_types jsonb,
            address text,
            header jsonb,
            last_seen_run_id varchar(64),
            observed_at timestamp,
            updated_at timestamp,
            PRIMARY KEY (source_id, resource_id)
        );
        """
    )
    statements = (
        f"CREATE INDEX IF NOT EXISTS provider_directory_endpoint_source_managing_org_idx ON {_qt(schema, 'provider_directory_endpoint')} (source_id, managing_organization_ref)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_endpoint_status_idx ON {_qt(schema, 'provider_directory_endpoint')} (status)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_endpoint_connection_type_idx ON {_qt(schema, 'provider_directory_endpoint')} (connection_type_code)",
    )
    for statement in statements:
        bind.exec_driver_sql(statement + ";")


def downgrade():
    return None
