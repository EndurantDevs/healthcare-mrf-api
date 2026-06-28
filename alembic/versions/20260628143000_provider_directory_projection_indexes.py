# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

from alembic import op


revision = "20260628143000_provider_directory_projection_indexes"
down_revision = "20260628100000_provider_directory_fhir_tables"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def upgrade():
    bind = op.get_bind()
    schema = _schema()
    bind.exec_driver_sql(
        f"""
        CREATE UNLOGGED TABLE IF NOT EXISTS {_qt(schema, 'provider_directory_import_seen')} (
            run_id varchar(64) NOT NULL,
            resource_type varchar(64) NOT NULL,
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            seen_at timestamp NOT NULL DEFAULT now(),
            PRIMARY KEY (run_id, resource_type, source_id, resource_id)
        );
        """
    )
    statements = (
        f"CREATE INDEX IF NOT EXISTS provider_directory_import_seen_source_idx ON {_qt(schema, 'provider_directory_import_seen')} (run_id, resource_type, source_id)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_role_source_practitioner_idx ON {_qt(schema, 'provider_directory_practitioner_role')} (source_id, practitioner_ref)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_role_source_organization_idx ON {_qt(schema, 'provider_directory_practitioner_role')} (source_id, organization_ref)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_affiliation_source_organization_idx ON {_qt(schema, 'provider_directory_organization_affiliation')} (source_id, organization_ref)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_affiliation_source_participating_idx ON {_qt(schema, 'provider_directory_organization_affiliation')} (source_id, participating_organization_ref)",
    )
    for statement in statements:
        bind.exec_driver_sql(statement + ";")


def downgrade():
    return None
