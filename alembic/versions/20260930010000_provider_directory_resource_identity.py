# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add immutable indexed plan and practitioner-role source identities."""

import os

from alembic import op

revision = "20260930010000_provider_directory_resource_identity"
down_revision = "20260929030000_provider_directory_mrf_payer_binding"
branch_labels = None
depends_on = None


def _schema():
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"
    if os.getenv("DB_SCHEMA") and os.getenv("DB_SCHEMA") != runtime_schema:
        raise RuntimeError("database schema settings must match")
    return '"' + runtime_schema.replace('"', '""') + '"'


def upgrade():
    """Create additive identity storage; existing resources require bounded backfill."""
    schema = _schema()
    op.execute(f"""
        CREATE TABLE {schema}.provider_directory_resource_identity (
            source_id varchar(64) NOT NULL,
            resource_type varchar(64) NOT NULL,
            resource_id varchar(64) NOT NULL,
            entity_id uuid NOT NULL,
            created_at timestamptz NOT NULL,
            PRIMARY KEY (source_id, resource_type, resource_id),
            CONSTRAINT pd_resource_identity_type_check CHECK (resource_type IN ('InsurancePlan', 'PractitionerRole'))
        )
    """)
    op.execute(
        f"CREATE UNIQUE INDEX pd_resource_identity_seek_idx ON "
        f"{schema}.provider_directory_resource_identity (source_id, resource_type, entity_id)"
    )
    op.execute(f"""
        CREATE FUNCTION {schema}.guard_provider_directory_resource_identity() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
            RAISE EXCEPTION 'provider_directory_resource_identity_immutable';
        END; $$
    """)
    op.execute(
        f"CREATE TRIGGER pd_resource_identity_immutable BEFORE UPDATE OR DELETE ON "
        f"{schema}.provider_directory_resource_identity FOR EACH ROW "
        f"EXECUTE FUNCTION {schema}.guard_provider_directory_resource_identity()"
    )
    op.execute(
        f"CREATE TRIGGER pd_resource_identity_no_truncate BEFORE TRUNCATE ON "
        f"{schema}.provider_directory_resource_identity FOR EACH STATEMENT "
        f"EXECUTE FUNCTION {schema}.guard_provider_directory_resource_identity()"
    )


def downgrade():
    """Refuse removal of durable public identities without an explicit migration plan."""
    raise RuntimeError("provider_directory_resource_identity_downgrade_requires_explicit_plan")
