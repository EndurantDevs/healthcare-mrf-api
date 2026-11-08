# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Add original-object storage without changing existing capture contracts."""

from __future__ import annotations

import os

import sqlalchemy as sa
from sqlalchemy.schema import CreateIndex, CreateTable

from alembic import op
from db.models.custom_import_raw_capture import CustomImportCaptureRawObject

revision = "20261017010000_custom_import_raw_objects"
down_revision = "20261017210000_provider_directory_readiness_indexes"
branch_labels = None
depends_on = None


def _schema():
    """Require the runtime and legacy namespace settings to agree."""
    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def _close_default_grants(table):
    """Require explicit registrar provisioning, not inherited default write grants."""
    connection = op.get_bind()
    quote = connection.dialect.identifier_preparer.quote
    relation = f"{quote(table.schema)}.{quote(table.name)}"
    grantees = (
        connection.execute(
            sa.text(
                "SELECT DISTINCT role.rolname "
                "FROM pg_class relation CROSS JOIN LATERAL aclexplode(coalesce(relation.relacl, "
                "acldefault('r',relation.relowner))) acl JOIN pg_roles role ON role.oid=acl.grantee "
                "WHERE relation.oid=to_regclass(:relation) AND acl.grantee<>relation.relowner AND acl.grantee<>0"
            ),
            {"relation": relation},
        )
        .scalars()
        .all()
    )
    op.execute(sa.text(f"REVOKE ALL ON TABLE {relation} FROM PUBLIC"))
    for grantee in sorted(set(grantees)):
        op.execute(sa.text(f"REVOKE ALL ON TABLE {relation} FROM {quote(grantee)}"))


def _bound_lock_wait():
    """Cap schema lock waits without widening an existing stricter limit."""
    op.execute(
        sa.text(
            "SELECT set_config('lock_timeout', CASE "
            "WHEN current_setting('lock_timeout')::interval > interval '0' "
            "AND current_setting('lock_timeout')::interval < interval '2 seconds' "
            "THEN current_setting('lock_timeout') ELSE '2s' END, true)"
        )
    )


def upgrade():
    """Create an empty migration-owned relation with no automatic runtime grants."""
    table = CustomImportCaptureRawObject.__table__.to_metadata(sa.MetaData(), schema=_schema())
    _bound_lock_wait()
    op.execute(CreateTable(table))
    for index in table.indexes:
        op.execute(CreateIndex(index))
    _close_default_grants(table)


def downgrade():
    """Refuse to remove retained originals, including unbound or abandoned captures."""
    schema = _schema()
    if op.get_bind().scalar(sa.text("SELECT current_setting('transaction_isolation')")) != "read committed":
        raise RuntimeError("custom_import_raw_objects_downgrade_isolation")
    _bound_lock_wait()
    quote = op.get_bind().dialect.identifier_preparer.quote
    relation = f"{quote(schema)}.custom_import_capture_raw_object"
    op.execute(sa.text(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE"))
    if op.get_bind().scalar(sa.text(f"SELECT EXISTS(SELECT 1 FROM {relation})")):
        raise RuntimeError("custom_import_raw_objects_downgrade_blocked")
    op.drop_table("custom_import_capture_raw_object", schema=schema)
