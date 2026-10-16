# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Add projection custody and nullable, legacy-safe immutable seal commitments."""

from __future__ import annotations

import os

import sqlalchemy as sa
from sqlalchemy.schema import CreateTable

from alembic import op
from db.models.custom_import import CustomImportGeneration, CustomImportGenerationSeal
from db.models.custom_import_query_projection import CustomImportQueryProjection
from db.models.custom_import_storage import CustomImportSnapshotFamily

revision = "20261015020000_custom_import_query_projections"
down_revision = "20261013010000_registry_custom_history_events"
branch_labels = None
depends_on = None

_SEAL_COLUMNS = (
    "query_projection_contract",
    "query_projection_capability_sha256",
    "query_projection_sha256",
    "query_projection_root_count",
    "query_projection_child_count",
)
_CHECK = "custom_import_generation_seal_query_projection_check"


def _schema():
    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def _seal_metadata(schema):
    """Keep this additive metadata usable by historical writer qualification."""

    table = CustomImportGenerationSeal.__table__
    for name in _SEAL_COLUMNS:
        op.add_column(table.name, sa.Column(name, table.c[name].type), schema=schema)
    check = next(constraint for constraint in table.constraints if constraint.name == _CHECK)
    op.create_check_constraint(_CHECK, table.name, str(check.sqltext), schema=schema)


def _manifest_table(schema):
    metadata = sa.MetaData()
    for model in (CustomImportGeneration, CustomImportSnapshotFamily, CustomImportQueryProjection):
        table = model.__table__.to_metadata(
            metadata,
            schema=schema,
            referred_schema_fn=lambda *args: schema,
        )
    return table


def _read_privileges(schema):
    """Copy existing seal read grants, closing default table and column writes."""

    connection = op.get_bind()
    quote = connection.dialect.identifier_preparer.quote
    destination = f"{quote(schema)}.custom_import_query_projection"
    control = f"{quote(schema)}.custom_import_generation_seal"
    owner = connection.scalar(sa.text("SELECT current_user"))
    grantees = (
        connection.execute(
            sa.text(
                "SELECT DISTINCT CASE WHEN acl.grantee=0 THEN 'PUBLIC' ELSE role.rolname END AS grantee "
                "FROM pg_class relation CROSS JOIN LATERAL aclexplode(coalesce(relation.relacl, "
                "acldefault('r',relation.relowner))) acl LEFT JOIN pg_roles role ON role.oid=acl.grantee "
                "WHERE relation.oid=to_regclass(:relation) AND acl.grantee<>relation.relowner"
            ),
            {"relation": destination},
        )
        .scalars()
        .all()
    )
    for grantee in set(grantees) | {"PUBLIC"}:
        role = "PUBLIC" if grantee == "PUBLIC" else quote(grantee)
        op.execute(sa.text(f"REVOKE ALL ON TABLE {destination} FROM {role}"))
    readers = (
        connection.execute(
            sa.text(
                "SELECT DISTINCT role.rolname FROM pg_class relation CROSS JOIN LATERAL "
                "aclexplode(coalesce(relation.relacl,acldefault('r',relation.relowner))) acl "
                "JOIN pg_roles role ON role.oid=acl.grantee "
                "WHERE relation.oid=to_regclass(:relation) AND acl.privilege_type='SELECT' "
                "AND role.rolname<>:owner"
            ),
            {"relation": control, "owner": owner},
        )
        .scalars()
        .all()
    )
    for reader in readers:
        op.execute(sa.text(f"GRANT SELECT ON TABLE {destination} TO {quote(reader)}"))


def upgrade():
    """Add nullable seal evidence and a low-volume custody manifest."""

    schema = _schema()
    # Bound metadata lock waits while preserving stricter session limits.
    op.execute(
        sa.text(
            "SELECT set_config('lock_timeout', LEAST(NULLIF(setting::bigint, 0), 2000)::text, true) "
            "FROM pg_settings WHERE name = 'lock_timeout'"
        )
    )
    _seal_metadata(schema)
    op.execute(CreateTable(_manifest_table(schema)))
    _read_privileges(schema)


def downgrade():
    """Remove unused metadata only; retained projection evidence blocks rollback."""

    schema = _schema()
    quote = op.get_bind().dialect.identifier_preparer.quote
    seal = f"{quote(schema)}.custom_import_generation_seal"
    manifest = f"{quote(schema)}.custom_import_query_projection"
    op.execute(sa.text(f"LOCK TABLE {seal}, {manifest} IN SHARE ROW EXCLUSIVE MODE"))
    if op.get_bind().scalar(
        sa.text(
            f"SELECT EXISTS(SELECT 1 FROM {manifest}) OR "
            f"EXISTS(SELECT 1 FROM {seal} WHERE query_projection_contract IS NOT NULL)"
        )
    ):
        raise RuntimeError("custom_import_query_projection_downgrade_blocked")
    op.drop_table("custom_import_query_projection", schema=schema)
    op.drop_constraint(_CHECK, "custom_import_generation_seal", schema=schema, type_="check")
    for name in reversed(_SEAL_COLUMNS):
        op.drop_column("custom_import_generation_seal", name, schema=schema)
