# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Add empty, owner-only prepared replay storage and an explicit object binding."""

from __future__ import annotations

import os

import sqlalchemy as sa
from sqlalchemy.schema import CreateIndex, CreateTable

from alembic import op
from db.models.custom_import_prepared_source import CustomImportPreparedSource, CustomImportPreparedSourcePart

revision = "20261023010000_custom_import_prepared_source"
down_revision = "20261024050000_provider_directory_first_retirement_owner_lineage"
branch_labels = None
depends_on = None

# shortcut: freeze this revision's table DDL before changing the runtime models used below.
_MODELS = (CustomImportPreparedSource, CustomImportPreparedSourcePart)
_BINDING_TABLE = "custom_import_source_binding_revision"
_BINDING_CHECK = "custom_import_source_binding_revision_shape_check"
_OLD_VARIANT = (
    "binding_contract IN ('custom-import/source-binding/v1', 'custom-import/source-binding/v2') "
    "AND connector_kind = 'snowflake_bundle'"
)
_IDENTITY_CHECKS = (
    "revision_number > 0 AND octet_length(definition_sha256) = 32 AND octet_length(schema_sha256) = 32 AND "
    "octet_length(source_object_fingerprint_sha256) = 32 AND octet_length(binding_sha256) = 32 AND "
    "source_object_version ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$' AND "
    "octet_length(canonical_binding) BETWEEN 2 AND 1048576 AND "
    "binding_sha256 = pg_catalog.sha256(convert_to(binding_contract || ':' || canonical_binding, 'UTF8'))"
)


def _schema():
    """Require the runtime and legacy namespace settings to agree."""
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def _relation(schema, name):
    quote = op.get_bind().dialect.identifier_preparer.quote
    return f"{quote(schema)}.{quote(name)}"


def _bound_lock_wait():
    """Preserve a stricter existing lock budget while bounding schema waits."""
    op.execute(
        sa.text(
            "SELECT set_config('lock_timeout', CASE WHEN current_setting('lock_timeout')::interval > interval '0' "
            "AND current_setting('lock_timeout')::interval < interval '2 seconds' "
            "THEN current_setting('lock_timeout') ELSE '2s' END, true)"
        )
    )


def _close_default_grants(relation):
    """New replay storage needs explicit provisioning, not inherited grants."""
    connection = op.get_bind()
    grantees = connection.scalars(
        sa.text(
            "SELECT DISTINCT role.rolname FROM pg_class relation CROSS JOIN LATERAL "
            "aclexplode(coalesce(relation.relacl,acldefault('r',relation.relowner))) acl "
            "JOIN pg_roles role ON role.oid=acl.grantee WHERE relation.oid=to_regclass(:relation) "
            "AND acl.grantee<>relation.relowner AND acl.grantee<>0"
        ),
        {"relation": relation},
    )
    op.execute(sa.text(f"REVOKE ALL ON TABLE {relation} FROM PUBLIC"))
    quote = connection.dialect.identifier_preparer.quote
    for grantee in sorted(set(grantees)):
        op.execute(sa.text(f"REVOKE ALL ON TABLE {relation} FROM {quote(grantee)}"))


def _replace_binding_shape(schema, *, generic):
    variant = f"({_OLD_VARIANT})"
    if generic:
        variant = (
            f"({variant} OR (binding_contract = 'custom-import/source-binding/v3' "
            "AND connector_kind IN ('https', 's3', 'upload')))"
        )
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(
        sa.text(
            f"ALTER TABLE {_relation(schema, _BINDING_TABLE)} DROP CONSTRAINT {quote(_BINDING_CHECK)}, "
            f"ADD CONSTRAINT {quote(_BINDING_CHECK)} CHECK ({variant} AND {_IDENTITY_CHECKS})"
        )
    )


def upgrade():
    """Create empty relations without populating rows or changing existing guards."""
    schema = _schema()
    _bound_lock_wait()
    for model in _MODELS:
        table = model.__table__.to_metadata(sa.MetaData(), schema=schema)
        op.execute(CreateTable(table))
        for index in table.indexes:
            op.execute(CreateIndex(index))
        _close_default_grants(_relation(schema, table.name))
    _replace_binding_shape(schema, generic=True)


def downgrade():
    """Refuse removal while any replay or generic binding remains retained."""
    schema = _schema()
    if op.get_bind().scalar(sa.text("SELECT current_setting('transaction_isolation')")) != "read committed":
        raise RuntimeError("custom_import_prepared_source_downgrade_isolation")
    _bound_lock_wait()
    relations = [_relation(schema, model.__tablename__) for model in _MODELS]
    binding = _relation(schema, _BINDING_TABLE)
    op.execute(sa.text("LOCK TABLE " + ",".join([binding, *relations]) + " IN ACCESS EXCLUSIVE MODE"))
    probes = [f"EXISTS(SELECT 1 FROM {relation})" for relation in relations]
    probes.append(f"EXISTS(SELECT 1 FROM {binding} WHERE binding_contract='custom-import/source-binding/v3')")
    if op.get_bind().scalar(sa.text("SELECT " + " OR ".join(probes))):
        raise RuntimeError("custom_import_prepared_source_downgrade_blocked")
    _replace_binding_shape(schema, generic=False)
    for model in reversed(_MODELS):
        op.drop_table(model.__tablename__, schema=schema)
