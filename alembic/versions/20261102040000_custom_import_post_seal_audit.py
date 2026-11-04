# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Persist owner-only append-only structural audit attempts and checkpoints."""

import os

from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20261102040000_custom_import_post_seal_audit"
down_revision = "20261102030000_custom_import_build_scratch"
branch_labels = None
depends_on = None

_TABLE_DDL = (
    (
        "custom_import_post_seal_audit",
        (
            "\n"
            "CREATE TABLE __SCHEMA__.custom_import_post_seal_audit (\n"
            "\taudit_id UUID NOT NULL, \n"
            "\tfamily_id BIGINT NOT NULL, \n"
            "\tengine_id VARCHAR(63) NOT NULL, \n"
            "\tbuild_id BIGINT NOT NULL, \n"
            "\tgeneration_id BIGINT NOT NULL, \n"
            "\tdataset_id BIGINT NOT NULL, \n"
            "\tsql_sha256 BYTEA NOT NULL, \n"
            "\tbinding_sha256 BYTEA NOT NULL, \n"
            "\tbinding JSONB NOT NULL, \n"
            "\tstarted_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL, \n"
            "\tPRIMARY KEY (audit_id), \n"
            "\tCONSTRAINT custom_import_post_seal_audit_binding CHECK (jsonb_typeof(binding)='object' AND "
            "binding->>'contract' IS NOT DISTINCT FROM 'custom-import/post-seal-structure/v1' AND "
            "pg_column_size(binding)<=262144), \n"
            "\tCONSTRAINT custom_import_post_seal_audit_identity CHECK (family_id>0 AND build_id>0 AND "
            "generation_id>0 AND dataset_id>0 AND engine_id ~ '^[a-z][a-z0-9_]{0,62}$' AND "
            "octet_length(sql_sha256)=32 AND octet_length(binding_sha256)=32)\n"
            ")\n"
            "\n"
        ),
        (
            "CREATE INDEX custom_import_post_seal_audit_family_idx ON __SCHEMA__.custom_import_post_seal_audit (family_id, started_at, audit_id)",
        ),
    ),
    (
        "custom_import_post_seal_audit_checkpoint",
        (
            "\n"
            "CREATE TABLE __SCHEMA__.custom_import_post_seal_audit_checkpoint (\n"
            "\taudit_id UUID NOT NULL, \n"
            "\tcheckpoint BIGINT NOT NULL, \n"
            "\tarm_ordinal SMALLINT NOT NULL, \n"
            "\tnext_ordinal SMALLINT NOT NULL, \n"
            "\troot_after_id BIGINT, \n"
            "\troot_eof BOOLEAN NOT NULL, \n"
            "\toutcome VARCHAR(24) NOT NULL, \n"
            "\tpage_root_count SMALLINT NOT NULL, \n"
            "\trange_sha256 BYTEA, \n"
            "\tstate VARCHAR(16) NOT NULL, \n"
            "\tfailure_code VARCHAR(96), \n"
            "\tcounts JSONB, \n"
            "\tprevious_sha256 BYTEA NOT NULL, \n"
            "\tcheckpoint_sha256 BYTEA NOT NULL, \n"
            "\trecorded_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL, \n"
            "\tPRIMARY KEY (audit_id, checkpoint), \n"
            "\tCONSTRAINT custom_import_post_seal_checkpoint_identity CHECK (checkpoint>=0 AND arm_ordinal "
            "BETWEEN 0 AND 62 AND next_ordinal BETWEEN 1 AND 63 AND (root_after_id IS NULL OR "
            "root_after_id>0) AND octet_length(previous_sha256)=32 AND "
            "octet_length(checkpoint_sha256)=32), \n"
            "\tCONSTRAINT custom_import_post_seal_checkpoint_progress CHECK (state='failed' OR "
            "(next_ordinal=arm_ordinal+1 AND root_after_id IS NULL AND root_eof) OR (arm_ordinal IN "
            "(41,42) AND next_ordinal=arm_ordinal AND root_after_id IS NOT NULL AND NOT root_eof)), \n"
            "\tCONSTRAINT custom_import_post_seal_checkpoint_state CHECK ((state='scanning' AND "
            "failure_code IS NULL AND counts IS NULL AND next_ordinal<=62) OR (state='failed' AND "
            "failure_code IS NOT NULL AND counts IS NULL AND next_ordinal<=62) OR (state='complete' AND "
            "failure_code IS NULL AND arm_ordinal=62 AND next_ordinal=63 AND root_after_id IS NULL AND "
            "root_eof AND counts IS NOT NULL AND jsonb_typeof(counts)='object')), \n"
            "\tCONSTRAINT custom_import_post_seal_checkpoint_outcome CHECK (page_root_count BETWEEN 0 AND "
            "16384 AND outcome IN "
            "('started','passed','range_page','range_eof','not_applicable','counts','failed') AND "
            "(range_sha256 IS NULL OR octet_length(range_sha256)=32))\n"
            ")\n"
            "\n"
        ),
        (),
    ),
)


def _schema():
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def _quote(name):
    return '"' + name.replace('"', '""') + '"'


def _protect(identity, owner):
    op.execute(f"ALTER TABLE {identity} OWNER TO {_quote(owner)}")
    op.execute(f"REVOKE ALL ON TABLE {identity} FROM PUBLIC")
    roles = (
        op.get_bind()
        .scalars(
            text("""SELECT DISTINCT pg_get_userbyid(acl.grantee)
        FROM pg_class c,LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) acl
        WHERE c.oid=to_regclass(:relation) AND acl.grantee NOT IN (0,c.relowner)"""),
            {"relation": identity},
        )
        .all()
    )
    for role in roles:
        op.execute(f"REVOKE ALL ON TABLE {identity} FROM {_quote(role)}")


def upgrade():
    """Create owner-only evidence tables without a new callable or relationship hook."""
    schema = _schema()
    owner = op.get_bind().scalar(
        text("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid=to_regclass(:registry)"),
        {"registry": f"{_quote(schema)}.custom_import_snapshot_family"},
    )
    if owner is None:
        raise RuntimeError("custom_import_post_seal_audit_owner_missing")
    quoted = postgresql.dialect().identifier_preparer.quote_schema(schema)
    for name, definition, indexes in _TABLE_DDL:
        op.execute(definition.replace("__SCHEMA__", quoted))
        for index in indexes:
            op.execute(index.replace("__SCHEMA__", quoted))
        _protect(f"{_quote(schema)}.{name}", owner)


def downgrade():
    """Preserve every audit attempt once its original scratch may be gone."""
    quoted = _quote(_schema())
    names = ("custom_import_post_seal_audit_checkpoint", "custom_import_post_seal_audit")
    op.execute("LOCK TABLE " + ",".join(f"{quoted}.{name}" for name in names) + " IN ACCESS EXCLUSIVE MODE")
    if any(op.get_bind().scalar(text(f"SELECT EXISTS(SELECT 1 FROM {quoted}.{name})")) for name in names):
        raise RuntimeError("custom_import_post_seal_audit_downgrade_blocked")
    for name in names:
        op.execute(f"DROP TABLE {quoted}.{name} RESTRICT")
