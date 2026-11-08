# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Keep aggregate scratch cleanup evidence under existing native owner custody."""

import os

from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20261102030000_custom_import_build_scratch"
down_revision = "20261102010000_custom_import_snapshot_retirement"
branch_labels = None
depends_on = None

_TABLE_DDL = (
    "\n"
    "CREATE TABLE __SCHEMA__.custom_import_build_scratch_cleanup (\n"
    "\tfamily_id BIGINT NOT NULL, \n"
    "\tengine_id VARCHAR(63) NOT NULL, \n"
    "\tbuild_id BIGINT NOT NULL, \n"
    "\tgeneration_id BIGINT NOT NULL, \n"
    "\tdataset_id BIGINT NOT NULL, \n"
    "\tdefinition_revision_id BIGINT NOT NULL, \n"
    "\tschema_revision_id BIGINT NOT NULL, \n"
    "\texecution_id BIGINT NOT NULL, \n"
    "\tcapture_bundle_id BIGINT NOT NULL, \n"
    "\tproducing_fence BIGINT NOT NULL, \n"
    "\tproducing_token_sha256 BYTEA NOT NULL, \n"
    "\tsource_frozen_at TIMESTAMP WITH TIME ZONE NOT NULL, \n"
    "\tgraph_frozen_at TIMESTAMP WITH TIME ZONE NOT NULL, \n"
    "\toutput_frozen_at TIMESTAMP WITH TIME ZONE NOT NULL, \n"
    "\tverified_at TIMESTAMP WITH TIME ZONE NOT NULL, \n"
    "\tsealed_at TIMESTAMP WITH TIME ZONE NOT NULL, \n"
    "\tmaterialization_sha256 BYTEA NOT NULL, \n"
    "\teffective_output_sha256 BYTEA NOT NULL, \n"
    "\tsource_bundle_sha256 BYTEA NOT NULL, \n"
    "\tcandidate_sha256 BYTEA NOT NULL, \n"
    "\tretained_bytes BIGINT NOT NULL, \n"
    "\tremaining_bytes BIGINT NOT NULL, \n"
    "\taggregate_evidence JSONB NOT NULL, \n"
    "\tcleaned_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL, \n"
    "\tPRIMARY KEY (family_id), \n"
    "\tCONSTRAINT custom_import_scratch_cleanup_digests CHECK "
    "(octet_length(producing_token_sha256)=32 AND octet_length(materialization_sha256)=32 AND "
    "octet_length(effective_output_sha256)=32 AND octet_length(source_bundle_sha256)=32 AND "
    "octet_length(candidate_sha256)=32), \n"
    "\tCONSTRAINT custom_import_scratch_cleanup_evidence CHECK "
    "(jsonb_typeof(aggregate_evidence)='object' AND aggregate_evidence->>'contract' IS NOT "
    "DISTINCT FROM 'custom-import/build-scratch-cleanup/v1' AND "
    "pg_column_size(aggregate_evidence)<=65536), \n"
    "\tCONSTRAINT custom_import_scratch_cleanup_identity CHECK (family_id>0 AND build_id>0 AND "
    "generation_id>0 AND dataset_id>0 AND definition_revision_id>0 AND schema_revision_id>0 AND "
    "execution_id>0 AND capture_bundle_id>0 AND producing_fence>0 AND engine_id ~ "
    "'^[a-z][a-z0-9_]{0,62}$' AND retained_bytes>=0 AND remaining_bytes>=0)\n"
    ")\n"
    "\n"
)


def _schema():
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def _quote(value):
    return '"' + value.replace('"', '""') + '"'


def upgrade():
    """Create one owner-only table, preserving the existing privilege boundary."""
    schema = _schema()
    quoted = _quote(schema)
    owner = op.get_bind().scalar(
        text(
            "SELECT pg_catalog.pg_get_userbyid(relowner) FROM pg_catalog.pg_class "
            "WHERE oid=pg_catalog.to_regclass(:registry) AND relkind='r'"
        ),
        {"registry": f"{quoted}.custom_import_snapshot_family"},
    )
    if owner is None:
        raise RuntimeError("custom_import_scratch_registry_owner_missing")
    ddl_schema = postgresql.dialect().identifier_preparer.quote_schema(schema)
    op.execute(_TABLE_DDL.replace("__SCHEMA__", ddl_schema))
    identity = f"{quoted}.custom_import_build_scratch_cleanup"
    op.execute(f"ALTER TABLE {identity} OWNER TO {_quote(owner)}")
    op.execute(f"REVOKE ALL ON TABLE {identity} FROM PUBLIC")
    grantees = (
        op.get_bind()
        .scalars(
            text("""
        SELECT DISTINCT pg_catalog.pg_get_userbyid(acl.grantee)
        FROM pg_catalog.pg_class c,
             LATERAL pg_catalog.aclexplode(coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))) acl
        WHERE c.oid=pg_catalog.to_regclass(:relation) AND acl.grantee NOT IN (0,c.relowner)
    """),
            {"relation": identity},
        )
        .all()
    )
    for grantee in grantees:
        op.execute(f"REVOKE ALL ON TABLE {identity} FROM {_quote(grantee)}")


def downgrade():
    """Never discard evidence for scratch which has already been removed."""
    identity = f"{_quote(_schema())}.custom_import_build_scratch_cleanup"
    op.execute(f"LOCK TABLE {identity} IN ACCESS EXCLUSIVE MODE")
    if op.get_bind().scalar(text(f"SELECT EXISTS(SELECT 1 FROM {identity})")):
        raise RuntimeError("custom_import_scratch_cleanup_downgrade_blocked")
    op.execute(f"DROP TABLE {identity} RESTRICT")
