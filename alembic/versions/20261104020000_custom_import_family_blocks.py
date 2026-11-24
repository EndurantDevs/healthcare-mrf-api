# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Add compact layout data and resolve snapshots by their qualified names."""

import importlib.util
from pathlib import Path

from sqlalchemy import text

from alembic import op

revision = "20261104020000_custom_import_family_blocks"
down_revision = "20261123182000_custom_import_root_boolean_index"
branch_labels = None
depends_on = None

_ROLES = (
    (1, "root_record", "custom_import_root_record"),
    (2, "entity_binding", "custom_import_entity_binding"),
    (3, "pack", "custom_import_pack"),
    (4, "rejection", "custom_import_rejection"),
    (9, "generation_family", "custom_import_generation_family"),
    (12, "winner", "custom_import_winner"),
    (101, "family", "custom_import_block_family"),
    (102, "fragment", "custom_import_block_fragment"),
    (103, "revision_run", "custom_import_block_revision_run"),
    (104, "key_dictionary", "custom_import_block_key"),
    (105, "query_root", "custom_import_block_query_root"),
    (106, "query_child", "custom_import_block_query_child"),
    (108, "query_record", "custom_import_block_query_record"),
)

_DDL = (
    """
CREATE TABLE __SCHEMA__.custom_import_family_block_layout (
 family_id bigint PRIMARY KEY CHECK (family_id>0),
 layout_contract varchar(63) NOT NULL CHECK (layout_contract='custom-import/family-block-layout/v1'),
 definition_sha256 bytea NOT NULL CHECK (octet_length(definition_sha256)=32),
 collection_slots jsonb NOT NULL CHECK (jsonb_typeof(collection_slots)='object'),
 phase varchar(16) NOT NULL CHECK (phase IN ('loading','closed','verified','logged')),
 source_accepted_family_count bigint, source_rejection_count bigint,
 CHECK ((phase='loading' AND source_accepted_family_count IS NULL AND source_rejection_count IS NULL) OR
        (source_accepted_family_count IS NOT NULL AND source_rejection_count IS NOT NULL AND
         source_accepted_family_count>=0 AND source_rejection_count>=0)),
 closed_at timestamptz, validation_sha256 bytea, source_record_count bigint, role_counts jsonb,
 CHECK ((phase IN ('loading','closed') AND source_record_count IS NULL AND role_counts IS NULL) OR
        (phase IN ('verified','logged') AND
         source_record_count IS NOT NULL AND source_record_count>=0 AND role_counts IS NOT NULL AND jsonb_typeof(role_counts)='object')),
 CHECK ((phase='loading' AND closed_at IS NULL) OR (phase<>'loading' AND closed_at IS NOT NULL)),
 CHECK ((phase IN ('loading','closed') AND validation_sha256 IS NULL) OR
        (phase IN ('verified','logged') AND validation_sha256 IS NOT NULL AND octet_length(validation_sha256)=32))
)
""",
)


def _previous(name):
    path = Path(__file__).with_name(name + ".py")
    spec = importlib.util.spec_from_file_location("family_blocks_" + name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _schema():
    return _previous("20261005030000_custom_import_snapshot_storage")._schema()


def _compact_relation_locks(procedure_sql, schema):
    """Select compact leaves only inside the existing validated snapshot scope."""
    storage = _previous("20261005030000_custom_import_snapshot_storage")
    control = storage._quote(schema)
    marker = f"    PERFORM {control}.resolve_custom_import_snapshot_relations(f.family_id);"
    lock = "    EXECUTE 'LOCK TABLE ' || lock_targets || ' IN ACCESS SHARE MODE';"
    if procedure_sql.count(marker) != 2:
        raise RuntimeError("custom_import_family_blocks_binding_mismatch")
    before, legacy, after = procedure_sql.split(marker)
    if legacy.count(lock) != 1:
        raise RuntimeError("custom_import_family_blocks_lock_mismatch")
    block_names = ",".join(storage._literal(name) for _slot, _role, name in _ROLES)
    return (
        before
        + f"""    IF EXISTS (SELECT 1 FROM {control}.custom_import_family_block_layout
               WHERE family_id=f.family_id) THEN
        IF NOT EXISTS (SELECT 1 FROM {control}.custom_import_family_block_layout
                       WHERE family_id=f.family_id AND phase='logged') THEN
            RAISE EXCEPTION 'custom_import_family_block_phase_mismatch';
        END IF;
        SELECT string_agg(format('%I.%I','ci_snapshot_' || f.family_id::text,name),',' ORDER BY ordinal)
            INTO lock_targets FROM unnest(ARRAY[{block_names}]) WITH ORDINALITY AS relations(name,ordinal);
        EXECUTE 'LOCK TABLE ' || lock_targets || ' IN ACCESS SHARE MODE';
    ELSE
"""
        + marker
        + legacy
        + marker
        + "\n    END IF;"
        + after
    )


def upgrade():
    """Create compact metadata and preserve the existing read entrypoint and grants."""
    schema = _schema()
    storage = _previous("20261005030000_custom_import_snapshot_storage")
    quoted = storage._quote(schema)
    connection = op.get_bind()
    op.execute("SET LOCAL lock_timeout='100ms'")
    op.execute(storage._sql(schema, _DDL[0]))
    owner = connection.execute(
        text("SELECT pg_catalog.pg_get_userbyid(relowner) FROM pg_catalog.pg_class WHERE oid=to_regclass(:name)"),
        {"name": f"{quoted}.custom_import_snapshot_family"},
    ).scalar_one()
    layout_table = f"{quoted}.custom_import_family_block_layout"
    op.execute(f"ALTER TABLE {layout_table} OWNER TO {storage._quote(owner)}")
    op.execute(f"REVOKE ALL ON TABLE {layout_table} FROM PUBLIC")
    grantees = (
        connection.execute(
            text(
                "SELECT DISTINCT r.rolname FROM pg_catalog.pg_class c, "
                "LATERAL pg_catalog.aclexplode(c.relacl) acl JOIN pg_catalog.pg_roles r ON r.oid=acl.grantee "
                "WHERE c.oid=to_regclass(:name) AND r.rolname<>:owner"
            ),
            {"name": layout_table, "owner": owner},
        )
        .scalars()
        .all()
    )
    for grantee in grantees:
        op.execute(f"REVOKE ALL ON TABLE {layout_table} FROM {storage._quote(grantee)}")
    names = _previous("20261114010000_custom_import_snapshot_names")
    names.op = op
    for name, arguments in (
        ("resolve_custom_import_generation_snapshot", "bigint,bigint,bigint,bigint"),
        ("lock_custom_import_snapshot_finality", names._FINALITY),
    ):
        names._refresh(f"{quoted}.{name}({arguments})", lambda body: _compact_relation_locks(body, schema))


def downgrade():
    """Keep compact snapshots readable until a forward migration retires them."""
    raise RuntimeError("custom_import_family_blocks_require_forward_migration")
