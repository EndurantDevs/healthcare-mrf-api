# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Register owner-created family blocks without a new callable or privilege."""

import importlib.util
from pathlib import Path

from sqlalchemy import text

from alembic import op

revision = "20261104020000_custom_import_family_blocks"
down_revision = "20261107010000_custom_import_child_reference_aggregate"
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
    (107, "shard_receipt", "custom_import_block_shard"),
    (108, "query_record", "custom_import_block_query_record"),
)

_DDL = (
    """
CREATE TABLE __SCHEMA__.custom_import_family_block_layout (
 family_id bigint PRIMARY KEY CHECK (family_id>0),
 layout_contract varchar(63) NOT NULL CHECK (layout_contract='custom-import/family-block-layout/v1'),
 definition_sha256 bytea NOT NULL CHECK (octet_length(definition_sha256)=32),
 layout_sha256 bytea NOT NULL CHECK (octet_length(layout_sha256)=32),
 collection_slots jsonb NOT NULL CHECK (jsonb_typeof(collection_slots)='object'),
 phase varchar(16) NOT NULL CHECK (phase IN ('loading','closed','verified','logged')),
 owner_checkpoint_revision bigint, owner_checkpoint_sha256 bytea,
 CHECK ((owner_checkpoint_revision IS NULL AND owner_checkpoint_sha256 IS NULL) OR
        (owner_checkpoint_revision IS NOT NULL AND owner_checkpoint_revision>0 AND
         owner_checkpoint_sha256 IS NOT NULL AND octet_length(owner_checkpoint_sha256)=32)),
 source_accepted_family_count bigint, source_rejection_count bigint,
 CHECK ((phase='loading' AND source_accepted_family_count IS NULL AND source_rejection_count IS NULL) OR
        (source_accepted_family_count IS NOT NULL AND source_rejection_count IS NOT NULL AND
         source_accepted_family_count>=0 AND source_rejection_count>=0)),
 closed_at timestamptz, validation_sha256 bytea, expected_completed_shards bigint, source_record_count bigint, role_counts jsonb,
 CHECK ((phase IN ('loading','closed') AND expected_completed_shards IS NULL AND source_record_count IS NULL AND role_counts IS NULL) OR
        (phase IN ('verified','logged') AND expected_completed_shards IS NOT NULL AND expected_completed_shards>0 AND
         source_record_count IS NOT NULL AND source_record_count>=0 AND role_counts IS NOT NULL AND jsonb_typeof(role_counts)='object')),
 CHECK ((phase='loading' AND closed_at IS NULL) OR (phase<>'loading' AND closed_at IS NOT NULL)),
 CHECK ((phase IN ('loading','closed') AND validation_sha256 IS NULL) OR
        (phase IN ('verified','logged') AND validation_sha256 IS NOT NULL AND octet_length(validation_sha256)=32))
)""",
    """
CREATE TABLE __SCHEMA__.custom_import_family_block_relation (
 family_id bigint NOT NULL CHECK (family_id>0),
 role_slot smallint NOT NULL CHECK (role_slot IN (1,2,3,4,9,12,101,102,103,104,105,106,107,108)),
 role_name text NOT NULL, table_oid bigint NOT NULL UNIQUE CHECK (table_oid>0),
 table_owner bigint NOT NULL CHECK (table_owner>0),
 columns_sha256 bytea NOT NULL CHECK (octet_length(columns_sha256)=32),
 constraints_sha256 bytea NOT NULL CHECK (octet_length(constraints_sha256)=32),
 PRIMARY KEY (family_id,role_slot), UNIQUE (family_id,role_name)
)""",
)

# Inserted before, not in place of, the original fifteen-relation path.
_RESOLVER = r"""
    IF EXISTS (SELECT 1 FROM __SCHEMA__.custom_import_family_block_layout WHERE family_id=f.family_id) THEN
        IF f.landing_table_oid IS NOT NULL OR f.origin_table_oid IS NOT NULL
           OR EXISTS (SELECT 1 FROM __SCHEMA__.custom_import_snapshot_relation WHERE family_id=f.family_id)
           OR NOT EXISTS (
            SELECT 1 FROM __SCHEMA__.custom_import_family_block_layout d
            JOIN __SCHEMA__.custom_import_definition_revision v ON v.definition_revision_id=f.definition_revision_id
            JOIN pg_namespace n ON n.nspname=namespace
            JOIN pg_class owner_table ON owner_table.oid=to_regclass(format('%I.custom_import_snapshot_family',__SCHEMA_LITERAL__))
            WHERE d.family_id=f.family_id AND d.layout_contract='custom-import/family-block-layout/v1'
              AND v.dataset_id=f.dataset_id AND v.schema_revision_id=f.schema_revision_id
              AND v.definition_sha256=d.definition_sha256 AND n.nspowner=owner_table.relowner
              AND (f.frozen_at IS NULL OR d.phase='logged')
              AND d.collection_slots=(SELECT coalesce(jsonb_object_agg(c.collection_name,c.collection_slot),'{}'::jsonb)
                  FROM __SCHEMA__.custom_import_child_collection c
                  WHERE c.schema_revision_id=f.schema_revision_id AND c.dataset_id=f.dataset_id)
              AND (obj_description(to_regclass(format('%I.custom_import_block_family',namespace)),'pg_class')::jsonb
                  ->>'layout_sha256')=encode(d.layout_sha256,'hex')
              AND (obj_description(to_regclass(format('%I.custom_import_block_family',namespace)),'pg_class')::jsonb
                  ->>'contract')=d.layout_contract
           ) THEN RAISE EXCEPTION 'custom_import_family_block_layout_mismatch'; END IF;
        IF (SELECT count(*) FROM __SCHEMA__.custom_import_family_block_relation WHERE family_id=f.family_id) <> 14
           OR EXISTS (
            SELECT 1 FROM (VALUES __ROLE_VALUES__) expected(slot,role,name)
            LEFT JOIN __SCHEMA__.custom_import_family_block_relation r
                ON r.family_id=f.family_id AND r.role_slot=expected.slot
            LEFT JOIN pg_class c ON c.oid=r.table_oid::oid
            LEFT JOIN pg_namespace n ON n.oid=c.relnamespace
            JOIN __SCHEMA__.custom_import_family_block_layout d ON d.family_id=f.family_id
            WHERE r.role_name IS DISTINCT FROM expected.role OR c.oid IS NULL OR c.relkind<>'r'
              OR c.relowner::bigint IS DISTINCT FROM r.table_owner OR c.relowner IS DISTINCT FROM n.nspowner
              OR n.nspname IS DISTINCT FROM namespace OR c.relname IS DISTINCT FROM expected.name
              OR c.relpersistence NOT IN ('p','u') OR (d.phase='logged' AND c.relpersistence<>'p')
              OR (d.phase IN ('loading','closed') AND c.relpersistence<>'u')
              OR __SCHEMA__.custom_import_snapshot_columns_sha256(r.table_oid) IS DISTINCT FROM r.columns_sha256
              OR r.constraints_sha256 IS DISTINCT FROM (
                  SELECT sha256(convert_to(coalesce(string_agg(ROW(k.contype,pg_get_constraintdef(k.oid))::text,
                       E'\n' ORDER BY k.contype,pg_get_constraintdef(k.oid)),''),'UTF8'))
                  FROM pg_constraint k WHERE k.conrelid=c.oid)
              OR EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
              OR EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid=c.oid AND contype='f')
              OR EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal)
              OR EXISTS (SELECT 1 FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) acl
                         WHERE acl.grantee<>c.relowner AND (acl.grantee=0 OR acl.privilege_type<>'SELECT' OR acl.is_grantable))
              OR EXISTS (SELECT 1 FROM pg_attribute a,LATERAL aclexplode(a.attacl) acl
                         WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped AND acl.grantee<>c.relowner)
           ) OR EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
               WHERE n.nspname=namespace AND c.relkind IN ('r','p','v','m','f','S')
                 AND NOT EXISTS (SELECT 1 FROM __SCHEMA__.custom_import_family_block_relation r
                                 WHERE r.family_id=f.family_id AND r.table_oid=c.oid::bigint))
           OR EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=namespace)
           THEN RAISE EXCEPTION 'custom_import_family_block_relation_mismatch'; END IF;
        RETURN QUERY SELECT r.role_slot,r.table_oid FROM __SCHEMA__.custom_import_family_block_relation r
            WHERE r.family_id=f.family_id ORDER BY r.role_slot;
        RETURN;
    END IF;
"""
_LOCK = """    SELECT string_agg(format('%I.%I','ci_snapshot_' || f.family_id::text,name),',' ORDER BY ordinal)
        INTO lock_targets FROM unnest(names) WITH ORDINALITY AS relations(name,ordinal);"""
_LOCK_NEW = """    IF EXISTS (SELECT 1 FROM __SCHEMA__.custom_import_family_block_layout WHERE family_id=f.family_id) THEN
        SELECT string_agg(format('%I.%I',n.nspname,c.relname),',' ORDER BY r.role_slot) INTO lock_targets
        FROM __SCHEMA__.custom_import_family_block_relation r JOIN pg_class c ON c.oid=r.table_oid::oid
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE r.family_id=f.family_id;
    ELSE
__OLD_LOCK__
    END IF;""".replace("__OLD_LOCK__", _LOCK)


def _previous(name):
    path = Path(__file__).with_name(name + ".py")
    spec = importlib.util.spec_from_file_location("family_blocks_" + name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _schema():
    return _previous("20261005030000_custom_import_snapshot_storage")._schema()


def _replace(source, old, new):
    if source.count(old) != 1:
        raise RuntimeError("custom_import_family_block_guard_source_mismatch")
    return source.replace(old, new, 1)


def _definitions(schema, *, retirement=False):
    unlogged = _previous("20261104010000_custom_import_unlogged_landing")
    storage = unlogged._continuation()._bulk()._storage()
    render = lambda value: storage._sql(schema, storage._body(schema, value))
    _, _, _, definition, delimiter, _ = list(unlogged._definitions(schema))[1]
    old = definition.split(delimiter)[1]
    catalog_values = ",".join(
        f"({slot},{storage._literal(role)},{storage._literal(name)})" for slot, role, name in _ROLES
    )
    arm = render(_RESOLVER.replace("__ROLE_VALUES__", catalog_values))
    marker = "    namespace := 'ci_snapshot_' || f.family_id::text;\n"
    yield "resolve_custom_import_snapshot_relations", "bigint", old, _replace(old, marker, marker + arm)
    read = " " + render(storage._READ_BINDING_BODY) + " "
    if retirement:
        read = next(
            catalog_row[3]
            for catalog_row in _previous("20261102010000_custom_import_snapshot_retirement")._definitions(schema)
            if catalog_row[0] == "resolve_custom_import_generation_snapshot"
        )
    for name, args, catalog_source in (
        ("resolve_custom_import_generation_snapshot", "bigint,bigint,bigint,bigint", read),
        (
            "lock_custom_import_writable_snapshot",
            "bigint,bigint,bytea",
            " " + render(storage._WRITE_BINDING_BODY) + " ",
        ),
        (
            "lock_custom_import_snapshot_finality",
            "bigint,bigint,bigint,bigint,bigint,bigint,bigint,bytea",
            " " + render(storage._FINALITY_BINDING_BODY) + " ",
        ),
        ("freeze_custom_import_snapshot_family", "bigint,bigint,bytea", " " + render(storage._FREEZE_BODY) + " "),
    ):
        changed = _replace(catalog_source, render(_LOCK), render(_LOCK_NEW))
        if name in {"lock_custom_import_writable_snapshot", "freeze_custom_import_snapshot_family"}:
            phase = "loading" if name == "lock_custom_import_writable_snapshot" else "logged"
            guard = render(
                "\n    IF EXISTS (SELECT 1 FROM __SCHEMA__.custom_import_family_block_layout "
                f"WHERE family_id=f.family_id AND phase<>'{phase}') THEN\n"
                "        RAISE EXCEPTION 'custom_import_family_block_phase_mismatch'; END IF;\n"
            )
            changed = _replace(changed, render(_LOCK_NEW), guard + render(_LOCK_NEW))
        yield name, args, catalog_source, changed


def upgrade():
    """Install the isolated family roles and their existing authority checks."""
    continuation = _previous("20261019010000_custom_import_build_continuation")
    storage = continuation._bulk()._storage()
    schema = _schema()
    quoted = storage._quote(schema)
    op.execute(f"LOCK TABLE {quoted}.custom_import_snapshot_family IN SHARE ROW EXCLUSIVE MODE")
    owner = (
        op.get_bind()
        .execute(
            text(
                "SELECT pg_catalog.pg_get_userbyid(relowner) FROM pg_catalog.pg_class "
                "WHERE oid=pg_catalog.to_regclass(:name)"
            ),
            {"name": f"{quoted}.custom_import_snapshot_family"},
        )
        .scalar_one()
    )
    storage.op = op
    for statement, name in zip(
        _DDL, ("custom_import_family_block_layout", "custom_import_family_block_relation"), strict=True
    ):
        op.execute(storage._sql(schema, statement))
        identity = f"{quoted}.{name}"
        op.execute(f"ALTER TABLE {identity} OWNER TO {storage._quote(owner)}")
        op.execute(f"REVOKE ALL ON TABLE {identity} FROM PUBLIC")
        storage._revoke_defaults("TABLE", identity)
    continuation.op = op
    continuation._schema = lambda: schema
    # Admit only these two exact previous bodies and preserve any retirement guard.
    # A later retirement migration must compose this new branch before installation.
    old_read = (
        op.get_bind()
        .execute(
            text("SELECT prosrc FROM pg_proc WHERE oid=to_regprocedure(:identity)"),
            {"identity": f"{quoted}.resolve_custom_import_generation_snapshot(bigint,bigint,bigint,bigint)"},
        )
        .scalar_one()
    )
    retired_read = list(_definitions(schema, retirement=True))[1][2]
    for name, args, old, new in _definitions(schema, retirement=old_read == retired_read):
        continuation._refresh(f"{quoted}.{name}({args})", old, new)


def downgrade():
    """Require a forward migration for the family-block storage contract."""
    raise RuntimeError("custom_import_family_blocks_require_forward_migration")
