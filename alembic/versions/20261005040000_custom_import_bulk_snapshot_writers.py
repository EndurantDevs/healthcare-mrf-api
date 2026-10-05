# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Install fixed, protected bulk writers for isolated import snapshots."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

from alembic import op

revision = "20261005040000_custom_import_bulk_snapshot_writers"
down_revision = "20261005030000_custom_import_snapshot_storage"
branch_labels = None
depends_on = None

_RESOURCE_DIRECTORY = Path(__file__).resolve().parents[1] / "sql/custom_import_bulk_snapshot_writers"
_BOUNDARY = "\n-- statement boundary --\n"
_ROOT_RESULT = (
    "TABLE(root_record_id bigint,family_revision_id bigint,root_revision_id bigint,entity_binding_id bigint,"
    "attached_child_count bigint,last_child_collection_slot smallint,last_child_key_sha256 bytea,"
    "last_input_child_revision_id bigint,complete boolean)"
)
_CHILD_RESULT = (
    "TABLE(root_record_id bigint,attached_child_count bigint,complete boolean,"
    "last_child_collection_slot smallint,last_child_key_sha256 bytea,last_input_child_revision_id bigint)"
)
_GRAPH_AUTHORITY = "p_build_id bigint,p_execution_id bigint,p_fence bigint,p_token_sha256 bytea,"
_SCALARS = (
    "p_field_slots smallint[],p_field_types text[],p_value_states text[],p_string_values text[],"
    "p_integer_values bigint[],p_decimal_values numeric[],p_boolean_values boolean[],p_date_values date[],"
    "p_timestamp_values timestamptz[],"
)
_WRITERS = (
    (
        "source_bulk_authorize",
        "p_build bigint,p_stream smallint,p_fence bigint,p_token bytea,p_count integer,p_bytes bigint",
        "uuid",
    ),
    ("source_set_finalize", "p_batch uuid,p_verified_parts integer[]", "integer"),
    (
        "finish_custom_import_build_source_part",
        "p_build_id bigint,p_stream_slot smallint,p_part_ordinal integer",
        "integer",
    ),
    ("freeze_custom_import_build_source", "p_build_id bigint", "text"),
    (
        "admit_custom_import_build_page",
        "p_build_id bigint,p_expected_after_id bigint",
        "TABLE(phase text,after_occurrence_id bigint,rows_processed integer,candidate_error_count bigint)",
    ),
    (
        "start_custom_import_build_source_roots_page",
        _GRAPH_AUTHORITY
        + "p_root_ids bigint[],p_occurrence_ids bigint[],p_revision_ids bigint[],p_payload_hashes bytea[],"
        "p_family_hashes bytea[],p_child_counts bigint[],p_entity_values text[],p_entity_hashes bytea[],"
        "p_scalar_root_ids bigint[],"
        + _SCALARS
        + "p_context_root_ids bigint[],p_profile_slots smallint[],p_context_keys text[],p_page_sizes integer[]",
        _ROOT_RESULT,
    ),
    (
        "copy_custom_import_build_retained_roots_page",
        _GRAPH_AUTHORITY
        + "p_root_ids bigint[],p_base_family_ids bigint[],p_base_root_ids bigint[],p_entity_ids bigint[],"
        "p_family_hashes bytea[],p_child_counts bigint[],p_context_root_ids bigint[],"
        "p_profile_slots smallint[],p_context_keys text[],p_page_sizes integer[]",
        _ROOT_RESULT,
    ),
    (
        "append_custom_import_build_source_families_page",
        _GRAPH_AUTHORITY
        + "p_root_ids bigint[],p_family_ids bigint[],p_expected_counts bigint[],p_after_slots smallint[],"
        "p_after_hashes bytea[],p_after_ids bigint[],p_child_root_ids bigint[],p_child_ids bigint[],"
        "p_scalar_child_ids bigint[],"
        + _SCALARS
        + "p_context_child_ids bigint[],p_profile_slots smallint[],p_context_keys text[],p_page_sizes integer[]",
        _CHILD_RESULT,
    ),
    (
        "copy_custom_import_build_retained_families_page",
        _GRAPH_AUTHORITY
        + "p_root_ids bigint[],p_family_ids bigint[],p_expected_counts bigint[],p_after_slots smallint[],"
        "p_after_ids bigint[],p_child_root_ids bigint[],p_child_ids bigint[],p_context_child_ids bigint[],"
        "p_profile_slots smallint[],p_context_keys text[],p_page_sizes integer[]",
        _CHILD_RESULT,
    ),
    (
        "plan_custom_import_build_family_page",
        "p_build_id bigint,p_expected_page_sequence bigint",
        "TABLE(phase text,plan_stage text,page_sequence bigint,rows_processed integer,plan_complete boolean)",
    ),
    (
        "membership_batch_finalize",
        "p_build_id bigint,p_after_root_record_id bigint",
        "TABLE(after_root_record_id bigint,rows_processed integer,generation_family_count bigint,inserted_count integer)",
    ),
    (
        "winner_batch_finalize",
        "p_build_id bigint,p_candidate_context_ids bigint[],p_after_profile_slot smallint,"
        "p_after_entity_binding_id bigint,p_after_context_key_sha256 bytea,p_page_sizes integer[]",
        "TABLE(output_after_profile_slot smallint,output_after_entity_binding_id bigint,"
        "output_after_context_key_sha256 bytea,winner_count bigint,inserted_count integer)",
    ),
    ("open_custom_import_build_output", "p_build_id bigint,p_generation_id bigint", "text"),
    ("freeze_custom_import_build_output", "p_build_id bigint", "text"),
    (
        "check_custom_import_source_replay_homes",
        "p_build_id bigint,p_stream_slot smallint,p_part_ordinal integer,p_first_row bigint,p_count integer",
        "integer",
    ),
)
_EXISTING = frozenset(
    (
        "finish_custom_import_build_source_part",
        "freeze_custom_import_build_source",
        "admit_custom_import_build_page",
        "plan_custom_import_build_family_page",
        "open_custom_import_build_output",
        "freeze_custom_import_build_output",
    )
)
_COPY_COLUMNS = (
    "batch_id,landing_ordinal,pack_ordinal,pack_sha256,part_ordinal,part_row_ordinal,source_ordinal,"
    "raw_key,raw_hash,typed_key,typed_hash,payload,payload_hash,child_key,child_hash,"
    "rejection_code,rejection_key,rejection_hash,rejection_evidence"
)
_CLOSE_SOURCE_LANDING_INSERT = f"""
        IF NOT EXISTS(SELECT 1 FROM __CONTROL__.source_bulk_authorization a
            WHERE a.build_id=(SELECT closed.build_id FROM __CONTROL__.source_bulk_authorization closed
                WHERE closed.batch_id=p_batch) AND a.opened_by=session_user
                AND a.transaction_id=pg_current_xact_id() AND a.accepting) THEN
            EXECUTE format('REVOKE INSERT ({_COPY_COLUMNS}) ON TABLE %I.source_bulk_landing FROM %I',namespace,session_user);
        END IF;
        """

_SEALED_BASE_BODY = r"""
DECLARE g __CONTROL__.custom_import_generation; result bigint; relation_name text;
BEGIN
    IF p_generation_id IS NULL THEN RETURN NULL; END IF;
    SELECT * INTO g FROM __CONTROL__.custom_import_generation WHERE generation_id=p_generation_id;
    IF g.generation_id IS NULL OR g.dataset_id IS DISTINCT FROM p_dataset_id THEN
        RAISE EXCEPTION 'custom_import_snapshot_base_mismatch'; END IF;
    result:=__CONTROL__.resolve_custom_import_generation_snapshot(
        g.generation_id,g.dataset_id,g.definition_revision_id,g.schema_revision_id);
    IF result IS NULL THEN
        FOREACH relation_name IN ARRAY __RELATION_NAMES__ LOOP
            EXECUTE format('LOCK TABLE %I.%I IN ACCESS SHARE MODE',__CONTROL_LITERAL__,relation_name);
        END LOOP;
    END IF;
    RETURN result;
END;
"""

_BUILD_BASE_BODY = r"""
DECLARE b __CONTROL__.custom_import_build_attempt;
BEGIN
    b:=__CONTROL__.lock_custom_import_build(p_build_id);
    RETURN __CONTROL__.lock_custom_import_sealed_snapshot_base(b.base_generation_id,b.dataset_id);
END;
"""

_VERIFY_WRITERS_BODY = r"""
DECLARE f __CONTROL__.custom_import_snapshot_family; namespace text; owner_oid oid;
BEGIN
    SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family WHERE family_id=p_family_id;
    SELECT oid INTO owner_oid FROM pg_roles WHERE rolname=current_user;
    namespace:='ci_snapshot_'||f.family_id::text;
    IF f.family_id IS NULL OR f.landing_table_owner IS DISTINCT FROM owner_oid::bigint
        OR NOT EXISTS(SELECT 1 FROM pg_namespace n WHERE n.nspname=namespace AND n.nspowner=owner_oid)
        OR EXISTS(SELECT 1 FROM pg_namespace n,
            LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a
            WHERE n.nspname=namespace AND a.grantee<>owner_oid AND a.privilege_type='CREATE') THEN
        RAISE EXCEPTION 'custom_import_snapshot_writer_owner_mismatch'; END IF;
    PERFORM __CONTROL__.resolve_custom_import_snapshot_relations(f.family_id);
    IF EXISTS(SELECT 1 FROM unnest(__SIGNATURES__,__RESULTS__) expected(signature,result)
        LEFT JOIN pg_proc p ON p.oid=to_regprocedure(format('%I.%s',namespace,expected.signature))
        WHERE p.oid IS NULL OR p.proowner<>owner_oid OR p.prokind<>'f' OR NOT p.prosecdef
            OR p.proconfig IS DISTINCT FROM ARRAY['search_path=pg_catalog']::text[]
            OR regexp_replace(pg_get_function_result(p.oid),'\s','','g')<>
                regexp_replace(expected.result,'\s','','g')
            OR EXISTS(SELECT 1 FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
                WHERE a.grantee<>p.proowner)) THEN
        RAISE EXCEPTION 'custom_import_snapshot_writer_signature_mismatch'; END IF;
END;
"""

_INSTALL_BODY = r"""
DECLARE f __CONTROL__.custom_import_snapshot_family; e __CONTROL__.custom_import_execution;
    b __CONTROL__.custom_import_build_attempt; g __CONTROL__.custom_import_generation;
    namespace text; base_namespace text; base_generation bigint; base_family bigint;
    statement text; function_identity text; grantee record; relation_oid oid; owner_oid oid;
BEGIN
    SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family WHERE family_id=p_family_id;
    IF f.family_id IS NULL THEN RAISE EXCEPTION 'custom_import_snapshot_writer_binding_mismatch'; END IF;
    e:=__CONTROL__.lock_custom_import_snapshot_attempt(f.execution_id,f.producing_fence,f.producing_token_sha256);
    IF p_family_id IS DISTINCT FROM __CONTROL__.lock_custom_import_writable_snapshot(
        e.execution_id,f.producing_fence,f.producing_token_sha256) THEN
        RAISE EXCEPTION 'custom_import_snapshot_writer_binding_mismatch'; END IF;
    SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family WHERE family_id=p_family_id FOR UPDATE;
    IF f.landing_table_oid IS NOT NULL THEN
        PERFORM __CONTROL__.verify_custom_import_snapshot_writers(f.family_id);
        RETURN f.family_id;
    END IF;
    SELECT * INTO b FROM __CONTROL__.custom_import_build_attempt
        WHERE execution_id=f.execution_id AND producing_fence=f.producing_fence;
    IF b.build_id IS NOT NULL THEN
        b:=__CONTROL__.lock_custom_import_build(b.build_id);
        IF ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,b.execution_id,
            b.capture_bundle_id,b.producing_fence,b.producing_token_sha256)
            IS DISTINCT FROM ROW(f.dataset_id,f.definition_revision_id,f.schema_revision_id,f.execution_id,
                f.capture_bundle_id,f.producing_fence,f.producing_token_sha256) THEN
            RAISE EXCEPTION 'custom_import_snapshot_writer_binding_mismatch'; END IF;
        base_generation:=b.base_generation_id;
    ELSE
        SELECT * INTO g FROM __CONTROL__.custom_import_generation WHERE generation_id=f.generation_id;
        IF g.generation_id IS NULL OR ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id,
            g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256)
            IS DISTINCT FROM ROW(f.dataset_id,f.definition_revision_id,f.schema_revision_id,f.execution_id,
                f.capture_bundle_id,f.producing_fence,f.producing_token_sha256) THEN
            RAISE EXCEPTION 'custom_import_snapshot_writer_generation_required'; END IF;
        base_generation:=g.base_generation_id;
    END IF;
    base_family:=__CONTROL__.lock_custom_import_sealed_snapshot_base(base_generation,f.dataset_id);
    IF base_family=f.family_id THEN RAISE EXCEPTION 'custom_import_snapshot_base_mismatch'; END IF;
    namespace:='ci_snapshot_'||f.family_id::text;
    base_namespace:=CASE WHEN base_family IS NULL THEN __CONTROL_LITERAL__ ELSE 'ci_snapshot_'||base_family::text END;
    FOREACH statement IN ARRAY __LEAF_DDL__ LOOP
        EXECUTE replace(replace(replace(statement,'__CANDIDATE__',quote_ident(namespace)),
            '__BASE__',quote_ident(base_namespace)),'__FAMILY_ID__',f.family_id::text||'::bigint');
    END LOOP;
    SELECT c.oid,c.relowner INTO relation_oid,owner_oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=namespace AND c.relname='source_bulk_landing' AND c.relkind='r' AND c.relpersistence='p';
    IF relation_oid IS NULL OR owner_oid IS DISTINCT FROM (SELECT oid FROM pg_roles WHERE rolname=current_user) THEN
        RAISE EXCEPTION 'custom_import_snapshot_writer_owner_mismatch'; END IF;
    EXECUTE format('REVOKE ALL ON TABLE %I.source_bulk_landing FROM PUBLIC',namespace);
    FOR grantee IN SELECT DISTINCT a.grantee FROM pg_class c,
        LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
        WHERE c.oid=relation_oid AND a.grantee<>c.relowner AND a.grantee<>0
    LOOP
        EXECUTE format('REVOKE ALL ON TABLE %I.source_bulk_landing FROM %I',namespace,pg_get_userbyid(grantee.grantee));
    END LOOP;
    FOREACH function_identity IN ARRAY __SIGNATURES__ LOOP
        function_identity:=format('%I.%s',namespace,function_identity);
        EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC',function_identity);
        FOR grantee IN SELECT DISTINCT a.grantee FROM pg_proc p,
            LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
            WHERE p.oid=to_regprocedure(function_identity) AND a.grantee<>p.proowner AND a.grantee<>0
        LOOP
            EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I',function_identity,pg_get_userbyid(grantee.grantee));
        END LOOP;
    END LOOP;
    UPDATE __CONTROL__.custom_import_snapshot_family SET landing_table_oid=relation_oid::bigint,
        landing_table_owner=owner_oid::bigint,
        landing_columns_sha256=__CONTROL__.custom_import_snapshot_columns_sha256(relation_oid::bigint)
        WHERE family_id=f.family_id AND landing_table_oid IS NULL AND frozen_at IS NULL;
    IF NOT FOUND THEN RAISE EXCEPTION 'custom_import_snapshot_writer_binding_mismatch'; END IF;
    PERFORM __CONTROL__.verify_custom_import_snapshot_writers(f.family_id);
    PERFORM __CONTROL__.lock_custom_import_writable_snapshot(f.execution_id,f.producing_fence,f.producing_token_sha256);
    RETURN f.family_id;
END;
"""

_BUILD_SNAPSHOT_BODY = r"""
DECLARE b __CONTROL__.custom_import_build_attempt; f __CONTROL__.custom_import_snapshot_family; result bigint;
BEGIN
    b:=__CONTROL__.lock_custom_import_build(p_build_id);
    SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family
        WHERE execution_id=b.execution_id AND producing_fence=b.producing_fence;
    IF f.family_id IS NULL THEN
        IF b.phase<>'source' OR b.generation_id IS NOT NULL
            OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_build_stream
                WHERE build_id=b.build_id AND next_source_ordinal<>0)
            OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_pack WHERE execution_id=b.execution_id)
            OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_rejection WHERE execution_id=b.execution_id) THEN
            RAISE EXCEPTION 'custom_import_snapshot_existing_build_requires_migration'; END IF;
        result:=__CONTROL__.create_custom_import_snapshot_family(
            b.execution_id,b.producing_fence,b.producing_token_sha256);
        SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family WHERE family_id=result;
    END IF;
    IF f.frozen_at IS NULL THEN
        result:=__CONTROL__.install_custom_import_snapshot_writers(f.family_id);
        result:=__CONTROL__.lock_custom_import_writable_snapshot(
            b.execution_id,b.producing_fence,b.producing_token_sha256);
    ELSE
        result:=__CONTROL__.lock_custom_import_snapshot_finality(b.generation_id,b.dataset_id,
            b.definition_revision_id,b.schema_revision_id,b.execution_id,b.capture_bundle_id,
            b.producing_fence,b.producing_token_sha256);
        PERFORM __CONTROL__.verify_custom_import_snapshot_writers(result);
    END IF;
    IF result IS NULL OR result<=0 OR result IS DISTINCT FROM f.family_id THEN
        RAISE EXCEPTION 'custom_import_snapshot_writer_binding_mismatch'; END IF;
    PERFORM __CONTROL__.lock_custom_import_build(p_build_id);
    RETURN result;
END;
"""


def _previous(name: str):
    path = Path(__file__).with_name(name + ".py")
    spec = importlib.util.spec_from_file_location("bulk_writers_" + name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = op
    return module


def _storage():
    return _previous("20261005030000_custom_import_snapshot_storage")


def _schema() -> str:
    return _storage()._schema()


def _resource(name: str) -> tuple[str, ...]:
    """The migration reads only the three fixed, packaged SQL resources."""
    if name not in ("source_control.sql", "snapshot_writers.sql", "source_batch_snapshot.sql"):
        raise ValueError("unknown bulk writer resource")
    return tuple(block.strip() for block in (_RESOURCE_DIRECTORY / name).read_text().split(_BOUNDARY))


def _control_sql(storage, schema: str, statement: str) -> str:
    """Quote both identifiers and the fixed regclass/regprocedure literals."""
    statement = re.sub(
        r"'__CONTROL__\.([^']*)'",
        lambda match: storage._literal(storage._quote(schema) + "." + match[1]),
        statement,
    )
    return statement.replace("__CONTROL__", storage._quote(schema))


def _argument_types(arguments: str) -> str:
    return ",".join(argument.strip().split(" ", 1)[1] for argument in arguments.split(","))


def _array(storage, values) -> str:
    return "ARRAY[" + ",".join(storage._literal(value) for value in values) + "]::text[]"


def _body(storage, schema: str, source: str) -> str:
    signatures = (f"{name}({_argument_types(arguments)})" for name, arguments, _ in _WRITERS)
    source = source.replace("__CONTROL_LITERAL__", storage._literal(schema))
    source = source.replace("__RELATION_NAMES__", _array(storage, storage._RELATION_NAMES))
    source = source.replace("__SIGNATURES__", _array(storage, signatures))
    source = source.replace("__RESULTS__", _array(storage, (result for _, _, result in _WRITERS)))
    if "__LEAF_DDL__" in source:
        statements = (_control_sql(storage, schema, sql) for sql in _resource("snapshot_writers.sql"))
        # Render control names before quoting the complete fixed DDL literals.
        source = _control_sql(storage, schema, source).replace("__LEAF_DDL__", _array(storage, statements))
        return source
    return _control_sql(storage, schema, source)


def _function(storage, schema: str, name: str, arguments: str, result: str, body: str) -> None:
    identity = f"{storage._quote(schema)}.{name}({_argument_types(arguments)})"
    creation = "CREATE OR REPLACE" if name in _EXISTING else "CREATE"
    op.execute(
        f"{creation} FUNCTION {storage._quote(schema)}.{name}({arguments}) RETURNS {result}\n"
        "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog\n"
        f"AS $bulk_snapshot$ {_body(storage, schema, body)} $bulk_snapshot$"
    )
    op.execute(f"REVOKE ALL ON FUNCTION {identity} FROM PUBLIC")
    if name not in _EXISTING:
        storage._revoke_defaults("FUNCTION", identity)


def _dispatcher(name: str, arguments: str, return_sql: str) -> str:
    """Render a fixed writer with transaction-bound transport and finality checks."""
    argument_names = [argument.strip().split(" ", 1)[0] for argument in arguments.split(",")]
    placeholders = ",".join(f"${position}" for position in range(1, len(argument_names) + 1))
    call = f"EXECUTE format('SELECT * FROM %I.{name}({placeholders})',namespace)"
    using = ",".join(argument_names)
    if name == "source_set_finalize":
        binding = """
        family_id:=__CONTROL__.resolve_custom_import_source_batch_snapshot(p_batch);
        SELECT a.build_id INTO build_id FROM __CONTROL__.source_bulk_authorization a WHERE a.batch_id=p_batch;
        PERFORM __CONTROL__.verify_custom_import_snapshot_writers(family_id);
        """
    else:
        binding = f"""
        build_id:={argument_names[0]};
        family_id:=__CONTROL__.resolve_custom_import_build_snapshot(build_id);
        """
    before_return = ""
    if name == "source_bulk_authorize":
        before_return = f"""
        IF family_id IS DISTINCT FROM __CONTROL__.resolve_custom_import_source_batch_snapshot(answer) THEN
            RAISE EXCEPTION 'custom_import_snapshot_writer_binding_mismatch'; END IF;
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I',namespace,session_user);
        EXECUTE format('GRANT INSERT ({_COPY_COLUMNS}) ON TABLE %I.source_bulk_landing TO %I',namespace,session_user);
        """
    elif name == "source_set_finalize":
        before_return = _CLOSE_SOURCE_LANDING_INSERT
    elif name == "open_custom_import_build_output":
        before_return = """
        b:=__CONTROL__.lock_custom_import_build(build_id);
        IF family_id IS DISTINCT FROM __CONTROL__.bind_custom_import_snapshot_generation(
            b.execution_id,b.producing_fence,b.producing_token_sha256,p_generation_id) THEN
            RAISE EXCEPTION 'custom_import_snapshot_writer_binding_mismatch'; END IF;
        """
    elif name == "freeze_custom_import_build_output":
        before_return = """
        b:=__CONTROL__.lock_custom_import_build(build_id);
        IF family_id IS DISTINCT FROM __CONTROL__.freeze_custom_import_snapshot_family(
            b.execution_id,b.producing_fence,b.producing_token_sha256) THEN
            RAISE EXCEPTION 'custom_import_snapshot_writer_binding_mismatch'; END IF;
        """
    is_scalar = not return_sql.startswith("TABLE(")
    invocation = f"{call} INTO answer USING {using};" if is_scalar else f"RETURN QUERY {call} USING {using};"
    return f"""
    DECLARE family_id bigint; build_id bigint; namespace text;
        b __CONTROL__.custom_import_build_attempt; {f"answer {return_sql};" if is_scalar else ""}
    BEGIN
        {binding}
        PERFORM __CONTROL__.resolve_custom_import_build_base_snapshot(build_id);
        namespace:='ci_snapshot_'||family_id::text;
        {invocation}
        {before_return}
        PERFORM __CONTROL__.lock_custom_import_build(build_id);
        {"RETURN answer;" if is_scalar else "RETURN;"}
    END;
    """


def upgrade() -> None:
    """Install definitions only; candidate creation requires real live authority."""
    storage = _storage()
    schema = _schema()
    for statement in _resource("source_control.sql"):
        op.execute(_control_sql(storage, schema, statement))
    for kind, suffix in (
        ("TABLE", "source_bulk_completion"),
        ("TABLE", "source_bulk_authorization"),
        ("FUNCTION", "source_bulk_canonical(jsonb)"),
        ("FUNCTION", "source_bulk_digest(text,text)"),
    ):
        identity = f"{storage._quote(schema)}.{suffix}"
        op.execute(f"REVOKE ALL ON {kind} {identity} FROM PUBLIC")
        storage._revoke_defaults(kind, identity)
    functions = (
        (
            "lock_custom_import_sealed_snapshot_base",
            "p_generation_id bigint,p_dataset_id bigint",
            "bigint",
            _SEALED_BASE_BODY,
        ),
        ("resolve_custom_import_build_base_snapshot", "p_build_id bigint", "bigint", _BUILD_BASE_BODY),
        ("verify_custom_import_snapshot_writers", "p_family_id bigint", "void", _VERIFY_WRITERS_BODY),
        ("install_custom_import_snapshot_writers", "p_family_id bigint", "bigint", _INSTALL_BODY),
        ("resolve_custom_import_build_snapshot", "p_build_id bigint", "bigint", _BUILD_SNAPSHOT_BODY),
    )
    for name, arguments, return_sql, body in functions:
        _function(storage, schema, name, arguments, return_sql, body)
    op.execute(_control_sql(storage, schema, _resource("source_batch_snapshot.sql")[0]))
    batch_identity = f"{storage._quote(schema)}.resolve_custom_import_source_batch_snapshot(uuid)"
    op.execute(f"REVOKE ALL ON FUNCTION {batch_identity} FROM PUBLIC")
    storage._revoke_defaults("FUNCTION", batch_identity)
    for name, arguments, return_sql in _WRITERS:
        _function(storage, schema, name, arguments, return_sql, _dispatcher(name, arguments, return_sql))


def downgrade() -> None:
    """Restore old dispatch bodies only if no installed snapshot or batch survives."""
    storage = _storage()
    schema = _schema()
    op.execute(
        _control_sql(
            storage,
            schema,
            """
        DO $bulk_snapshot$ BEGIN
            LOCK TABLE __CONTROL__.custom_import_snapshot_family,
                __CONTROL__.source_bulk_authorization,__CONTROL__.source_bulk_completion IN ACCESS EXCLUSIVE MODE;
            IF EXISTS(SELECT 1 FROM __CONTROL__.custom_import_snapshot_family WHERE landing_table_oid IS NOT NULL)
                OR EXISTS(SELECT 1 FROM __CONTROL__.source_bulk_authorization)
                OR EXISTS(SELECT 1 FROM __CONTROL__.source_bulk_completion) THEN
                RAISE EXCEPTION 'custom_import_bulk_snapshot_writers_downgrade_blocked'; END IF;
        END $bulk_snapshot$
    """,
        )
    )
    memberships = _previous("20261002040000_custom_import_child_memberships")
    previous = memberships._previous()
    bounded = previous._legacy()
    body_by_name = {
        "finish_custom_import_build_source_part": bounded._FINISH_CUSTOM_IMPORT_BUILD_SOURCE_PART_BODY,
        "freeze_custom_import_build_source": bounded._FREEZE_CUSTOM_IMPORT_BUILD_SOURCE_BODY,
        "admit_custom_import_build_page": memberships._admission_body(previous, bounded),
        "plan_custom_import_build_family_page": memberships._planner_body(bounded),
        "open_custom_import_build_output": bounded._OPEN_CUSTOM_IMPORT_BUILD_OUTPUT_BODY,
        "freeze_custom_import_build_output": bounded._FREEZE_CUSTOM_IMPORT_BUILD_OUTPUT_BODY,
    }
    for name, arguments, return_sql in _WRITERS:
        if name in _EXISTING:
            _function(
                storage, schema, name, arguments, return_sql, body_by_name[name].replace("__SCHEMA__", "__CONTROL__")
            )
        else:
            op.execute(f"DROP FUNCTION {storage._quote(schema)}.{name}({_argument_types(arguments)})")
    for signature in (
        "resolve_custom_import_source_batch_snapshot(uuid)",
        "resolve_custom_import_build_snapshot(bigint)",
        "install_custom_import_snapshot_writers(bigint)",
        "verify_custom_import_snapshot_writers(bigint)",
        "resolve_custom_import_build_base_snapshot(bigint)",
        "lock_custom_import_sealed_snapshot_base(bigint,bigint)",
        "source_bulk_digest(text,text)",
        "source_bulk_canonical(jsonb)",
    ):
        op.execute(f"DROP FUNCTION {storage._quote(schema)}.{signature}")
    for table in ("source_bulk_authorization", "source_bulk_completion"):
        op.execute(f"DROP TABLE {storage._quote(schema)}.{table}")
