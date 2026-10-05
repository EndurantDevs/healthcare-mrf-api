# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Register protected legacy set writers without creating or publishing snapshots."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op

revision = "20261005050000_custom_import_legacy_snapshot_writers"
down_revision = "20261005040000_custom_import_bulk_snapshot_writers"
branch_labels = None
depends_on = None

_RESOURCE_DIRECTORY = Path(__file__).resolve().parents[1] / "sql/custom_import_legacy_snapshot_writers"
_BOUNDARY = "\n-- statement boundary --\n"
_WRITERS = (
    (
        "persist_custom_import_legacy_identity_set",
        "p_dataset_id bigint,p_definition_revision_id bigint,p_schema_revision_id bigint, p_execution_id bigint,p_capture_bundle_id bigint,p_fence bigint,p_token_sha256 bytea,p_deadline_at timestamptz, p_contracts bytea[],p_keys text[],p_hashes bytea[],p_entities text[],p_entity_hashes bytea[], p_expected_roots bigint[],p_expected_entities bigint[],p_single_key text",
        "bigint[]",
    ),
    (
        "persist_custom_import_legacy_family_root_set",
        "p_dataset_id bigint,p_definition_revision_id bigint,p_schema_revision_id bigint, p_execution_id bigint,p_capture_bundle_id bigint,p_fence bigint,p_token_sha256 bytea,p_deadline_at timestamptz, p_revision_ids bigint[],p_family_ids bigint[],p_root_ids bigint[],p_entity_ids bigint[],p_pack_ids bigint[], p_ordinals bigint[],p_payloads text[],p_payload_hashes bytea[],p_family_hashes bytea[], p_child_counts bigint[],p_base_families bigint[],p_single_payload text",
        "bigint[]",
    ),
    (
        "persist_custom_import_legacy_child_set",
        "p_dataset_id bigint,p_definition_revision_id bigint,p_schema_revision_id bigint, p_execution_id bigint,p_capture_bundle_id bigint,p_fence bigint,p_token_sha256 bytea,p_deadline_at timestamptz, p_revision_ids bigint[],p_family_ids bigint[],p_root_ids bigint[],p_collections smallint[],p_pack_ids bigint[], p_ordinals bigint[],p_parent_keys text[],p_parent_hashes bytea[],p_child_keys text[],p_child_hashes bytea[], p_payloads text[],p_payload_hashes bytea[],p_base_families bigint[],p_base_children bigint[], p_single_parent_key text,p_single_child_key text,p_single_payload text",
        "bigint[]",
    ),
    (
        "persist_custom_import_legacy_pack_set",
        "p_dataset_id bigint, p_definition_revision_id bigint, p_schema_revision_id bigint, p_execution_id bigint, p_capture_bundle_id bigint, p_fence bigint, p_token_sha256 bytea, p_deadline_at timestamptz, p_stream_slots smallint[], p_pack_ordinals integer[], p_record_counts bigint[], p_pack_hashes bytea[]",
        "bigint[]",
    ),
    (
        "persist_custom_import_legacy_rejection_set",
        "p_dataset_id bigint, p_definition_revision_id bigint, p_schema_revision_id bigint, p_execution_id bigint, p_capture_bundle_id bigint, p_fence bigint, p_token_sha256 bytea, p_deadline_at timestamptz, p_ordinals bigint[], p_root_keys text[], p_root_hashes bytea[], p_codes text[], p_evidence text[], p_single_root_key text",
        "integer",
    ),
    (
        "persist_custom_import_legacy_generation_family_set",
        "p_generation_id bigint, p_dataset_id bigint, p_definition_revision_id bigint, p_schema_revision_id bigint, p_execution_id bigint, p_capture_bundle_id bigint, p_fence bigint, p_token bytea, p_root_record_ids bigint[], p_family_revision_ids bigint[], p_expected_authority bigint[], p_expected_token bytea, p_expected_expires timestamptz",
        "bigint",
    ),
)
_AUTHORITY_FUNCTIONS = (
    (
        "lock_custom_import_legacy_generation",
        "p_generation_id bigint",
        "__CONTROL__.custom_import_generation",
    ),
    (
        "lock_custom_import_legacy_generation_snapshot",
        "p_generation_id bigint",
        "bigint",
    ),
    (
        "check_custom_import_materialization_expected",
        "p_expected bigint[], p_token bytea, p_deadline timestamptz",
        "void",
    ),
    (
        "check_custom_import_materialization_authority",
        "p_dataset_id bigint,p_definition_revision_id bigint,p_schema_revision_id bigint, p_execution_id bigint,p_capture_bundle_id bigint,p_fence bigint,p_token_sha256 bytea,p_deadline_at timestamptz",
        "void",
    ),
    (
        "check_custom_import_generation_materialization_authority",
        "p_generation_id bigint, p_dataset_id bigint, p_definition_revision_id bigint, p_schema_revision_id bigint, p_execution_id bigint, p_capture_bundle_id bigint, p_fence bigint, p_token bytea, p_expected_authority bigint[], p_expected_token bytea, p_expected_expires timestamptz",
        "void",
    ),
)

_VERIFY_BODY = r"""
DECLARE f __CONTROL__.custom_import_snapshot_family; namespace text; owner_oid oid;
BEGIN
    PERFORM __CONTROL__.verify_custom_import_snapshot_writers(p_family_id);
    SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family WHERE family_id=p_family_id;
    SELECT oid INTO owner_oid FROM pg_roles WHERE rolname=current_user;
    namespace:='ci_snapshot_'||f.family_id::text;
    IF f.origin_table_oid IS NULL OR f.origin_table_owner IS DISTINCT FROM owner_oid::bigint THEN
        RAISE EXCEPTION 'custom_import_legacy_origin_unregistered'; END IF;
    PERFORM __CONTROL__.resolve_custom_import_snapshot_relations(f.family_id);
    IF EXISTS (SELECT 1 FROM unnest(__SIGNATURES__,__RESULTS__) expected(signature,result)
        LEFT JOIN pg_proc p ON p.oid=to_regprocedure(format('%I.%s',namespace,expected.signature))
        WHERE p.oid IS NULL OR p.proowner<>owner_oid OR p.prokind<>'f' OR NOT p.prosecdef
            OR p.proconfig IS DISTINCT FROM ARRAY['search_path=pg_catalog']::text[]
            OR regexp_replace(pg_get_function_result(p.oid),'\s','','g')<>
                regexp_replace(expected.result,'\s','','g')
            OR EXISTS(SELECT 1 FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
                WHERE a.grantee<>p.proowner)) THEN
        RAISE EXCEPTION 'custom_import_legacy_writer_signature_mismatch'; END IF;
END;
"""

_INSTALL_BODY = r"""
DECLARE f __CONTROL__.custom_import_snapshot_family; g __CONTROL__.custom_import_generation;
    namespace text; base_namespace text; base_family bigint; statement text;
    function_identity text; grantee record; relation_oid oid; owner_oid oid;
BEGIN
    SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family WHERE family_id=p_family_id;
    IF f.generation_id IS NULL OR p_family_id IS DISTINCT FROM
        __CONTROL__.lock_custom_import_legacy_generation_snapshot(f.generation_id) THEN
        RAISE EXCEPTION 'custom_import_legacy_install_binding_mismatch'; END IF;
    PERFORM __CONTROL__.install_custom_import_snapshot_writers(p_family_id);
    SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family WHERE family_id=p_family_id FOR UPDATE;
    IF f.origin_table_oid IS NOT NULL THEN
        PERFORM __CONTROL__.verify_custom_import_legacy_snapshot_writers(p_family_id);
        RETURN p_family_id;
    END IF;
    SELECT * INTO g FROM __CONTROL__.custom_import_generation WHERE generation_id=f.generation_id;
    base_family:=__CONTROL__.lock_custom_import_sealed_snapshot_base(g.base_generation_id,g.dataset_id);
    namespace:='ci_snapshot_'||f.family_id::text;
    base_namespace:=CASE WHEN base_family IS NULL THEN __CONTROL_LITERAL__ ELSE 'ci_snapshot_'||base_family::text END;
    SELECT oid INTO owner_oid FROM pg_roles WHERE rolname=current_user;
    IF EXISTS(SELECT 1 FROM unnest(__SIGNATURES__) signature
        WHERE to_regprocedure(format('%I.%s',namespace,signature)) IS NOT NULL) THEN
        RAISE EXCEPTION 'custom_import_legacy_partial_install'; END IF;
    FOREACH statement IN ARRAY __ORIGIN_DDL__ LOOP
        EXECUTE replace(statement,'__CANDIDATE__',quote_ident(namespace));
    END LOOP;
    relation_oid:=to_regclass(format('%I.legacy_copy_origin',namespace));
    EXECUTE format('REVOKE ALL ON TABLE %s FROM PUBLIC',relation_oid::regclass);
    FOR grantee IN SELECT DISTINCT a.grantee FROM pg_class c,
        LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
        WHERE c.oid=relation_oid AND a.grantee NOT IN (0,owner_oid)
    LOOP
        EXECUTE format('REVOKE ALL ON TABLE %s FROM %I',relation_oid::regclass,pg_get_userbyid(grantee.grantee));
    END LOOP;
    FOREACH statement IN ARRAY __LEAF_DDL__ LOOP
        statement:=replace(replace(replace(statement,'__CANDIDATE__',quote_ident(namespace)),
            '__BASE__',quote_ident(base_namespace)),'__FAMILY_ID__',f.family_id::text);
        EXECUTE statement;
    END LOOP;
    FOREACH function_identity IN ARRAY __SIGNATURES__ LOOP
        function_identity:=format('%I.%s',namespace,function_identity);
        EXECUTE 'REVOKE ALL ON FUNCTION '||function_identity||' FROM PUBLIC';
        FOR grantee IN SELECT DISTINCT a.grantee FROM pg_proc p,
            LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
            WHERE p.oid=to_regprocedure(function_identity) AND a.grantee NOT IN (0,owner_oid)
        LOOP
            EXECUTE 'REVOKE ALL ON FUNCTION '||function_identity||' FROM '||quote_ident(pg_get_userbyid(grantee.grantee));
        END LOOP;
    END LOOP;
    UPDATE __CONTROL__.custom_import_snapshot_family SET origin_table_oid=relation_oid::bigint,
        origin_table_owner=owner_oid::bigint,
        origin_columns_sha256=__CONTROL__.custom_import_snapshot_columns_sha256(relation_oid::bigint)
    WHERE family_id=f.family_id AND origin_table_oid IS NULL AND frozen_at IS NULL;
    IF NOT FOUND THEN RAISE EXCEPTION 'custom_import_legacy_install_binding_mismatch'; END IF;
    PERFORM __CONTROL__.verify_custom_import_legacy_snapshot_writers(p_family_id);
    IF p_family_id IS DISTINCT FROM __CONTROL__.lock_custom_import_legacy_generation_snapshot(g.generation_id) THEN
        RAISE EXCEPTION 'custom_import_legacy_install_binding_mismatch'; END IF;
    RETURN p_family_id;
END;
"""

_RESOLVE_BODY = r"""
DECLARE g __CONTROL__.custom_import_generation; f __CONTROL__.custom_import_snapshot_family; snapshot_id bigint;
BEGIN
    g:=__CONTROL__.lock_custom_import_legacy_generation(p_generation_id);
    IF (SELECT count(*) FROM __CONTROL__.custom_import_snapshot_family
        WHERE generation_id=g.generation_id OR (execution_id=g.execution_id AND producing_fence=g.producing_fence))>1 THEN
        RAISE EXCEPTION 'custom_import_legacy_snapshot_binding_mismatch'; END IF;
    SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family
        WHERE generation_id=g.generation_id OR (execution_id=g.execution_id AND producing_fence=g.producing_fence)
        FOR UPDATE;
    IF f.family_id IS NOT NULL AND (f.frozen_at IS NOT NULL OR
        ROW(f.dataset_id,f.definition_revision_id,f.schema_revision_id,f.execution_id,f.capture_bundle_id,
            f.producing_fence,f.producing_token_sha256)
        IS DISTINCT FROM ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id,g.execution_id,g.capture_bundle_id,
            g.producing_fence,g.producing_token_sha256)
        OR (f.generation_id IS NOT NULL AND f.generation_id<>g.generation_id)) THEN
        RAISE EXCEPTION 'custom_import_legacy_snapshot_binding_mismatch'; END IF;
    -- Do not split one producer between canonical and candidate hot storage.
    -- Global identity interners are intentionally excluded.
    IF EXISTS(SELECT 1 FROM __CONTROL__.custom_import_pack
        WHERE execution_id=g.execution_id AND producing_fence=g.producing_fence)
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_rejection
            WHERE execution_id=g.execution_id AND producing_fence=g.producing_fence)
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_family_revision
            WHERE producing_execution_id=g.execution_id AND producing_fence=g.producing_fence)
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_generation_family WHERE generation_id=g.generation_id)
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_winner WHERE generation_id=g.generation_id) THEN
        RAISE EXCEPTION 'custom_import_legacy_canonical_hot_rows_exist'; END IF;
    snapshot_id:=__CONTROL__.create_custom_import_snapshot_family(
        g.execution_id,g.producing_fence,g.producing_token_sha256);
    IF snapshot_id IS DISTINCT FROM __CONTROL__.bind_custom_import_snapshot_generation(
        g.execution_id,g.producing_fence,g.producing_token_sha256,g.generation_id) THEN
        RAISE EXCEPTION 'custom_import_legacy_snapshot_binding_mismatch'; END IF;
    PERFORM __CONTROL__.install_custom_import_legacy_snapshot_writers(snapshot_id);
    IF snapshot_id IS DISTINCT FROM __CONTROL__.lock_custom_import_legacy_generation_snapshot(g.generation_id) THEN
        RAISE EXCEPTION 'custom_import_legacy_snapshot_binding_mismatch'; END IF;
    RETURN snapshot_id;
END;
"""

_HELPERS = (
    ("verify_custom_import_legacy_snapshot_writers", "p_family_id bigint", "void", _VERIFY_BODY),
    ("install_custom_import_legacy_snapshot_writers", "p_family_id bigint", "bigint", _INSTALL_BODY),
    ("resolve_custom_import_legacy_generation_snapshot", "p_generation_id bigint", "bigint", _RESOLVE_BODY),
)


def _bulk():
    path = Path(__file__).with_name("20261005040000_custom_import_bulk_snapshot_writers.py")
    spec = importlib.util.spec_from_file_location("legacy_bulk_writer_prerequisite", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("bulk snapshot writer migration missing")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = op
    return module


def _schema() -> str:
    return _bulk()._schema()


def _resource(name: str) -> tuple[str, ...]:
    if name not in ("legacy_authority.sql", "legacy_writers.sql", "legacy_origin.sql"):
        raise ValueError("unknown legacy writer resource")
    return tuple(block.strip() for block in (_RESOURCE_DIRECTORY / name).read_text().split(_BOUNDARY))


def _render(bulk, storage, schema: str, body: str) -> str:
    signatures = (f"{name}({bulk._argument_types(arguments)})" for name, arguments, _ in _WRITERS)
    body = body.replace("__SIGNATURES__", bulk._array(storage, signatures))
    body = body.replace("__RESULTS__", bulk._array(storage, (result for _, _, result in _WRITERS)))
    body = body.replace("__CONTROL_LITERAL__", storage._literal(schema))
    body = bulk._control_sql(storage, schema, body)
    for marker, resource in (("__ORIGIN_DDL__", "legacy_origin.sql"), ("__LEAF_DDL__", "legacy_writers.sql")):
        if marker in body:
            statements = (bulk._control_sql(storage, schema, sql) for sql in _resource(resource))
            body = body.replace(marker, bulk._array(storage, statements))
    return body


def _deny_function(bulk, storage, schema: str, name: str, arguments: str) -> None:
    identity = f"{storage._quote(schema)}.{name}({bulk._argument_types(arguments)})"
    op.execute(f"REVOKE ALL ON FUNCTION {identity} FROM PUBLIC")
    storage._revoke_defaults("FUNCTION", identity)


def _function(bulk, storage, schema: str, name: str, arguments: str, result: str, body: str) -> None:
    op.execute(
        f"CREATE FUNCTION {storage._quote(schema)}.{name}({arguments}) RETURNS {result}\n"
        "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog\n"
        f"AS $legacy_snapshot$ {_render(bulk, storage, schema, body)} $legacy_snapshot$"
    )
    _deny_function(bulk, storage, schema, name, arguments)


def _dispatcher(name: str, arguments: str, return_type: str) -> str:
    argument_names = [argument.strip().split(" ", 1)[0] for argument in arguments.split(",")]
    placeholders = ",".join(f"${position}" for position in range(1, len(argument_names) + 1))
    if name == "persist_custom_import_legacy_generation_family_set":
        binding = """
        PERFORM __CONTROL__.check_custom_import_generation_materialization_authority(
            p_generation_id,p_dataset_id,p_definition_revision_id,p_schema_revision_id,p_execution_id,
            p_capture_bundle_id,p_fence,p_token,p_expected_authority,p_expected_token,p_expected_expires);
        generation_id:=p_generation_id;
        """
    else:
        binding = """
        PERFORM __CONTROL__.check_custom_import_materialization_authority(
            p_dataset_id,p_definition_revision_id,p_schema_revision_id,p_execution_id,
            p_capture_bundle_id,p_fence,p_token_sha256,p_deadline_at);
        SELECT g.generation_id INTO generation_id FROM __CONTROL__.custom_import_generation g
            WHERE g.execution_id=p_execution_id AND g.producing_fence=p_fence;
        """
    return f"""
    DECLARE family_id bigint; generation_id bigint; namespace text; answer {return_type};
    BEGIN
        {binding}
        family_id:=__CONTROL__.lock_custom_import_legacy_generation_snapshot(generation_id);
        PERFORM __CONTROL__.verify_custom_import_legacy_snapshot_writers(family_id);
        namespace:='ci_snapshot_'||family_id::text;
        EXECUTE format('SELECT %I.{name}({placeholders})',namespace)
            INTO answer USING {",".join(argument_names)};
        {binding}
        IF family_id IS DISTINCT FROM __CONTROL__.lock_custom_import_legacy_generation_snapshot(generation_id) THEN
            RAISE EXCEPTION 'custom_import_legacy_snapshot_binding_mismatch'; END IF;
        RETURN answer;
    END;
    """


def upgrade() -> None:
    """Install only fixed control functions; real authority creates private leaves."""
    bulk = _bulk()
    storage = bulk._storage()
    schema = _schema()
    for statement, (name, arguments, _) in zip(_resource("legacy_authority.sql"), _AUTHORITY_FUNCTIONS, strict=True):
        op.execute(bulk._control_sql(storage, schema, statement))
        _deny_function(bulk, storage, schema, name, arguments)
    for name, arguments, result, body in _HELPERS:
        _function(bulk, storage, schema, name, arguments, result, body)
    for name, arguments, result in _WRITERS:
        _function(bulk, storage, schema, name, arguments, result, _dispatcher(name, arguments, result))


def downgrade() -> None:
    """Never remove functions underneath retained immutable origin evidence."""
    bulk = _bulk()
    storage = bulk._storage()
    schema = _schema()
    op.execute(
        bulk._control_sql(
            storage,
            schema,
            """
        DO $guard$ BEGIN
            LOCK TABLE __CONTROL__.custom_import_snapshot_family IN ACCESS EXCLUSIVE MODE;
            IF EXISTS(SELECT 1 FROM __CONTROL__.custom_import_snapshot_family WHERE origin_table_oid IS NOT NULL) THEN
                RAISE EXCEPTION 'custom_import_legacy_snapshot_downgrade_blocked'; END IF;
        END $guard$
    """,
        )
    )
    signatures = [*reversed(_WRITERS), *(helper[:3] for helper in reversed(_HELPERS)), *reversed(_AUTHORITY_FUNCTIONS)]
    for name, arguments, _ in signatures:
        identity = f"{storage._quote(schema)}.{name}({bulk._argument_types(arguments)})"
        op.execute(f"DROP FUNCTION {identity}")
