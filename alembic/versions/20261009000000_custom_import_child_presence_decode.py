# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Decode admitted child fields once in the existing scalar-presence guard."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

from sqlalchemy import text

from alembic import op

revision = "20261009000000_custom_import_child_presence_decode"
down_revision = "20261006010000_nucc_reference_result_generation"
branch_labels = None
depends_on = None

_WRITER = "append_custom_import_build_source_families_page"
_INSTALLER = "install_custom_import_snapshot_writers"
_OLD_EXPECTED = """WITH expected AS (
        SELECT c.child_revision_id child_id,f.field_slot,e.value->'value'->>'state' value_state
        FROM unnest(p_child_ids) i(child_id) JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=i.child_id
        JOIN __CONTROL__.custom_import_field f ON f.dataset_id=b.dataset_id AND f.schema_revision_id=b.schema_revision_id
            AND f.collection_slot=c.collection_slot AND f.projection_slot>0
        JOIN LATERAL json_array_elements(replace(c.canonical_payload,chr(92)||'u0000',chr(92)||'u0001')::json->'fields') e(value)
            ON e.value->>'field'=f.field_name WHERE e.value->'value'->>'state' IS DISTINCT FROM 'missing'
    )"""
_NEW_EXPECTED = """WITH admitted_children AS MATERIALIZED (
        SELECT c.child_revision_id,c.collection_slot,c.canonical_payload
        FROM unnest(p_child_ids) i(child_id) JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=i.child_id
        WHERE EXISTS(SELECT 1 FROM __CONTROL__.custom_import_field f
            WHERE f.dataset_id=b.dataset_id AND f.schema_revision_id=b.schema_revision_id
                AND f.collection_slot=c.collection_slot AND f.projection_slot>0)
    ), decoded_fields AS MATERIALIZED (
        SELECT c.child_revision_id,c.collection_slot,e.value->>'field' field_name,
            e.value->'value'->>'state' value_state
        FROM admitted_children c
        CROSS JOIN LATERAL json_array_elements(replace(c.canonical_payload,chr(92)||'u0000',chr(92)||'u0001')::json->'fields') e(value)
    ), expected AS (
        SELECT c.child_revision_id child_id,f.field_slot,c.value_state
        FROM decoded_fields c JOIN __CONTROL__.custom_import_field f
            ON f.dataset_id=b.dataset_id AND f.schema_revision_id=b.schema_revision_id
                AND f.collection_slot=c.collection_slot AND f.projection_slot>0 AND f.field_name=c.field_name
        WHERE c.value_state IS DISTINCT FROM 'missing'
    )"""


def _bulk():
    path = Path(__file__).with_name("20261005040000_custom_import_bulk_snapshot_writers.py")
    spec = importlib.util.spec_from_file_location("child_presence_bulk_writers", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _schema() -> str:
    return _bulk()._schema()


def _replace_once(source: str, previous: str, corrected: str) -> str:
    if source.count(previous) != 1:
        raise RuntimeError("custom_import_child_presence_source_mismatch")
    return source.replace(previous, corrected)


def _leaf(bulk, schema: str, *, corrected: bool, namespace: str | None = None) -> str:
    """Keep historical resources exact for earlier body-identity checks."""
    statements = [
        sql
        for sql in bulk._resource("snapshot_writers.sql")
        if sql.startswith(f"CREATE FUNCTION __CANDIDATE__.{_WRITER}(")
    ]
    if len(statements) != 1:
        raise RuntimeError("custom_import_child_presence_source_mismatch")
    statement = statements[0]
    if corrected:
        statement = _replace_once(statement, _OLD_EXPECTED, _NEW_EXPECTED)
    statement = bulk._control_sql(bulk._storage(), schema, statement)
    if namespace is not None:
        if not re.fullmatch(r"ci_snapshot_[1-9][0-9]*", namespace):
            raise RuntimeError("custom_import_child_presence_namespace_mismatch")
        # The installer uses quote_ident for this safe lowercase namespace in the body.
        statement = statement.replace("CREATE FUNCTION __CANDIDATE__.", f'CREATE FUNCTION "{namespace}".', 1)
        statement = statement.replace("__CANDIDATE__", namespace)
    if "__BASE__" in statement or "__FAMILY_ID__" in statement:
        raise RuntimeError("custom_import_child_presence_source_mismatch")
    return statement


def _installer(bulk, schema: str, *, corrected: bool) -> str:
    storage = bulk._storage()
    body = bulk._body(storage, schema, bulk._INSTALL_BODY)
    if corrected:
        body = _replace_once(
            body,
            storage._literal(_leaf(bulk, schema, corrected=False)),
            storage._literal(_leaf(bulk, schema, corrected=True)),
        )
    return (
        f"CREATE FUNCTION {storage._quote(schema)}.{_INSTALLER}(p_family_id bigint) RETURNS bigint\n"
        "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog\n"
        f"AS $bulk_snapshot$ {body} $bulk_snapshot$"
    )


def _installed(bind, identity: str, owner_table: str, result: str, *, returns_set: bool, preserve_execute_grants=False):
    """Reject drift and retain every non-body catalog attribute for comparison."""
    return bind.execute(
        text(r"""
            SELECT p.prosrc,to_jsonb(p)-'prosrc' metadata FROM pg_proc p
            JOIN pg_language language ON language.oid=p.prolang
            JOIN pg_class owner_table ON owner_table.oid=to_regclass(:owner_table)
            WHERE p.oid=to_regprocedure(:identity) AND p.prokind='f' AND p.prosecdef
              AND language.lanname='plpgsql' AND p.proretset=:returns_set
              AND regexp_replace(pg_get_function_result(p.oid),'\s','','g')=regexp_replace(:result,'\s','','g')
              AND NOT p.proisstrict AND NOT p.proleakproof AND p.prosqlbody IS NULL
              AND p.provolatile='v' AND p.proparallel='u' AND p.pronargdefaults=0 AND p.prosupport=0
              AND p.procost=100 AND p.prorows=CASE WHEN :returns_set THEN 1000 ELSE 0 END
              AND p.proconfig=ARRAY['search_path=pg_catalog']::text[]
              AND p.proowner=owner_table.relowner AND NOT EXISTS (
                SELECT 1 FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) privilege
                WHERE (privilege.grantee<>p.proowner AND NOT :preserve_execute_grants)
                  OR privilege.grantee=0 OR privilege.privilege_type<>'EXECUTE')
        """),
        dict(
            identity=identity,
            owner_table=owner_table,
            result=result,
            returns_set=returns_set,
            preserve_execute_grants=preserve_execute_grants,
        ),
    ).first()


def _refresh(
    bind,
    identity: str,
    owner_table: str,
    result: str,
    previous: str,
    corrected: str,
    delimiter: str,
    *,
    preserve_execute_grants=False,
):
    old_body, new_body = previous.split(delimiter)[1], corrected.split(delimiter)[1]
    options_by_name = dict(returns_set=result.startswith("TABLE("), preserve_execute_grants=preserve_execute_grants)
    before = _installed(bind, identity, owner_table, result, **options_by_name)
    if before is None or before.prosrc not in (old_body, new_body):
        raise RuntimeError("custom_import_child_presence_identity_mismatch")
    op.execute(corrected.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1))
    after = _installed(bind, identity, owner_table, result, **options_by_name)
    if after is None or after.prosrc != new_body or after.metadata != before.metadata:
        raise RuntimeError("custom_import_child_presence_refresh_mismatch")


def _registered_namespaces(bind, schema: str, bulk):
    """Use registered storage identities, including already frozen families."""
    quoted = bulk._storage()._quote(schema)
    rows = bind.execute(
        text(f"""
            SELECT f.family_id,n.nspname,
                c.oid IS NOT NULL AND c.relowner::bigint=f.landing_table_owner
                AND c.relowner=owner_table.relowner AND n.nspowner=c.relowner
                AND c.relkind='r' AND c.relpersistence='p' AND c.relname='source_bulk_landing'
                AND {quoted}.custom_import_snapshot_columns_sha256(c.oid::bigint)=f.landing_columns_sha256
                AND NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
                AND NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal)
                AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid=c.oid AND contype='f')
                AND NOT EXISTS (
                    SELECT 1 FROM aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) privilege
                    WHERE privilege.grantee<>n.nspowner AND privilege.privilege_type='CREATE') valid
            FROM {quoted}.custom_import_snapshot_family f
            LEFT JOIN pg_namespace n ON n.nspname='ci_snapshot_'||f.family_id::text
            LEFT JOIN pg_class c ON c.oid=f.landing_table_oid::oid AND c.relnamespace=n.oid
            LEFT JOIN pg_class owner_table ON owner_table.oid=to_regclass(:owner_table)
            WHERE f.landing_table_oid IS NOT NULL ORDER BY f.family_id
        """),
        dict(owner_table=f"{quoted}.custom_import_generation"),
    )
    for row in rows:
        if row.valid is not True or row.family_id <= 0:
            raise RuntimeError("custom_import_child_presence_storage_mismatch")
        yield row.nspname


def upgrade() -> None:
    """Refresh only the existing installer and source-child guard, atomically."""
    bulk_migration = _bulk()
    schema = _schema()
    quoted = bulk_migration._storage()._quote(schema)
    owner_table = f"{quoted}.custom_import_generation"
    bind = op.get_bind()
    # Stabilize registration; deployment must also use a quiet writer/installer window.
    op.execute(f"LOCK TABLE {quoted}.custom_import_snapshot_family IN SHARE ROW EXCLUSIVE MODE")
    _refresh(
        bind,
        f"{quoted}.{_INSTALLER}(bigint)",
        owner_table,
        "bigint",
        _installer(bulk_migration, schema, corrected=False),
        _installer(bulk_migration, schema, corrected=True),
        "$bulk_snapshot$",
    )
    _, arguments, result_type = next(writer for writer in bulk_migration._WRITERS if writer[0] == _WRITER)
    for namespace in _registered_namespaces(bind, schema, bulk_migration):
        identity = (
            f"{bulk_migration._storage()._quote(namespace)}.{_WRITER}({bulk_migration._argument_types(arguments)})"
        )
        _refresh(
            bind,
            identity,
            owner_table,
            result_type,
            _leaf(bulk_migration, schema, corrected=False, namespace=namespace),
            _leaf(bulk_migration, schema, corrected=True, namespace=namespace),
            "$fn$",
        )


def downgrade() -> None:
    """Keep the equivalent guarded path; rollback requires a forward migration."""
    raise RuntimeError("custom_import_child_presence_requires_forward_migration")
