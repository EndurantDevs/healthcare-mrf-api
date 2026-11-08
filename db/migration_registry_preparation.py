# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Adopt owner-prepared registry changes without ordinary-role DDL authority."""

import importlib.util
import re
from pathlib import Path

from sqlalchemy import text

from db.migration_expression_adoption import _database_connection

_REVISIONS = {
    "20261007130000_registry_network_bindings",
    "20261009040000_registry_ptg_published_plan_scope",
    "20261016070000_registry_allowed_plan_scope",
    "20261017010000_registry_cms_source_notifications",
    "20261017030000_registry_ptg_office_recipe_selection",
    "20261018011000_network_candidate_custody",
    "20261018040000_registry_aca_source_notifications",
    "20261028010000_registry_fhir_catalog",
}
_TABLES = {
    "registry_ptg_published_plan_scope",
    "registry_network_binding",
    "registry_cms_source_notification",
    "registry_aca_source_notification",
    "registry_fhir_catalog_snapshot",
}
_COLUMNS = """SELECT attname,format_type(atttypid,atttypmod),attnotnull,
 attidentity::text,attgenerated::text,attcollation::bigint,pg_get_expr(d.adbin,d.adrelid)
 FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
 WHERE a.attrelid=:oid AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum"""
_CONSTRAINTS = """SELECT conname,contype::text,pg_get_constraintdef(oid,false),convalidated,
 coalesce((to_jsonb(c)->>'conenforced')::boolean,true),condeferrable,condeferred
 FROM pg_constraint c WHERE conrelid=:oid ORDER BY conname"""
_INDEXES = """SELECT c.relname,am.amname,i.indisunique,i.indisprimary,i.indisexclusion,
 i.indimmediate,i.indisvalid,i.indisready,i.indislive,i.indnkeyatts,i.indnatts,
 i.indnullsnotdistinct,i.indclass::text,i.indcollation::text,i.indoption::text,
 ARRAY(SELECT pg_get_indexdef(i.indexrelid,n,false) FROM generate_series(1,i.indnatts) n),
 pg_get_expr(i.indpred,i.indrelid),c.reloptions
 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am am ON am.oid=c.relam
 WHERE i.indrelid=:oid ORDER BY c.relname"""
_ACL = """SELECT a.grantor,a.grantee,a.privilege_type,a.is_grantable,NULL::text AS column_name
 FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
 WHERE c.oid=:oid UNION ALL
 SELECT a.grantor,a.grantee,a.privilege_type,a.is_grantable,att.attname
 FROM pg_attribute att CROSS JOIN LATERAL aclexplode(att.attacl) a
 WHERE att.attrelid=:oid AND att.attnum>0 AND NOT att.attisdropped"""
_BINDING_INSERT = {
    "binding_id",
    "source_system",
    "source_id",
    "dataset_schema",
    "dataset_id",
    "producer_id",
    "edition_id",
    "source_key",
    "source_scope_json",
    "binding_key",
    "network_id",
    "evidence_id",
    "evidence_sha256",
    "archived",
    "revision",
    "office_recipe_json",
}
_BINDING_UPDATE = {"network_id", "evidence_id", "evidence_sha256", "archived", "revision", "office_recipe_json"}


def _identifier(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", value) is None:
        raise ValueError("Registry preparation identifier is invalid")
    return '"' + value + '"'


def registry_migration(revision):
    """Load only the frozen definitions needed by this bounded preparation."""
    if revision not in _REVISIONS:
        raise ValueError("Registry preparation revision is unsupported")
    path = Path(__file__).parents[1] / "alembic" / "versions" / (revision + ".py")
    specification = importlib.util.spec_from_file_location("registry_preparation_" + revision, path)
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    return migration


def _shape(connection, oid):
    return tuple(
        [tuple(row) for row in connection.execute(text(query), {"oid": oid})]
        for query in (_COLUMNS, _CONSTRAINTS, _INDEXES)
    )


def _owner(connection, schema):
    owner = (
        connection.execute(
            text("""SELECT n.oid AS schema_oid,r.*,
        pg_has_role(current_user,r.oid,'USAGE') AS caller_owns
        FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner WHERE n.nspname=:schema"""),
            {"schema": schema},
        )
        .mappings()
        .one_or_none()
    )
    if owner is None:
        raise RuntimeError("Registry preparation requires an existing namespace")
    return owner


def _is_safe_grantee(connection, grantee, owner):
    return (
        connection.execute(
            text("""SELECT NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls
        OR pg_has_role(oid,CAST(:owner AS oid),'MEMBER') OR pg_has_role(oid,CAST(:owner AS oid),'SET')
        OR pg_has_role(oid,'pg_write_all_data','MEMBER')
        OR has_parameter_privilege(oid,'session_replication_role','SET')
        OR has_schema_privilege(oid,CAST(:schema AS oid),'CREATE'))
        FROM pg_roles WHERE oid=:grantee"""),
            {"grantee": grantee, "owner": owner["oid"], "schema": owner["schema_oid"]},
        ).scalar_one_or_none()
        is True
    )


def _is_allowed_registry_grant(grant, table):
    """Keep each prepared relation's table and column privileges closed."""
    privilege, column = grant["privilege_type"], grant["column_name"]
    is_allowed = privilege == "SELECT"
    if table == "registry_network_binding":
        is_allowed |= privilege == "INSERT" and column in _BINDING_INSERT
        is_allowed |= privilege == "UPDATE" and column in _BINDING_UPDATE
    elif table == "registry_ptg_published_plan_scope":
        is_allowed |= privilege == "INSERT" and column is None
    elif table in {"network_candidate_custody", "registry_fhir_catalog_snapshot"}:
        is_allowed = privilege == "SELECT" and column is None
    else:
        is_allowed = False
    return is_allowed


def _validate_acl(connection, relation, owner):
    """Preserve bounded existing roles; notification creation starts owner-only."""
    grants = connection.execute(text(_ACL), {"oid": relation["oid"]}).mappings().all()
    grantees = {grant["grantee"] for grant in grants if grant["grantee"] != owner["oid"]}
    if len(grantees) > 128 or any(not _is_safe_grantee(connection, role, owner) for role in grantees):
        raise RuntimeError("Registry preparation grantee authority differs")
    writers = {
        grant["grantee"]
        for grant in grants
        if grant["grantee"] != owner["oid"] and grant["privilege_type"] in {"INSERT", "UPDATE"}
    }
    if len(writers) > 1:
        raise RuntimeError("Registry preparation writer inventory differs")
    for grant in grants:
        if grant["grantee"] == owner["oid"]:
            continue
        if (
            not _is_allowed_registry_grant(grant, relation["relname"])
            or grant["is_grantable"]
            or grant["grantor"] != owner["oid"]
        ):
            raise RuntimeError("Registry preparation privileges differ")


def _require_namespace_owner(connection, table, owner):
    """Retain closed namespace and serving-owner prerequisites during ordinary adoption."""
    if connection.execute(
        text("""SELECT EXISTS(SELECT FROM pg_namespace n
        CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a
        WHERE n.oid=:schema AND a.grantee<>n.nspowner AND a.privilege_type='CREATE')"""),
        {"schema": owner["schema_oid"]},
    ).scalar_one():
        raise RuntimeError("Registry preparation namespace custody differs")
    if any(
        owner[name]
        for name in ("rolcanlogin", "rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")
    ):
        raise RuntimeError("Registry preparation owner is not protected")
    if (
        table == "registry_aca_source_notification"
        and connection.execute(
            text("""SELECT EXISTS(SELECT FROM pg_class WHERE relnamespace=:schema
        AND relname='network_serving_control' AND relkind='r' AND relpersistence='p'
        AND relowner=:owner)"""),
            {"schema": owner["schema_oid"], "owner": owner["oid"]},
        ).scalar_one()
        is not True
    ):
        raise RuntimeError("Registry preparation serving owner differs")


def _protected_relation(connection, schema, table, owner):
    """Require ordinary persistent storage with the original protected authority."""
    _require_namespace_owner(connection, table, owner)
    relation = (
        connection.execute(
            text("""SELECT c.oid,c.relname,c.relowner,c.relkind::text,c.relpersistence::text,
        c.relispartition,c.relrowsecurity,c.relforcerowsecurity,c.reloptions,
        EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS inheritance,
        EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid) AS rules,
        EXISTS(SELECT FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal) AS triggers,
        EXISTS(SELECT FROM pg_attribute WHERE attrelid=c.oid AND attnum>0 AND attisdropped) AS dropped_columns,
        EXISTS(SELECT FROM pg_constraint WHERE confrelid=c.oid) AS incoming_foreign_keys
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=:schema AND c.relname=:table"""),
            {"schema": schema, "table": table},
        )
        .mappings()
        .one_or_none()
    )
    if relation is None:
        raise RuntimeError("Registry owner preparation is missing")
    if (
        relation["relowner"] != owner["oid"]
        or relation["relkind"] != "r"
        or relation["relpersistence"] != "p"
        or any(
            relation[name]
            for name in (
                "relispartition",
                "relrowsecurity",
                "relforcerowsecurity",
                "reloptions",
                "inheritance",
                "rules",
                "triggers",
                "dropped_columns",
                "incoming_foreign_keys",
            )
        )
    ):
        raise RuntimeError("Registry preparation storage custody differs")
    _validate_acl(connection, relation, owner)
    return relation


def validate_prepared_registry_table(connection, schema, table, expected_sql):
    """Compare the entire native postimage to frozen DDL in private temporary storage."""
    if table not in _TABLES:
        raise ValueError("Registry preparation table is unsupported")
    _identifier(schema)
    owner = _owner(connection, schema)
    relation = _protected_relation(connection, schema, table, owner)
    temporary = "pg_temp." + _identifier(table)
    if connection.execute(text("SELECT to_regclass(:name)"), {"name": temporary}).scalar_one() is not None:
        raise RuntimeError("Registry preparation temporary name is already occupied")
    with connection.begin_nested():
        previous = connection.execute(text("SELECT current_setting('search_path')")).scalar_one()
        connection.execute(text("SELECT set_config('search_path','pg_catalog,pg_temp',true)"))
        statements = expected_sql("pg_temp")
        connection.exec_driver_sql(statements[0].replace("CREATE TABLE", "CREATE TEMPORARY TABLE", 1))
        for statement in statements[1:]:
            connection.exec_driver_sql(statement)
        oid = connection.execute(text("SELECT to_regclass(:name)::oid"), {"name": temporary}).scalar_one()
        if _shape(connection, relation["oid"]) != _shape(connection, oid):
            raise RuntimeError("Registry owner preparation native postimage differs")
        connection.exec_driver_sql(f"DROP TABLE IF EXISTS {temporary}")
        connection.execute(text("SELECT set_config('search_path',:previous,true)"), {"previous": previous})


def can_adopt_prepared_registry_table(operations, schema, table, expected_sql):
    """Ordinary migrations may validate preparation, never acquire protected ownership."""
    connection = _database_connection(operations)
    if connection is None or _owner(connection, schema)["caller_owns"]:
        return False
    validate_prepared_registry_table(connection, schema, table, expected_sql)
    return True
