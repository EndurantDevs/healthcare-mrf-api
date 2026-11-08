# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact native adoption of independently owner-prepared candidate custody."""

from sqlalchemy import text

from db.migration_expression_adoption import _database_connection
from db.migration_registry_preparation import _ACL, _identifier, _owner, _protected_relation, _shape, registry_migration

REVISION = "20261018011000_network_candidate_custody"
TABLE = "network_candidate_custody"


def candidate_custody_readers(connection, schema):
    """Retain exactly the migration's existing explicit nongrantable candidate readers."""
    readers = list(
        connection.execute(
            text("""SELECT DISTINCT role.rolname
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) acl
        JOIN pg_roles role ON role.oid=acl.grantee WHERE n.nspname=:schema
        AND c.relname='network_membership_candidate' AND acl.privilege_type='SELECT'
        AND acl.grantee<>c.relowner AND NOT acl.is_grantable ORDER BY role.rolname LIMIT 129"""),
            {"schema": schema},
        ).scalars()
    )
    if len(readers) > 128:
        raise RuntimeError("Candidate custody reader inventory exceeds its bound")
    return readers


def candidate_custody_parent(connection, schema):
    """Bind the existing native parent and retain its identity and every ACL tuple."""
    parent = (
        connection.execute(
            text("""SELECT c.oid,c.relowner,c.relacl::text,
        ARRAY(SELECT ROW(a.attname,a.attacl::text)::text FROM pg_attribute a
          WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum) AS column_acl
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=:schema AND c.relname='network_membership_candidate' AND c.relowner=n.nspowner
        AND c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition
        AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
        AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
        AND NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid)
        AND NOT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal)
        AND EXISTS(SELECT FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid
          WHERE k.conrelid=c.oid AND k.contype='p' AND k.convalidated
          AND i.indisvalid AND i.indisready AND i.indislive
          AND k.conkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=c.oid
            AND attname='candidate_id' AND atttypid='uuid'::regtype AND attnotnull AND NOT attisdropped)]::smallint[])"""),
            {"schema": schema},
        )
        .mappings()
        .one_or_none()
    )
    if parent is None:
        raise RuntimeError("Candidate custody preparation parent differs")
    return dict(parent)


def _validate_foreign_key(connection, oid, parent):
    matches = connection.execute(
        text("""SELECT count(*)=1 AND bool_and(
        conname='network_candidate_custody_candidate_id_fkey' AND convalidated
        AND coalesce((to_jsonb(c)->>'conenforced')::boolean,true)
        AND NOT condeferrable AND NOT condeferred AND confrelid=:parent
        AND confdeltype='a' AND confupdtype='a' AND confmatchtype='s'
        AND conindid=(SELECT indexrelid FROM pg_index WHERE indrelid=:parent AND indisprimary)
        AND conkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=:oid
          AND attname='candidate_id' AND NOT attisdropped)]::smallint[]
        AND confkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=:parent
          AND attname='candidate_id' AND NOT attisdropped)]::smallint[])
        FROM pg_constraint c WHERE conrelid=:oid AND contype='f'"""),
        {"oid": oid, "parent": parent["oid"]},
    ).scalar_one()
    if matches is not True:
        raise RuntimeError("Candidate custody preparation foreign key differs")


def _validate_readers(connection, schema, oid, owner):
    expected = connection.execute(
        text("""SELECT a.grantor,a.grantee,a.privilege_type,
        a.is_grantable,NULL::text FROM aclexplode(acldefault('r',CAST(:owner AS oid))) a
        UNION ALL SELECT CAST(:owner AS oid),r.oid,'SELECT',false,NULL::text
        FROM pg_roles r WHERE r.rolname=ANY(CAST(:readers AS text[]))"""),
        {"owner": owner["oid"], "readers": candidate_custody_readers(connection, schema)},
    )
    actual = connection.execute(text(_ACL), {"oid": oid})
    if {tuple(row) for row in actual} != {tuple(row) for row in expected}:
        raise RuntimeError("Candidate custody preparation reader privileges differ")


def validate_prepared_candidate_custody(connection, schema):
    """Compare complete native storage plus the real parent FK and exact reader ACL."""
    _identifier(schema)
    owner = _owner(connection, schema)
    relation = _protected_relation(connection, schema, TABLE, owner)
    parent = candidate_custody_parent(connection, schema)
    _validate_foreign_key(connection, relation["oid"], parent)
    _validate_readers(connection, schema, relation["oid"], owner)
    temporary = "pg_temp." + _identifier(TABLE)
    if connection.execute(text("SELECT to_regclass(:name)"), {"name": temporary}).scalar_one() is not None:
        raise RuntimeError("Candidate custody preparation temporary name is already occupied")
    with connection.begin_nested():
        previous = connection.execute(text("SELECT current_setting('search_path')")).scalar_one()
        connection.execute(text("SELECT set_config('search_path','pg_catalog,pg_temp',true)"))
        expected_sql = registry_migration(REVISION)._postimage("pg_temp")
        connection.exec_driver_sql(expected_sql.replace("CREATE TABLE", "CREATE TEMPORARY TABLE", 1))
        oid = connection.execute(text("SELECT to_regclass(:name)::oid"), {"name": temporary}).scalar_one()
        columns, constraints, indexes = _shape(connection, relation["oid"])
        # Temporary FKs cannot reference permanent storage; validate the real FK above.
        actual = (columns, [row for row in constraints if row[1] != "f"], indexes)
        if actual != _shape(connection, oid):
            raise RuntimeError("Candidate custody preparation native postimage differs")
        connection.exec_driver_sql(f"DROP TABLE {temporary}")
        connection.execute(text("SELECT set_config('search_path',:previous,true)"), {"previous": previous})
    return {"table_oid": relation["oid"], "candidate_table_oid": parent["oid"]}


def can_adopt_prepared_candidate_custody(operations, schema):
    """Validate existing preparation without protected ownership or any ACL change."""
    connection = _database_connection(operations)
    if connection is None or _owner(connection, schema)["caller_owns"]:
        return False
    validate_prepared_candidate_custody(connection, schema)
    return True
