# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare candidate custody and hand off only its empty native ledger."""

from sqlalchemy import text

from db.migration_network_candidate_custody import (
    REVISION,
    TABLE,
    candidate_custody_parent,
    candidate_custody_readers,
    validate_prepared_candidate_custody,
)
from db.migration_registry_preparation import _ACL, _shape, registry_migration
from process.network_address_projection import _identifier
from process.network_membership_writer_closure import _protected_owner
from process.registry_management_permissions import _aca_controls, _catalog, assume_registry_source_owner


def prepare_candidate_custody_table(connection, schema):
    """Create once or validate an incumbent; never repair its ownership, shape, or ACL."""
    namespace = _identifier(schema)
    connection.exec_driver_sql(f"LOCK TABLE {namespace}.network_membership_candidate IN SHARE MODE NOWAIT")
    before = candidate_custody_parent(connection, schema)
    exists = connection.execute(text("SELECT to_regclass(:name)"), {"name": namespace + "." + TABLE}).scalar_one()
    if exists is None:
        registry_migration(REVISION)._create(connection, schema)
    identity = validate_prepared_candidate_custody(connection, schema)
    if candidate_custody_parent(connection, schema) != before:
        raise RuntimeError("Candidate custody preparation changed existing parent identity or privileges")
    return {
        "component": "registry_candidate_custody_preparation",
        "prepared_revisions": [REVISION],
        "created": exists is None,
        **identity,
    }


async def prepare_registry_candidate_custody(connection, *, owner_role, control_schema):
    """Use existing maintenance authority after scoped writer admission and physical drain."""
    if not connection.in_transaction():
        raise ValueError("Candidate custody preparation requires a caller transaction")
    async with connection.begin_nested():
        raw = await connection.get_raw_connection()
        driver = raw.driver_connection
        caller = await driver.fetchval("SELECT quote_ident(current_user)")
        await assume_registry_source_owner(driver, owner_role, control_schema=control_schema)
        receipt = await connection.run_sync(prepare_candidate_custody_table, control_schema)
        await driver.execute(f"SET LOCAL ROLE {caller}")
        return receipt


def _empty_custody_identity(connection, schema):
    """Lock and validate the original empty publication boundary without changing it."""
    namespace = _identifier(schema)
    for table in (TABLE, "network_membership_candidate", "network_serving_control", "network_serving_manifest"):
        connection.exec_driver_sql(f"LOCK TABLE {namespace}.{_identifier(table)} IN ACCESS EXCLUSIVE MODE NOWAIT")
    identity = validate_prepared_candidate_custody(connection, schema)
    if (
        connection.scalar(
            text(
                "SELECT count(*)=4 AND bool_and(c.relowner=n.nspowner AND c.relkind='r' AND c.relpersistence='p') "
                "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=:schema AND c.relname=ANY(CAST(:tables AS text[]))"
            ),
            {
                "schema": schema,
                "tables": [
                    TABLE,
                    "network_membership_candidate",
                    "network_serving_control",
                    "network_serving_manifest",
                ],
            },
        )
        is not True
    ):
        raise RuntimeError("Candidate custody handoff publication ownership differs")
    for table in (TABLE, "network_membership_candidate", "network_serving_manifest"):
        if connection.scalar(text(f"SELECT EXISTS(SELECT FROM {namespace}.{_identifier(table)})")):
            raise RuntimeError("Candidate custody handoff requires empty publication storage")
    if (
        connection.scalar(
            text(
                f"SELECT count(*)=1 AND bool_and(id=1 AND generation_id IS NULL) FROM {namespace}.network_serving_control"
            )
        )
        is not True
    ):
        raise RuntimeError("Candidate custody handoff requires the original empty serving head")
    return identity


def _split_empty_candidate_custody(connection, schema, custody_owner_role, caller):
    """Transfer the validated ledger while retaining native storage and exact readers."""
    identity = _empty_custody_identity(connection, schema)
    namespace = _identifier(schema)
    parent = candidate_custody_parent(connection, schema)
    readers = candidate_custody_readers(connection, schema)
    shape = _shape(connection, identity["table_oid"])
    storage_sql = text("SELECT oid,relfilenode,relam,reltablespace FROM pg_class WHERE oid=:oid")
    storage_fields = tuple(connection.execute(storage_sql, {"oid": identity["table_oid"]}).one())
    schema_acl_sql = text("SELECT oid,nspowner,nspacl::text FROM pg_namespace WHERE nspname=:schema")
    schema_acl_fields = tuple(connection.execute(schema_acl_sql, {"schema": schema}).one())
    connection.exec_driver_sql(f"SET LOCAL ROLE {caller}")
    connection.exec_driver_sql(f"ALTER TABLE {namespace}.{TABLE} OWNER TO {_identifier(custody_owner_role)}")
    connection.exec_driver_sql(f"SET LOCAL ROLE {_identifier(custody_owner_role)}")
    for reader in readers:
        connection.exec_driver_sql(f"GRANT SELECT ON {namespace}.{TABLE} TO {_identifier(reader)}")
    connection.exec_driver_sql(f"SET LOCAL ROLE {caller}")
    owner_oid = connection.scalar(text("SELECT oid FROM pg_roles WHERE rolname=:role"), {"role": custody_owner_role})
    expected_acl = connection.execute(
        text("""SELECT a.grantor,a.grantee,a.privilege_type,a.is_grantable,NULL::text
        FROM aclexplode(acldefault('r',CAST(:owner AS oid))) a
        UNION ALL SELECT CAST(:owner AS oid),r.oid,'SELECT',false,NULL::text
        FROM pg_roles r WHERE r.rolname=ANY(CAST(:readers AS text[]))"""),
        {"owner": owner_oid, "readers": readers},
    )
    actual_acl = connection.execute(text(_ACL), {"oid": identity["table_oid"]})
    if (
        {tuple(grant) for grant in actual_acl} != {tuple(grant) for grant in expected_acl}
        or _shape(connection, identity["table_oid"]) != shape
        or tuple(connection.execute(storage_sql, {"oid": identity["table_oid"]}).one()) != storage_fields
        or candidate_custody_parent(connection, schema) != parent
        or tuple(connection.execute(schema_acl_sql, {"schema": schema}).one()) != schema_acl_fields
    ):
        raise RuntimeError("Candidate custody handoff changed protected storage or reader privileges")
    return {**identity, "reader_roles": readers}


async def split_registry_candidate_custody(connection, *, owner_role, custody_owner_role, control_schema):
    """Hand off an empty native postimage after the caller verifies adoption ordering."""
    if not connection.in_transaction():
        raise ValueError("Candidate custody handoff requires a caller transaction")
    for name in (owner_role, custody_owner_role, control_schema):
        _identifier(name)
    if owner_role == custody_owner_role:
        raise ValueError("Candidate custody handoff requires a separate owner")
    async with connection.begin_nested():
        raw = await connection.get_raw_connection()
        driver = raw.driver_connection
        actor = await driver.fetchrow(
            "SELECT r.rolcanlogin,r.rolsuper,current_user=session_user AS original "
            "FROM pg_roles r WHERE r.rolname=current_user"
        )
        if actor is None or not all(actor.values()):
            raise ValueError("Candidate custody handoff requires the original LOGIN administrator")
        custody_owner = await driver.fetchrow("SELECT * FROM pg_roles WHERE rolname=$1", custody_owner_role)
        if not _protected_owner(custody_owner) or not await driver.fetchval(
            "SELECT has_schema_privilege($1::name,$2,'USAGE') "
            "AND NOT has_schema_privilege($1::name,$2,'CREATE') "
            "AND NOT pg_has_role($1::name,$3::name,'MEMBER') "
            "AND NOT pg_has_role($3::name,$1::name,'MEMBER')",
            custody_owner_role,
            control_schema,
            owner_role,
        ):
            raise ValueError("Candidate custody handoff owner privileges are unsafe")
        caller = await driver.fetchval("SELECT quote_ident(current_user)")
        await assume_registry_source_owner(driver, owner_role, control_schema=control_schema)
        schema = await driver.fetchrow("SELECT oid,nspowner FROM pg_namespace WHERE nspname=$1", control_schema)
        tables, sequences = await _catalog(driver, schema["oid"])
        controls = await _aca_controls(driver, control_schema)
        if any(relation["relowner"] != schema["nspowner"] for relation in tables + sequences + controls):
            raise ValueError("Candidate custody handoff requires original uniform registry ownership")
        identity = await connection.run_sync(_split_empty_candidate_custody, control_schema, custody_owner_role, caller)
        from process.network_candidate_custody import _control

        control = await _control(driver, control_schema)
        if (
            control["owner_oid"] != custody_owner["oid"]
            or control["schema_owner_role"] != owner_role
            or control["seal_table_oid"] != identity["table_oid"]
        ):
            raise RuntimeError("Candidate custody handoff ownership differs")
        return {
            "component": "registry_candidate_custody_owner_split",
            "revision": 1,
            "schema_name": control_schema,
            "schema_oid": control["schema_oid"],
            "owner_role": owner_role,
            "custody_owner_role": custody_owner_role,
            **identity,
        }
