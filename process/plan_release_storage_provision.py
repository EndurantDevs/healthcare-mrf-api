# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit administrator-only ownership cut; never invoked at request/startup."""

from db.plan_release_storage import (
    BINDING_TABLE,
    LEGACY_BINDING_TABLE,
    PIN_TABLE,
    REVISION_TABLE,
    storage_identifier,
    storage_relation,
)
from process.plan_release_storage_custody import _require, storage_profile, storage_roles, verify_storage_family

_FAMILY_SQL = """WITH RECURSIVE family(oid) AS (
 SELECT to_regclass(name)::oid FROM unnest($1::text[]) names(name)
 UNION SELECT child.inhrelid FROM pg_inherits child JOIN family parent ON parent.oid=child.inhparent
) SELECT c.oid::bigint,n.nspname,c.relname FROM family f JOIN pg_class c ON c.oid=f.oid
 JOIN pg_namespace n ON n.oid=c.relnamespace ORDER BY c.oid"""
_GRANTEES_SQL = """SELECT DISTINCT a.grantee,r.rolname,NULL::text AS column_name
 FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
 LEFT JOIN pg_roles r ON r.oid=a.grantee WHERE c.oid=$1 AND a.grantee<>c.relowner
 UNION SELECT DISTINCT a.grantee,r.rolname,field.attname FROM pg_class c
 JOIN pg_attribute field ON field.attrelid=c.oid CROSS JOIN LATERAL aclexplode(field.attacl) a
 LEFT JOIN pg_roles r ON r.oid=a.grantee WHERE c.oid=$1 AND a.grantee<>c.relowner"""


async def _close_relation_grants(connection, relation, owner_role, reader_roles):
    qualified_name = storage_relation(relation["nspname"], relation["relname"])
    await connection.execute(f"ALTER TABLE {qualified_name} OWNER TO {storage_identifier(owner_role)}")
    grants = await connection.fetch(_GRANTEES_SQL, relation["oid"])
    for grant in grants:
        principal = "PUBLIC" if grant["grantee"] == 0 else storage_identifier(grant["rolname"])
        column = grant["column_name"]
        privileges = f"ALL PRIVILEGES ({storage_identifier(column)})" if column else "ALL PRIVILEGES"
        await connection.execute(f"REVOKE {privileges} ON TABLE {qualified_name} FROM {principal} CASCADE")
    readers = ",".join(storage_identifier(name) for name in reader_roles)
    await connection.execute(f"GRANT SELECT ON TABLE {qualified_name} TO {readers}")


async def _retire_closed_legacy_guards(connection, schema_name):
    legacy = storage_relation(schema_name, LEGACY_BINDING_TABLE)
    allowed_trigger_names = {
        BINDING_TABLE + "_attempt_" + suffix
        for suffix in ("insert_guard", "update_guard", "delete_guard", "lifecycle_lock")
    }
    triggers = await connection.fetch(
        "SELECT tgname FROM pg_trigger WHERE tgrelid=to_regclass($1) AND NOT tgisinternal", legacy
    )
    _require(all(trigger["tgname"] in allowed_trigger_names for trigger in triggers), "legacy_hook_changed")
    for trigger in triggers:
        await connection.execute(f"DROP TRIGGER {storage_identifier(trigger['tgname'])} ON {legacy}")
    await connection.execute(
        f"ALTER TABLE {legacy} DROP CONSTRAINT IF EXISTS plan_release_snapshot_binding_revision_fkey"
    )


async def _provision_plan_release_storage(
    connection, schema_name, *, owner_role, publisher_role, reader_roles, control_reader_roles, verify_role_profile
):
    """Close all roots/children first, then retire only obsolete binding guards.

    The caller supplies the trusted deployment role profile and an independent
    native administrator connection. Global shared-role admission is unchanged.
    Native DDL still enforces the caller's actual ownership/role privileges.
    """
    _require(connection.is_in_transaction(), "transaction_required")
    names = storage_profile(owner_role, publisher_role, reader_roles, control_reader_roles)
    actor = await connection.fetchrow("SELECT session_user::text AS session,current_user::text AS current")
    _require(actor["session"] == actor["current"] and actor["current"] not in names, "independent_admin_required")
    await storage_roles(connection, schema_name, owner_role, publisher_role, reader_roles, control_reader_roles)
    roots = [
        storage_relation(schema_name, name) for name in (REVISION_TABLE, BINDING_TABLE, PIN_TABLE, LEGACY_BINDING_TABLE)
    ]
    await connection.execute("SET LOCAL lock_timeout='5s'")
    await connection.execute("LOCK TABLE " + ",".join(roots) + " IN ACCESS EXCLUSIVE MODE")
    family = await connection.fetch(_FAMILY_SQL, roots)
    _require(len(family) >= 4, "roots_missing")
    for relation in family:
        await _close_relation_grants(connection, relation, owner_role, reader_roles)
    # Whole-family native ACL closure precedes dropping either the FK or hooks.
    owner_oid = await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", owner_role)
    from process.plan_release_storage_catalog import CATALOG_SQL

    reader_oids = list(
        await connection.fetchval(
            "SELECT array_agg(oid ORDER BY oid) FROM pg_roles WHERE rolname=ANY($1::text[])", list(reader_roles)
        )
    )
    closed = await connection.fetch(CATALOG_SQL, roots, owner_oid, reader_oids)
    _require(
        all(
            relation["relowner"] == owner_oid and relation["direct_acl_closed"] and relation["readers_closed"]
            for relation in closed
        ),
        "provision_closure_failed",
    )
    # This trusted application callback owns the complete existing role-profile
    # policy. The local custody checker is supplementary, never its replacement.
    profile_by_name = dict(owner_role=owner_role, publisher_role=publisher_role, reader_roles=reader_roles)
    if control_reader_roles:
        profile_by_name["control_reader_roles"] = control_reader_roles
    await verify_role_profile(connection, schema_name, **profile_by_name)
    await _retire_closed_legacy_guards(connection, schema_name)
    return await verify_storage_family(
        connection,
        schema_name,
        owner_role=owner_role,
        publisher_role=publisher_role,
        reader_roles=reader_roles,
        control_reader_roles=control_reader_roles,
    )


async def provision_plan_release_storage(
    connection,
    schema_name,
    *,
    owner_role,
    publisher_role,
    reader_roles,
    control_reader_roles=(),
    verify_role_profile=None,
):
    """Roll back the complete admin cut even when a caller catches a Python rejection."""
    _require(connection.is_in_transaction(), "transaction_required")
    _require(callable(verify_role_profile), "complete_profile_verifier_required")
    async with connection.transaction():
        return await _provision_plan_release_storage(
            connection,
            schema_name,
            owner_role=owner_role,
            publisher_role=publisher_role,
            reader_roles=reader_roles,
            control_reader_roles=control_reader_roles,
            verify_role_profile=verify_role_profile,
        )
