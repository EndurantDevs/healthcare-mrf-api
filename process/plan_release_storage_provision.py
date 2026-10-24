# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit administrator-only ownership cut; never invoked at request/startup."""

from db.migration_plan_release_storage import MAX_LEGACY_REVISIONS
from db.plan_release_storage import (
    BINDING_TABLE,
    LEGACY_BINDING_TABLE,
    PIN_TABLE,
    REVISION_TABLE,
    storage_identifier,
    storage_relation,
    storage_snapshot_index_sql,
    storage_table_sql,
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
_LEGACY_PIN_TABLE = PIN_TABLE + "_legacy"
_INPUT_COLUMNS = {
    "ptg2_snapshot": ("snapshot_id", "status", "manifest"),
    "ptg2_v3_snapshot_plan_scope": ("snapshot_id", "plan_id", "plan_market_type"),
}


async def _require_input_reads(connection, schema_name, owner_oid):
    """Keep external snapshot metadata readable without ownership, writes or delegation."""
    for table_name, columns in _INPUT_COLUMNS.items():
        _require(
            await connection.fetchval(
                "SELECT NOT pg_has_role($1::oid,c.relowner,'SET') "
                "AND NOT has_table_privilege($1::oid,c.oid,"
                "'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN,SELECT WITH GRANT OPTION') "
                "AND NOT has_any_column_privilege($1::oid,c.oid,"
                "'INSERT,UPDATE,REFERENCES,SELECT WITH GRANT OPTION') "
                "AND (SELECT bool_and(has_column_privilege($1::oid,c.oid,column_name,'SELECT')) "
                "FROM unnest($3::text[]) required(column_name)) "
                "FROM pg_class c WHERE c.oid=to_regclass($2)",
                owner_oid,
                storage_relation(schema_name, table_name),
                list(columns),
            )
            is True,
            "input_metadata_privileges_invalid",
        )


async def _grant_input_reads(connection, schema_name, owner_role, owner_oid):
    """Add only the six columns used by native publication's source anti-join."""
    for table_name, columns in _INPUT_COLUMNS.items():
        fields = ",".join(storage_identifier(column) for column in columns)
        await connection.execute(
            f"GRANT SELECT ({fields}) ON TABLE {storage_relation(schema_name, table_name)} TO {storage_identifier(owner_role)}"
        )
    await _require_input_reads(connection, schema_name, owner_oid)


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


async def _legacy_binding_revisions(connection, schema_name):
    """Use only the closed retained heap; no publication or admission is invented."""
    legacy = storage_relation(schema_name, LEGACY_BINDING_TABLE)
    bindings = storage_relation(schema_name, BINDING_TABLE)
    header = storage_relation(schema_name, REVISION_TABLE)
    revisions = await connection.fetch(
        f"SELECT serving_revision_id FROM ONLY {legacy} GROUP BY serving_revision_id "
        "ORDER BY serving_revision_id LIMIT $1",
        MAX_LEGACY_REVISIONS + 1,
    )
    _require(len(revisions) <= MAX_LEGACY_REVISIONS, "legacy_revision_bound_exceeded")
    _require(
        not await connection.fetchval(
            f"WITH actual AS (SELECT serving_revision_id,count(*) AS count FROM {bindings} "
            "GROUP BY serving_revision_id) SELECT EXISTS(SELECT FROM actual "
            f"FULL JOIN {header} h USING(serving_revision_id) WHERE h.serving_revision_id IS NULL "
            "OR actual.count>h.expected_binding_count OR (h.serving_status<>'building' "
            "AND (h.release_status='candidate' OR coalesce(actual.count,0)<>h.expected_binding_count)))"
        ),
        "legacy_binding_cohort_changed",
    )
    return tuple(row["serving_revision_id"] for row in revisions)


def _legacy_pin_rows(schema_name):
    """Derive retention from immutable bindings, including history whose old pins retired."""
    legacy = storage_relation(schema_name, LEGACY_BINDING_TABLE)
    return (
        "SELECT serving_revision_id,snapshot_id,'legacy binding retention'::varchar(256) AS reason,"
        f"min(created_at) AS created_at FROM ONLY {legacy} GROUP BY serving_revision_id,snapshot_id"
    )


async def _require_legacy_pin_replay(connection, schema_name, custody, revision_ids, child_oid):
    by_name = {row["relname"]: row for row in custody["relations"] if row["nspname"] == schema_name}
    child = by_name.get(_LEGACY_PIN_TABLE)
    expected_bound = "FOR VALUES IN (" + ", ".join("'" + revision + "'" for revision in revision_ids) + ")"
    _require(
        child is not None
        and child["oid"] == child_oid
        and child["parents"] == [by_name[PIN_TABLE]["oid"]]
        and child["bound"] == expected_bound,
        "legacy_pin_enrollment_conflict",
    )
    relation = storage_relation(schema_name, _LEGACY_PIN_TABLE)
    expected = _legacy_pin_rows(schema_name)
    _require(
        not await connection.fetchval(
            f"SELECT EXISTS(({expected} EXCEPT ALL SELECT * FROM ONLY {relation}) UNION ALL "
            f"(SELECT * FROM ONLY {relation} EXCEPT ALL {expected}))"
        ),
        "legacy_pin_enrollment_changed",
    )


async def _enroll_legacy_pins(connection, schema_name, custody, owner_role, reader_roles):
    """Enroll exact existing retention once; never repair or adopt a partial child."""
    revision_ids = await _legacy_binding_revisions(connection, schema_name)
    relation = storage_relation(schema_name, _LEGACY_PIN_TABLE)
    child_oid = await connection.fetchval("SELECT to_regclass($1)::oid", relation)
    if child_oid is not None:
        await _require_legacy_pin_replay(connection, schema_name, custody, revision_ids, child_oid)
        return
    if not revision_ids:
        return
    root = storage_relation(schema_name, PIN_TABLE)
    _require(
        not await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {root} WHERE serving_revision_id=ANY($1::text[]))", list(revision_ids)
        ),
        "legacy_pin_enrollment_conflict",
    )
    await connection.execute(storage_table_sql(schema_name, _LEGACY_PIN_TABLE, pin=True, primary_key=False))
    await connection.execute(f"INSERT INTO {relation} " + _legacy_pin_rows(schema_name))
    await connection.execute(f"ALTER TABLE {relation} ADD PRIMARY KEY(serving_revision_id,snapshot_id)")
    await connection.execute(storage_snapshot_index_sql(schema_name, _LEGACY_PIN_TABLE, _LEGACY_PIN_TABLE + "_idx"))
    literals = ",".join("'" + revision + "'" for revision in revision_ids)
    await connection.execute(f"ALTER TABLE {relation} ADD CHECK(serving_revision_id IN ({literals}))")
    child = await connection.fetchrow(
        "SELECT c.oid,n.nspname,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE c.oid=to_regclass($1)",
        relation,
    )
    await _close_relation_grants(connection, child, owner_role, reader_roles)
    await connection.execute(f"ALTER TABLE {root} ATTACH PARTITION {relation} FOR VALUES IN ({literals})")


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
    await _grant_input_reads(connection, schema_name, owner_role, owner_oid)
    # This trusted application callback owns the complete existing role-profile
    # policy. The local custody checker is supplementary, never its replacement.
    profile_by_name = dict(owner_role=owner_role, publisher_role=publisher_role, reader_roles=reader_roles)
    if control_reader_roles:
        profile_by_name["control_reader_roles"] = control_reader_roles
    await verify_role_profile(connection, schema_name, **profile_by_name)
    await _retire_closed_legacy_guards(connection, schema_name)
    custody = await verify_storage_family(connection, schema_name, **profile_by_name)
    await _enroll_legacy_pins(connection, schema_name, custody, owner_role, reader_roles)
    await _require_input_reads(connection, schema_name, owner_oid)
    return await verify_storage_family(connection, schema_name, **profile_by_name)


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
