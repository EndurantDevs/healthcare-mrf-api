# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native ownership and privilege closure for one registered candidate."""

from __future__ import annotations

import json
from typing import Any

from process.network_address_projection import _identifier
from process.network_membership_candidate_lifecycle import (
    _control_namespace,
    _locked_candidate,
    _require_transaction,
)
from process.network_membership_copy import MembershipCopyTarget


class NetworkWriterClosureError(ValueError):
    """Candidate ownership, native identity or privileges are not closed."""


def _is_protected_owner(role_row: Any) -> bool:
    """Accept a native role mapping without imposing membership or INHERIT policy."""
    flags = ("rolcanlogin", "rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")
    return role_row is not None and all(role_row[flag] is False for flag in flags)


_protected_owner = _is_protected_owner


_NATIVE_RUNTIME_ROLE_UNSAFE_SQL = """r.oid IS NULL OR r.rolsuper OR r.rolcreaterole
 OR pg_has_role(r.oid,s.owner_oid,'MEMBER') OR pg_has_role(r.oid,s.owner_oid,'USAGE')
 OR pg_has_role(r.oid,s.owner_oid,'SET')
 OR EXISTS(SELECT 1 FROM pg_roles elevated WHERE (elevated.rolsuper OR elevated.rolcreaterole)
   AND pg_has_role(r.oid,elevated.oid,'MEMBER'))"""


_NATIVE_WRITER_PRIVILEGES_SQL = f""",
runtime_roles AS (
 SELECT configured.name,r.* FROM closure_scope s CROSS JOIN unnest(s.role_names) configured(name)
 LEFT JOIN pg_roles r ON r.rolname=configured.name
), native_acl AS (
 SELECT a.grantee,a.privilege_type,a.is_grantable,'USAGE'::text AS allowed
 FROM closure_scope s JOIN pg_namespace n ON n.oid=s.schema_oid
 CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a
 WHERE a.grantee<>s.owner_oid
 UNION ALL SELECT a.grantee,a.privilege_type,a.is_grantable,'SELECT'
 FROM closure_scope s JOIN pg_class c ON c.relnamespace=s.schema_oid AND c.relkind='r'
 CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
 WHERE a.grantee<>s.owner_oid AND (s.relation_names IS NULL OR c.relname=ANY(s.relation_names))
 UNION ALL SELECT a.grantee,a.privilege_type,a.is_grantable,'SELECT'
 FROM closure_scope s JOIN pg_class c ON c.relnamespace=s.schema_oid
 JOIN pg_attribute att ON att.attrelid=c.oid AND att.attnum>0 AND NOT att.attisdropped
 CROSS JOIN LATERAL aclexplode(att.attacl) a
 WHERE a.grantee<>s.owner_oid AND (s.relation_names IS NULL OR c.relname=ANY(s.relation_names))
 UNION ALL SELECT a.grantee,a.privilege_type,a.is_grantable,
 CASE WHEN d.defaclobjtype='r' THEN 'SELECT' ELSE '' END
 FROM closure_scope s JOIN pg_default_acl d ON d.defaclnamespace=s.schema_oid
 CROSS JOIN LATERAL aclexplode(d.defaclacl) a WHERE a.grantee<>d.defaclrole
)
SELECT (SELECT schema_oid IS NOT NULL AND owner_oid IS NOT NULL AND cardinality(role_names)<=64
 AND EXISTS(SELECT 1 FROM pg_roles protected_owner WHERE protected_owner.oid=owner_oid
   AND NOT protected_owner.rolcanlogin AND NOT protected_owner.rolsuper AND NOT protected_owner.rolcreatedb
   AND NOT protected_owner.rolcreaterole AND NOT protected_owner.rolreplication AND NOT protected_owner.rolbypassrls)
 FROM closure_scope)
 AND NOT EXISTS(SELECT 1 FROM runtime_roles r CROSS JOIN closure_scope s
   WHERE {_NATIVE_RUNTIME_ROLE_UNSAFE_SQL})
 AND NOT EXISTS(SELECT 1 FROM native_acl a WHERE a.is_grantable OR a.privilege_type<>a.allowed
   OR NOT EXISTS(SELECT 1 FROM runtime_roles r WHERE r.oid=a.grantee))
 AND NOT EXISTS(SELECT 1 FROM runtime_roles r CROSS JOIN closure_scope s
   WHERE NOT has_schema_privilege(r.oid,s.schema_oid,'USAGE') OR has_schema_privilege(r.oid,s.schema_oid,'CREATE')
   OR EXISTS(SELECT 1 FROM pg_class c WHERE c.relnamespace=s.schema_oid AND c.relkind='r'
     AND (s.relation_names IS NULL OR c.relname=ANY(s.relation_names))
     AND (NOT has_table_privilege(r.oid,c.oid,'SELECT')
       OR has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
       OR has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE,REFERENCES')))) AS closed
"""


def _role_names(owner_role: str, loader_roles: tuple[str, ...], reader_roles: tuple[str, ...]) -> dict:
    _identifier(owner_role)
    for role_names in (loader_roles, reader_roles):
        if type(role_names) is not tuple or len(role_names) > 32:
            raise NetworkWriterClosureError("Reader and loader roles must be bounded tuples")
        for role_name in role_names:
            _identifier(role_name)
    configured = sorted(set(loader_roles + reader_roles))
    if owner_role in configured:
        raise NetworkWriterClosureError("Protected owner cannot be a runtime role")
    return {
        "owner_role": owner_role,
        "loader_roles": sorted(set(loader_roles)),
        "reader_roles": sorted(set(reader_roles)),
    }


async def _check_roles(connection: Any, roles: dict) -> None:
    owner = await connection.fetchrow("SELECT * FROM pg_roles WHERE rolname=$1", roles["owner_role"])
    if not _protected_owner(owner):
        raise NetworkWriterClosureError("Owner must be an existing protected native role")
    runtime_roles = sorted(set(roles["loader_roles"] + roles["reader_roles"]))
    unsafe = await connection.fetchval(
        f"""SELECT EXISTS(SELECT 1 FROM unnest($1::text[]) configured(name)
        LEFT JOIN pg_roles r ON r.rolname=configured.name
        CROSS JOIN (SELECT $2::oid AS owner_oid) s WHERE {_NATIVE_RUNTIME_ROLE_UNSAFE_SQL})""",
        runtime_roles,
        owner["oid"],
    )
    if unsafe:
        raise NetworkWriterClosureError("Runtime role can bypass protected ownership")
    if not await connection.fetchval("SELECT pg_has_role(current_user,$1::name,'SET')", roles["owner_role"]):
        raise NetworkWriterClosureError("Publisher must be able to assume the protected owner")


async def _catalog(connection: Any, copy_target: MembershipCopyTarget) -> dict:
    namespace = await connection.fetchrow(
        "SELECT oid::bigint, nspowner::bigint FROM pg_namespace WHERE nspname=$1", copy_target.schema_name
    )
    if namespace is None:
        raise NetworkWriterClosureError("Registered candidate schema is absent")
    relations = await connection.fetch(
        """SELECT c.oid::bigint,c.relname,c.relkind,c.relpersistence,c.relowner::bigint,c.relacl,
        i.indrelid::bigint FROM pg_class c LEFT JOIN pg_index i ON i.indexrelid=c.oid
        WHERE c.relnamespace=$1::oid ORDER BY c.relname LIMIT 129""",
        namespace["oid"],
    )
    heap_oid_by_name = {relation["relname"]: relation["oid"] for relation in relations if relation["relkind"] == b"r"}
    required_tables = {"network_membership", "provider_location_binding"}
    if not required_tables <= heap_oid_by_name.keys() or heap_oid_by_name.keys() - required_tables - {
        "entity_address_unified"
    }:
        raise NetworkWriterClosureError("Candidate must contain only registered membership heaps")
    if len(relations) > 128 or any(
        relation["relkind"] not in (b"r", b"i") or relation["relpersistence"] != b"p" for relation in relations
    ):
        raise NetworkWriterClosureError("Candidate contains an unsupported native relation")
    if any(
        relation["relkind"] == b"i" and (relation["indrelid"] not in heap_oid_by_name.values() or relation["relacl"])
        for relation in relations
    ):
        raise NetworkWriterClosureError("Candidate index identity or privileges are invalid")
    if await connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace=$1::oid)", namespace["oid"]):
        raise NetworkWriterClosureError("Candidate must not contain stored routines")
    return {
        "namespace": dict(namespace),
        "relations": [dict(relation) for relation in relations],
        "heaps": heap_oid_by_name,
    }


def _receipt(target: MembershipCopyTarget, roles: dict, catalog: dict) -> dict:
    indexes = [row for row in catalog["relations"] if row["relkind"] == b"i"]
    return {
        "component": "network_candidate_writer_closure",
        "revision": 1,
        "scope": {
            name: getattr(target, name)
            for name in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        },
        **roles,
        "schema_oid": catalog["namespace"]["oid"],
        "relation_oids": dict(sorted(catalog["heaps"].items())),
        "index_oids": {row["relname"]: row["oid"] for row in indexes},
        "index_table_oids": {row["relname"]: row["indrelid"] for row in indexes},
    }


def _retained(candidate: dict) -> dict | None:
    validation = candidate["validation_json"]
    if isinstance(validation, str):
        validation = json.loads(validation)
    if validation is None:
        return None
    if type(validation) is not dict:
        raise NetworkWriterClosureError("Retained validation must be an object")
    retained = validation.get("writer_closure")
    if retained is not None and type(retained) is not dict:
        raise NetworkWriterClosureError("Retained writer closure is invalid")
    return retained


def _check_identity(current: dict, retained: dict | None, *, extending: bool) -> None:
    if retained is None:
        return
    if (
        retained.keys() != current.keys()
        or type(retained.get("revision")) is not int
        or type(retained.get("schema_oid")) is not int
    ):
        raise NetworkWriterClosureError("Retained writer closure shape is invalid")
    stable = ("component", "revision", "scope", "owner_role", "loader_roles", "reader_roles", "schema_oid")
    if any(current[key] != retained.get(key) for key in stable):
        raise NetworkWriterClosureError("Retained writer closure scope or owner changed")
    previous_heaps = retained.get("relation_oids")
    if type(previous_heaps) is not dict:
        raise NetworkWriterClosureError("Retained candidate heap identities are invalid")
    allow_growth = extending and set(previous_heaps) == {"network_membership", "provider_location_binding"}
    for key in ("relation_oids", "index_oids", "index_table_oids"):
        previous = retained.get(key)
        if (
            type(previous) is not dict
            or len(previous) > 128
            or any(type(oid) is not int or current[key].get(name) != oid for name, oid in previous.items())
        ):
            raise NetworkWriterClosureError("Retained candidate object identity changed")
        if not allow_growth and current[key] != previous:
            raise NetworkWriterClosureError("Closed candidate object set changed")


async def _lock_catalog(connection: Any, target: MembershipCopyTarget, roles: dict) -> dict:
    catalog = await _catalog(connection, target)
    for owner_oid in {catalog["namespace"]["nspowner"], *(row["relowner"] for row in catalog["relations"])}:
        if not await connection.fetchval(
            "SELECT pg_has_role(current_user,$1::oid,'USAGE') OR "
            "($1::oid=(SELECT oid FROM pg_roles WHERE rolname=$2) AND pg_has_role(current_user,$1::oid,'SET'))",
            owner_oid,
            roles["owner_role"],
        ):
            raise NetworkWriterClosureError("Publisher does not inherit current candidate ownership")
    publisher = await connection.fetchval("SELECT quote_ident(current_user)")
    for table_name in sorted(catalog["heaps"]):
        table_owner = next(row["relowner"] for row in catalog["relations"] if row["relname"] == table_name)
        inherited = await connection.fetchval("SELECT pg_has_role(current_user,$1::oid,'USAGE')", table_owner)
        if not inherited:
            await connection.execute(f"SET LOCAL ROLE {_identifier(roles['owner_role'])}")
        table = f"{_identifier(target.schema_name)}.{_identifier(table_name)}"
        await connection.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE NOWAIT")
        if not inherited:
            await connection.execute(f"SET LOCAL ROLE {publisher}")
    locked = await _catalog(connection, target)
    if catalog != locked:
        raise NetworkWriterClosureError("Candidate native catalog changed while acquiring locks")
    return locked


async def _acl_rows(connection: Any, catalog: dict) -> list:
    return await connection.fetch(
        """SELECT 'schema' AS kind,NULL::text AS relation,NULL::text AS column,
        a.grantee::bigint,a.privilege_type,a.is_grantable,
        CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END AS grantee_sql
        FROM pg_namespace n CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a
        WHERE n.oid=$1::oid
        UNION ALL SELECT 'table',c.relname,NULL,a.grantee::bigint,a.privilege_type,a.is_grantable,
        CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END
        FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
        WHERE c.relnamespace=$1::oid AND c.relkind='r'
        UNION ALL SELECT 'column',c.relname,quote_ident(att.attname),a.grantee::bigint,a.privilege_type,a.is_grantable,
        CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END
        FROM pg_class c JOIN pg_attribute att ON att.attrelid=c.oid
        CROSS JOIN LATERAL aclexplode(att.attacl) a
        WHERE c.relnamespace=$1::oid AND att.attnum>0 AND NOT att.attisdropped""",
        catalog["namespace"]["oid"],
    )


async def _default_acl_rows(connection: Any, catalog: dict) -> list:
    return await connection.fetch(
        """SELECT d.defaclrole::bigint AS creator,d.defaclobjtype,
        quote_ident(pg_get_userbyid(d.defaclrole)) AS creator_sql,
        a.grantee::bigint,a.privilege_type,a.is_grantable,
        CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END AS grantee_sql
        FROM pg_default_acl d CROSS JOIN LATERAL aclexplode(d.defaclacl) a
        WHERE d.defaclnamespace=$1::oid""",
        catalog["namespace"]["oid"],
    )


async def _remove_default_grants(connection: Any, namespace: str, catalog: dict) -> None:
    object_type_by_code = {b"r": "TABLES", b"S": "SEQUENCES", b"f": "FUNCTIONS", b"T": "TYPES"}
    seen_grants = set()
    for acl in await _default_acl_rows(connection, catalog):
        if acl["grantee"] == acl["creator"]:
            continue
        identity = (acl["creator_sql"], acl["defaclobjtype"], acl["grantee_sql"])
        if identity in seen_grants:
            continue
        seen_grants.add(identity)
        object_type = object_type_by_code.get(acl["defaclobjtype"])
        if object_type is None:
            raise NetworkWriterClosureError("Candidate has unsupported scoped default privileges")
        await connection.execute(
            f"ALTER DEFAULT PRIVILEGES FOR ROLE {acl['creator_sql']} IN SCHEMA {namespace} "
            f"REVOKE ALL ON {object_type} FROM {acl['grantee_sql']}"
        )


async def _check_closed(connection: Any, roles: dict, catalog: dict, *, retained: dict | None = None) -> None:
    owner_oid = await connection.fetchval("SELECT oid::bigint FROM pg_roles WHERE rolname=$1", roles["owner_role"])
    relations = catalog["relations"]
    if retained is not None:
        names = set(retained["relation_oids"]) | set(retained["index_oids"])
        relations = [row for row in relations if row["relname"] in names]
    if catalog["namespace"]["nspowner"] != owner_oid or any(row["relowner"] != owner_oid for row in relations):
        raise NetworkWriterClosureError("Candidate native ownership is not protected")
    runtime_names = sorted(set(roles["loader_roles"] + roles["reader_roles"]))
    relation_names = list(retained["relation_oids"]) if retained is not None else None
    is_closed = await connection.fetchval(
        "WITH closure_scope AS (SELECT $1::oid AS schema_oid,$2::oid AS owner_oid,"
        "$3::text[] AS role_names,$4::text[] AS relation_names)" + _NATIVE_WRITER_PRIVILEGES_SQL,
        catalog["namespace"]["oid"],
        owner_oid,
        runtime_names,
        relation_names,
    )
    if is_closed is not True:
        raise NetworkWriterClosureError("Candidate native ACLs or effective runtime roles are not closed")


async def _remove_grants(connection: Any, namespace: str, catalog: dict, owner_oid: int) -> None:
    seen_grants = set()
    for acl in await _acl_rows(connection, catalog):
        if acl["grantee"] == owner_oid:
            continue
        identity = (acl["kind"], acl["relation"], acl["column"], acl["grantee_sql"])
        if identity in seen_grants:
            continue
        seen_grants.add(identity)
        if acl["kind"] == "schema":
            statement = f"REVOKE ALL ON SCHEMA {namespace} FROM {acl['grantee_sql']} CASCADE"
        else:
            columns = f" ({acl['column']})" if acl["kind"] == "column" else ""
            statement = f"REVOKE ALL{columns} ON TABLE {namespace}.{_identifier(acl['relation'])} FROM {acl['grantee_sql']} CASCADE"
        await connection.execute(statement)


async def _transfer_and_grant(connection: Any, target: MembershipCopyTarget, roles: dict, catalog: dict) -> None:
    if not await connection.fetchval(
        "SELECT has_database_privilege(current_user,current_database(),'CREATE') "
        "AND has_database_privilege($1,current_database(),'CREATE')",
        roles["owner_role"],
    ):
        raise NetworkWriterClosureError("Publisher and protected owner require database CREATE for schema transfer")
    namespace = _identifier(target.schema_name)
    owner = _identifier(roles["owner_role"])
    await _remove_default_grants(connection, namespace, catalog)
    publisher = await connection.fetchval("SELECT quote_ident(current_user)")
    await connection.execute(f"ALTER SCHEMA {namespace} OWNER TO {owner}")
    await connection.execute(f"SET LOCAL ROLE {owner}")
    await connection.execute(f"GRANT USAGE ON SCHEMA {namespace} TO {publisher}")
    await connection.execute(f"SET LOCAL ROLE {publisher}")
    owner_oid = await connection.fetchval("SELECT oid::bigint FROM pg_roles WHERE rolname=$1", roles["owner_role"])
    for table_name in sorted(catalog["heaps"]):
        table_owner = next(row["relowner"] for row in catalog["relations"] if row["relname"] == table_name)
        if table_owner != owner_oid:
            await connection.execute(f"ALTER TABLE {namespace}.{_identifier(table_name)} OWNER TO {owner}")
    await connection.execute(f"SET LOCAL ROLE {owner}")
    await _remove_grants(connection, namespace, catalog, owner_oid)
    for role_name in sorted(set(roles["loader_roles"] + roles["reader_roles"])):
        await connection.execute(f"GRANT USAGE ON SCHEMA {namespace} TO {_identifier(role_name)}")
        for table_name in sorted(catalog["heaps"]):
            await connection.execute(
                f"GRANT SELECT ON TABLE {namespace}.{_identifier(table_name)} TO {_identifier(role_name)}"
            )
    await connection.execute(f"SET LOCAL ROLE {publisher}")


async def _candidate_context(
    connection: Any, target: MembershipCopyTarget, roles: dict, control_schema: str | None
) -> tuple:
    candidate = await _locked_candidate(connection, target, _control_namespace(control_schema))
    if candidate["state"] not in ("sealed", "validated", "ready", "published"):
        raise NetworkWriterClosureError("Writer closure requires a sealed candidate")
    await _check_roles(connection, roles)
    catalog = await _lock_catalog(connection, target, roles)
    if candidate["state"] in ("ready", "published") and "entity_address_unified" not in catalog["heaps"]:
        raise NetworkWriterClosureError("Ready candidate requires its projected address heap")
    return candidate, catalog


async def freeze_network_candidate_writers(
    connection: Any,
    copy_target: MembershipCopyTarget,
    *,
    owner_role: str,
    loader_roles: tuple[str, ...],
    reader_roles: tuple[str, ...],
    control_schema: str | None = None,
) -> dict:
    """Transfer only the locked candidate to its protected owner in the caller transaction."""
    _require_transaction(connection, copy_target)
    roles = _role_names(owner_role, loader_roles, reader_roles)
    async with connection.transaction():
        candidate, catalog = await _candidate_context(connection, copy_target, roles, control_schema)
        retained = _retained(candidate)
        current = _receipt(copy_target, roles, catalog)
        _check_identity(current, retained, extending=True)
        if retained is not None:
            await _check_closed(connection, roles, catalog, retained=retained)
            if current == retained:
                return current
        else:
            needs_transfer = False
            try:
                await _check_closed(connection, roles, catalog)
            except NetworkWriterClosureError:
                needs_transfer = True
            if not needs_transfer:
                return current
        await _transfer_and_grant(connection, copy_target, roles, catalog)
        closed = await _catalog(connection, copy_target)
        if _receipt(copy_target, roles, closed) != current:
            raise NetworkWriterClosureError("Ownership transfer changed native object identity")
        await _check_closed(connection, roles, closed)
        return current


async def verify_network_candidate_writer_closure(
    connection: Any,
    copy_target: MembershipCopyTarget,
    *,
    owner_role: str,
    loader_roles: tuple[str, ...],
    reader_roles: tuple[str, ...],
    control_schema: str | None = None,
) -> dict:
    """Verify retained identity and current native privileges without changing candidate DDL."""
    _require_transaction(connection, copy_target)
    roles = _role_names(owner_role, loader_roles, reader_roles)
    async with connection.transaction():
        candidate, catalog = await _candidate_context(connection, copy_target, roles, control_schema)
        retained = _retained(candidate)
        if retained is None:
            raise NetworkWriterClosureError("Candidate has no retained writer closure")
        current = _receipt(copy_target, roles, catalog)
        _check_identity(current, retained, extending=False)
        await _check_closed(connection, roles, catalog)
        return current
