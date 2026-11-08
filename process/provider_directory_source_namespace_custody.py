# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One operator-owned CREATE grant on an existing, separately owned namespace."""

import asyncio
import json
from copy import deepcopy

from db.plan_release_storage import storage_identifier
from process.plan_release_storage_custody import storage_roles
from process.provider_directory_generic_source_policy import GenericSourcePolicy

_NAMESPACE_SQL = """SELECT jsonb_build_object(
 'namespace',to_jsonb(n),
 'acl',COALESCE((SELECT jsonb_agg(jsonb_build_array(a.grantee::bigint,a.grantor::bigint,
 a.privilege_type,a.is_grantable) ORDER BY a.grantee,a.grantor,a.privilege_type,a.is_grantable)
 FROM aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a),'[]'::jsonb),
 'items',COALESCE((SELECT jsonb_agg(jsonb_build_object('value',item::text,
 'grantee',(SELECT grantee::bigint FROM aclexplode(ARRAY[item]) LIMIT 1),
 'grantor',(SELECT grantor::bigint FROM aclexplode(ARRAY[item]) LIMIT 1)) ORDER BY position)
 FROM unnest(COALESCE(n.nspacl,acldefault('n',n.nspowner))) WITH ORDINALITY a(item,position)),'[]'::jsonb),
 'create_item',makeaclitem($2::regrole,n.nspowner,CASE WHEN EXISTS(
 SELECT FROM aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a
 WHERE a.grantee=$2::regrole AND a.grantor=n.nspowner AND a.privilege_type='USAGE')
 THEN 'USAGE,CREATE' ELSE 'CREATE' END,false)::text,
 'role_graph',jsonb_build_object(
 'roles',(SELECT jsonb_agg(to_jsonb(r)-'rolpassword' ORDER BY oid) FROM pg_roles r),
 'memberships',(SELECT jsonb_agg(to_jsonb(m) ORDER BY roleid,member) FROM pg_auth_members m)),
 'access',(SELECT jsonb_agg(jsonb_build_object('oid',r.oid::bigint,'superuser',r.rolsuper,
 'usage',has_schema_privilege(r.oid,n.oid,'USAGE'),'create',has_schema_privilege(r.oid,n.oid,'CREATE'),
 'owner_usage',pg_has_role(r.oid,$2::regrole,'USAGE'),'owner_set',pg_has_role(r.oid,$2::regrole,'SET'),
 'namespace_member',pg_has_role(r.oid,n.nspowner,'MEMBER'),
 'namespace_set',pg_has_role(r.oid,n.nspowner,'SET'),
 'database_create',has_database_privilege(r.oid,current_database(),'CREATE'),
 'ordinary',EXISTS(SELECT FROM pg_roles ordinary WHERE ordinary.rolname=ANY($3::text[])
 AND pg_has_role(r.oid,ordinary.oid,'MEMBER'))) ORDER BY r.oid) FROM pg_roles r))
 FROM pg_namespace n WHERE n.nspname=$1"""


def _require(condition, code):
    if not condition:
        raise ValueError("source_namespace_" + code)


def _decoded(value):
    return json.loads(value) if type(value) is str else value


async def capture_source_namespace_custody(connection, policy, *, namespace_owner_role):
    """Capture truthful native rights; a catalog observation never enrolls itself."""
    _require(type(policy) is GenericSourcePolicy, "policy_required")
    storage_identifier(namespace_owner_role)
    _require(
        namespace_owner_role not in (policy.owner_role, policy.publisher_role, *policy.runtime_roles),
        "namespace_owner_overlap",
    )
    owner, _ = await storage_roles(connection, policy.schema, **policy.role_arguments())
    captured = _decoded(
        await connection.fetchval(_NAMESPACE_SQL, policy.schema, policy.owner_role, list(policy.runtime_roles))
    )
    _require(captured is not None, "missing")
    role_by_name = {role["rolname"]: role for role in captured["role_graph"]["roles"]}
    namespace_owner = role_by_name.get(namespace_owner_role)
    _require(
        namespace_owner is not None
        and not any(
            namespace_owner[flag]
            for flag in ("rolcanlogin", "rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")
        )
        and int(captured["namespace"]["nspowner"]) == int(namespace_owner["oid"]),
        "owner_changed",
    )
    namespace_owner_oid = int(namespace_owner["oid"])
    publisher = int(role_by_name[policy.publisher_role]["oid"])
    access_by_oid = {role_access["oid"]: role_access for role_access in captured["access"]}
    _require(
        access_by_oid[owner]["usage"]
        and not access_by_oid[owner]["namespace_member"]
        and not access_by_oid[owner]["namespace_set"],
        "owner_authority_changed",
    )
    _require(
        all(
            role_access["superuser"]
            or not role_access["ordinary"]
            or role_access["oid"] == publisher
            or not (role_access["owner_usage"] or role_access["owner_set"])
            for role_access in captured["access"]
        ),
        "ordinary_owner_authority",
    )
    _require(
        all(
            grantor == namespace_owner_oid and not grantable and privilege in {"USAGE", "CREATE"}
            for grantee, grantor, privilege, grantable in captured["acl"]
            if grantee == owner
        ),
        "owner_acl_changed",
    )
    return dict(owner_oid=owner, namespace_owner_oid=namespace_owner_oid, **captured)


def _expected_namespace(before):
    expected = deepcopy(before)
    owner, grantor = before["owner_oid"], before["namespace_owner_oid"]
    acl_item_by_field = {"value": before["create_item"], "grantee": owner, "grantor": grantor}
    for position, previous in enumerate(expected["items"]):
        if previous["grantee"] == owner and previous["grantor"] == grantor:
            expected["items"][position] = acl_item_by_field
            break
    else:
        expected["items"].append(acl_item_by_field)
    expected["namespace"]["nspacl"] = [entry["value"] for entry in expected["items"]]
    expected["acl"] = sorted({tuple(entry) for entry in before["acl"]} | {(owner, grantor, "CREATE", False)})
    expected["acl"] = [list(entry) for entry in expected["acl"]]
    for access in expected["access"]:
        access["create"] = access["create"] or access["owner_usage"]
    return expected


def require_source_namespace_adoption_delta(before, after):
    """The complete native delta is one non-grantable CREATE and its inherited use."""
    _require(_expected_namespace(before) == after, "adoption_changed")


async def _admin_identity(connection, policy, namespace_owner_role):
    actor_by_field = dict(
        await connection.fetchrow(
            "SELECT session_user::text AS session,current_user::text AS current,pg_backend_pid() AS backend,"
            "pg_current_xact_id()::text AS xid,(SELECT rolsuper FROM pg_roles WHERE rolname=current_user) AS native_admin"
        )
    )
    _require(
        actor_by_field["native_admin"] is True
        and actor_by_field["session"] == actor_by_field["current"]
        and actor_by_field["current"]
        not in (namespace_owner_role, policy.owner_role, policy.publisher_role, *policy.runtime_roles),
        "independent_admin_required",
    )
    return actor_by_field


async def provision_source_namespace_custody(connection, policy, *, namespace_owner_role, verify_role_profile):
    """Grant only in the supplied native admin transaction and full-profile savepoint."""
    _require(
        type(policy) is GenericSourcePolicy
        and connection.is_in_transaction() is True
        and callable(verify_role_profile),
        "explicit_admin_required",
    )
    task, actor = asyncio.current_task(), await _admin_identity(connection, policy, namespace_owner_role)
    async with connection.transaction():
        await verify_role_profile(connection, policy.schema, **policy.role_arguments())
        before = await capture_source_namespace_custody(connection, policy, namespace_owner_role=namespace_owner_role)
        if not any(
            grantee == before["owner_oid"] and privilege == "CREATE" for grantee, _, privilege, _ in before["acl"]
        ):
            await connection.execute(
                f"GRANT CREATE ON SCHEMA {storage_identifier(policy.schema)} TO {storage_identifier(policy.owner_role)}"
            )
        after = await capture_source_namespace_custody(connection, policy, namespace_owner_role=namespace_owner_role)
        require_source_namespace_adoption_delta(before, after)
        await verify_role_profile(connection, policy.schema, **policy.role_arguments())
        _require(
            asyncio.current_task() is task and await _admin_identity(connection, policy, namespace_owner_role) == actor,
            "admin_transaction_changed",
        )
        return after
