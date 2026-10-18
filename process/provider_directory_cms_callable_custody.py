# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protect existing CMS callable bodies; table-hook lifetime remains a separate cut gate."""

import asyncio
import json
from copy import deepcopy
from hashlib import sha256

from db.plan_release_storage import storage_identifier, storage_relation
from process.plan_release_storage_custody import storage_roles
from process.provider_directory_generic_source_policy import GenericSourcePolicy

# Exact prosrc contracts from the receipt migration, shared builder and forward argument repair.
_CONTRACTS = {
    "cms_serving_address_transition": (
        (),
        "trigger",
        "plpgsql",
        "v",
        "afb90c15e4a45c3022403891afaaf85b4cbc81bd759a98cbdb1d227bba8ebef7",
    ),
    "cms_serving_archive_result_matches": (
        (("p", "jsonb"),),
        "boolean",
        "plpgsql",
        "s",
        "38426c90255c28e871039fe2aa9e844758cdb67de6ab8e2199ef04912ef06b66",
    ),
    "cms_serving_current_receipt_matches": (
        (("p", "jsonb"),),
        "boolean",
        "sql",
        "s",
        "dc6a70bf64fd378e2a83a7caf180e856490c5a66676335be6fe8a1df1860f756",
    ),
    "cms_serving_dataset_transition": (
        (),
        "trigger",
        "plpgsql",
        "v",
        "64da00faca333a3e97821a2bceeaaf6900618f22a8119de54b08f78109367522",
    ),
    "cms_serving_doctors_transition": (
        (),
        "trigger",
        "plpgsql",
        "v",
        "ee37d843ebe88cf5433990eabb8f742200eaf3cdf2333cceaf22672f7d4f6fd0",
    ),
    "cms_serving_native_matches": (
        (("p", "jsonb"),),
        "boolean",
        "sql",
        "s",
        "059b4c093c0c089ce9d5b43bdfa67111586f33d24072517fe57b37f2ea1fb287",
    ),
    "cms_serving_native_snapshot": (
        (),
        "jsonb",
        "sql",
        "s",
        "91306c115e0835db8ece5ed05b21f48d021735d464d64a70f4587d6c9fc77f4e",
    ),
    "cms_serving_no_truncate": (
        (),
        "trigger",
        "plpgsql",
        "v",
        "f4f8f7df3cb932032a88345290d71f39cbe9720324b60d7699cec8ccd32612e3",
    ),
    "cms_serving_oids": (
        (("names", "text[]"),),
        "jsonb",
        "sql",
        "s",
        "90a3b2c46612c899a8695c8196de49fa4ca90e3ac4806e243302a167dc2532d7",
    ),
    "cms_serving_profile_transition": (
        (),
        "trigger",
        "plpgsql",
        "v",
        "a8af12e1bb98c589ae88fd3e622c69b5b3b4e853875fb16ff0b57020f8fb6539",
    ),
    "cms_serving_receipt_immutable": (
        (),
        "trigger",
        "plpgsql",
        "v",
        "6eac5c7ed6443bf8f45d3414170ce849bf51d6726f64039bc5f31ef972e63727",
    ),
    "cms_serving_receipt_insert": (
        (),
        "trigger",
        "plpgsql",
        "v",
        "4ce374dcc2f22282e5996e3693248e74b4c9c6a4c740448b91cc6296ce00beb3",
    ),
    "cms_serving_receipt_matches": (
        (("p", "jsonb"),),
        "boolean",
        "plpgsql",
        "s",
        "a7e3da06af9af8c047bb262f27cb94e6cd776f40ff05087b24167ddb35b20ec9",
    ),
    "cms_serving_require_fresh": (
        (("kind", "text"), ("previous", "jsonb")),
        "void",
        "plpgsql",
        "v",
        "7a1769065c5da4ac2fbd9c04e8f846e3093be25baa078f5c3e2da56ed60b8469",
    ),
    "cms_serving_snapshot_receipt_matches": (
        (("p", "jsonb"),),
        "boolean",
        "sql",
        "s",
        "b2f575c615b7e076fb8628e3f9ba7cab0411ac7ca6c14fb310a6724441860d3c",
    ),
    "cms_serving_source_transition": (
        (),
        "trigger",
        "plpgsql",
        "v",
        "a54cf5a5ba64a12d78832923b8482bd2fc266058f2b80d2438b861ad96b43de6",
    ),
}
CMS_CALLABLES = tuple(_CONTRACTS)
_TYPE_OIDS = {"text[]": 1009, "text": 25, "jsonb": 3802, "boolean": 16, "trigger": 2279, "void": 2278}
_FUNCTION_SQL = """SELECT signature,to_jsonb(p) AS definition,l.lanname AS language,
 COALESCE((SELECT jsonb_agg(jsonb_build_array(a.grantee::bigint,a.grantor::bigint,
 a.privilege_type,a.is_grantable) ORDER BY a.grantee,a.grantor,a.privilege_type,a.is_grantable)
 FROM aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a),'[]'::jsonb) AS acl,
 (SELECT jsonb_agg(jsonb_build_array(r.oid::bigint,r.rolsuper,
 EXISTS(SELECT FROM pg_roles ordinary WHERE ordinary.rolname=ANY($4::text[])
 AND pg_has_role(r.oid,ordinary.oid,'MEMBER')),pg_has_role(r.oid,p.proowner,'MEMBER'),pg_has_role(r.oid,p.proowner,'SET'),
 has_function_privilege(r.oid,p.oid,'EXECUTE')) ORDER BY r.oid) FROM pg_roles r) AS access
 FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
 LEFT JOIN unnest($1::text[]) f(signature) ON p.oid=to_regprocedure(signature)
 WHERE p.pronamespace=to_regnamespace($2) AND p.proname=ANY($3::text[]) ORDER BY p.oid"""
_ROLE_GRAPH_SQL = """SELECT jsonb_build_object(
 'roles',(SELECT jsonb_agg(to_jsonb(r)-'rolpassword' ORDER BY oid) FROM pg_roles r),
 'memberships',(SELECT jsonb_agg(to_jsonb(m) ORDER BY roleid,member) FROM pg_auth_members m))"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_callable_" + code)


def _decoded(value):
    return json.loads(value) if type(value) is str else value


def _body_digest(body, schema):
    canonical = body.replace('"' + schema + '"', '"__schema__"').replace("'" + schema + "'", "'__schema__'")
    return sha256(canonical.encode("utf-8")).hexdigest()


def _signatures(schema):
    return tuple(
        storage_relation(schema, name) + "(" + ",".join(kind for _, kind in spec[0]) + ")"
        for name, spec in _CONTRACTS.items()
    )


def _require_definition(entry, schema, namespace):
    definition = entry["definition"]
    spec = _CONTRACTS.get(definition["proname"])
    _require(spec is not None, "function_set_changed")
    arguments, returns, language, volatility, body_hash = spec
    expected_arguments = [str(_TYPE_OIDS[kind]) for _, kind in arguments]
    _require(
        int(definition["pronamespace"]) == namespace
        and definition["prokind"] == "f"
        and entry["language"] == language
        and int(definition["prorettype"]) == _TYPE_OIDS[returns]
        and int(definition["pronargs"]) == len(arguments)
        and definition["proargtypes"] == expected_arguments
        and definition["proargnames"] == ([name for name, _ in arguments] or None)
        and definition["proargmodes"] is None
        and definition["proallargtypes"] is None
        and definition["proargdefaults"] is None
        and int(definition["pronargdefaults"]) == int(definition["provariadic"]) == 0
        and definition["prosecdef"] is False
        and definition["proleakproof"] is False
        and definition["proisstrict"] is False
        and definition["proretset"] is False
        and definition["prosupport"] == "-"
        and definition["probin"] is None
        and definition["prosqlbody"] is None
        and definition["protrftypes"] is None
        and definition["procost"] == 100
        and definition["prorows"] == 0
        and definition["provolatile"] == volatility
        and definition["proparallel"] == "u"
        and definition["proconfig"] == ["search_path=pg_catalog"]
        and _body_digest(definition["prosrc"], schema) == body_hash,
        "definition_changed",
    )
    owner = int(definition["proowner"])
    _require(
        sorted(entry["acl"]) == sorted([[0, owner, "EXECUTE", False], [owner, owner, "EXECUTE", False]]),
        "acl_changed",
    )


async def capture_cms_callable_custody(connection, policy, *, legacy=False):
    """Attest only the fixed existing callables, never infer authority from their names."""
    _require(type(policy) is GenericSourcePolicy and type(legacy) is bool, "policy_required")
    owner, _ = await storage_roles(connection, policy.schema, **policy.role_arguments())
    graph = _decoded(await connection.fetchval(_ROLE_GRAPH_SQL))
    role_oid_by_name = {role["rolname"]: int(role["oid"]) for role in graph["roles"]}
    publisher = role_oid_by_name[policy.publisher_role]
    allowed_owners = {owner, *(role_oid_by_name[name] for name in policy.control_runtime_roles)} if legacy else {owner}
    namespace = await connection.fetchval("SELECT to_regnamespace($1)::oid::bigint", policy.schema)
    signatures = _signatures(policy.schema)
    function_rows = await connection.fetch(
        _FUNCTION_SQL, list(signatures), policy.schema, list(CMS_CALLABLES), list(policy.runtime_roles)
    )
    _require(
        len(function_rows) == len(signatures)
        and {function_row["signature"] for function_row in function_rows} == set(signatures),
        "function_set_changed",
    )
    function_by_name = {}
    for native_row in function_rows:
        function_by_field = {
            key: _decoded(native_row[key]) if key != "language" and key != "signature" else native_row[key]
            for key in native_row.keys()
        }
        _require_definition(function_by_field, policy.schema, namespace)
        _require(int(function_by_field["definition"]["proowner"]) in allowed_owners, "owner_changed")
        if not legacy:
            _require(
                all(
                    superuser or not ordinary or role in {owner, publisher} or not (member or can_set)
                    for role, superuser, ordinary, member, can_set, _ in function_by_field["access"]
                ),
                "replacement_authority",
            )
        function_by_name[function_by_field["definition"]["proname"]] = function_by_field
    return {"schema_oid": namespace, "owner_oid": owner, "role_graph": graph, "functions": function_by_name}


def require_cms_callable_adoption_delta(before, after):
    """Allow only owner and its native default-ACL rewrite, retaining every OID/body/right."""
    _require(
        before["schema_oid"] == after["schema_oid"]
        and before["owner_oid"] == after["owner_oid"]
        and before["role_graph"] == after["role_graph"]
        and before["functions"].keys() == after["functions"].keys() == _CONTRACTS.keys(),
        "adoption_scope_changed",
    )
    owner = after["owner_oid"]
    for name, previous in before["functions"].items():
        current = after["functions"][name]
        expected = deepcopy(previous["definition"])
        expected["proowner"] = str(owner) if type(expected["proowner"]) is str else owner
        # ALTER OWNER rewrites explicit owner ACL entries; NULL stays NULL.
        if expected["proacl"] is None:
            _require(current["definition"]["proacl"] is None, "adoption_acl_changed")
        else:
            expected["proacl"] = current["definition"]["proacl"]
        _require(
            expected == current["definition"] and previous["signature"] == current["signature"], "adoption_changed"
        )
        prior_acl_owner = int(previous["definition"]["proowner"])
        expected_acl_rows = [
            [
                owner if grantee == prior_acl_owner else grantee,
                owner if grantor == prior_acl_owner else grantor,
                privilege,
                grantable,
            ]
            for grantee, grantor, privilege, grantable in previous["acl"]
        ]
        _require(sorted(expected_acl_rows) == sorted(current["acl"]), "adoption_acl_changed")
        previous_execute_by_role = {access[0]: access[-1] for access in previous["access"] if access[0] != owner}
        current_execute_by_role = {access[0]: access[-1] for access in current["access"] if access[0] != owner}
        _require(previous_execute_by_role == current_execute_by_role, "ordinary_execute_changed")


async def _admin_identity(connection, policy):
    actor_by_field = dict(
        await connection.fetchrow(
            "SELECT session_user::text AS session,current_user::text AS current,pg_backend_pid() AS backend,"
            "pg_current_xact_id()::text AS xid,(SELECT rolsuper FROM pg_roles WHERE rolname=current_user) AS native_admin"
        )
    )
    _require(
        actor_by_field["native_admin"] is True
        and actor_by_field["session"] == actor_by_field["current"]
        and actor_by_field["current"] not in (policy.owner_role, policy.publisher_role, *policy.runtime_roles),
        "independent_admin_required",
    )
    return actor_by_field


async def provision_cms_callable_custody(connection, policy, *, verify_role_profile):
    """Use the supplied admin transaction/savepoint; this never activates final publication."""
    _require(
        type(policy) is GenericSourcePolicy
        and connection.is_in_transaction() is True
        and callable(verify_role_profile),
        "explicit_admin_required",
    )
    task, actor = asyncio.current_task(), await _admin_identity(connection, policy)
    async with connection.transaction():
        await verify_role_profile(connection, policy.schema, **policy.role_arguments())
        before = await capture_cms_callable_custody(connection, policy, legacy=True)
        for name, entry in sorted(before["functions"].items(), key=lambda item: int(item[1]["definition"]["oid"])):
            if int(entry["definition"]["proowner"]) != before["owner_oid"]:
                await connection.execute(
                    f"ALTER FUNCTION {entry['signature']} OWNER TO {storage_identifier(policy.owner_role)}"
                )
        after = await capture_cms_callable_custody(connection, policy)
        require_cms_callable_adoption_delta(before, after)
        await verify_role_profile(connection, policy.schema, **policy.role_arguments())
        _require(
            asyncio.current_task() is task and await _admin_identity(connection, policy) == actor,
            "admin_transaction_changed",
        )
        return after
