# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Custody of five existing initial-Profile callables; table hooks remain separate."""

import asyncio
from copy import deepcopy

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_profile_initial_guards as guards
from process.plan_release_storage_custody import storage_roles
from process.provider_directory_cms_callable_custody import _FUNCTION_SQL, _ROLE_GRAPH_SQL, _admin_identity, _decoded
from process.provider_directory_generic_source_policy import GenericSourcePolicy

INITIAL_CALLABLES = (
    "pd_profile_initial_receipt_write_guard",
    "pd_profile_initial_receipt_truncate_guard",
    "pd_profile_initial_receipt_insert",
    "pd_profile_initial_serving_insert",
    "pd_profile_initial_receipt_matches",
)
_RELATIONS = ("provider_directory_profile_initial_receipt", "provider_directory_profile_serving_generation")
_OWNER_USAGE_SQL = """SELECT COALESCE(jsonb_agg(jsonb_build_array(r.oid::bigint,
 pg_catalog.pg_has_role(r.oid,$1::oid,'USAGE')) ORDER BY r.oid),'[]'::jsonb) FROM pg_catalog.pg_roles r"""
_RELATION_SQL = """SELECT c.relname,to_jsonb(c) AS relation,to_jsonb(t) AS composite,
 COALESCE((SELECT jsonb_agg(to_jsonb(a) ORDER BY a.attnum) FROM pg_attribute a WHERE a.attrelid=c.oid),'[]'::jsonb) AS attributes,
 COALESCE((SELECT jsonb_agg(to_jsonb(d) ORDER BY d.oid) FROM pg_attrdef d WHERE d.adrelid=c.oid),'[]'::jsonb) AS defaults,
 COALESCE((SELECT jsonb_agg(to_jsonb(i) ORDER BY i.indexrelid) FROM pg_index i WHERE i.indrelid=c.oid),'[]'::jsonb) AS indexes,
 COALESCE((SELECT jsonb_agg(to_jsonb(k) ORDER BY k.oid) FROM pg_constraint k WHERE k.conrelid=c.oid),'[]'::jsonb) AS constraints,
 COALESCE((SELECT jsonb_agg(to_jsonb(g)||jsonb_build_object('columns',g.tgattr::text,
 'arguments',encode(g.tgargs,'hex'),'no_condition',g.tgqual IS NULL) ORDER BY g.oid)
 FROM pg_trigger g WHERE g.tgrelid=c.oid),'[]'::jsonb) AS triggers,
 EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS inherited
 FROM pg_class c JOIN pg_type t ON t.oid=c.reltype
 WHERE c.relnamespace=to_regnamespace($1) AND c.relname=ANY($2::text[]) ORDER BY c.relname"""


def _require(condition, code):
    if not condition:
        raise ValueError("profile_initial_callable_" + code)


def _specifications(schema, composite_oid, *, receipt_version=1):
    bodies = (
        guards.IMMUTABLE_BODY,
        guards.IMMUTABLE_BODY,
        guards._receipt_insert_body(schema),
        guards._serving_insert_body(schema),
        guards._receipt_matches_sql(schema)
        if receipt_version == 1
        else guards._reactivated_receipt_matches_sql(schema),
    )
    return {
        name: dict(
            arguments=[str(composite_oid)] if index == 4 else [],
            argument_names=["receipt"] if index == 4 else None,
            returns="16" if index == 4 else "2279",
            language="sql" if index == 4 else "plpgsql",
            body=" " + body + " ",
        )
        for index, (name, body) in enumerate(zip(INITIAL_CALLABLES, bodies, strict=True))
    }


def _signatures(schema):
    receipt = storage_relation(schema, _RELATIONS[0])
    return tuple(
        storage_relation(schema, name) + "(" + (receipt if index == 4 else "") + ")"
        for index, name in enumerate(INITIAL_CALLABLES)
    )


def _require_definition(entry, specification, namespace):
    definition = entry["definition"]
    _require(
        int(definition["pronamespace"]) == namespace
        and definition["prokind"] == "f"
        and entry["language"] == specification["language"]
        and str(definition["prorettype"]) == specification["returns"]
        and definition["pronargs"] == len(specification["arguments"])
        and definition["proargtypes"] == specification["arguments"]
        and definition["proargnames"] == specification["argument_names"]
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
        and definition["provolatile"] == "v"
        and definition["proparallel"] == "u"
        and definition["proconfig"] == ["search_path=pg_catalog"]
        and definition["prosrc"] == specification["body"],
        "definition_changed",
    )


def _require_acl(entry, owner, controls):
    current = int(entry["definition"]["proowner"])
    expected_acl_rows = [[current, current, "EXECUTE", False]]
    if entry["definition"]["proname"] == INITIAL_CALLABLES[-1]:
        expected_acl_rows.append([0, current, "EXECUTE", False])
    ordinary_grants = [item for item in entry["acl"] if item not in expected_acl_rows]
    _require(
        all(item == [item[0], owner, "EXECUTE", False] and item[0] in controls for item in ordinary_grants)
        and (not ordinary_grants or current == owner)
        and len(ordinary_grants) <= 1
        and sorted(entry["acl"]) == sorted(expected_acl_rows + ordinary_grants),
        "acl_changed",
    )


async def _capture_relations(connection, schema):
    rows = await connection.fetch(_RELATION_SQL, schema, list(_RELATIONS))
    _require(len(rows) == 2 and {row["relname"] for row in rows} == set(_RELATIONS), "relations_changed")
    relation_by_name = {}
    for row in rows:
        native_by_field = {key: _decoded(row[key]) if key != "relname" else row[key] for key in row.keys()}
        relation, composite = native_by_field["relation"], native_by_field["composite"]
        _require(
            relation["relkind"] == "r"
            and relation["relpersistence"] == "p"
            and not relation["relrowsecurity"]
            and not relation["relforcerowsecurity"]
            and not native_by_field["inherited"]
            and composite["typtype"] == "c"
            and str(composite["typrelid"]) == str(relation["oid"])
            and str(composite["typnamespace"]) == str(relation["relnamespace"]),
            "relations_changed",
        )
        relation_by_name[row["relname"]] = native_by_field
    return relation_by_name


async def _receipt_catalog_version(connection, schema):
    checks = await connection.fetch(guards._RECEIPT_CHECKS_SQL.replace(":schema", "$1"), schema)
    try:
        defaults = {entry["key_default"] for entry in checks}
        return guards.receipt_catalog_version(
            [entry["definition"] for entry in checks], next(iter(defaults)) if len(defaults) == 1 else None
        )
    except RuntimeError:
        raise ValueError("profile_initial_callable_receipt_catalog_changed") from None


async def _capture_receipt_rows(connection, schema):
    """Hash immutable history in the database without transporting or accumulating its payloads."""
    statement = """WITH rows AS MATERIALIZED (
        SELECT build_id,to_jsonb(r) AS body,row_number() OVER (ORDER BY build_id) AS ordinal FROM __RELATION__ r),
        sized AS (SELECT count(*)::bigint AS count,COALESCE(sum(octet_length(body::text)),0)::bigint AS bytes,
          COALESCE(max(octet_length(body::text)),0)::bigint AS largest_bytes,
          COALESCE(jsonb_agg(body) FILTER (WHERE ordinal=1),'[]'::jsonb) AS first_payload,
          string_agg(encode(sha256(convert_to(body::text,'UTF8')),'hex'),'' ORDER BY build_id) AS hashes FROM rows)
        SELECT count,bytes,largest_bytes,CASE WHEN largest_bytes<=16777216 THEN
          encode(sha256(convert_to(CASE WHEN count<=1 THEN first_payload::text ELSE
            'provider-directory-profile-initial-history.v1:'||hashes END,'UTF8')),'hex') END AS sha256 FROM sized"""
    observed_by_field = dict(
        await connection.fetchrow(statement.replace("__RELATION__", storage_relation(schema, _RELATIONS[0])))
    )
    _require(
        observed_by_field.pop("largest_bytes") <= 16_777_216 and observed_by_field["sha256"] is not None,
        "rows_unbounded",
    )
    return observed_by_field


async def _capture_rows(connection, schema):
    data_by_name = {}
    for name in _RELATIONS:
        if name == _RELATIONS[0]:
            data_by_name[name] = await _capture_receipt_rows(connection, schema)
            continue
        # The serving head stays a native singleton; never transport its payload.
        statement = """WITH rows AS MATERIALIZED (SELECT to_jsonb(r) AS body FROM __RELATION__ r LIMIT 2),
            sized AS (SELECT count(*)::integer AS count,COALESCE(sum(octet_length(body::text)),0)::bigint AS bytes,
            COALESCE(jsonb_agg(body),'[]'::jsonb) AS payload FROM rows)
            SELECT count,bytes,CASE WHEN count<=1 AND bytes<=16777216 THEN
            encode(sha256(convert_to(payload::text,'UTF8')),'hex') END AS sha256 FROM sized"""
        data_by_field = dict(
            await connection.fetchrow(statement.replace("__RELATION__", storage_relation(schema, name)))
        )
        _require(
            data_by_field["count"] <= 1
            and data_by_field["bytes"] <= 16_777_216
            and data_by_field["sha256"] is not None,
            "rows_unbounded",
        )
        data_by_name[name] = data_by_field
    return data_by_name


def _require_trigger_links(relations, functions):
    expected_by_root = {
        _RELATIONS[0]: {
            INITIAL_CALLABLES[0]: (26, False),
            INITIAL_CALLABLES[1]: (34, False),
            INITIAL_CALLABLES[2]: (5, True),
        },
        _RELATIONS[1]: {INITIAL_CALLABLES[3]: (21, True)},
    }
    for root, expected_by_name in expected_by_root.items():
        links_by_name = {
            trigger["tgname"]: trigger
            for trigger in relations[root]["triggers"]
            if trigger["tgname"].startswith("pd_profile_initial_")
        }
        _require(links_by_name.keys() == expected_by_name.keys(), "trigger_links_changed")
        for name, (mask, deferred) in expected_by_name.items():
            trigger = links_by_name[name]
            _require(
                str(trigger["tgfoid"]) == str(functions[name]["definition"]["oid"])
                and str(trigger["tgrelid"]) == str(relations[root]["relation"]["oid"])
                and trigger["tgtype"] == mask
                and trigger["tgenabled"] == "A"
                and trigger["tgdeferrable"] is deferred
                and trigger["tginitdeferred"] is deferred
                and trigger["tgisinternal"] is False
                and trigger["tgnargs"] == 0
                and trigger["columns"] == trigger["arguments"] == ""
                and trigger["no_condition"] is True
                and trigger["tgoldtable"] is None
                and trigger["tgnewtable"] is None,
                "trigger_links_changed",
            )


def _require_function_access(entry, owner, role_by_name, policy, legacy):
    controls = {role_by_name[name] for name in policy.control_runtime_roles}
    _require_acl(entry, owner, controls)
    _require(int(entry["definition"]["proowner"]) in ({owner, *controls} if legacy else {owner}), "owner_changed")
    if not legacy:
        _require(
            all(
                superuser
                or not ordinary
                or role in {owner, role_by_name[policy.publisher_role]}
                or not (member or can_set)
                for role, superuser, ordinary, member, can_set, _ in entry["access"]
            ),
            "replacement_authority",
        )


def _role_rows(rows, role_ids, width):
    _require(
        type(rows) is list
        and all(
            type(row) is list
            and len(row) == width
            and type(row[0]) is int
            and all(type(value) is bool for value in row[1:])
            for row in rows
        ),
        "role_coverage_changed",
    )
    by_role = {row[0]: row[1:] for row in rows}
    _require(by_role.keys() == role_ids and len(rows) == len(role_ids), "role_coverage_changed")
    return by_role


async def capture_initial_callable_custody(connection, policy, *, legacy=False):
    """Capture exact overloads, current composite and native rights without changing authority."""
    _require(
        type(policy) is GenericSourcePolicy and type(legacy) is bool and connection.is_in_transaction() is True,
        "policy_required",
    )
    owner, _ = await storage_roles(connection, policy.schema, **policy.role_arguments())
    graph = _decoded(await connection.fetchval(_ROLE_GRAPH_SQL))
    role_by_name = {role["rolname"]: int(role["oid"]) for role in graph["roles"]}
    role_ids = {int(role["oid"]) for role in graph["roles"]}
    _require(len(role_ids) == len(graph["roles"]), "role_coverage_changed")
    owner_usage = _decoded(await connection.fetchval(_OWNER_USAGE_SQL, owner))
    _role_rows(owner_usage, role_ids, 2)
    relations = await _capture_relations(connection, policy.schema)
    composite_oid = int(relations[_RELATIONS[0]]["composite"]["oid"])
    namespace = int(relations[_RELATIONS[0]]["relation"]["relnamespace"])
    receipt_version = await _receipt_catalog_version(connection, policy.schema)
    specifications = _specifications(policy.schema, composite_oid, receipt_version=receipt_version)
    function_rows = await connection.fetch(
        _FUNCTION_SQL,
        list(_signatures(policy.schema)),
        policy.schema,
        list(INITIAL_CALLABLES),
        list(policy.runtime_roles),
    )
    _require(
        len(function_rows) == 5
        and {function_row["signature"] for function_row in function_rows} == set(_signatures(policy.schema)),
        "overloads_changed",
    )
    function_by_name = {}
    for function_row in function_rows:
        function_by_field = {
            key: _decoded(function_row[key]) if key not in {"signature", "language"} else function_row[key]
            for key in function_row.keys()
        }
        name = function_by_field["definition"]["proname"]
        if name == "pd_profile_initial_serving_insert":
            body = guards.validated_serving_guard_body(policy.schema, function_by_field["definition"]["prosrc"])
            specifications[name]["body"] = " " + body + " "
        _require_definition(function_by_field, specifications[name], namespace)
        _role_rows(function_by_field["access"], role_ids, 6)
        _require_function_access(function_by_field, owner, role_by_name, policy, legacy)
        function_by_name[name] = function_by_field
    _require_trigger_links(relations, function_by_name)
    return dict(
        database_oid=await connection.fetchval("SELECT oid::bigint FROM pg_database WHERE datname=current_database()"),
        schema_oid=namespace,
        owner_oid=owner,
        publisher_role=policy.publisher_role,
        role_graph=graph,
        owner_usage=owner_usage,
        relations=relations,
        functions=function_by_name,
        rows=await _capture_rows(connection, policy.schema),
    )


def _expected_acl(entry, owner):
    previous = int(entry["definition"]["proowner"])
    expected_acl_rows = [
        [owner if grantee == previous else grantee, owner if grantor == previous else grantor, privilege, grantable]
        for grantee, grantor, privilege, grantable in entry["acl"]
    ]
    if previous != owner and not any(grantee == 0 for grantee, *_ in expected_acl_rows):
        expected_acl_rows.append([previous, owner, "EXECUTE", False])
    return sorted(expected_acl_rows)


def _require_execution_delta(previous, current, owner_usage, boundaries):
    before = _role_rows(previous["access"], owner_usage.keys(), 6)
    after = _role_rows(current["access"], owner_usage.keys(), 6)
    for role, prior in before.items():
        actual = after[role]
        _require(prior[:2] == actual[:2], "role_classifier_changed")
        if prior[1] and role not in boundaries:
            _require(prior[-1] == actual[-1], "ordinary_execute_changed")
        _require(actual[-1] == (prior[-1] or owner_usage[role][0]), "execute_changed")


def require_initial_callable_delta(before, after):
    """Permit only native owner/ACL changes preserving old ordinary EXECUTE and all data."""
    _require(
        {key: scope_value for key, scope_value in before.items() if key != "functions"}
        == {key: scope_value for key, scope_value in after.items() if key != "functions"},
        "adoption_scope_changed",
    )
    _require(
        before["functions"].keys() == after["functions"].keys() == set(INITIAL_CALLABLES), "adoption_scope_changed"
    )
    owner = after["owner_oid"]
    roles = {int(role["oid"]) for role in after["role_graph"]["roles"]}
    _require(len(roles) == len(after["role_graph"]["roles"]), "role_coverage_changed")
    _role_rows(before["owner_usage"], roles, 2)
    owner_usage = _role_rows(after["owner_usage"], roles, 2)
    publisher = next(
        int(role["oid"]) for role in after["role_graph"]["roles"] if role["rolname"] == after["publisher_role"]
    )
    for name, previous in before["functions"].items():
        current = after["functions"][name]
        expected = deepcopy(previous["definition"])
        expected["proowner"] = str(owner) if type(expected["proowner"]) is str else owner
        if expected["proacl"] is None:
            _require(current["definition"]["proacl"] is None, "adoption_acl_changed")
        else:
            expected["proacl"] = current["definition"]["proacl"]
        _require(
            expected == current["definition"] and previous["signature"] == current["signature"], "adoption_changed"
        )
        _require(_expected_acl(previous, owner) == sorted(current["acl"]), "adoption_acl_changed")
        _require_execution_delta(previous, current, owner_usage, {owner, publisher})


async def provision_initial_callable_custody(connection, policy, *, verify_role_profile):
    """Adopt only five callables in the supplied original independent-admin transaction."""
    _require(
        type(policy) is GenericSourcePolicy
        and connection.is_in_transaction() is True
        and callable(verify_role_profile),
        "explicit_admin_required",
    )
    task, actor = asyncio.current_task(), await _admin_identity(connection, policy)
    async with connection.transaction():
        await verify_role_profile(connection, policy.schema, **policy.role_arguments())
        await connection.execute(
            "LOCK TABLE "
            + ",".join(storage_relation(policy.schema, name) for name in _RELATIONS)
            + " IN SHARE MODE NOWAIT"
        )
        before = await capture_initial_callable_custody(connection, policy, legacy=True)
        for entry in sorted(before["functions"].values(), key=lambda value: int(value["definition"]["oid"])):
            previous = int(entry["definition"]["proowner"])
            if previous != before["owner_oid"]:
                await connection.execute(
                    f"ALTER FUNCTION {entry['signature']} OWNER TO {storage_identifier(policy.owner_role)}"
                )
                if not any(grantee == 0 for grantee, *_ in entry["acl"]):
                    legacy_role = next(
                        role["rolname"] for role in before["role_graph"]["roles"] if int(role["oid"]) == previous
                    )
                    await connection.execute(
                        f"GRANT EXECUTE ON FUNCTION {entry['signature']} TO {storage_identifier(legacy_role)}"
                    )
        after = await capture_initial_callable_custody(connection, policy)
        require_initial_callable_delta(before, after)
        await verify_role_profile(connection, policy.schema, **policy.role_arguments())
        _require(
            asyncio.current_task() is task and await _admin_identity(connection, policy) == actor,
            "admin_transaction_changed",
        )
        return after
