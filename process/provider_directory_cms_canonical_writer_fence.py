# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reader-compatible locks for legacy history prework and the short CMS terminal window.

The caller authenticates the protected profile and owns admission, the complete
phase/commit deadline, rollback and terminal accounting. Preparation may inspect
header identities; acquisition and rechecks read catalogs and the fixed provenance row. Prework
locks only the two source-fixed legacy heaps while validating the full family;
the terminal fence locks the full existing canonical family. Newly attached
private outputs still require their separate sealed custody.
No function here grants rights, changes ownership, copies payloads or commits.
"""

import asyncio
import json
import math
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_callable_custody as callables
from process import provider_directory_cms_callee_inventory as callee_inventory
from process import provider_directory_cms_history_conversion as history
from process import provider_directory_cms_history_postimage as history_postimage
from process import provider_directory_cms_preparation_custody as custody
from process import provider_directory_cms_preparation_policy as policy_checks
from process import provider_directory_cms_prepared_context as physical_scope
from process import provider_directory_cms_routine_settings as routine_settings
from process import provider_directory_cms_transition_routines as routine_checks
from process.provider_directory_cms_control_custody import _FOREIGN_SQL, _require_native_foreign_functions

_ACTOR_SQL = custody._ACTOR_SQL.replace(
    " AS transaction",
    " AS transaction,current_setting('transaction_isolation') AS isolation,"
    "current_setting('transaction_read_only') AS readonly,current_setting('search_path') AS search_path,"
    "(SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=session_user) AS superuser",
)
_IDENTITY_SQL = """SELECT c.oid::bigint,n.nspname,c.relname,c.relowner::bigint,c.relkind::text,
 c.relpersistence::text,ARRAY(SELECT inhparent::bigint FROM pg_catalog.pg_inherits
 WHERE inhrelid=c.oid ORDER BY inhparent) AS parents
 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE c.oid=ANY($1::oid[]) ORDER BY c.oid"""
_LOCKS_SQL = """SELECT count(*)=cardinality($1::oid[]) AND COALESCE(bool_and(
 EXISTS(SELECT FROM pg_catalog.pg_locks l WHERE l.pid=pg_catalog.pg_backend_pid()
 AND l.locktype='relation' AND l.database=$2::oid AND l.relation=r.oid AND l.granted
 AND l.mode='ShareRowExclusiveLock')
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_locks l WHERE l.pid=pg_catalog.pg_backend_pid()
 AND l.locktype='relation' AND l.database=$2::oid AND l.relation=r.oid AND l.granted
 AND l.mode IN ('ExclusiveLock','AccessExclusiveLock'))),false)
 FROM pg_catalog.pg_class r WHERE r.oid=ANY($1::oid[])"""
_HISTORY_LOCKS_SQL = _LOCKS_SQL.replace("ShareRowExclusiveLock", "ShareLock")
_PROOF_FUNCTIONS = (
    "provider_directory_terminal_root_retirement_valid",
    "provider_directory_terminal_root_retirement_v2_valid",
)
_OWNERS_SQL = """WITH owners AS (
 SELECT c.relowner AS oid FROM pg_catalog.pg_class c WHERE c.oid=ANY($1::oid[])
 UNION SELECT n.nspowner FROM pg_catalog.pg_namespace n WHERE n.nspname=$2
)
SELECT NOT EXISTS(SELECT FROM owners target JOIN pg_catalog.pg_roles owner ON owner.oid=target.oid
 WHERE owner.rolcanlogin OR owner.rolsuper OR owner.rolcreatedb OR owner.rolcreaterole
 OR owner.rolreplication OR owner.rolbypassrls
 OR EXISTS(SELECT FROM pg_catalog.pg_roles elevated
 WHERE pg_catalog.pg_has_role(owner.oid,elevated.oid,'MEMBER') AND
 (elevated.rolsuper OR elevated.rolcreatedb OR elevated.rolcreaterole
 OR elevated.rolreplication OR elevated.rolbypassrls))
 OR EXISTS(SELECT FROM pg_catalog.pg_roles actor
 WHERE actor.rolcanlogin AND NOT actor.rolsuper AND actor.rolname<>$3
 AND pg_catalog.pg_has_role(actor.oid,owner.oid,'MEMBER')))"""
_DEPENDENCIES_SQL = """WITH actors AS (
 SELECT DISTINCT reachable.oid FROM pg_catalog.pg_roles actor JOIN pg_catalog.pg_roles reachable
 ON pg_catalog.pg_has_role(actor.oid,reachable.oid,'MEMBER')
 WHERE actor.rolcanlogin AND NOT actor.rolsuper AND actor.rolname<>$2
)
SELECT NOT EXISTS(SELECT FROM pg_catalog.pg_rewrite WHERE ev_class=ANY($1::oid[]))
 AND NOT EXISTS(SELECT FROM actors a WHERE pg_catalog.has_schema_privilege(a.oid,$3::text,'CREATE'))
 AND NOT EXISTS(SELECT FROM actors a,unnest($1::oid[]) r(oid) WHERE
 pg_catalog.has_table_privilege(a.oid,r.oid,'REFERENCES,TRIGGER')
 OR pg_catalog.has_any_column_privilege(a.oid,r.oid,'REFERENCES'))"""
_BOUND_DEPENDENCIES_SQL = _DEPENDENCIES_SQL.replace(
    " AND NOT EXISTS(SELECT FROM actors a WHERE pg_catalog.has_schema_privilege(a.oid,$3::text,'CREATE'))\n",
    "",
    1,
)
_DEFINER_AUTHORITY_SQL = """WITH RECURSIVE protected AS (
 SELECT c.relowner AS oid FROM pg_catalog.pg_class c WHERE c.oid=ANY($1::oid[])
 UNION SELECT p.proowner FROM pg_catalog.pg_proc p WHERE p.oid=ANY($2::oid[])
 UNION SELECT n.nspowner FROM pg_catalog.pg_namespace n WHERE n.nspname=$4 OR n.oid IN (
 SELECT c.relnamespace FROM pg_catalog.pg_class c WHERE c.oid=ANY($1::oid[])
 UNION SELECT p.pronamespace FROM pg_catalog.pg_proc p WHERE p.oid=ANY($2::oid[]))
), unqualified AS (
 SELECT p.oid,p.proowner FROM pg_catalog.pg_proc p
 WHERE p.prosecdef AND NOT p.oid=ANY($3::oid[])
), actors AS (
 SELECT reachable.oid FROM pg_catalog.pg_roles actor JOIN pg_catalog.pg_roles reachable
 ON pg_catalog.pg_has_role(actor.oid,reachable.oid,'MEMBER')
 WHERE actor.rolcanlogin AND NOT actor.rolsuper AND actor.rolname<>$5
 UNION
 SELECT reachable.oid FROM actors actor JOIN unqualified p
 ON pg_catalog.has_function_privilege(actor.oid,p.oid,'EXECUTE')
 JOIN pg_catalog.pg_roles reachable ON pg_catalog.pg_has_role(p.proowner,reachable.oid,'MEMBER')
)
SELECT NOT EXISTS(SELECT FROM actors actor JOIN unqualified p
 ON pg_catalog.has_function_privilege(actor.oid,p.oid,'EXECUTE')
 WHERE EXISTS(SELECT FROM protected owner WHERE pg_catalog.pg_has_role(p.proowner,owner.oid,'MEMBER'))
 OR EXISTS(SELECT FROM pg_catalog.pg_roles elevated
 WHERE pg_catalog.pg_has_role(p.proowner,elevated.oid,'MEMBER') AND
 (elevated.rolsuper OR elevated.rolcreatedb OR elevated.rolcreaterole
 OR elevated.rolreplication OR elevated.rolbypassrls)))"""


@dataclass(frozen=True)
class _WriterPreparation:
    connection: object
    task: object
    native: str


@dataclass(frozen=True)
class _WriterFence:
    preparation: _WriterPreparation
    deadline: float
    history_only: bool = False
    history_scope: tuple | None = None


def _require(condition, code):
    if not condition:
        raise ValueError("cms_canonical_writer_" + code)


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _identity(family):
    return [
        {
            **{key: entry[key] for key in ("oid", "nspname", "relname", "relowner", "relkind", "relpersistence")},
            "parents": entry["native"]["parents"],
        }
        for entry in sorted(family, key=lambda entry: entry["oid"])
    ]


def _deadline(deadline):
    _require(type(deadline) in {int, float} and math.isfinite(deadline), "deadline_required")
    _require(deadline > asyncio.get_running_loop().time(), "deadline_expired")
    return deadline


async def _actor(connection, storage_binding, policy, migration_role, *, allow_serializable=False):
    actor_by_field = dict(await connection.fetchrow(_ACTOR_SQL))
    _require(
        actor_by_field["session"] == actor_by_field["current"]
        and actor_by_field["current"]
        not in {
            migration_role,
            policy.owner_role,
            policy.publisher_role,
            *policy.runtime_roles,
            *policy.control_runtime_roles,
        }
        and actor_by_field["superuser"] is True
        and actor_by_field["backend"] == connection.get_server_pid()
        and actor_by_field["database_oid"] == storage_binding.database_oid
        and actor_by_field["replication"] == "origin"
        and actor_by_field["isolation"]
        in ({"read committed", "serializable"} if allow_serializable else {"read committed"})
        and actor_by_field["readonly"] == "off"
        and actor_by_field["search_path"] in {"pg_catalog", "pg_catalog, pg_temp"}
        and bool(actor_by_field["transaction"]),
        "original_admin_required",
    )
    return actor_by_field


async def _guard_allocator(connection, arguments, function_rows):
    """Pin one compatibility fact without extending the fixed callable frontier."""
    guards = [
        row
        for row in function_rows
        if callables._decoded(row["definition"])["proname"] == "guard_pd_dataset_candidate_parent"
    ]
    if len(guards) != 1:
        return None
    owner = int(callables._decoded(guards[0]["definition"])["proowner"])
    if set(map(tuple, callables._decoded(guards[0]["acl"]))) == {(owner, owner, "EXECUTE", False)}:
        return None
    schema = arguments["catalog_schema"]
    name = "prepare_pd_generic_dataset_storage"
    allocator_rows = await connection.fetch(
        callables._FUNCTION_SQL + " FOR SHARE OF p NOWAIT",
        [storage_relation(schema, name) + "()"],
        schema,
        [name],
        [],
    )
    _require(len(allocator_rows) == 1, "allocator_function_changed")
    return {key: value for key, value in dict(allocator_rows[0]).items() if key != "access"}


def _verified_proofs(function_rows, arguments):
    """Verify only the two source-fixed proof roots outside the fixed32 frontier."""
    if "proof_functions" not in arguments:
        return {}
    from process.provider_directory_cms_proof_contract import cms_proof_actor_oids

    document = arguments["source_document"]
    owner = arguments["namespace_owner_oid"]
    actors = cms_proof_actor_oids(document, document["role_graph"]["roles"], namespace_owner_oid=owner)
    specs = routine_checks._contracts(arguments["catalog_schema"], arguments["preparation_schema"])
    verified_by_name = {}
    for function_row in function_rows:
        entry_by_field = {
            **dict(function_row),
            "definition": callables._decoded(function_row["definition"]),
            "acl": callables._decoded(function_row["acl"]),
        }
        definition = entry_by_field["definition"]
        name = definition["proname"]
        if name not in _PROOF_FUNCTIONS:
            continue
        _require(name not in verified_by_name, "proof_functions_changed")
        callee_inventory._require_fingerprint(entry_by_field, arguments["proof_functions"][name])
        routine_checks._require_source_definition(
            entry_by_field,
            specs[name],
            arguments["catalog_schema"],
            arguments["catalog_schema_oid"],
            arguments["preparation_schema"],
        )
        edges = set(map(tuple, entry_by_field["acl"]))
        _require(
            int(definition["proowner"]) == owner
            and len(edges) == len(entry_by_field["acl"])
            and (owner, owner, "EXECUTE", False) in edges
            and all(
                grantee in actors | {owner} and grantor == owner and right == "EXECUTE" and not grantable
                for grantee, grantor, right, grantable in edges
            ),
            "proof_acl_changed",
        )
        verified_by_name[name] = {key: field_value for key, field_value in entry_by_field.items() if key != "access"}
    _require(set(verified_by_name) == set(_PROOF_FUNCTIONS), "proof_functions_changed")
    return verified_by_name


async def _retained_routine_inventory(connection, arguments, relation_oids, roots, protected_sha256):
    query = custody._ROUTINES_SQL
    if protected_sha256 is not None:
        from process import provider_directory_cms_dependency_policy as dependencies

        _require(
            "source_document" in arguments
            and type(protected_sha256) is str
            and re.fullmatch(r"[0-9a-f]{64}", protected_sha256)
            and protected_sha256 != "0" * 64,
            "protected_routines_required",
        )
        query = dependencies._ROUTINES_SQL
    routines = [
        {"oid": row["oid"], "native": custody._decoded(row["native"])}
        for row in await connection.fetch(query, relation_oids, roots)
    ]
    if protected_sha256 is not None:
        _require(custody._digest(routines) == protected_sha256, "protected_routines_changed")
    return routines


def _read_definers(routines, arguments, protected_sha256):
    if protected_sha256 is None:
        return []
    from process.provider_directory_cms_read_definers import require_cms_read_definers

    return require_cms_read_definers(
        routines,
        catalog_schema=arguments["catalog_schema"],
        catalog_schema_oid=arguments["catalog_schema_oid"],
        preparation_schema=arguments["preparation_schema"],
        source_document=arguments["source_document"],
        namespace_owner_oid=arguments["namespace_owner_oid"],
    )


async def _callable_closure(connection, arguments, relation_oids, retained_routines=(), protected_routines_sha256=None):
    """Verify fresh routine rows and settings before deriving the fixed execution frontier."""
    signatures = (
        *callables.cms_callee_signatures(arguments["catalog_schema"]),
        *(
            storage_relation(arguments["catalog_schema"], name) + "(pg_catalog.text)"
            for name in _PROOF_FUNCTIONS
            if "proof_functions" in arguments
        ),
    )
    names = sorted(set(arguments["expected_functions"]) | set(arguments.get("proof_functions", ())))
    function_rows = await connection.fetch(
        callables._FUNCTION_SQL, list(signatures), arguments["catalog_schema"], names, []
    )
    allocator = await _guard_allocator(connection, arguments, function_rows)
    fixed_rows = [
        function_row
        for function_row in function_rows
        if callables._decoded(function_row["definition"])["proname"] in arguments["expected_functions"]
    ]
    arguments_by_field = {
        key: field_value for key, field_value in arguments.items() if key not in {"proof_functions", "source_metadata"}
    }
    if "source_document" in arguments:
        verified = callee_inventory.require_cms_source_callee_inventory(
            fixed_rows, **arguments_by_field, allocator=allocator
        )
    else:
        verified = callee_inventory.require_cms_callee_inventory(fixed_rows, **arguments, allocator=allocator)
    proofs = _verified_proofs(function_rows, arguments)
    frontier_oids = [int(entry["definition"]["oid"]) for entry in (*verified.values(), *proofs.values())]
    routine_roots = sorted(set(frontier_oids) | set(retained_routines)) if retained_routines else frontier_oids
    routines = await _retained_routine_inventory(
        connection, arguments, relation_oids, routine_roots, protected_routines_sha256
    )
    _require(set(frontier_oids) <= {entry["oid"] for entry in routines}, "callable_closure_changed")
    callables.require_cms_terminal_guard_resolution(
        routines,
        catalog_schema=arguments["catalog_schema"],
        catalog_schema_oid=arguments["catalog_schema_oid"],
        preparation_schema=arguments["preparation_schema"],
    )
    read_definers = _read_definers(routines, arguments, protected_routines_sha256)
    if protected_routines_sha256 is None:
        routine_settings.require_cms_terminal_routine_settings(
            routines,
            catalog_schema=arguments["catalog_schema"],
            catalog_schema_oid=arguments["catalog_schema_oid"],
            preparation_schema=arguments["preparation_schema"],
        )
    return {
        "frontier": {
            name: {key: field_value for key, field_value in entry.items() if key != "access"}
            for name, entry in verified.items()
        },
        "routines": routines,
        "read_definers": read_definers,
        "allocator": allocator,
        **({"proofs": proofs} if proofs else {}),
    }


def _verified_definer_oids(functions):
    """Keep qualified read definers separate from the fixed publication frontier."""
    return [
        *(
            int(entry["definition"]["oid"])
            for entry in (*functions["frontier"].values(), *functions.get("proofs", {}).values())
        ),
        *functions.get("read_definers", ()),
    ]


def _require_foreign_boundary(edges, relation_oids):
    """Internal cascades are locked; external mutation paths need a separate fixed contract."""
    scope_oids = set(relation_oids)
    hooks = set()
    for edge in edges:
        constraint = edge["constraint"]
        _require(constraint["convalidated"] and constraint.get("conenforced", True), "foreign_key_changed")
        is_internal = int(constraint["conrelid"]) in scope_oids and int(constraint["confrelid"]) in scope_oids
        _require(
            is_internal or {constraint["confupdtype"], constraint["confdeltype"]} <= {"a", "r"},
            "external_cascade_contract_required",
        )
        _require(bool(edge["hooks"]), "foreign_hook_changed")
        for hook in edge["hooks"]:
            _require(
                hook["tgisinternal"]
                and hook["tgenabled"] in {"O", "A"}
                and str(hook["tgconstraint"]) == str(constraint["oid"])
                and hook["tgqual"] is None
                and hook["tgnargs"] == 0
                and not hook["tgattr"],
                "foreign_hook_changed",
            )
            hooks.add(int(hook["tgfoid"]))
    return hooks


def _require_creator_bodies(native, routines):
    """Require the complete fixed postimage before accepting ordinary staging CREATE."""
    from process.provider_directory_cms_routine_bodies import _targets

    arguments = native["arguments"]
    routine_checks.require_cms_transition_routine_definitions(
        routines,
        catalog_schema=arguments["catalog_schema"],
        catalog_schema_oid=arguments["catalog_schema_oid"],
        preparation_schema=arguments["preparation_schema"],
    )
    targets = _targets(arguments["catalog_schema"], arguments["preparation_schema"])
    matched_names = set()
    for routine in routines:
        native_routine = routine["native"]
        name, body = native_routine["definition"]["proname"], native_routine["definition"]["prosrc"]
        if name not in targets:
            continue
        allowed, target = targets[name]
        _require(
            name not in matched_names
            and body in allowed
            and body == target
            and native_routine["body"].count(body) == 1,
            "creator_body_postimage_required",
        )
        matched_names.add(name)
    _require(matched_names == targets.keys(), "creator_body_postimage_required")


async def _closure_relation_oids(connection, native):
    """Recheck fixed read dependencies before including them in the writer scope."""
    read_dependencies = native.get("read_dependencies", [])
    dependency_histories = native.get("dependency_histories", [])
    if read_dependencies:
        from process import provider_directory_cms_dependency_policy as dependencies

        schema = native["arguments"]["catalog_schema"]
        _require(
            await dependencies._parents(connection, schema) == read_dependencies,
            "read_dependency_changed",
        )
        plan = native["dependency_migration_plan"][dependencies.PROVENANCE_ROOT]
        _require(await dependencies._read_migration_plan(connection, schema) == plan, "dependency_plan_changed")
        _require(
            await dependencies._history_relations(connection, schema, read_dependencies, plan) == dependency_histories,
            "dependency_history_changed",
        )
    return sorted({entry["oid"] for entry in (*native["relations"], *read_dependencies, *dependency_histories)})


async def _closure(connection, native):
    """Retain every writer boundary; only fully qualified CMS callers may retain CREATE."""
    oids = await _closure_relation_oids(connection, native)
    schema, migration = native["arguments"]["catalog_schema"], native["migration_role"]
    _require(await connection.fetchval(_OWNERS_SQL, oids, schema, migration) is True, "ddl_owner_unprotected")
    dependencies_safe = await connection.fetchval(_DEPENDENCIES_SQL, oids, migration, schema)
    _require(
        dependencies_safe is True or native.get("retained_routines_sha256") is not None,
        "indirect_writer_reachable",
    )
    functions = await _callable_closure(
        connection,
        native["arguments"],
        oids,
        native.get("retained_routines", ()),
        native.get("retained_routines_sha256"),
    )
    _require(
        await connection.fetchval(
            custody._CALLABLE_OWNER_SQL, [routine["oid"] for routine in functions["routines"]], migration
        )
        is True,
        "callable_owner_unprotected",
    )
    # Dynamic SQL need not create pg_depend edges. Only source-verified bodies
    # bound the authority of a definer; unknown invokers add no owner authority.
    # There is no schema/extension exemption. Existing native-callee trust stays
    # in _CALLABLE_OWNER_SQL; ordinary native invokers do not enter this graph.
    _require(
        await connection.fetchval(
            _DEFINER_AUTHORITY_SQL,
            oids,
            [routine["oid"] for routine in functions["routines"]],
            _verified_definer_oids(functions),
            schema,
            migration,
        )
        is True,
        "definer_authority_reachable",
    )
    if dependencies_safe is not True:
        _require_creator_bodies(native, functions["routines"])
        _require(
            await connection.fetchval(_BOUND_DEPENDENCIES_SQL, oids, migration) is True,
            "indirect_writer_reachable",
        )
    foreign_edges = [
        {key: custody._decoded(edge[key]) for key in edge.keys()} for edge in await connection.fetch(_FOREIGN_SQL, oids)
    ]
    await _require_native_foreign_functions(connection, _require_foreign_boundary(foreign_edges, oids))
    return {"functions": functions, "foreign": foreign_edges}


async def _original(connection, preparation):
    _require(
        type(preparation) is _WriterPreparation
        and preparation.connection is connection
        and preparation.task is asyncio.current_task(),
        "original_scope_required",
    )
    _require(connection.is_closed() is False and connection.is_in_transaction() is True, "transaction_required")
    native = json.loads(preparation.native)
    _require(dict(await connection.fetchrow(_ACTOR_SQL)) == native["actor"], "original_admin_changed")
    return native


def _callee_arguments(arguments, storage_binding):
    """Reuse the fixed verifier's existing interface, copying coordinates before IO."""
    fields = {"catalog_schema", "catalog_schema_oid", "expected_functions", "namespace_owner_oid", "preparation_schema"}
    _require(
        type(arguments) is dict
        and set(arguments)
        in (
            fields,
            fields | {"source_metadata"},
            fields | {"source_document", "proof_functions"},
            fields | {"source_metadata", "source_document", "proof_functions"},
        )
        and ("source_metadata" not in arguments or type(arguments["source_metadata"]) is dict),
        "callee_contract_required",
    )
    _require(
        arguments["catalog_schema"] == storage_binding.catalog_schema
        and arguments["catalog_schema_oid"] == storage_binding.catalog_schema_oid,
        "callee_coordinates_changed",
    )
    if "source_document" in arguments:
        _require(
            type(arguments["source_document"]) is dict
            and custody._digest(arguments["source_document"]) == storage_binding.enrollment_sha256
            and (
                "source_metadata" not in arguments
                or arguments["source_metadata"]
                == arguments["source_document"]["storage_custody"]["canonical_authority"]["metadata"]
            ),
            "source_document_changed",
        )
        proofs = arguments["proof_functions"]
        _require(
            type(proofs) is dict
            and set(proofs) == set(_PROOF_FUNCTIONS)
            and all(
                type(pin) is dict
                and set(pin) == {"oid", "sha256"}
                and type(pin["oid"]) is int
                and 0 < pin["oid"] < 2**32
                and type(pin["sha256"]) is str
                and re.fullmatch(r"[0-9a-f]{64}", pin["sha256"])
                for pin in proofs.values()
            )
            and len({pin["oid"] for pin in proofs.values()}) == len(proofs),
            "proof_functions_required",
        )
    return json.loads(_encoded(arguments))


async def _profile_fingerprints(connection, storage_binding, policy, profile, source_proofs):
    """Retain authenticated v3 canonical evidence and the existing Source-proof roots."""
    if not source_proofs and profile["contract"] != policy_checks.DEPENDENCY_CONTRACT:
        return await policy_checks._observe_profile(connection, storage_binding, policy, profile), [], None
    evidence = await policy_checks._collect_profile(connection, storage_binding, policy, profile)
    routines = sorted(row["oid"] for row in evidence["routines"]) if source_proofs else []
    canonical = evidence["canonical"] if profile["contract"] == policy_checks.DEPENDENCY_CONTRACT else None
    return custody._evidence_digests(evidence), routines, canonical


def _history_prework(history_prework):
    _require(
        type(history_prework) is dict
        and set(history_prework) == {"history_dataset_ids", "bounds_validated"}
        and type(history_prework["bounds_validated"]) is bool,
        "history_prework_required",
    )
    datasets_by_root = history_prework["history_dataset_ids"]
    _require(
        type(datasets_by_root) is dict and set(datasets_by_root) == set(history.ROOTS), "history_datasets_required"
    )
    for dataset_ids in datasets_by_root.values():
        history._bounds(dataset_ids)
    return json.loads(_encoded(history_prework))


async def _preparation_fingerprints(connection, storage_binding, policy, profile, arguments, deadline, history_prework):
    if history_prework is None:
        return (
            *await _profile_fingerprints(connection, storage_binding, policy, profile, "source_document" in arguments),
            {},
        )
    inventory_by_kind, bounds_by_root = await history_postimage.require_cms_history_prework_postimage(
        connection,
        storage_binding=storage_binding,
        policy=policy,
        cms_profile=profile,
        deadline=deadline,
        **history_prework,
    )
    projected_by_kind = history_postimage._without_history_bounds(inventory_by_kind, bounds_by_root)
    retained_routines = (
        sorted(entry["oid"] for entry in inventory_by_kind["routines"]) if "source_document" in arguments else []
    )
    return (
        custody._evidence_digests(projected_by_kind),
        retained_routines,
        inventory_by_kind["canonical"],
        bounds_by_root,
    )


async def _prepared_canonical(connection, storage_binding, policy, profile, authenticated_canonical, bounds_by_root):
    canonical_by_field, family = await policy_checks._canonical(
        connection, storage_binding, policy, profile["fixed_descendant_oids"]
    )
    if profile["contract"] == policy_checks.DEPENDENCY_CONTRACT or bounds_by_root:
        _require(
            {field: authenticated_canonical[field] for field in ("relations", "migration_plan")} == canonical_by_field,
            "canonical_policy_changed",
        )
        canonical_by_field = authenticated_canonical
    projected_by_field = canonical_by_field
    if bounds_by_root:
        projected_by_field = history_postimage._without_history_bounds(
            {"canonical": canonical_by_field}, bounds_by_root
        )["canonical"]
    _require(custody._digest(projected_by_field) == profile["canonical_sha256"], "canonical_policy_changed")
    return canonical_by_field, family


async def _prepare_writer_fence(
    connection,
    *,
    storage_binding,
    policy,
    cms_profile,
    callee_arguments,
    migration_role,
    deadline,
    history_prework=None,
):
    """Preflight source-defined custody before the short lock window; caller authenticates the profile."""
    _deadline(deadline)
    storage_identifier(migration_role)
    profile = policy_checks._profile(cms_profile, storage_binding, policy)
    arguments_by_field = _callee_arguments(callee_arguments, storage_binding)
    _require(connection.is_in_transaction() is True and connection.is_closed() is False, "transaction_required")
    async with asyncio.timeout_at(deadline):
        actor = await _actor(
            connection, storage_binding, policy, migration_role, allow_serializable=history_prework is not None
        )
        observed_profile, retained_routines, authenticated_canonical, bounds_by_root = await _preparation_fingerprints(
            connection, storage_binding, policy, profile, arguments_by_field, deadline, history_prework
        )
        _require(
            observed_profile == {field: profile[field] for field in custody._DIGEST_FIELDS}, "protected_profile_changed"
        )
        canonical_by_field, family = await _prepared_canonical(
            connection, storage_binding, policy, profile, authenticated_canonical, bounds_by_root
        )
        read_dependencies = (
            canonical_by_field["dependency_relations"]
            if profile["contract"] == policy_checks.DEPENDENCY_CONTRACT
            else []
        )
        native_by_field = dict(
            actor=actor,
            deadline=deadline,
            migration_role=migration_role,
            relations=_identity(family),
            inventory_names=sorted(entry["relname"] for entry in family),
            family=family,
            arguments=arguments_by_field,
            **({"retained_routines": retained_routines} if retained_routines else {}),
            **({"history_prework_only": True} if history_prework is not None else {}),
        )
        native_by_field = json.loads(_encoded(native_by_field))
        if retained_routines and profile["contract"] == policy_checks.DEPENDENCY_CONTRACT:
            native_by_field["retained_routines_sha256"] = profile["routines_sha256"]
        if read_dependencies:
            native_by_field["read_dependencies"] = read_dependencies
            native_by_field["dependency_histories"] = canonical_by_field["dependency_histories"]
            native_by_field["dependency_migration_plan"] = canonical_by_field["dependency_migration_plan"]
        native_by_field["closure"] = await _closure(connection, native_by_field)
        preparation = _WriterPreparation(connection, asyncio.current_task(), _encoded(native_by_field))
        await _original(connection, preparation)
        return preparation


async def prepare_cms_canonical_writer_fence(
    connection, *, storage_binding, policy, cms_profile, callee_arguments, migration_role, deadline
):
    """Preflight source-defined custody before the short terminal lock window."""
    return await _prepare_writer_fence(
        connection,
        storage_binding=storage_binding,
        policy=policy,
        cms_profile=cms_profile,
        callee_arguments=callee_arguments,
        migration_role=migration_role,
        deadline=deadline,
    )


async def prepare_cms_history_prework_fence(
    connection, *, storage_binding, policy, cms_profile, callee_arguments, migration_role, deadline, history_prework
):
    """Authenticate genuine profile custody with exactly two proved CHECK additions; prework only.

    history_prework contains only history_dataset_ids and the boolean bounds_validated.
    Read committed and serializable OriginalAdmin transactions are supported.
    """
    history_prework = _history_prework(history_prework)
    return await _prepare_writer_fence(
        connection,
        storage_binding=storage_binding,
        policy=policy,
        cms_profile=cms_profile,
        callee_arguments=callee_arguments,
        migration_role=migration_role,
        deadline=deadline,
        history_prework=history_prework,
    )


async def require_cms_history_prework_fence(connection, preparation):
    """Recheck the same prework actor, transaction, actual family and complete writer closure."""
    _require(type(preparation) is _WriterPreparation, "preparation_required")
    native = json.loads(preparation.native)
    _require(native.get("history_prework_only") is True, "history_prework_required")
    _deadline(native["deadline"])
    async with asyncio.timeout_at(native["deadline"]):
        native = await _original(connection, preparation)
        observed_family = await custody._inventory(
            connection, native["arguments"]["catalog_schema"], native["inventory_names"]
        )
        _require(observed_family == native["family"], "family_changed")
        _require(await _closure(connection, native) == native["closure"], "closure_changed")
        await _original(connection, preparation)


async def _require_locks(connection, native):
    _require(
        await connection.fetchval(
            _LOCKS_SQL, [entry["oid"] for entry in native["relations"]], native["actor"]["database_oid"]
        )
        is True,
        "reader_compatible_locks_required",
    )


async def acquire_cms_canonical_writer_fence(connection, preparation, *, deadline):
    """Lock existing families NOWAIT before terminal mutation; caller owns rollback and commit.

    SHARE ROW EXCLUSIVE blocks direct DML, cascades and native maintenance while
    retaining ACCESS SHARE readers. A failed acquisition can leave earlier locks;
    the caller must roll back, never continue with a partial scope.
    """
    _deadline(deadline)
    async with asyncio.timeout_at(deadline):
        native = await _original(connection, preparation)
        _require(native.get("history_prework_only") is not True, "history_prework_only")
        _require(deadline <= native["deadline"], "deadline_extended")
        relations = sorted(storage_relation(entry["nspname"], entry["relname"]) for entry in native["relations"])
        await connection.execute(
            "LOCK TABLE " + ",".join("ONLY " + name for name in relations) + " IN SHARE ROW EXCLUSIVE MODE NOWAIT"
        )
        observed = await custody._inventory(
            connection, native["arguments"]["catalog_schema"], native["inventory_names"]
        )
        _require(observed == native["family"], "family_changed")
        await _require_locks(connection, native)
        _require(await _closure(connection, native) == native["closure"], "closure_changed")
        await _original(connection, preparation)
        return _WriterFence(preparation, deadline)


async def require_cms_canonical_writer_fence(connection, fence):
    """Recheck the same real locks and native closure before and after terminal CAS."""
    _require(type(fence) is _WriterFence and fence.history_only is False, "fence_required")
    _deadline(fence.deadline)
    async with asyncio.timeout_at(fence.deadline):
        native = await _original(connection, fence.preparation)
        _require(native.get("history_prework_only") is not True, "history_prework_only")
        observed_relations = [
            dict(row) for row in await connection.fetch(_IDENTITY_SQL, [entry["oid"] for entry in native["relations"]])
        ]
        _require(observed_relations == native["relations"], "identity_changed")
        await _require_locks(connection, native)
        _require(await _closure(connection, native) == native["closure"], "closure_changed")
        await _original(connection, fence.preparation)


def _history_relations(native):
    """Select source-fixed heaps from the already authenticated canonical family."""
    selected_relations = [entry for entry in native["relations"] if entry["relname"] in history.ROOTS]
    _require(
        len(selected_relations) == len(history.ROOTS)
        and {entry["relname"] for entry in selected_relations} == set(history.ROOTS)
        and all(
            entry["nspname"] == native["arguments"]["catalog_schema"]
            and entry["relkind"] == "r"
            and entry["relpersistence"] == "p"
            and not entry["parents"]
            and not any(entry["oid"] in relation["parents"] for relation in native["relations"])
            for entry in selected_relations
        ),
        "legacy_history_required",
    )
    return selected_relations


async def _history_physical(connection, fence):
    _require(
        type(fence.preparation) is _WriterPreparation and fence.preparation.connection is connection,
        "original_scope_required",
    )
    _require(type(fence.history_scope) is tuple and len(fence.history_scope) == 3, "history_scope_required")
    _require(connection.is_closed() is False and connection.is_in_transaction() is True, "transaction_required")
    database, session, transaction = fence.history_scope
    await physical_scope._physical(database, session, connection, fence.preparation.task, transaction)


async def acquire_cms_canonical_history_fence(connection, preparation, *, database, session, deadline):
    """Close both legacy heaps before their first scan; keep locks until transaction end.

    Preparation must already authenticate the protected CMS profile and complete
    family/callable authority. Only the two source-fixed legacy heaps receive
    SHARE locks; the complete family remains the validation scope. This supplies
    no history bounds, content commitment, conversion plan or terminal authority.
    The caller owns rollback on failure/cancellation and the outer commit.
    """
    _deadline(deadline)
    _require(
        isinstance(database, physical_scope.Database) and isinstance(session, physical_scope.AsyncSession),
        "history_transport_required",
    )
    fence = _WriterFence(
        preparation, deadline, history_only=True, history_scope=(database, session, session.get_transaction())
    )
    async with asyncio.timeout_at(deadline):
        await _history_physical(connection, fence)
        native = await _original(connection, preparation)
        _require(deadline <= native["deadline"], "deadline_extended")
        relations = sorted(storage_relation(entry["nspname"], entry["relname"]) for entry in _history_relations(native))
        await connection.execute(
            "LOCK TABLE " + ",".join("ONLY " + name for name in relations) + " IN SHARE MODE NOWAIT"
        )
        await require_cms_canonical_history_fence(connection, fence)
        return fence


@asynccontextmanager
async def cms_canonical_history_fence(connection, preparation, *, database, session, deadline):
    """Hold the original physical scope; exit invalidates locally even after COMMIT."""
    fence = await acquire_cms_canonical_history_fence(
        connection, preparation, database=database, session=session, deadline=deadline
    )
    try:
        yield fence
    finally:
        object.__setattr__(fence, "history_scope", None)


async def require_cms_canonical_history_fence(connection, fence):
    """Require original scope, both reader-compatible locks and the whole closure.

    Transaction/closed-connection checks precede SQL. No context exit, COMMIT,
    ROLLBACK or post-commit read is performed here. Terminal acquisition remains
    separate and does not release the already held history locks.
    """
    _require(type(fence) is _WriterFence and fence.history_only is True, "history_fence_required")
    _deadline(fence.deadline)
    async with asyncio.timeout_at(fence.deadline):
        await _history_physical(connection, fence)
        native = await _original(connection, fence.preparation)
        observed = await custody._inventory(
            connection, native["arguments"]["catalog_schema"], native["inventory_names"]
        )
        _require(observed == native["family"], "family_changed")
        _require(
            await connection.fetchval(
                _HISTORY_LOCKS_SQL,
                [entry["oid"] for entry in _history_relations(native)],
                native["actor"]["database_oid"],
            )
            is True,
            "history_locks_required",
        )
        _require(await _closure(connection, native) == native["closure"], "closure_changed")
        await _original(connection, fence.preparation)
