# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fence reverse trigger bindings during the caller's short common ownership cut.

The caller loads genuine enabled Source/profile records and owns their complete
prewrite and postimage checks. This verifies supplied protected data and native
transport/catalog custody; it does not authorize ownership, grants or publication.
All rechecks precede caller COMMIT. Exit is local-only and releases no native lock.
Never retain this context over payload COPY or a long scan.
"""

import asyncio
import math
from contextlib import asynccontextmanager
from copy import deepcopy

from process import provider_directory_cms_owner_permissions as permissions
from process import provider_directory_cms_preparation_custody as custody
from process import provider_directory_cms_preparation_policy as policy_checks

_RUN_GUARD = "guard_provider_directory_terminal_root_retirement_run"
_RUN_HOOKS = {"pd_trr_import_run_row": 31, "pd_trr_import_run_truncate": 34}
_REVERSE_SQL = """SELECT 'pg_catalog.pg_trigger'::pg_catalog.regclass::pg_catalog.oid::bigint AS catalog_oid,
 COALESCE((SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid) FROM pg_catalog.pg_trigger t
 WHERE t.tgfoid=ANY($1::oid[])),'[]'::jsonb) AS bindings"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_reverse_hook_" + code)


def _oid(value):
    if type(value) is str and value.isascii() and value.isdecimal():
        value = int(value)
    _require(type(value) is int and 0 < value < 2**32, "identity_required")
    return value


def _deadline(deadline):
    _require(
        type(deadline) in {int, float}
        and math.isfinite(deadline)
        and 0 < deadline - asyncio.get_running_loop().time() <= 3,
        "deadline_required",
    )


def _routine_oids(inventory):
    selected, retirement = set(), None
    for routine in inventory["routines"]:
        definition = routine["native"]["definition"]
        if definition["prosecdef"] is True and definition["prorettype"] == "2279":
            _require(routine["native"]["namespace"][1] != "pg_catalog", "application_callback_required")
            selected.add(_oid(routine["oid"]))
            if definition["proname"] == _RUN_GUARD:
                retirement = routine["oid"]
    _require(bool(selected) and retirement in selected, "retirement_callback_required")
    return sorted(selected), retirement


def _expected_hooks(inventory, function_oids, dynamic_relations):
    canonical = inventory["canonical"]
    relations = [
        *canonical["relations"],
        *canonical["dependency_relations"],
        *canonical["dependency_histories"],
        *inventory["private"],
        *dynamic_relations,
    ]
    expected, relation_oids = {}, set()
    for relation in relations:
        relation_oid = _oid(relation["oid"])
        _require(relation_oid not in relation_oids, "relation_identity_changed")
        relation_oids.add(relation_oid)
        hooks = relation["native"].get("triggers")
        _require(type(hooks) is list, "complete_hook_inventory_required")
        for hook in hooks:
            _require(_oid(hook["tgrelid"]) == relation_oid, "hook_relation_changed")
            if _oid(hook["tgfoid"]) in function_oids:
                identity = _oid(hook["oid"])
                _require(identity not in expected, "duplicate_hook_identity")
                expected[identity] = deepcopy(hook)
    return expected


def _inputs(inventory, binding, policy, cms_profile, source_document):
    document = source_document
    permissions._authenticated(inventory, binding, policy, cms_profile)
    permissions._source_roles(document, binding, policy, permissions._roles(inventory))
    admission = custody._allocation_admission_relations(document["storage_custody"])
    _require(
        all(entry["nspname"] == binding.catalog_schema for entry in admission)
        and len({_oid(entry["oid"]) for entry in admission}) == 3,
        "source_admission_changed",
    )
    run_oid = next(_oid(entry["oid"]) for entry in admission if entry["relname"] == "import_run")
    function_oids, retirement_oid = _routine_oids(inventory)
    excluded_roles = {policy.owner_role, policy.publisher_role, *policy.runtime_roles, *policy.control_runtime_roles}
    excluded_roles.update(
        document["policy"][field]
        for field in ("owner_role", "publisher_role", "namespace_owner_role", "migration_role")
        if field in document["policy"]
    )
    return function_oids, run_oid, retirement_oid, excluded_roles


async def _dynamic_relations(connection, inventory, binding, policy, cms_profile):
    """Reuse complete allocation validation against the protected fixed projection."""
    fixed, family = await policy_checks._canonical(connection, binding, policy, cms_profile["fixed_descendant_oids"])
    canonical = inventory["canonical"]
    _require(
        fixed == {key: canonical[key] for key in ("relations", "migration_plan")},
        "canonical_changed",
    )
    fixed_oids = {relation["oid"] for relation in fixed["relations"]}
    return [relation for relation in family if relation["oid"] not in fixed_oids]


def _run_hook(hook, run_oid, function_oid):
    required_by_field = dict(
        tgrelid=str(run_oid),
        tgfoid=str(function_oid),
        tgparentid="0",
        tgenabled="A",
        tgisinternal=False,
        tgconstrrelid="0",
        tgconstrindid="0",
        tgconstraint="0",
        tgdeferrable=False,
        tginitdeferred=False,
        tgnargs=0,
        tgqual=None,
        tgoldtable=None,
        tgnewtable=None,
    )
    return (
        set(hook) == {*required_by_field, "oid", "tgname", "tgtype", "tgattr", "tgargs"}
        and all(type(hook[key]) is type(value) and hook[key] == value for key, value in required_by_field.items())
        and hook["tgname"] in _RUN_HOOKS
        and type(hook["tgtype"]) is int
        and hook["tgtype"] == _RUN_HOOKS[hook["tgname"]]
        and (hook["tgattr"] == [] or hook["tgattr"] == "")
        and type(hook["tgargs"]) is str
        and hook["tgargs"] in {"\\x", ""}
    )


def _identity(hook):
    return ":".join(str(hook.get(key)) for key in ("oid", "tgrelid", "tgfoid", "tgname"))


def _require_hooks(hooks, expected, run_oid, retirement_oid):
    _require(type(hooks) is list, "bindings_required")
    seen, run_names = set(), set()
    for hook in hooks:
        identity = _oid(hook["oid"])
        _require(identity not in seen, "duplicate_binding:" + _identity(hook))
        seen.add(identity)
        if identity in expected:
            _require(hook == expected[identity], "binding_changed:" + _identity(hook))
        else:
            _require(_run_hook(hook, run_oid, retirement_oid), "external_binding:" + _identity(hook))
            _require(hook["tgname"] not in run_names, "duplicate_import_run_hook")
            run_names.add(hook["tgname"])
    _require(expected.keys() <= seen and run_names == _RUN_HOOKS.keys(), "bindings_missing")


async def _original(connection, task, actor_by_field, deadline):
    _deadline(deadline)
    _require(asyncio.current_task() is task, "original_task_required")
    _require(connection.is_in_transaction() is True and connection.is_closed() is False, "transaction_required")
    _require(connection.get_server_pid() == actor_by_field["backend"], "physical_connection_changed")
    await custody._require_allocation_resolution(connection)
    _require(dict(await connection.fetchrow(custody._ALLOCATION_ACTOR_SQL)) == actor_by_field, "original_admin_changed")


async def _bindings(connection, function_oids, catalog_oids):
    observed = await connection.fetchrow(_REVERSE_SQL, function_oids)
    _require(observed is not None, "bindings_required")
    _require(
        await connection.fetchval(
            custody._ALLOCATION_LOCKS_SQL, sorted({*catalog_oids, _oid(observed["catalog_oid"])}), ["ShareLock"]
        )
        is True,
        "catalog_locks_required",
    )
    return dict(catalog_oid=_oid(observed["catalog_oid"]), bindings=custody._decoded(observed["bindings"]))


@asynccontextmanager
async def cms_reverse_hook_custody(
    connection, *, inventory, storage_binding, policy, cms_profile, source_document, deadline
):
    """Yield a same-task recheck; caller retains one <=3s clock through commit.

    Existing routine/role catalog locks are reacquired before pg_trigger. The
    operator must call the recheck before commit and validate its intentional
    owner/body/ACL/Source/profile postimage separately. No old digest is reloaded
    after intentional changes. Exit only invalidates this local callback.
    """
    _deadline(deadline)
    inventory, cms_profile, source_document = deepcopy((inventory, cms_profile, source_document))
    function_oids, run_oid, retirement_oid, excluded_roles = _inputs(
        inventory, storage_binding, policy, cms_profile, source_document
    )
    _deadline(deadline)
    task, has_scope = asyncio.current_task(), True
    try:
        async with asyncio.timeout_at(deadline):
            _require(connection.is_in_transaction() is True and connection.is_closed() is False, "transaction_required")
            await custody._require_allocation_resolution(connection)
            actor_by_field = dict(await connection.fetchrow(custody._ALLOCATION_ACTOR_SQL))
            _require(
                actor_by_field["administrator"] is True
                and actor_by_field["session"] == actor_by_field["current"]
                and actor_by_field["current"] not in excluded_roles
                and actor_by_field["backend"] == connection.get_server_pid()
                and actor_by_field["database_oid"] == storage_binding.database_oid
                and actor_by_field["transaction"]
                and actor_by_field["replication"] == "origin"
                and actor_by_field["readonly"] == "off",
                "original_admin_required",
            )
            catalog_oids = await custody._lock_allocation_catalogs(connection)
            _require(
                type(catalog_oids) is list
                and len({_oid(oid) for oid in catalog_oids}) == len(catalog_oids) == len(custody._ALLOCATION_CATALOGS),
                "catalog_identity_changed",
            )
            await connection.execute("LOCK TABLE pg_catalog.pg_trigger IN SHARE MODE NOWAIT")
            before = await _bindings(connection, function_oids, catalog_oids)
            dynamic_relations = await _dynamic_relations(connection, inventory, storage_binding, policy, cms_profile)
            expected = _expected_hooks(inventory, function_oids, dynamic_relations)
            _require_hooks(before["bindings"], expected, run_oid, retirement_oid)
            await _original(connection, task, actor_by_field, deadline)

            async def recheck():
                """Check the same native hook scope before the caller commits."""
                _require(has_scope, "inactive_context")
                async with asyncio.timeout_at(deadline):
                    await _original(connection, task, actor_by_field, deadline)
                    current = await _bindings(connection, function_oids, catalog_oids)
                    _require(current == before, "bindings_changed")
                    await _original(connection, task, actor_by_field, deadline)

            yield recheck
    finally:
        has_scope = False
