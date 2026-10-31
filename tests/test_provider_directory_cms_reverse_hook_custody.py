# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Short reverse-hook custody preserves exact pins and the original Admin TX."""

import asyncio
import json
from copy import deepcopy
from time import sleep
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db import migration_provider_directory_first_acquisition_retirement as retirement
from process import provider_directory_cms_reverse_hook_custody as reverse
from tests.test_provider_directory_cms_callee_custody import _catalog_definition, _parse_statements
from tests.test_provider_directory_cms_owner_permissions import _authenticate
from tests.test_provider_directory_cms_owner_permissions import owner_case as owner_case
from tests.test_provider_directory_cms_transition_routines import _raw


def _hook(oid, relation, function, name, mask=31):
    return dict(
        oid=str(oid),
        tgrelid=str(relation),
        tgparentid="0",
        tgname=name,
        tgfoid=str(function),
        tgtype=mask,
        tgenabled="A",
        tgisinternal=False,
        tgconstrrelid="0",
        tgconstrindid="0",
        tgconstraint="0",
        tgdeferrable=False,
        tginitdeferred=False,
        tgnargs=0,
        tgattr=[],
        tgargs="\\x",
        tgqual=None,
        tgoldtable=None,
        tgnewtable=None,
    )


def _retirement_routine():
    name = reverse._RUN_GUARD
    spec = _parse_statements([retirement.run_sql("catalog")], "catalog")[name]
    routine = _raw(dict(definition=_catalog_definition(name, spec, 101, 9000), language=spec[3]), "catalog")
    routine["native"]["acl"] = [[101, 101, "EXECUTE", False]]
    return routine


def _mock_connection(case):
    async def fetchrow(statement, *arguments):
        if statement == reverse.custody._ALLOCATION_ACTOR_SQL:
            return deepcopy(case.actor)
        assert statement == reverse._REVERSE_SQL
        assert arguments == (case.function_oids,)
        bindings = deepcopy(case.hooks)
        return dict(catalog_oid=2620, bindings=json.dumps(bindings) if case.json_codec else bindings)

    async def fetchval(statement, *arguments):
        if statement == "SHOW search_path":
            return case.actor["search_path"]
        if statement == reverse.custody._ALLOCATION_LOCKS_SQL:
            assert arguments == (sorted({*case.catalog_oids, 2620}), ["ShareLock"])
            return case.locks
        assert arguments == (list(reverse.custody._ALLOCATION_CATALOGS),)
        return case.catalog_oids.copy()

    return SimpleNamespace(
        fetchrow=AsyncMock(side_effect=fetchrow),
        fetchval=AsyncMock(side_effect=fetchval),
        execute=AsyncMock(),
        is_in_transaction=Mock(return_value=True),
        is_closed=Mock(return_value=False),
        get_server_pid=Mock(return_value=91),
    )


def _mock_canonical_family(case):
    async def canonical_family(connection, storage, policy, retained):
        assert connection is case.connection and storage == case.storage and policy == case.policy
        assert retained == case.profile["fixed_descendant_oids"]
        assert case.connection.execute.await_args.args == ("LOCK TABLE pg_catalog.pg_trigger IN SHARE MODE NOWAIT",)
        assert case.connection.fetchval.await_args.args[0] == reverse.custody._ALLOCATION_LOCKS_SQL
        return deepcopy(case.fixed), deepcopy(case.family)

    return AsyncMock(side_effect=canonical_family)


@pytest.fixture
def reverse_case(owner_case, monkeypatch):
    case = owner_case
    case.inventory["routines"].append(_retirement_routine())
    canonical = case.inventory["canonical"]
    canonical.setdefault("dependency_histories", [])
    relations = [*canonical["relations"], *canonical["dependency_relations"], *canonical["dependency_histories"]]
    for relation in relations:
        relation["native"]["triggers"] = []
    callback = next(
        routine
        for routine in case.inventory["routines"]
        if routine["native"]["definition"]["proname"] == "guard_pd_dataset_candidate_parent"
    )
    protected = _hook(8000, relations[0]["oid"], callback["oid"], "protected_callback")
    relations[0]["native"]["triggers"] = [deepcopy(protected)]
    case.inventory["private"] = [
        dict(oid=60000, native=dict(triggers=[_hook(8001, 60000, callback["oid"], "private_callback")]))
    ]
    admission_by_name = {
        relation["relname"]: {key: relation[key] for key in ("oid", "relname", "nspname")}
        for relation in canonical["dependency_relations"]
        if relation["relname"] in {"provider_directory_source", "provider_directory_api_endpoint"}
    }
    admission_by_name["import_run"] = dict(oid=50000, nspname="catalog", relname="import_run")
    case.source["storage_custody"]["canonical_authority"]["admission"] = admission_by_name
    _authenticate(case)
    case.function_oids, _ = reverse._routine_oids(case.inventory)
    case.hooks = [
        protected,
        deepcopy(case.inventory["private"][0]["native"]["triggers"][0]),
        _hook(8002, 50000, 9000, "pd_trr_import_run_row"),
        _hook(8003, 50000, 9000, "pd_trr_import_run_truncate", 34),
    ]
    case.actor = dict(
        session="original_admin",
        current="original_admin",
        backend=91,
        database_oid=1,
        replication="origin",
        transaction="1/1",
        administrator=True,
        role_oid=10,
        readonly="off",
        isolation="serializable",
        search_path="pg_catalog, pg_temp",
    )
    case.catalog_oids = [1255, 1260, 1261, 2612, 2964, 3466]
    case.locks, case.json_codec = True, False
    case.connection = _mock_connection(case)
    case.fixed = {key: deepcopy(canonical[key]) for key in ("relations", "migration_plan")}
    case.family = deepcopy(canonical["relations"])
    case.canonical_family = _mock_canonical_family(case)
    monkeypatch.setattr(reverse.policy_checks, "_canonical", case.canonical_family)
    return case


def _context(case, **changes):
    arguments_by_name = dict(
        inventory=case.inventory,
        storage_binding=case.storage,
        policy=case.policy,
        cms_profile=case.profile,
        source_document=case.source,
        deadline=asyncio.get_running_loop().time() + 2.9,
    )
    arguments_by_name.update(changes)
    return reverse.cms_reverse_hook_custody(case.connection, **arguments_by_name)


@pytest.mark.parametrize("json_codec", [False, True])
async def test_exact_protected_and_source_import_hooks_keep_short_locks(reverse_case, json_codec):
    case = reverse_case
    case.json_codec = json_codec
    before = deepcopy((case.inventory, case.source, case.profile))
    async with _context(case) as recheck:
        assert callable(recheck)
        await recheck()
    assert (case.inventory, case.source, case.profile) == before
    statements = [call.args[0] for call in case.connection.execute.await_args_list]
    assert statements == [
        "LOCK TABLE "
        + ",".join("pg_catalog." + name for name in reverse.custody._ALLOCATION_CATALOGS)
        + " IN SHARE MODE NOWAIT",
        "LOCK TABLE pg_catalog.pg_trigger IN SHARE MODE NOWAIT",
    ]
    assert case.connection.is_in_transaction()


def _dynamic_child(case):
    parent = next(row for row in case.family if row["relname"] == "provider_directory_dataset_resource")
    hooks = []
    function_oid = case.hooks[0]["tgfoid"]
    for number, (operation, image, mask) in enumerate(
        (("insert", "NEW", 4), ("update", "NEW", 16), ("update", "OLD", 16), ("delete", "OLD", 8))
    ):
        hook = _hook(8100 + number, 61000, function_oid, "a_pd_candidate_" + operation + "_" + image.lower(), mask)
        hook.update(
            tgenabled="O",
            tgnargs=2,
            tgargs="\\x" + (parent["relname"] + "\0" + image + "\0").encode().hex(),
            tgoldtable="affected_rows" if image == "OLD" else None,
            tgnewtable="affected_rows" if image == "NEW" else None,
        )
        hooks.append(hook)
    child = deepcopy(parent)
    child.update(oid=61000, relname="qualified_child")
    child["native"].update(parents=[parent["oid"]], triggers=hooks)
    case.family.append(child)
    case.hooks.extend(deepcopy(hooks))
    return child


async def test_qualified_dynamic_hooks_share_the_locked_fixed_inventory_and_recheck(reverse_case):
    case = reverse_case
    child = _dynamic_child(case)
    before = deepcopy((case.inventory, case.source, case.profile))
    async with _context(case) as recheck:
        await recheck()
        assert child["oid"] not in {row["oid"] for row in case.inventory["canonical"]["relations"]}
    case.canonical_family.assert_awaited_once()
    assert (case.inventory, case.source, case.profile) == before


async def test_protected_dependency_hook_needs_no_external_exception(reverse_case):
    case = reverse_case
    relation = case.inventory["canonical"]["dependency_relations"][0]
    hook = _hook(8200, relation["oid"], case.hooks[0]["tgfoid"], "protected_dependency_callback")
    relation["native"]["triggers"].append(deepcopy(hook))
    case.hooks.append(hook)
    _authenticate(case)
    async with _context(case) as recheck:
        await recheck()


def _history_hooks(case):
    histories = case.inventory["canonical"]["dependency_histories"]
    assert len(histories) == 2
    hooks = [
        _hook(8300 + index, history["oid"], case.hooks[0]["tgfoid"], "protected_history_" + str(index), 34)
        for index, history in enumerate(histories)
    ]
    for history, hook in zip(histories, hooks, strict=True):
        history["native"]["triggers"] = [deepcopy(hook)]
    case.hooks.extend(hooks)
    _authenticate(case)
    return hooks


async def test_both_protected_histories_use_exact_expected_hooks(reverse_case):
    case = reverse_case
    _history_hooks(case)
    async with _context(case) as recheck:
        await recheck()


@pytest.mark.parametrize("index", [0, 1])
@pytest.mark.parametrize("fault", ["missing", "unissued", "disabled"])
async def test_each_protected_history_binding_is_required_without_an_exception(reverse_case, index, fault):
    case = reverse_case
    hook = _history_hooks(case)[index]
    if fault == "missing":
        case.hooks.remove(hook)
    elif fault == "unissued":
        hook["oid"] = "8399"
    else:
        hook["tgenabled"] = "D"
    with pytest.raises(ValueError, match="binding"):
        async with _context(case):
            pytest.fail("missing or changed history hook entered")


@pytest.mark.parametrize("fault", ["relation", "hook"])
async def test_history_union_keeps_relation_and_hook_identities_unique(reverse_case, fault):
    case = reverse_case
    _history_hooks(case)
    histories = case.inventory["canonical"]["dependency_histories"]
    if fault == "relation":
        case.inventory["canonical"]["dependency_relations"].append(deepcopy(histories[0]))
    else:
        histories[1]["native"]["triggers"][0]["oid"] = histories[0]["native"]["triggers"][0]["oid"]
    _authenticate(case)
    with pytest.raises(ValueError, match="relation_identity_changed|duplicate_hook_identity"):
        async with _context(case):
            pytest.fail("duplicate protected identity entered")


@pytest.mark.parametrize("field", ["owner", "columns", "hooks", "plan", "missing", "extra"])
async def test_fresh_fixed_drift_cannot_authenticate_dynamic_hooks(reverse_case, field):
    case = reverse_case
    _dynamic_child(case)
    relation = case.fixed["relations"][0]
    mutations_by_field = {
        "owner": lambda: relation.update(relowner=relation["relowner"] + 1),
        "columns": lambda: relation["native"]["columns"][0]["attribute"].update(attname="changed"),
        "hooks": lambda: relation["native"]["triggers"][0].update(tgargs="\\x01"),
        "plan": lambda: case.fixed["migration_plan"].update(complete=False),
        "missing": lambda: case.fixed["relations"].pop(),
        "extra": lambda: case.fixed.update(unprotected_field=True),
    }
    mutations_by_field[field]()
    with pytest.raises(ValueError, match="canonical_changed"):
        async with _context(case):
            pytest.fail("unbound fixed catalog authenticated a dynamic child")


@pytest.mark.parametrize("phase", ["header_changed", "guards_changed", "storage_changed"])
async def test_allocation_qualification_refusal_is_preserved(reverse_case, phase):
    case = reverse_case
    _dynamic_child(case)
    failure = ValueError("cms_allocation_semantics_" + phase)
    case.canonical_family.side_effect = failure
    with pytest.raises(ValueError) as caught:
        async with _context(case):
            pytest.fail("refused allocation supplied accepted hooks")
    assert caught.value is failure
    assert case.connection.is_in_transaction()


@pytest.mark.parametrize("change", ["missing", "argument", "disabled", "relation", "extra"])
async def test_dynamic_reverse_binding_must_equal_its_qualified_hook(reverse_case, change):
    case = reverse_case
    _dynamic_child(case)
    if change == "missing":
        case.hooks.pop()
    elif change == "extra":
        case.hooks.append({**case.hooks[-1], "oid": "8199"})
    else:
        field, value = {
            "argument": ("tgargs", "\\x01"),
            "disabled": ("tgenabled", "D"),
            "relation": ("tgrelid", "61001"),
        }[change]
        case.hooks[-1][field] = value
    with pytest.raises(ValueError, match="binding"):
        async with _context(case):
            pytest.fail("unqualified reverse binding entered")


async def test_dynamic_hook_change_after_entry_refuses_without_reloading_old_profile(reverse_case):
    case = reverse_case
    _dynamic_child(case)
    async with _context(case) as recheck:
        case.hooks[-1]["tgenabled"] = "D"
        with pytest.raises(ValueError, match="bindings_changed"):
            await recheck()
    case.canonical_family.assert_awaited_once()


@pytest.mark.parametrize("section", ["canonical", "private", "routines", "roles"])
async def test_unbound_inventory_refuses_before_any_sql(reverse_case, section):
    case = reverse_case
    case.inventory[section] = []
    with pytest.raises(ValueError, match="inventory_changed"):
        async with _context(case):
            pytest.fail("tampered inventory entered")
    case.connection.fetchrow.assert_not_awaited()
    case.connection.execute.assert_not_awaited()


@pytest.mark.parametrize("field", ["manifest_id", "version", "publication"])
async def test_changed_source_refuses_before_any_sql(reverse_case, field):
    case = reverse_case
    case.source[field] = "changed"
    with pytest.raises(ValueError, match="source_changed"):
        async with _context(case):
            pytest.fail("tampered Source entered")
    case.connection.fetchrow.assert_not_awaited()


async def test_unknown_source_function_is_not_authenticated_by_a_rehashed_profile(reverse_case):
    case = reverse_case
    case.inventory["routines"][-1]["native"]["definition"]["prosrc"] += "\nRETURN NULL;"
    _authenticate(case)
    with pytest.raises(ValueError, match="definition_changed"):
        async with _context(case):
            pytest.fail("changed callback entered")
    case.connection.fetchrow.assert_not_awaited()


@pytest.mark.parametrize("mode", ["A", "O", "D", "R"])
async def test_every_unknown_external_binding_refuses_with_its_identity(reverse_case, mode):
    case = reverse_case
    case.hooks.append({**case.hooks[0], "oid": "9999", "tgrelid": "7777", "tgname": "unexpected", "tgenabled": mode})
    with pytest.raises(ValueError, match="external_binding:9999:7777:.*:unexpected"):
        async with _context(case):
            pytest.fail("unprotected callback entered")


@pytest.mark.parametrize(
    "field,value",
    [
        ("tgrelid", "50001"),
        ("tgfoid", "9001"),
        ("tgname", "other"),
        ("tgtype", 27),
        ("tgenabled", "D"),
        ("tgisinternal", True),
        ("tgconstraint", "1"),
        ("tgparentid", "1"),
        ("tgconstrrelid", "1"),
        ("tgconstrindid", "1"),
        ("tgdeferrable", True),
        ("tginitdeferred", True),
        ("tgnargs", 1),
        ("tgattr", [1]),
        ("tgargs", "\\x01"),
        ("tgqual", "true"),
        ("tgoldtable", "old_rows"),
        ("tgnewtable", "new_rows"),
        ("extra_catalog_field", "unqualified"),
    ],
)
async def test_import_run_exception_keeps_the_complete_source_defined_shape(reverse_case, field, value):
    case = reverse_case
    case.hooks[2][field] = value
    with pytest.raises(ValueError, match="external_binding:"):
        async with _context(case):
            pytest.fail("changed import-run hook entered")


@pytest.mark.parametrize("fault", ["missing", "duplicate", "protected_changed", "run_missing"])
async def test_missing_or_replaced_protected_bindings_refuse(reverse_case, fault):
    case = reverse_case
    if fault == "missing":
        case.hooks.pop(0)
    elif fault == "duplicate":
        case.hooks.append(deepcopy(case.hooks[0]))
    elif fault == "protected_changed":
        case.hooks[0]["tgenabled"] = "D"
    else:
        case.hooks.pop()
    with pytest.raises(ValueError, match="binding|bindings"):
        async with _context(case):
            pytest.fail("changed topology entered")


async def test_import_run_oid_comes_only_from_the_authenticated_source(reverse_case):
    case = reverse_case
    case.source["storage_custody"]["canonical_authority"]["admission"]["import_run"]["oid"] += 1
    _authenticate(case)
    with pytest.raises(ValueError, match="external_binding:"):
        async with _context(case):
            pytest.fail("observed relation became expected authority")


@pytest.mark.parametrize("deadline", [None, True, 0, float("inf"), float("nan")])
async def test_bad_deadline_refuses_before_sql(reverse_case, deadline):
    case = reverse_case
    with pytest.raises(ValueError, match="deadline_required"):
        async with _context(case, deadline=deadline):
            pytest.fail("invalid deadline entered")
    case.connection.fetchrow.assert_not_awaited()


async def test_extended_deadline_is_not_a_new_cut_budget(reverse_case):
    case = reverse_case
    with pytest.raises(ValueError, match="deadline_required"):
        async with _context(case, deadline=asyncio.get_running_loop().time() + 3.1):
            pytest.fail("extended deadline entered")
    case.connection.fetchrow.assert_not_awaited()


async def test_expiry_during_pure_authentication_refuses_before_sql(reverse_case, monkeypatch):
    case = reverse_case
    inputs = reverse._inputs

    def delayed_inputs(*arguments):
        expected = inputs(*arguments)
        sleep(0.03)
        return expected

    monkeypatch.setattr(reverse, "_inputs", delayed_inputs)
    with pytest.raises(ValueError, match="deadline_required"):
        async with _context(case, deadline=asyncio.get_running_loop().time() + 0.01):
            pytest.fail("expired authentication reached SQL")
    case.connection.fetchrow.assert_not_awaited()
    case.connection.fetchval.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("administrator", False),
        ("current", "another_actor"),
        ("backend", 92),
        ("database_oid", 2),
        ("transaction", None),
        ("replication", "replica"),
        ("readonly", "on"),
    ],
)
async def test_only_original_admin_enters_the_catalog_scope(reverse_case, field, value):
    case = reverse_case
    case.actor[field] = value
    with pytest.raises(ValueError, match="original_admin_required"):
        async with _context(case):
            pytest.fail("invalid actor entered")
    case.connection.execute.assert_not_awaited()


@pytest.mark.parametrize("role", ["source_owner", "publication_publisher", "preparation_owner", "protected_owner"])
async def test_declared_source_and_publication_actors_cannot_become_admin(reverse_case, role):
    case = reverse_case
    case.actor.update(session=role, current=role)
    with pytest.raises(ValueError, match="original_admin_required"):
        async with _context(case):
            pytest.fail("Source actor entered as Admin")


@pytest.mark.parametrize("path", ["pg_catalog", "public, pg_catalog", "pg_temp, pg_catalog"])
async def test_unsafe_resolution_refuses_before_actor_query(reverse_case, path):
    case = reverse_case
    case.actor["search_path"] = path
    with pytest.raises(ValueError, match="resolution_required"):
        async with _context(case):
            pytest.fail("unsafe resolution entered")
    case.connection.fetchrow.assert_not_awaited()


async def test_catalog_locks_are_proved_in_the_same_native_backend(reverse_case):
    case = reverse_case
    case.locks = False
    with pytest.raises(ValueError, match="catalog_locks_required"):
        async with _context(case):
            pytest.fail("unheld metadata scope entered")


async def test_unknown_catalog_identity_cannot_shrink_the_existing_lock_set(reverse_case):
    case = reverse_case
    case.catalog_oids.pop()
    with pytest.raises(ValueError, match="catalog_identity_changed"):
        async with _context(case):
            pytest.fail("partial catalog scope entered")


@pytest.mark.parametrize(
    "fault", ["transaction", "ended", "closed", "backend", "actor", "search_path", "hooks", "locks"]
)
async def test_recheck_retains_original_physical_custody(reverse_case, fault):
    case = reverse_case
    mutation_by_fault = {
        "transaction": lambda: case.actor.update(transaction="1/2"),
        "ended": lambda: setattr(case.connection.is_in_transaction, "return_value", False),
        "closed": lambda: setattr(case.connection.is_closed, "return_value", True),
        "backend": lambda: setattr(case.connection.get_server_pid, "return_value", 92),
        "actor": lambda: case.actor.update(current="another_actor"),
        "search_path": lambda: case.actor.update(search_path="public, pg_catalog"),
        "hooks": lambda: case.hooks[0].update(tgname="changed"),
        "locks": lambda: setattr(case, "locks", False),
    }
    async with _context(case) as recheck:
        mutation_by_fault[fault]()
        with pytest.raises(ValueError):
            await recheck()


async def test_an_inherited_child_task_cannot_use_the_recheck(reverse_case):
    case = reverse_case
    async with _context(case) as recheck:
        before = case.connection.fetchrow.await_count
        with pytest.raises(ValueError, match="original_task_required"):
            await asyncio.create_task(recheck())
        assert case.connection.fetchrow.await_count == before


async def test_exit_and_expired_callback_do_no_sql_after_commit(reverse_case):
    case = reverse_case
    async with _context(case) as recheck:
        await recheck()
        before = (case.connection.fetchrow.await_count, case.connection.fetchval.await_count)
        case.connection.is_in_transaction.return_value = False
    assert (case.connection.fetchrow.await_count, case.connection.fetchval.await_count) == before
    with pytest.raises(ValueError, match="inactive_context"):
        await recheck()
    assert (case.connection.fetchrow.await_count, case.connection.fetchval.await_count) == before


async def test_ownership_postimage_is_left_to_the_outer_validator(reverse_case):
    case = reverse_case
    async with _context(case) as recheck:
        case.inventory["routines"][0]["native"]["definition"]["proowner"] = "104"
        case.source["manifest_id"] = "intentional_successor"
        await recheck()


async def test_timeout_has_no_cleanup_sql_and_preserves_caller_transaction(reverse_case):
    case = reverse_case
    with pytest.raises(TimeoutError):
        async with _context(case, deadline=asyncio.get_running_loop().time() + 0.1):
            before = (case.connection.fetchrow.await_count, case.connection.fetchval.await_count)
            await asyncio.Event().wait()
    assert (case.connection.fetchrow.await_count, case.connection.fetchval.await_count) == before
    assert case.connection.is_in_transaction()


async def test_cancellation_keeps_the_first_error_and_invalidates_only_locally(reverse_case):
    case = reverse_case
    failure = asyncio.CancelledError("interrupted")
    with pytest.raises(asyncio.CancelledError) as caught:
        async with _context(case) as recheck:
            before = (case.connection.fetchrow.await_count, case.connection.fetchval.await_count)
            raise failure
    assert caught.value is failure
    assert (case.connection.fetchrow.await_count, case.connection.fetchval.await_count) == before
    with pytest.raises(ValueError, match="inactive_context"):
        await recheck()
    assert case.connection.is_in_transaction()


async def test_lock_failure_is_preserved_without_cleanup_sql(reverse_case):
    case = reverse_case
    failure = PermissionError("busy catalog")

    async def execute(statement):
        if statement == "LOCK TABLE pg_catalog.pg_trigger IN SHARE MODE NOWAIT":
            raise failure

    case.connection.execute.side_effect = execute
    with pytest.raises(PermissionError) as caught:
        async with _context(case):
            pytest.fail("failed lock entered")
    assert caught.value is failure
    assert case.connection.fetchrow.await_count == 1
    assert case.connection.execute.await_count == 2


def test_reverse_query_never_ignores_disabled_or_external_hooks():
    assert "WHERE t.tgfoid=ANY($1::oid[])" in reverse._REVERSE_SQL
    assert "t.tgenabled" not in reverse._REVERSE_SQL
    assert "t.tgrelid" not in reverse._REVERSE_SQL
    assert "ORDER BY t.oid" in reverse._REVERSE_SQL
