# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The terminal component needs actual locks and independently checked callees."""

import asyncio
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db.migration_cms_retirement_resolution import retirement_resolution_bodies
from db.migration_generic_source_candidates import _canonical_copy_guard_bodies, _cms_canonical_copy_guard_body
from process import provider_directory_cms_callee_inventory as callee_inventory
from process import provider_directory_cms_canonical_writer_fence as writers
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding
from tests.test_provider_directory_cms_callee_custody import _SOURCE_ROLES, _allocator_row, _catalog_rows
from tests.test_provider_directory_cms_callee_inventory import _namespace_source_rows, _source_metadata
from tests.test_provider_directory_cms_proof_callees import source_case as source_proof_case
from tests.test_provider_directory_cms_terminal_guard_resolution import terminal_guard_routines
from tests.test_provider_directory_cms_transition_routines import _native_foreign, _raw
from tests.test_provider_directory_generic_source_release_checks import legacy_catalog_entry


@pytest.fixture(scope="module")
def authentic_callees():
    rows = _catalog_rows("catalog")
    typed = retirement_resolution_bodies("catalog")
    for row in rows:
        if row["definition"]["proname"] == "guard_pd_dataset_candidate_parent":
            row["definition"]["prosrc"] = _cms_canonical_copy_guard_body("catalog", "preparation_schema")
        if row["definition"]["proname"] in typed:
            row["definition"]["prosrc"] = typed[row["definition"]["proname"]][1]
        _target_settings(row)
    return rows


def _target_settings(function_row):
    definition = function_row["definition"]
    definition["proconfig"] = writers.routine_settings.approved_cms_routine_settings(definition["proconfig"])[1]


def _terminal_routines(schema="catalog"):
    routines = []
    for routine in terminal_guard_routines(schema):
        function_by_field = dict(
            definition=routine["native"]["definition"], language=routine["native"]["language"]["lanname"]
        )
        _target_settings(function_by_field)
        routines.append(_raw(function_by_field, schema))
    return routines


def _retained_routine():
    routine = _native_foreign("RI_FKey_cascade_del")
    routine["native"]["namespace"][2] = 10
    return routine


def _relation(oid, name, parents=()):
    return dict(
        oid=oid,
        nspname="catalog",
        relname=name,
        relowner=108,
        relkind="r",
        relpersistence="p",
        native={"parents": list(parents)},
    )


def _actor():
    return dict(
        session="administrator",
        current="administrator",
        backend=91,
        database_oid=1,
        replication="origin",
        transaction="1/1",
        isolation="read committed",
        readonly="off",
        search_path="pg_catalog",
        superuser=True,
    )


def _foreign(parent=1, child=2, action="c"):
    return {
        "constraint": {
            "oid": "50",
            "conrelid": str(child),
            "confrelid": str(parent),
            "convalidated": True,
            "conenforced": True,
            "confupdtype": action,
            "confdeltype": action,
        },
        "hooks": [
            {
                "tgisinternal": True,
                "tgenabled": "O",
                "tgconstraint": "50",
                "tgqual": None,
                "tgnargs": 0,
                "tgattr": [],
                "tgfoid": "99",
            }
        ],
    }


def _callee_arguments(function_rows, storage):
    return dict(
        catalog_schema=storage.catalog_schema,
        catalog_schema_oid=storage.catalog_schema_oid,
        expected_functions={
            entry["definition"]["proname"]: dict(
                oid=int(entry["definition"]["oid"]), sha256=callee_inventory.callee_fingerprint(entry)
            )
            for entry in function_rows
        },
        namespace_owner_oid=_SOURCE_ROLES["namespace_owner"],
        preparation_schema="preparation_schema",
    )


@pytest.fixture
def case(monkeypatch, authentic_callees):
    policy = GenericSourcePolicy("private_source", "prepared_owner", "prepared_publisher", ("reader",))
    storage = FHIRSourceStorageBinding("catalog", policy.schema, 1, 42, 3, 4, 5, "a" * 64)
    family_rows = [_relation(1, "parent"), _relation(2, "retained_child", [1]), _relation(3, "detached_history")]
    canonical_by_field = {"relations": family_rows, "migration_plan": {"complete": True}}
    state = SimpleNamespace(
        actor=_actor(),
        family=family_rows,
        functions=deepcopy(authentic_callees),
        allocator_rows=[],
        routines=[
            native_routine
            for native_routine in _terminal_routines()
            if native_routine["native"]["definition"]["proname"] not in writers.callables._cms_callee_contracts()
        ],
        foreign=[],
        locks=False,
        history_locks=False,
        owners=True,
        dependencies=True,
        callable_owners=True,
        definers=True,
        events=[],
    )
    profile_by_field = dict(
        contract=writers.policy_checks.CONTRACT,
        source_ids=["cms-npd"],
        storage=asdict(storage),
        policy=writers.json.loads(writers._encoded(asdict(policy))),
        fixed_descendant_oids=[2],
        canonical_sha256=writers.custody._digest(canonical_by_field),
        private_sha256="b" * 64,
        routines_sha256="c" * 64,
        roles_sha256="d" * 64,
    )
    canonical_reader = AsyncMock(return_value=(canonical_by_field, family_rows))
    monkeypatch.setattr(
        writers.policy_checks,
        "_observe",
        AsyncMock(return_value={field: profile_by_field[field] for field in writers.custody._DIGEST_FIELDS}),
    )
    monkeypatch.setattr(writers.policy_checks, "_coordinates", AsyncMock())
    monkeypatch.setattr(writers.policy_checks, "_canonical", canonical_reader)
    monkeypatch.setattr(writers.custody, "_inventory", AsyncMock(side_effect=lambda *args: deepcopy(state.family)))
    argument_by_name = dict(
        storage_binding=storage,
        policy=policy,
        cms_profile=profile_by_field,
        callee_arguments=_callee_arguments(authentic_callees, storage),
        migration_role="migration",
    )
    return SimpleNamespace(
        connection=_connection(state), state=state, arguments=argument_by_name, canonical_reader=canonical_reader
    )


def _connection(state):
    async def fetch(query, *args):
        state.events.append(("fetch", query))
        if query == writers.callables._FUNCTION_SQL:
            return deepcopy(state.functions)
        if query == writers.callables._FUNCTION_SQL + " FOR SHARE OF p NOWAIT":
            return deepcopy(state.allocator_rows)
        if query == writers.custody._ROUTINES_SQL:
            return [_raw(row, "catalog") for row in state.functions] + deepcopy(state.routines)
        if query == writers._FOREIGN_SQL:
            return deepcopy(state.foreign)
        if query == writers._IDENTITY_SQL:
            return writers._identity(state.family)
        raise AssertionError(query)

    async def fetchval(query, *args):
        state.events.append(("fetchval", query))
        if query == writers._LOCKS_SQL:
            return state.locks
        if query == writers._HISTORY_LOCKS_SQL:
            return state.history_locks
        if query == writers._OWNERS_SQL:
            return state.owners
        if query == writers._DEPENDENCIES_SQL:
            return state.dependencies
        if query == writers.custody._CALLABLE_OWNER_SQL:
            return state.callable_owners
        if query == writers._DEFINER_AUTHORITY_SQL:
            return state.definers
        assert "RI_FKey_" not in query and "p.prosrc=p.proname" in query
        return True

    async def execute(query):
        state.events.append(("execute", query))
        if " IN SHARE MODE " in query:
            state.history_locks = True
        else:
            state.locks = True

    return SimpleNamespace(
        fetch=AsyncMock(side_effect=fetch),
        fetchval=AsyncMock(side_effect=fetchval),
        fetchrow=AsyncMock(side_effect=lambda *args: dict(state.actor)),
        execute=AsyncMock(side_effect=execute),
        is_in_transaction=Mock(return_value=True),
        is_closed=Mock(return_value=False),
        get_server_pid=Mock(return_value=91),
    )


async def _prepare(case):
    return await writers.prepare_cms_canonical_writer_fence(
        case.connection, **case.arguments, deadline=asyncio.get_running_loop().time() + 10
    )


async def _acquire(case, preparation):
    return await writers.acquire_cms_canonical_writer_fence(
        case.connection, preparation, deadline=asyncio.get_running_loop().time() + 5
    )


@pytest.mark.asyncio
async def test_preflight_then_actual_nowait_locks_and_catalog_only_rechecks(case):
    preparation = await _prepare(case)
    case.connection.execute.assert_not_awaited()
    fence = await _acquire(case, preparation)
    assert case.connection.execute.await_args.args == (
        'LOCK TABLE ONLY "catalog"."detached_history",ONLY "catalog"."parent",'
        'ONLY "catalog"."retained_child" IN SHARE ROW EXCLUSIVE MODE NOWAIT',
    )
    await writers.require_cms_canonical_writer_fence(case.connection, fence)
    case.canonical_reader.assert_awaited_once()
    assert case.connection.execute.await_count == 1
    assert not hasattr(fence, "commit")
    assert sum(query == writers._DEFINER_AUTHORITY_SQL for _, query in case.state.events) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("guard", [0, 1])
async def test_preflight_refuses_old_terminal_guard(case, guard):
    from db.migration_cms_replay_resolution import replay_guard_bodies
    from db.migration_cms_snapshot_allocator import generic_finalization_bodies

    renderer = generic_finalization_bodies if guard == 0 else replay_guard_bodies
    case.state.routines[guard]["native"]["definition"]["prosrc"] = renderer("catalog")[0]
    with pytest.raises(ValueError, match="terminal_guard_resolution_required"):
        await _prepare(case)
    case.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_generic_parent_refuses_terminal_even_with_rebound_fingerprint(case):
    parent = next(
        row for row in case.state.functions if row["definition"]["proname"] == "guard_pd_dataset_candidate_parent"
    )
    parent["definition"]["prosrc"] = _canonical_copy_guard_bodies("catalog", "preparation_schema")[1]
    case.arguments["callee_arguments"]["expected_functions"][parent["definition"]["proname"]] = dict(
        oid=int(parent["definition"]["oid"]), sha256=callee_inventory.callee_fingerprint(parent)
    )
    with pytest.raises(ValueError, match="terminal_guard_resolution_required"):
        await _prepare(case)
    case.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name",
    [
        "cms_serving_oids",
        "provider_directory_subset_payload_sha256",
        "provider_directory_terminal_root_retirement_evidence",
    ],
)
async def test_old_settings_refuse_even_with_rebound_callee_fingerprint(case, name):
    function_row = next(row for row in case.state.functions if row["definition"]["proname"] == name)
    function_row["definition"]["proconfig"] = list(
        writers.callables._CALLEE_SETTINGS.get(name, ["search_path=pg_catalog"])
    )
    case.arguments["callee_arguments"]["expected_functions"][name] = dict(
        oid=int(function_row["definition"]["oid"]), sha256=callee_inventory.callee_fingerprint(function_row)
    )
    with pytest.raises(ValueError, match="terminal_settings_required"):
        await _prepare(case)
    case.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_definer_inputs_require_verified_frontier(case):
    case.state.routines.append(_retained_routine())
    await _prepare(case)
    fixed_oids = [int(entry["definition"]["oid"]) for entry in case.state.functions]
    checked = next(
        call.args for call in case.connection.fetchval.await_args_list if call.args[0] == writers._DEFINER_AUTHORITY_SQL
    )
    assert checked == (
        writers._DEFINER_AUTHORITY_SQL,
        [1, 2, 3],
        [*fixed_oids, *(entry["oid"] for entry in case.state.routines)],
        fixed_oids,
        "catalog",
        "migration",
    )


def _allow_original_allocator(case):
    guard = next(
        row for row in case.state.functions if row["definition"]["proname"] == "guard_pd_dataset_candidate_parent"
    )
    guard["acl"].append([_SOURCE_ROLES["namespace_owner"], int(guard["definition"]["proowner"]), "EXECUTE", False])
    case.arguments["callee_arguments"]["expected_functions"]["guard_pd_dataset_candidate_parent"] = dict(
        oid=int(guard["definition"]["oid"]), sha256=callee_inventory.callee_fingerprint(guard)
    )
    case.state.allocator_rows = [_allocator_row("catalog")]


@pytest.mark.asyncio
async def test_allocator_fact_is_pinned_without_extending_frontier(case):
    _allow_original_allocator(case)
    preparation = await _prepare(case)
    fence = await _acquire(case, preparation)
    await writers.require_cms_canonical_writer_fence(case.connection, fence)
    closure = writers.json.loads(preparation.native)["closure"]["functions"]
    assert len(closure["frontier"]) == 32
    assert closure["allocator"]["definition"]["oid"] == "2000"
    queries = [call.args for call in case.connection.fetch.await_args_list]
    allocator_query = (
        writers.callables._FUNCTION_SQL + " FOR SHARE OF p NOWAIT",
        ['"catalog"."prepare_pd_generic_dataset_storage"()'],
        "catalog",
        ["prepare_pd_generic_dataset_storage"],
        [],
    )
    assert queries.count(allocator_query) == 3
    definer_queries = [call.args for call in case.connection.fetchval.await_args_list]
    assert all(
        len(args[3]) == 32 and 2000 not in args[3]
        for args in definer_queries
        if args[0] == writers._DEFINER_AUTHORITY_SQL
    )


@pytest.mark.asyncio
async def test_namespace_owned_frontier_keeps_the_protected_source_binding(case):
    case.state.functions = _namespace_source_rows(case.state.functions)
    metadata = _source_metadata(case.state.functions)
    case.arguments["callee_arguments"] = {
        **_callee_arguments(case.state.functions, case.arguments["storage_binding"]),
        "source_metadata": metadata,
    }
    preparation = await _prepare(case)
    captured = writers.json.loads(preparation.native)["arguments"]["source_metadata"]
    assert captured == metadata
    metadata.clear()
    fence = await _acquire(case, preparation)
    await writers.require_cms_canonical_writer_fence(case.connection, fence)
    assert writers.json.loads(preparation.native)["arguments"]["source_metadata"] == captured


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["definition", "language", "acl"])
async def test_source_binding_drift_refuses_before_writer_locks(case, field):
    case.state.functions = _namespace_source_rows(case.state.functions)
    metadata = _source_metadata(case.state.functions)
    metadata[next(iter(metadata))][field] = "unbound"
    case.arguments["callee_arguments"] = {
        **_callee_arguments(case.state.functions, case.arguments["storage_binding"]),
        "source_metadata": metadata,
    }
    with pytest.raises(ValueError, match="source_metadata_changed"):
        await _prepare(case)
    case.connection.execute.assert_not_awaited()


def _supplementary_arguments(source_proof_case):
    case = source_proof_case
    schema = "snapshot_schema"
    for function_row in case.rows:
        if function_row["definition"]["proname"] == "guard_pd_dataset_candidate_parent":
            function_row["definition"]["prosrc"] = _cms_canonical_copy_guard_body(schema, "preparation_schema")
        _target_settings(function_row)
    case.document["storage_custody"]["canonical_authority"]["metadata"] = _source_metadata(case.rows)
    specs = writers.routine_checks._contracts(schema, "preparation_schema")
    bodies = retirement_resolution_bodies(schema)
    proof_rows = []
    for oid, name in enumerate(writers._PROOF_FUNCTIONS, 8000):
        spec = specs[name]
        proof_row = legacy_catalog_entry(oid, name + "(text)", (spec[0], "text", *spec[2:]), bodies[name][1])
        proof_row["definition"].update(pronamespace="42", proowner="104", prorettype="16")
        _target_settings(proof_row)
        proof_row["acl"] = [[104, 104, "EXECUTE", False], [103, 104, "EXECUTE", False]]
        proof_rows.append(proof_row)
    storage = FHIRSourceStorageBinding(
        schema, "private_source", 1, 42, 43, 201, 104, writers.custody._digest(case.document)
    )
    arguments_by_field = dict(
        **_callee_arguments(case.rows, storage),
        source_document=case.document,
        source_metadata=case.document["storage_custody"]["canonical_authority"]["metadata"],
        proof_functions={
            proof_row["definition"]["proname"]: dict(
                oid=int(proof_row["definition"]["oid"]), sha256=callee_inventory.callee_fingerprint(proof_row)
            )
            for proof_row in proof_rows
        },
    )
    return proof_rows, arguments_by_field, storage


def test_exact_source_supplementary_proofs_keep_separate_frontier(source_proof_case):
    rows, arguments, storage = _supplementary_arguments(source_proof_case)
    copied = writers._callee_arguments(arguments, storage)
    verified = writers._verified_proofs(rows, copied)
    assert set(verified) == set(writers._PROOF_FUNCTIONS)
    assert set(arguments["expected_functions"]).isdisjoint(verified)
    arguments["source_document"]["publication"].clear()
    assert copied["source_document"]["publication"]


@pytest.mark.asyncio
async def test_source_proof_closure_keeps_fixed_frontier_and_all_authenticated_roots(source_proof_case):
    for function_row in source_proof_case.rows:
        name = function_row["definition"]["proname"]
        bodies = retirement_resolution_bodies("snapshot_schema")
        if name in bodies:
            function_row["definition"]["prosrc"] = bodies[name][1]
    proof_rows, arguments, storage = _supplementary_arguments(source_proof_case)
    function_rows = [*source_proof_case.rows, *proof_rows]
    routine_rows = [_raw(function_row) for function_row in function_rows]
    selected_names = {function_row["definition"]["proname"] for function_row in function_rows}
    routine_rows.extend(
        routine
        for routine in _terminal_routines("snapshot_schema")
        if routine["native"]["definition"]["proname"] not in selected_names
    )
    retained = _retained_routine()
    routine_rows.append(retained)
    connection = SimpleNamespace(fetch=AsyncMock(side_effect=[function_rows, routine_rows]))
    closure = await writers._callable_closure(
        connection, writers._callee_arguments(arguments, storage), [1, 2], (retained["oid"],)
    )
    assert len(closure["frontier"]) == 32
    assert closure["proofs"].keys() == arguments["proof_functions"].keys()
    assert closure["allocator"] is None
    assert closure["routines"] == routine_rows
    assert connection.fetch.await_args.args == (
        writers.custody._ROUTINES_SQL,
        [1, 2],
        sorted({retained["oid"], *(int(function_row["definition"]["oid"]) for function_row in function_rows)}),
    )


@pytest.mark.parametrize(
    "fault", ["body", "owner", "public", "other_actor", "grantor", "grant_option", "duplicate", "missing", "pin"]
)
def test_supplementary_proof_cannot_promote_unverified_authority(source_proof_case, fault):
    function_rows, arguments, _storage = _supplementary_arguments(source_proof_case)
    function_row = function_rows[0]
    match fault:
        case "body":
            function_row["definition"]["prosrc"] += " changed"
        case "owner":
            function_row["definition"]["proowner"] = "103"
        case "public":
            function_row["acl"][-1][0] = 0
        case "other_actor":
            function_row["acl"][-1][0] = 999
        case "grantor":
            function_row["acl"][-1][1] = 103
        case "grant_option":
            function_row["acl"][-1][-1] = True
        case "duplicate":
            function_rows.append(deepcopy(function_row))
        case "missing":
            function_rows.pop()
        case "pin":
            arguments["proof_functions"][function_row["definition"]["proname"]]["oid"] += 1
    if fault != "pin":
        arguments["proof_functions"] = {
            entry["definition"]["proname"]: dict(
                oid=int(entry["definition"]["oid"]), sha256=callee_inventory.callee_fingerprint(entry)
            )
            for entry in function_rows
        }
    with pytest.raises(ValueError):
        writers._verified_proofs(function_rows, arguments)


@pytest.mark.parametrize("fault", ["document", "metadata", "missing_pin", "extra_pin", "zero_oid", "duplicate_oid"])
def test_source_proof_inputs_are_bound_before_io(source_proof_case, fault):
    rows, arguments, storage = _supplementary_arguments(source_proof_case)
    first, second = writers._PROOF_FUNCTIONS
    match fault:
        case "document":
            arguments["source_document"]["publication"]["owner_role"] = "changed"
        case "metadata":
            arguments["source_metadata"] = {}
        case "missing_pin":
            arguments["proof_functions"].pop(first)
        case "extra_pin":
            arguments["proof_functions"]["unbound"] = arguments["proof_functions"][first]
        case "zero_oid":
            arguments["proof_functions"][first]["oid"] = 0
        case "duplicate_oid":
            arguments["proof_functions"][first]["oid"] = arguments["proof_functions"][second]["oid"]
    with pytest.raises(ValueError):
        writers._callee_arguments(arguments, storage)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["acquire", "recheck"])
async def test_allocator_fact_identity_is_rechecked(case, phase):
    _allow_original_allocator(case)
    preparation = await _prepare(case)
    fence = await _acquire(case, preparation) if phase == "recheck" else None
    case.state.allocator_rows[0]["definition"]["oid"] = "2001"
    with pytest.raises(ValueError, match="closure_changed"):
        if phase == "acquire":
            await _acquire(case, preparation)
        else:
            await writers.require_cms_canonical_writer_fence(case.connection, fence)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "duplicate", "body", "owner"])
async def test_allocator_fact_refuses_before_fence_locks(case, damage):
    _allow_original_allocator(case)
    if damage == "missing":
        case.state.allocator_rows.clear()
    elif damage == "duplicate":
        case.state.allocator_rows *= 2
    else:
        field = "prosrc" if damage == "body" else "proowner"
        case.state.allocator_rows[0]["definition"][field] = "changed"
    with pytest.raises(ValueError):
        await _prepare(case)
    case.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("allocator_busy"), asyncio.CancelledError()])
async def test_allocator_fact_transport_failure_propagates(case, failure):
    _allow_original_allocator(case)
    original_fetch = case.connection.fetch.side_effect

    async def fetch(query, *arguments):
        if query == writers.callables._FUNCTION_SQL + " FOR SHARE OF p NOWAIT":
            raise failure
        return await original_fetch(query, *arguments)

    case.connection.fetch.side_effect = fetch
    with pytest.raises(type(failure)) as raised:
        await _prepare(case)
    assert raised.value is failure
    case.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["body", "settings", "acl", "owner", "missing"])
async def test_fixed32_authentication_is_mandatory_before_locking(case, kind):
    row = next(
        row
        for row in case.state.functions
        if row["definition"]["proname"] == "provider_directory_terminal_root_retirement_evidence"
    )
    if kind == "body":
        row["definition"]["prosrc"] += "\n-- changed"
    elif kind == "settings":
        row["definition"]["proconfig"] = ["search_path=pg_catalog"]
    elif kind == "acl":
        row["acl"].append([999, int(row["definition"]["proowner"]), "EXECUTE", False])
    elif kind == "owner":
        row["definition"]["proowner"] = "999"
    else:
        case.state.functions.remove(row)
    with pytest.raises(ValueError):
        await _prepare(case)
    case.connection.execute.assert_not_awaited()
    assert not any(query == writers._DEFINER_AUTHORITY_SQL for _, query in case.state.events)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["owners", "dependencies", "callable_owners", "definers"])
async def test_unsupported_owner_or_indirect_authority_refuses_before_locking(case, field):
    setattr(case.state, field, False)
    with pytest.raises(ValueError, match="unprotected|reachable"):
        await _prepare(case)
    case.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["acquire", "recheck"])
async def test_definer_grant_is_rechecked(case, phase):
    preparation = await _prepare(case)
    fence = await _acquire(case, preparation) if phase == "recheck" else None
    case.state.definers = False
    with pytest.raises(ValueError, match="cms_canonical_writer_definer_authority_reachable"):
        if phase == "acquire":
            await _acquire(case, preparation)
        else:
            await writers.require_cms_canonical_writer_fence(case.connection, fence)
    assert case.state.locks is True


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["c", "n", "d"])
@pytest.mark.parametrize("direction", ["incoming", "outgoing"])
async def test_external_cascade_paths_require_an_explicit_contract(case, action, direction):
    case.state.foreign = [
        _foreign(parent=99 if direction == "incoming" else 1, child=2 if direction == "incoming" else 99, action=action)
    ]
    with pytest.raises(ValueError, match="external_cascade_contract_required"):
        await _prepare(case)
    case.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("edge", [_foreign(), _foreign(parent=99, action="a"), _foreign(child=99, action="r")])
async def test_internal_cascade_or_external_noaction_retains_native_hook_checks(case, edge):
    case.state.foreign = [edge]
    preparation = await _prepare(case)
    fence = await _acquire(case, preparation)
    await writers.require_cms_canonical_writer_fence(case.connection, fence)
    assert any("p.prosrc=p.proname" in query for kind, query in case.state.events if kind == "fetchval")


@pytest.mark.asyncio
async def test_incoming_foreign_change_is_rechecked_at_terminal_boundary(case):
    fence = await _acquire(case, await _prepare(case))
    case.state.foreign = [_foreign(parent=99, action="a")]
    with pytest.raises(ValueError, match="closure_changed"):
        await writers.require_cms_canonical_writer_fence(case.connection, fence)


@pytest.mark.asyncio
async def test_racing_family_change_refuses_after_lock_before_a_fence_is_returned(case):
    preparation = await _prepare(case)
    case.state.family.append(_relation(4, "racing_child", [1]))
    with pytest.raises(ValueError, match="family_changed"):
        await _acquire(case, preparation)
    assert case.state.locks is True  # Caller must roll back the original transaction.


@pytest.mark.asyncio
async def test_released_native_locks_cannot_be_replaced_by_a_comparison(case):
    fence = await _acquire(case, await _prepare(case))
    case.state.locks = False
    with pytest.raises(ValueError, match="reader_compatible_locks_required"):
        await writers.require_cms_canonical_writer_fence(case.connection, fence)


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("transaction", "2/2"), ("backend", 92), ("current", "other")])
async def test_original_transaction_and_actor_are_retained(case, field, value):
    fence = await _acquire(case, await _prepare(case))
    case.state.actor[field] = value
    with pytest.raises(ValueError, match="original_admin_changed"):
        await writers.require_cms_canonical_writer_fence(case.connection, fence)


@pytest.mark.asyncio
async def test_replacement_connection_and_child_task_cannot_borrow_fence(case):
    fence = await _acquire(case, await _prepare(case))
    with pytest.raises(ValueError, match="original_scope_required"):
        await writers.require_cms_canonical_writer_fence(SimpleNamespace(), fence)
    with pytest.raises(ValueError, match="original_scope_required"):
        await asyncio.create_task(writers.require_cms_canonical_writer_fence(case.connection, fence))


@pytest.mark.asyncio
@pytest.mark.parametrize("fence", [None, {}, {"locks": True}])
async def test_serialized_or_missing_fence_refuses_before_sql(case, fence):
    with pytest.raises(ValueError, match="fence_required"):
        await writers.require_cms_canonical_writer_fence(case.connection, fence)
    case.connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_deadline_refuses_acquisition_and_terminal_recheck(case):
    preparation = await _prepare(case)
    with pytest.raises(ValueError, match="deadline_expired"):
        await writers.acquire_cms_canonical_writer_fence(case.connection, preparation, deadline=0)
    case.connection.execute.assert_not_awaited()
    fence = await _acquire(case, preparation)
    with pytest.raises(ValueError, match="deadline_expired"):
        await writers.require_cms_canonical_writer_fence(case.connection, replace(fence, deadline=0))


@pytest.mark.asyncio
async def test_lock_cancellation_propagates_without_commit_or_retry(case):
    preparation = await _prepare(case)
    case.connection.execute.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await _acquire(case, preparation)
    assert case.connection.execute.await_count == 1


@pytest.mark.asyncio
async def test_acquisition_cannot_extend_the_original_phase_deadline(case):
    preparation = await _prepare(case)
    with pytest.raises(ValueError, match="deadline_extended"):
        await writers.acquire_cms_canonical_writer_fence(
            case.connection, preparation, deadline=asyncio.get_running_loop().time() + 20
        )
    case.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", writers.custody._DIGEST_FIELDS)
async def test_all_existing_profile_evidence_is_checked_before_locking(case, field):
    case.arguments["cms_profile"][field] = "9" * 64
    with pytest.raises(ValueError, match="protected_profile_changed"):
        await _prepare(case)
    case.connection.execute.assert_not_awaited()


@pytest.fixture
def history_case(case):
    case.state.family.extend(_relation(oid, root) for oid, root in enumerate(writers.history.ROOTS, 4))
    canonical, _family = case.canonical_reader.return_value
    digest = writers.custody._digest(canonical)
    case.arguments["cms_profile"]["canonical_sha256"] = digest
    writers.policy_checks._observe.return_value["canonical_sha256"] = digest
    return case


def _history_transport(case):
    if not hasattr(case, "session"):
        case.database = Mock(spec=writers.physical_scope.Database)
        case.session = Mock(spec=writers.physical_scope.AsyncSession)
        case.session.in_transaction.return_value = True
        case.session.in_nested_transaction.return_value = False
        case.database._transaction_binding.return_value = SimpleNamespace(session=case.session)
        physical = SimpleNamespace(
            get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=case.connection))
        )
        case.session.connection = AsyncMock(return_value=physical)
    return case.database, case.session


async def _acquire_history(case, preparation):
    database, session = _history_transport(case)
    return await writers.acquire_cms_canonical_history_fence(
        case.connection, preparation, database=database, session=session, deadline=asyncio.get_running_loop().time() + 5
    )


@pytest.mark.asyncio
async def test_history_locks_keep_full_scope(history_case):
    case = history_case
    preparation = await _prepare(case)
    fence = await _acquire_history(case, preparation)
    assert case.connection.execute.await_args.args == (
        'LOCK TABLE ONLY "catalog"."provider_directory_cms_npd_relationship",'
        'ONLY "catalog"."provider_directory_cms_npd_resource_witness" IN SHARE MODE NOWAIT',
    )
    assert case.state.history_locks is True and case.state.locks is False
    await writers.require_cms_canonical_history_fence(case.connection, fence)
    owner_calls = [
        call.args for call in case.connection.fetchval.await_args_list if call.args[0] == writers._OWNERS_SQL
    ]
    assert all(call[1] == [1, 2, 3, 4, 5] for call in owner_calls)
    assert case.canonical_reader.await_count == 1
    assert "history_conversion" not in writers.json.loads(fence.preparation.native)


@pytest.mark.asyncio
async def test_history_requires_source_fixed_heaps(case):
    with pytest.raises(ValueError, match="legacy_history_required"):
        await _acquire_history(case, await _prepare(case))
    case.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["kind", "namespace", "parent", "child"])
async def test_history_refuses_unsupported_native_shape(history_case, damage):
    native = writers.json.loads((await _prepare(history_case)).native)
    relation = native["relations"][3]
    if damage == "kind":
        relation["relkind"] = "p"
    elif damage == "namespace":
        relation["nspname"] = "elsewhere"
    elif damage == "parent":
        relation["parents"] = [1]
    else:
        native["relations"][0]["parents"] = [relation["oid"]]
    with pytest.raises(ValueError, match="legacy_history_required"):
        writers._history_relations(native)


@pytest.mark.asyncio
async def test_history_and_terminal_handles_differ(history_case):
    case = history_case
    preparation = await _prepare(case)
    history_fence = await _acquire_history(case, preparation)
    case.connection.fetchrow.reset_mock()
    with pytest.raises(ValueError, match="fence_required"):
        await writers.require_cms_canonical_writer_fence(case.connection, history_fence)
    case.connection.fetchrow.assert_not_awaited()
    terminal = await _acquire(case, preparation)
    case.connection.fetchrow.reset_mock()
    with pytest.raises(ValueError, match="history_fence_required"):
        await writers.require_cms_canonical_history_fence(case.connection, terminal)
    case.connection.fetchrow.assert_not_awaited()
    assert case.state.history_locks is True
    await writers.require_cms_canonical_history_fence(case.connection, history_fence)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["owners", "dependencies", "callable_owners", "definers", "history_locks"])
async def test_history_rechecks_complete_authority(history_case, field):
    case = history_case
    fence = await _acquire_history(case, await _prepare(case))
    setattr(case.state, field, False)
    with pytest.raises(ValueError, match="unprotected|reachable|history_locks_required"):
        await writers.require_cms_canonical_history_fence(case.connection, fence)


@pytest.mark.asyncio
async def test_history_detects_unlocked_family_drift(history_case):
    case = history_case
    fence = await _acquire_history(case, await _prepare(case))
    case.state.family.append(_relation(6, "new_descendant", [1]))
    with pytest.raises(ValueError, match="family_changed"):
        await writers.require_cms_canonical_history_fence(case.connection, fence)


@pytest.mark.asyncio
@pytest.mark.parametrize("ended", ["committed", "rolled_back", "closed"])
async def test_history_ended_scope_refuses_without_sql(history_case, ended):
    case = history_case
    fence = await _acquire_history(case, await _prepare(case))
    if ended == "closed":
        case.connection.is_closed.return_value = True
    else:
        case.connection.is_in_transaction.return_value = False
    previous_events = list(case.state.events)
    case.connection.fetchrow.reset_mock()
    with pytest.raises(ValueError, match="transaction_required"):
        await writers.require_cms_canonical_history_fence(case.connection, fence)
    case.connection.fetchrow.assert_not_awaited()
    assert case.state.events == previous_events


@pytest.mark.asyncio
async def test_history_binds_task_and_transaction(history_case):
    case = history_case
    fence = await _acquire_history(case, await _prepare(case))
    case.connection.fetchrow.reset_mock()
    with pytest.raises(ValueError, match="original_task_required"):
        await asyncio.create_task(writers.require_cms_canonical_history_fence(case.connection, fence))
    case.connection.fetchrow.assert_not_awaited()
    case.state.actor["transaction"] = "2/2"
    with pytest.raises(ValueError, match="original_admin_changed"):
        await writers.require_cms_canonical_history_fence(case.connection, fence)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["lock", "recheck"])
async def test_history_cancellation_preserves_caller_rollback(history_case, monkeypatch, phase):
    case = history_case
    preparation = await _prepare(case)
    if phase == "lock":
        case.connection.execute.side_effect = asyncio.CancelledError
    else:
        monkeypatch.setattr(writers, "_closure", AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await _acquire_history(case, preparation)
    assert case.connection.execute.await_count == 1
    assert "COMMIT" not in case.connection.execute.await_args.args[0]
    assert case.state.history_locks is (phase == "recheck")


@pytest.mark.asyncio
async def test_history_deadline_cannot_be_extended(history_case):
    case = history_case
    preparation = await _prepare(case)
    database, session = _history_transport(case)
    with pytest.raises(ValueError, match="deadline_extended"):
        await writers.acquire_cms_canonical_history_fence(
            case.connection,
            preparation,
            database=database,
            session=session,
            deadline=asyncio.get_running_loop().time() + 20,
        )
    case.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_history_replaced_transaction_precedes_sql(history_case):
    case = history_case
    fence = await _acquire_history(case, await _prepare(case))
    case.session.get_transaction.return_value = object()
    case.connection.fetchrow.reset_mock()
    case.session.connection.reset_mock()
    with pytest.raises(ValueError, match="original_transaction_required"):
        await writers.require_cms_canonical_history_fence(case.connection, fence)
    case.connection.fetchrow.assert_not_awaited()
    case.session.connection.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("ended", [False, True])
async def test_history_context_exit_is_local(history_case, ended):
    case = history_case
    preparation = await _prepare(case)
    database, session = _history_transport(case)
    async with writers.cms_canonical_history_fence(
        case.connection, preparation, database=database, session=session, deadline=asyncio.get_running_loop().time() + 5
    ) as fence:
        case.connection.is_in_transaction.return_value = not ended
        previous_events = list(case.state.events)
        case.connection.fetchrow.reset_mock()
    assert fence.history_scope is None and case.state.events == previous_events
    with pytest.raises(ValueError, match="history_scope_required"):
        await writers.require_cms_canonical_history_fence(case.connection, fence)
    case.connection.fetchrow.assert_not_awaited()
