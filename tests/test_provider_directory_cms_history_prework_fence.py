# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""History prework retains genuine profile pins and the complete existing writer closure."""

import asyncio
import json
from copy import deepcopy
from dataclasses import replace

import pytest

from process import provider_directory_cms_canonical_writer_fence as writers
from process import provider_directory_cms_history_bounds as bounds
from tests.test_provider_directory_cms_canonical_writer_fence import _acquire_history, _prepare, _relation
from tests.test_provider_directory_cms_canonical_writer_fence import authentic_callees as authentic_callees
from tests.test_provider_directory_cms_canonical_writer_fence import case as base_case
from tests.test_provider_directory_cms_history_bounds import _observed_bound
from tests.test_provider_directory_cms_proof_callees import source_case as source_case
from tests.test_provider_directory_cms_read_definer_binding import bound_case as bound_case


def _add_history_bounds(case):
    bound_rows = []
    for index, root in enumerate(writers.history.ROOTS):
        relation = _relation(4 + index, root)
        relation["relowner"] = case.arguments["storage_binding"].namespace_owner_oid
        relation["native"]["constraints"] = []
        case.state.family.append(relation)
    for relation in case.state.family:
        relation["native"].setdefault("constraints", [])
    case.arguments["cms_profile"].update(writers.custody._evidence_digests(case.evidence))
    for index, root in enumerate(writers.history.ROOTS):
        relation = next(entry for entry in case.state.family if entry["relname"] == root)
        observed = _observed_bound(["one"])
        observed["constraints"][0]["oid"] += index
        proof = bounds.require_cms_history_bound(observed, ["one"])["constraint"]
        relation["native"]["constraints"].append(
            [
                dict(
                    oid=str(proof["oid"]),
                    conname="cms_history_bound",
                    conrelid=str(relation["oid"]),
                    contype="c",
                    conbin=proof["tree"],
                    convalidated=True,
                    conkey=proof["columns"],
                ),
                "CHECK (dataset_id::text = 'one'::text)",
            ]
        )
        bound_rows.append(
            dict(
                oid=relation["oid"],
                relname=root,
                nspname="catalog",
                relowner=relation["relowner"],
                relkind="r",
                bound=observed,
            )
        )
    return bound_rows


@pytest.fixture
def prework_case(bound_case):
    case = bound_case
    case.bound_rows = _add_history_bounds(case)
    case.history_prework = dict(
        history_dataset_ids={root: ["one"] for root in writers.history.ROOTS}, bounds_validated=True
    )
    original_fetch = case.connection.fetch.side_effect

    async def fetch(query, *arguments):
        if query == bounds._READ_SQL:
            return deepcopy(case.bound_rows)
        return await original_fetch(query, *arguments)

    case.connection.fetch.side_effect = fetch
    return case


async def _prepare_prework(case, *, phase=None):
    return await writers.prepare_cms_history_prework_fence(
        case.connection,
        **case.arguments,
        deadline=asyncio.get_running_loop().time() + 10,
        history_prework=case.history_prework if phase is None else phase,
    )


def _pending_bounds(case):
    for bound_row in case.bound_rows:
        bound_row["bound"]["constraints"][0]["validated"] = False
    for relation in case.state.family:
        if relation["relname"] in writers.history.ROOTS:
            relation["native"]["constraints"][0][0]["convalidated"] = False


@pytest.mark.parametrize("validated", [False, True])
async def test_prework_keeps_original_profile_and_actual_family_with_full_source_closure(prework_case, validated):
    case = prework_case
    if not validated:
        _pending_bounds(case)
        case.history_prework["bounds_validated"] = False
    original_by_field = deepcopy(case.arguments)
    preparation = await _prepare_prework(case)
    native_by_field = json.loads(preparation.native)
    assert native_by_field["history_prework_only"] is True
    assert native_by_field["family"] == case.state.family
    assert case.arguments == original_by_field
    functions_by_kind = native_by_field["closure"]["functions"]
    assert len(functions_by_kind["frontier"]) == 32 and len(functions_by_kind["proofs"]) == 2
    assert functions_by_kind["read_definers"] == list(range(21000, 21011))
    assert native_by_field["retained_routines_sha256"] == case.arguments["cms_profile"]["routines_sha256"]
    await writers.require_cms_history_prework_fence(case.connection, preparation)
    case.connection.execute.assert_not_awaited()


@pytest.mark.parametrize("isolation", ["read committed", "serializable"])
async def test_prework_supports_both_required_original_admin_isolations(prework_case, isolation):
    case = prework_case
    case.state.actor["isolation"] = isolation
    preparation = await _prepare_prework(case)
    await writers.require_cms_history_prework_fence(case.connection, preparation)


@pytest.mark.parametrize("isolation", ["repeatable read", "read uncommitted"])
async def test_prework_refuses_other_isolations_before_full_collection(prework_case, isolation):
    case = prework_case
    case.state.actor["isolation"] = isolation
    with pytest.raises(ValueError, match="original_admin_required"):
        await _prepare_prework(case)
    writers.policy_checks._collect_profile.assert_not_awaited()


async def test_normal_preparation_still_requires_read_committed(base_case):
    base_case.state.actor["isolation"] = "serializable"
    with pytest.raises(ValueError, match="original_admin_required"):
        await _prepare(base_case)


@pytest.mark.parametrize("validated", [False, True])
async def test_pending_and_validated_native_states_cannot_be_interchanged(prework_case, validated):
    case = prework_case
    if validated:
        _pending_bounds(case)
    case.history_prework["bounds_validated"] = validated
    with pytest.raises(ValueError, match="validated_bound_changed"):
        await _prepare_prework(case)
    case.connection.execute.assert_not_awaited()


@pytest.mark.parametrize("field", ["private", "roles", "routines", "physical"])
async def test_prework_cannot_project_unrelated_protected_native_changes(prework_case, field):
    case = prework_case
    if field == "physical":
        case.evidence["canonical"]["physical"] = [{"unplanned": True}]
    elif field == "routines":
        case.evidence[field][0]["native"]["definition"]["prosrc"] += " changed"
    else:
        case.evidence[field] = {**case.evidence[field], "unplanned": True}
    with pytest.raises(ValueError, match="protected_profile_changed"):
        await _prepare_prework(case)
    case.connection.execute.assert_not_awaited()


async def test_prework_keeps_unrelated_fields_on_the_two_history_heaps_exact(prework_case):
    case = prework_case
    relation = next(entry for entry in case.state.family if entry["relname"] in writers.history.ROOTS)
    relation["native"]["unplanned"] = True
    with pytest.raises(ValueError, match="protected_profile_changed"):
        await _prepare_prework(case)


async def test_actual_fresh_canonical_must_match_authenticated_inventory(prework_case):
    case = prework_case
    canonical_by_field, family = case.canonical_reader.return_value
    fresh_by_field = deepcopy(canonical_by_field)
    fresh_by_field["migration_plan"] = {"unplanned": True}
    case.canonical_reader.return_value = fresh_by_field, family
    with pytest.raises(ValueError, match="canonical_policy_changed"):
        await _prepare_prework(case)


@pytest.mark.parametrize(
    ("field", "reason"),
    [
        ("owners", "ddl_owner_unprotected"),
        ("callable_owners", "callable_owner_unprotected"),
        ("definers", "definer_authority_reachable"),
    ],
)
async def test_prework_retains_existing_owner_and_definer_gates(prework_case, field, reason):
    setattr(prework_case.state, field, False)
    with pytest.raises(ValueError, match=reason):
        await _prepare_prework(prework_case)


async def test_phase_inputs_are_detached_before_the_first_native_await(prework_case):
    case = prework_case
    original_fetchrow = case.connection.fetchrow.side_effect

    async def fetchrow(*arguments):
        case.history_prework["history_dataset_ids"][writers.history.ROOTS[0]] = ["changed"]
        case.history_prework["bounds_validated"] = False
        return original_fetchrow(*arguments)

    case.connection.fetchrow.side_effect = fetchrow
    preparation = await _prepare_prework(case)
    assert json.loads(preparation.native)["history_prework_only"] is True


@pytest.mark.parametrize("terminal", ["acquire", "require"])
async def test_prework_preparation_cannot_supply_a_terminal_handle(prework_case, terminal):
    case = prework_case
    preparation = await _prepare_prework(case)
    deadline = asyncio.get_running_loop().time() + 5
    with pytest.raises(ValueError, match="history_prework_only"):
        if terminal == "acquire":
            await writers.acquire_cms_canonical_writer_fence(case.connection, preparation, deadline=deadline)
        else:
            await writers.require_cms_canonical_writer_fence(
                case.connection, writers._WriterFence(preparation, deadline)
            )
    case.connection.execute.assert_not_awaited()


async def test_prework_preparation_can_use_existing_physical_history_acquisition(prework_case):
    case = prework_case
    fence = await _acquire_history(case, await _prepare_prework(case))
    assert fence.history_only is True and case.state.history_locks is True and case.state.locks is False
    await writers.require_cms_canonical_history_fence(case.connection, fence)


@pytest.mark.parametrize("field", ["transaction", "backend", "current", "isolation"])
async def test_raw_prework_recheck_pins_the_original_native_actor(prework_case, field):
    case = prework_case
    preparation = await _prepare_prework(case)
    case.state.actor[field] = "changed"
    with pytest.raises(ValueError, match="original_admin_changed"):
        await writers.require_cms_history_prework_fence(case.connection, preparation)


async def test_raw_prework_recheck_refuses_another_task_before_sql(prework_case):
    case = prework_case
    preparation = await _prepare_prework(case)
    case.connection.fetchrow.reset_mock()
    with pytest.raises(ValueError, match="original_scope_required"):
        await asyncio.create_task(writers.require_cms_history_prework_fence(case.connection, preparation))
    case.connection.fetchrow.assert_not_awaited()


@pytest.mark.parametrize("field", ["is_closed", "is_in_transaction"])
async def test_raw_prework_recheck_refuses_ended_transactions_before_sql(prework_case, field):
    case = prework_case
    preparation = await _prepare_prework(case)
    getattr(case.connection, field).return_value = field == "is_closed"
    case.connection.fetchrow.reset_mock()
    with pytest.raises(ValueError, match="transaction_required"):
        await writers.require_cms_history_prework_fence(case.connection, preparation)
    case.connection.fetchrow.assert_not_awaited()


@pytest.mark.parametrize("fault", ["family", "closure"])
async def test_raw_prework_recheck_retains_actual_family_and_full_closure(prework_case, fault):
    case = prework_case
    preparation = await _prepare_prework(case)
    if fault == "family":
        case.state.family[-1]["native"]["constraints"][0][0]["convalidated"] = False
    else:
        case.state.definers = False
    with pytest.raises(ValueError, match="family_changed|definer_authority_reachable"):
        await writers.require_cms_history_prework_fence(case.connection, preparation)


async def test_raw_prework_recheck_refuses_normal_preparation(base_case):
    preparation = await _prepare(base_case)
    base_case.connection.fetchrow.reset_mock()
    with pytest.raises(ValueError, match="history_prework_required"):
        await writers.require_cms_history_prework_fence(base_case.connection, preparation)
    base_case.connection.fetchrow.assert_not_awaited()


async def test_raw_prework_recheck_keeps_original_deadline(prework_case):
    case = prework_case
    preparation = await _prepare_prework(case)
    native_by_field = json.loads(preparation.native)
    native_by_field["deadline"] = 0
    case.connection.fetchrow.reset_mock()
    with pytest.raises(ValueError, match="deadline_expired"):
        await writers.require_cms_history_prework_fence(
            case.connection, replace(preparation, native=json.dumps(native_by_field))
        )
    case.connection.fetchrow.assert_not_awaited()


@pytest.mark.parametrize(
    "phase",
    [
        None,
        {},
        {"history_dataset_ids": {}},
        {"history_dataset_ids": {}, "bounds_validated": 1},
        {"history_dataset_ids": {}, "bounds_validated": True},
        {"history_dataset_ids": None, "bounds_validated": True},
        {
            "history_dataset_ids": {root: ["one"] for root in writers.history.ROOTS},
            "bounds_validated": True,
            "authority": True,
        },
        {"history_dataset_ids": {root: ("one",) for root in writers.history.ROOTS}, "bounds_validated": True},
        {"history_dataset_ids": {root: ["two", "one"] for root in writers.history.ROOTS}, "bounds_validated": True},
    ],
)
async def test_invalid_phase_fields_refuse_before_native_io(base_case, phase):
    with pytest.raises(ValueError, match="history_prework_required|history_datasets_required|bounds_required"):
        await writers.prepare_cms_history_prework_fence(
            base_case.connection,
            **base_case.arguments,
            deadline=asyncio.get_running_loop().time() + 5,
            history_prework=phase,
        )
    base_case.connection.fetchrow.assert_not_awaited()
