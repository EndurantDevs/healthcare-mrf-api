# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected routine vectors bind every read-definer exception to exact current custody."""

import json
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.migration_cms_retirement_resolution import retirement_resolution_bodies
from db.migration_generic_source_candidates import _canonical_copy_guard_bodies
from process import provider_directory_cms_callee_inventory as inventory
from process import provider_directory_cms_canonical_writer_fence as writers
from process import provider_directory_cms_dependency_policy as dependencies
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from tests.test_provider_directory_cms_callee_inventory import _namespace_source_rows, _source_metadata
from tests.test_provider_directory_cms_canonical_writer_fence import (
    _acquire,
    _prepare,
    _terminal_routines,
)
from tests.test_provider_directory_cms_canonical_writer_fence import (
    authentic_callees as authentic_callees,
)
from tests.test_provider_directory_cms_canonical_writer_fence import (
    case as base_case,
)
from tests.test_provider_directory_cms_proof_callees import source_case as source_case
from tests.test_provider_directory_cms_read_definers import _read_case
from tests.test_provider_directory_cms_read_definers import read_case as read_case
from tests.test_provider_directory_cms_transition_routines import _raw
from tests.test_provider_directory_generic_source_release_checks import legacy_catalog_entry


def _proof_rows():
    specs = writers.routine_checks._contracts("catalog", "preparation_schema")
    bodies = retirement_resolution_bodies("catalog")
    proof_rows = []
    for oid, name in enumerate(writers._PROOF_FUNCTIONS, 65000):
        spec = specs[name]
        entry = legacy_catalog_entry(oid, name + "(text)", (spec[0], "text", *spec[2:]), bodies[name][1])
        entry["definition"].update(
            pronamespace="42", proowner="104", prorettype="16", proconfig=["search_path=pg_catalog, pg_temp"]
        )
        entry["acl"] = [[104, 104, "EXECUTE", False], [103, 104, "EXECUTE", False]]
        proof_rows.append(entry)
    return proof_rows


def _bound_routines(functions, callback_case):
    routine_by_name = {}
    for routine in [*callback_case.routines, *_terminal_routines()]:
        routine_by_name[routine["native"]["definition"]["proname"]] = deepcopy(routine)
    for entry in functions:
        routine = _raw(entry, "catalog")
        routine["native"].update(acl=deepcopy(entry["acl"]), namespace_identifier="catalog", dependencies=[])
        routine_by_name[entry["definition"]["proname"]] = routine
    return sorted(routine_by_name.values(), key=lambda routine: routine["oid"])


def _source_inputs(case, source_case):
    fixed_rows = _namespace_source_rows(case.state.functions)
    for entry in fixed_rows:
        entry["definition"]["prolang"] = "14" if entry["language"] == "sql" else "13563"
    callback_case = _read_case(source_case, "catalog")
    document = callback_case.document
    document["storage_custody"]["canonical_authority"]["metadata"] = _source_metadata(fixed_rows)
    proof_rows = _proof_rows()
    case.state.functions = [*fixed_rows, *proof_rows]
    case.state.protected_routines = _bound_routines(case.state.functions, callback_case)
    arguments = case.arguments["callee_arguments"]
    arguments.update(
        source_document=document,
        source_metadata=_source_metadata(fixed_rows),
        proof_functions={
            row["definition"]["proname"]: dict(
                oid=int(row["definition"]["oid"]), sha256=inventory.callee_fingerprint(row)
            )
            for row in proof_rows
        },
    )
    arguments["expected_functions"] = {
        row["definition"]["proname"]: dict(oid=int(row["definition"]["oid"]), sha256=inventory.callee_fingerprint(row))
        for row in fixed_rows
    }
    return document


def _bind_transport(case):
    original_fetch = case.connection.fetch.side_effect

    async def fetch(query, *arguments):
        if query == dependencies._ROUTINES_SQL:
            case.state.events.append(("fetch", query))
            return deepcopy(case.state.protected_routines)
        return await original_fetch(query, *arguments)

    case.connection.fetch.side_effect = fetch


@pytest.fixture
def bound_case(base_case, source_case, monkeypatch):
    case = base_case
    document = _source_inputs(case, source_case)
    policy = GenericSourcePolicy("private_source", "private_owner", "prepared_publisher", ("reader",))
    storage = replace(
        case.arguments["storage_binding"],
        owner_oid=201,
        namespace_owner_oid=104,
        enrollment_sha256=writers.custody._digest(document),
    )
    canonical_by_field, family = case.canonical_reader.return_value
    canonical_by_field = dict(
        canonical_by_field, dependency_relations=[], dependency_histories=[], dependency_migration_plan={}
    )
    case.evidence = dict(
        canonical=canonical_by_field,
        private={"bound": True},
        routines=case.state.protected_routines,
        roles=document["role_graph"],
    )
    profile_by_field = dict(
        contract=writers.policy_checks.DEPENDENCY_CONTRACT,
        source_ids=["cms-npd"],
        storage=asdict(storage),
        policy=json.loads(writers._encoded(asdict(policy))),
        fixed_descendant_oids=[2],
        **writers.custody._evidence_digests(case.evidence),
    )
    case.arguments.update(storage_binding=storage, policy=policy, cms_profile=profile_by_field)
    case.canonical_reader.return_value = (
        {key: canonical_by_field[key] for key in ("relations", "migration_plan")},
        family,
    )
    monkeypatch.setattr(
        writers.policy_checks, "_collect_profile", AsyncMock(side_effect=lambda *args: deepcopy(case.evidence))
    )
    _bind_transport(case)
    return case


@pytest.mark.parametrize("protected", [False, True])
@pytest.mark.parametrize("encoded", [False, True])
async def test_retained_vector_uses_exact_versioned_query_and_decodes_native(read_case, protected, encoded):
    routines = deepcopy(read_case.routines)
    observed_rows = [
        dict(oid=row["oid"], native=json.dumps(row["native"]) if encoded else row["native"]) for row in routines
    ]
    connection = SimpleNamespace(fetch=AsyncMock(return_value=observed_rows))
    pin = writers.custody._digest(routines) if protected else None
    assert (
        await writers._retained_routine_inventory(connection, {"source_document": read_case.document}, [1, 2], [3], pin)
        == routines
    )
    connection.fetch.assert_awaited_once_with(
        dependencies._ROUTINES_SQL if protected else writers.custody._ROUTINES_SQL, [1, 2], [3]
    )


@pytest.mark.parametrize("fault", ["body", "acl", "identifier", "dependency", "order", "missing", "extra", "pin"])
async def test_complete_protected_vector_refuses_all_drift_before_exemption(read_case, fault):
    routines = deepcopy(read_case.routines)
    pin = writers.custody._digest(routines)
    match fault:
        case "body":
            routines[0]["native"]["definition"]["prosrc"] += " changed"
        case "acl":
            routines[0]["native"]["acl"].append([999, 104, "EXECUTE", False])
        case "identifier":
            routines[0]["native"]["namespace_identifier"] = '"changed"'
        case "dependency":
            routines[0]["native"]["dependencies"].append({"refobjid": "999"})
        case "order":
            routines.reverse()
        case "missing":
            routines.pop()
        case "extra":
            routines.append(deepcopy(routines[0]))
        case "pin":
            pin = "f" * 64
    connection = SimpleNamespace(fetch=AsyncMock(return_value=routines))
    with pytest.raises(ValueError, match="protected_routines_changed"):
        await writers._retained_routine_inventory(connection, {"source_document": read_case.document}, [1], [2], pin)


@pytest.mark.parametrize("pin", ["0" * 64, "F" * 64, "short", True])
async def test_invalid_protected_pin_refuses_before_catalog_reads(pin):
    connection = SimpleNamespace(fetch=AsyncMock())
    with pytest.raises(ValueError, match="protected_routines_required"):
        await writers._retained_routine_inventory(connection, {"source_document": {}}, [1], [2], pin)
    connection.fetch.assert_not_awaited()


async def test_protected_inventory_requires_current_source_binding():
    connection = SimpleNamespace(fetch=AsyncMock())
    with pytest.raises(ValueError, match="protected_routines_required"):
        await writers._retained_routine_inventory(connection, {}, [1], [2], "a" * 64)
    connection.fetch.assert_not_awaited()


async def test_protected_pin_and_read_set_survive_prepare_acquire_and_recheck(bound_case):
    case = bound_case
    preparation = await _prepare(case)
    native = json.loads(preparation.native)
    expected_pin = case.arguments["cms_profile"]["routines_sha256"]
    assert native["retained_routines_sha256"] == expected_pin
    assert native["closure"]["functions"]["read_definers"] == list(range(21000, 21011))
    assert len(native["closure"]["functions"]["frontier"]) == 32
    assert len(native["closure"]["functions"]["proofs"]) == 2
    fence = await _acquire(case, preparation)
    await writers.require_cms_canonical_writer_fence(case.connection, fence)
    routine_queries = [
        call.args for call in case.connection.fetch.await_args_list if call.args[0] == dependencies._ROUTINES_SQL
    ]
    assert len(routine_queries) == 3
    assert all(arguments[2] == native["retained_routines"] for arguments in routine_queries)
    definer_queries = [
        call.args for call in case.connection.fetchval.await_args_list if call.args[0] == writers._DEFINER_AUTHORITY_SQL
    ]
    assert len(definer_queries) == 3
    closure = native["closure"]["functions"]
    expected_exceptions = [
        *(int(entry["definition"]["oid"]) for entry in case.state.functions),
        *closure["read_definers"],
    ]
    assert all(arguments[3] == expected_exceptions for arguments in definer_queries)


async def test_profile_collector_retains_existing_three_value_abi(bound_case):
    case = bound_case
    observed = await writers._profile_fingerprints(
        case.connection,
        case.arguments["storage_binding"],
        case.arguments["policy"],
        case.arguments["cms_profile"],
        True,
    )
    assert observed == (
        writers.custody._evidence_digests(case.evidence),
        sorted(routine["oid"] for routine in case.state.protected_routines),
        case.evidence["canonical"],
    )


async def test_legacy_preparation_keeps_old_query_and_no_read_exemption(base_case):
    preparation = await _prepare(base_case)
    native = json.loads(preparation.native)
    assert "retained_routines_sha256" not in native
    assert native["closure"]["functions"]["read_definers"] == []
    assert len(native["closure"]["functions"]["frontier"]) == 32
    queries = [call.args[0] for call in base_case.connection.fetch.await_args_list]
    assert writers.custody._ROUTINES_SQL in queries
    assert dependencies._ROUTINES_SQL not in queries


@pytest.mark.parametrize("field", writers.custody._DIGEST_FIELDS)
async def test_all_profile_digests_precede_any_exception_inventory(bound_case, field):
    case = bound_case
    case.arguments["cms_profile"][field] = "9" * 64
    with pytest.raises(ValueError, match="protected_profile_changed"):
        await _prepare(case)
    case.connection.execute.assert_not_awaited()
    case.connection.fetch.assert_not_awaited()
    assert not any(query == writers._DEFINER_AUTHORITY_SQL for _kind, query in case.state.events)


@pytest.mark.parametrize("phase", ["acquire", "recheck"])
async def test_later_raw_vector_drift_is_refused_against_original_profile(bound_case, phase):
    case = bound_case
    preparation = await _prepare(case)
    fence = await _acquire(case, preparation) if phase == "recheck" else None
    case.state.protected_routines[-1]["native"]["dependencies"].append({"refobjid": "999"})
    with pytest.raises(ValueError, match="protected_routines_changed"):
        if phase == "acquire":
            await _acquire(case, preparation)
        else:
            await writers.require_cms_canonical_writer_fence(case.connection, fence)


async def test_unknown_definer_refuses_even_if_complete_raw_digest_is_rebound(bound_case):
    case = bound_case
    unknown = deepcopy(case.state.protected_routines[-1])
    unknown["oid"] = 70000
    unknown["native"]["definition"].update(oid="70000", proname="unknown_owner_wrapper", prosecdef=True)
    case.state.protected_routines.append(unknown)
    case.arguments["cms_profile"]["routines_sha256"] = writers.custody._digest(case.state.protected_routines)
    with pytest.raises(ValueError, match="function_set_changed"):
        await _prepare(case)
    case.connection.execute.assert_not_awaited()
    assert not any(query == writers._DEFINER_AUTHORITY_SQL for _kind, query in case.state.events)


async def test_generic_parent_refuses_even_if_function_and_profile_pins_are_rebound(bound_case):
    case = bound_case
    name = "guard_pd_dataset_candidate_parent"
    parent = next(entry for entry in case.state.functions if entry["definition"]["proname"] == name)
    parent["definition"]["prosrc"] = _canonical_copy_guard_bodies("catalog", "preparation_schema")[1]
    raw_parent = next(
        entry for entry in case.state.protected_routines if entry["native"]["definition"]["proname"] == name
    )
    raw_parent["native"]["definition"]["prosrc"] = parent["definition"]["prosrc"]
    case.arguments["callee_arguments"]["expected_functions"][name] = dict(
        oid=int(parent["definition"]["oid"]), sha256=inventory.callee_fingerprint(parent)
    )
    case.arguments["cms_profile"]["routines_sha256"] = writers.custody._digest(case.state.protected_routines)
    with pytest.raises(ValueError, match="terminal_guard_resolution_required"):
        await _prepare(case)
    case.connection.execute.assert_not_awaited()
    assert not any(query == writers._DEFINER_AUTHORITY_SQL for _kind, query in case.state.events)


async def test_unqualified_definer_sql_refusal_is_preserved(bound_case):
    case = bound_case
    case.state.definers = False
    with pytest.raises(ValueError, match="definer_authority_reachable"):
        await _prepare(case)
    case.connection.execute.assert_not_awaited()
    assert any(query == writers._DEFINER_AUTHORITY_SQL for _kind, query in case.state.events)
