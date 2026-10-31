# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact body succession and original transport composition with synthetic catalog IO."""

import asyncio
from contextlib import contextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from db import migration_cms_noncurrent_seal as lifecycle
from db import migration_cms_ptg_json_resolution as ptg_json
from db import migration_cms_replay_resolution as replay
from db import migration_cms_retirement_resolution as retirement
from db import migration_cms_snapshot_allocator as allocator
from db import migration_cms_terminal_resolution as terminal
from db import migration_generic_source_candidates as copy_guard
from process import provider_directory_cms_owner_execution as execution
from process import provider_directory_cms_routine_bodies as bodies
from process import provider_directory_cms_routine_settings as settings
from process.provider_directory_source_control_privileges import METADATA_FUNCTIONS
from tests.test_provider_directory_cms_allocation_guard_contracts import original_guards as original_guards
from tests.test_provider_directory_cms_callee_custody import _allocator_row
from tests.test_provider_directory_cms_routine_settings import _declaration
from tests.test_provider_directory_cms_routine_settings import routines as routines
from tests.test_provider_directory_cms_terminal_guard_resolution import terminal_guard_routines
from tests.test_provider_directory_cms_transition_routines import _raw
from tests.test_provider_directory_source_callable_custody import _entry

_SCHEMA = "snapshot_schema"
_PREPARATION = "hp_snapshot_retention"
_ARGUMENTS = dict(catalog_schema=_SCHEMA, catalog_schema_oid=42, preparation_schema=_PREPARATION)
_NAMES = (
    "prepare_pd_generic_dataset_storage",
    "guard_tin_npi_connector_endpoint_dataset",
    "guard_pd_generic_dataset_finalization",
    "guard_pd_endpoint_dataset_subset_replay_evidence",
    "guard_provider_directory_subset_abandonment_dataset",
    "provider_directory_terminal_root_retirement_evidence",
    "provider_directory_terminal_root_retirement_v2_evidence",
    "provider_directory_terminal_root_retirement_valid",
    "provider_directory_terminal_root_retirement_v2_valid",
    "guard_provider_directory_terminal_root_retirement_parent",
    "guard_provider_directory_terminal_root_retirement_run",
    "guard_pd_dataset_candidate_parent",
    ptg_json.FUNCTION,
)


def _additional_rows():
    lifecycle_row = _entry(lifecycle.GUARD + "()")
    lifecycle_row["definition"]["prosrc"] = lifecycle.lifecycle_guard_bodies(_SCHEMA)[0]
    finalization = next(
        row
        for row in terminal_guard_routines(_SCHEMA)
        if row["native"]["definition"]["proname"] == "guard_pd_generic_dataset_finalization"
    )
    finalization_by_field = dict(
        definition=finalization["native"]["definition"], language=finalization["native"]["language"]["lanname"]
    )
    finalization_by_field["definition"]["prosrc"] = allocator.generic_finalization_bodies(_SCHEMA)[0]
    additional_rows = []
    for oid, row in enumerate((_allocator_row(_SCHEMA), lifecycle_row, finalization_by_field), 9000):
        row["definition"]["oid"] = str(oid)
        native = _raw(row)
        native["native"].update(body=_declaration(native), dependencies=[], acl=[[105, 105, "EXECUTE", False]])
        additional_rows.append(native)
    return additional_rows


@pytest.fixture
def body_rows(routines):
    result = deepcopy(routines)
    result.extend(_additional_rows())
    parent = next(
        row for row in result if row["native"]["definition"]["proname"] == "guard_pd_dataset_candidate_parent"
    )
    original = parent["native"]["definition"]["prosrc"]
    generic = copy_guard._canonical_copy_guard_bodies(_SCHEMA, _PREPARATION)[1]
    parent["native"]["body"] = parent["native"]["body"].replace(original, generic, 1)
    parent["native"]["definition"]["prosrc"] = generic
    return sorted(result, key=lambda row: row["oid"])


def _source_targets():
    return {
        "prepare_pd_generic_dataset_storage": allocator.bound_cms_allocator_body(_SCHEMA),
        lifecycle.GUARD: lifecycle.lifecycle_guard_bodies(_SCHEMA)[1],
        "guard_pd_generic_dataset_finalization": allocator.generic_finalization_bodies(_SCHEMA)[1],
        replay.GUARD: replay.replay_guard_bodies(_SCHEMA)[1],
        terminal.GUARD: terminal.terminal_guard_bodies(_SCHEMA)[1],
        ptg_json.FUNCTION: ptg_json.ptg_json_bodies(_SCHEMA)[1],
        **{name: pair[1] for name, pair in retirement.retirement_resolution_bodies(_SCHEMA).items()},
        "guard_pd_dataset_candidate_parent": copy_guard._cms_canonical_copy_guard_body(_SCHEMA, _PREPARATION),
    }


def _postimage(rows):
    return bodies.cms_terminal_body_postimage(rows, **_ARGUMENTS)


def test_thirteen_source_bodies_change_only_prosrc_and_matching_declaration(body_rows):
    before = deepcopy(body_rows)
    after = _postimage(body_rows)
    targets = _source_targets()
    assert len(targets) == 13 and set(targets) == set(_NAMES)
    changed_names = set()
    for original, current in zip(before, after, strict=True):
        name = original["native"]["definition"]["proname"]
        expected = deepcopy(original)
        if name in targets:
            old = expected["native"]["definition"]["prosrc"]
            expected["native"]["definition"]["prosrc"] = targets[name]
            expected["native"]["body"] = expected["native"]["body"].replace(old, targets[name], 1)
            changed_names.add(name)
        assert current == expected
    assert changed_names == set(_NAMES) and body_rows == before
    metadata = {signature.split("(")[0] for signature in METADATA_FUNCTIONS}
    assert len(metadata) == 5 and not metadata.intersection(changed_names)
    assert _postimage(after) == after


@pytest.mark.parametrize("name", _NAMES)
@pytest.mark.parametrize("fault", ["missing", "body", "declaration"])
def test_every_required_body_and_native_declaration_remains_fixed(body_rows, name, fault):
    row = next(row for row in body_rows if row["native"]["definition"]["proname"] == name)
    if fault == "missing":
        body_rows.remove(row)
    elif fault == "body":
        row["native"]["definition"]["prosrc"] = "BEGIN RETURN NULL; END"
    else:
        row["native"]["body"] = "unknown declaration"
    with pytest.raises(ValueError):
        _postimage(body_rows)


@pytest.mark.parametrize("name", _NAMES)
def test_each_individually_upgraded_body_is_idempotent_without_mutating_inputs(body_rows, name):
    target = _postimage(body_rows)
    for index, row in enumerate(body_rows):
        if row["native"]["definition"]["proname"] == name:
            body_rows[index] = deepcopy(target[index])
    before = deepcopy(body_rows)
    assert _postimage(body_rows) == target and body_rows == before


def _admin_transport(driver):
    admin = Mock(spec=AsyncConnection)
    admin.closed = False
    admin.invalidated = False
    admin.in_transaction.return_value = True
    admin.in_nested_transaction.return_value = False
    admin.get_raw_connection = AsyncMock(return_value=SimpleNamespace(driver_connection=driver))
    admin.run_sync = AsyncMock()
    return admin


@pytest.mark.parametrize(
    "fault", [None, "type", "closed", "invalidated", "transaction", "nested", "driver_closed", "driver_transaction"]
)
async def test_admin_transport_requires_same_active_sqlalchemy_and_native_transaction(fault):
    driver = SimpleNamespace(is_closed=Mock(return_value=False), is_in_transaction=Mock(return_value=True))
    admin = _admin_transport(driver)
    match fault:
        case "type":
            admin = SimpleNamespace(get_raw_connection=AsyncMock())
        case "closed" | "invalidated":
            setattr(admin, fault, True)
        case "transaction":
            admin.in_transaction.return_value = False
        case "nested":
            admin.in_nested_transaction.return_value = True
        case "driver_closed":
            driver.is_closed.return_value = True
        case "driver_transaction":
            driver.is_in_transaction.return_value = False
    if fault:
        with pytest.raises(ValueError, match="original_(admin_connection|native_transaction)_required"):
            await bodies.cms_body_admin_driver(admin)
    else:
        assert await bodies.cms_body_admin_driver(admin) is driver
        admin.get_raw_connection.assert_awaited_once_with()


@pytest.fixture
def orchestration(body_rows, monkeypatch):
    state = SimpleNamespace(writes=[], calls=[], fail_at=None)

    @contextmanager
    def savepoint():
        before_writes = list(state.writes)
        try:
            yield
        except BaseException:
            state.writes = before_writes
            raise

    installers = (
        (allocator, "upgrade_cms_snapshot_allocator"),
        (lifecycle, "upgrade_cms_noncurrent_seal"),
        (allocator, "upgrade_cms_generic_finalization_guard"),
        (replay, "upgrade_cms_replay_guard_resolution"),
        (terminal, "upgrade_cms_terminal_guard_resolution"),
        (ptg_json, "upgrade_cms_ptg_json_resolution"),
        (retirement, "upgrade_cms_retirement_resolution"),
        (copy_guard, "upgrade_generic_source_canonical_copy_guard"),
    )
    for module, name in installers:

        def invoke(*args, _name=name, **kwargs):
            state.calls.append((_name, args, kwargs))
            state.writes.append(_name)
            if state.fail_at == len(state.calls):
                raise RuntimeError("original fixed-installer failure")

        monkeypatch.setattr(module, name, Mock(side_effect=invoke))
    state.connection = SimpleNamespace(begin_nested=Mock(side_effect=savepoint))
    state.routines = body_rows
    return state


def test_installers_bind_pins_and_preserve_outer_transaction(orchestration):
    case = orchestration
    before = deepcopy(case.routines)
    bodies.install_cms_terminal_bodies(case.connection, _SCHEMA, _PREPARATION, case.routines)
    names = [name for name, _args, _kwargs in case.calls]
    assert names == [
        "upgrade_cms_snapshot_allocator",
        "upgrade_cms_noncurrent_seal",
        "upgrade_cms_generic_finalization_guard",
        "upgrade_cms_replay_guard_resolution",
        "upgrade_cms_terminal_guard_resolution",
        "upgrade_cms_ptg_json_resolution",
        "upgrade_cms_retirement_resolution",
        "upgrade_generic_source_canonical_copy_guard",
    ]
    pins_by_name = {
        routine_row["native"]["definition"]["proname"]: dict(
            oid=routine_row["oid"], owner_oid=int(routine_row["native"]["definition"]["proowner"])
        )
        for routine_row in case.routines
    }
    for name, args, kwargs in case.calls:
        assert args == (case.connection, _SCHEMA)
        if name == names[-1]:
            assert kwargs == dict(preparation_schema=_PREPARATION, include_cms=True)
        elif name == names[-2]:
            assert kwargs == dict(expected={name: pins_by_name[name] for name in retirement.ORIGINAL_SHA256})
        else:
            target_name = {
                **dict(zip(names[:5], _NAMES[:5], strict=True)),
                "upgrade_cms_ptg_json_resolution": ptg_json.FUNCTION,
            }[name]
            assert kwargs == dict(expected=pins_by_name[target_name])
    case.connection.begin_nested.assert_called_once_with()
    assert case.routines == before and not hasattr(case.connection, "commit")


def test_outer_installer_savepoint_restores_partial_fixed_work_without_committing(orchestration):
    case = orchestration
    case.fail_at = 3
    with pytest.raises(RuntimeError, match="original fixed-installer failure"):
        bodies.install_cms_terminal_bodies(case.connection, _SCHEMA, _PREPARATION, case.routines)
    assert len(case.calls) == 3 and case.writes == []
    assert not hasattr(case.connection, "commit")


@pytest.fixture
def common_case(body_rows, monkeypatch):
    inventory_by_kind = dict(canonical={"bound": 1}, private={"bound": 2}, routines=body_rows, roles={"bound": 3})
    expected = deepcopy(inventory_by_kind)
    expected["routines"] = _postimage(body_rows)
    driver = SimpleNamespace(
        is_closed=Mock(return_value=False),
        is_in_transaction=Mock(return_value=True),
        fetch=AsyncMock(return_value=[dict(oid=9000, description="retained", hooks=[])]),
    )
    admin = _admin_transport(driver)
    current = AsyncMock(return_value=deepcopy(body_rows))
    observed = AsyncMock(return_value=deepcopy(expected))
    monkeypatch.setattr(settings, "_current_routines", current)
    monkeypatch.setattr(execution.profiles, "_collect_profile", observed)
    return SimpleNamespace(
        connection=driver,
        admin=admin,
        current=current,
        observed=observed,
        inventory=inventory_by_kind,
        expected=expected,
        profile=execution.custody._evidence_digests(inventory_by_kind),
        storage=SimpleNamespace(catalog_schema=_SCHEMA, catalog_schema_oid=42),
        policy=object(),
    )


async def _apply(case):
    return await execution.apply_cms_common_routine_bodies(
        case.connection,
        case.admin,
        inventory=case.inventory,
        storage_binding=case.storage,
        policy=case.policy,
        cms_profile=case.profile,
        deadline=asyncio.get_running_loop().time() + 3,
    )


async def test_common_body_wrapper_preserves_all_inventory_and_original_driver(common_case):
    case = common_case
    before = deepcopy(case.inventory)
    assert await _apply(case) == case.expected and case.inventory == before
    case.admin.run_sync.assert_awaited_once_with(
        bodies.install_cms_terminal_bodies, _SCHEMA, _PREPARATION, case.inventory["routines"]
    )
    case.observed.assert_awaited_once_with(case.connection, case.storage, case.policy, case.profile)
    assert case.connection.fetch.await_args_list[0].args == (
        settings._STABILITY_SQL,
        [row["oid"] for row in case.inventory["routines"]],
    )
    assert case.connection.fetch.await_count == 2
    assert not hasattr(case.connection, "commit")


@pytest.mark.parametrize("field", ["canonical", "private", "routines", "roles"])
async def test_complete_postimage_comparison_rejects_every_unplanned_inventory_change(common_case, field):
    case = common_case
    case.observed.return_value[field] = {"unexpected": True}
    with pytest.raises(ValueError, match="body_postimage_changed"):
        await _apply(case)
    assert case.admin.run_sync.await_count == 1


@pytest.mark.parametrize("fault", ["preimage", "transport", "comment", "hooks"])
async def test_native_preimage_transport_and_external_stability_remain_bound(common_case, fault):
    case = common_case
    if fault == "preimage":
        case.current.return_value = []
    elif fault == "transport":
        case.admin.get_raw_connection.return_value.driver_connection = SimpleNamespace(
            is_closed=lambda: False, is_in_transaction=lambda: True
        )
    else:
        before_stability_rows = [dict(oid=9000, description="retained", hooks=[])]
        after_stability_rows = deepcopy(before_stability_rows)
        after_stability_rows[0]["description" if fault == "comment" else "hooks"] = "changed"
        case.connection.fetch.side_effect = [before_stability_rows, after_stability_rows]
    with pytest.raises(ValueError, match="body_preimage_changed|admin_transport_changed|body_hooks_changed"):
        await _apply(case)
    if fault in {"preimage", "transport"}:
        case.admin.run_sync.assert_not_awaited()
        case.observed.assert_not_awaited()


@pytest.fixture
def terminal_case(common_case, monkeypatch):
    case = common_case
    targets = settings._source_targets(case.inventory["routines"], _SCHEMA, 42, _PREPARATION)
    case.settings_rows = settings._expected_postimage(case.inventory["routines"], targets)
    case.expected = deepcopy(case.inventory)
    case.expected["routines"] = _postimage(case.settings_rows)
    case.current.return_value = deepcopy(case.settings_rows)
    case.observed.return_value = deepcopy(case.expected)
    case.install_settings = AsyncMock(return_value=deepcopy(case.settings_rows))
    monkeypatch.setattr(settings, "install_cms_routine_settings", case.install_settings)
    case.apply_bodies = AsyncMock(wraps=execution.apply_cms_common_routine_bodies)
    monkeypatch.setattr(execution, "apply_cms_common_routine_bodies", case.apply_bodies)
    return case


async def _apply_terminal(case, deadline):
    return await execution.apply_cms_common_terminal_routines(
        case.connection,
        case.admin,
        inventory=case.inventory,
        storage_binding=case.storage,
        policy=case.policy,
        cms_profile=case.profile,
        deadline=deadline,
    )


async def test_combined_terminal_uses_one_full_postimage_and_exact_clock(terminal_case):
    case = terminal_case
    original = deepcopy(case.inventory)
    deadline = asyncio.get_running_loop().time() + 3
    assert await _apply_terminal(case, deadline) == case.expected
    assert case.inventory == original
    case.install_settings.assert_awaited_once_with(
        case.connection,
        expected_routines=original["routines"],
        catalog_schema=_SCHEMA,
        catalog_schema_oid=42,
        preparation_schema=_PREPARATION,
        deadline=deadline,
    )
    supplied_rows = case.install_settings.await_args.kwargs["expected_routines"]
    assert supplied_rows is not case.inventory["routines"]
    assert supplied_rows[0]["native"] is not case.inventory["routines"][0]["native"]
    settings_inventory = deepcopy(original)
    settings_inventory["routines"] = case.settings_rows
    case.apply_bodies.assert_awaited_once_with(
        case.connection,
        case.admin,
        inventory=settings_inventory,
        storage_binding=case.storage,
        policy=case.policy,
        cms_profile=case.profile,
        deadline=deadline,
    )
    case.admin.run_sync.assert_awaited_once_with(
        bodies.install_cms_terminal_bodies, _SCHEMA, _PREPARATION, case.settings_rows
    )
    case.observed.assert_awaited_once_with(case.connection, case.storage, case.policy, case.profile)
    assert case.admin.get_raw_connection.await_count == 2
    assert not hasattr(case.connection, "commit")


@pytest.mark.parametrize("failure", [RuntimeError("original settings failure"), asyncio.CancelledError()])
async def test_combined_terminal_preserves_settings_failure_and_skips_body(terminal_case, failure):
    case = terminal_case
    original = deepcopy(case.inventory)
    case.install_settings.side_effect = failure
    with pytest.raises(type(failure)) as caught:
        await _apply_terminal(case, asyncio.get_running_loop().time() + 3)
    assert caught.value is failure and case.inventory == original
    case.apply_bodies.assert_not_awaited()
    case.admin.run_sync.assert_not_awaited()
    case.observed.assert_not_awaited()
    case.connection.fetch.assert_not_awaited()


async def test_combined_terminal_refuses_wrong_transport_before_settings(terminal_case):
    case = terminal_case
    case.admin.get_raw_connection.return_value.driver_connection = SimpleNamespace(
        is_closed=lambda: False, is_in_transaction=lambda: True
    )
    with pytest.raises(ValueError, match="admin_transport_changed"):
        await _apply_terminal(case, asyncio.get_running_loop().time() + 3)
    case.install_settings.assert_not_awaited()
    case.apply_bodies.assert_not_awaited()
    case.admin.run_sync.assert_not_awaited()
    case.observed.assert_not_awaited()
    case.current.assert_not_awaited()


@pytest.mark.parametrize("field", ["canonical", "private", "routines", "roles"])
async def test_combined_terminal_final_comparison_keeps_all_inventory(terminal_case, field):
    case = terminal_case
    case.observed.return_value[field] = {"unplanned": True}
    with pytest.raises(ValueError, match="body_postimage_changed"):
        await _apply_terminal(case, asyncio.get_running_loop().time() + 3)
    assert case.install_settings.await_count == case.apply_bodies.await_count == 1
    assert case.observed.await_count == 1
