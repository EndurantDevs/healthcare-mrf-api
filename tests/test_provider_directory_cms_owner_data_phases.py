# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact owner data phases and complete terminal postimages with synthetic native IO."""

import asyncio
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_cms_owner_execution as execution
from process import provider_directory_cms_routine_bodies as bodies
from process import provider_directory_cms_routine_settings as settings
from tests.test_provider_directory_cms_allocation_guard_contracts import original_guards as original_guards
from tests.test_provider_directory_cms_owner_permissions import owner_case as owner_case
from tests.test_provider_directory_cms_owner_postimage import _expansions, _expected_after, _raw_acl
from tests.test_provider_directory_cms_owner_postimage import structure_case as structure_case
from tests.test_provider_directory_cms_routine_bodies import _admin_transport
from tests.test_provider_directory_cms_routine_bodies import body_rows as body_rows
from tests.test_provider_directory_cms_routine_settings import routines as routines


def _preparation(case):
    expanded = _expansions(case.before, case.after)
    return dict(
        before=deepcopy(case.before),
        plan=deepcopy(case.plan),
        family=deepcopy(execution._relations(case.before)),
        expected_roles=deepcopy(case.after["roles"]),
        acl_expansions=[row for row in expanded if row["phase"] == "before"],
        reader_view=deepcopy(dict(routines=case.after["routines"], roles=case.after["roles"])),
    )


def _acl_rows(payload, expansions):
    parsed = json.loads(payload)
    return [
        dict(
            position=index,
            edges=next(row["edges"] for row in expansions if all(row[key] == value for key, value in site.items())),
        )
        for index, site in enumerate(parsed, 1)
    ]


@pytest.fixture
def routine_case(structure_case, monkeypatch):
    case = structure_case
    case.preparation = _preparation(case)
    case.observed = deepcopy(case.after["routines"])
    case.expanded = _expansions(case.before, case.after)
    case.connection = SimpleNamespace(
        fetch=AsyncMock(side_effect=lambda query, payload: _acl_rows(payload, case.expanded))
    )
    case.current = AsyncMock(side_effect=lambda *args: deepcopy(case.observed))
    monkeypatch.setattr(settings, "_current_routines", case.current)
    return case


async def test_owner_routine_phase_checks_all_119(routine_case):
    case = routine_case
    before = deepcopy(case.preparation)
    actual = await execution._owner_routines(case.connection, case.preparation)
    assert actual == case.after["routines"] and len(actual) == 119
    assert case.preparation == before
    case.current.assert_awaited_once_with(case.connection, [row["oid"] for row in case.before["routines"]])
    assert case.connection.fetch.await_args.args[0] == execution._ACL_SQL


@pytest.mark.parametrize("field", ["prosrc", "proconfig", "proowner", "proargtypes", "proacl"])
async def test_owner_routine_drift_refuses(routine_case, field):
    case = routine_case
    row = case.observed[0]
    row["native"]["definition"][field] = ["unissued"] if field in {"proconfig", "proacl"} else "unissued"
    if field == "proacl":
        case.expanded = _expansions(case.before, {**case.after, "routines": case.observed})
        next(
            item
            for item in case.expanded
            if item["phase"] == "after" and item["kind"] == "routine" and item["oid"] == row["oid"]
        )["edges"] = []
    with pytest.raises(ValueError):
        await execution._owner_routines(case.connection, case.preparation)


@pytest.mark.parametrize("field", ["body", "dependencies", "acl", "namespace", "language"])
async def test_owner_native_fields_remain_exact(routine_case, field):
    case = routine_case
    row = case.observed[0]
    row["native"][field] = [{"unissued": True}] if field == "dependencies" else "unissued"
    with pytest.raises((ValueError, TypeError, IndexError)):
        await execution._owner_routines(case.connection, case.preparation)


@pytest.mark.parametrize("fault", ["missing", "duplicate", "reordered"])
async def test_owner_routine_scope_refuses_before_acl_query(routine_case, fault):
    case = routine_case
    if fault == "missing":
        case.observed.pop()
    elif fault == "duplicate":
        case.observed.append(case.observed[-1])
    else:
        case.observed.reverse()
    with pytest.raises(ValueError, match="routine_identity_changed"):
        await execution._owner_routines(case.connection, case.preparation)
    case.connection.fetch.assert_not_awaited()


async def test_owner_raw_acl_requires_independent_binding(routine_case):
    case = routine_case
    altered = next(row for row in case.expanded if row["kind"] == "routine" and row["phase"] == "after")
    altered["edges"] = []
    with pytest.raises(ValueError, match="routine_acl_changed"):
        await execution._owner_routines(case.connection, case.preparation)


async def test_owner_roles_reuses_exact_v3_query(structure_case):
    case = structure_case
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=json.dumps(case.after["roles"])))
    storage = SimpleNamespace(catalog_schema="catalog", storage_schema="private_source")
    policy = SimpleNamespace(owner_role="owner", publisher_role="publisher", runtime_roles=("worker",))
    assert (
        await execution._owner_roles(connection, case.before, case.after["routines"], storage, policy)
        == case.after["roles"]
    )
    connection.fetchval.assert_awaited_once_with(
        execution.custody._ROLES_SQL,
        sorted({row["oid"] for row in (*execution._relations(case.before), *case.before["private"])}),
        ["catalog", "private_source"],
        [row["oid"] for row in case.after["routines"]],
        ["owner", "publisher", "worker"],
    )


@pytest.fixture
def phase_case(routine_case, monkeypatch):
    case = routine_case
    case.calls = []
    case.connection.fetchval = AsyncMock(return_value=deepcopy(case.after["roles"]))
    case.policy = SimpleNamespace(owner_role="owner", publisher_role="publisher", runtime_roles=("worker",))
    case.storage = SimpleNamespace(catalog_schema="catalog", storage_schema="private_source")
    case.inventory = deepcopy(case.before)
    case.profile, case.source = {}, {}
    case.plan = deepcopy(case.preparation["plan"])
    case.mutating = AsyncMock(return_value=case.preparation)
    monkeypatch.setattr(execution.permissions, "plan_cms_common_owner_permissions", Mock(return_value=case.plan))
    monkeypatch.setattr(execution, "_mutate_owner", case.mutating)
    return case


async def _mutate(case):
    return await execution.mutate_cms_common_owner_permissions(
        case.connection,
        inventory=case.inventory,
        storage_binding=case.storage,
        policy=case.policy,
        cms_profile=case.profile,
        source_document=case.source,
        deadline=asyncio.get_running_loop().time() + 3,
    )


async def test_reader_view_follows_native_owner_proof(phase_case):
    case = phase_case
    original = deepcopy(case.inventory)
    actual = await _mutate(case)
    assert actual["reader_view"] == dict(routines=case.after["routines"], roles=case.after["roles"])
    assert actual["reader_view"]["routines"] is not case.observed
    assert case.inventory == original
    supplied = case.mutating.await_args.args[1]
    assert supplied == case.inventory and supplied is not case.inventory
    assert case.current.await_count == case.connection.fetchval.await_count == 1


@pytest.mark.parametrize("fault", ["routine", "role", "cancelled"])
async def test_failed_owner_proof_returns_no_reader_view(phase_case, fault):
    case = phase_case
    case.preparation.pop("reader_view")
    failure = asyncio.CancelledError()
    if fault == "routine":
        case.observed[0]["native"]["definition"]["prosrc"] = "unissued"
    elif fault == "role":
        case.connection.fetchval.return_value = {"unissued": True}
    else:
        case.current.side_effect = failure
    with pytest.raises((ValueError, asyncio.CancelledError)) as caught:
        await _mutate(case)
    assert "reader_view" not in case.preparation
    if fault == "cancelled":
        assert caught.value is failure
    if fault != "role":
        case.connection.fetchval.assert_not_awaited()


def _body_plan(rows, owner):
    return [
        dict(
            oid=row["oid"],
            nspname=row["native"]["namespace"][1],
            proname=row["native"]["definition"]["proname"],
            owner_oid=int(row["native"]["definition"]["proowner"])
            if row["native"]["namespace"][1] == "pg_catalog"
            else owner,
            routine_acl=deepcopy(row["native"]["acl"])
            if row["native"]["namespace"][1] == "pg_catalog"
            else [[owner, owner, "EXECUTE", False]],
        )
        for row in rows
    ]


def _terminal_rows(rows):
    targets = settings._source_targets(rows, "snapshot_schema", 42, "hp_snapshot_retention")
    return bodies.cms_terminal_body_postimage(
        settings._expected_postimage(rows, targets),
        catalog_schema="snapshot_schema",
        catalog_schema_oid=42,
        preparation_schema="hp_snapshot_retention",
    )


@pytest.fixture
def final_case(structure_case, body_rows, owner_case, monkeypatch):
    case = structure_case
    case.before["routines"] = deepcopy(body_rows)
    for routine_row in case.before["routines"]:
        routine_row["native"].setdefault("namespace_identifier", routine_row["native"]["namespace"][1])
        routine_row["native"]["definition"]["proacl"] = _raw_acl(routine_row["native"]["acl"])
    case.plan["routines"] = _body_plan(case.before["routines"], case.plan["owner_oid"])
    case.after = _expected_after(case.before, case.plan)
    case.preparation = _preparation(case)
    case.final = {**deepcopy(case.after), "routines": _terminal_rows(case.after["routines"])}
    case.terminal = dict(
        routines=deepcopy(case.final["routines"]),
        identities=[routine_row["oid"] for routine_row in case.final["routines"]],
        stability=[
            dict(oid=routine_row["oid"], description="retained", hooks=[]) for routine_row in case.final["routines"]
        ],
    )
    case.expanded = _expansions(case.before, case.after)
    case.calls = []
    case.connection = SimpleNamespace(
        fetch=AsyncMock(side_effect=lambda query, *args: _final_fetch(case, query, *args))
    )
    case.collect = AsyncMock(return_value=case.final)
    case.family = AsyncMock(return_value=deepcopy(execution._relations(case.after)))
    monkeypatch.setattr(execution.profiles, "_collect_profile", case.collect)
    monkeypatch.setattr(execution, "_family", case.family)
    case.storage = replace(owner_case.storage, catalog_schema="snapshot_schema")
    case.policy = owner_case.policy
    case.profile = dict(
        contract=execution.permissions.dependencies.CONTRACT,
        source_ids=["cms-npd"],
        storage=asdict(case.storage),
        policy=json.loads(json.dumps(asdict(case.policy))),
        fixed_descendant_oids=sorted(owner_case.retained),
        **execution.custody._evidence_digests(case.before),
    )
    return case


def _final_fetch(case, query, *args):
    if query == settings._STABILITY_SQL:
        case.calls.append("terminal_hooks")
        return deepcopy(case.terminal["stability"])
    assert query == execution._ACL_SQL
    case.calls.append("final_acl")
    return _acl_rows(args[0], case.expanded)


async def _finish(case):
    return await execution.finish_cms_common_owner_terminal(
        case.connection,
        preparation=case.preparation,
        terminal=case.terminal,
        storage_binding=case.storage,
        policy=case.policy,
        cms_profile=case.profile,
        deadline=asyncio.get_running_loop().time() + 3,
    )


async def test_final_source_and_owner_proofs_keep_inputs(final_case):
    case = final_case
    original = deepcopy((case.preparation, case.terminal, case.final))
    actual = await _finish(case)
    assert actual is case.final
    assert (case.preparation, case.terminal, case.final) == original
    assert actual["routines"] != case.preparation["reader_view"]["routines"]
    case.collect.assert_awaited_once_with(case.connection, case.storage, case.policy, case.profile)
    assert case.calls == ["terminal_hooks", "final_acl"] and case.family.await_count == 1


@pytest.mark.parametrize("field", ["prosrc", "proconfig", "proowner", "proacl"])
async def test_final_definition_drift_refuses_before_projection(final_case, field):
    case = final_case
    case.final["routines"][0]["native"]["definition"][field] = "unissued"
    with pytest.raises(ValueError, match="body_postimage_changed"):
        await _finish(case)
    assert case.calls == []
    case.family.assert_not_awaited()


@pytest.mark.parametrize("field", ["body", "dependencies", "acl", "language", "namespace_identifier"])
async def test_final_native_drift_refuses_before_projection(final_case, field):
    case = final_case
    case.final["routines"][0]["native"][field] = "unissued"
    with pytest.raises(ValueError, match="body_postimage_changed"):
        await _finish(case)
    assert case.calls == []


async def test_terminal_mapping_cannot_promote_observation(final_case):
    case = final_case
    case.terminal["routines"][0]["native"]["definition"]["prosrc"] = "unissued"
    case.final["routines"] = deepcopy(case.terminal["routines"])
    with pytest.raises(ValueError, match="terminal_projection_changed"):
        await _finish(case)
    case.collect.assert_not_awaited()


@pytest.mark.parametrize("field", ["private", "roles", "physical", "family"])
async def test_final_owner_and_family_checks_remain(final_case, field):
    case = final_case
    if field == "private":
        case.final["private"][0]["relname"] = "unissued"
    elif field == "roles":
        case.final["roles"] = {"unissued": True}
    elif field == "physical":
        case.final["canonical"]["physical"][0]["native"]["relfilenode"] += 1
    else:
        case.family.return_value[0]["native"]["triggers"].append({"unissued": True})
    with pytest.raises(ValueError):
        await _finish(case)
    assert case.calls == (["terminal_hooks"] if field == "private" else ["terminal_hooks", "final_acl"])


async def test_partial_terminal_failure_preserves_caller(final_case):
    case = final_case
    failure = asyncio.CancelledError()
    case.collect.side_effect = failure
    original = deepcopy((case.preparation, case.terminal))
    with pytest.raises(asyncio.CancelledError) as caught:
        await _finish(case)
    assert caught.value is failure and (case.preparation, case.terminal) == original
    case.connection.fetch.assert_not_awaited()


async def test_terminal_installer_keeps_full_checks(final_case, monkeypatch):
    case = final_case
    case.connection.is_closed = Mock(return_value=False)
    case.connection.is_in_transaction = Mock(return_value=True)
    admin = _admin_transport(case.connection)
    rows = case.preparation["reader_view"]["routines"]
    targets = settings._source_targets(rows, "snapshot_schema", 42, "hp_snapshot_retention")
    setting_rows = settings._expected_postimage(rows, targets)
    install = AsyncMock(return_value=setting_rows)
    current = AsyncMock(return_value=deepcopy(setting_rows))
    monkeypatch.setattr(settings, "install_cms_routine_settings", install)
    monkeypatch.setattr(settings, "_current_routines", current)
    before = deepcopy(rows)
    terminal = await execution.install_cms_common_terminal_routines(
        case.connection,
        admin,
        routines=rows,
        storage_binding=case.storage,
        deadline=asyncio.get_running_loop().time() + 3,
    )
    assert terminal["routines"] == case.final["routines"] and rows == before
    assert terminal["stability"] == case.terminal["stability"]
    admin.run_sync.assert_awaited_once_with(
        bodies.install_cms_terminal_bodies, "snapshot_schema", "hp_snapshot_retention", setting_rows
    )
    case.collect.assert_not_awaited()
    assert not hasattr(case.connection, "commit")


@pytest.mark.parametrize("failure", [RuntimeError("settings refusal"), asyncio.CancelledError()])
async def test_terminal_installer_keeps_first_failure(final_case, monkeypatch, failure):
    case = final_case
    case.connection.is_closed = Mock(return_value=False)
    case.connection.is_in_transaction = Mock(return_value=True)
    admin = _admin_transport(case.connection)
    install = AsyncMock(side_effect=failure)
    monkeypatch.setattr(settings, "install_cms_routine_settings", install)
    with pytest.raises(type(failure)) as caught:
        await execution.install_cms_common_terminal_routines(
            case.connection,
            admin,
            routines=case.preparation["reader_view"]["routines"],
            storage_binding=case.storage,
            deadline=asyncio.get_running_loop().time() + 3,
        )
    assert caught.value is failure
    admin.run_sync.assert_not_awaited()
    case.collect.assert_not_awaited()
    case.connection.fetch.assert_not_awaited()


async def test_owner_scope_spans_terminal_and_exit(monkeypatch, owner_case):
    calls = []

    @asynccontextmanager
    async def reverse_scope(*args, **kwargs):
        calls.append("scope_enter")
        try:
            yield AsyncMock(side_effect=lambda: calls.append("reverse_recheck"))
        finally:
            calls.append("scope_exit")

    monkeypatch.setattr(execution.reverse, "cms_reverse_hook_custody", reverse_scope)
    connection = SimpleNamespace(
        fetchrow=AsyncMock(
            side_effect=lambda *args: calls.append("database") or dict(oid=owner_case.storage.database_oid)
        ),
        fetchval=AsyncMock(return_value=True),
    )
    async with execution.cms_common_owner_custody(
        connection,
        inventory=owner_case.inventory,
        storage_binding=owner_case.storage,
        policy=owner_case.policy,
        cms_profile=owner_case.profile,
        source_document=owner_case.source,
        deadline=asyncio.get_running_loop().time() + 3,
    ) as scope_result:
        assert scope_result is None
        calls.extend(["owner_proof", "readers", "settings", "bodies", "full_final", "all_proofs"])
    assert calls == [
        "scope_enter",
        "database",
        "owner_proof",
        "readers",
        "settings",
        "bodies",
        "full_final",
        "all_proofs",
        "database",
        "reverse_recheck",
        "scope_exit",
    ]


@pytest.mark.parametrize("fault", ["comment", "hooks"])
async def test_final_terminal_stability_refuses(final_case, fault):
    case = final_case
    changed = deepcopy(case.terminal["stability"])
    changed[0]["description" if fault == "comment" else "hooks"] = "unissued"
    case.connection.fetch.side_effect = lambda *args: changed
    with pytest.raises(ValueError, match="body_hooks_changed"):
        await _finish(case)
    case.family.assert_not_awaited()


@pytest.mark.parametrize("failure", [RuntimeError("partial body refusal"), asyncio.CancelledError()])
async def test_body_failure_stops_final_collection(final_case, monkeypatch, failure):
    case = final_case
    case.connection.is_closed = Mock(return_value=False)
    case.connection.is_in_transaction = Mock(return_value=True)
    admin = _admin_transport(case.connection)
    admin.run_sync.side_effect = failure
    rows = case.preparation["reader_view"]["routines"]
    targets = settings._source_targets(rows, "snapshot_schema", 42, "hp_snapshot_retention")
    setting_rows = settings._expected_postimage(rows, targets)
    monkeypatch.setattr(settings, "install_cms_routine_settings", AsyncMock(return_value=setting_rows))
    monkeypatch.setattr(settings, "_current_routines", AsyncMock(return_value=deepcopy(setting_rows)))
    original = deepcopy(rows)
    with pytest.raises(type(failure)) as caught:
        await execution.install_cms_common_terminal_routines(
            case.connection,
            admin,
            routines=rows,
            storage_binding=case.storage,
            deadline=asyncio.get_running_loop().time() + 3,
        )
    assert caught.value is failure and rows == original
    case.collect.assert_not_awaited()
    assert not hasattr(case.connection, "commit")


@pytest.mark.parametrize("failure", [RuntimeError("partial owner refusal"), asyncio.CancelledError()])
async def test_failed_scope_runs_no_exit_sql(monkeypatch, owner_case, failure):
    recheck = AsyncMock()

    @asynccontextmanager
    async def reverse_scope(*args, **kwargs):
        yield recheck

    monkeypatch.setattr(execution.reverse, "cms_reverse_hook_custody", reverse_scope)
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value=dict(oid=owner_case.storage.database_oid)),
        fetchval=AsyncMock(return_value=True),
    )
    with pytest.raises(type(failure)) as caught:
        async with execution.cms_common_owner_custody(
            connection,
            inventory=owner_case.inventory,
            storage_binding=owner_case.storage,
            policy=owner_case.policy,
            cms_profile=owner_case.profile,
            source_document=owner_case.source,
            deadline=asyncio.get_running_loop().time() + 3,
        ):
            raise failure
    assert caught.value is failure
    assert connection.fetchrow.await_count == 1
    recheck.assert_not_awaited()


async def test_standalone_keeps_only_full_captures(final_case, monkeypatch):
    case = final_case
    mutation = AsyncMock(return_value=case.preparation)
    narrow_routines, narrow_roles = AsyncMock(), AsyncMock()
    monkeypatch.setattr(execution, "_mutate_owner", mutation)
    monkeypatch.setattr(execution, "_owner_routines", narrow_routines)
    monkeypatch.setattr(execution, "_owner_roles", narrow_roles)
    case.collect.return_value = case.after
    actual = await execution._execute(
        case.connection,
        case.before,
        case.storage,
        case.policy,
        case.profile,
        case.plan,
        asyncio.get_running_loop().time() + 3,
    )
    assert actual is case.after
    case.collect.assert_awaited_once()
    narrow_routines.assert_not_awaited()
    narrow_roles.assert_not_awaited()


@pytest.mark.parametrize("field", ["canonical", "private", "routines", "roles"])
async def test_phase_preimage_stays_profile_bound(final_case, field):
    case = final_case
    case.preparation["before"][field] = {"unissued": True}
    with pytest.raises(ValueError, match="inventory_changed"):
        await _finish(case)
    case.collect.assert_not_awaited()
    case.connection.fetch.assert_not_awaited()


async def test_phase_target_stays_source_bound(final_case):
    case = final_case
    case.preparation["plan"]["owner_oid"] += 1
    with pytest.raises(ValueError, match="plan_owner_changed"):
        await _finish(case)
    case.collect.assert_not_awaited()


async def test_phase_routine_cannot_shift_preimage(final_case):
    case = final_case
    row = case.preparation["reader_view"]["routines"][0]
    row["native"]["definition"]["proowner"] = "99999"
    case.terminal["routines"] = _terminal_rows(case.preparation["reader_view"]["routines"])
    case.final["routines"] = deepcopy(case.terminal["routines"])
    case.expanded = _expansions(case.before, {**case.after, "routines": case.preparation["reader_view"]["routines"]})
    with pytest.raises(ValueError, match="routines_changed"):
        await _finish(case)
