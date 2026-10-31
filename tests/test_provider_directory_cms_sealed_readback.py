# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Readonly postimage routing; mocked IO does not establish native custody."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from process import provider_directory_cms_prepared_outputs as outputs
from process import provider_directory_cms_prepared_workspace as workspace
from process import provider_directory_cms_snapshot_custody as custody
from process import provider_directory_cms_snapshot_readback as readback
from tests.test_provider_directory_cms_admin_workspace import retained as retained
from tests.test_provider_directory_cms_prepared_context import case as case


def _closed_row(state, root):
    original = state.binding["tables"][root]
    return dict(
        relname=original["name"],
        oid=original["oid"],
        nspname=state.case.policy.schema,
        relowner=state.case.storage.namespace_owner_oid,
        relkind="r",
        relpersistence="p",
        relrowsecurity=False,
        relforcerowsecurity=False,
        inert=True,
        native_checks=True,
        direct_acl_closed=True,
        readers_closed=True,
        parents=[],
        relispartition=False,
        columns=[],
        constraints=[],
        indexes=[],
    )


def _proof_records(state):
    case = state.case
    state.closed_rows = [_closed_row(state, root) for root in workspace.storage.RELATIONS[:4]]
    for root, native_row in zip(workspace.storage.RELATIONS[:4], state.closed_rows, strict=True):
        original = state.binding["tables"][root]
        original["layout_sha256"] = workspace.storage._table_proof(native_row, private=True)["layout_sha256"]
        state.outputs[original["name"]] = deepcopy(original)
    state.request = dict(
        source_binding=case.storage,
        prepared_inputs={
            **deepcopy(state.inputs),
            "origin": case.origin.copy(),
            "native_coordinates": case.coordinates.copy(),
        },
        handoff=state.handoff,
        expected_workspace=state.binding,
        owner_role=state.namespace,
        owner_oid=case.storage.namespace_owner_oid,
        migration_role="migration_login",
    )
    state.sealed = dict(
        contract=custody.CONTRACT,
        owner_oid=case.storage.namespace_owner_oid,
        schema_oid=case.storage.storage_schema_oid,
        namespace_owner_oid=case.storage.namespace_owner_oid,
        tables={
            root: {**state.binding["tables"][root], "owner": case.storage.namespace_owner_oid}
            for root in workspace.storage.RELATIONS
        },
    )
    state.attached = dict(
        contract=readback.attach.CONTRACT,
        parents={root: 2001 + index for index, root in enumerate(readback.attach.ROOTS)},
        tables={root: {**state.sealed["tables"][root], "layout_sha256": "d" * 64} for root in readback.attach.ROOTS},
    )


@pytest_asyncio.fixture
async def sealed_case(retained, monkeypatch):
    state, case = retained, retained.case
    _proof_records(state)
    state.closed, state.descendant = True, False
    state.savepoint = SimpleNamespace(start=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock())
    case.driver.transaction = lambda: state.savepoint
    original_fetchrow, original_fetchval, original_fetch = case.driver.fetchrow, case.driver.fetchval, case.driver.fetch

    async def fetchrow(sql, *args):
        if sql == custody._PRINCIPALS_SQL:
            return dict(
                protected=True, owner_oid=75, schema_oid=73, namespace_owner_oid=75, publisher_oid=77, migration_oid=78
            )
        return await original_fetchrow(sql, *args)

    async def fetchval(sql, *args):
        if sql == custody._WRITERS_SQL:
            assert len(args[0]) == 6 and args[1:] == (75, 75, 78)
            return state.closed
        if "FROM pg_catalog.pg_inherits WHERE inhparent" in sql:
            assert len(args[0]) == 6
            return not state.descendant
        return await original_fetchval(sql, *args)

    async def fetch(sql, *args):
        if sql == readback.CATALOG_SQL:
            assert args[1:] == (75, [77]) and len(args[0]) in {4, 6}
            return deepcopy(state.closed_rows if len(args[0]) == 4 else state.unattached_rows)
        return await original_fetch(sql, *args)

    case.driver.fetchrow = AsyncMock(side_effect=fetchrow)
    case.driver.fetchval = AsyncMock(side_effect=fetchval)
    case.driver.fetch = AsyncMock(side_effect=fetch)
    state.attachment_read = AsyncMock(side_effect=lambda *args, **kwargs: deepcopy(state.attached))
    monkeypatch.setattr(readback, "read_attached_cms_payload_snapshots", state.attachment_read)
    state.read_content = AsyncMock(side_effect=lambda current, handoff: _report(state, current, handoff))
    monkeypatch.setattr(outputs, "read_prepared_cms_outputs", state.read_content)
    return state


def _report(state, current, handoff):
    assert handoff is state.handoff
    state.current = current
    return {"contract": outputs.CONTRACT, "state": "verified_private", "tables": current.receipts}


async def _read(state, **changes):
    case = state.case
    arguments_by_name = dict(expected_seal=state.sealed, expected_attachment=state.attached)
    arguments_by_name.update(changes)
    return await readback.read_sealed_cms_outputs(
        case.database, case.session, case.driver, state.request, **arguments_by_name
    )


async def test_sealed_read_keeps_original_receipts_and_five_original_owner_checks(sealed_case):
    state, case = sealed_case, sealed_case.case
    before = deepcopy(state.binding)
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        transaction = case.session.get_transaction()
        report = await _read(state)
        assert report["tables"] == before["tables"]
        assert state.current.binding == before and state.current._readonly is True
        assert case.session.get_transaction() is transaction and case.session.in_transaction()
        state.attachment_read.assert_awaited()
        for call in workspace.storage._capture.await_args_list:
            assert len(call.args[2]) == 5
        for operation in (state.current.copy_rows("invalid", (), ()), state.current.finish_indexes()):
            with pytest.raises(ValueError, match="readonly_workspace"):
                await operation
        with pytest.raises(ValueError, match="scope_ended"):
            await state.current.verify_current()
    assert state.binding == before
    report["tables"].clear()
    assert state.binding == before


@pytest.mark.parametrize("field", ["owner_oid", "schema_oid", "namespace_owner_oid", "tables"])
async def test_forged_seal_refuses_before_content(sealed_case, field):
    state, case = sealed_case, sealed_case.case
    changed = deepcopy(state.sealed)
    changed[field] = {} if field == "tables" else 999
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        with pytest.raises(ValueError, match="seal_required"):
            await _read(state, expected_seal=changed)
        state.read_content.assert_not_awaited()


@pytest.mark.parametrize("change", ["owner", "layout", "identity", "writer", "descendant", "attachment"])
async def test_native_postimage_or_rights_drift_refuses(sealed_case, change):
    state, case = sealed_case, sealed_case.case
    match change:
        case "owner":
            state.closed_rows[0]["relowner"] = 76
        case "layout":
            state.closed_rows[0]["indexes"] = [{"valid": True}]
        case "identity":
            root = workspace.storage.AUXILIARY_ROOTS[0]
            state.outputs[state.binding["tables"][root]["name"]]["layout_sha256"] = "e" * 64
        case "writer":
            state.closed = False
        case "descendant":
            state.descendant = True
        case "attachment":
            state.attachment_read.side_effect = lambda *args, **kwargs: {}
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        with pytest.raises(
            ValueError, match="postimage_changed|identity_changed|writer_reachable|unexpected_descendant"
        ):
            await _read(state)
        state.read_content.assert_not_awaited()


async def test_original_preparation_owner_is_not_rewritten_to_make_read_pass(sealed_case):
    state, case = sealed_case, sealed_case.case
    changed = deepcopy(state.request["expected_workspace"])
    changed["tables"][workspace.storage.RESOURCE]["owner"] = 75
    state.request["expected_workspace"] = changed
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        with pytest.raises(ValueError, match="retained_outputs_changed"):
            await _read(state)


async def test_publisher_cannot_open_admin_postimage_reader(sealed_case):
    state, case = sealed_case, sealed_case.case
    case.origin.update(session=case.policy.publisher_role, current=case.policy.publisher_role)
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        with pytest.raises(ValueError, match="independent_original_admin_required"):
            await _read(state)
        state.read_content.assert_not_awaited()


@pytest_asyncio.fixture
async def unattached_case(sealed_case):
    state = sealed_case
    state.unattached_rows = [_closed_row(state, root) for root in workspace.storage.RELATIONS]
    for root, row in zip(workspace.storage.RELATIONS, state.unattached_rows, strict=True):
        proof = workspace.storage._table_proof(row, private=True)
        original = state.binding["tables"][root]
        original["layout_sha256"] = proof["layout_sha256"]
        state.outputs[original["name"]] = deepcopy(original)
        state.sealed["tables"][root] = proof
    return state


async def _read_unattached(state):
    case = state.case
    async with readback._open_sealed_reader(
        case.database,
        case.session,
        case.driver,
        state.request,
        expected_seal=state.sealed,
        expected_attachment=None,
    ) as (current, handoff):
        return await state.read_content(current, handoff)


async def test_unattached_read_checks_all_six_heaps_without_attachment_receipt(unattached_case):
    state, case = unattached_case, unattached_case.case
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        report = await _read_unattached(state)
        assert report["tables"] == state.binding["tables"]
        state.attachment_read.assert_not_awaited()
        assert state.current._readonly and not state.current._active
    assert any(
        len(call.args[1]) == 6 for call in case.driver.fetch.await_args_list if call.args[0] == readback.CATALOG_SQL
    )


@pytest.mark.parametrize("change", ["attached", "layout", "identity", "writer", "descendant"])
async def test_unattached_read_retains_layout_identity_and_writer_checks(unattached_case, change):
    state, case = unattached_case, unattached_case.case
    if change == "attached":
        state.unattached_rows[-1].update(parents=[123], relispartition=True)
    elif change == "layout":
        state.unattached_rows[-1]["columns"] = [{"unexpected": True}]
    elif change == "identity":
        root = workspace.storage.AUXILIARY_ROOTS[0]
        state.outputs[state.binding["tables"][root]["name"]]["layout_sha256"] = "e" * 64
    elif change == "writer":
        state.closed = False
    else:
        state.descendant = True
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        with pytest.raises(ValueError):
            await _read_unattached(state)
        state.read_content.assert_not_awaited()
        state.attachment_read.assert_not_awaited()


@pytest.mark.parametrize("change", ["request", "seal", "attachment", "native_inputs"])
async def test_inputs_changed_during_content_refuse_and_close_workspace(sealed_case, change):
    state, case = sealed_case, sealed_case.case

    async def change_after_read(current, handoff):
        report = _report(state, current, handoff)
        if change == "request":
            state.request["migration_role"] = "changed_migration"
        elif change == "seal":
            state.sealed["owner_oid"] += 1
        elif change == "attachment":
            state.attached["parents"][readback.attach.ROOTS[0]] += 1
        else:
            case.coordinates["prepared_owner_oid"] += 1
        return report

    state.read_content.side_effect = change_after_read
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        with pytest.raises(ValueError):
            await _read(state)
        assert state.current._active is False
        state.savepoint.rollback.assert_awaited_once()
        state.savepoint.commit.assert_not_awaited()


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_content_failure_closes_workspace_and_rolls_back_only_savepoint(sealed_case, failure):
    state, case = sealed_case, sealed_case.case

    async def fail_after_read(current, handoff):
        _report(state, current, handoff)
        raise failure("after retained content")

    state.read_content.side_effect = fail_after_read
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        transaction = case.session.get_transaction()
        with pytest.raises(failure):
            await _read(state)
        assert state.current._active is False
        assert case.session.get_transaction() is transaction
        state.savepoint.rollback.assert_awaited_once()
        state.savepoint.commit.assert_not_awaited()
