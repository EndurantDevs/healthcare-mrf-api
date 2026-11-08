# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Four-target COPY mechanics on a genuine readonly Admin workspace.

Allocation provenance, guards and all-writer closure remain caller obligations;
these controlled SQL responses are not native or protected-finisher acceptance.
"""

import asyncio
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from process import provider_directory_cms_allocation_pin as pins
from process import provider_directory_cms_prepared_candidate as candidate
from process import provider_directory_cms_prepared_payload_publication as payloads
from tests.test_provider_directory_cms_admin_workspace import open_workspace
from tests.test_provider_directory_cms_admin_workspace import retained as retained
from tests.test_provider_directory_cms_prepared_candidate import _header
from tests.test_provider_directory_cms_prepared_context import case as case


def _inventory(schema):
    parents = [
        dict(oid=300 + index, nspname=schema, relname=root, relkind="p", relowner=41, native={"parents": []})
        for index, root in enumerate(payloads.ROOTS[:4])
    ]
    child_tables = [
        dict(
            oid=400 + index,
            nspname=schema,
            relname="allocated_leaf_" + str(index),
            relkind="r",
            relowner=41,
            native={"parents": [parent["oid"]]},
        )
        for index, parent in enumerate(parents)
    ]
    return parents + child_tables


def _transaction(state):
    class Savepoint:
        async def start(self):
            self.before = deepcopy(state.copied)
            state.events.append("savepoint")

        async def commit(self):
            state.events.append("release")

        async def rollback(self):
            state.copied = self.before
            state.events.append("rollback")

    return Savepoint


@pytest_asyncio.fixture
async def generic_copy(retained, monkeypatch):
    state, case = retained, retained.case
    state.inventory = _inventory(case.storage.catalog_schema)
    state.allocation = dict(
        contract=payloads.allocations.CONTRACT,
        workspace=deepcopy(state.binding),
        transaction="100",
        header_xmin="101",
        inventory=deepcopy(state.inventory),
    )
    state.events, state.copied, state.inventory_reads = [], {}, 0
    state.committed, state.occupied, state.fault = True, False, None
    case.driver.transaction = _transaction(state)
    _configure_allocation(state, monkeypatch)
    _configure_payloads(state, monkeypatch)
    return state


def _configure_allocation(state, monkeypatch):
    case = state.case
    expected_header = _header()
    original_fetchrow = case.driver.fetchrow.side_effect
    original_fetchval = case.driver.fetchval.side_effect

    async def header_inputs(*args):
        return '"private_source"."frozen_header"', deepcopy(expected_header)

    async def fetchrow(sql, *arguments):
        if "FOR UPDATE NOWAIT" in sql:
            state.events.append("header_lock")
            return {**expected_header, "status": "acquiring", "validated_at": None, "candidate_storage_json": None}
        return await original_fetchrow(sql, *arguments)

    async def fetchval(sql, *arguments):
        if "::pg_catalog.xid8" in sql:
            return state.committed
        if "SELECT xmin::text" in sql:
            return "101"
        if "IS DISTINCT FROM" in sql:
            state.events.append("source_scan")
            return False
        if sql.startswith("SELECT EXISTS(SELECT FROM"):
            state.events.append("target_scan")
            return state.occupied
        return await original_fetchval(sql, *arguments)

    async def inventory(*args):
        state.events.append("inventory")
        state.inventory_reads += 1
        observed = deepcopy(state.inventory)
        if (state.fault, state.inventory_reads) in {("postlock_inventory", 3), ("postcopy_inventory", 4)}:
            observed[-1]["relowner"] = 99
        return observed

    monkeypatch.setattr(candidate, "_inputs", header_inputs)
    monkeypatch.setattr(payloads.allocations, "_inventory", AsyncMock(side_effect=inventory))
    monkeypatch.setattr(
        pins, "pin_cms_allocation_targets", AsyncMock(side_effect=lambda *args: state.events.append("object_pins"))
    )
    case.driver.fetchrow = AsyncMock(side_effect=fetchrow)
    case.driver.fetchval = AsyncMock(side_effect=fetchval)


def _configure_payloads(state, monkeypatch):
    async def execute(sql, *arguments):
        state.events.append(sql)
        if state.fault == "lock" and "SHARE UPDATE EXCLUSIVE" in sql:
            raise RuntimeError("lock_busy")

    async def copy_table(current, root, receipt):
        state.events.append("copy:" + root)
        assert current._readonly is True
        state.copied[root] = 1
        if state.fault == "copy":
            raise RuntimeError("interrupted_copy")
        if state.fault == "cancel":
            raise asyncio.CancelledError
        return 1

    async def parity(current, root, receipt):
        state.events.append("parity:" + root)
        return 2 if state.fault == "parity" else state.copied[root]

    state.case.driver.execute = AsyncMock(side_effect=execute)
    monkeypatch.setattr(payloads, "_copy_table", AsyncMock(side_effect=copy_table))
    monkeypatch.setattr(payloads, "_parity", AsyncMock(side_effect=parity))


async def _copy(state, current):
    return await payloads.copy_allocated_cms_generic_payloads(current, state.handoff, allocation=state.allocation)


async def test_four_targets_pin_before_scans(generic_copy):
    state, case = generic_copy, generic_copy.case
    before = deepcopy(state.outputs)
    async with (
        case.session.begin(),
        case.database.bind_existing_session(case.session),
        open_workspace(state) as current,
    ):
        state.events.clear()
        proof = await _copy(state, current)
        assert current._readonly is True
        assert current.binding == state.binding
        with pytest.raises(ValueError, match="readonly_workspace"):
            await current.copy_rows("invalid", (), ())
    locks = [event for event in state.events if event.startswith("LOCK TABLE")]
    expected = sorted(
        payloads.storage_relation(case.storage.catalog_schema, "allocated_leaf_" + str(index)) for index in range(4)
    )
    assert locks == [
        "LOCK TABLE " + ",".join("ONLY " + name for name in expected) + " IN SHARE UPDATE EXCLUSIVE MODE NOWAIT"
    ]
    assert state.events.index("header_lock") < state.events.index("object_pins") < state.events.index(locks[0])
    assert state.events.index(locks[0]) < state.events.index("source_scan") < state.events.index("target_scan")
    assert state.events.index("target_scan") < min(state.events.index("copy:" + root) for root in payloads.ROOTS[:4])
    assert set(state.copied) == set(proof["row_counts"]) == set(proof["tables"]) == set(payloads.ROOTS[:4])
    assert proof["contract"] == "cms-prepared-generic-payload-copy-v1" and proof["state"] == "verified_data"
    assert "native_publication" not in proof and state.outputs == before
    assert state.events.count("release") == 1 and "rollback" not in state.events


@pytest.mark.parametrize("fault", ["heap", "missing", "alias", "owner", "wrong_parent", "extra"])
async def test_wrong_allocated_shape_refuses_copy(generic_copy, fault):
    state, case = generic_copy, generic_copy.case
    variants_by_fault = {
        "heap": [{**state.inventory[0], "relkind": "r"}, *state.inventory[1:]],
        "missing": state.inventory[:-1],
        "alias": [*state.inventory[:-1], {**state.inventory[-1], "oid": state.inventory[0]["oid"]}],
        "owner": [*state.inventory[:-1], {**state.inventory[-1], "relowner": 99}],
        "wrong_parent": [*state.inventory[:-1], {**state.inventory[-1], "native": {"parents": [99]}}],
        "extra": [*state.inventory, {**state.inventory[-1], "oid": 99, "relname": "unexpected"}],
    }
    state.inventory = variants_by_fault[fault]
    state.allocation["inventory"] = deepcopy(state.inventory)
    async with (
        case.session.begin(),
        case.database.bind_existing_session(case.session),
        open_workspace(state) as current,
    ):
        with pytest.raises(ValueError, match="generic_inventory_required|generic_partition_required"):
            await _copy(state, current)
    payloads._copy_table.assert_not_awaited()
    assert not state.copied and state.events[-1] == "rollback"


@pytest.mark.parametrize("fault", ["uncommitted", "inventory", "occupied"])
async def test_preconditions_refuse_before_copy(generic_copy, fault):
    state, case = generic_copy, generic_copy.case
    if fault == "uncommitted":
        state.committed = False
    elif fault == "inventory":
        state.inventory[0]["relowner"] = 99
    else:
        state.occupied = True
    async with (
        case.session.begin(),
        case.database.bind_existing_session(case.session),
        open_workspace(state) as current,
    ):
        with pytest.raises(ValueError, match="allocation_not_committed|inventory_changed|target_not_empty"):
            await _copy(state, current)
    payloads._copy_table.assert_not_awaited()
    assert not state.copied and "rollback" in state.events


@pytest.mark.parametrize("fault", ["postlock_inventory", "postcopy_inventory", "lock"])
async def test_exact_inventory_survives_full_copy(generic_copy, fault):
    state, case = generic_copy, generic_copy.case
    state.fault = fault
    error = RuntimeError if fault == "lock" else ValueError
    async with (
        case.session.begin(),
        case.database.bind_existing_session(case.session),
        open_workspace(state) as current,
    ):
        with pytest.raises(error, match="inventory_changed|coordinates_changed|lock_busy"):
            await _copy(state, current)
    if fault != "postcopy_inventory":
        payloads._copy_table.assert_not_awaited()
    assert not state.copied and state.events[-1] == "rollback"


@pytest.mark.parametrize("fault", ["copy", "cancel", "parity"])
async def test_failure_rolls_back_all_targets(generic_copy, fault):
    state, case = generic_copy, generic_copy.case
    state.fault = fault
    error = asyncio.CancelledError if fault == "cancel" else RuntimeError if fault == "copy" else ValueError
    async with (
        case.session.begin(),
        case.database.bind_existing_session(case.session),
        open_workspace(state) as current,
    ):
        with pytest.raises(error):
            await _copy(state, current)
        assert case.session.in_transaction()
    assert not state.copied and state.events[-1] == "rollback"


async def test_untyped_workspace_refuses_without_sql():
    with pytest.raises(ValueError, match="scope_required"):
        await payloads.copy_allocated_cms_generic_payloads(None, None, allocation={})


async def test_four_target_read_keeps_private_heaps_and_performs_no_copy(generic_copy):
    state, case = generic_copy, generic_copy.case
    state.copied = dict.fromkeys(payloads.ROOTS[:4], 1)
    async with (
        case.session.begin(),
        case.database.bind_existing_session(case.session),
        open_workspace(state) as current,
    ):
        before = deepcopy(state.outputs)
        proof = await payloads.verify_allocated_cms_generic_payloads(
            current, state.handoff, allocation=state.allocation
        )
        assert set(proof["row_counts"]) == set(proof["tables"]) == set(payloads.ROOTS[:4])
        assert proof["contract"] == "cms-prepared-generic-payload-copy-v1"
        assert state.inventory_reads == 2 and state.outputs == before
        assert not any(event.startswith("copy:") for event in state.events)
        payloads._copy_table.assert_not_awaited()


@pytest.mark.parametrize("change", ["initial_inventory", "final_inventory", "coordinates", "parity"])
async def test_four_target_read_refuses_drift(generic_copy, monkeypatch, change):
    state, case = generic_copy, generic_copy.case
    state.copied = dict.fromkeys(payloads.ROOTS[:4], 1)
    async with (
        case.session.begin(),
        case.database.bind_existing_session(case.session),
        open_workspace(state) as current,
    ):
        if change == "initial_inventory":
            state.inventory[0]["relowner"] = 99
        elif change == "final_inventory":
            payloads.allocations._inventory.side_effect = [deepcopy(state.inventory), []]
        elif change == "coordinates":
            original = payloads._coordinates
            sources = await original(current, state.handoff)
            changed = deepcopy(sources)
            changed[payloads.ROOTS[0]]["oid"] += 1
            monkeypatch.setattr(payloads, "_coordinates", AsyncMock(side_effect=[sources, changed]))
        else:
            payloads._parity.side_effect = ValueError("cms_prepared_payloads_content_changed")
        with pytest.raises(ValueError, match="inventory_changed|coordinates_changed|content_changed"):
            await payloads.verify_allocated_cms_generic_payloads(current, state.handoff, allocation=state.allocation)
        payloads._copy_table.assert_not_awaited()
