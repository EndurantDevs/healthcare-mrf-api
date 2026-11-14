# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh cleanup reads preserve complete logical inventory scope and bounded folds."""

import asyncio
import copy
import json
from dataclasses import replace
from functools import partial
from types import SimpleNamespace
from uuid import UUID

import pytest

from process import registry_source_inventory_maintenance as maintenance
from process import registry_source_inventory_store as store
from process.registry_source_inventory_contract import (
    RegistrySourceInventoryError,
    source_inventory_page,
    source_inventory_root,
)
from process.registry_source_inventory_reference import SourceInventoryReference
from tests.registry_source_recipe_support import _example
from tests.test_registry_source_inventory_contract import _entries
from tests.test_registry_source_inventory_reference import _candidate
from tests.test_registry_source_inventory_store_contracts import _connection, _target


async def _snapshot_rows(case, query, *arguments):
    if query.startswith("SELECT * FROM"):
        assert query == 'SELECT * FROM "control".network_membership_candidate WHERE candidate_id=$1'
        assert arguments == (UUID(case.target.candidate_id),)
        case.snapshots += 1
        return copy.deepcopy(case.initial_candidate if case.snapshots == 1 else case.final_candidate)
    assert query.startswith("SELECT count(*)") and query.endswith("FROM " + store._relation(case.target))
    assert arguments == () and case.snapshots == 1
    return {"total": len(case.stored_entries), "invalid": 0}


async def _inventory_page(case, connection, relation, after, row_limit, byte_limit):
    assert connection is case.connection and relation == store._relation(case.target)
    assert row_limit == 100 and byte_limit == maintenance.MAX_INVENTORY_PAGE_BYTES and case.snapshots == 1
    case.pages.append(after)
    return source_inventory_page(
        (entry for entry in case.stored_entries if entry.ordinal > after),
        after_ordinal=after,
        row_limit=row_limit,
        byte_limit=byte_limit,
    )


def _case(monkeypatch, count=1):
    copy_target = _target()
    entries = _entries(_example(index) for index in range(count))
    root = source_inventory_root(entries)
    candidate_by_field = {
        **{
            field: getattr(copy_target, field)
            for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        },
        **_candidate(SourceInventoryReference(root)),
        "state": "ready",
        "validation_json": {},
    }
    final_candidate_by_field = {
        **copy.deepcopy(candidate_by_field),
        "state": "published",
        "validation_json": {"checkpoint": "advanced"},
    }
    case = SimpleNamespace(
        target=copy_target,
        initial_candidate=candidate_by_field,
        final_candidate=final_candidate_by_field,
        root=root,
        expected_entries=entries,
        stored_entries=list(entries),
        connection=_connection(),
        snapshots=0,
        pages=[],
    )
    case.connection.fetchval.return_value = "read committed"
    case.connection.fetchrow.side_effect = partial(_snapshot_rows, case)
    monkeypatch.setattr(store, "_read_page", partial(_inventory_page, case))
    return case


async def _read(case):
    return await maintenance.read_sealed_source_inventory_for_maintenance(
        case.connection, case.target, control_schema="control"
    )


@pytest.mark.parametrize("count", [0, 1, 201])
async def test_maintenance_folds_complete_named_inventory(monkeypatch, count):
    case = _case(monkeypatch, count)
    candidate, receipt, entries = await _read(case)
    assert candidate == case.final_candidate and entries == case.expected_entries
    assert (receipt.schema_name, receipt.table_name, receipt.root) == (
        case.target.schema_name,
        store.INVENTORY_TABLE,
        source_inventory_root(entries),
    )
    assert case.snapshots == 2 and len(case.pages) == max(1, (count + 99) // 100)
    case.connection.fetchval.assert_awaited_once_with("SHOW transaction_isolation")
    case.connection.execute.assert_awaited_once_with(
        f"LOCK TABLE ONLY {store._relation(case.target)} IN ACCESS SHARE MODE NOWAIT"
    )
    case.connection.copy_records_to_table.assert_not_awaited()
    assert case.connection.events == []


@pytest.mark.parametrize(
    "isolation,active", [("repeatable read", True), ("serializable", True), ("read committed", False)]
)
async def test_maintenance_requires_fresh_active_rc_before_reads(monkeypatch, isolation, active):
    case = _case(monkeypatch)
    case.connection.is_in_transaction.return_value = active
    case.connection.fetchval.return_value = isolation
    with pytest.raises(ValueError):
        await _read(case)
    case.connection.fetchrow.assert_not_awaited()
    case.connection.execute.assert_not_awaited()
    assert case.pages == []


async def test_maintenance_refuses_non_candidate_context_before_io(monkeypatch):
    case = _case(monkeypatch)
    with pytest.raises(ValueError):
        await maintenance.read_sealed_source_inventory_for_maintenance(
            case.connection, object(), control_schema="control"
        )
    case.connection.fetchval.assert_not_awaited()
    case.connection.fetchrow.assert_not_awaited()


@pytest.mark.parametrize("missing", ["candidate", "inventory"])
async def test_maintenance_requires_candidate_and_inventory_before_lock(monkeypatch, missing):
    case = _case(monkeypatch)
    if missing == "candidate":
        case.initial_candidate = None
    else:
        case.initial_candidate.update(source_recipes_json=[], source_generations={})
    with pytest.raises(RegistrySourceInventoryError):
        await _read(case)
    case.connection.execute.assert_not_awaited()
    assert case.pages == []


@pytest.mark.parametrize("stage", ["before", "after"])
@pytest.mark.parametrize(
    "field", ["dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name", "state", "reference"]
)
async def test_maintenance_rejects_changed_candidate_scope(monkeypatch, stage, field):
    case = _case(monkeypatch)
    candidate = case.initial_candidate if stage == "before" else case.final_candidate
    if field == "state":
        candidate["state"] = "open"
    elif field == "reference":
        candidate.update(_candidate(SourceInventoryReference(source_inventory_root(()))))
    else:
        candidate[field] = "other"
    with pytest.raises(RegistrySourceInventoryError):
        await _read(case)
    if stage == "before" and field != "reference":
        case.connection.execute.assert_not_awaited()
        assert case.pages == []


@pytest.mark.parametrize("field", ["source_recipes_json", "source_generations"])
async def test_maintenance_rejects_post_fold_source_metadata_change(monkeypatch, field):
    case = _case(monkeypatch)
    case.final_candidate[field] = {}
    with pytest.raises(RegistrySourceInventoryError):
        await _read(case)
    assert case.snapshots == 2 and case.pages == [0]


@pytest.mark.parametrize("changed", [False, True])
async def test_maintenance_compares_all_logical_generation_pins(monkeypatch, changed):
    case = _case(monkeypatch)
    case.initial_candidate["source_generations"]["npi"] = "edition-one"
    case.final_candidate["source_generations"]["npi"] = "edition-two" if changed else "edition-one"
    case.final_candidate["source_generations"] = json.dumps(case.final_candidate["source_generations"])
    if changed:
        with pytest.raises(RegistrySourceInventoryError, match="root_changed"):
            await _read(case)
    else:
        _, receipt, entries = await _read(case)
        assert receipt.root == case.root and entries == case.expected_entries
    assert case.snapshots == 2 and case.pages == [0]


@pytest.mark.parametrize("damage", ["missing", "content", "ordinal"])
async def test_maintenance_keeps_real_fold_data_refusals(monkeypatch, damage):
    case = _case(monkeypatch)
    if damage == "missing":
        case.stored_entries.clear()
    elif damage == "content":
        case.stored_entries[:] = _entries((_example(99),))
    else:
        case.stored_entries[0] = replace(case.stored_entries[0], ordinal=2)
    with pytest.raises(RegistrySourceInventoryError):
        await _read(case)
    assert case.snapshots == 1


@pytest.mark.parametrize("stage", ["first_read", "read_lock", "fold", "second_read"])
@pytest.mark.parametrize("error_type", [ValueError, asyncio.CancelledError])
async def test_maintenance_propagates_errors_and_cancellation(monkeypatch, stage, error_type):
    case = _case(monkeypatch)
    refusal = error_type("inventory read failed")
    original_snapshot = case.connection.fetchrow.side_effect

    async def snapshot(query, *arguments):
        if query.startswith("SELECT * FROM") and (
            stage == "first_read" or stage == "second_read" and case.snapshots == 1
        ):
            raise refusal
        return await original_snapshot(query, *arguments)

    case.connection.fetchrow.side_effect = snapshot
    if stage == "read_lock":
        case.connection.execute.side_effect = refusal
    if stage == "fold":

        async def page(*arguments):
            raise refusal

        monkeypatch.setattr(store, "_read_page", page)
    with pytest.raises(error_type) as error:
        await _read(case)
    assert error.value is refusal
    assert case.connection.events == []
