# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named inventory operations preserve complete scope, bounded rows and atomic writes."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from process import registry_source_inventory_store as store
from process.network_membership_candidate_lifecycle import MembershipCandidateError
from process.network_membership_copy import MembershipCopyTarget
from process.registry_source_inventory_contract import RegistrySourceInventoryError, source_inventory_root
from process.registry_source_inventory_reference import (
    INVENTORY_DIGEST_KEY,
    SourceInventoryReference,
    encode_source_inventory_reference,
)


class _CatalogRow(dict):
    def __iter__(self):
        return iter(self.values())


def _target():
    candidate_id = uuid4()
    return MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(candidate_id), "network_candidate_" + candidate_id.hex
    )


def _candidate(target, *, state="open"):
    root = source_inventory_root(())
    return {
        **{
            field: getattr(target, field)
            for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        },
        "state": state,
        "source_generations": {INVENTORY_DIGEST_KEY: root.entries_sha256},
        "source_recipes_json": encode_source_inventory_reference(SourceInventoryReference(root)),
        "validation_json": {},
    }


def _connection(*, active=True, rows=()):
    events = []

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    return SimpleNamespace(
        is_in_transaction=Mock(return_value=active),
        transaction=transaction,
        execute=AsyncMock(),
        fetchrow=AsyncMock(side_effect=rows),
        fetch=AsyncMock(),
        fetchval=AsyncMock(),
        copy_records_to_table=AsyncMock(),
        events=events,
    )


async def _write(connection, target):
    return await store.write_source_inventory(
        connection, target, (), expected_root=source_inventory_root(()), control_schema="registry_example"
    )


@pytest.mark.asyncio
async def test_inventory_write_requires_outer_transaction_before_control_or_ddl():
    connection = _connection(active=False)
    with pytest.raises(MembershipCandidateError, match="caller-owned transaction"):
        await _write(connection, _target())
    connection.fetchrow.assert_not_awaited()
    connection.execute.assert_not_awaited()
    assert connection.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "foreign", "closed"])
async def test_inventory_write_refuses_unregistered_foreign_or_closed_candidate_before_ddl(change):
    target = _target()
    candidate_by_field = (
        None if change == "missing" else _candidate(target, state="sealed" if change == "closed" else "open")
    )
    if change == "foreign":
        candidate_by_field["producer_id"] = str(uuid4())
    connection = _connection(rows=[candidate_by_field])
    with pytest.raises(MembershipCandidateError):
        await _write(connection, target)
    connection.execute.assert_not_awaited()
    connection.copy_records_to_table.assert_not_awaited()
    assert connection.events == ["begin", "rollback"]


@pytest.mark.asyncio
async def test_existing_inventory_view_cannot_be_reused_as_its_original_heap():
    target = _target()
    connection = _connection(rows=[_candidate(target), _CatalogRow(relkind="v", relpersistence="p")])
    with pytest.raises(RegistrySourceInventoryError, match="relation_invalid"):
        await _write(connection, target)
    connection.execute.assert_not_awaited()
    connection.copy_records_to_table.assert_not_awaited()
    assert connection.events == ["begin", "rollback"]


@pytest.mark.asyncio
async def test_existing_inventory_replay_locks_and_verifies_full_content_root(monkeypatch):
    target = _target()
    connection = _connection(rows=[_candidate(target), _CatalogRow(relkind="r", relpersistence="p")])
    folded = AsyncMock(return_value=source_inventory_root(()))
    monkeypatch.setattr(store, "_fold_inventory", folded)
    receipt = await _write(connection, target)
    assert (receipt.schema_name, receipt.table_name, receipt.root) == (
        target.schema_name,
        store.INVENTORY_TABLE,
        source_inventory_root(()),
    )
    assert connection.execute.await_count == 1
    assert connection.execute.await_args.args[0].startswith("LOCK TABLE ONLY ")
    folded.assert_awaited_once_with(
        connection, store._relation(target), receipt.root, 100, store.MAX_INVENTORY_PAGE_BYTES
    )
    connection.copy_records_to_table.assert_not_awaited()
    connection.fetchval.assert_not_awaited()
    assert connection.events == ["begin", "commit"]


@pytest.mark.asyncio
async def test_raw_inventory_page_uses_qualified_name_without_catalog_locks(monkeypatch):
    target = _target()
    scope_by_field = {
        field: getattr(target, field)
        for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    }
    connection = _connection(rows=[("repeatable read", "on"), scope_by_field])
    page = object()
    read = AsyncMock(return_value=page)
    monkeypatch.setattr(store, "_read_page", read)
    assert await store.read_source_inventory_page(connection, target, control_schema="registry_example") is page
    read.assert_awaited_once_with(connection, store._relation(target), 0, 100, store.MAX_INVENTORY_PAGE_BYTES)
    connection.execute.assert_not_awaited()
    connection.fetchval.assert_not_awaited()
    assert all(
        "pg_locks" not in call.args[0] and "::oid" not in call.args[0] for call in connection.fetchrow.await_args_list
    )
