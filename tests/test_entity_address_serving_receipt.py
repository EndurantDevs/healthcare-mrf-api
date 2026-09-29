# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary cutover callbacks preserve one common serving predecessor."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_serving_receipt as continuity
from tests.test_provider_directory_cms_serving_receipt import _payload


class _Database:
    def __init__(self, *, table=True, history=True, successor=False):
        self.session = SimpleNamespace(
            in_transaction=lambda: True,
            scalar=AsyncMock(side_effect=[history, successor]),
            execute=AsyncMock(),
        )
        self.scalar = AsyncMock(side_effect=[table, "50ms"])
        self.status = AsyncMock()

    def _transaction_binding(self):
        return SimpleNamespace(session=self.session)

    @asynccontextmanager
    async def transaction(self):
        yield self.session


@pytest.fixture
def current_pair(monkeypatch):
    payload = _payload()
    payload["address"].update(local_lineage_id="lineage", local_generation=1)
    snapshot_by_field = {
        key: deepcopy(payload[key]) for key in ("address", "doctors", "profile", "alias_generation", "overlay_oid")
    }
    result = deepcopy(snapshot_by_field)
    result["address"].update(local_generation=2, origin_lineage_id="lineage", origin_generation=2)
    predecessor_by_field = {"receipt_id": "a" * 64, "payload": payload}
    capture = AsyncMock(side_effect=[snapshot_by_field, result])
    read = AsyncMock(return_value=predecessor_by_field)
    append = AsyncMock(return_value="b" * 64)
    monkeypatch.setattr(continuity.receipts, "capture_native_dependencies", capture)
    monkeypatch.setattr(continuity.receipts, "read_current_receipt", read)
    monkeypatch.setattr(continuity.receipts, "append_serving_receipt", append)
    return predecessor_by_field, snapshot_by_field, result, capture, read, append


@pytest.mark.asyncio
@pytest.mark.parametrize("table,history", [(False, False), (True, False)])
async def test_no_common_history_preserves_ordinary_publication(table, history, current_pair):
    database = _Database(table=table, history=history)
    callbacks = continuity.ordinary_address_receipt_callbacks(database, "synthetic", repr)
    await callbacks.before_cutover()
    await callbacks.after_publish()
    current_pair[3].assert_not_awaited()
    current_pair[4].assert_not_awaited()
    current_pair[5].assert_not_awaited()


@pytest.mark.asyncio
async def test_successor_uses_exact_predecessor_and_native_result(current_pair):
    predecessor, snapshot, result, capture, read, append = current_pair
    database = _Database()
    callbacks = continuity.ordinary_address_receipt_callbacks(database, "synthetic", repr)
    await callbacks.before_cutover()
    assert "IN SHARE MODE NOWAIT" in str(database.session.execute.await_args_list[0].args[0])
    database.status.assert_awaited_once_with("SET LOCAL lock_timeout = '50ms';")
    await callbacks.after_publish()
    assert capture.await_args_list[0].args == (database.session, "synthetic")
    assert capture.await_args_list[0].kwargs == {"lock": True}
    assert capture.await_args_list[1].args == (database.session, "synthetic")
    payload = append.await_args.args[2]
    assert payload["predecessor_receipt_id"] == predecessor["receipt_id"]
    assert payload["expected_incumbent"] == payload["desired_datasets"][0]
    assert {key: payload[key] for key in result} == result
    assert payload["selection"] == predecessor["payload"]["selection"]
    assert payload["cms"] == predecessor["payload"]["cms"]
    assert predecessor["payload"]["address"] == snapshot["address"]


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_current,successor", [(True, False), (False, True)])
async def test_inconsistent_existing_history_fails_before_swap(current_pair, missing_current, successor):
    if missing_current:
        current_pair[4].return_value = None
    callbacks = continuity.ordinary_address_receipt_callbacks(_Database(successor=successor), "synthetic", repr)
    with pytest.raises(RuntimeError, match="history_inconsistent"):
        await callbacks.before_cutover()
    current_pair[5].assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["profile", "doctors", "alias_generation", "overlay_oid", "address"])
async def test_after_publish_rejects_unrelated_drift_or_missing_address_advance(current_pair, field):
    if field == "address":
        current_pair[2][field] = current_pair[1][field]
    else:
        current_pair[2][field] = "changed"
    callbacks = continuity.ordinary_address_receipt_callbacks(_Database(), "synthetic", repr)
    await callbacks.before_cutover()
    with pytest.raises(RuntimeError, match="dependencies_changed|address_not_advanced"):
        await callbacks.after_publish()
    current_pair[5].assert_not_awaited()


@pytest.mark.asyncio
async def test_callbacks_cannot_append_on_a_different_session(current_pair):
    database = _Database()
    callbacks = continuity.ordinary_address_receipt_callbacks(database, "synthetic", repr)
    await callbacks.before_cutover()
    database.session = SimpleNamespace(in_transaction=lambda: True)
    with pytest.raises(RuntimeError, match="transaction_changed"):
        await callbacks.after_publish()
    current_pair[5].assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("bound", [False, True])
async def test_existing_history_requires_an_active_publication_transaction(current_pair, bound):
    database = _Database()
    if bound:
        database.session.in_transaction = lambda: False
    else:
        database._transaction_binding = lambda: None
    callbacks = continuity.ordinary_address_receipt_callbacks(database, "synthetic", repr)
    with pytest.raises(RuntimeError, match="requires_transaction"):
        await callbacks.before_cutover()
    current_pair[3].assert_not_awaited()
    current_pair[5].assert_not_awaited()
