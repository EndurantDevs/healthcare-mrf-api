# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pin facade ownership and reliable original-session cleanup."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_dataset_rehydrate_pin as pin
from process.provider_directory_dataset_rehydrate_types import DatasetRehydrationError


@pytest.mark.asyncio
async def test_reader_facade_refuses_other_tasks_and_expired_scope():
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    reader = pin._SourceReader(session, "fixture", {"root": {"name": "child"}})
    assert reader.relation("root") == '"fixture"."child"'
    with pytest.raises(DatasetRehydrationError, match="reader_owner_changed"):
        await asyncio.create_task(reader.scalar("SELECT 1"))
    with pytest.raises(DatasetRehydrationError, match="relation_not_pinned"):
        reader.relation("other")
    reader.deactivate()
    with pytest.raises(DatasetRehydrationError, match="reader_owner_changed"):
        await reader.scalar("SELECT 1")
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_reader_facade_refuses_ended_transaction():
    reader = pin._SourceReader(SimpleNamespace(in_transaction=lambda: False), "fixture", {})
    with pytest.raises(DatasetRehydrationError, match="reader_transaction_ended"):
        await reader.first("SELECT 1")


@pytest.mark.asyncio
async def test_cleanup_closes_even_when_rollback_raises():
    session = SimpleNamespace(rollback=AsyncMock(side_effect=RuntimeError("rollback failed")), close=AsyncMock())
    with pytest.raises(RuntimeError, match="rollback failed"):
        await pin._close_reader(session)
    session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_cleanup_drains_cancellation_before_closing():
    entered, release, completed = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def rollback():
        entered.set()
        await release.wait()
        completed.set()

    session = SimpleNamespace(rollback=rollback, close=AsyncMock())
    task = asyncio.create_task(pin._close_reader(session))
    await entered.wait()
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert completed.is_set()
    session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_database_probe_requires_native_lock_contention():
    publisher, reader = (
        SimpleNamespace(fetchval=AsyncMock(return_value=True)),
        SimpleNamespace(fetchval=AsyncMock(return_value=True)),
    )
    with pytest.raises(DatasetRehydrationError, match="reader_database_changed"):
        await pin._same_database(publisher, reader)
    assert publisher.fetchval.call_args.args[1] == reader.fetchval.call_args.args[1]
