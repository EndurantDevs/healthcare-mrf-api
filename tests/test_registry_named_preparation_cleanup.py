# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Interrupted administrative transactions finish rollback or discard bounded capacity."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import ResourceClosedError

from process import registry_named_preparation_administration as administration


@pytest.mark.asyncio
async def test_owned_transaction_commits_once_and_preserves_body_failure():
    transaction = SimpleNamespace(start=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock())
    owner = SimpleNamespace(invalidate=AsyncMock())
    async with administration._owned_transaction(transaction, owner) as active:
        assert active is transaction
    transaction.commit.assert_awaited_once()
    original = ValueError("candidate validation failed")
    with pytest.raises(ValueError) as failure:
        async with administration._owned_transaction(transaction, owner):
            raise original
    assert failure.value is original
    transaction.rollback.assert_awaited_once()
    assert transaction.commit.await_count == 1
    owner.invalidate.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["commit", "rollback"])
async def test_failed_transaction_cleanup_discards_connection_and_preserves_original_error(phase):
    original, cleanup_failure = ValueError("candidate failed"), RuntimeError("connection lost")
    transaction = SimpleNamespace(start=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock())
    owner = SimpleNamespace(invalidate=AsyncMock(side_effect=ResourceClosedError()))
    getattr(transaction, phase).side_effect = cleanup_failure
    with pytest.raises((ValueError, RuntimeError)) as failure:
        async with administration._owned_transaction(transaction, owner):
            if phase == "rollback":
                raise original
    assert failure.value is (original if phase == "rollback" else cleanup_failure)
    if phase == "rollback":
        assert failure.value.__cause__ is cleanup_failure
    owner.invalidate.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("suppress_first_cancel", [False, True])
async def test_rollback_timeout_discards_capacity_and_cancels_pending_work(monkeypatch, suppress_first_cancel):
    monkeypatch.setattr(administration, "_CLEANUP_SECONDS", 0.01)
    stopped = asyncio.Event()

    async def rollback():
        try:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                if not suppress_first_cancel:
                    raise
                await asyncio.Event().wait()
        finally:
            stopped.set()

    transaction = SimpleNamespace(start=AsyncMock(), commit=AsyncMock(), rollback=rollback)
    owner = SimpleNamespace(invalidate=AsyncMock())
    original = ValueError("candidate failed")
    with pytest.raises(ValueError) as failure:
        async with administration._owned_transaction(transaction, owner):
            raise original
    assert failure.value is original
    assert isinstance(failure.value.__cause__, TimeoutError)
    await asyncio.wait_for(stopped.wait(), 1)
    owner.invalidate.assert_awaited_once()
    transaction.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_repeated_cancellation_drains_rollback_before_returning_capacity():
    rolling_back, release = asyncio.Event(), asyncio.Event()

    async def rollback():
        rolling_back.set()
        await release.wait()

    transaction = SimpleNamespace(start=AsyncMock(), commit=AsyncMock(), rollback=rollback)
    owner = SimpleNamespace(invalidate=AsyncMock())

    async def candidate():
        async with administration._owned_transaction(transaction, owner):
            raise asyncio.CancelledError("initial cancellation")

    task = asyncio.create_task(candidate())
    try:
        await rolling_back.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError, match="initial cancellation"):
        await task
    owner.invalidate.assert_not_awaited()
    transaction.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_commit_failure_retains_original_error_when_invalidation_times_out(monkeypatch):
    monkeypatch.setattr(administration, "_CLEANUP_SECONDS", 0.01)
    stopped = asyncio.Event()

    async def invalidate():
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    original = RuntimeError("commit acknowledgement lost")
    transaction = SimpleNamespace(start=AsyncMock(), commit=AsyncMock(side_effect=original), rollback=AsyncMock())
    with pytest.raises(RuntimeError) as failure:
        async with administration._owned_transaction(transaction, SimpleNamespace(invalidate=invalidate)):
            assert transaction.commit.await_count == 0
    assert failure.value is original and isinstance(failure.value.__cause__, TimeoutError)
    await asyncio.wait_for(stopped.wait(), 1)
    transaction.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_administrative_freeze_rejects_incomplete_configuration_before_starting_a_transaction():
    connection = SimpleNamespace(begin=AsyncMock())
    with pytest.raises(ValueError, match="config_required"):
        await administration._freeze_registry_generation(connection, {}, "invalid")
    connection.begin.assert_not_called()
