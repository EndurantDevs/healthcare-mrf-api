# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Interrupted COPY drains cancellation before abandoning its exact transport."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import native_copy_cleanup as cleanup


async def test_interrupted_copy_closes_only_its_exact_driver():
    transport = SimpleNamespace(abort=Mock())
    driver = SimpleNamespace(close=AsyncMock(), terminate=Mock(), _transport=transport)
    unrelated = SimpleNamespace(close=AsyncMock(), terminate=Mock())
    await cleanup.close_interrupted_copy(driver)
    driver.close.assert_awaited_once_with(timeout=cleanup._COPY_CLOSE_TIMEOUT_SECONDS)
    driver.terminate.assert_not_called()
    transport.abort.assert_not_called()
    unrelated.terminate.assert_not_called()
    unrelated.close.assert_not_awaited()


async def test_interrupted_copy_terminates_exact_driver_when_close_fails():
    driver = SimpleNamespace(close=AsyncMock(side_effect=TimeoutError("cancel timeout")), terminate=Mock())
    await cleanup.close_interrupted_copy(driver)
    driver.close.assert_awaited_once_with(timeout=cleanup._COPY_CLOSE_TIMEOUT_SECONDS)
    driver.terminate.assert_called_once_with()


@pytest.mark.parametrize("terminate_fails", [False, True])
async def test_interrupted_copy_aborts_saved_transport_after_driver_loses_protocol(terminate_fails):
    transport = SimpleNamespace(abort=Mock())
    driver = SimpleNamespace(_transport=transport)

    async def close(*, timeout):
        assert timeout == cleanup._COPY_CLOSE_TIMEOUT_SECONDS
        driver._transport = None
        raise TimeoutError("driver already marked closed")

    driver.close = close
    driver.terminate = Mock(side_effect=RuntimeError("termination failed") if terminate_fails else None)
    if terminate_fails:
        with pytest.raises(RuntimeError, match="termination failed"):
            await cleanup.close_interrupted_copy(driver)
    else:
        await cleanup.close_interrupted_copy(driver)
    driver.terminate.assert_called_once_with()
    transport.abort.assert_called_once_with()


async def test_interrupted_copy_bounds_a_hanging_graceful_close(monkeypatch):
    monkeypatch.setattr(cleanup, "_COPY_CLOSE_TIMEOUT_SECONDS", 0.01)

    async def close(*, timeout):
        assert timeout == 0.01
        await asyncio.Future()

    driver = SimpleNamespace(close=close, terminate=Mock())
    await asyncio.wait_for(cleanup.close_interrupted_copy(driver), 1)
    driver.terminate.assert_called_once_with()


async def test_invalidating_native_driver_never_calls_session_invalidation():
    driver = SimpleNamespace(invalidate=AsyncMock())
    await cleanup.invalidate_copy_owner(driver, driver)
    driver.invalidate.assert_not_awaited()


async def test_caller_cancellation_still_drains_original_session_invalidation():
    entered, release, drained = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def invalidate():
        entered.set()
        try:
            await release.wait()
        finally:
            drained.set()

    session = SimpleNamespace(invalidate=invalidate)
    task = asyncio.create_task(cleanup.invalidate_copy_owner(session, object()))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        release.set()
        await asyncio.wait_for(task, 1)
        assert drained.is_set()
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
