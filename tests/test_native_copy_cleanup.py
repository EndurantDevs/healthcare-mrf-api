# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Interrupted COPY drops its exact transport without a second cancel handshake."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from process import native_copy_cleanup as cleanup


async def test_interrupted_copy_terminates_only_its_exact_driver_without_graceful_close():
    driver = SimpleNamespace(close=AsyncMock(), terminate=Mock())
    unrelated = SimpleNamespace(close=AsyncMock(), terminate=Mock())
    await cleanup.close_interrupted_copy(driver)
    driver.terminate.assert_called_once_with()
    driver.close.assert_not_awaited()
    unrelated.terminate.assert_not_called()
    unrelated.close.assert_not_awaited()


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
