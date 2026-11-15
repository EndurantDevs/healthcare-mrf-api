# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exceptional cancellation-test paths join publication before releasing resources."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from process import scoped_catalog_named_publication as publication
from tests import test_named_scoped_catalog_postgres as native


@pytest.mark.parametrize("failure", ["before_copy", "cancel_transaction"])
async def test_cancellation_helper_joins_pending_publication_on_failure(monkeypatch, failure):
    finished = asyncio.Event()
    pending_tasks = []
    wait_for = asyncio.wait_for

    async def bounded_wait(awaitable, _timeout):
        return await wait_for(awaitable, 0.05)

    @asynccontextmanager
    async def publisher_transaction():
        yield None

    @asynccontextmanager
    async def failed_cancel_transaction():
        raise RuntimeError("synthetic cancel transaction failure")
        yield

    async def held_publish(_session, _attempt, _settings, *, source_copy):
        pending_tasks.append(asyncio.current_task())
        try:
            if failure == "before_copy":
                await asyncio.Event().wait()
            else:
                await source_copy.copy_rows()
        finally:
            finished.set()

    monkeypatch.setattr(publication, "publish_managed_attempt", held_publish)
    monkeypatch.setattr(native, "_attempt", lambda candidate: candidate)
    monkeypatch.setattr(native.asyncio, "wait_for", bounded_wait)
    actors = SimpleNamespace(
        publisher=SimpleNamespace(begin=publisher_transaction), builder=SimpleNamespace(begin=failed_cancel_transaction)
    )
    if failure == "before_copy":
        with pytest.raises(TimeoutError):
            await native._assert_cancellation_during_copy(actors, {}, None)
    else:
        with pytest.raises(RuntimeError, match="synthetic cancel transaction failure"):
            await native._assert_cancellation_during_copy(actors, {}, None)
    assert finished.is_set()
    assert len(pending_tasks) == 1 and pending_tasks[0].done()
