# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private plan scratch state never outlives its physical Reader connection."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import custom_import_plan_sql
from db.reader_plan_workspace import plan_reader_connection


def workspace_case(monkeypatch, outcome):
    events = []
    connection = SimpleNamespace()

    async def options(**kwargs):
        events.append(("readonly", kwargs["postgresql_readonly"]))
        return connection

    @asynccontextmanager
    async def begin():
        events.append("begin")
        yield
        events.append("committed")

    @asynccontextmanager
    async def connect():
        try:
            yield connection
        finally:
            events.append("closed")

    async def invalidate():
        events.append("invalidated")

    async def prepare(actual, *, preserve_rows):
        assert actual is connection and preserve_rows is True
        events.append("prepared")
        if outcome == "prepare_failure":
            raise RuntimeError("preparation failed")

    connection.execution_options = options
    connection.begin = begin
    connection.invalidate = invalidate
    monkeypatch.setattr(custom_import_plan_sql, "prepare_plan_query_tables", prepare)
    reader = SimpleNamespace(engine=SimpleNamespace(connect=connect), _reader_login=("reader",))
    return reader, connection, events


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "prepare_failure"])
async def test_workspace_prepares_before_read_only_and_retires_connection_on_every_exit(monkeypatch, outcome):
    reader, connection, events = workspace_case(monkeypatch, outcome)
    if outcome == "success":
        async with plan_reader_connection(reader) as actual:
            assert actual is connection
            assert events == [
                ("readonly", False),
                "begin",
                "prepared",
                "committed",
                ("readonly", True),
            ]
    else:
        with pytest.raises(RuntimeError, match="preparation failed"):
            async with plan_reader_connection(reader):
                pytest.fail("An invalid workspace was returned")
    assert events[-2:] == ["invalidated", "closed"]


@pytest.mark.asyncio
async def test_cancelled_reader_waits_for_connection_retirement(monkeypatch):
    entered = asyncio.Event()
    invalidation_started = asyncio.Event()
    invalidation_finished = asyncio.Event()
    release_invalidation = asyncio.Event()
    connection = SimpleNamespace()
    connection.execution_options = AsyncMock(return_value=connection)

    @asynccontextmanager
    async def transaction():
        yield

    @asynccontextmanager
    async def connect():
        yield connection

    async def invalidate():
        invalidation_started.set()
        await release_invalidation.wait()
        invalidation_finished.set()

    async def consume():
        async with plan_reader_connection(reader):
            entered.set()

    connection.begin = transaction
    connection.invalidate = invalidate
    monkeypatch.setattr(custom_import_plan_sql, "prepare_plan_query_tables", AsyncMock())
    reader = SimpleNamespace(engine=SimpleNamespace(connect=connect), _reader_login=("reader",))
    task = asyncio.create_task(consume())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.wait_for(invalidation_started.wait(), 1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert not invalidation_finished.is_set()
    finally:
        release_invalidation.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    assert invalidation_finished.is_set()
