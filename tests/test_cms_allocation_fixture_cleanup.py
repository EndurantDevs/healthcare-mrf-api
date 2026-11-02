# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native allocation cleanup waits for disconnected backends without forcing removal."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests import test_provider_directory_cms_allocation_semantics_postgres as fixture


@pytest.fixture
def allocation_io(monkeypatch):
    state = SimpleNamespace(events=[], activity_times=[], persistent=False)
    catalog, publisher = object(), object()

    async def fetchval(query, *_arguments):
        if "pg_stat_activity" in query:
            assert state.events == ["publisher.closed", "lifecycle.closed"]
            state.activity_times.append(asyncio.get_running_loop().time())
            return state.persistent or len(state.activity_times) == 1
        assert query.startswith("SELECT EXISTS(SELECT FROM pg_database WHERE ") or query.startswith(
            "SELECT EXISTS(SELECT FROM pg_roles WHERE "
        )
        return False

    @asynccontextmanager
    async def lifecycle(_monkeypatch, *, request_failure_budget):
        assert request_failure_budget is False
        try:
            yield catalog
        finally:
            state.events.append("lifecycle.closed")

    @asynccontextmanager
    async def reader(actual_catalog, _url, _database, _publisher):
        assert actual_catalog is catalog
        try:
            yield publisher
        finally:
            state.events.append("publisher.closed")

    state.admin = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(side_effect=fetchval), close=AsyncMock())
    state.connect = AsyncMock(return_value=state.admin)
    state.protect = AsyncMock()
    state.catalog, state.publisher = catalog, publisher
    monkeypatch.setenv("HLTHPRT_CMS_CUSTODY_TEST_DSN", "postgresql://synthetic@127.0.0.1:5432/postgres")
    monkeypatch.setattr(fixture.asyncpg, "connect", state.connect)
    monkeypatch.setattr(fixture, "_lifecycle_scope", lifecycle)
    monkeypatch.setattr(fixture, "_publisher_reader", reader)
    monkeypatch.setattr(fixture, "_protect_allocator", state.protect)
    return state


@pytest.mark.asyncio
async def test_allocation_cleanup_waits_for_backend_then_checks_exact_removal(monkeypatch, allocation_io):
    state = allocation_io
    async with fixture._native_allocations(monkeypatch) as case:
        assert case.catalog is state.catalog and case.publisher is state.publisher
        owner = case.owner
    statements = [call.args[0] for call in state.admin.execute.await_args_list]
    database = statements[0].split('"')[1]
    publisher = statements[2].split('"')[1]
    assert statements == [
        f'CREATE DATABASE "{database}" TEMPLATE template0',
        f'CREATE ROLE "{owner}" NOLOGIN',
        f'CREATE ROLE "{publisher}" LOGIN',
        f'DROP DATABASE IF EXISTS "{database}"',
        f'DROP ROLE IF EXISTS "{publisher}"',
        f'DROP ROLE IF EXISTS "{owner}"',
    ]
    activity_calls = [call for call in state.admin.fetchval.await_args_list if "pg_stat_activity" in call.args[0]]
    assert len(activity_calls) == 3 and all(call.args[1] == database for call in activity_calls)
    catalog_checks = [call for call in state.admin.fetchval.await_args_list if "pg_stat_activity" not in call.args[0]]
    assert [call.args[1] for call in catalog_checks] == [database, owner, publisher, database, publisher, owner]
    assert state.activity_times[1] - state.activity_times[0] >= 0.01
    state.protect.assert_awaited_once_with(state.catalog, owner)
    state.admin.close.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_allocation_cleanup_deadline_preserves_resources_with_active_backend(monkeypatch, allocation_io):
    state = allocation_io
    state.persistent = True
    async with asyncio.timeout(4) as watchdog:
        with pytest.raises(TimeoutError):
            async with fixture._native_allocations(monkeypatch) as case:
                assert case.publisher is state.publisher
    assert not watchdog.expired()
    assert len(state.activity_times) > 1
    assert asyncio.get_running_loop().time() - state.activity_times[0] >= 2.9
    statements = [call.args[0] for call in state.admin.execute.await_args_list]
    assert len(statements) == 3
    assert statements[0].startswith("CREATE DATABASE ")
    assert all(statement.startswith("CREATE ROLE ") for statement in statements[1:])
    state.admin.close.assert_awaited_once_with()
