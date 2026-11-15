# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generic head locks use configured access and exact logical row sets."""

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import registry_company_approval_fence as company
from process import registry_membership_refresh_generic_fence as generic
from process import registry_membership_refresh_runtime as runtime
from process.registry_membership_refresh_contract import RegistryMembershipRefreshUnavailable
from process.registry_membership_refresh_source_fence import RegistryMembershipRefreshHeadFences
from process.registry_membership_refresh_sources import RegistryMembershipRefreshSourceReaders
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorUnavailable
from tests.test_registry_membership_refresh_runtime import configuration


def test_generic_head_pool_preserves_configured_source_access():
    configured = configuration()
    configured["source_profiles"] = {
        "generic": {"head_fence_dsn": "postgresql://metadata_actor:{password}@db.example/example"}
    }
    parsed = runtime._settings(configured)
    assert parsed.generic_head_fence_url.username == "metadata_actor"
    assert parsed.source_reader_url.username == "source_reader"
    assert parsed.aca_head_fence_url is None


@pytest.mark.parametrize(
    "profile",
    [
        {},
        {"head_fence_dsn": "postgresql://metadata_actor:{password}@other.example/example"},
        {"head_fence_dsn": "postgresql://metadata_actor:{password}@db.example/other"},
        {"head_fence_dsn": "postgresql://metadata_actor:{password}@db.example/example", "grant_update": True},
    ],
)
def test_generic_head_configuration_refuses_scope_and_permission_options(profile):
    configured = configuration()
    configured["source_profiles"] = {"generic": profile}
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        runtime._settings(configured)


@pytest.mark.parametrize("reader", ["fence_dsn", "source_reader_dsn", "address_reader_dsn"])
def test_generic_head_configuration_reuses_declared_access(reader):
    configured = configuration()
    configured["source_profiles"] = {"generic": {"head_fence_dsn": configured[reader]}}
    assert runtime._settings(configured).generic_head_fence_url.username


@pytest.mark.asyncio
async def test_head_and_reader_pools_keep_separate_capacity_for_snapshot_lifetime():
    async with AsyncExitStack() as cleanup:
        engines = [
            create_async_engine(
                "postgresql+asyncpg://reader:{password}@db.example/example", pool_size=1, max_overflow=0
            )
            for _ in range(2)
        ]
        for engine in engines:
            cleanup.push_async_callback(engine.dispose)
        readers = RegistryMembershipRefreshSourceReaders(
            "registry",
            async_sessionmaker(engines[0]),
            head_fences=RegistryMembershipRefreshHeadFences(generic_sessions=async_sessionmaker(engines[1])),
        )
        assert (
            readers.source_reader_sessions.kw["bind"].pool is not readers.head_fences.generic_sessions.kw["bind"].pool
        )


@pytest.fixture
def head_scope(monkeypatch):
    events = []
    endpoints = (("source", "source-one", "endpoint-one"), ("source", "source-two", "endpoint-two"))
    driver = SimpleNamespace(fetchval=AsyncMock(return_value=True), fetch=AsyncMock())
    session = SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def entered(*args, **kwargs):
        events.append("enter")
        try:
            yield session
        finally:
            events.append("exit")

    session.begin = entered
    readers = SimpleNamespace(head_fences=SimpleNamespace(generic_sessions=entered))
    monkeypatch.setattr(company, "_company_approval_connection", entered)
    monkeypatch.setattr(generic, "native_driver", AsyncMock(return_value=driver))
    driver.fetch.side_effect = [
        [{"endpoint_id": endpoint[2]} for endpoint in endpoints],
        [{"source_id": endpoint[1], "endpoint_id": endpoint[2]} for endpoint in endpoints],
        [
            {"endpoint_id": endpoint[2], "dataset_id": "generation-" + str(index)}
            for index, endpoint in enumerate(endpoints)
        ],
    ]
    return SimpleNamespace(driver=driver, session=session, readers=readers, endpoints=endpoints, events=events)


@pytest.mark.asyncio
async def test_head_fence_locks_exact_source_and_current_rows_before_yield(head_scope):
    scope = head_scope
    async with generic.generic_refresh_head_fence(scope.readers, scope.endpoints):
        assert scope.driver.fetch.await_count == 3 and scope.events == ["enter", "enter", "enter"]
        assert all("FOR SHARE NOWAIT" in call.args[0] for call in scope.driver.fetch.call_args_list)
    assert scope.events[-2:] == ["exit", "exit"]
    statements = [str(call.args[0]) for call in scope.session.execute.call_args_list]
    assert statements == [
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE",
        "SET LOCAL lock_timeout='250ms'",
        "SET LOCAL statement_timeout='3s'",
    ]
    assert scope.driver.fetchval.await_count == 2
    assert all("pg_try_advisory_xact_lock_shared" in call.args[0] for call in scope.driver.fetchval.call_args_list)
    assert [call.args[1] for call in scope.driver.fetchval.call_args_list] == ["endpoint-one", "endpoint-two"]
    assert "is_current IS TRUE AND status='published'" in scope.driver.fetch.call_args_list[-1].args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["endpoint", "source", "current"])
async def test_head_fence_refuses_missing_or_changed_logical_rows(head_scope, changed):
    scope = head_scope
    row_batches = [
        [{"endpoint_id": endpoint[2]} for endpoint in scope.endpoints],
        [{"source_id": endpoint[1], "endpoint_id": endpoint[2]} for endpoint in scope.endpoints],
        [{"endpoint_id": endpoint[2], "dataset_id": "generation"} for endpoint in scope.endpoints],
    ]
    row_batches[{"endpoint": 0, "source": 1, "current": 2}[changed]] = []
    scope.driver.fetch.side_effect = row_batches
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        async with generic.generic_refresh_head_fence(scope.readers, scope.endpoints):
            pytest.fail("changed logical source rows yielded")
    assert scope.events[-2:] == ["exit", "exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [PermissionError("denied"), asyncio.CancelledError(), RuntimeError("lock")])
async def test_native_lock_failure_propagates_and_releases_context(head_scope, failure):
    head_scope.driver.fetchval.side_effect = failure
    with pytest.raises(type(failure)) as caught:
        async with generic.generic_refresh_head_fence(head_scope.readers, head_scope.endpoints):
            pytest.fail("failed native access yielded")
    assert caught.value is failure and head_scope.events[-2:] == ["exit", "exit"]
    head_scope.driver.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancellation_under_pinned_head_keeps_fast_limits_and_releases_context(head_scope):
    cancellation = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError) as caught:
        async with generic.generic_refresh_head_fence(head_scope.readers, head_scope.endpoints):
            statements = [str(call.args[0]) for call in head_scope.session.execute.call_args_list]
            assert statements[-2:] == ["SET LOCAL lock_timeout='250ms'", "SET LOCAL statement_timeout='3s'"]
            raise cancellation
    assert caught.value is cancellation
    assert head_scope.events[-2:] == ["exit", "exit"]


@pytest.mark.asyncio
async def test_busy_head_lock_refuses_before_row_reads(head_scope):
    head_scope.driver.fetchval.return_value = False
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        async with generic.generic_refresh_head_fence(head_scope.readers, head_scope.endpoints):
            pytest.fail("busy head yielded")
    head_scope.driver.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_or_unordered_head_scope_refuses_before_connection(head_scope):
    head_scope.readers.head_fences.generic_sessions = None
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        async with generic.generic_refresh_head_fence(head_scope.readers, head_scope.endpoints):
            pytest.fail("missing configured head yielded")
    assert head_scope.events == []


@pytest.mark.asyncio
async def test_unordered_head_scope_refuses_before_connection(head_scope):
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        async with generic.generic_refresh_head_fence(head_scope.readers, tuple(reversed(head_scope.endpoints))):
            pytest.fail("unordered configured scope yielded")
    assert head_scope.events == []
