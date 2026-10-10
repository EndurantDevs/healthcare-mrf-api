# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native one-connection capture custody; component transaction proof only."""

import asyncio
import json
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from db.connection import Database
from process import network_registry_cms_capture_lock as custody
from tests.cms_npd_admission_postgres_support import _database_url, _owned_database
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template


@pytest_asyncio.fixture
async def capture_pool(tmp_path):
    """Register exact database cleanup before creating the native pool fixture."""
    journal = tmp_path / "capture-lock-cleanup.json"
    journal.write_text(json.dumps({"phase": "registered"}))
    async with _owned_database(_database_url()) as (url, _admin):
        journal.write_text(json.dumps({"phase": "created", "database": url.database}))
        engines = []
        try:
            for _ in range(2):
                engines.append(create_async_engine(url, pool_size=1, max_overflow=0, pool_timeout=1))
            async with engines[0].begin() as connection:
                await connection.execute(text("CREATE TABLE capture_probe (marker text PRIMARY KEY)"))
            yield SimpleNamespace(
                engines=engines,
                sessions=async_sessionmaker(
                    engines[0].execution_options(isolation_level="REPEATABLE READ"), expire_on_commit=False
                ),
                other_sessions=async_sessionmaker(engines[1], expire_on_commit=False),
                capture_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
            )
        finally:
            for engine in reversed(engines):
                await engine.dispose()
    journal.write_text(json.dumps({"phase": "cleanup_verified", "database": url.database}))


async def _pid(session):
    return await session.scalar(text("SELECT pg_backend_pid()"))


async def _assert_unlocked(session, capture_id):
    key = int.from_bytes(capture_id.bytes[:8], "big", signed=True)
    assert await session.scalar(text("SELECT pg_try_advisory_lock(:key)"), {"key": key}) is True
    assert await session.scalar(text("SELECT pg_advisory_unlock(:key)"), {"key": key}) is True


async def _assert_native_lock_owner(session, capture_id, backend_pid):
    key = int.from_bytes(capture_id.bytes[:8], "big", signed=True)
    assert await session.scalar(text("SELECT pg_try_advisory_lock(:key)"), {"key": key}) is False
    assert await session.scalar(
        text(
            "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=:pid "
            "AND classid::bigint=:class_id AND objid::bigint=:object_id AND objsubid=1 AND granted)"
        ),
        {
            "pid": backend_pid,
            "class_id": int.from_bytes(capture_id.bytes[:4], "big"),
            "object_id": int.from_bytes(capture_id.bytes[4:8], "big"),
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["commit", "rollback"])
async def test_native_same_backend_transaction_and_successful_pool_reuse(capture_pool, outcome):
    pool = capture_pool
    observed_pids = []

    async def transaction():
        async with custody.registry_cms_capture_transaction(pool.sessions, capture_id=pool.capture_id) as session:
            assert session.in_transaction() and not session.in_nested_transaction()
            coordinates = tuple(
                (await session.execute(text("SELECT pg_backend_pid(),pg_current_xact_id()::text"))).one()
            )
            observed_pids.append(coordinates)
            assert await session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
            assert pool.engines[0].pool.checkedout() == 1
            database = Database()
            database._database_override = pool.engines[0].url.database
            async with database.bind_existing_session(session):
                assert tuple(await database.first("SELECT pg_backend_pid(),pg_current_xact_id()::text")) == coordinates
            await session.execute(text("INSERT INTO capture_probe VALUES ('candidate')"))
            async with pool.other_sessions() as observer:
                assert await observer.scalar(text("SELECT count(*) FROM capture_probe")) == 0
                await _assert_native_lock_owner(observer, pool.capture_id, coordinates[0])
                with pytest.raises(ValueError, match="registry_cms_capture_busy"):
                    async with custody.registry_cms_capture_transaction(pool.sessions, capture_id=pool.capture_id):
                        pytest.fail("A busy one-connection pool must not wait for checkout")
            if outcome == "rollback":
                raise RuntimeError("synthetic body failure")

    if outcome == "rollback":
        with pytest.raises(RuntimeError, match="synthetic body failure"):
            await transaction()
    else:
        await transaction()
    async with pool.sessions() as session:
        assert await session.scalar(text("SELECT count(*) FROM capture_probe")) == (outcome == "commit")
        assert (await _pid(session) == observed_pids[0][0]) == (outcome == "commit")
        await _assert_unlocked(session, pool.capture_id)


@pytest.mark.asyncio
async def test_native_contention_refuses_then_reads_a_fresh_committed_snapshot(capture_pool):
    pool = capture_pool
    async with custody.registry_cms_capture_transaction(pool.sessions, capture_id=pool.capture_id) as holder:
        await holder.execute(text("INSERT INTO capture_probe VALUES ('previous-holder')"))
        with pytest.raises(ValueError, match="registry_cms_capture_busy"):
            async with custody.registry_cms_capture_transaction(pool.other_sessions, capture_id=pool.capture_id):
                pytest.fail("Native capture contention must refuse before yielding a transaction")
    async with custody.registry_cms_capture_transaction(pool.other_sessions, capture_id=pool.capture_id) as fresh:
        assert await fresh.scalar(text("SELECT marker FROM capture_probe")) == "previous-holder"
        assert await fresh.scalar(text("SHOW transaction_isolation")) == "repeatable read"


@pytest.mark.asyncio
async def test_simultaneous_pool_checkout_refuses_without_waiting(capture_pool):
    pool = capture_pool
    release = asyncio.Event()

    async def transaction():
        async with custody.registry_cms_capture_transaction(pool.sessions, capture_id=pool.capture_id) as session:
            await _pid(session)
            await release.wait()

    tasks = tuple(asyncio.create_task(transaction()) for _ in range(2))
    try:
        done, pending = await asyncio.wait(tasks, timeout=2, return_when=asyncio.FIRST_COMPLETED)
        assert len(done) == len(pending) == 1
        error = next(iter(done)).exception()
        assert type(error) is ValueError and str(error) == "registry_cms_capture_busy"
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
    async with pool.sessions() as session:
        await _assert_unlocked(session, pool.capture_id)


@pytest.mark.asyncio
async def test_native_cancellation_rolls_back_and_retires_the_physical_connection(capture_pool):
    pool = capture_pool
    entered = asyncio.Event()
    observed_pids = []

    async def transaction():
        async with custody.registry_cms_capture_transaction(pool.sessions, capture_id=pool.capture_id) as session:
            observed_pids.append(await _pid(session))
            await session.execute(text("INSERT INTO capture_probe VALUES ('cancelled')"))
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(transaction())
    await asyncio.wait_for(entered.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=5)
    async with pool.sessions() as session:
        assert await _pid(session) != observed_pids[0]
        assert await session.scalar(text("SELECT count(*) FROM capture_probe")) == 0
        await _assert_unlocked(session, pool.capture_id)


@pytest.mark.asyncio
async def test_native_cancellation_during_unlock_drains_and_retires(capture_pool, monkeypatch):
    pool = capture_pool
    unlocking = asyncio.Event()
    release = asyncio.Event()
    observed_pids = []
    unlock = custody._unlock_capture

    async def delayed_unlock(connection, key):
        unlocking.set()
        await release.wait()
        await unlock(connection, key)

    monkeypatch.setattr(custody, "_unlock_capture", delayed_unlock)

    async def transaction():
        async with custody.registry_cms_capture_transaction(pool.sessions, capture_id=pool.capture_id) as session:
            observed_pids.append(await _pid(session))
            await session.execute(text("INSERT INTO capture_probe VALUES ('committed-before-cancel')"))

    task = asyncio.create_task(transaction())
    await asyncio.wait_for(unlocking.wait(), timeout=5)
    task.cancel()
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=5)
    async with pool.sessions() as session:
        assert await _pid(session) != observed_pids[0]
        assert await session.scalar(text("SELECT marker FROM capture_probe")) == "committed-before-cancel"
        await _assert_unlocked(session, pool.capture_id)


@pytest.mark.asyncio
async def test_native_commit_ack_loss_retires_without_inventing_rollback(capture_pool, monkeypatch):
    pool = capture_pool
    observed_pids = []
    with pytest.raises(OSError, match="synthetic commit acknowledgement loss"):
        async with custody.registry_cms_capture_transaction(pool.sessions, capture_id=pool.capture_id) as session:
            observed_pids.append(await _pid(session))
            await session.execute(text("INSERT INTO capture_probe VALUES ('commit-unknown')"))
            raw = await (await session.connection()).get_raw_connection()
            adapter = raw.dbapi_connection
            commit = type(adapter).commit

            def lose_ack(connection):
                commit(connection)
                if connection is adapter:
                    raise OSError("synthetic commit acknowledgement loss")

            monkeypatch.setattr(type(adapter), "commit", lose_ack)
    async with pool.sessions() as session:
        assert await _pid(session) != observed_pids[0]
        assert await session.scalar(text("SELECT marker FROM capture_probe")) == "commit-unknown"
        await _assert_unlocked(session, pool.capture_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("capture_id", [None, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", True])
async def test_capture_requires_exact_uuid_before_checkout(capture_pool, capture_id):
    with pytest.raises(ValueError, match="registry_cms_capture_transaction_invalid"):
        async with custody.registry_cms_capture_transaction(capture_pool.sessions, capture_id=capture_id):
            pytest.fail("Invalid capture identity must not enter the transaction")
    assert capture_pool.engines[0].pool.checkedout() == 0


@pytest.mark.asyncio
async def test_capture_requires_exact_native_factory_and_one_connection_pool(capture_pool):
    pool = capture_pool

    class OtherSession(AsyncSession):
        pass

    factories = (
        lambda: pool.sessions(),
        async_sessionmaker(),
        async_sessionmaker(pool.engines[0], class_=OtherSession),
    )
    for factory in factories:
        with pytest.raises(ValueError, match="registry_cms_capture_transaction_invalid"):
            async with custody.registry_cms_capture_transaction(factory, capture_id=uuid4()):
                pytest.fail("Only the native publisher factory is accepted")
    engine = create_async_engine(pool.engines[0].url, pool_size=2, max_overflow=0)
    try:
        with pytest.raises(ValueError, match="registry_cms_capture_transaction_invalid"):
            async with custody.registry_cms_capture_transaction(async_sessionmaker(engine), capture_id=uuid4()):
                pytest.fail("A different pool contract must refuse")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_zero_uuid_refuses_before_any_native_connection():
    """Exercise the input boundary without creating a database or connecting."""
    engine = create_async_engine("postgresql+asyncpg://", pool_size=1, max_overflow=0)

    def refuse_connection(_dialect, _record, _connection_args, _connection_parameters):
        raise AssertionError("Zero UUID must not reach native connection acquisition")

    event.listen(engine.sync_engine, "do_connect", refuse_connection)
    try:
        with pytest.raises(ValueError, match="registry_cms_capture_transaction_invalid"):
            async with custody.registry_cms_capture_transaction(async_sessionmaker(engine), capture_id=UUID(int=0)):
                pytest.fail("Zero UUID must refuse before entering the capture transaction")
        assert engine.pool.checkedout() == 0
    finally:
        await engine.dispose()
