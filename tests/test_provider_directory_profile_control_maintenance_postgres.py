# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native fence primitives; these cases do not certify a signed worker run."""

from __future__ import annotations

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, PendingRollbackError

from process import provider_directory_profile as profile
from process import provider_directory_profile_control_maintenance as maintenance
from tests.provider_directory_profile_artifact_pg_fixtures import (
    _POSTGRES_DSN_ENV,
    _configure_database,
    _require_postgresql_18,
)
from tests.provider_directory_profile_delta_test_support import _delta_database


@dataclass(frozen=True)
class _NativeFixture:
    engine: object
    refs: tuple[str, ...]
    oids: tuple[int, ...]
    database_oid: int
    outside_ref: str
    outside_oid: int
    advisory_key: int


@pytest.fixture
async def native_fence(monkeypatch):
    dsn = os.getenv(_POSTGRES_DSN_ENV, "")
    if not dsn:
        pytest.skip("Maintenance primitives require disposable PostgreSQL 18")
    _configure_database(monkeypatch, dsn)
    async with _delta_database(monkeypatch) as (database, schema):
        await _require_postgresql_18(database)
        refs = tuple(f'{profile.quote_identifier(schema)}."control_{index}"' for index in range(5))
        outside_ref = f'{profile.quote_identifier(schema)}."unrelated"'
        for ref in (*refs, outside_ref):
            await database.status(f"CREATE TABLE {ref} (id integer PRIMARY KEY, value integer NOT NULL)")
        oid_values = [int(await database.scalar("SELECT to_regclass(:ref)::oid", ref=ref)) for ref in refs]
        oids = tuple(oid_values)
        outside_oid = int(await database.scalar("SELECT to_regclass(:ref)::oid", ref=outside_ref))
        database_oid = int(await database.scalar("SELECT oid FROM pg_database WHERE datname = current_database()"))
        yield _NativeFixture(
            database.engine,
            refs,
            oids,
            database_oid,
            outside_ref,
            outside_oid,
            int(uuid.uuid4().hex[:15], 16),
        )


async def _lock(connection, refs, mode="SHARE UPDATE EXCLUSIVE"):
    await connection.execute(text("LOCK TABLE " + ", ".join(refs) + " IN " + mode + " MODE NOWAIT"))


async def _native_lock_count(connection, fixture, pid):
    return int(
        (
            await connection.execute(
                text(
                    "SELECT count(DISTINCT relation) FROM pg_locks WHERE pid = :pid "
                    "AND database = CAST(:database_oid AS oid) AND locktype = 'relation' "
                    "AND mode = 'ShareUpdateExclusiveLock' AND granted "
                    "AND relation = ANY(CAST(:oids AS oid[]))"
                ),
                {"pid": pid, "database_oid": fixture.database_oid, "oids": list(fixture.oids)},
            )
        ).scalar_one()
    )


@asynccontextmanager
async def _held(fixture):
    async with fixture.engine.connect() as connection:
        await connection.execute(text("SELECT pg_advisory_lock(:key)"), {"key": fixture.advisory_key})
        pid = int((await connection.execute(text("SELECT pg_backend_pid()"))).scalar_one())
        await connection.commit()
        slots = fixture.engine.pool.checkedout()
        try:
            await connection.begin()
            await _lock(connection, fixture.refs)
            assert fixture.engine.pool.checkedout() == slots
            assert int((await connection.execute(text("SELECT pg_backend_pid()"))).scalar_one()) == pid
            yield connection, pid
        finally:
            lost = connection.invalidated
            if connection.in_transaction():
                await connection.rollback()
            if not lost:
                assert (
                    await connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": fixture.advisory_key})
                ).scalar_one()
                await connection.commit()


def _liveness_lease(connection, fixture):
    # Only the production native liveness query consumes these coordinates.
    # No signed admission, scope authorization or capacity receipt is synthesized.
    deadline = asyncio.get_running_loop().time() + 5

    async def remaining(_):
        return max(1, int((deadline - asyncio.get_running_loop().time()) * 1000))

    fhir = SimpleNamespace(_profile_capacity_remaining_ms=remaining)
    admission = SimpleNamespace(geometry=SimpleNamespace(database_oid=fixture.database_oid))
    lease = maintenance._Lease(
        fhir,
        connection,
        admission,
        asyncio.current_task(),
        has_acquired=True,
        coordinates=tuple(zip(fixture.oids, fixture.refs, strict=True)),
    )
    return lease


async def test_existing_advisory_connection_allows_row_exclusive_dml(native_fence):
    async with _held(native_fence) as (connection, pid):
        assert await _native_lock_count(connection, native_fence, pid) == 5
        async with native_fence.engine.begin() as worker:
            assert native_fence.engine.pool.checkedout() == 2
            await worker.execute(text("SET LOCAL lock_timeout = '250ms'"))
            for ref in native_fence.refs:
                await worker.execute(text(f"INSERT INTO {ref} VALUES (1, 2)"))
                await worker.execute(text(f"UPDATE {ref} SET value = 3 WHERE id = 1"))
                await worker.execute(text(f"DELETE FROM {ref} WHERE id = 1"))
        assert await _native_lock_count(connection, native_fence, pid) == 5
    assert native_fence.engine.pool.checkedout() == 0


async def test_conflicting_maintenance_lock_refuses_nowait(native_fence):
    async with _held(native_fence):
        async with native_fence.engine.connect() as contender:
            with pytest.raises(DBAPIError) as refused:
                await _lock(contender, native_fence.refs)
            assert refused.value.orig.sqlstate == "55P03"
            await contender.rollback()


@pytest.mark.parametrize("operation", ["vacuum", "analyze", "ddl"])
async def test_native_maintenance_and_ddl_obey_bounded_timeout(native_fence, operation):
    ref = native_fence.refs[0]
    statement = {
        "vacuum": f"VACUUM {ref}",
        "analyze": f"ANALYZE {ref}",
        "ddl": f"ALTER TABLE {ref} ADD COLUMN extra integer",
    }[operation]
    async with _held(native_fence), native_fence.engine.connect() as raw:
        contender = await raw.execution_options(isolation_level="AUTOCOMMIT")
        try:
            await contender.execute(text("SET lock_timeout = '100ms'"))
            await contender.execute(text("SET statement_timeout = '1000ms'"))
            with pytest.raises(DBAPIError) as refused:
                async with asyncio.timeout(2):
                    await contender.execute(text(statement))
            assert refused.value.orig.sqlstate == "55P03"
        finally:
            await contender.rollback()
            await contender.execute(text("RESET lock_timeout"))
            await contender.execute(text("RESET statement_timeout"))
    async with native_fence.engine.connect() as raw:
        contender = await raw.execution_options(isolation_level="AUTOCOMMIT")
        await contender.execute(text(statement))


async def test_commit_releases_fence_before_stronger_cutover_keeps_advisory(native_fence):
    async with _held(native_fence) as (connection, pid):
        async with native_fence.engine.connect() as contender:
            with pytest.raises(DBAPIError) as refused:
                await _lock(contender, native_fence.refs, "ACCESS EXCLUSIVE")
            assert refused.value.orig.sqlstate == "55P03"
            await contender.rollback()
            await connection.commit()
            await _lock(contender, native_fence.refs, "ACCESS EXCLUSIVE")
            assert await _native_lock_count(contender, native_fence, pid) == 0
            assert not (
                await contender.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": native_fence.advisory_key})
            ).scalar_one()
            await contender.rollback()


@pytest.mark.parametrize("changed", ["oid", "transaction", "connection"])
async def test_production_liveness_refuses_misbound_or_lost_native_lease(native_fence, changed):
    async with _held(native_fence) as (connection, _):
        lease = _liveness_lease(connection, native_fence)
        token = maintenance._ACTIVE.set(lease)
        try:
            await maintenance.assert_held(lease.fhir)
            if changed == "oid":
                lease.coordinates = (*lease.coordinates[:4], (native_fence.outside_oid, native_fence.outside_ref))
            elif changed == "transaction":
                await connection.commit()
            else:
                await connection.invalidate()
            expected = PendingRollbackError if changed == "connection" else RuntimeError
            with pytest.raises(expected):
                await maintenance.assert_held(lease.fhir)
        finally:
            maintenance._ACTIVE.reset(token)
    async with native_fence.engine.begin() as contender:
        await _lock(contender, native_fence.refs)
        acquired = (
            await contender.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": native_fence.advisory_key})
        ).scalar_one()
        assert acquired
        assert (
            await contender.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": native_fence.advisory_key})
        ).scalar_one()


async def test_cancel_drains_production_rollback_before_advisory_unlock(native_fence):
    ready = asyncio.Event()

    async def worker():
        async with _held(native_fence) as (connection, _):
            lease = _liveness_lease(connection, native_fence)
            ready.set()
            try:
                await asyncio.Event().wait()
            finally:
                await maintenance._rollback_drained(lease)
                assert lease.released

    task = asyncio.create_task(worker())
    try:
        async with asyncio.timeout(5):
            await ready.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert native_fence.engine.pool.checkedout() == 0
    async with native_fence.engine.begin() as contender:
        await _lock(contender, native_fence.refs)
        assert (
            await contender.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": native_fence.advisory_key})
        ).scalar_one()
        await contender.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": native_fence.advisory_key})


async def test_global_wal_observation_still_includes_unrelated_logged_write(native_fence):
    async with _held(native_fence) as (connection, pid):
        before = str((await connection.execute(text("SELECT pg_current_wal_insert_lsn()::text"))).scalar_one())
        async with native_fence.engine.begin() as unrelated:
            await unrelated.execute(text(f"INSERT INTO {native_fence.outside_ref} VALUES (1, 1)"))
        observed = (
            await connection.execute(
                text("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), CAST(CAST(:before AS text) AS pg_lsn))"),
                {"before": before},
            )
        ).scalar_one()
        assert observed > 0
        assert await _native_lock_count(connection, native_fence, pid) == 5
        for ref in native_fence.refs:
            assert (await connection.execute(text(f"SELECT count(*) FROM {ref}"))).scalar_one() == 0
