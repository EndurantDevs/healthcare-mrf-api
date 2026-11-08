# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real WAL boundaries and rollback on a UUID-owned local database, without a full import."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from db.connection import Database
from process import provider_directory_cms_nonprofile_capacity as capacity
from tests.cms_npd_admission_postgres_support import _database_url
from tests.test_provider_directory_cms_nonprofile_capacity import _producer, _signed_plan
from tests.test_provider_directory_cms_wal_budget import _check


@asynccontextmanager
async def _native_database(monkeypatch):
    """Reuse the repository's localhost/UUID safety gate and clean only this schema."""
    url = _database_url()
    schema = "cms_wal_test_" + uuid4().hex
    for key, value in {
        "DRIVER": "asyncpg",
        "HOST": url.host,
        "PORT": str(url.port),
        "USER": url.username,
        "PASSWORD": url.password or "",
        "DATABASE": url.database,
        "SCHEMA": schema,
    }.items():
        monkeypatch.setenv("HLTHPRT_DB_" + key, value)
    database = Database()
    await database.connect()
    try:
        await database.status(f"CREATE SCHEMA {schema}")
        await database.status(f"CREATE TABLE {schema}.wal_events (phase text, payload text)")
        yield database, schema
    finally:
        try:
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()


async def _start_profile_interval(database, schema, producer, monkeypatch, preparation_budget):
    """Pause the real admission meter and independently verify the resumed Profile interval."""
    admission_start = await database.scalar("SELECT pg_current_wal_insert_lsn()::text")
    await database.status(f"INSERT INTO {schema}.wal_events VALUES ('profile-admission','consumed')")
    original_profile = SimpleNamespace(
        run_id=producer.run_id,
        lease=producer.profile_lease,
        initial_wal_offset_bytes=0,
        wal_tracker=SimpleNamespace(lock=asyncio.Lock()),
        geometry=SimpleNamespace(reservation_bytes_by_storage_class={"wal": 64 * 1024**2}),
    )

    async def admission_wal(_profile):
        return await database.scalar(
            "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),CAST(CAST(:start AS text) AS pg_lsn))::bigint",
            start=admission_start,
        )

    producer.fhir._provider_directory_profile_current_wal_bytes = admission_wal
    paused = await producer.pause_profile(original_profile)
    assert paused.spent_wal_bytes > 0
    profile_start = await database.scalar("SELECT pg_current_wal_insert_lsn()::text")
    profile = SimpleNamespace(initial_wal_lsn=profile_start)
    monkeypatch.setattr(capacity, "resume_profile_capacity", AsyncMock(return_value=profile))
    await producer.resume_profile(paused, "resource-fence", frozenset({"Practitioner"}))
    preparation_wal = producer.preparation_wal_end_bytes
    assert preparation_wal == await database.scalar(
        "SELECT pg_wal_lsn_diff(CAST(CAST(:end AS text) AS pg_lsn),CAST(CAST(:start AS text) AS pg_lsn))::bigint",
        end=profile_start,
        start=producer.initial_wal_lsn,
    )
    assert 0 < preparation_wal < preparation_budget
    producer.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(profile)
    producer._assert_active_profile = AsyncMock()

    async def check_profile_budget(admission):
        assert admission is profile
        observed = await database.scalar(
            "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),CAST(CAST(:start AS text) AS pg_lsn))::bigint",
            start=profile_start,
        )
        assert 0 <= observed <= 64 * 1024**2

    producer.fhir._assert_provider_directory_profile_wal_budget = check_profile_budget
    return preparation_wal


@pytest.mark.asyncio
@pytest.mark.parametrize("cutover_budget", [1024, 64 * 1024**2])
async def test_real_admission_wal_profile_boundary_and_cutover_rollback(monkeypatch, cutover_budget):
    """Actual PostgreSQL writes spend each CMS phase; only the verified Profile interval is excluded."""
    async with _native_database(monkeypatch) as (database, schema):
        producer = _producer()
        preparation_budget = 64 * 1024**2
        producer.plan = replace(
            producer.plan,
            logging_wal_upper_bound_bytes=preparation_budget,
            cutover_wal_upper_bound_bytes=cutover_budget,
            reservation_bytes=(("data", 100_000), ("temp", 100_000), ("wal", preparation_budget + cutover_budget)),
        )
        producer.lease = _signed_plan(producer.plan)
        producer.fhir.db = database
        producer.initial_wal_lsn = await database.scalar("SELECT pg_current_wal_insert_lsn()::text")
        await database.status(f"INSERT INTO {schema}.wal_events VALUES ('admission','consumed')")
        assert await producer._current_wal_bytes() > 0
        await producer._assert_wal_budget(_check(producer, "pre_scratch"))
        await database.status(f"CREATE UNLOGGED TABLE {schema}.scratch (value int)")
        await database.status(f"INSERT INTO {schema}.scratch SELECT generate_series(1,100)")
        await producer._assert_wal_budget(_check(producer, "readiness"))

        preparation_wal = await _start_profile_interval(database, schema, producer, monkeypatch, preparation_budget)
        await database.status(
            f"INSERT INTO {schema}.wal_events SELECT 'profile',repeat(md5(value::text),16) "
            "FROM generate_series(1,1000) value"
        )
        assert await producer._current_wal_bytes() > preparation_wal
        await producer._assert_wal_budget(_check(producer, "readiness"))
        assert producer.preparation_wal_end_bytes == preparation_wal

        async def publish():
            async with database.transaction():
                await producer._assert_wal_budget(_check(producer, "cutover"))
                await database.status(
                    f"INSERT INTO {schema}.wal_events SELECT 'cutover',repeat(md5(value::text),16) "
                    "FROM generate_series(1,1000) value"
                )
                await producer._assert_wal_budget(_check(producer, "cutover"))

        if cutover_budget == 1024:
            with pytest.raises(RuntimeError, match="cutover_wal_budget_exceeded"):
                await publish()
            expected_cutover_rows = 0
        else:
            await publish()
            expected_cutover_rows = 1000
        assert (
            await database.scalar(f"SELECT count(*) FROM {schema}.wal_events WHERE phase='cutover'")
            == expected_cutover_rows
        )
        assert await database.scalar(f"SELECT count(*) FROM {schema}.wal_events WHERE phase='profile'") == 1000
        assert producer.preparation_wal_end_bytes == preparation_wal
        assert await producer._current_wal_bytes() > producer.cutover_wal_start_bytes
