# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL concurrency proof for hospital-price publication."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import uuid

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from db.connection import Database
from process import hospital_price_attempt_store, hospital_price_store
from process.hospital_price_acquisition import Attempt, Candidate
from tests.test_hospital_price_storage import (
    _database_url,
    _drop_schema,
    _load_migration,
    _prepare_schema,
    _quote,
    _run_migration,
    _seed_attempt,
    _seed_version,
)


class _SignalingConnection:
    def __init__(self, connection, admission_started: asyncio.Event) -> None:
        self.connection = connection
        self.raw_connection = connection.raw_connection
        self.admission_started = admission_started

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def all(self, statement: str, **values):
        self.admission_started.set()
        return await self.connection.all(statement, **values)


class _SignalingDatabase:
    def __init__(self, database, admission_started: asyncio.Event) -> None:
        self.database = database
        self.admission_started = admission_started

    @asynccontextmanager
    async def acquire(self):
        async with self.database.acquire() as connection:
            yield _SignalingConnection(connection, self.admission_started)


async def _seed_expired_attempt(database_url, schema: str, quoted: str):
    connection = await asyncpg.connect(
        str(database_url.set(drivername="postgresql"))
    )
    try:
        content_sha, version_id, _quoted = await _seed_version(connection, schema)
        await _seed_attempt(connection, quoted)
        await connection.execute(
            f"UPDATE {quoted}.hospital_price_import_attempt "
            "SET started_at=clock_timestamp() - interval '3 seconds', "
            "heartbeat_at=clock_timestamp() - interval '2 seconds', "
            "lease_expires_at=clock_timestamp() - interval '1 second' "
            "WHERE attempt_id='attempt-a'"
        )
        return content_sha, version_id
    finally:
        await connection.close()


async def _start_overlap(
    monkeypatch, database, schema: str, version_id: str, content_sha: str,
    resume_publication: asyncio.Event,
):
    admission_started, cas_started = asyncio.Event(), asyncio.Event()
    monkeypatch.setattr(hospital_price_store, "db", database)
    monkeypatch.setattr(hospital_price_store, "schema_name", lambda: schema)
    monkeypatch.setattr(hospital_price_attempt_store, "schema_name", lambda: schema)
    monkeypatch.setattr(
        hospital_price_attempt_store, "db",
        _SignalingDatabase(database, admission_started),
    )
    original_cas_publish = hospital_price_store._cas_publish

    async def paused_cas(*args, **kwargs):
        cas_started.set()
        await resume_publication.wait()
        return await original_cas_publish(*args, **kwargs)

    async def publish():
        attempt = Attempt(
            "attempt-a", "hospital-a", "Hospital A",
            "https://hospital.example/prices.json", 0, "Hospital A",
        )
        async with database.acquire() as connection:
            return await hospital_price_store._bind_and_publish(
                connection, version_id, content_sha, (attempt,),
                ((0, "Hospital A"), (1, "Hospital B")),
            )

    monkeypatch.setattr(hospital_price_store, "_cas_publish", paused_cas)
    publication = asyncio.create_task(publish())
    await asyncio.wait_for(cas_started.wait(), timeout=2)
    candidate = Candidate(
        "hospital-a", "Hospital A", "locator-1", "observation-1",
        "https://hospital.example/new.json", "Hospital A",
    )
    admission = asyncio.create_task(hospital_price_attempt_store.admit_attempts(
        (candidate,), lease_owner="hospital-prices:replacement", lease_seconds=30,
    ))
    await asyncio.wait_for(admission_started.wait(), timeout=2)
    return publication, admission


@pytest.mark.asyncio
async def test_expired_admission_overlaps_publication_without_deadlock(
    monkeypatch,
) -> None:
    """Current-row ordering prevents the admission/publication ABBA cycle."""

    database_url = _database_url()
    schema = f"hospital_price_test_{uuid.uuid4().hex}"
    quoted = _quote(schema)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg"), poolclass=NullPool
    )
    database = Database(engine=engine)
    resume_publication = asyncio.Event()
    publication = admission = None
    await _prepare_schema(engine, schema)
    try:
        await _run_migration(engine, _load_migration(), "upgrade")
        content_sha, version_id = await _seed_expired_attempt(
            database_url, schema, quoted
        )
        publication, admission = await _start_overlap(
            monkeypatch, database, schema, version_id, content_sha,
            resume_publication,
        )
        await asyncio.sleep(0.05)
        resume_publication.set()
        published, admitted = await asyncio.wait_for(
            asyncio.gather(publication, admission), timeout=5
        )
        assert published == (1, 0, 0)
        assert len(admitted) == 1 and tuple(admitted[0])[:1] == ("hospital-a",)
    finally:
        resume_publication.set()
        tasks = tuple(task for task in (publication, admission) if task is not None)
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await _drop_schema(engine, schema)
        await engine.dispose()
