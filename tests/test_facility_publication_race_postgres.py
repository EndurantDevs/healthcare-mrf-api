# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected adoption cannot interleave with ordinary facility publication."""

from __future__ import annotations

import asyncio
import importlib
import os
import re
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.models import db
from process.facility_address_contribution_capture import require_local_family_publication

anchors = importlib.import_module("process.facility_anchors")


def _dsn() -> str:
    raw = os.getenv("HLTHPRT_FACILITY_PUBLICATION_TEST_DSN", "")
    if not raw:
        pytest.skip("HLTHPRT_FACILITY_PUBLICATION_TEST_DSN is not set")
    url = make_url(raw)
    if (
        (url.drivername, url.username, url.host) != ("postgresql", "postgres", "127.0.0.1")
        or url.port not in {5432, 5440}
    ):
        pytest.fail("facility publication test requires a dedicated local PostgreSQL database")
    if not re.fullmatch(r"hc_facility_publication_[0-9a-f]{32}", url.database or ""):
        pytest.fail("facility publication test requires its dedicated local database")
    return url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


async def _seed_family(connection, schema: str) -> None:
    await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    await connection.execute(text(f'CREATE TABLE "{schema}".facility_anchor (id integer PRIMARY KEY)'))
    await connection.execute(text(f'CREATE TABLE "{schema}".facility_address_contribution (id integer)'))
    await connection.execute(
        text(f'CREATE TABLE "{schema}".reference_family_result_generation (importer_id text PRIMARY KEY)')
    )
    await connection.execute(
        text(f"INSERT INTO \"{schema}\".reference_family_result_generation VALUES ('facility-anchors')")
    )
    await connection.execute(text(f'CREATE TABLE "{schema}".address_archive (id integer, value text)'))
    await connection.execute(text(f"INSERT INTO \"{schema}\".address_archive VALUES (1,'before')"))


async def _seed_worker_stage(connection, schema: str) -> None:
    await connection.execute(
        text(
            f'CREATE TABLE "{schema}".facility_anchor_20260923 '
            "(facility_type text,latitude numeric,longitude numeric,address_line1 text,city text,state text,zip_code text)"
        )
    )
    await connection.execute(
        text(f"INSERT INTO \"{schema}\".facility_anchor_20260923 VALUES ('Hospital',1,1,'One Way','City','NY','10001')")
    )
    await connection.execute(text(f'CREATE TABLE "{schema}".facility_address_contribution_20260923 (kind text)'))


@pytest.mark.asyncio
async def test_adoption_waits_and_failed_swap_rolls_back_shared_addresses():
    """A concurrent adopter waits while failed ordinary DDL rolls back addresses."""
    engine = create_async_engine(_dsn())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    schema = f"facility_race_{uuid4().hex[:12]}"
    entered, attempting, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    try:
        async with engine.begin() as connection:
            await _seed_family(connection, schema)

        async def ordinary_publication():
            async with sessions.begin() as session:
                await require_local_family_publication(session, schema=schema)
                await session.execute(text(f"UPDATE \"{schema}\".address_archive SET value='after' WHERE id=1"))
                entered.set()
                await release.wait()
                # Force the final family DDL to fail after shared-address writes.
                await session.execute(
                    text(f'ALTER TABLE "{schema}".facility_anchor RENAME TO facility_address_contribution')
                )

        async def protected_adoption():
            async with sessions.begin() as session:
                attempting.set()
                await session.execute(text(f'ALTER TABLE "{schema}".facility_anchor RENAME TO facility_anchor_adopted'))

        ordinary = asyncio.create_task(ordinary_publication())
        await asyncio.wait_for(entered.wait(), 5)
        adoption = asyncio.create_task(protected_adoption())
        try:
            await asyncio.wait_for(attempting.wait(), 5)
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(adoption), 0.2)
        finally:
            release.set()
        with pytest.raises(DBAPIError, match="already exists"):
            await ordinary
        await asyncio.wait_for(adoption, 5)
        async with sessions() as session:
            assert await session.scalar(text(f'SELECT value FROM "{schema}".address_archive WHERE id=1')) == "before"
            assert await session.scalar(
                text("SELECT to_regclass(:relation) IS NOT NULL"),
                {"relation": f'"{schema}".facility_anchor_adopted'},
            )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_two_ordinary_publishers_do_not_deadlock_on_lock_upgrade():
    """A second publisher waits before taking a conflicting family lock."""
    engine = create_async_engine(_dsn())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    schema = f"facility_publishers_{uuid4().hex[:12]}"
    entered, attempting, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    try:
        async with engine.begin() as connection:
            await _seed_family(connection, schema)

        async def first_publisher():
            async with sessions.begin() as session:
                await require_local_family_publication(session, schema=schema)
                entered.set()
                await release.wait()
                await session.execute(text(f'LOCK TABLE "{schema}".facility_anchor IN ACCESS EXCLUSIVE MODE'))

        async def second_publisher():
            async with sessions.begin() as session:
                attempting.set()
                await require_local_family_publication(session, schema=schema)

        first = asyncio.create_task(first_publisher())
        await asyncio.wait_for(entered.wait(), 5)
        second = asyncio.create_task(second_publisher())
        try:
            await asyncio.wait_for(attempting.wait(), 5)
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(second), 0.2)
        finally:
            release.set()
        await asyncio.wait_for(first, 5)
        await asyncio.wait_for(second, 5)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_worker_swap_failure_rolls_back_resolver_and_geocode(monkeypatch):
    """The actual finalizer binds both address effects and family DDL to one transaction."""
    database_name = make_url(_dsn()).database
    schema = f"facility_worker_{uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", database_name)
    monkeypatch.setenv("HLTHPRT_DB_HOST", "127.0.0.1")
    monkeypatch.setenv("HLTHPRT_DB_PORT", "5440")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setattr(anchors, "DEFAULT_MIN_ROWS", 0)
    monkeypatch.setattr(anchors, "DEFAULT_MIN_HOSPITAL_COORD_COVERAGE", 0.0)
    monkeypatch.setattr(anchors, "source_enabled", lambda importer_id: True)

    async def no_backfill(*args):
        return 0

    async def stage_write(*args, **kwargs):
        # Real stage helpers use child tasks; they must finish before the publication bind.
        await asyncio.create_task(db.status(f'UPDATE "{schema}".facility_anchor_20260923 SET zip_code=zip_code'))
        return 0

    async def resolve(*args, **kwargs):
        assert kwargs["zip_restore_complete"] is True
        async with db.transaction():
            await db.status(f"UPDATE \"{schema}\".address_archive SET value='resolved' WHERE id=1")

    async def geocode(*args, **kwargs):
        async with db.transaction():
            await db.status(f"UPDATE \"{schema}\".address_archive SET value='geocoded' WHERE id=1")
        return 1

    monkeypatch.setattr(anchors, "_backfill_hospital_coordinates_from_existing_live", no_backfill)
    monkeypatch.setattr(anchors, "stamp_address_keys", stage_write)
    monkeypatch.setattr(anchors, "restore_missing_zip_from_tiger_zcta", stage_write)
    monkeypatch.setattr(anchors, "resolve_into_archive", resolve)
    monkeypatch.setattr(anchors, "_refresh_archive_geocodes_from_facility_anchors", geocode)
    try:
        await db.connect()
        async with db.transaction() as session:
            await _seed_family(session, schema)
            await _seed_worker_stage(session, schema)
        before_oid = await db.scalar(f"SELECT to_regclass('{schema}.facility_anchor')::oid::bigint")
        with pytest.raises(DBAPIError, match="does not exist"):
            await anchors.publish_facility_anchors_generation({"import_date": "20260923", "context": {"run": 1}})
        assert await db.scalar(f'SELECT value FROM "{schema}".address_archive WHERE id=1') == "before"
        assert await db.scalar(f"SELECT to_regclass('{schema}.facility_anchor')::oid::bigint") == before_oid
        assert await db.scalar(f"SELECT to_regclass('{schema}.facility_anchor_20260923') IS NOT NULL") is True
        assert await db.scalar(f'SELECT count(*) FROM "{schema}".reference_family_result_generation') == 1
    finally:
        await db.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await db.disconnect()
