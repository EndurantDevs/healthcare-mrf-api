# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native statement scope for the provider-only header join planner setting."""

import asyncio
import os
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from process import network_provider_only_projection as projection


@pytest.fixture
async def planner_connection():
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    url = make_url(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    admin = await asyncpg.connect(url.render_as_string(hide_password=False))
    role = "provider_plan_" + uuid4().hex
    password = uuid4().hex
    connection, is_role_created = None, False
    database = (await admin.fetchval("SELECT current_database()")).replace('"', '""')
    try:
        await admin.execute(f"CREATE ROLE \"{role}\" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{password}'")
        is_role_created = True
        await admin.execute(f'GRANT TEMP ON DATABASE "{database}" TO "{role}"')
        connection = await asyncpg.connect(
            url.set(username=role, password=password).render_as_string(hide_password=False),
            server_settings={"work_mem": "16MB", "statement_timeout": "120s"},
        )
        assert await connection.fetchval("SHOW is_superuser") == "off"
        await projection._create_evidence_heap(connection, "planner_output", temporary=True)
        yield connection, admin
    finally:
        if connection is not None:
            await connection.close()
        if is_role_created:
            await admin.execute(f'REVOKE TEMP ON DATABASE "{database}" FROM "{role}"')
            await admin.execute(f'DROP ROLE "{role}"')
        await admin.close()


def _statement(monkeypatch, *, sleep=False, error=False):
    selected = (
        "SELECT 'npi'::text provider_system,current_setting('enable_nestloop') provider_id,"
        + ("1/0" if error else "1")
        + " AS network_id,repeat('a',64) evidence_id"
        + (" FROM pg_sleep(30)" if sleep else "")
    )
    monkeypatch.setattr(projection, "_source_query", lambda *args, **kwargs: selected)
    monkeypatch.setattr(projection, "_source_arguments", lambda *args, **kwargs: ())


async def _append(connection, version=2):
    await projection._append(connection, "planner_output", "facts", None, None, None, descriptor_version=version)


@pytest.mark.asyncio
@pytest.mark.parametrize("original", ["on", "off"])
async def test_provider_only_plan_restores_success_and_outer_savepoint(planner_connection, monkeypatch, original):
    connection, _admin = planner_connection
    _statement(monkeypatch)
    await connection.execute("SELECT set_config('enable_nestloop',$1,false)", original)
    async with connection.transaction():
        savepoint = connection.transaction()
        await savepoint.start()
        nested = "off" if original == "on" else "on"
        await connection.execute("SELECT set_config('enable_nestloop',$1,true)", nested)
        await _append(connection)
        assert await connection.fetchval("SELECT provider_id FROM planner_output") == "off"
        assert await connection.fetchval("SHOW enable_nestloop") == nested
        await savepoint.rollback()
        assert await connection.fetchval("SELECT count(*) FROM planner_output") == 0
        assert await connection.fetchval("SHOW enable_nestloop") == original
    assert await connection.fetchval("SHOW enable_nestloop") == original


@pytest.mark.asyncio
@pytest.mark.parametrize("original", ["on", "off"])
async def test_provider_only_plan_restores_database_error(planner_connection, monkeypatch, original):
    connection, _admin = planner_connection
    _statement(monkeypatch, error=True)
    await connection.execute("SELECT set_config('enable_nestloop',$1,false)", original)
    async with connection.transaction():
        with pytest.raises(asyncpg.DivisionByZeroError):
            await _append(connection)
        assert await connection.fetchval("SHOW enable_nestloop") == original
        assert await connection.fetchval("SELECT count(*) FROM planner_output") == 0
    assert await connection.fetchval("SHOW enable_nestloop") == original


async def _wait_for_sleep(admin, pid):
    while not await admin.fetchval("SELECT wait_event='PgSleep' FROM pg_stat_activity WHERE pid=$1", pid):
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
@pytest.mark.parametrize("original", ["on", "off"])
async def test_provider_only_plan_restores_real_query_cancellation(planner_connection, monkeypatch, original):
    connection, admin = planner_connection
    _statement(monkeypatch, sleep=True)
    await connection.execute("SELECT set_config('enable_nestloop',$1,false)", original)
    async with connection.transaction():
        task = asyncio.create_task(_append(connection))
        try:
            await asyncio.wait_for(_wait_for_sleep(admin, connection.get_server_pid()), 5)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert await connection.fetchval("SHOW enable_nestloop") == original
        assert await connection.fetchval("SELECT count(*) FROM planner_output") == 0
    assert await connection.fetchval("SHOW enable_nestloop") == original


@pytest.mark.asyncio
async def test_legacy_selector_keeps_existing_planner_setting(planner_connection, monkeypatch):
    connection, _admin = planner_connection
    _statement(monkeypatch)
    await connection.execute("SET enable_nestloop=on")
    async with connection.transaction():
        await _append(connection, version=None)
        assert await connection.fetchval("SELECT provider_id FROM planner_output") == "on"
        assert await connection.fetchval("SHOW enable_nestloop") == "on"
