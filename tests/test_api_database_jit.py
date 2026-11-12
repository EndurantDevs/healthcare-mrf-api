# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""API lifecycle connection defaults stay separate from worker and CLI pools."""

import os
from contextlib import AsyncExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from db.connection import Database


def _listeners(database):
    callbacks_by_name = {}

    class App:
        def listener(self, name):
            def register(callback):
                callbacks_by_name[name] = callback
                return callback

            return register

        def middleware(self, _name):
            return lambda callback: callback

    database.init_app(App())
    return callbacks_by_name


@pytest.mark.asyncio
@pytest.mark.parametrize("driver", ["asyncpg", "postgresql+asyncpg", "psycopg", "postgresql+psycopg"])
async def test_api_startup_replaces_preconnected_pool_and_preserves_worker_defaults(monkeypatch, driver):
    engines, options = [], []

    def create_engine(_url, **kwargs):
        engine = SimpleNamespace(dispose=AsyncMock(), pool=SimpleNamespace(size=lambda: 0))
        engines.append(engine)
        options.append(kwargs)
        return engine

    monkeypatch.setenv("HLTHPRT_DB_DRIVER", driver)
    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "false")
    monkeypatch.setenv("PGOPTIONS", "-c work_mem=32MB -c jit=on")
    monkeypatch.setattr("db.connection.create_async_engine", create_engine)
    monkeypatch.setattr("db.connection.async_sessionmaker", lambda *_args, **_kwargs: object())
    database = Database()
    callbacks = _listeners(database)

    await database.connect()
    assert "connect_args" not in options[-1]
    await callbacks["before_server_start"](None, None)
    engines[0].dispose.assert_awaited_once()
    assert database.engine is engines[1]
    expected = (
        {"server_settings": {"jit": "off"}}
        if "asyncpg" in driver
        else {"options": "-c work_mem=32MB -c jit=on -c jit=off"}
    )
    assert options[-1]["connect_args"] == expected
    assert "isolation_level" not in options[-1]

    await database.disconnect()
    await database.connect()
    assert options[-1]["connect_args"] == expected
    await callbacks["before_server_stop"](None, None)
    await database.connect()
    assert "connect_args" not in options[-1]
    await database.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize("driver", ["asyncpg", "psycopg"])
async def test_reader_jit_and_read_only_defaults_remain_independent_of_api_startup(monkeypatch, driver):
    options_by_name = {}

    def create_engine(_url, **kwargs):
        options_by_name.update(kwargs)
        return SimpleNamespace(dispose=AsyncMock())

    monkeypatch.setenv("HLTHPRT_DB_DRIVER", driver)
    monkeypatch.setenv("PGOPTIONS", "-c work_mem=32MB")
    monkeypatch.setattr("db.connection.create_async_engine", create_engine)
    monkeypatch.setattr("db.connection.async_sessionmaker", lambda *_args, **_kwargs: object())
    reader = Database()
    reader._reader_login = ("reader", "password")
    await reader.connect()
    assert options_by_name["connect_args"] == (
        {"server_settings": {"jit": "off"}} if driver == "asyncpg" else {"options": "-c work_mem=32MB -c jit=off"}
    )
    assert options_by_name["isolation_level"] == "REPEATABLE READ"
    assert options_by_name["execution_options"] == {"postgresql_readonly": True}
    await reader.disconnect()


@pytest.mark.asyncio
async def test_api_startup_rebuilds_preconnected_primary_and_reader_pools(monkeypatch):
    engines = []

    def create_engine(_url, **kwargs):
        engine = SimpleNamespace(dispose=AsyncMock(), pool=SimpleNamespace(size=lambda: 0), options=kwargs)
        engines.append(engine)
        return engine

    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_USER", "writer")
    monkeypatch.setenv("HLTHPRT_DB_READER_USER", "reader")
    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "true")
    monkeypatch.setattr("db.connection.create_async_engine", create_engine)
    monkeypatch.setattr("db.connection.async_sessionmaker", lambda *_args, **_kwargs: object())
    database = Database()
    callbacks = _listeners(database)
    await database.connect()
    previous_reader = await database._connect_reader()
    await callbacks["before_server_start"](None, None)
    for engine in engines[:2]:
        engine.dispose.assert_awaited_once()
    assert database._reader_database is not previous_reader
    assert all(engine.options["connect_args"] == {"server_settings": {"jit": "off"}} for engine in engines[2:])
    await callbacks["before_server_stop"](None, None)


def _configure_native_database(monkeypatch):
    configured = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if configured:
        database_url = make_url(configured)
        assert database_url.host in {"127.0.0.1", "localhost", "postgres"} and database_url.database
        for name, value in {
            "HOST": database_url.host,
            "PORT": database_url.port or 5432,
            "USER": database_url.username or "postgres",
            "PASSWORD": database_url.password or "",
            "DATABASE": database_url.database,
        }.items():
            monkeypatch.setenv("HLTHPRT_DB_" + name, str(value))
        monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
    elif "test" not in os.getenv("HLTHPRT_DB_DATABASE", "").lower():
        pytest.skip("native API JIT proof requires an explicit disposable test database")
    assert os.getenv("HLTHPRT_DB_HOST", "127.0.0.1") in {"127.0.0.1", "localhost", "postgres"}


def test_native_api_jit_uses_the_registry_fixture_database(monkeypatch):
    from sqlalchemy.engine import URL

    database_url = URL.create(
        "postgresql+asyncpg", username="fixture", password="x", host="postgres", port=5440, database="registry_fixture"
    )
    monkeypatch.setenv("NETWORK_REGISTRY_TEST_DSN", database_url.render_as_string(hide_password=False))
    monkeypatch.setenv("HLTHPRT_DB_DATABASE_OVERRIDE", "unselected_test")
    _configure_native_database(monkeypatch)
    assert Database()._requested_database_name() == "registry_fixture"
    assert os.environ["HLTHPRT_DB_HOST"] == "postgres" and os.environ["HLTHPRT_DB_PORT"] == "5440"
    assert os.environ["HLTHPRT_DB_USER"] == "fixture" and os.environ["HLTHPRT_DB_PASSWORD"] == "x"


@pytest.mark.asyncio
async def test_native_api_jit_covers_pool_growth_transaction_end_and_reconnect(monkeypatch):
    _configure_native_database(monkeypatch)
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MIN_SIZE", "2")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "3")
    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "false")
    worker_database, api_database = Database(), Database()
    callbacks = _listeners(api_database)
    try:
        await worker_database.connect()
        inherited_jit = await worker_database.scalar(text("SELECT current_setting('jit')"))
        await api_database.connect()
        assert await api_database.scalar(text("SELECT current_setting('jit')")) == inherited_jit
        await callbacks["before_server_start"](None, None)
        async with AsyncExitStack() as stack:
            connections = [await stack.enter_async_context(api_database.engine.connect()) for _ in range(3)]
            connection_settings = [
                (await connection.execute(text("SELECT current_setting('jit'),pg_backend_pid()"))).one()
                for connection in connections
            ]
            assert (
                all(connection_setting[0] == "off" for connection_setting in connection_settings)
                and len({connection_setting[1] for connection_setting in connection_settings}) == 3
            )
            await connections[0].commit()
            assert await connections[0].scalar(text("SELECT current_setting('jit')")) == "off"
            await connections[0].rollback()
            assert await connections[0].scalar(text("SELECT current_setting('jit')")) == "off"
        async with api_database.engine.connect() as connection:
            previous_pid = await connection.scalar(text("SELECT pg_backend_pid()"))
            await connection.rollback()
            await connection.invalidate()
            reconnected_settings = (
                await connection.execute(text("SELECT current_setting('jit'),pg_backend_pid()"))
            ).one()
            assert reconnected_settings[0] == "off" and reconnected_settings[1] != previous_pid
        await api_database.disconnect()
        await api_database.connect()
        assert await api_database.scalar(text("SELECT current_setting('jit')")) == "off"
        assert await worker_database.scalar(text("SELECT current_setting('jit')")) == inherited_jit
        await callbacks["before_server_stop"](None, None)
        await api_database.connect()
        assert await api_database.scalar(text("SELECT current_setting('jit')")) == inherited_jit
    finally:
        await api_database.disconnect()
        await worker_database.disconnect()
