# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent cleanup preserves exact attempted resources and native failures."""

import asyncio
from functools import partial
from types import SimpleNamespace

import pytest
from sqlalchemy.engine import make_url

from tests import test_entity_address_alias_guard_postgres as fixture


async def _execute_owned_statement(state, failure, error, statement):
    if statement.startswith("CREATE ROLE"):
        name = statement.split('"')[1]
        state.attempted_roles.append(name)
        if failure == "create_role" and len(state.attempted_roles) == 2:
            raise error
        state.roles.add(name)
        return
    if statement.startswith("CREATE DATABASE"):
        state.database = statement.split('"')[1]
        if failure == "create_database":
            raise error
        return
    if statement.startswith("DROP DATABASE"):
        assert statement == f'DROP DATABASE IF EXISTS "{state.database}"'
        state.events.append("database.drop")
        if failure == "database_drop":
            raise error
        state.is_database_dropped = True
        return
    if statement.startswith("REVOKE SET"):
        name = statement.split('"')[1]
        assert name in state.attempted_roles
        state.events.append("role.revoke:" + name)
        if failure == "parameter_revoke" and name == state.attempted_roles[-1]:
            raise error
        return
    if statement.startswith("DROP ROLE"):
        name = statement.split('"')[1]
        assert name in state.attempted_roles
        state.events.append("role.drop:" + name)
        if failure == "role_drop" and name == state.attempted_roles[-1]:
            raise error
        state.roles.discard(name)


def _fake_resources(monkeypatch, failure, error):
    state = SimpleNamespace(
        events=[], attempted_roles=[], roles=set(), connections=[], engines=[], database=None, is_database_dropped=False
    )

    async def fetchval(query, *arguments):
        if "pg_database" in query:
            return state.database is not None and (not state.is_database_dropped or failure == "database_assertion")
        if "ANY" in query:
            return bool(state.roles.intersection(arguments[0]))
        return arguments[0] in state.roles

    async def connect(_url, **_kwargs):
        index = len(state.connections)

        async def close():
            state.events.append("admin.close" if index == 0 else "target.close")
            if failure == "target_close" and index == 1:
                raise error

        connection = SimpleNamespace(
            execute=partial(_execute_owned_statement, state, failure, error), fetchval=fetchval, close=close
        )
        state.connections.append(connection)
        return connection

    def create_engine(_url, **_kwargs):
        index = len(state.engines)

        async def dispose():
            state.events.append(f"database.{index}.disconnect")
            if failure in {"disconnect_cancel", "disconnect_error"} and index == 1:
                raise error

        engine = SimpleNamespace(dispose=dispose)
        state.engines.append(engine)
        return engine

    monkeypatch.setattr(fixture, "_admin_url", lambda: make_url("postgresql://synthetic@127.0.0.1/postgres"))
    monkeypatch.setattr(fixture.asyncpg, "connect", connect)
    monkeypatch.setattr(fixture, "create_async_engine", create_engine)
    monkeypatch.setattr(fixture, "async_sessionmaker", lambda engine, **_kwargs: engine)
    return state


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    (
        "disconnect_cancel",
        "disconnect_error",
        "target_close",
        "database_drop",
        "role_drop",
        "parameter_revoke",
        "database_assertion",
        "create_role",
        "create_database",
        "body",
    ),
)
async def test_owned_address_fixture_attempts_independent_cleanup_and_preserves_failure(monkeypatch, failure):
    error = (
        asyncio.CancelledError("synthetic cancellation")
        if failure == "disconnect_cancel"
        else RuntimeError("synthetic failure")
    )
    state = _fake_resources(monkeypatch, failure, error)
    expected_error = AssertionError if failure == "database_assertion" else type(error)
    with pytest.raises(expected_error) as raised:
        async with fixture._owned_guard_database(monkeypatch) as resources:
            assert resources.publisher.engine is state.engines[1]
            if failure == "body":
                raise error
    if failure != "database_assertion":
        assert raised.value is error
    assert state.events[-1] == "admin.close"
    assert state.events.count("admin.close") == 1
    assert [event for event in state.events if event.startswith("role.drop:")] == [
        "role.drop:" + name for name in reversed(state.attempted_roles)
    ]
    assert state.events.count("database.drop") == int(state.database is not None)
    if state.database is not None:
        assert state.events.index("database.drop") < state.events.index("role.drop:" + state.attempted_roles[-1])
    if len(state.connections) == 2:
        assert state.events.index("target.close") < state.events.index("database.drop")
        assert [event for event in state.events if event.endswith(".disconnect")] == [
            "database.1.disconnect",
            "database.0.disconnect",
        ]
        assert state.events.index("database.0.disconnect") < state.events.index("target.close")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ("disconnect_cancel", "disconnect_error"))
async def test_cleanup_failure_retains_original_body_exception_context(monkeypatch, failure):
    cleanup_error = (
        asyncio.CancelledError("synthetic cancellation")
        if failure == "disconnect_cancel"
        else RuntimeError("synthetic cleanup failure")
    )
    body_error = ValueError("synthetic operation failure")
    state = _fake_resources(monkeypatch, failure, cleanup_error)
    with pytest.raises(type(cleanup_error)) as raised:
        async with fixture._owned_guard_database(monkeypatch):
            raise body_error
    assert raised.value is cleanup_error
    assert raised.value.__context__ is body_error
    assert state.events[-1] == "admin.close"
    assert state.events.count("target.close") == 1
    assert state.events.count("database.drop") == 1
