# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Owned fixture cleanup survives failures without dropping uncreated resources."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.engine import make_url

from tests import test_tiger_captured_epoch_postgres as capture_fixture
from tests import test_tiger_snapshot_inheritance_postgres as inheritance_fixture


def _mock_owned_resources(monkeypatch, module, failure):
    state = SimpleNamespace(events=[], created_roles=[], dropped_roles=[], connections=[], engines=[], is_created=False)

    async def execute(statement):
        if statement.startswith("CREATE ROLE"):
            name = statement.split('"')[1]
            if failure == "create_role" and len(state.created_roles) == 1:
                raise RuntimeError("synthetic failure")
            state.created_roles.append(name)
        elif statement.startswith("CREATE DATABASE"):
            if failure == "create_database":
                raise RuntimeError("synthetic failure")
            state.is_created = True
        elif statement.startswith("DROP DATABASE"):
            assert state.is_created and statement.endswith(" WITH (FORCE)")
            state.events.append("database.drop")
            if failure == "drop_database":
                raise RuntimeError("synthetic failure")
        elif statement.startswith("DROP ROLE"):
            state.dropped_roles.append(statement.split('"')[1])
            state.events.append("role.drop")

    async def connect(_url):
        index = len(state.connections)

        async def close(**_kwargs):
            state.events.append("admin.close" if index == 0 else "connection.close")

        connection = SimpleNamespace(execute=execute, fetchval=AsyncMock(return_value=False), close=close)
        state.connections.append(connection)
        return connection

    def create_engine(_url):
        index = len(state.engines)

        async def dispose():
            state.events.append(f"engine.{index}.dispose")
            if failure == "dispose" and index == 1:
                raise RuntimeError("synthetic failure")

        engine = SimpleNamespace(dispose=dispose)
        state.engines.append(engine)
        return engine

    monkeypatch.setattr(module.asyncpg, "connect", connect)
    monkeypatch.setattr(module, "create_async_engine", create_engine)
    monkeypatch.setattr(module, "async_sessionmaker", lambda engine, **_kwargs: engine)
    return state


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ("capture", "inheritance"))
@pytest.mark.parametrize("failure", ("create_role", "create_database", "drop_database", "dispose"))
async def test_fixture_attempts_all_owned_cleanup_after_failure(monkeypatch, kind, failure):
    """Failed CREATE never earns DROP authority; later cleanup failures do not stop unwinding."""
    module = capture_fixture if kind == "capture" else inheritance_fixture
    state = _mock_owned_resources(monkeypatch, module, failure)
    url = make_url("postgresql+asyncpg://synthetic@127.0.0.1:5432/postgres")
    if kind == "capture":
        monkeypatch.delenv("HLTHPRT_TIGER_CAPTURE_CI_TEST", raising=False)
        monkeypatch.setenv("HLTHPRT_TIGER_CAPTURE_TEST_DSN", url.render_as_string(hide_password=False))
        monkeypatch.setattr(module, "_seed_inherited_tiger", AsyncMock())
        context = module.owned_tiger_capture_database
    else:
        monkeypatch.setattr(module, "_database_url", lambda: url)
        context = module._owned_database

    with pytest.raises(RuntimeError, match="synthetic failure"):
        async with context() as resource:
            assert resource is not None

    assert state.dropped_roles == list(reversed(state.created_roles))
    assert state.events.count("database.drop") == int(state.is_created)
    assert state.events.count("admin.close") == 1
    if state.is_created:
        assert state.events.index("database.drop") < state.events.index("role.drop") < state.events.index("admin.close")
    if state.engines:
        assert [event for event in state.events if event.startswith("engine.")] == [
            f"engine.{index}.dispose" for index in reversed(range(len(state.engines)))
        ]
        assert state.events.index("engine.0.dispose") < state.events.index("database.drop")
    if kind == "capture" and state.is_created:
        assert state.events.index("connection.close") < state.events.index("database.drop")


def _em_heap_resources(failure):
    import asyncpg

    sentinel_by_field = {"oid": 17, "sentinel_by_field": "preserve"}
    schemas = {"tiger": sentinel_by_field} if failure == "preexisting" else {}
    statements = []

    async def execute(statement):
        statements.append(statement)
        if statement.startswith("CREATE SCHEMA"):
            namespace = statement.split('"')[1] if '"' in statement else statement.split()[-1]
            if namespace in schemas:
                raise asyncpg.DuplicateSchemaError("synthetic existing schema")
            schemas[namespace] = {"oid": 100 + len(schemas)}
        elif statement.startswith("CREATE TABLE tiger.zcta5"):
            if failure == "replacement":
                schemas["tiger"] = sentinel_by_field
            raise RuntimeError("synthetic table failure")
        elif statement.startswith("DROP SCHEMA"):
            schemas.pop(statement.split('"')[1], None)

    async def fetchval(statement, *arguments):
        if "current_user" in statement:
            return "synthetic_operator"
        if "to_regnamespace" in statement:
            return schemas.get(arguments[0], {}).get("oid")
        return False

    connection = SimpleNamespace(
        execute=execute, fetchval=fetchval, fetch=AsyncMock(return_value=[]), close=AsyncMock()
    )
    return connection, schemas, statements, sentinel_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize("caller", ("published", "native"))
@pytest.mark.parametrize("failure", ("preexisting", "partial", "replacement"))
async def test_em_heap_cleanup_preserves_unowned_schema(monkeypatch, caller, failure):
    """Failed creation and same-name replacements never authorize schema cleanup."""
    import asyncpg

    from db import connection as database_module
    from tests import managed_em_resolution_postgres_support as resolver_fixture
    from tests import test_plan_pricing_source_policy_postgres as provider_fixture

    connection, schemas, statements, sentinel_by_field = _em_heap_resources(failure)
    monkeypatch.setenv("NETWORK_REGISTRY_TEST_DSN", "postgresql://synthetic@127.0.0.1/postgres")
    monkeypatch.setattr(resolver_fixture, "_allow_composition_owner", AsyncMock())
    monkeypatch.setattr(provider_fixture, "_em_provider_create_roles", AsyncMock())
    monkeypatch.setattr(asyncpg, "connect", AsyncMock(return_value=connection))
    monkeypatch.setattr(database_module, "Database", lambda: SimpleNamespace(_reader_database=None))
    error = asyncpg.DuplicateSchemaError if failure == "preexisting" else RuntimeError
    with pytest.raises(error, match="synthetic"):
        if caller == "published":
            fixture = SimpleNamespace(
                connection=connection, roles={"owner": "owner", "reader": "reader"}, control_schema="control"
            )
            await resolver_fixture.test_published_provider_resolver(fixture, monkeypatch)
        else:
            async with provider_fixture._em_provider_native_context((connection, "control", None), monkeypatch):
                pytest.fail("fixture setup must fail")
    assert schemas == ({"tiger": sentinel_by_field} if failure != "partial" else {})
    dropped_names = [statement.split('"')[1] for statement in statements if statement.startswith("DROP SCHEMA")]
    assert ("tiger" in dropped_names) is (failure == "partial")
    assert "RESET ROLE" in statements
