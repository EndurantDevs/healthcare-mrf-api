# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native fixture teardown closes its administrator while preserving failures."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tests import test_provider_directory_cms_prepared_context_postgres as fixture


def _connection():
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(), is_closed=Mock(return_value=False))
    connection.close = AsyncMock(side_effect=lambda: setattr(connection.is_closed, "return_value", True))
    return connection


def _fixture_resources(monkeypatch):
    admin, native = _connection(), _connection()
    admin.fetchval.side_effect = [True, False, False, False, *([False] * 6)]
    connect = AsyncMock(side_effect=[admin, native])
    monkeypatch.setenv("HLTHPRT_CMS_CUSTODY_TEST_DSN", "postgresql://synthetic@127.0.0.1:5449/postgres")
    monkeypatch.setattr(fixture.asyncpg, "connect", connect)

    async def register_roles(_admin, role_by_kind, _password, created_roles):
        created_roles.extend(role_by_kind.values())

    case = object()
    monkeypatch.setattr(fixture, "_create_input_roles", register_roles)
    monkeypatch.setattr(fixture, "_create_input_relations", AsyncMock())
    monkeypatch.setattr(fixture, "_input_case", AsyncMock(return_value=case))
    return SimpleNamespace(admin=admin, native=native, connect=connect, case=case)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ("native_close", "database_drop", "database_census"))
async def test_fixture_closes_administrator_after_cleanup_failure(monkeypatch, failure):
    resources = _fixture_resources(monkeypatch)
    error = TimeoutError("synthetic cleanup timeout")
    if failure == "native_close":
        resources.native.close.side_effect = error
    elif failure == "database_drop":

        async def execute(statement):
            if statement.startswith("DROP DATABASE"):
                assert statement.endswith(" WITH (FORCE)")
                raise error

        resources.admin.execute.side_effect = execute
    else:
        resources.admin.fetchval.side_effect = [True, False, error]

    generator = fixture.native_case.__wrapped__()
    assert await anext(generator) is resources.case
    with pytest.raises(TimeoutError) as raised:
        await anext(generator)
    assert raised.value is error
    resources.native.close.assert_awaited_once()
    resources.admin.close.assert_awaited_once()
    assert resources.admin.is_closed()
    if failure != "native_close":
        assert resources.native.is_closed()
    assert all(call.kwargs == {"command_timeout": 10} for call in resources.connect.await_args_list)
    assert not any(call.args[0].startswith("DROP ROLE") for call in resources.admin.execute.await_args_list)
