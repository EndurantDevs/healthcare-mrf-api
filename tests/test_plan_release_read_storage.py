# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Statement-local catalog tuning keeps the caller's transaction intact."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api import plan_release_read_storage as storage


def catalog_connection(previous="on", failure=None):
    savepoint = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock())
    setting = Mock()
    rows = Mock()
    rows.__iter__ = Mock(return_value=iter([SimpleNamespace(_mapping={"catalog": True})]))
    connection = SimpleNamespace(
        begin_nested=AsyncMock(return_value=savepoint),
        scalar=AsyncMock(side_effect=[previous, previous]),
        execute=AsyncMock(side_effect=[setting, failure or rows]),
        invalidate=AsyncMock(),
    )
    return connection, savepoint, setting, rows


@pytest.mark.parametrize("previous", ["on", "off"])
@pytest.mark.parametrize("orm", [False, True])
async def test_catalog_restores_actual_jit_and_releases_without_orm_flush(previous, orm):
    connection, savepoint, setting, rows = catalog_connection(previous)
    session = Mock(spec=AsyncSession) if orm else connection
    if orm:
        session.connection = AsyncMock(return_value=connection)
        session.flush = AsyncMock()
        session.begin_nested = Mock(side_effect=AssertionError("session savepoint would flush"))
    statement, parameters = text("SELECT :value"), {"value": 3}
    assert await storage._catalog_session_rows(session, statement, parameters) == [{"catalog": True}]
    assert [str(call.args[0]) for call in connection.execute.await_args_list] == ["SET LOCAL jit = off", str(statement)]
    assert connection.execute.await_args_list[1].args == (statement, parameters)
    assert connection.scalar.await_args_list[1].args[1] == {"previous_jit": previous}
    savepoint.commit.assert_awaited_once()
    savepoint.rollback.assert_not_awaited()
    setting.close.assert_called_once()
    rows.close.assert_called_once()
    if orm:
        session.flush.assert_not_awaited()
        session.begin_nested.assert_not_called()


@pytest.mark.parametrize("failure", [ValueError("catalog failed"), asyncio.CancelledError()])
async def test_query_failure_or_cancel_keeps_original_error_and_rolls_back(failure):
    connection, savepoint, setting, _rows = catalog_connection(failure=failure)
    with pytest.raises(type(failure)) as caught:
        await storage._catalog_session_rows(connection, text("SELECT 1"), {})
    assert caught.value is failure
    savepoint.rollback.assert_awaited_once()
    savepoint.commit.assert_not_awaited()
    connection.invalidate.assert_not_awaited()
    assert connection.scalar.await_count == 1
    setting.close.assert_called_once()


@pytest.mark.parametrize("stage", ["capture", "disable", "decode", "restore", "readback", "release"])
async def test_every_incomplete_path_rolls_back_without_returning_rows(stage):
    connection, savepoint, _setting, rows = catalog_connection()
    failure = RuntimeError("synthetic failure")
    failure_target = {
        "capture": connection.scalar,
        "disable": connection.execute,
        "decode": rows.__iter__,
        "restore": connection.scalar,
        "readback": connection.scalar,
        "release": savepoint.commit,
    }[stage]
    if stage == "restore":
        failure_target.side_effect = ["on", failure]
    elif stage == "readback":
        failure_target.side_effect = ["on", "off"]
    else:
        failure_target.side_effect = failure
    with pytest.raises(RuntimeError):
        await storage._catalog_session_rows(connection, text("SELECT 1"), {})
    savepoint.rollback.assert_awaited_once()
    if stage in {"decode", "restore", "readback", "release"}:
        rows.close.assert_called_once()


@pytest.mark.parametrize("invalidation_fails", [False, True])
async def test_failed_savepoint_cleanup_invalidates_and_preserves_primary(invalidation_fails):
    failure = asyncio.CancelledError()
    connection, savepoint, _setting, _rows = catalog_connection(failure=failure)
    savepoint.rollback.side_effect = RuntimeError("rollback failed")
    if invalidation_fails:
        connection.invalidate.side_effect = RuntimeError("invalidation failed")
    with pytest.raises(asyncio.CancelledError) as caught:
        await storage._catalog_session_rows(connection, text("SELECT 1"), {})
    assert caught.value is failure
    connection.invalidate.assert_awaited_once()
    assert len(failure.__notes__) == 1 + invalidation_fails


async def test_only_exact_catalog_uses_orm_tuning_and_native_callback_is_unchanged(monkeypatch):
    direct_rows = [SimpleNamespace(_mapping={"ordinary": True})]
    session = SimpleNamespace(execute=AsyncMock(return_value=direct_rows))
    tuned = AsyncMock(return_value=[{"catalog": True}])
    calls = []

    async def is_retained_binding(fetch, schema):
        calls.append(schema)
        assert await fetch("SELECT $1::text", "value") == [{"ordinary": True}]
        assert await fetch(storage.CATALOG_SQL, ["root"], 2, [3]) == [{"catalog": True}]
        return True

    monkeypatch.setattr(storage, "is_retained_binding_read", is_retained_binding)
    monkeypatch.setattr(storage, "_catalog_session_rows", tuned)
    assert await storage.is_retained_session_read(session, "schema") is True
    assert calls == ["schema"]
    session.execute.assert_awaited_once()
    assert str(session.execute.await_args.args[0]) == "SELECT CAST(:p1 AS text)"
    assert tuned.await_args.args[0] is session
    assert tuned.await_args.args[2] == {"p1": ["root"], "p2": 2, "p3": [3]}
