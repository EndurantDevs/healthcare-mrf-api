# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import asyncpg
import pytest
from click.testing import CliRunner

import db.connection
import main
import process.ext.utils
from process import procedure_taxonomy_signals as publication


@pytest.mark.parametrize("action", ["initialize", "refresh", "rollback"])
def test_publication_command_dispatches_and_closes_connection(monkeypatch, action):
    connection = object()
    acquire = Mock(return_value=nullcontext(connection))
    disconnect = AsyncMock()
    ensure_database = AsyncMock()
    initialize = AsyncMock()
    refresh = AsyncMock(return_value={"status": "published"})
    rollback = AsyncMock(return_value={"status": "rolled_back"})
    monkeypatch.setenv("HLTHPRT_PRICING_TAXONOMY_PUBLICATION_SCHEMA", "taxonomy_fixture")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "source_fixture")
    monkeypatch.setattr(db.connection, "db", SimpleNamespace(acquire_driver=acquire, disconnect=disconnect))
    monkeypatch.setattr(process.ext.utils, "ensure_database", ensure_database)
    monkeypatch.setattr(publication, "initialize_procedure_taxonomy_publication", initialize)
    monkeypatch.setattr(publication, "rebuild_procedure_taxonomy_signals", refresh)
    monkeypatch.setattr(publication, "rollback_procedure_taxonomy_signals", rollback)

    result = CliRunner().invoke(main.manage, ["procedure-taxonomy", action])

    assert result.exit_code == 0, result.exception
    status_by_action = {"initialize": "initialized", "refresh": "published", "rollback": "rolled_back"}
    assert json.loads(result.output)["status"] == status_by_action[action]
    assert initialize.await_args_list == ([call(connection, "taxonomy_fixture")] if action == "initialize" else [])
    assert refresh.await_args_list == (
        [call(connection, "source_fixture", "taxonomy_fixture")] if action == "refresh" else []
    )
    assert rollback.await_args_list == ([call(connection, "taxonomy_fixture")] if action == "rollback" else [])
    ensure_database.assert_awaited_once_with(False)
    acquire.assert_called_once_with(detached=True)
    disconnect.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_unconfigured_publication_refuses_to_connect(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PRICING_TAXONOMY_PUBLICATION_SCHEMA", raising=False)
    ensure_database = AsyncMock()
    monkeypatch.setattr(process.ext.utils, "ensure_database", ensure_database)

    with pytest.raises(RuntimeError, match="publication schema is not configured"):
        await publication.run_procedure_taxonomy_publication("refresh")

    ensure_database.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["refresh", "rollback"])
async def test_publication_never_changes_the_view_when_the_lock_is_held(action):
    connection = SimpleNamespace(
        transaction=Mock(return_value=nullcontext()), fetchval=AsyncMock(return_value=False), execute=AsyncMock()
    )

    if action == "refresh":
        result = await publication.rebuild_procedure_taxonomy_signals(connection, "source_fixture", "taxonomy_fixture")
        connection.execute.assert_not_awaited()
    else:
        result = await publication.rollback_procedure_taxonomy_signals(connection, "taxonomy_fixture")
        connection.execute.assert_awaited_once_with("SET LOCAL lock_timeout = '2s'")

    assert result == {"status": "busy"}
    assert connection.fetchval.await_count == 1


@pytest.mark.asyncio
async def test_refresh_discards_unpublished_evidence_when_inputs_change(monkeypatch):
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=True),
        fetchrow=AsyncMock(return_value={"input_revision": 2, "current_revision": 1, "rollback_at_revision": None}),
        execute=AsyncMock(),
    )
    cleanup = AsyncMock(return_value=None)
    build = AsyncMock(return_value=(2, 1))
    publish = AsyncMock(return_value="inputs_changed")
    monkeypatch.setattr(publication, "_drop_unpublished", cleanup)
    monkeypatch.setattr(publication, "_build_generation", build)
    monkeypatch.setattr(publication, "_publish_generation", publish)

    assert await publication.rebuild_procedure_taxonomy_signals(connection, "source_fixture", "taxonomy_fixture") == {
        "status": "inputs_changed"
    }

    built_name = build.await_args.args[-1]
    publish.assert_awaited_once_with(connection, "taxonomy_fixture", built_name, 2)
    assert cleanup.await_args_list == [call(connection, "taxonomy_fixture"), call(connection, "taxonomy_fixture")]
    connection.execute.assert_awaited_once_with(
        "SELECT pg_advisory_unlock(hashtextextended($1, 0))", "procedure-taxonomy:taxonomy_fixture"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [asyncpg.LockNotAvailableError(), PermissionError("cleanup denied")])
async def test_publication_leaves_serving_evidence_alone_when_cleanup_fails(error):
    connection = SimpleNamespace(
        transaction=Mock(return_value=nullcontext()),
        fetchval=AsyncMock(side_effect=[True, error]),
        fetchrow=AsyncMock(return_value={"input_revision": 2, "current_revision": 1, "rollback_at_revision": None}),
        execute=AsyncMock(),
    )

    if isinstance(error, asyncpg.LockNotAvailableError):
        assert await publication.rebuild_procedure_taxonomy_signals(
            connection, "source_fixture", "taxonomy_fixture"
        ) == {"status": "cleanup_pending"}
    else:
        with pytest.raises(PermissionError, match="cleanup denied"):
            await publication.rebuild_procedure_taxonomy_signals(connection, "source_fixture", "taxonomy_fixture")

    assert connection.execute.await_args_list == [
        call("SET LOCAL lock_timeout = '2s'"),
        call("SELECT pg_advisory_unlock(hashtextextended($1, 0))", "procedure-taxonomy:taxonomy_fixture"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("previous_table", "error", "message"),
    [
        (None, RuntimeError, "no previous procedure taxonomy generation"),
        ("unrelated_table", ValueError, "Invalid procedure taxonomy generation name"),
    ],
)
async def test_rollback_refuses_a_missing_or_invalid_previous_generation(previous_table, error, message):
    connection = SimpleNamespace(
        transaction=Mock(return_value=nullcontext()),
        fetchval=AsyncMock(side_effect=[True, previous_table]),
        execute=AsyncMock(),
    )

    with pytest.raises(error, match=message):
        await publication.rollback_procedure_taxonomy_signals(connection, "taxonomy_fixture")

    connection.execute.assert_awaited_once_with("SET LOCAL lock_timeout = '2s'")
