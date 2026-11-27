# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Safety and CLI contracts for explicit terminology rollback."""

from __future__ import annotations

import asyncio
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from click.testing import CliRunner

import main

terminology_synonyms = importlib.import_module("process.terminology_synonyms")
COMMAND = "rollback-terminology-synonyms"


def _row(**values):
    return SimpleNamespace(_mapping=values)


def _transaction(outcomes):
    @asynccontextmanager
    async def transaction():
        try:
            yield
        except Exception:
            outcomes.append("rollback")
            raise
        outcomes.append("commit")

    return transaction


@pytest.mark.asyncio
async def test_rollback_rejects_missing_relation_before_locking(monkeypatch):
    all_rows = AsyncMock(return_value=[_row(present=False)])
    status = AsyncMock()
    transaction = AsyncMock()
    monkeypatch.setattr(terminology_synonyms.db, "all", all_rows)
    monkeypatch.setattr(terminology_synonyms.db, "status", status)
    monkeypatch.setattr(terminology_synonyms.db, "transaction", transaction)

    with pytest.raises(RuntimeError, match="relation is missing"):
        await terminology_synonyms._rollback_terminology_snapshot("tenant")

    status.assert_not_awaited()
    transaction.assert_not_called()


@pytest.mark.asyncio
async def test_rollback_rejects_empty_predecessor_after_lock(monkeypatch):
    outcomes = []
    all_rows = AsyncMock(side_effect=[[_row(present=True)], [_row(has_rows=False)]])
    status = AsyncMock()
    monkeypatch.setattr(terminology_synonyms.db, "all", all_rows)
    monkeypatch.setattr(terminology_synonyms.db, "status", status)
    monkeypatch.setattr(terminology_synonyms.db, "transaction", _transaction(outcomes))

    with pytest.raises(RuntimeError, match="predecessor is empty"):
        await terminology_synonyms._rollback_terminology_snapshot("tenant")

    assert outcomes == ["rollback"]
    assert [call.args[0] for call in status.await_args_list] == [
        'LOCK TABLE "tenant"."terminology_synonym_old", "tenant"."terminology_synonym" IN ACCESS EXCLUSIVE MODE;'
    ]


@pytest.mark.asyncio
async def test_rollback_swaps_by_name_without_relation_identity_checks(monkeypatch):
    outcomes = []
    all_rows = AsyncMock(side_effect=[[_row(present=True)], [_row(has_rows=True)]])
    status = AsyncMock()
    publish = AsyncMock()
    monkeypatch.setattr(terminology_synonyms.db, "all", all_rows)
    monkeypatch.setattr(terminology_synonyms.db, "status", status)
    monkeypatch.setattr(terminology_synonyms.db, "transaction", _transaction(outcomes))
    monkeypatch.setattr(terminology_synonyms, "publish_local_reference_family_generation", publish)

    assert await terminology_synonyms._rollback_terminology_snapshot("tenant") == {
        "live": "terminology_synonym",
        "predecessor": "terminology_synonym_old",
        "schema": "tenant",
    }

    assert outcomes == ["commit"]
    assert all_rows.await_count == 2
    assert "oid" not in str(all_rows.await_args_list[0].args[0])
    assert sum("ALTER TABLE" in call.args[0] for call in status.await_args_list) == 3
    publish.assert_awaited_once_with(terminology_synonyms.db, importer_id="terminology-synonyms", schema_name="tenant")


@pytest.mark.asyncio
async def test_rollback_runtime_connects_and_disconnects(monkeypatch):
    calls = []

    async def init_db(database):
        calls.append(("connect", database))

    async def rollback(schema):
        calls.append(("rollback", schema))
        return {"schema": schema}

    async def disconnect():
        calls.append(("disconnect",))

    monkeypatch.setattr(terminology_synonyms, "init_db", init_db)
    monkeypatch.setattr(terminology_synonyms, "_schema", lambda: "tenant")
    monkeypatch.setattr(terminology_synonyms, "_rollback_terminology_snapshot", rollback)
    monkeypatch.setattr(terminology_synonyms.db, "disconnect", disconnect)

    rollback_result = await terminology_synonyms.rollback_terminology_snapshot()

    assert rollback_result == {"schema": "tenant"}
    assert calls == [
        ("connect", terminology_synonyms.db),
        ("rollback", "tenant"),
        ("disconnect",),
    ]


@pytest.mark.asyncio
async def test_rollback_runtime_preserves_primary_failure_during_disconnect(
    monkeypatch,
    caplog,
):
    async def init_db(_database):
        return None

    async def rollback(*_args):
        raise RuntimeError("synthetic rollback failure")

    async def disconnect():
        raise RuntimeError("synthetic disconnect failure")

    monkeypatch.setattr(terminology_synonyms, "init_db", init_db)
    monkeypatch.setattr(terminology_synonyms, "_rollback_terminology_snapshot", rollback)
    monkeypatch.setattr(terminology_synonyms.db, "disconnect", disconnect)

    with pytest.raises(RuntimeError, match="synthetic rollback failure"):
        await terminology_synonyms.rollback_terminology_snapshot()
    assert "synthetic disconnect failure" in caplog.text

    monkeypatch.setattr(
        terminology_synonyms,
        "_rollback_terminology_snapshot",
        AsyncMock(return_value={"schema": "tenant"}),
    )
    with pytest.raises(RuntimeError, match="synthetic disconnect failure"):
        await terminology_synonyms.rollback_terminology_snapshot()


def _invoke(arguments):
    return CliRunner().invoke(main.manage, [COMMAND, *arguments])


def test_rollback_command_takes_no_relation_oids(monkeypatch):
    calls = []

    async def rollback():
        calls.append("rollback")
        return {"live": "terminology_synonym", "schema": "tenant"}

    monkeypatch.setattr(
        terminology_synonyms,
        "rollback_terminology_snapshot",
        rollback,
    )
    monkeypatch.setattr(main, "_run_async", asyncio.run)

    result = _invoke([])

    assert result.exit_code == 0, result.output
    assert calls == ["rollback"]
    assert result.output == '{"live":"terminology_synonym","schema":"tenant"}\n'

    old_flag_result = _invoke(["--expected-live-oid", "101"])
    assert old_flag_result.exit_code == 2
    assert "No such option" in old_flag_result.output


def test_rollback_command_redacts_runtime_failures(monkeypatch):
    async def rollback():
        raise RuntimeError("postgresql://private.invalid?password=secret")

    monkeypatch.setattr(
        terminology_synonyms,
        "rollback_terminology_snapshot",
        rollback,
    )
    monkeypatch.setattr(main, "_run_async", asyncio.run)

    result = _invoke([])

    assert result.exit_code == 1
    assert "terminology synonym rollback failed" in result.output
    assert "private.invalid" not in result.output
    assert "secret" not in result.output
