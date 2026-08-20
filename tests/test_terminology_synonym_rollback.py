# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Safety and CLI contracts for explicit terminology rollback."""

from __future__ import annotations

import asyncio
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

from click.testing import CliRunner
import pytest

import main


terminology_synonyms = importlib.import_module("process.terminology_synonyms")
COMMAND = "rollback-terminology-synonyms"
ARGUMENTS = [
    "--expected-live-oid",
    "101",
    "--expected-old-oid",
    "202",
]


def _relation_oid_row(live_oid, old_oid):
    return SimpleNamespace(
        _mapping={"live_oid": live_oid, "old_oid": old_oid},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("relation_row", "expected_message"),
    [
        (_relation_oid_row(None, 202), "relation is missing"),
        (_relation_oid_row(303, 202), "relation identity changed"),
    ],
)
async def test_rollback_rejects_missing_or_stale_precheck(
    monkeypatch,
    relation_row,
    expected_message,
):
    all_rows = AsyncMock(return_value=[relation_row])
    status = AsyncMock()
    transaction = AsyncMock()
    monkeypatch.setattr(terminology_synonyms.db, "all", all_rows)
    monkeypatch.setattr(terminology_synonyms.db, "status", status)
    monkeypatch.setattr(terminology_synonyms.db, "transaction", transaction)

    with pytest.raises(RuntimeError, match=expected_message):
        await terminology_synonyms._rollback_terminology_snapshot(
            "tenant",
            expected_live_oid=101,
            expected_old_oid=202,
        )

    status.assert_not_awaited()
    transaction.assert_not_called()


@pytest.mark.asyncio
async def test_rollback_rechecks_relation_identity_after_lock(monkeypatch):
    transaction_outcomes = []

    @asynccontextmanager
    async def transaction():
        try:
            yield
        except Exception:
            transaction_outcomes.append("rollback")
            raise
        transaction_outcomes.append("commit")

    all_rows = AsyncMock(
        side_effect=[
            [_relation_oid_row(101, 202)],
            [_relation_oid_row(303, 202)],
        ]
    )
    status = AsyncMock()
    monkeypatch.setattr(terminology_synonyms.db, "all", all_rows)
    monkeypatch.setattr(terminology_synonyms.db, "status", status)
    monkeypatch.setattr(terminology_synonyms.db, "transaction", transaction)

    with pytest.raises(RuntimeError, match="relation identity changed while acquiring locks"):
        await terminology_synonyms._rollback_terminology_snapshot(
            "tenant",
            expected_live_oid=101,
            expected_old_oid=202,
        )

    assert transaction_outcomes == ["rollback"]
    assert [call.args[0] for call in status.await_args_list] == [
        'LOCK TABLE "tenant"."terminology_synonym_old", '
        '"tenant"."terminology_synonym" IN ACCESS EXCLUSIVE MODE;'
    ]


@pytest.mark.asyncio
async def test_rollback_rejects_duplicate_oids_and_empty_predecessor(monkeypatch):
    all_rows = AsyncMock(
        side_effect=[
            [_relation_oid_row(101, 202)],
            [_relation_oid_row(101, 202)],
            [SimpleNamespace(_mapping={"has_rows": False})],
        ]
    )
    status = AsyncMock()

    @asynccontextmanager
    async def transaction():
        yield

    monkeypatch.setattr(terminology_synonyms.db, "all", all_rows)
    monkeypatch.setattr(terminology_synonyms.db, "status", status)
    monkeypatch.setattr(terminology_synonyms.db, "transaction", transaction)

    with pytest.raises(ValueError, match="must be PostgreSQL OIDs"):
        await terminology_synonyms._rollback_terminology_snapshot(
            "tenant",
            expected_live_oid=0,
            expected_old_oid=202,
        )
    with pytest.raises(ValueError, match="must be distinct"):
        await terminology_synonyms._rollback_terminology_snapshot(
            "tenant",
            expected_live_oid=101,
            expected_old_oid=101,
        )
    all_rows.assert_not_awaited()

    with pytest.raises(RuntimeError, match="predecessor is empty"):
        await terminology_synonyms._rollback_terminology_snapshot(
            "tenant",
            expected_live_oid=101,
            expected_old_oid=202,
        )

    assert all("ALTER TABLE" not in call.args[0] for call in status.await_args_list)


@pytest.mark.asyncio
async def test_rollback_verification_failure_rolls_back_all_renames(monkeypatch):
    transaction_outcomes = []

    @asynccontextmanager
    async def transaction():
        try:
            yield
        except Exception:
            transaction_outcomes.append("rollback")
            raise
        transaction_outcomes.append("commit")

    all_rows = AsyncMock(
        side_effect=[
            [_relation_oid_row(101, 202)],
            [_relation_oid_row(101, 202)],
            [SimpleNamespace(_mapping={"has_rows": True})],
            [_relation_oid_row(303, 101)],
        ]
    )
    status = AsyncMock()
    monkeypatch.setattr(terminology_synonyms.db, "all", all_rows)
    monkeypatch.setattr(terminology_synonyms.db, "status", status)
    monkeypatch.setattr(terminology_synonyms.db, "transaction", transaction)

    with pytest.raises(RuntimeError, match="rollback verification failed"):
        await terminology_synonyms._rollback_terminology_snapshot(
            "tenant",
            expected_live_oid=101,
            expected_old_oid=202,
        )

    assert transaction_outcomes == ["rollback"]
    assert sum("ALTER TABLE" in call.args[0] for call in status.await_args_list) == 3


@pytest.mark.asyncio
async def test_rollback_runtime_connects_and_disconnects(monkeypatch):
    calls = []

    async def init_db(database):
        calls.append(("connect", database))

    async def rollback(schema, **oids):
        calls.append(("rollback", schema, oids))
        return {"live_oid": 202}

    async def disconnect():
        calls.append(("disconnect",))

    monkeypatch.setattr(terminology_synonyms, "init_db", init_db)
    monkeypatch.setattr(terminology_synonyms, "_schema", lambda: "tenant")
    monkeypatch.setattr(terminology_synonyms, "_rollback_terminology_snapshot", rollback)
    monkeypatch.setattr(terminology_synonyms.db, "disconnect", disconnect)

    rollback_result = await terminology_synonyms.rollback_terminology_snapshot(
        expected_live_oid=101,
        expected_old_oid=202,
    )

    assert rollback_result == {"live_oid": 202}
    assert calls == [
        ("connect", terminology_synonyms.db),
        (
            "rollback",
            "tenant",
            {"expected_live_oid": 101, "expected_old_oid": 202},
        ),
        ("disconnect",),
    ]


@pytest.mark.asyncio
async def test_rollback_runtime_preserves_primary_failure_during_disconnect(
    monkeypatch,
    caplog,
):
    async def init_db(_database):
        return None

    async def rollback(*_args, **_kwargs):
        raise RuntimeError("synthetic rollback failure")

    async def disconnect():
        raise RuntimeError("synthetic disconnect failure")

    monkeypatch.setattr(terminology_synonyms, "init_db", init_db)
    monkeypatch.setattr(terminology_synonyms, "_rollback_terminology_snapshot", rollback)
    monkeypatch.setattr(terminology_synonyms.db, "disconnect", disconnect)

    with pytest.raises(RuntimeError, match="synthetic rollback failure"):
        await terminology_synonyms.rollback_terminology_snapshot(
            expected_live_oid=101,
            expected_old_oid=202,
        )
    assert "synthetic disconnect failure" in caplog.text

    monkeypatch.setattr(
        terminology_synonyms,
        "_rollback_terminology_snapshot",
        AsyncMock(return_value={"live_oid": 202}),
    )
    with pytest.raises(RuntimeError, match="synthetic disconnect failure"):
        await terminology_synonyms.rollback_terminology_snapshot(
            expected_live_oid=101,
            expected_old_oid=202,
        )


def _invoke(arguments):
    return CliRunner().invoke(main.manage, [COMMAND, *arguments])


def test_rollback_command_requires_and_forwards_both_relation_oids(monkeypatch):
    calls = []

    async def rollback(*, expected_live_oid, expected_old_oid):
        calls.append((expected_live_oid, expected_old_oid))
        return {
            "live_oid": expected_old_oid,
            "predecessor_oid": expected_live_oid,
        }

    monkeypatch.setattr(
        terminology_synonyms,
        "rollback_terminology_snapshot",
        rollback,
    )
    monkeypatch.setattr(main, "_run_async", asyncio.run)

    result = _invoke(ARGUMENTS)

    assert result.exit_code == 0, result.output
    assert calls == [(101, 202)]
    assert result.output == '{"live_oid":202,"predecessor_oid":101}\n'

    missing_oid_result = _invoke(ARGUMENTS[:-2])
    assert missing_oid_result.exit_code == 2
    assert "Missing option" in missing_oid_result.output


def test_rollback_command_redacts_runtime_failures(monkeypatch):
    async def rollback(**_kwargs):
        raise RuntimeError("postgresql://private.invalid?password=secret")

    monkeypatch.setattr(
        terminology_synonyms,
        "rollback_terminology_snapshot",
        rollback,
    )
    monkeypatch.setattr(main, "_run_async", asyncio.run)

    result = _invoke(ARGUMENTS)

    assert result.exit_code == 1
    assert "terminology synonym rollback failed" in result.output
    assert "private.invalid" not in result.output
    assert "secret" not in result.output
