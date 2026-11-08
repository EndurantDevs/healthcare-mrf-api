# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ownership cleanup retries must restart a rolled-back transaction, without hiding refusals."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import DBAPIError

from tests import provider_directory_source_postgres_support as source_fixture

pytestmark = pytest.mark.asyncio


def _sql_error(code):
    original = RuntimeError("synthetic native refusal")
    original.sqlstate = code
    return DBAPIError("REASSIGN OWNED", None, original)


class _CleanupEngine:
    """Record transaction boundaries and transactional ownership changes."""

    def __init__(self, errors):
        self.errors = errors
        self.state = {"retention": 42, "roles": {"owner_role", "publisher_role"}, "reassigned": []}
        self.starts, self.outcomes = [], []
        self.cancels = 0

    @asynccontextmanager
    async def begin(self):
        before = deepcopy(self.state)
        attempt = len(self.starts)
        self.starts.append(deepcopy(before))

        async def scalar(statement, parameters=None):
            if "to_regnamespace" in str(statement):
                return self.state["retention"]
            return parameters["role"] in self.state["roles"]

        async def execute(statement):
            sql = str(statement)
            if sql.startswith("SELECT pg_cancel_backend"):
                assert not self.state["reassigned"], "autovacuum workers must be stopped before any REASSIGN"
                self.cancels += 1
                return
            if sql.startswith("DROP SCHEMA"):
                self.state["retention"] = None
                return
            role = sql.split('"')[1]
            if sql.startswith("REASSIGN"):
                if role == "owner_role" and self.errors[attempt] is not None:
                    raise self.errors[attempt]
                self.state["reassigned"].append(role)
            elif sql.startswith("DROP ROLE"):
                self.state["roles"].remove(role)

        try:
            yield SimpleNamespace(scalar=scalar, execute=execute)
        except BaseException:
            self.state = before
            self.outcomes.append("rollback")
            raise
        else:
            self.outcomes.append("commit")


async def _cleanup(engine, *, owned_reader=False):
    database = SimpleNamespace(_reader_database=object(), disconnect=AsyncMock())
    await source_fixture._cleanup_native_source_component(
        engine, database, owned_reader, 42, ["owner_role", "publisher_role"], "admin_role"
    )
    database.disconnect.assert_awaited_once()
    return database


@pytest.mark.parametrize("deadlocks", (1, 2))
async def test_deadlock_restarts_complete_cleanup_after_rollback(deadlocks):
    engine = _CleanupEngine([*(_sql_error("40P01") for _ in range(deadlocks)), None])
    initial = deepcopy(engine.state)
    database = await _cleanup(engine)
    assert engine.starts == [initial] * (deadlocks + 1) and engine.cancels == deadlocks + 1
    assert engine.outcomes == ["rollback"] * deadlocks + ["commit"]
    assert engine.state == {"retention": None, "roles": set(), "reassigned": ["publisher_role", "owner_role"]}
    assert database._reader_database is None


async def test_deadlock_exhaustion_rolls_back_and_propagates():
    errors = [_sql_error("40P01") for _ in range(3)]
    engine = _CleanupEngine(errors)
    initial = deepcopy(engine.state)
    with pytest.raises(DBAPIError) as refused:
        await _cleanup(engine)
    assert refused.value is errors[-1]
    assert engine.starts == [initial] * 3 and engine.outcomes == ["rollback"] * 3
    assert engine.state == initial


@pytest.mark.parametrize(
    "error", [_sql_error("42501"), _sql_error("55P03"), _sql_error("40001"), asyncio.CancelledError()]
)
async def test_other_sql_failures_and_cancellation_are_not_retried(error):
    engine = _CleanupEngine([error])
    initial = deepcopy(engine.state)
    with pytest.raises(type(error)) as refused:
        await _cleanup(engine)
    assert refused.value is error
    assert engine.starts == [initial] and engine.outcomes == ["rollback"]
    assert engine.state == initial


async def test_retention_identity_refusal_is_not_retried():
    engine = _CleanupEngine([None])
    engine.state["retention"] = 43
    initial = deepcopy(engine.state)
    with pytest.raises(AssertionError):
        await _cleanup(engine)
    assert engine.starts == [initial] and engine.outcomes == ["rollback"]
    assert engine.state == initial


async def test_cancellation_during_backoff_preserves_completed_rollback(monkeypatch):
    engine = _CleanupEngine([_sql_error("40P01")])
    initial = deepcopy(engine.state)
    monkeypatch.setattr(source_fixture.asyncio, "sleep", AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await _cleanup(engine)
    assert engine.starts == [initial] and engine.outcomes == ["rollback"]
    assert engine.state == initial
