# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host transport tests for issued readonly authority; Native proof is separate."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_source_observation as observation
from process.provider_directory_generic_source_policy import GenericSourcePolicy


class Driver:
    def __init__(self):
        self.is_open = True
        self.calls = []
        self.actor_by_field = dict(
            session="publisher",
            current="owner",
            backend=123,
            readonly="on",
            isolation="repeatable read",
            transaction="7/91",
        )

    def is_in_transaction(self):
        return self.is_open

    async def fetchrow(self, sql):
        assert sql == observation._ORIGIN_SQL
        self.calls.append(sql)
        return dict(self.actor_by_field)


class Database:
    def __init__(self):
        self.binding = ContextVar("host_observation_binding", default=None)
        self.events = []

    def _transaction_binding(self):
        return self.binding.get()

    @asynccontextmanager
    async def bind_existing_session(self, session):
        assert self.binding.get() is None
        token = self.binding.set(SimpleNamespace(session=session, borrowed=True, owner_task=asyncio.current_task()))
        self.events.append("bind")
        try:
            yield session
        finally:
            self.events.append("unbind")
            self.binding.reset(token)


class Factory:
    def __init__(self):
        self.policy = GenericSourcePolicy("synthetic", "owner", "publisher", ("reader",), ())
        self.producer_identity = ("a" * 40, "sha256:" + "b" * 64)
        self.driver = Driver()
        self.session = SimpleNamespace(driver=self.driver)
        self.events = []
        self.policy_checks = []

    def _require_current_policy(self):
        self.policy_checks.append(True)

    @asynccontextmanager
    async def observation_transaction(self, *, isolation):
        self.driver.actor_by_field["isolation"] = isolation
        self.events.append(("enter", isolation))
        try:
            yield self.session
        finally:
            self.events.append(("exit", isolation))


@pytest.fixture
async def case(monkeypatch):
    factory = Factory()
    directory = SimpleNamespace(db=Database(), _schema=lambda: "synthetic")
    monkeypatch.setattr(observation.source, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(observation, "_copy_driver", AsyncMock(side_effect=lambda session: session.driver))
    token = observation.source._FACTORY.set(factory)
    try:
        yield SimpleNamespace(factory=factory, directory=directory)
    finally:
        observation.source._FACTORY.reset(token)
        assert observation._ACTIVE.get() is None


async def test_genuine_observation_is_bound_and_leaves_no_writer_authority(case):
    directory, factory = case.directory, case.factory
    assert observation.current_observation(directory) is None
    async with observation.observation_transaction(directory, isolation="repeatable read") as session:
        issued = observation.current_observation(directory)
        assert issued.session is session is factory.session
        assert issued.connection is factory.driver
        assert issued.origin == (123, "7/91")
        assert issued.task is asyncio.current_task() and issued.producer == factory.producer_identity
        assert observation.source._PUBLISHER.get() is None and observation.source._OWNER.get() is None
        assert directory.db._transaction_binding().session is session
    assert observation.current_observation(directory) is None
    assert directory.db._transaction_binding() is None
    assert factory.events == [("enter", "repeatable read"), ("exit", "repeatable read")]
    assert directory.db.events == ["bind", "unbind"]


async def test_nested_read_reuses_exact_session_and_stronger_isolation(case):
    async with observation.observation_transaction(case.directory, isolation="serializable") as outer:
        async with observation.observation_transaction(case.directory, isolation="repeatable read") as inner:
            assert inner is outer
        with pytest.raises(RuntimeError, match="observation_required"):
            async with observation.observation_transaction(case.directory, isolation="invalid"):
                pytest.fail("invalid isolation exposed")
    assert len(case.factory.events) == 2


@pytest.mark.parametrize(
    ("field", "replacement"),
    [("session", "reader"), ("current", "reader"), ("readonly", "off"), ("backend", 0), ("transaction", None)],
)
async def test_native_wrong_actor_or_transaction_refuses_before_body(case, field, replacement):
    case.factory.driver.actor_by_field[field] = replacement
    with pytest.raises(RuntimeError, match="actor_changed"):
        async with observation.observation_transaction(case.directory, isolation="repeatable read"):
            pytest.fail("wrong native actor exposed")
    assert case.directory.db._transaction_binding() is None
    assert case.factory.events[-1][0] == "exit"


async def test_read_committed_cannot_satisfy_repeatable_read(case):
    async with observation.observation_transaction(case.directory, isolation="read committed"):
        with pytest.raises(RuntimeError, match="isolation_changed"):
            async with observation.observation_transaction(case.directory, isolation="repeatable read"):
                pytest.fail("weaker snapshot exposed")


async def test_inherited_task_cannot_use_parent_observation(case):
    async with observation.observation_transaction(case.directory, isolation="repeatable read"):
        before = len(case.factory.driver.calls)

        async def child():
            with pytest.raises(RuntimeError, match="observation_changed"):
                observation.current_observation(case.directory)

        await asyncio.create_task(child())
        assert len(case.factory.driver.calls) == before


async def test_unissued_or_closed_record_cannot_be_replayed(case):
    async with observation.observation_transaction(case.directory, isolation="repeatable read"):
        issued = observation.current_observation(case.directory)
        token = observation._ACTIVE.set(replace(issued))
        try:
            with pytest.raises(RuntimeError, match="observation_changed"):
                observation.current_observation(case.directory)
        finally:
            observation._ACTIVE.reset(token)
    token = observation._ACTIVE.set(issued)
    try:
        with pytest.raises(RuntimeError, match="observation_changed"):
            observation.current_observation(case.directory)
    finally:
        observation._ACTIVE.reset(token)


@pytest.mark.parametrize("change", ["backend", "transaction", "readonly", "isolation", "driver", "binding", "policy"])
async def test_postcheck_rejects_replaced_native_or_logical_binding(case, change):
    with pytest.raises(RuntimeError):
        async with observation.observation_transaction(case.directory, isolation="repeatable read"):
            if change in {"backend", "transaction", "readonly", "isolation"}:
                case.factory.driver.actor_by_field[change] = "changed"
            if change == "driver":
                case.factory.session.driver = Driver()
            if change == "binding":
                case.directory.db.binding.set(None)
            if change == "policy":
                case.factory.policy = GenericSourcePolicy("other", "owner", "publisher", ("reader",), ())
    assert observation._ACTIVE.get() is None


async def test_primary_cancellation_preserved_with_postcheck_failure(case):
    original = asyncio.CancelledError("original")
    with pytest.raises(asyncio.CancelledError) as caught:
        async with observation.observation_transaction(case.directory, isolation="repeatable read"):
            case.factory.driver.actor_by_field["readonly"] = "off"
            raise original
    assert caught.value is original
    assert isinstance(caught.value.__cause__, BaseExceptionGroup)
    assert caught.value.__cause__.exceptions[0] is original
    assert case.directory.db._transaction_binding() is None


async def test_primary_error_preserved_when_observation_stays_valid(case):
    original = LookupError("original")
    with pytest.raises(LookupError) as caught:
        async with observation.observation_transaction(case.directory, isolation="repeatable read"):
            raise original
    assert caught.value is original


@pytest.mark.parametrize("context_name", ["_PUBLISHER", "_OWNER"])
async def test_writer_capability_cannot_be_reclassified_as_observation(case, context_name):
    variable = getattr(observation.source, context_name)
    token = variable.set(object())
    try:
        with pytest.raises(RuntimeError, match="own_transaction_required"):
            async with observation.observation_transaction(case.directory, isolation="repeatable read"):
                pytest.fail("writer binding laundered")
    finally:
        variable.reset(token)
    assert case.factory.events == []


async def test_ordinary_bound_transaction_cannot_become_source_observation(case):
    async with case.directory.db.bind_existing_session(object()):
        with pytest.raises(RuntimeError, match="own_transaction_required"):
            async with observation.observation_transaction(case.directory, isolation="repeatable read"):
                pytest.fail("ordinary binding exposed")
    assert case.factory.events == []
