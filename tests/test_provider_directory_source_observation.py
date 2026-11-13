# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named observation routing, logical transaction lifetime and failure cleanup."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.ext.asyncio import AsyncSession

from process import provider_directory_source_observation as observation
from process import provider_directory_source_storage_binding as storage
from process.provider_directory_generic_source_policy import GenericSourcePolicy


class Driver:
    def __init__(self):
        self.calls = []

    async def fetchrow(self, sql):
        self.calls.append(sql)
        raise AssertionError("observation attempted physical identity lookup")


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
        self.driver = Driver()
        self.transaction = SimpleNamespace(is_active=False)
        self.session = SimpleNamespace(driver=self.driver, get_transaction=lambda: self.transaction)
        self.events = []

    @asynccontextmanager
    async def observation_transaction(self, *, isolation):
        self.transaction.is_active = True
        self.events.append(("enter", isolation))
        try:
            yield self.session
        finally:
            self.transaction.is_active = False
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
        assert issued.policy == factory.policy and issued.isolation == "repeatable read"
        assert session.get_transaction().is_active
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


async def test_named_observation_checks_transaction_lifecycle_without_connection_identity(case):
    factory = case.factory
    factory.storage_binding = storage.NamedFHIRSourceStorage("catalog", factory.policy.schema)
    case.directory._schema = lambda: "catalog"
    transaction = SimpleNamespace(is_active=True)
    factory.session.get_transaction = lambda: transaction
    async with observation.observation_transaction(case.directory, isolation="repeatable read"):
        assert transaction.is_active
    with pytest.raises(RuntimeError, match="observation_transaction_required"):
        async with observation.observation_transaction(case.directory, isolation="repeatable read"):
            transaction.is_active = False
    assert factory.driver.calls == []


async def test_named_observation_cannot_restart_factory_managed_transaction(case):
    factory = case.factory
    factory.storage_binding = storage.NamedFHIRSourceStorage("catalog", factory.policy.schema)
    case.directory._schema = lambda: "catalog"
    session = AsyncSession()
    session.driver = factory.driver

    @asynccontextmanager
    async def managed_observation(*, isolation):
        assert isolation == "repeatable read"
        async with session, session.begin():
            yield session

    factory.observation_transaction = managed_observation
    with pytest.raises(RuntimeError, match="source_observation_transaction_required"):
        async with observation.observation_transaction(case.directory, isolation="repeatable read"):
            await session.commit()
            with pytest.raises(InvalidRequestError, match="closed transaction inside context manager"):
                async with session.begin():
                    pytest.fail("replacement transaction exposed")
    assert observation.current_observation(case.directory) is None
    assert case.directory.db._transaction_binding() is None
    assert factory.driver.calls == []


async def test_named_observation_accepts_equal_logical_storage_coordinates(case):
    factory = case.factory
    factory.storage_binding = storage.NamedFHIRSourceStorage("catalog", factory.policy.schema)
    factory.session.get_transaction = lambda: SimpleNamespace(is_active=True)
    case.directory._schema = lambda: "catalog"
    async with observation.observation_transaction(case.directory, isolation="repeatable read"):
        factory.storage_binding = replace(factory.storage_binding)
        assert observation.current_observation(case.directory).storage_binding == factory.storage_binding
    assert factory.driver.calls == []


@pytest.mark.parametrize("transaction", [None, SimpleNamespace(is_active=False)])
async def test_inactive_transaction_refuses_before_body(case, transaction):
    case.factory.session.get_transaction = lambda: transaction
    with pytest.raises(RuntimeError, match="observation_transaction_required"):
        async with observation.observation_transaction(case.directory, isolation="repeatable read"):
            pytest.fail("inactive transaction exposed")
    assert case.directory.db._transaction_binding() is None
    assert case.factory.events[-1][0] == "exit"
    assert case.factory.driver.calls == []


async def test_read_committed_cannot_satisfy_repeatable_read(case):
    async with observation.observation_transaction(case.directory, isolation="read committed"):
        with pytest.raises(RuntimeError, match="isolation_changed"):
            async with observation.observation_transaction(case.directory, isolation="repeatable read"):
                pytest.fail("weaker snapshot exposed")


async def test_closed_observation_transaction_cannot_be_reused(case):
    async with observation.observation_transaction(case.directory, isolation="repeatable read"):
        issued = observation.current_observation(case.directory)
    token = observation._ACTIVE.set(issued)
    try:
        with pytest.raises(RuntimeError, match="observation_transaction_required"):
            async with observation.observation_transaction(case.directory, isolation="repeatable read"):
                pytest.fail("closed transaction exposed")
    finally:
        observation._ACTIVE.reset(token)


@pytest.mark.parametrize("change", ["inactive", "missing", "policy"])
async def test_postcheck_rejects_closed_transaction_or_changed_schema(case, change):
    with pytest.raises(RuntimeError, match="transaction_required|schema_changed"):
        async with observation.observation_transaction(case.directory, isolation="repeatable read"):
            if change == "inactive":
                case.factory.transaction.is_active = False
            elif change == "missing":
                case.factory.session.get_transaction = lambda: None
            else:
                case.factory.policy = GenericSourcePolicy("other", "owner", "publisher", ("reader",), ())
    assert observation._ACTIVE.get() is None
    assert case.directory.db._transaction_binding() is None


async def test_primary_cancellation_preserved_with_postcheck_failure(case):
    original = asyncio.CancelledError("original")
    with pytest.raises(asyncio.CancelledError) as caught:
        async with observation.observation_transaction(case.directory, isolation="repeatable read"):
            case.factory.transaction.is_active = False
            raise original
    assert caught.value is original
    assert isinstance(caught.value.__cause__, BaseExceptionGroup)
    assert caught.value.__cause__.exceptions[0] is original
    assert case.directory.db._transaction_binding() is None


@pytest.mark.parametrize("original", [LookupError("original"), PermissionError("read-only transaction")])
async def test_primary_error_preserved_when_observation_stays_valid(case, original):
    with pytest.raises(type(original)) as caught:
        async with observation.observation_transaction(case.directory, isolation="repeatable read"):
            raise original
    assert caught.value is original
    assert case.directory.db._transaction_binding() is None


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


@pytest.fixture
def private_case(case):
    case.factory.policy = replace(case.factory.policy, schema="private_source")
    case.factory.storage_binding = storage.FHIRSourceStorageBinding("synthetic", "private_source")
    return case


async def test_private_observation_keeps_catalog_and_storage_separate(private_case):
    case = private_case
    assert observation.current_storage_binding("synthetic") is None
    async with observation.observation_transaction(case.directory, isolation="repeatable read"):
        assert observation.current_storage_binding("synthetic") is case.factory.storage_binding
        assert observation.source._PUBLISHER.get() is None and observation.source._OWNER.get() is None
        assert case.factory.driver.calls == []
        with pytest.raises(RuntimeError, match="schema_changed"):
            observation.current_storage_binding("private_source")
    assert observation.current_storage_binding("synthetic") is None


@pytest.mark.parametrize("change", ["catalog_schema", "storage_schema", "locator"])
async def test_private_observation_refuses_changed_coordinates(private_case, change):
    case = private_case
    mutation_by_kind = {
        "catalog_schema": (case.directory, "_schema", lambda: "other_catalog"),
        "storage_schema": (case.factory, "policy", replace(case.factory.policy, schema="other_private")),
        "locator": (
            case.factory,
            "storage_binding",
            SimpleNamespace(catalog_schema="synthetic", storage_schema="private_source"),
        ),
    }
    with pytest.raises(ExceptionGroup, match="source_observation_and_verification_failed") as failures:
        async with observation.observation_transaction(case.directory, isolation="repeatable read"):
            setattr(*mutation_by_kind[change])
            observation.current_storage_binding("synthetic")
    assert len(failures.value.exceptions) == 2
    assert all(
        isinstance(error, RuntimeError) and "source_observation" in str(error) for error in failures.value.exceptions
    )


async def test_named_input_capture_uses_issued_reader_with_another_supplied_session(case, monkeypatch):
    from process import provider_directory_generic_source_provision as provision

    factory = case.factory
    factory.storage_binding = storage.NamedFHIRSourceStorage("catalog", factory.policy.schema)
    case.directory._schema = lambda: "catalog"
    factory.session.get_transaction = lambda: SimpleNamespace(is_active=True)
    factory.observe_source_callable_capability = AsyncMock(return_value=())
    proof_by_field = {"data": "verified"}
    capture = AsyncMock(return_value=proof_by_field)
    monkeypatch.setattr(provision, "capture_protected_fhir_input", capture)
    async with observation.observation_transaction(case.directory, isolation="repeatable read"):
        assert await observation.capture_fhir_input(case.directory, object(), factory.policy.schema) is proof_by_field
    capture.assert_awaited_once_with(factory.driver, source_policy=factory.policy)
    factory.observe_source_callable_capability.assert_awaited_once_with(factory.driver, factory.policy)
    assert factory.driver.calls == []
