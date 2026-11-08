# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""An isolation request cannot change the backend or the issued SOURCE owner."""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from process import provider_directory_generic_source_policy as source
from process import provider_directory_owned_wal_transaction as owned
from process import provider_directory_profile_source_wal as source_wal
from tests.test_provider_directory_profile_source_wal import IssuedFactory
from tests.test_provider_directory_profile_source_wal import fixture as fixture


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["replacement", "ignored"])
async def test_unapplied_isolation_never_begins_source_work(fixture, monkeypatch, change):
    database, connection = fixture
    original = connection.execution_options
    requests = []

    async def set_isolation(*, isolation_level):
        requests.append(isolation_level)
        if len(requests) == 1:
            return object() if change == "replacement" else connection
        return await original(isolation_level=isolation_level)

    monkeypatch.setattr(connection, "execution_options", set_isolation)
    factory = IssuedFactory(database, connection)
    token = source._FACTORY.set(factory)
    try:
        message = "isolation_connection_changed" if change == "replacement" else "isolation_changed"
        with pytest.raises(RuntimeError, match=message):
            async with source_wal.source_owned_wal_transaction(SimpleNamespace(db=database), isolation="serializable"):
                pytest.fail("Unapplied isolation reached the transaction body")
        assert requests == ["SERIALIZABLE", "READ COMMITTED"]
        assert "begin" not in connection.events and "commit" not in connection.events
        assert connection.adapter.isolation_level == "read_committed"
        assert not connection.closed and not connection.driver.closed
        assert database._transaction_binding() is None
    finally:
        source._FACTORY.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("when", ["entry", "exit"])
async def test_changed_source_factory_cannot_commit_or_keep_the_backend(fixture, monkeypatch, when):
    database, connection = fixture
    factory = IssuedFactory(database, connection)
    original = factory.bind_existing_transaction

    @asynccontextmanager
    async def bind(*args, **kwargs):
        async with original(*args, **kwargs):
            if when == "entry":
                source._FACTORY.set(object())
            yield
            if when == "exit":
                source._FACTORY.set(object())

    monkeypatch.setattr(factory, "bind_existing_transaction", bind)
    token = source._FACTORY.set(factory)
    has_entered = False
    try:
        with pytest.raises(owned.OwnedWalTransactionError) as caught:
            async with source_wal.source_owned_wal_transaction(SimpleNamespace(db=database)):
                has_entered = True
        outcome = caught.value.outcome
        assert has_entered is (when == "exit")
        assert not outcome.is_committed and outcome.measurement is None
        assert "commit" not in connection.events and connection.events.count("rollback") == 1
        assert connection.closed and connection.driver.closed
        assert database._transaction_binding() is None and owned.current_owned_wal_transaction(database) is None
    finally:
        source._FACTORY.reset(token)
