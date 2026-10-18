# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real retained-owner composition with the existing explicitly mocked transport."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from process import provider_directory_generic_source_policy as source
from process import provider_directory_owned_wal_transaction as owned
from process import provider_directory_profile_control_custody as control_custody
from process.provider_directory_profile_source_wal import source_owned_wal_transaction
from tests.test_provider_directory_profile_source_wal import IssuedFactory, JoinedSession
from tests.test_provider_directory_profile_source_wal import fixture as fixture


@asynccontextmanager
async def _composed_owner(database, connection, via_source):
    if not via_source:
        async with owned.registry_retained_wal_transaction(connection) as outcome:
            async with owned.bind_retained_wal_transaction(database, outcome):
                yield outcome
        return
    token = source._FACTORY.set(IssuedFactory(database, connection))
    try:
        async with source_owned_wal_transaction(SimpleNamespace(db=database), isolation="serializable") as outcome:
            yield outcome
    finally:
        source._FACTORY.reset(token)


def _causal_errors(error):
    pending, seen = [error], set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        yield current
        if isinstance(current, BaseExceptionGroup):
            pending.extend(current.exceptions)
        pending.extend(cause for cause in (current.__cause__, current.__context__) if cause is not None)


def _assert_rolled_back(database, connection, outcome):
    assert not outcome.is_committed and outcome.measurement is None
    assert outcome.commit_state in {"rolled_back", "not_attempted"}
    assert outcome.status == "accounting_incomplete" and outcome.cleanup_complete
    assert "commit" not in connection.events and connection.events.count("rollback") == 1
    assert connection.closed and connection.driver.closed
    assert database._transaction_binding() is None and owned.current_owned_wal_transaction(database) is None
    assert owned._RETAINED_OWNED_TRANSACTION.get() is None


@pytest.mark.parametrize("via_source", [False, True])
@pytest.mark.parametrize("rollback_failed", [False, True])
async def test_body_cancel_keeps_close_and_rollback_causes(fixture, monkeypatch, via_source, rollback_failed):
    database, connection = fixture
    primary = asyncio.CancelledError("synthetic original body cancellation")
    close_error = OSError("synthetic wrapper close failure")
    rollback_error = OSError("synthetic retained rollback failure")

    async def close(session):
        session.active = False
        connection.events.append("session_close")
        raise close_error

    async def rollback():
        connection.events.append("rollback")
        raise rollback_error

    monkeypatch.setattr(JoinedSession, "close", close)
    if rollback_failed:
        monkeypatch.setattr(connection, "rollback", rollback)
    with pytest.raises(asyncio.CancelledError) as caught:
        async with _composed_owner(database, connection, via_source) as outcome:
            raise primary
    assert isinstance(caught.value, owned.OwnedWalTransactionCancelled) and caught.value.outcome is outcome
    causes = tuple(_causal_errors(caught.value))
    assert any(cause is primary for cause in causes)
    assert any(cause is close_error for cause in causes)
    assert any(cause is rollback_error for cause in causes) is rollback_failed
    _assert_rolled_back(database, connection, outcome)


@pytest.mark.parametrize("rollback_failed", [False, True])
async def test_control_body_cancel_keeps_cleanup_causes(fixture, monkeypatch, rollback_failed):
    database, connection = fixture
    primary = asyncio.CancelledError("synthetic original control cancellation")
    close_error = OSError("synthetic control wrapper close failure")
    rollback_error = OSError("synthetic control rollback failure")

    async def close(session):
        session.active = False
        connection.events.append("session_close")
        raise close_error

    async def rollback():
        connection.events.append("rollback")
        raise rollback_error

    monkeypatch.setattr(JoinedSession, "close", close)
    if rollback_failed:
        monkeypatch.setattr(connection, "rollback", rollback)
    fhir = SimpleNamespace(db=database, profile_owned_wal=owned)
    group_by_field = dict(
        identity=("capacity_admission", "synthetic"),
        original_outcome=None,
        outcome=None,
        body_complete=False,
        failure=None,
    )
    token = source._FACTORY.set(IssuedFactory(database, connection))
    try:
        with pytest.raises(asyncio.CancelledError) as caught:
            async with control_custody._owned_control_group(fhir, group_by_field):
                raise primary
        outcome = group_by_field["original_outcome"]
        assert type(outcome) is owned.OwnedWalTransaction and group_by_field["outcome"] is outcome
        assert group_by_field["failure"] is caught.value
        causes = tuple(_causal_errors(caught.value))
        assert any(cause is primary for cause in causes) and any(cause is close_error for cause in causes)
        assert any(cause is rollback_error for cause in causes) is rollback_failed
        _assert_rolled_back(database, connection, outcome)
    finally:
        source._FACTORY.reset(token)


async def test_cancel_cleanup_failure_never_claims_retirement(fixture, monkeypatch):
    database, connection = fixture
    primary = asyncio.CancelledError("synthetic original cancellation")
    close_error = OSError("synthetic wrapper close failure")
    retirement_error = OSError("synthetic native retirement failure")

    async def close(session):
        session.active = False
        connection.events.append("session_close")
        raise close_error

    def terminate():
        raise retirement_error

    monkeypatch.setattr(JoinedSession, "close", close)
    original_terminate = connection.driver.terminate
    monkeypatch.setattr(connection.driver, "terminate", terminate)
    try:
        with pytest.raises(owned.OwnedWalTransactionCancelled) as caught:
            async with _composed_owner(database, connection, False) as outcome:
                raise primary
        assert caught.value.outcome is outcome
        assert not outcome.cleanup_complete and not outcome.is_committed and outcome.measurement is None
        assert not connection.closed and not connection.driver.closed
        causes = tuple(_causal_errors(caught.value))
        assert all(any(cause is expected for cause in causes) for expected in (primary, close_error, retirement_error))
        assert database._transaction_binding() is None and owned.current_owned_wal_transaction(database) is None
    finally:
        original_terminate()
        await connection.invalidate()
        await connection.close()
    assert not outcome.cleanup_complete
