# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retry only a closed transaction's first queue-row lock, never publication work."""

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, call
from uuid import UUID

import asyncpg
import pytest

from process import registry_publication_execution as execution

pytestmark = pytest.mark.asyncio
REQUEST_ID = UUID("11111111-1111-4111-8111-111111111111")
LEASE_ID = UUID("22222222-2222-4222-8222-222222222222")
NAMESPACE = '"registry"'


class _Connection:
    def __init__(self, *, rollback_stays_active=False, commit_error=None):
        self.active = False
        self.rollback_stays_active = rollback_stays_active
        self.commit_error = commit_error
        self.attempts = self.rollbacks = self.commits = 0

    def is_in_transaction(self):
        return self.active

    @asynccontextmanager
    async def transaction(self, *, isolation):
        assert isolation == "repeatable_read" and not self.active
        self.attempts += 1
        self.active = True
        try:
            yield
        except BaseException:
            self.rollbacks += 1
            self.active = self.rollback_stays_active
            raise
        else:
            self.commits += 1
            self.active = False
            if self.commit_error is not None:
                raise self.commit_error


@pytest.fixture
def request_lock(monkeypatch):
    lock = AsyncMock(return_value={"request_id": REQUEST_ID})
    monkeypatch.setattr(execution, "_request", lock)
    return lock


@pytest.mark.parametrize("failures", [1, 2])
async def test_initial_serialization_failures_retry_before_yielding_once(request_lock, failures):
    connection = _Connection()
    locked_by_name = {"request_id": REQUEST_ID}
    request_lock.side_effect = [asyncpg.SerializationError("renewed row") for _ in range(failures)] + [locked_by_name]
    body = AsyncMock()
    async with execution._locked_request_transaction(connection, REQUEST_ID, LEASE_ID, NAMESPACE) as record:
        assert record is locked_by_name and connection.active
        await body()
    body.assert_awaited_once()
    assert request_lock.await_args_list == [call(connection, REQUEST_ID, LEASE_ID, NAMESPACE, lock=True)] * (
        failures + 1
    )
    assert (connection.attempts, connection.rollbacks, connection.commits) == (failures + 1, failures, 1)
    assert not connection.active


async def test_third_lock_failure_exhausts_retries_and_rolls_back(request_lock):
    connection = _Connection()
    failures = [asyncpg.SerializationError("renewed row") for _ in range(3)]
    request_lock.side_effect = failures
    with pytest.raises(asyncpg.SerializationError) as caught:
        async with execution._locked_request_transaction(connection, REQUEST_ID, LEASE_ID, NAMESPACE):
            pytest.fail("An unacquired lock cannot yield publication work")
    assert caught.value is failures[-1]
    assert request_lock.await_count == 3
    assert (connection.attempts, connection.rollbacks, connection.commits) == (3, 3, 0)
    assert not connection.active


async def test_existing_caller_transaction_is_refused_without_locking(request_lock):
    connection = _Connection()
    connection.active = True
    with pytest.raises(ValueError, match="registry_execution_requires_phase_owned_transactions"):
        async with execution._locked_request_transaction(connection, REQUEST_ID, LEASE_ID, NAMESPACE):
            pytest.fail("A nested caller transaction cannot be replayed")
    request_lock.assert_not_awaited()
    assert (connection.attempts, connection.rollbacks, connection.commits) == (0, 0, 0)
    assert connection.active


@pytest.mark.parametrize("failure_type", [asyncpg.DeadlockDetectedError, RuntimeError, asyncio.CancelledError])
async def test_other_prelock_errors_and_cancellation_are_not_retried(request_lock, failure_type):
    connection = _Connection()
    failure = failure_type("stopped before lock")
    request_lock.side_effect = failure
    with pytest.raises(failure_type) as caught:
        async with execution._locked_request_transaction(connection, REQUEST_ID, LEASE_ID, NAMESPACE):
            pytest.fail("Failed lock acquisition cannot yield publication work")
    assert caught.value is failure
    request_lock.assert_awaited_once()
    assert (connection.attempts, connection.rollbacks, connection.commits) == (1, 1, 0)
    assert not connection.active


@pytest.mark.parametrize("phase", ["body", "commit"])
@pytest.mark.parametrize("failure_type", [asyncpg.SerializationError, asyncio.CancelledError])
async def test_body_and_commit_failures_never_replay_publication(request_lock, phase, failure_type):
    failure = failure_type("stopped after lock")
    connection = _Connection(commit_error=failure if phase == "commit" else None)
    body = AsyncMock(side_effect=failure if phase == "body" else None)
    with pytest.raises(failure_type) as caught:
        async with execution._locked_request_transaction(connection, REQUEST_ID, LEASE_ID, NAMESPACE):
            await body()
    assert caught.value is failure
    body.assert_awaited_once()
    request_lock.assert_awaited_once()
    assert connection.attempts == 1
    assert (connection.rollbacks, connection.commits) == ((1, 0) if phase == "body" else (0, 1))
    assert not connection.active


async def test_serialization_failure_cannot_retry_while_rollback_leaves_transaction_active(request_lock):
    connection = _Connection(rollback_stays_active=True)
    failure = asyncpg.SerializationError("rollback did not close transaction")
    request_lock.side_effect = failure
    with pytest.raises(asyncpg.SerializationError) as caught:
        async with execution._locked_request_transaction(connection, REQUEST_ID, LEASE_ID, NAMESPACE):
            pytest.fail("An active failed transaction cannot yield publication work")
    assert caught.value is failure
    request_lock.assert_awaited_once()
    assert (connection.attempts, connection.rollbacks, connection.commits) == (1, 1, 0)
    assert connection.active
