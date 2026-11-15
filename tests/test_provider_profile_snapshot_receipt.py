# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Optional provider reads use the owning SQLAlchemy savepoint."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import asyncpg
import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy.exc import DBAPIError, InterfaceError

from api.provider_profile_snapshot import provider_optional_read, provider_read_savepoint


class Session:
    def __init__(self, cleanup_failure=None):
        self.transaction = object()
        self.cleanup_failure = cleanup_failure
        self.rollbacks = self.commits = 0

    @asynccontextmanager
    async def begin_nested(self):
        try:
            yield
        except BaseException:
            self.rollbacks += 1
            if self.cleanup_failure is not None:
                raise self.cleanup_failure
            raise
        else:
            self.commits += 1


async def test_optional_statement_error_rolls_back_and_preserves_outer_transaction():
    session = Session()
    transaction = session.transaction
    logger = SimpleNamespace(debug=Mock())
    async with provider_optional_read(session, logger):
        raise asyncpg.DivisionByZeroError("optional count failed")
    assert session.transaction is transaction
    assert session.rollbacks == 1 and session.commits == 0
    logger.debug.assert_called_once()


@pytest.mark.parametrize("failure", [ServiceUnavailable("unavailable"), asyncio.CancelledError()])
async def test_terminal_failure_reaches_the_transaction_owner(failure):
    session = Session()
    with pytest.raises(type(failure)) as caught:
        async with provider_optional_read(session, SimpleNamespace(debug=Mock())):
            raise failure
    assert caught.value is failure and session.rollbacks == 1


async def test_failed_savepoint_cleanup_prevents_optional_fallback():
    session = Session(RuntimeError("rollback failed"))
    with pytest.raises(ServiceUnavailable):
        async with provider_optional_read(session, SimpleNamespace(debug=Mock())):
            raise asyncpg.DivisionByZeroError("optional count failed")


async def test_cancellation_preserves_primary_error_if_savepoint_cleanup_fails():
    session = Session(RuntimeError("rollback failed"))
    failure = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError) as caught:
        async with provider_read_savepoint(session):
            raise failure
    assert caught.value is failure
    assert failure.__notes__ == ["Optional savepoint cleanup failed: RuntimeError"]


async def test_success_closes_only_the_nested_transaction():
    session = Session()
    transaction = session.transaction
    async with provider_read_savepoint(session):
        assert session.transaction is transaction
    assert session.transaction is transaction and session.commits == 1 and session.rollbacks == 0


@pytest.mark.parametrize(
    "failure",
    [
        asyncpg.ConnectionDoesNotExistError("closed"),
        asyncpg.CannotConnectNowError("unavailable"),
        DBAPIError(None, None, asyncpg.DivisionByZeroError("division"), connection_invalidated=True),
        InterfaceError(None, None, RuntimeError("closed")),
    ],
)
async def test_lost_or_invalidated_connection_never_becomes_optional_fallback(failure):
    session = Session()
    with pytest.raises(ServiceUnavailable):
        async with provider_optional_read(session, SimpleNamespace(debug=Mock())):
            raise failure
    assert session.rollbacks == 1
