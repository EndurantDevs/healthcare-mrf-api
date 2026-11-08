# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Buffered rows stay private across native context drift and guard failures."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sanic.exceptions import ServiceUnavailable

from api import network_read_execution as execution
from process.network_compact_layout import CompactAddressRelation
from tests.test_network_bound_names import _installable_session
from tests.test_network_read_execution import _session

PIN = CompactAddressRelation("held", "office", 23)


@pytest.mark.parametrize("phase", ["acquire", "sql", "guard", "reacquire"])
@pytest.mark.parametrize("change", ["transaction", "driver", "pid", "inactive"])
async def test_context_drift_never_exposes_buffered_rows(monkeypatch, phase, change):
    events = []
    session, driver, buffered = _session(events)
    context = SimpleNamespace(driver=driver, acquisitions=0)

    def mutate():
        if change == "transaction":
            session.get_transaction = lambda: object()
        elif change == "driver":
            context.driver = SimpleNamespace(get_server_pid=lambda: 42, is_in_transaction=lambda: True)
        elif change == "pid":
            driver.get_server_pid = lambda: 43
        else:
            driver.is_in_transaction = lambda: False

    async def connection():
        context.acquisitions += 1
        if phase == "acquire" or phase == "reacquire" and context.acquisitions == 2:
            mutate()
        return SimpleNamespace(
            get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=context.driver))
        )

    async def execute(*_arguments):
        events.append("sql")
        if phase == "sql":
            mutate()
        return buffered

    async def guard(*_arguments, **_options):
        events.append("guard")
        if phase == "guard":
            mutate()

    session.connection = AsyncMock(side_effect=connection)
    session.execute = AsyncMock(side_effect=execute)
    monkeypatch.setattr(execution, "verify_bound_network_names", guard)
    with execution.network_read_name_inventory(driver, (PIN,)):
        with pytest.raises(ServiceUnavailable):
            await execution.execute_network_read(session, "SELECT payload FROM ONLY held.office")
    if phase == "acquire":
        session.execute.assert_not_awaited()
        buffered.close.assert_not_called()
    else:
        buffered.close.assert_called_once()


@pytest.mark.parametrize("failure", [RuntimeError("guard failed"), asyncio.CancelledError()])
@pytest.mark.parametrize("cleanup_failure", [None, RuntimeError("cleanup failed"), asyncio.CancelledError()])
async def test_every_post_read_failure_closes_rows_and_preserves_primary_error(monkeypatch, failure, cleanup_failure):
    session, driver, buffered = _session([])
    if cleanup_failure is not None:
        buffered.close = Mock(side_effect=cleanup_failure)
    monkeypatch.setattr(execution, "verify_bound_network_names", AsyncMock(side_effect=failure))
    with execution.network_read_name_inventory(driver, (PIN,)):
        with pytest.raises(type(failure)) as caught:
            await execution.execute_network_read(session, "SELECT payload FROM ONLY held.office")
    assert caught.value is failure
    buffered.close.assert_called_once()
    if cleanup_failure is not None:
        assert "Buffered result cleanup failed: " + type(cleanup_failure).__name__ in failure.__notes__


async def test_inventory_install_cannot_adopt_a_replaced_transaction_during_acquisition():
    session, driver, _ = _installable_session()
    connection = await session.connection()

    async def rotated():
        transaction = object()
        session.get_transaction = lambda: transaction
        return connection

    session.connection = AsyncMock(side_effect=rotated)
    with pytest.raises(ServiceUnavailable):
        await execution.install_network_read_inventory(session, (PIN,))
    driver.fetchrow.assert_not_awaited()
    assert not execution.has_network_read_inventory(session)
