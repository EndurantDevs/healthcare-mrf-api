# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded native reads use the configured SQLAlchemy transaction."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest
from sanic.exceptions import ServiceUnavailable

from api.network_read_execution import _read_driver, bounded_network_read_rows, qualified_relation_sql


def _session(driver, *, transaction=object()):
    raw = SimpleNamespace(driver_connection=driver)
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=raw))
    return SimpleNamespace(
        get_transaction=lambda: transaction,
        connection=AsyncMock(return_value=connection),
    )


async def test_native_query_interface_uses_the_configured_session():
    driver = SimpleNamespace(fetchrow=AsyncMock())
    session = _session(driver)
    assert await _read_driver(session) is driver
    session.connection.assert_awaited_once_with()
    driver.fetchrow.assert_not_awaited()


async def test_native_query_requires_an_active_transaction():
    session = _session(object(), transaction=None)
    with pytest.raises(ServiceUnavailable):
        await _read_driver(session)
    session.connection.assert_not_awaited()


async def test_native_rows_keep_bound_parameters_and_response_bounds():
    driver = SimpleNamespace(
        fetchrow=AsyncMock(return_value={"count": 1, "bytes": 12, "rows_json": json.dumps([{"npi": "1"}])})
    )
    rows = await bounded_network_read_rows(_session(driver), 'SELECT * FROM "published"."provider" WHERE npi=$1', "1")
    assert rows == [{"npi": "1"}]
    query, argument = driver.fetchrow.await_args.args
    assert argument == "1" and "count<=5000 AND bytes<=8388608" in query


@pytest.mark.parametrize(
    "result",
    [
        None,
        {"count": 5001, "bytes": 12, "rows_json": None},
        {"count": 1, "bytes": 8388609, "rows_json": None},
        {"count": 1, "bytes": 2, "rows_json": "[]"},
    ],
)
async def test_native_rows_reject_unbounded_or_incomplete_results(result):
    driver = SimpleNamespace(fetchrow=AsyncMock(return_value=result))
    with pytest.raises(ValueError):
        await bounded_network_read_rows(_session(driver), "SELECT npi FROM published.provider")


@pytest.mark.parametrize("failure", [asyncpg.UndefinedTableError("missing"), asyncio.CancelledError()])
async def test_native_query_errors_and_cancellation_reach_the_transaction_owner(failure):
    driver = SimpleNamespace(fetchrow=AsyncMock(side_effect=failure))
    with pytest.raises(type(failure)) as caught:
        await bounded_network_read_rows(_session(driver), "SELECT npi FROM published.provider")
    assert caught.value is failure


def test_relation_sql_uses_validated_schema_qualified_names():
    assert qualified_relation_sql("published_snapshot", "provider") == '"published_snapshot"."provider"'
    with pytest.raises(ValueError):
        qualified_relation_sql("published;DROP SCHEMA published", "provider")
