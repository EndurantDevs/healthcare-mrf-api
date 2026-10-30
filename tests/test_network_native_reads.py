# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native aggregate results remain opaque until their original inventory is checked."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import ServiceUnavailable

from api import network_read_execution as execution
from api.network_address_scope import network_address_read_scope, scoped_address_relation_sql
from process.network_read_identity import _BOUND_NAMES_SQL
from tests.test_network_bound_names import _PIN, _installable_session
from tests.test_network_read_execution import _manifest


class _OpaqueBounds(dict):
    def __init__(self, events):
        super().__init__(count=1, bytes=24, rows_json=json.dumps([{"npi": 1000000491}]))
        self.events = events

    def __getitem__(self, name):
        assert self.events[-1] in {"guard", "decode"}
        self.events.append("decode")
        return super().__getitem__(name)


async def _reader(*, mutation=None, refuse=False):
    session, driver, _ = _installable_session()
    await execution.install_network_read_inventory(session, (_PIN,))
    events = []

    async def fetchrow(query, *_parameters):
        if query == _BOUND_NAMES_SQL:
            events.append("guard")
            match mutation:
                case "transaction":
                    session.get_transaction = lambda: object()
                case "driver":
                    connection = SimpleNamespace(
                        get_raw_connection=AsyncMock(
                            return_value=SimpleNamespace(driver_connection=SimpleNamespace(get_server_pid=lambda: 42))
                        )
                    )
                    session.connection = AsyncMock(return_value=connection)
                case "pid":
                    driver.get_server_pid = lambda: 43
                case "inactive":
                    driver.is_in_transaction = lambda: False
            return {"backend_matches": True, "relation_count": 1, "valid": not refuse}
        assert query.startswith("WITH source_rows AS MATERIALIZED (")
        events.append("sql")
        return _OpaqueBounds(events)

    driver.fetchrow = AsyncMock(side_effect=fetchrow)
    return session, driver, events


@pytest.mark.parametrize("refuse", [False, True])
async def test_native_aggregate_guard_precedes_bounds_and_json(refuse):
    session, driver, events = await _reader(refuse=refuse)
    operation = execution.bounded_network_read_rows(session, "SELECT npi FROM held_snapshot.provider_profile")
    if refuse:
        with pytest.raises(ServiceUnavailable):
            await operation
        assert events == ["sql", "guard"]
    else:
        assert await operation == [{"npi": 1000000491}]
        assert events[:3] == ["sql", "guard", "decode"]
    assert driver.fetchrow.await_count == 2


@pytest.mark.parametrize("mutation", ["transaction", "driver", "pid", "inactive"])
async def test_native_guard_rechecks_original_context_before_decode(mutation):
    session, _driver, events = await _reader(mutation=mutation)
    with pytest.raises(ServiceUnavailable):
        await execution.bounded_network_read_rows(session, "SELECT npi FROM held_snapshot.provider_profile")
    assert events == ["sql", "guard"]


async def test_native_seam_requires_installed_inventory_before_data_sql():
    session, driver, _ = _installable_session()
    driver.fetchrow.reset_mock()
    with pytest.raises(ServiceUnavailable):
        await execution.bounded_network_read_rows(session, "SELECT 1")
    driver.fetchrow.assert_not_awaited()


async def test_native_seam_keeps_executed_address_markers(monkeypatch):
    session, driver, _events = await _reader()
    consumed = AsyncMock()
    monkeypatch.setattr(execution, "verify_consumed_network_relations", consumed)
    with network_address_read_scope(_manifest(), (42,)):
        relation = scoped_address_relation_sql("mrf.entity_address_unified")
        assert await execution.bounded_network_read_rows(session, f"SELECT npi FROM {relation} office")
    consumed.assert_awaited_once()
    assert consumed.await_args.args[0] is driver
    assert consumed.await_args.args[1][0].table_oid == 20


async def test_native_data_cancellation_is_not_converted_or_decoded():
    session, driver, events = await _reader()
    driver.fetchrow.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await execution.bounded_network_read_rows(session, "SELECT npi FROM held_snapshot.provider_profile")
    assert not events
