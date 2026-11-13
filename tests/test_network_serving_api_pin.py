# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One named publication pin and logical context for each configured API snapshot."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest

from api import network_serving_api_read as pins
from process.network_published_api_pin import published_api_pin_statement
from process.network_serving_read import NetworkServingReadUnavailable
from process.registry_network_catalog_read import _PINNED_READ
from tests.test_network_published_api_pin import published_entry as published_entry

_SCHEMA = "registry_control"


@pytest.fixture
def pin_reader(monkeypatch, published_entry):
    driver = SimpleNamespace(fetchrow=AsyncMock(return_value=published_entry))
    session = object()
    monkeypatch.setattr(pins, "_entry_snapshot", lambda supplied: (supplied is session, None, None))
    reader = AsyncMock(return_value=driver)
    monkeypatch.setattr(pins, "_read_driver", reader)
    return SimpleNamespace(session=session, driver=driver, reader=reader)


async def test_published_pin_reads_once_with_the_requested_logical_generation(pin_reader):
    manifest, approved, relation = await pins.resolve_api_network_pin(
        pin_reader.session, generation_id=7, control_schema=_SCHEMA
    )
    pin_reader.reader.assert_awaited_once_with(pin_reader.session)
    pin_reader.driver.fetchrow.assert_awaited_once_with(published_api_pin_statement(_SCHEMA), 7)
    assert manifest.generation_id == 7 and approved.approved_revision == 4
    assert relation.schema_name == _SCHEMA and relation.table_name == "registry_approved_record"


@pytest.mark.parametrize("generation", [False, True, 0, -1, 2**63, 3.0, "3"])
async def test_invalid_generation_refuses_before_sql(pin_reader, generation):
    with pytest.raises(NetworkServingReadUnavailable):
        await pins.resolve_api_network_pin(pin_reader.session, generation_id=generation, control_schema=_SCHEMA)
    pin_reader.driver.fetchrow.assert_not_awaited()


async def test_inactive_snapshot_refuses_before_driver_access(pin_reader):
    with pytest.raises(NetworkServingReadUnavailable, match="active transaction"):
        await pins.resolve_api_network_pin(object(), control_schema=_SCHEMA)
    pin_reader.reader.assert_not_awaited()


@pytest.mark.parametrize(
    "failure",
    [
        asyncpg.UndefinedTableError("missing"),
        asyncpg.InsufficientPrivilegeError("denied"),
        RuntimeError("read"),
        asyncio.CancelledError(),
    ],
)
async def test_database_failure_propagates_without_additional_queries(pin_reader, failure):
    pin_reader.driver.fetchrow.side_effect = failure
    previous = (object(), "previous_control")
    token = _PINNED_READ.set(previous)
    try:
        with pytest.raises(type(failure)) as caught:
            async with pins.api_catalog_read_snapshot(pin_reader.session, generation_id=7, control_schema=_SCHEMA):
                raise AssertionError("an unavailable published query cannot enter the read scope")
        assert caught.value is failure and _PINNED_READ.get() is previous
    finally:
        _PINNED_READ.reset(token)
    assert pin_reader.driver.fetchrow.await_count == 1


async def test_catalog_snapshot_restores_nested_contexts(pin_reader):
    previous = _PINNED_READ.get()
    async with pins.api_catalog_read_snapshot(pin_reader.session, generation_id=7, control_schema=_SCHEMA) as (
        outer,
        _outer_relation,
    ):
        outer_pin = _PINNED_READ.get()
        async with pins.api_catalog_read_snapshot(pin_reader.session, generation_id=7, control_schema=_SCHEMA) as (
            inner,
            _inner_relation,
        ):
            assert inner == outer and inner is not outer
            assert _PINNED_READ.get() == (inner, _SCHEMA)
        assert _PINNED_READ.get() is outer_pin
    assert _PINNED_READ.get() is previous
    assert pin_reader.driver.fetchrow.await_count == 2


@pytest.mark.parametrize("failure", [None, ValueError("body"), asyncio.CancelledError()])
async def test_catalog_context_restores_the_previous_logical_pin(pin_reader, failure):
    previous = (object(), "previous_control")
    token = _PINNED_READ.set(previous)
    try:

        async def read():
            async with pins.api_catalog_read_snapshot(pin_reader.session, generation_id=7, control_schema=_SCHEMA) as (
                manifest,
                relation,
            ):
                assert _PINNED_READ.get() == (manifest, _SCHEMA)
                assert manifest._api_approved_source.approved_revision == 4
                assert relation.schema_name == _SCHEMA
                if failure is not None:
                    raise failure

        if failure is None:
            await read()
        else:
            with pytest.raises(type(failure)):
                await read()
        assert _PINNED_READ.get() is previous
    finally:
        _PINNED_READ.reset(token)
