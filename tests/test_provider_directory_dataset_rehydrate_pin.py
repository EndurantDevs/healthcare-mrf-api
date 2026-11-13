# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named reader scope follows the transaction and closes on every exit."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_dataset_rehydrate_pin as pin
from process.provider_directory_dataset_rehydrate_types import DatasetRehydrationError


@pytest.mark.asyncio
async def test_reader_facade_allows_active_callbacks_and_refuses_expired_scope():
    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(return_value=SimpleNamespace(scalar=lambda: 17)),
    )
    reader = pin._SourceReader(session, "fixture", {"root": {"schema": "fixture", "name": "child"}})
    assert reader.relation("root") == '"fixture"."child"'
    assert await asyncio.create_task(reader.scalar("SELECT 17")) == 17
    with pytest.raises(DatasetRehydrationError, match="relation_not_pinned"):
        reader.relation("other")
    reader.deactivate()
    with pytest.raises(DatasetRehydrationError, match="reader_transaction_ended"):
        await reader.scalar("SELECT 17")
    assert session.execute.await_count == 1


@pytest.mark.asyncio
async def test_reader_facade_refuses_ended_transaction():
    reader = pin._SourceReader(SimpleNamespace(in_transaction=lambda: False), "fixture", {})
    with pytest.raises(DatasetRehydrationError, match="reader_transaction_ended"):
        await reader.first("SELECT 1")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_type", [None, RuntimeError, asyncio.CancelledError])
async def test_public_pin_deactivates_before_session_exit(monkeypatch, failure_type):
    active_pins = []
    closed_states = []
    reader_session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    selected_scope = object()
    binding_by_field = {"tables": {pin.storage.RESOURCE: {"schema": "generation", "name": "details"}}}

    @asynccontextmanager
    async def source_transaction(database, *, isolation):
        assert isolation == "repeatable read"
        yield

    @asynccontextmanager
    async def reader_context(*, independent):
        assert independent is True
        try:
            yield reader_session
        finally:
            closed_states.append(not active_pins[0].reader.is_active)

    database = SimpleNamespace(reader_session=reader_context)
    runtime = SimpleNamespace(database=database, schema="catalog")
    monkeypatch.setattr(pin, "source_transaction", source_transaction)
    monkeypatch.setattr(pin, "_capture_source", AsyncMock(return_value=(selected_scope, object(), binding_by_field)))
    monkeypatch.setattr(pin, "_verify_pinned_header", AsyncMock())

    async def read_generation():
        async with pin.pinned_current_source(runtime, object(), directory=object()) as selected:
            active_pins.append(selected)
            assert selected.scope is selected_scope
            assert selected.source_relation(object()) == '"generation"."details"'
            if failure_type is not None:
                raise failure_type("synthetic exit")

    if failure_type is None:
        await read_generation()
    else:
        with pytest.raises(failure_type, match="synthetic exit"):
            await read_generation()
    assert closed_states == [True]
    assert (
        str(reader_session.execute.await_args.args[0])
        == 'LOCK TABLE "generation"."details" IN ACCESS SHARE MODE NOWAIT'
    )
