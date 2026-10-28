# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native receipt decoding preserves values while sharing the event loop."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from process import network_cms_registry_address_capture as capture


def _native_session(records):
    attributes = [
        SimpleNamespace(name=name, type=SimpleNamespace(name=kind))
        for name, kind in (("payload", "jsonb"), ("metadata", "json"), ("label", "text"))
    ]
    prepared = SimpleNamespace(get_attributes=lambda: attributes, fetch=AsyncMock(return_value=records))
    connection = SimpleNamespace(prepare=AsyncMock(return_value=prepared), is_in_transaction=lambda: True)
    return capture.NativeReceiptSession(connection), prepared


@pytest.mark.asyncio
async def test_native_receipt_decode_preserves_binds_values_and_mapping_interface():
    payload_by_field = {"nested": [1, None]}
    session, prepared = _native_session(
        [{"payload": payload_by_field, "metadata": '{"count":2}', "label": '{"count":3}'}]
    )
    rows = await session.execute(text("SELECT :selected"), {"selected": "synthetic"})
    prepared.fetch.assert_awaited_once_with("synthetic")
    assert rows.mappings() is rows
    assert rows.one_or_none() == {"payload": payload_by_field, "metadata": {"count": 2}, "label": '{"count":3}'}
    assert rows[0]["payload"] is payload_by_field
    assert session.in_transaction()
    assert await capture.native_driver(session) is session.native_connection


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_native_receipt_decode_yields_before_finishing_a_large_page(monkeypatch, cancel):
    decoded_values = []
    original_loads = json.loads

    def observed_loads(value):
        decoded_values.append(value)
        return original_loads(value)

    monkeypatch.setattr(capture.json, "loads", observed_loads)
    session, _prepared = _native_session([{"payload": str(number)} for number in range(1000)])
    task = asyncio.create_task(session.execute(text("SELECT payload")))
    await asyncio.sleep(0)
    assert len(decoded_values) == 256
    if cancel:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(decoded_values) == 256
    else:
        for expected_count in (512, 768):
            await asyncio.sleep(0)
            assert len(decoded_values) == expected_count
        rows = await task
        assert rows == [{"payload": number} for number in range(1000)]


@pytest.mark.asyncio
@pytest.mark.parametrize("column", ["payload", "metadata"])
async def test_native_receipt_decode_rejects_invalid_json(column):
    session, _prepared = _native_session([{column: "invalid json"}])
    with pytest.raises(json.JSONDecodeError):
        await session.execute(text("SELECT payload"))
