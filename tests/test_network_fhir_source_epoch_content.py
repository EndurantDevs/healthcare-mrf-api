# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical epoch hashing preserves the original task and failure boundary."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import network_fhir_source_epoch as epoch


@pytest.mark.asyncio
async def test_canonical_content_keeps_source_capabilities_on_original_task(monkeypatch):
    """Already canonical producers keep the existing body without a savepoint."""
    owner = asyncio.current_task()
    source = object()

    async def chunks(connection, schema, source_input):
        assert asyncio.current_task() is owner and source_input is source
        assert schema == "synthetic"
        return "a" * 64

    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=[value for _, value in epoch._CONTENT_SETTINGS]),
        transaction=Mock(side_effect=AssertionError("canonical producer needs no savepoint")),
    )
    monkeypatch.setattr(epoch, "_content_chunks", chunks)
    assert await epoch._content(connection, "synthetic", source) == "a" * 64
    connection.transaction.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("body_fails", (False, True))
async def test_failed_content_cleanup_preserves_original_exception_and_discards_connection(monkeypatch, body_fails):
    """Retire rollback failures after either body outcome, preserving a failed body's exception."""
    body_error = RuntimeError("original content failure")
    rollback_error = ConnectionError("rollback failed")
    transaction = SimpleNamespace(start=AsyncMock(), rollback=AsyncMock(side_effect=rollback_error), commit=AsyncMock())
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=["alternate"] * 5),
        fetch=AsyncMock(),
        transaction=lambda: transaction,
        terminate=Mock(),
    )
    monkeypatch.setattr(
        epoch, "_content_chunks", AsyncMock(side_effect=body_error if body_fails else None, return_value="a" * 64)
    )
    expected_error = body_error if body_fails else rollback_error
    with pytest.raises(type(expected_error)) as observed:
        await epoch._content(connection, "synthetic")
    assert observed.value is expected_error
    transaction.rollback.assert_awaited_once()
    transaction.commit.assert_not_awaited()
    connection.terminate.assert_called_once()
