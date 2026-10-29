# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only renewal row contention permits a fresh lease check."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import asyncpg
import pytest

from process import registry_publication_consumer as consumer
from process.registry_approval_store import RegistryApprovalConflict

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("error_type", [asyncpg.InsufficientPrivilegeError, asyncpg.QueryCanceledError, RuntimeError])
async def test_non_contention_renewal_errors_propagate(monkeypatch, error_type):
    error = error_type("synthetic failure")
    renew, request = AsyncMock(side_effect=error), AsyncMock()
    monkeypatch.setattr(consumer, "renew_registry_publication_lease", renew)
    monkeypatch.setattr(consumer, "_request", request)
    connection = MagicMock()
    request_by_name = {"request_id": uuid4(), "lease_id": uuid4()}
    with pytest.raises(error_type) as raised:
        await consumer._renew_active_lease(connection, SimpleNamespace(control_schema="registry"), request_by_name)
    assert raised.value is error
    request.assert_not_awaited()


@pytest.mark.parametrize("error_type", [RegistryApprovalConflict, asyncpg.InsufficientPrivilegeError])
async def test_contention_requires_successful_fresh_native_lease_check(monkeypatch, error_type):
    error = error_type("synthetic lease refusal")
    monkeypatch.setattr(
        consumer, "renew_registry_publication_lease", AsyncMock(side_effect=asyncpg.LockNotAvailableError())
    )
    request = AsyncMock(side_effect=error)
    monkeypatch.setattr(consumer, "_request", request)
    connection = MagicMock()
    request_by_name = {"request_id": uuid4(), "lease_id": uuid4()}
    with pytest.raises(error_type) as raised:
        await consumer._renew_active_lease(connection, SimpleNamespace(control_schema="registry"), request_by_name)
    assert raised.value is error
    request.assert_awaited_once_with(
        connection, request_by_name["request_id"], request_by_name["lease_id"], '"registry"'
    )
