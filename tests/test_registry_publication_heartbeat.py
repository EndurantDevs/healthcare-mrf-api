# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only renewal row contention permits a fresh lease check."""

import asyncio
from contextlib import asynccontextmanager
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


@pytest.fixture
def heartbeat_case(monkeypatch):
    events = SimpleNamespace(
        **{
            name: asyncio.Event()
            for name in (
                "renewing",
                "release_started",
                "release_allowed",
                "released",
                "heartbeat_stopped",
                "work_started",
                "cancelled",
            )
        }
    )
    release_markers = []
    connection = MagicMock()
    request_by_field = {"request_id": uuid4(), "lease_id": uuid4(), "command_json": {"operation": None}}
    config = SimpleNamespace(control_schema="registry", membership_refresh_runtime=None)

    @asynccontextmanager
    async def acquire():
        try:
            yield connection
        finally:
            release_markers.append("release")
            events.release_started.set()
            await events.release_allowed.wait()
            events.released.set()
            if getattr(events, "cleanup_error", False):
                raise RuntimeError("synthetic cleanup failure")

    heartbeat = consumer._heartbeat

    async def observe_heartbeat(*args):
        try:
            await heartbeat(*args)
        finally:
            release_markers.append("heartbeat_stopped")
            events.heartbeat_stopped.set()

    monkeypatch.setattr(consumer, "_verify_execution", AsyncMock())
    monkeypatch.setattr(consumer, "_verify_lifecycle", AsyncMock())
    monkeypatch.setattr(consumer, "_HEARTBEAT_SECONDS", 0)
    monkeypatch.setattr(consumer, "_heartbeat", observe_heartbeat)
    lifecycle = MagicMock()
    lifecycle.acquire.return_value.__aenter__.return_value = connection
    return SimpleNamespace(
        events=events,
        release_markers=release_markers,
        connection=connection,
        request=request_by_field,
        config=config,
        execution=SimpleNamespace(acquire=acquire),
        lifecycle=lifecycle,
    )


async def _run_case(case):
    return await consumer._run_with_heartbeat(
        case.lifecycle, case.execution, case.config, case.request, AsyncMock(), None
    )


@pytest.mark.parametrize("cleanup_error", [False, True])
async def test_committed_publication_stops_heartbeat_before_connection_release(
    monkeypatch, heartbeat_case, cleanup_error
):
    case, published_by_field = heartbeat_case, {"serving_generation": 2}
    publication_committed = asyncio.Event()
    case.events.cleanup_error = cleanup_error

    async def renew(*args, **kwargs):
        case.events.renewing.set()
        await case.events.release_started.wait()
        assert publication_committed.is_set()
        return None

    async def publish(*args):
        await case.events.renewing.wait()
        publication_committed.set()
        return published_by_field

    monkeypatch.setattr(consumer, "renew_registry_publication_lease", renew)
    monkeypatch.setattr(consumer, "_ordinary_publication", publish)
    pending = asyncio.create_task(_run_case(case))
    try:
        async with asyncio.timeout(2):
            await case.events.release_started.wait()
            await case.events.heartbeat_stopped.wait()
            assert case.release_markers == ["heartbeat_stopped", "release"]
            case.events.release_allowed.set()
            if cleanup_error:
                with pytest.raises(RuntimeError, match="^synthetic cleanup failure$"):
                    await pending
            else:
                assert await pending == published_by_field
        assert publication_committed.is_set() and case.events.released.is_set()
    finally:
        case.events.release_allowed.set()
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.parametrize("failure", ["lease_loss", "cancellation"])
async def test_unfinished_publication_remains_cancelled_and_fenced(monkeypatch, heartbeat_case, failure):
    case, writes = heartbeat_case, []
    case.events.release_allowed.set()

    async def renew(connection, request_id, lease_id, **kwargs):
        assert connection is case.connection
        assert (request_id, lease_id) == (case.request["request_id"], case.request["lease_id"])
        assert kwargs == {"lease_seconds": 300, "control_schema": "registry"}
        case.events.renewing.set()
        await case.events.work_started.wait()
        if failure == "cancellation":
            await asyncio.Event().wait()
        return None

    async def publish(connection, request, authorize, *args):
        await case.events.renewing.wait()
        case.events.work_started.set()
        try:
            await asyncio.Event().wait()
            writes.append("committed")
        except asyncio.CancelledError:
            with pytest.raises(RegistryApprovalConflict, match="^registry_queue_lease_conflict$"):
                await authorize(actor="synthetic")
            case.events.cancelled.set()
            raise

    monkeypatch.setattr(consumer, "renew_registry_publication_lease", renew)
    monkeypatch.setattr(consumer, "_ordinary_publication", publish)
    pending = asyncio.create_task(_run_case(case))
    try:
        async with asyncio.timeout(2):
            await case.events.work_started.wait()
            if failure == "cancellation":
                pending.cancel()
            with pytest.raises(asyncio.CancelledError if failure == "cancellation" else RegistryApprovalConflict):
                await pending
        assert not writes
        assert all(getattr(case.events, name).is_set() for name in ("cancelled", "released", "heartbeat_stopped"))
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
