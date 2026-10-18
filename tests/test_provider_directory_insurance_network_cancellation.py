# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve the original COPY interruption when invalidation breaks savepoint rollback."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from process import provider_directory_cms_identity_candidate as candidate
from process import provider_directory_insurance_network_batch as batch
from process.registry_record_store import RegistryAddressUnavailable


@asynccontextmanager
async def _failed_cleanup():
    try:
        yield
    finally:
        raise SQLAlchemyError("original COPY owner transport was invalidated")


def _prepare_batch(monkeypatch, session, outcome):
    monkeypatch.setattr(candidate, "identity_candidate", AsyncMock())
    monkeypatch.setattr(session, "connection", AsyncMock(return_value=object()))
    monkeypatch.setattr(session, "begin_nested", _failed_cleanup)
    monkeypatch.setattr(batch, "_encode", lambda value: (value, 1, 1, 0))
    writer = AsyncMock(side_effect=outcome) if isinstance(outcome, BaseException) else AsyncMock(return_value=outcome)
    monkeypatch.setattr(batch, "_write_batch", writer)
    return writer


async def _record(session):
    return await batch.record_insurance_network_batch(
        session,
        source_id="cms-npd",
        release_id="a" * 64,
        resource_type="Organization",
        resources=[{"resourceType": "Organization", "id": "network-one"}],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", [asyncio.CancelledError("original COPY"), TimeoutError("original COPY")])
async def test_original_interruption_survives_failed_savepoint_cleanup(monkeypatch, interruption):
    async with AsyncSession() as session, session.begin():
        writer = _prepare_batch(monkeypatch, session, interruption)
        with pytest.raises(type(interruption)) as rejected:
            await _record(session)
        assert rejected.value is interruption
        writer.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [SQLAlchemyError("original storage error"), {"input_count": 1}])
async def test_ordinary_storage_and_cleanup_errors_still_refuse(monkeypatch, outcome):
    async with AsyncSession() as session, session.begin():
        writer = _prepare_batch(monkeypatch, session, outcome)
        with pytest.raises(RegistryAddressUnavailable, match="insurance_network_store_unavailable$"):
            await _record(session)
        writer.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", [asyncio.CancelledError("original COPY"), TimeoutError("original COPY")])
@pytest.mark.parametrize("failure_stage", ["close", "invalidate"])
async def test_copy_cleanup_failure_preserves_original_interruption(monkeypatch, interruption, failure_stage):
    from process import native_copy_cleanup as cleanup
    from process import provider_directory_generic_source_policy as source

    driver = SimpleNamespace(copy_to_table=AsyncMock(side_effect=interruption))
    owner = object()
    cleanup_error = SQLAlchemyError("closed original COPY transport during cleanup")
    close = AsyncMock(side_effect=cleanup_error if failure_stage == "close" else None)
    invalidate = AsyncMock(side_effect=cleanup_error if failure_stage == "invalidate" else None)
    monkeypatch.setattr(cleanup, "close_interrupted_copy", close)
    monkeypatch.setattr(cleanup, "invalidate_copy_owner", invalidate)
    monkeypatch.setattr(source, "bound_publisher", lambda: SimpleNamespace(connection=driver))
    monkeypatch.setattr(source, "source_copy_session", lambda: owner)
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))

    with pytest.raises(type(interruption)) as rejected:
        await batch._copy_stage(connection, "owned_stage", (b"copy payload", 1, 1, 0))
    assert rejected.value is interruption and interruption.__cause__ is cleanup_error
    close.assert_awaited_once_with(driver)
    if failure_stage == "close":
        invalidate.assert_not_awaited()
    else:
        invalidate.assert_awaited_once_with(owner, driver)
