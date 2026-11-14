# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog Reader ownership never changes Source or writer transaction authority."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db.connection import _READER
from process import registry_fhir_catalog_registry_reader as readers


@pytest.fixture
def reader_case(monkeypatch):
    writer = SimpleNamespace(is_in_transaction=lambda: True)
    reader = SimpleNamespace(is_in_transaction=lambda: True)
    transaction = object()
    session = SimpleNamespace(
        execute=AsyncMock(),
        connection=AsyncMock(
            return_value=SimpleNamespace(
                get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=reader))
            )
        ),
        get_transaction=Mock(return_value=transaction),
        rollback=AsyncMock(),
        close=AsyncMock(),
    )
    database = SimpleNamespace(session_factory=Mock(return_value=session))
    connect = AsyncMock(return_value=database)
    monkeypatch.setattr(readers, "db", SimpleNamespace(_connect_reader=connect))
    return SimpleNamespace(
        writer=writer,
        reader=reader,
        session=session,
        database=database,
        connect=connect,
    )


async def test_reader_is_lazy_and_remains_open_after_writer_commit(reader_case, monkeypatch):
    case = reader_case
    resolve = AsyncMock(return_value="manifest")
    monkeypatch.setattr(readers, "resolve_network_serving_manifest", resolve)
    original_api_reader = _READER.get()
    async with readers.fhir_catalog_registry_reads(case.writer):
        case.connect.assert_not_awaited()
        assert (
            await readers.resolve_fhir_catalog_network_manifest(case.writer, generation_id=7, control_schema="registry")
            == "manifest"
        )
        resolve.assert_awaited_once_with(case.reader, generation_id=7, control_schema="registry")
        assert await readers.fhir_catalog_registry_connection(case.reader) is case.reader
        assert _READER.get() is original_api_reader
        case.writer.is_in_transaction = lambda: False
        case.session.rollback.assert_not_awaited()
        case.session.close.assert_not_awaited()
    case.session.rollback.assert_awaited_once()
    case.session.close.assert_awaited_once()
    assert readers._READS.get() is None and _READER.get() is original_api_reader


async def test_reader_requires_active_caller_and_same_reader_transaction(reader_case):
    case = reader_case
    with pytest.raises(ValueError, match="registry_reader_changed"):
        await readers.fhir_catalog_registry_connection(case.writer)
    async with readers.fhir_catalog_registry_reads(case.writer):
        with pytest.raises(ValueError, match="registry_reader_changed"):
            await readers.fhir_catalog_registry_connection(object())
        assert await asyncio.create_task(readers.fhir_catalog_registry_connection(case.writer)) is case.reader
        assert await readers.fhir_catalog_registry_connection(case.writer) is case.reader
        transaction = case.session.get_transaction.return_value
        case.session.get_transaction.return_value = object()
        with pytest.raises(ValueError, match="registry_reader_changed"):
            await readers.fhir_catalog_registry_connection(case.writer)
        case.session.get_transaction.return_value = transaction


async def test_cancellation_drains_reader_before_propagating(reader_case):
    case = reader_case
    opened, closing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def close():
        closing.set()
        await release.wait()

    case.session.close.side_effect = close

    async def operation():
        async with readers.fhir_catalog_registry_reads(case.writer):
            await readers.fhir_catalog_registry_connection(case.writer)
            opened.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(operation())
    await opened.wait()
    task.cancel()
    await closing.wait()
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    case.session.rollback.assert_awaited_once()
    case.session.close.assert_awaited_once()


async def test_reader_starts_snapshot_before_obtaining_native_driver(reader_case):
    """Execute real transaction SQL before using SQLAlchemy's lazy raw connection."""
    case = reader_case
    events = []

    async def begin(statement):
        assert str(statement) == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
        events.append("begin")

    connection = case.session.connection.return_value

    async def raw_connection():
        assert events == ["begin"]
        events.append("driver")
        return SimpleNamespace(driver_connection=case.reader)

    case.session.execute.side_effect = begin
    connection.get_raw_connection.side_effect = raw_connection
    async with readers.fhir_catalog_registry_reads(case.writer):
        assert await readers.fhir_catalog_registry_connection(case.writer) is case.reader
    assert events == ["begin", "driver"]


async def test_reader_begin_failure_keeps_original_error_and_cleans_session(reader_case):
    """A failed native snapshot start cannot expose a reader or mask its error."""
    case = reader_case
    failure = RuntimeError("snapshot start failed")
    case.session.execute.side_effect = failure
    case.session.close.side_effect = RuntimeError("close failed")
    with pytest.raises(RuntimeError) as raised:
        async with readers.fhir_catalog_registry_reads(case.writer):
            await readers.fhir_catalog_registry_connection(case.writer)
    assert raised.value is failure
    case.session.connection.assert_not_awaited()
    case.session.rollback.assert_awaited_once()
    case.session.close.assert_awaited_once()
    assert failure.__notes__ == ["Catalog Reader cleanup failed"]
    assert readers._READS.get() is None
