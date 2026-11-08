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
    identity_by_field = dict(
        login="catalog_reader",
        actor="catalog_reader",
        backend=12,
        database_oid=14,
        readonly="on",
        isolation="repeatable read",
        transaction="1/9",
    )
    writer_identity_by_field = {
        **identity_by_field,
        "login": "loader",
        "actor": "loader",
        "backend": 15,
        "readonly": "off",
    }
    writer = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(side_effect=lambda *_: writer_identity_by_field),
        fetchval=AsyncMock(return_value=True),
    )
    reader = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(side_effect=lambda *_: identity_by_field),
        fetchval=AsyncMock(return_value=False),
    )
    transaction = object()
    session = SimpleNamespace(
        execute=AsyncMock(return_value=Mock(mappings=lambda: Mock(one=lambda: dict(identity_by_field)))),
        connection=AsyncMock(
            return_value=SimpleNamespace(
                get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=reader))
            )
        ),
        get_transaction=Mock(return_value=transaction),
        rollback=AsyncMock(),
        close=AsyncMock(),
    )
    database = SimpleNamespace(session_factory=Mock(return_value=session), _reader_login=("catalog_reader", "unused"))
    connect = AsyncMock(return_value=database)
    monkeypatch.setattr(readers, "db", SimpleNamespace(_connect_reader=connect))
    return SimpleNamespace(
        writer=writer,
        reader=reader,
        identity=identity_by_field,
        writer_identity=writer_identity_by_field,
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


def _damage_reader_case(case, damage):
    if damage in {"login", "actor", "readonly", "isolation"}:
        case.identity[damage] = "changed"
        return
    if damage == "backend":
        case.identity[damage] = case.writer_identity[damage]
        return
    if damage == "database_oid":
        case.identity[damage] += 1
        return
    if damage == "same_database":
        case.reader.fetchval.return_value = True
        return
    if damage == "writer_actor":
        case.writer_identity["actor"] = "assumed_writer"
        return
    if damage == "writer_login":
        case.writer_identity.update(actor="catalog_reader", login="catalog_reader")
        return
    if damage == "writer_transaction":
        case.writer.is_in_transaction = lambda: False
        return
    case.reader.is_in_transaction = lambda: False


@pytest.mark.parametrize(
    "damage",
    [
        "login",
        "actor",
        "readonly",
        "isolation",
        "backend",
        "database_oid",
        "same_database",
        "writer_actor",
        "writer_transaction",
        "reader_transaction",
        "writer_login",
    ],
)
async def test_changed_reader_or_writer_refuses_before_manifest_query(reader_case, monkeypatch, damage):
    case = reader_case
    resolve = AsyncMock()
    monkeypatch.setattr(readers, "resolve_network_serving_manifest", resolve)
    _damage_reader_case(case, damage)
    with pytest.raises(ValueError, match="registry_reader_changed"):
        async with readers.fhir_catalog_registry_reads(case.writer):
            await readers.resolve_fhir_catalog_network_manifest(case.writer)
    resolve.assert_not_awaited()
    if case.connect.await_count:
        case.session.rollback.assert_awaited_once()
        case.session.close.assert_awaited_once()
    assert readers._READS.get() is None


async def test_reader_refuses_foreign_task_connection_and_reopened_transaction(reader_case):
    case = reader_case
    with pytest.raises(ValueError, match="registry_reader_changed"):
        await readers.fhir_catalog_registry_connection(case.writer)
    async with readers.fhir_catalog_registry_reads(case.writer):
        with pytest.raises(ValueError, match="registry_reader_changed"):
            await readers.fhir_catalog_registry_connection(object())
        with pytest.raises(ValueError, match="registry_reader_changed"):
            await asyncio.create_task(readers.fhir_catalog_registry_connection(case.writer))
        assert await readers.fhir_catalog_registry_connection(case.writer) is case.reader
        transaction = case.session.get_transaction.return_value
        case.session.get_transaction.return_value = object()
        with pytest.raises(ValueError, match="registry_reader_changed"):
            await readers.fhir_catalog_registry_connection(case.writer)
        case.session.get_transaction.return_value = transaction
        case.identity["transaction"] = "1/10"
        with pytest.raises(ValueError, match="registry_reader_changed"):
            await readers.fhir_catalog_registry_connection(case.writer)
        case.identity["transaction"] = "1/9"


async def test_precommit_verification_refuses_ended_reader(reader_case):
    case = reader_case
    with pytest.raises(ValueError, match="registry_reader_changed"):
        await readers.verify_fhir_catalog_registry_reads()
    with pytest.raises(ValueError, match="registry_reader_changed"):
        async with readers.fhir_catalog_registry_reads(case.writer):
            await readers.verify_fhir_catalog_registry_reads()
            case.connect.assert_not_awaited()
            await readers.fhir_catalog_registry_connection(case.writer)
            case.reader.is_in_transaction = lambda: False
            await readers.verify_fhir_catalog_registry_reads()
    case.session.rollback.assert_awaited_once()
    case.session.close.assert_awaited_once()


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
