# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host wiring checks; authority and publication require the genuine native test."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import network_registry_cms_prepared_pair as retention
from tests import cms_registry_native_producer_postgres_support as fixture


@asynccontextmanager
async def _context(value):
    yield value


@dataclass(frozen=True)
class _Job:
    capture_id: object
    notify_receipt: object


@pytest.fixture
def first_publication(monkeypatch):
    """Record calls and object identity without granting the doubles native authority."""
    tasks = []
    database = SimpleNamespace(_source_component=SimpleNamespace(factory=object()))
    state = SimpleNamespace(
        database=database,
        acquired=object(),
        roles=("owner", "reader"),
        retention_sessions=object(),
        captures=[],
        notices=[],
        copy_bounds=(1, 64 * 1024**2),
        store=object(),
    )
    prepared, factory, request, retained, execution = object(), object(), object(), object(), object()
    module = SimpleNamespace(
        _assert_retained_raw_parity=AsyncMock(),
        _assert_retained_address_parity=AsyncMock(),
        _verify_committed_notification=AsyncMock(),
    )
    monkeypatch.setattr(fixture.source, "bind_generic_source_transaction_factory", lambda value: _context(value))

    async def attest_execution(*_args):
        tasks.append(asyncio.current_task())
        return execution

    attest = AsyncMock(side_effect=attest_execution)
    monkeypatch.setattr(fixture.native, "attested_execution", attest)
    monkeypatch.setattr(fixture, "_new_notification_store", AsyncMock())
    job = _Job(uuid4(), object())
    issue = AsyncMock(return_value=job)
    monkeypatch.setattr(fixture, "issued_job", issue)
    original = AsyncMock(return_value=retained)
    monkeypatch.setattr(retention, "prepare_registry_cms_source_pair", original)

    def sender(actual_module, store, actual_state, *, fail):
        assert actual_module is module and store is state.store and actual_state is state and not fail

        async def notify(*_args, **_kwargs):
            state.pair = pair
            state.notices.append(SimpleNamespace(pair=pair))

        return notify

    publications = []

    async def publish(actual_database, actual_execution, patch, *, job, batch_size):
        assert actual_database is database and actual_execution is execution and batch_size == 1
        assert asyncio.current_task() is tasks[0]
        publications.append(job)
        await retention.prepare_registry_cms_source_pair(prepared, factory, request, capture_id=job.capture_id)
        await job.notify_receipt()

    monkeypatch.setattr(fixture, "_notification_sender", sender)
    monkeypatch.setattr(fixture, "publish_native_execution", publish)
    pair = SimpleNamespace(proof=SimpleNamespace(serving_receipt_id="receipt"))
    receipt_payload_by_field = {"predecessor_receipt_id": None, "expected_incumbent": None}
    committed = AsyncMock(return_value=(pair.proof, receipt_payload_by_field))
    monkeypatch.setattr(fixture.native, "committed_publication_proof", committed)
    return SimpleNamespace(**locals())


@pytest.mark.asyncio
async def test_first_publication_issues_one_job_and_verifies_bound_notification(first_publication, monkeypatch):
    case = first_publication
    await fixture._publish_first_pair(case.module, case.state, monkeypatch)
    case.attest.assert_awaited_once_with(case.database, case.state.acquired, monkeypatch)
    case.issue.assert_awaited_once_with(
        case.database,
        case.state.acquired,
        case.execution,
        case.state.roles,
        case.state.retention_sessions,
        case.state.store,
    )
    assert len(case.publications) == 1 and case.publications[0].capture_id == case.job.capture_id
    case.original.assert_awaited_once_with(case.prepared, case.factory, case.request, capture_id=case.job.capture_id)
    assert case.state.prepared is case.prepared and case.state.retained is case.retained
    assert case.state.captures == [case.job.capture_id]
    case.module._assert_retained_raw_parity.assert_awaited_once_with(case.database, case.retained)
    case.module._assert_retained_address_parity.assert_awaited_once_with(case.database, case.retained, case.prepared)
    case.module._verify_committed_notification.assert_awaited_once_with(
        case.database, case.pair, case.state.store, case.state.notices[0]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["predecessor_receipt_id", "expected_incumbent"])
async def test_first_publication_refuses_incumbent_receipt(first_publication, monkeypatch, field):
    case = first_publication
    case.receipt_payload_by_field[field] = "unexpected-incumbent"
    with pytest.raises(AssertionError):
        await fixture._publish_first_pair(case.module, case.state, monkeypatch)
    case.module._verify_committed_notification.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "publication", "consumer"])
async def test_first_publication_cleans_before_bootstrap_closes(monkeypatch, tmp_path, failure):
    import sqlalchemy.ext.asyncio as sqlalchemy_asyncio

    events = []
    component = SimpleNamespace(
        retention_owner="owner", owner="source_owner", publisher="publisher", retained_publisher="retained"
    )
    database = SimpleNamespace(
        _source_component=component, engine=SimpleNamespace(url=SimpleNamespace(set=lambda **_fields: "retained-url"))
    )

    @asynccontextmanager
    async def bootstrap(*_args):
        events.append("bootstrap")
        try:
            yield database
        finally:
            events.append("bootstrap_exit")

    async def publish(module, state, patch):
        assert state.database is database and state.acquired is acquired and state.retention_sessions is sessions
        events.append("publish")
        if failure == "publication":
            raise RuntimeError("publication")

    async def cleanup(actual_database, state, actual_engine):
        assert actual_database is database and actual_engine is engine and "bootstrap_exit" not in events
        events.append("cleanup")

    engine, sessions, acquired = object(), object(), object()
    monkeypatch.setattr(fixture, "_configure_native_producer", lambda *_args: None)
    monkeypatch.setattr(fixture.native, "source_native_publication_database", bootstrap)
    monkeypatch.setattr(fixture, "_first_publication_reader", AsyncMock())
    monkeypatch.setattr(fixture, "acquire_native_source", AsyncMock(return_value=acquired))
    monkeypatch.setattr(fixture, "_publish_first_pair", publish)
    monkeypatch.setattr(fixture, "_cleanup_producer", cleanup)
    monkeypatch.setattr(sqlalchemy_asyncio, "create_async_engine", lambda *_args, **_kwargs: engine)
    monkeypatch.setattr(sqlalchemy_asyncio, "async_sessionmaker", lambda *_args, **_kwargs: sessions)
    if failure:
        with pytest.raises(RuntimeError, match=failure):
            async with fixture.genuine_first_prepared_publication(monkeypatch, tmp_path, (1, 64 * 1024**2)):
                raise RuntimeError("consumer")
    else:
        async with fixture.genuine_first_prepared_publication(monkeypatch, tmp_path, (1, 64 * 1024**2)):
            events.append("consumer")
    assert events[:2] == ["bootstrap", "publish"] and events.count("publish") == 1
    assert events[-2:] == ["cleanup", "bootstrap_exit"]
    fixture.acquire_native_source.assert_awaited_once_with(
        database, monkeypatch, tmp_path / "first", revision="first", run_id="retention-first"
    )
