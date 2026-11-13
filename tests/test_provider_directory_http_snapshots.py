# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary provider HTTP reads own one bounded logical-directory snapshot."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_batch, provider_profile_snapshot
from api.endpoint import npi
from api.provider_list_sql import _provider_list_connection


class _ReadProbe(Exception):
    pass


class _SnapshotSession:
    def __init__(self, events):
        self.events = events
        self.active = False
        self.info = {}

    @asynccontextmanager
    async def begin(self):
        self.active = True
        self.events.append("begin")
        try:
            yield
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")
        finally:
            self.active = False

    @asynccontextmanager
    async def begin_nested(self):
        yield

    def get_transaction(self):
        return self if self.active else None

    def get_nested_transaction(self):
        return None

    async def execute(self, statement):
        assert self.active
        self.events.append(str(statement))


@pytest.fixture
def snapshots(monkeypatch):
    events = []
    owned_session = _SnapshotSession(events)
    original_session = SimpleNamespace(execute=AsyncMock(), rollback=AsyncMock(), close=AsyncMock())

    @asynccontextmanager
    async def own_session():
        events.append("open")
        try:
            yield owned_session
        finally:
            events.append("close")

    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "false")
    monkeypatch.setattr(npi.db, "session", own_session)
    return events, owned_session, original_session


def _request(original_session, *, args=None, body=None):
    return SimpleNamespace(args=args or {}, json=body, ctx=SimpleNamespace(sa_session=original_session))


def _assert_owned_snapshot(events, original_session, *, succeeded):
    assert events[:3] == ["open", "begin", "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"]
    assert events[-2:] == ["commit" if succeeded else "rollback", "close"]
    original_session.execute.assert_not_awaited()
    original_session.rollback.assert_not_awaited()
    original_session.close.assert_not_awaited()


@pytest.mark.parametrize("phone", [None, "5550100000"])
async def test_list_and_phone_begin_snapshot_before_directory_reads(monkeypatch, snapshots, phone):
    events, owned_session, original_session = snapshots

    async def directory_probe(session, *_args):
        assert session is owned_session and session.active
        events.extend(("pointer", "header"))
        raise _ReadProbe

    async def address_probe(*_args, session=None):
        return await directory_probe(session)

    monkeypatch.setattr(npi, "_named_directory_editions", directory_probe)
    monkeypatch.setattr(npi, "_address_serving_table_sql", address_probe)
    request = _request(original_session, args={"phone": phone} if phone else {})
    with pytest.raises(_ReadProbe):
        await npi.list_providers(request)
    assert request.ctx.sa_session is original_session
    assert events[3:5] == ["pointer", "header"]
    _assert_owned_snapshot(events, original_session, succeeded=False)


async def test_match_reuses_one_session_for_normalization_and_payload(monkeypatch, snapshots):
    events, owned_session, original_session = snapshots

    async def normalize(_request, *, request_session=None):
        assert request_session is owned_session and request_session.active
        events.append("normalize")
        return {"limit": 1}

    async def fetch(_params, *, started_at, database_session=None):
        assert database_session is owned_session and database_session.active
        events.extend(("pointer", "header", "payload"))
        return [], {}

    monkeypatch.setattr(npi, "_normalize_match_candidate_params", normalize)
    monkeypatch.setattr(npi, "_fetch_enriched_match_candidate_rows_bounded", fetch)
    request = _request(original_session)
    reply = await npi.match_candidates(request)
    assert reply.status == 200 and request.ctx.sa_session is original_session
    assert events[3:7] == ["normalize", "pointer", "header", "payload"]
    _assert_owned_snapshot(events, original_session, succeeded=True)


@pytest.mark.parametrize("native_query", [None, {"phone": "5550100000"}])
async def test_batch_entrypoint_passes_its_snapshot_to_both_read_modes(monkeypatch, snapshots, native_query):
    events, owned_session, original_session = snapshots

    async def fetch(*_args, session=None, **_kwargs):
        assert session is owned_session and session.active
        events.extend(("pointer", "header", "payload"))
        return {"items": [], "requested": 1, "found": 0, "not_found": 1}

    monkeypatch.setattr(provider_batch, "read_native_batch", fetch)
    monkeypatch.setattr(npi, "_build_npi_batch_payload", fetch)
    body_by_field = {"npis": [9000000000]}
    if native_query is not None:
        body_by_field["native_query"] = native_query
    request = _request(original_session, body=body_by_field)
    reply = await npi.get_npi_batch(request)
    assert reply.status == 200 and request.ctx.sa_session is original_session
    assert events[3:6] == ["pointer", "header", "payload"]
    _assert_owned_snapshot(events, original_session, succeeded=True)


async def test_existing_reader_is_reused_without_ordinary_pool_or_new_transaction(monkeypatch, snapshots):
    events, _owned_session, original_session = snapshots
    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "true")
    monkeypatch.setattr("db.connection.has_reader_session", lambda _database: True)

    async def fetch(_params, *, started_at, database_session=None):
        assert database_session is original_session
        return [], {}

    monkeypatch.setattr(npi, "_fetch_enriched_match_candidate_rows_bounded", fetch)
    reply = await npi.match_candidates(_request(original_session, args={"phone": "5550100000"}))
    assert reply.status == 200 and events == []
    original_session.execute.assert_not_awaited()


async def test_cancelled_http_read_rolls_back_owned_snapshot_and_closes(monkeypatch, snapshots):
    events, owned_session, original_session = snapshots
    started = asyncio.Event()

    async def fetch(_params, *, started_at, database_session=None):
        assert database_session is owned_session
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(npi, "_fetch_enriched_match_candidate_rows_bounded", fetch)
    request = _request(original_session, args={"phone": "5550100000"})
    task = asyncio.create_task(npi.match_candidates(request))
    await asyncio.wait_for(started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert request.ctx.sa_session is original_session
    _assert_owned_snapshot(events, original_session, succeeded=False)


async def test_standalone_status_reads_pointer_header_and_payload_inside_owned_snapshot(monkeypatch, snapshots):
    events, owned_session, original_session = snapshots
    record_id = "provider_directory_fhir:practitioner_role:synthetic:role"

    async def resolve(session, _schema):
        assert session is owned_session and session.active
        events.extend(("pointer", "header"))
        return ()

    async def fetch(_statement, *, session=None, params=None):
        assert session is owned_session and session.active
        assert params == {"source_record_ids": [record_id]}
        events.append("payload")
        return SimpleNamespace(all=lambda: [{"source_record_id": record_id, "location_status": "active"}])

    monkeypatch.setattr(npi, "_named_directory_editions", resolve)
    monkeypatch.setattr(npi, "_execute_stmt", fetch)
    assert await npi._fetch_location_status_by_record_id([record_id], session=original_session) == {record_id: "active"}
    assert events[3:6] == ["pointer", "header", "payload"]
    _assert_owned_snapshot(events, original_session, succeeded=True)


@pytest.mark.parametrize("scope", ["canonical", "imported", "explicit"])
async def test_existing_scoped_reads_preserve_their_caller_session(monkeypatch, snapshots, scope):
    events, _owned_session, original_session = snapshots

    @provider_profile_snapshot.provider_directory_snapshot_read
    async def read(_request, *, session=None, import_context=None):
        session = session if session is not None else npi._request_session(_request)
        assert session is original_session
        return "retained"

    options_by_field = {}
    if scope == "canonical":
        monkeypatch.setattr(npi, "current_network_address_scope", lambda: object())
    elif scope == "imported":
        options_by_field["import_context"] = object()
    else:
        options_by_field["session"] = original_session
    assert await read(_request(original_session), **options_by_field) == "retained"
    assert events == []


async def test_ordinary_list_sql_uses_explicit_snapshot_instead_of_another_connection(monkeypatch, snapshots):
    events, owned_session, original_session = snapshots
    acquire = AsyncMock(side_effect=AssertionError("explicit snapshot must remain bound"))
    monkeypatch.setattr(npi.db, "acquire", acquire)
    async with provider_profile_snapshot.provider_directory_read_snapshot(npi.db) as session:
        assert session is owned_session
        query = AsyncMock(return_value=SimpleNamespace(all=lambda: [("edition-a",)]))
        monkeypatch.setattr(session, "execute", query)
        async with _provider_list_connection(npi.db, None, session) as connection:
            assert await connection.all("SELECT :dataset_id", dataset_id="edition-a") == [("edition-a",)]
        query.assert_awaited_once()
        assert query.await_args.args[1] == {"dataset_id": "edition-a"}
    acquire.assert_not_called()
    _assert_owned_snapshot(events, original_session, succeeded=True)


async def test_status_child_reuses_ordinary_snapshot_without_opening_a_second_session(monkeypatch, snapshots):
    events, owned_session, original_session = snapshots
    record_id = "provider_directory_fhir:practitioner_role:synthetic:role"

    async def resolve(session, _schema):
        assert session is owned_session and session.active
        events.extend(("pointer", "header"))
        return ()

    async def fetch(_statement, *, session=None, params=None):
        assert session is owned_session and session.active
        events.append("payload")
        return SimpleNamespace(all=lambda: [{"source_record_id": record_id, "location_status": "inactive"}])

    monkeypatch.setattr(npi, "_named_directory_editions", resolve)
    monkeypatch.setattr(npi, "_execute_stmt", fetch)
    async with provider_profile_snapshot.provider_directory_read_snapshot(npi.db) as session:
        task = asyncio.create_task(
            npi._fetch_location_status_by_record_id([record_id], session=session, use_request_session=True)
        )
        assert await task == {record_id: "inactive"}
    assert events[3:6] == ["pointer", "header", "payload"]
    _assert_owned_snapshot(events, original_session, succeeded=True)


async def test_ordinary_count_uses_existing_pinned_timeout_recovery(monkeypatch, snapshots):
    events, owned_session, original_session = snapshots

    async def bounded_count(_count, *, session, has_reader, timeout):
        assert session is owned_session and session.active and has_reader
        assert timeout > 0
        return 2

    monkeypatch.setattr(npi, "_bounded_provider_list_count", bounded_count)
    reply = await npi.list_providers(_request(original_session, args={"count_only": "1"}))
    assert reply.status == 200
    _assert_owned_snapshot(events, original_session, succeeded=True)
