# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded SOURCE COPY, durable retry and genuine EOF host regressions."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from sqlalchemy.dialects.postgresql import dialect

import process.custom_import.build_source as staging
from process.custom_import import bulk_page_codec as codec
from process.custom_import import source_finalize_sql as finalizer
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_codec import pack_hash
from process.custom_import.runner_types import CandidateRegistry, CandidateRunnerError, LeaseAuthorityLost
from tests.test_custom_import_build_source import _child, _request, _root
from tests.test_custom_import_snowflake_capture import _policy


def _bulk_context(stream=0):
    request = _request(page_row_limit=256)
    registry = CandidateRegistry({"details": 1}, {"root_source": 1, "detail_source": 2}, 1)
    return staging._StreamContext(request, registry, 11, request.definition.source_streams[stream])


def _prepared_page(context, values=None, *, part=1, row=0, ordinal=0):
    return staging._SourcePage(
        part,
        row,
        ordinal,
        tuple(staging._prepare_row(context.request, context.stream, value) for value in values or [_root()]),
    )


def _copy_stubs(monkeypatch, *, part=1, first_row=0, ordinal=0):
    cursor = SimpleNamespace(
        next_pack_ordinal=7, next_part_ordinal=part, next_part_row_ordinal=first_row, next_source_ordinal=ordinal
    )
    copied = AsyncMock()
    connection = SimpleNamespace(
        dialect=dialect(),
        sync_connection=SimpleNamespace(
            get_execution_options=lambda: {
                "schema_translate_map": {staging.CustomImportBuildAttempt.__table__.schema: "synthetic_candidate"}
            }
        ),
        get_raw_connection=AsyncMock(
            return_value=SimpleNamespace(driver_connection=SimpleNamespace(copy_records_to_table=copied))
        ),
    )
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: False)),
        scalars=AsyncMock(return_value=SimpleNamespace(one=lambda: cursor)),
        connection=AsyncMock(return_value=connection),
        add=Mock(),
        add_all=Mock(),
    )
    monkeypatch.setattr(staging, "_page_session", lambda *_: nullcontext((session, SimpleNamespace(phase="source"))))
    monkeypatch.setattr(staging, "_prepare_statement", AsyncMock())

    async def call(_session, name, _arguments):
        if name == "source_bulk_authorize":
            return SimpleNamespace(scalar_one=lambda: UUID(int=17))
        if name == "resolve_custom_import_source_batch_snapshot":
            return SimpleNamespace(scalar_one=lambda: 23)
        assert name == "source_set_finalize"
        return SimpleNamespace(scalar_one=lambda: len(copied.await_args.kwargs["records"]))

    called = AsyncMock(side_effect=call)
    monkeypatch.setattr(staging, "_call", called)
    return session, copied, called


@pytest.mark.parametrize("owner", [True, False, None, 1])
@pytest.mark.parametrize("schema", ['synthetic " mapped', "synthetic_candidate"])
async def test_source_owner_gate_uses_current_user_and_exact_mapped_signatures(monkeypatch, owner, schema):
    session, _, _ = _copy_stubs(monkeypatch)
    connection = session.connection.return_value
    connection.sync_connection.get_execution_options = lambda: {
        "schema_translate_map": {staging.CustomImportBuildAttempt.__table__.schema: schema}
    }
    session.execute.return_value.scalar_one = lambda: owner
    assert await staging._is_source_writer_owner(session) is (owner is True)
    statement, parameters = session.execute.await_args.args
    assert "pg_catalog.pg_get_userbyid(p.proowner)=CURRENT_USER" in str(statement)
    assert "pg_catalog.count(*)=2" in str(statement) and "pg_catalog.pg_proc" in str(statement)
    assert "pg_catalog.bool_and" in str(statement)
    assert "pg_catalog.to_regprocedure" in str(statement)
    quoted = dialect().identifier_preparer.quote_schema(schema)
    assert parameters == {
        "authorize": (
            f"{quoted}.source_bulk_authorize("
            "pg_catalog.int8,pg_catalog.int2,pg_catalog.int8,pg_catalog.bytea,pg_catalog.int4,pg_catalog.int8)"
        ),
        "finalize": f"{quoted}.source_set_finalize(pg_catalog.uuid,pg_catalog.int4[])",
    }


async def test_source_owner_gate_rechecks_each_batch_and_requires_explicit_schema(monkeypatch):
    session, _, _ = _copy_stubs(monkeypatch)
    session.execute.side_effect = [
        SimpleNamespace(scalar_one=lambda: True),
        SimpleNamespace(scalar_one=lambda: False),
    ]
    assert await staging._is_source_writer_owner(session) is True
    assert await staging._is_source_writer_owner(session) is False
    connection = session.connection.return_value
    connection.sync_connection.get_execution_options = lambda: {
        "schema_translate_map": {staging.CustomImportBuildAttempt.__table__.schema: None}
    }
    with pytest.raises(CandidateRunnerError, match="explicit model schema"):
        await staging._is_source_writer_owner(session)
    assert session.execute.await_count == 2


@pytest.mark.parametrize("owner", [True, False])
async def test_source_route_uses_ordinary_sql_only_for_current_owner(monkeypatch, owner):
    context = _bulk_context()
    session, copied, called = _copy_stubs(monkeypatch)
    session.execute.return_value.scalar_one = lambda: owner
    finalized = AsyncMock(return_value=1)
    monkeypatch.setattr(finalizer, "finalize_source_batch", finalized)
    assert await staging._store_pages(None, context, (_prepared_page(context),)) == 1
    copied.assert_awaited_once()
    if owner:
        finalized.assert_awaited_once_with(session, UUID(int=17), [])
        assert [call.args[1] for call in called.await_args_list] == [
            "source_bulk_authorize",
            "resolve_custom_import_source_batch_snapshot",
        ]
    else:
        finalized.assert_not_awaited()
        assert called.await_args.args[1] == "source_set_finalize"


@pytest.mark.parametrize("stream", [0, 1])
async def test_copy_preserves_canonical_rows_and_promotes_one_set(monkeypatch, stream):
    context = _bulk_context(stream)
    record_values = [_root(), _root(score=None)] if stream == 0 else [_child(), _child(amount=None)]
    page = _prepared_page(context, record_values, row=3, ordinal=5)
    session, copied, called = _copy_stubs(monkeypatch, first_row=3, ordinal=5)
    assert await staging._store_pages(None, context, (page,)) == 2
    assert [call.args[1] for call in called.await_args_list] == [
        "source_bulk_authorize",
        "resolve_custom_import_source_batch_snapshot",
        "source_set_finalize",
    ]
    assert called.await_args_list[1].args[2] == (("uuid", UUID(int=17)),)
    assert called.await_args_list[0].args[2] == (
        ("bigint", 11),
        ("smallint", stream + 1),
        ("bigint", 1),
        ("bytea", lease_token_sha256(context.request.lease_token)),
        ("integer", 2),
        ("bigint", sum(prepared.byte_count for prepared in page.records)),
    )
    copied.assert_awaited_once()
    assert copied.await_args.args == ("source_bulk_landing",)
    assert copied.await_args.kwargs["schema_name"] == "ci_snapshot_23"
    for offset, landing in enumerate(copied.await_args.kwargs["records"]):
        landing_by_column = dict(zip(staging._SOURCE_COPY_COLUMNS, landing, strict=True))
        prepared = page.records[offset]
        assert landing_by_column["batch_id"] == UUID(int=17) and landing_by_column["landing_ordinal"] == offset
        assert landing_by_column["source_ordinal"] == 5 + offset and landing_by_column["part_row_ordinal"] == 3 + offset
        assert (
            landing_by_column["payload"] == prepared.payload
            and landing_by_column["payload_hash"] == prepared.payload_hash
        )
        assert landing_by_column["pack_ordinal"] == 7
        assert landing_by_column["rejection_evidence"] == (
            None if prepared.rejection is None else prepared.rejection.canonical_evidence
        )
    session.add.assert_not_called()
    session.add_all.assert_not_called()


@pytest.mark.parametrize("family_id", [None, True, 0, -1, 2**63, "23"])
async def test_copy_rejects_missing_or_malformed_binding_before_borrowing_driver(monkeypatch, family_id):
    """A bad binding never falls back to the canonical or mapped hot schema."""

    context = _bulk_context()
    session, copied, called = _copy_stubs(monkeypatch)
    ordinary_call = called.side_effect

    async def resolve_or_call(session, name, arguments):
        if name == "resolve_custom_import_source_batch_snapshot":
            return SimpleNamespace(scalar_one=lambda: family_id)
        return await ordinary_call(session, name, arguments)

    called.side_effect = resolve_or_call
    with pytest.raises(CandidateRunnerError, match="snapshot binding is malformed"):
        await staging._store_pages(None, context, (_prepared_page(context),))
    assert [call.args[1] for call in called.await_args_list] == [
        "source_bulk_authorize",
        "resolve_custom_import_source_batch_snapshot",
    ]
    session.connection.assert_not_awaited()
    copied.assert_not_awaited()


def test_codec_keeps_native_pack_hashes_and_independent_aggregate_caps():
    context = _bulk_context()
    context = replace(context, request=replace(context.request, page_byte_limit=262_144))
    first = _prepared_page(context, [_root()] * 256)
    second = _prepared_page(context, [_root(score=None)], row=256, ordinal=256)
    byte_count = sum(record.byte_count for page in (first, second) for record in page.records)
    encoded = codec.encode_landing_batch(
        context, (first, second), batch_id=UUID(int=17), first_pack_ordinal=7, row_limit=257, byte_limit=byte_count
    )
    assert codec.MAX_BATCH_ROWS == 100_000 and codec.MAX_BATCH_BYTES == 268_435_456
    assert encoded.pack_count == 2 and len(encoded.records) == 257 and encoded.byte_count == byte_count
    for page, ordinal in ((first, 7), (second, 8)):
        expected = pack_hash("root", [record.payload_hash for record in page.records if record.payload_hash])
        assert {landing.pack_sha256 for landing in encoded.records if landing.pack_ordinal == ordinal} == {expected}
    for changes in ({"row_limit": 256}, {"byte_limit": byte_count - 1}, {"row_limit": 100_001}):
        with pytest.raises(ValueError):
            codec.encode_landing_batch(context, (first, second), batch_id=UUID(int=17), first_pack_ordinal=7, **changes)
    for malformed in (
        replace(first, records=first.records + first.records[:1]),
        replace(first, records=(replace(first.records[0], byte_count=0),)),
    ):
        with pytest.raises(ValueError):
            codec.encode_landing_batch(context, (malformed,), batch_id=UUID(int=17), first_pack_ordinal=7)
    tighter = replace(
        context, request=replace(context.request, page_byte_limit=byte_count - second.records[0].byte_count - 1)
    )
    with pytest.raises(ValueError, match="source page.*byte limit"):
        codec.encode_landing_batch(tighter, (first,), batch_id=UUID(int=17), first_pack_ordinal=7)


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"raw_key": []}, "raw key is malformed"),
        ({"raw_key": (None, None)}, "raw key is malformed"),
        ({"raw_key": ("key", b"short")}, "SHA-256 digest"),
        ({"raw_key": None}, "accepted row lacks"),
        ({"typed_key": None}, "accepted row lacks"),
        ({"payload": None, "payload_hash": None}, "accepted row lacks"),
        ({"child_key": "child", "child_hash": bytes(32)}, "shape differs from its stream"),
    ],
)
def test_codec_rejects_corrupted_accepted_identity_before_encoding(changes, reason):
    context = _bulk_context()
    page = _prepared_page(context)
    malformed = replace(page, records=(replace(page.records[0], **changes),))
    with pytest.raises(ValueError, match=reason):
        codec.encode_landing_batch(context, (malformed,), batch_id=UUID(int=17), first_pack_ordinal=0)


@pytest.mark.parametrize("fault", ["accepted_payload", "nontext_evidence", "row_budget"])
def test_codec_preserves_rejection_shape_and_occurrence_budget(fault):
    context = _bulk_context()
    prepared = _prepared_page(context, [_root(score=None)]).records[0]
    if fault == "accepted_payload":
        prepared = replace(prepared, payload="[]", payload_hash=bytes(32))
        reason = "rejected row must not contain"
    elif fault == "nontext_evidence":
        prepared.rejection.canonical_evidence = 1
        reason = "canonical documents must be text"
    else:
        context = replace(context, request=replace(context.request, page_byte_limit=prepared.byte_count - 1))
        reason = "one source occurrence exceeds"
    with pytest.raises(ValueError, match=reason):
        codec._row_values(prepared, context, context.request.definition.root_fields)


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("code", "Invalid", "rejection code is malformed"),
        ("code", None, "rejection code is malformed"),
        ("canonical_root_key", "other", "prepared typed key"),
        ("canonical_evidence", "{}", "rejection evidence differs"),
        ("canonical_evidence", None, "rejection evidence differs"),
        ("dataset_id", 99, "source context"),
        ("definition_revision_id", 99, "source context"),
        ("schema_revision_id", 99, "source context"),
        ("execution_id", 99, "source context"),
        ("producing_fence", 99, "source context"),
    ],
)
def test_codec_rejects_evidence_or_authority_changes(field, value, reason):
    context = _bulk_context()
    prepared = _prepared_page(context, [_root(score=None)]).records[0]
    setattr(prepared.rejection, field, value)
    with pytest.raises(ValueError, match=reason):
        codec._rejection_values(prepared.rejection, context, prepared.typed_key)


@pytest.mark.parametrize("stream", [0, 1])
def test_codec_rejects_rejection_codes_from_the_other_stream_kind(stream):
    context = _bulk_context(stream)
    other = _bulk_context(1 - stream)
    value = _child(npi=None) if stream == 0 else _root(npi=None)
    prepared = _prepared_page(other, [value]).records[0]
    with pytest.raises(ValueError, match="rejection shape differs from its stream"):
        codec._rejection_values(prepared.rejection, context, prepared.typed_key or (None, None))


@pytest.mark.parametrize(
    "kind,collection",
    [("unexpected", None), ("root", "details"), ("child", None)],
)
def test_codec_rechecks_stream_shape_before_consuming_pages(kind, collection):
    context = _bulk_context()
    stream = replace(context.stream, record_kind=kind, child_collection=collection)
    request = SimpleNamespace(
        definition=SimpleNamespace(source_streams=(stream,), fields=()),
        page_row_limit=256,
        page_byte_limit=16_384,
    )
    context = replace(context, request=request, stream=stream)
    untouched = object()
    pages = iter((untouched,))
    with pytest.raises(ValueError, match="stream kind and collection differ"):
        codec.encode_landing_batch(context, pages, batch_id=UUID(int=17), first_pack_ordinal=0)
    assert next(pages) is untouched


def test_codec_requires_server_batch_id_and_definition_owned_stream():
    context = _bulk_context()
    with pytest.raises(ValueError, match="server-provided batch UUID"):
        codec.encode_landing_batch(context, (), batch_id="17", first_pack_ordinal=0)
    foreign = replace(context, stream=replace(context.stream, stream_id="foreign"))
    with pytest.raises(ValueError, match="source stream differs from its definition"):
        codec.encode_landing_batch(foreign, (), batch_id=UUID(int=17), first_pack_ordinal=0)


@pytest.mark.parametrize(
    "part,row,ordinal,reason",
    [
        (1, 1, 2, "gap, duplicate position"),
        (1, 1, 0, "gap, duplicate position"),
        (1, 1, 1, "reversed part"),
        (2, 0, 1, "part-row gap"),
        (3, 1, 1, "part-row gap"),
    ],
)
def test_codec_denies_gaps_duplicates_and_reversed_parts(part, row, ordinal, reason):
    context = _bulk_context()
    first = _prepared_page(context, part=2)
    second = _prepared_page(context, part=part, row=row, ordinal=ordinal)
    with pytest.raises(ValueError, match=reason):
        codec.encode_landing_batch(context, (first, second), batch_id=UUID(int=17), first_pack_ordinal=0)


def test_codec_requires_occurrences_and_rechecks_page_occurrence_size():
    context = _bulk_context()
    with pytest.raises(ValueError, match="must contain source occurrences"):
        codec.encode_landing_batch(context, (), batch_id=UUID(int=17), first_pack_ordinal=0)
    page = _prepared_page(context)
    tighter = replace(context, request=replace(context.request, page_byte_limit=page.records[0].byte_count - 1))
    with pytest.raises(ValueError, match="one source occurrence exceeds"):
        codec.encode_landing_batch(tighter, (page,), batch_id=UUID(int=17), first_pack_ordinal=0)


@pytest.mark.parametrize("bound", ["rows", "bytes", "stream"])
async def test_buffer_flushes_intact_packs_before_crossing_aggregate_bounds(monkeypatch, bound):
    context = _bulk_context()
    first = _prepared_page(context, [_root(), _root()])
    stored, finished = AsyncMock(), AsyncMock()
    monkeypatch.setattr(staging, "_store_pages", stored)
    monkeypatch.setattr(staging, "_finish_part", finished)
    if bound == "rows":
        monkeypatch.setattr(staging, "MAX_BATCH_ROWS", 4)
    elif bound == "bytes":
        monkeypatch.setattr(staging, "MAX_BATCH_BYTES", sum(row.byte_count for row in first.records) * 2)
    buffer = staging._SourceBatch("factory")
    second = _prepared_page(context, [_root(), _root()], row=2, ordinal=2)
    await buffer.store("factory", context, first)
    await buffer.store("factory", context, second)
    stored.assert_not_awaited()
    next_context = _bulk_context(1) if bound == "stream" else context
    third = _prepared_page(
        next_context,
        [_child()] if bound == "stream" else [_root()],
        row=0 if bound == "stream" else 4,
        ordinal=0 if bound == "stream" else 4,
    )
    await buffer.store("factory", next_context, third)
    stored.assert_awaited_once_with("factory", context, (first, second), verified_parts=set())
    assert buffer.pages == [third] and buffer.rows == 1
    assert buffer.bytes == third.records[0].byte_count
    finished.assert_not_awaited()


@pytest.mark.parametrize("fault", ["cursor", "earlier_close", "huge_part_gap"])
async def test_invalid_cursor_or_unverified_part_gap_fails_before_copy(monkeypatch, fault):
    context = _bulk_context()
    part = (1 << 31) - 1 if fault == "huge_part_gap" else 2 if fault == "earlier_close" else 1
    session, copied, called = _copy_stubs(monkeypatch, ordinal=99 if fault == "cursor" else 0)
    with pytest.raises(CandidateRunnerError):
        await staging._store_pages(None, context, (_prepared_page(context, part=part),))
    called.assert_not_awaited()
    copied.assert_not_awaited()
    session.add.assert_not_called()
    session.add_all.assert_not_called()


async def test_buffer_flushes_small_packs_before_a_short_lease_expires(monkeypatch):
    clock_values = [0.0]
    monkeypatch.setattr(staging, "time", SimpleNamespace(monotonic=lambda: clock_values[0]))
    context = _bulk_context()
    context = replace(context, request=replace(context.request, lease_seconds=1))
    deadline = context.request.build_deadline_at
    stored, finished = AsyncMock(), AsyncMock()
    monkeypatch.setattr(staging, "_store_pages", stored)
    monkeypatch.setattr(staging, "_finish_part", finished)
    buffer = staging._SourceBatch("factory")
    pages = []
    for ordinal in range(3):
        clock_values[0] += 0.4
        page = _prepared_page(context, part=ordinal + 1, ordinal=ordinal)
        pages.append(page)
        await buffer.store("factory", context, page)
        await buffer.finish(context.request, context.build_id, context.stream_slot, ordinal + 1)
        assert not buffer.pages
    assert [call.args[2] for call in stored.await_args_list] == [(page,) for page in pages]
    assert finished.await_count == 3
    assert context.request.build_deadline_at == deadline


async def test_buffer_sends_only_verified_earlier_parts_and_finishes_empty_parts(monkeypatch):
    context = _bulk_context()
    _, copied, called = _copy_stubs(monkeypatch)
    finished = AsyncMock()
    monkeypatch.setattr(staging, "_finish_part", finished)
    buffer = staging._SourceBatch("factory")
    first, second = _prepared_page(context), _prepared_page(context, part=2, ordinal=1)
    await buffer.store("factory", context, first)
    await buffer.finish(context.request, 11, 1, 1)
    await buffer.store("factory", context, second)
    copied.assert_not_awaited()
    finished.assert_not_awaited()
    await buffer.finish(context.request, 11, 1, 2)
    await buffer.finish(context.request, 11, 1, 3)
    copied.assert_awaited_once()
    assert len(copied.await_args.kwargs["records"]) == 2
    assert called.await_args.args[2] == (("uuid", UUID(int=17)), ("integer[]", [1]))
    assert [call.args[-1] for call in finished.await_args_list] == [2, 3]
    assert not buffer.pages and not buffer.verified_parts


def _transaction_attempts(session, outcomes):
    @asynccontextmanager
    async def transaction():
        try:
            yield
        except BaseException:
            outcomes.append("rollback")
            raise
        else:
            outcomes.append("commit")

    @asynccontextmanager
    async def sessions():
        yield session

    session.begin = transaction
    return sessions


@pytest.mark.parametrize("failure", ["owner_probe", "finalize", "count"])
async def test_owner_route_failure_rolls_back_copy_without_legacy_fallback(monkeypatch, failure):
    context = _bulk_context()
    owned_page = staging._page_session
    session, copied, called = _copy_stubs(monkeypatch)
    session.execute.return_value.scalar_one = lambda: True
    finalized = AsyncMock(return_value=0 if failure == "count" else 1)
    error = RuntimeError("synthetic source failure")
    if failure == "owner_probe":
        session.execute.side_effect = error
    elif failure == "finalize":
        finalized.side_effect = error
    monkeypatch.setattr(finalizer, "finalize_source_batch", finalized)
    outcomes = []
    sessions = _transaction_attempts(session, outcomes)
    monkeypatch.setattr(staging, "_page_session", owned_page)
    monkeypatch.setattr(staging, "_lock_page", AsyncMock(return_value=SimpleNamespace(phase="source")))
    monkeypatch.setattr(staging, "_flush_page", AsyncMock())
    with pytest.raises(CandidateRunnerError if failure == "count" else RuntimeError) as caught:
        await staging._store_pages(sessions, context, (_prepared_page(context),))
    if failure != "count":
        assert caught.value is error
    assert outcomes == ["rollback"]
    copied.assert_awaited_once()
    assert [call.args[1] for call in called.await_args_list] == [
        "source_bulk_authorize",
        "resolve_custom_import_source_batch_snapshot",
    ]
    assert finalized.await_count == (0 if failure == "owner_probe" else 1)


@pytest.mark.parametrize("owner", [True, False])
@pytest.mark.parametrize("cancel_copy", [False, True])
async def test_copy_cancel_or_fresh_precommit_loss_rolls_back_then_retries(monkeypatch, cancel_copy, owner):
    context = _bulk_context()
    owned_page = staging._page_session
    session, copied, called = _copy_stubs(monkeypatch)
    session.execute.return_value.scalar_one = lambda: owner
    finalized = AsyncMock(return_value=1)
    monkeypatch.setattr(finalizer, "finalize_source_batch", finalized)
    outcomes = []
    sessions = _transaction_attempts(session, outcomes)
    monkeypatch.setattr(staging, "_page_session", owned_page)
    monkeypatch.setattr(staging, "_lock_page", AsyncMock(return_value=SimpleNamespace(phase="source")))
    monkeypatch.setattr(staging, "_flush_page", AsyncMock())
    monkeypatch.setattr(staging, "lock_execution", AsyncMock(return_value=object()))
    monkeypatch.setattr(staging, "lock_lease", AsyncMock(return_value=object()))
    valid_time = context.request.build_deadline_at.replace(year=2029)
    verify = AsyncMock(
        side_effect=[valid_time] if cancel_copy else [LeaseAuthorityLost("synthetic precommit loss"), valid_time]
    )
    monkeypatch.setattr(staging, "verify_live_attempt", verify)
    if cancel_copy:
        copied.side_effect = [asyncio.CancelledError("synthetic copy cancellation"), None]
    pages = (_prepared_page(context),)
    with pytest.raises(asyncio.CancelledError if cancel_copy else LeaseAuthorityLost, match="synthetic"):
        await staging._store_pages(sessions, context, pages)
    assert outcomes == ["rollback"]
    assert await staging._store_pages(sessions, context, pages) == 1
    assert outcomes == ["rollback", "commit"] and verify.await_count == (1 if cancel_copy else 2)
    assert copied.await_count == 2
    assert called.await_count == (4 if owner else (5 if cancel_copy else 6))
    assert finalized.await_count == ((1 if cancel_copy else 2) if owner else 0)
    assert sum(call.args[1] == "resolve_custom_import_source_batch_snapshot" for call in called.await_args_list) == 2


def _reader_contexts(context, events, failure):
    @asynccontextmanager
    async def transaction():
        yield
        events.append("transaction")
        if failure == "transaction":
            raise RuntimeError("synthetic transaction close")

    @asynccontextmanager
    async def sessions():
        yield SimpleNamespace(begin=transaction)
        events.append("session")
        if failure == "session":
            raise RuntimeError("synthetic session close")

    @asynccontextmanager
    async def open_parts(*_args, **_kwargs):
        async def parts():
            yield SimpleNamespace(
                ordinal=1,
                receipt=SimpleNamespace(stream_id=context.stream.stream_id, canonical_manifest='{"part_count":1}'),
            )
            if failure == "iterator":
                raise RuntimeError("synthetic iterator close")

        yield parts()
        events.append("reader")
        if failure == "reader":
            raise RuntimeError("synthetic reader close")

    return sessions, open_parts


def _buffered_replay(monkeypatch, events, failure):
    buffers = []
    original = staging._SourceBatch

    def make_buffer(factory):
        buffer = original(factory)
        buffers.append(buffer)
        return buffer

    async def replay_part(factory, context, _part, _policy, _cursor, *, store_page):
        await store_page(factory, context, _prepared_page(context))
        return 1

    async def store(*_args, **_kwargs):
        events.append("copy")
        if failure == "uncertain":
            raise ConnectionError("synthetic uncertain acknowledgement")
        return 1

    async def finish(*_args):
        events.append("finish")

    async def freeze(*_args):
        events.append("freeze")

    monkeypatch.setattr(staging, "_SourceBatch", make_buffer)
    monkeypatch.setattr(staging, "_set_timeout", AsyncMock())
    monkeypatch.setattr(staging, "_replay_part", replay_part)
    monkeypatch.setattr(staging, "_store_pages", AsyncMock(side_effect=store))
    monkeypatch.setattr(staging, "_finish_part", AsyncMock(side_effect=finish))
    monkeypatch.setattr(staging, "_page_session", lambda *_: nullcontext((object(), None)))
    monkeypatch.setattr(staging, "_call", AsyncMock(side_effect=freeze))
    return buffers


@pytest.mark.parametrize("failure", [None, "iterator", "reader", "transaction", "session", "uncertain"])
async def test_final_flush_requires_actual_outer_eof_and_successful_cleanup(monkeypatch, failure):
    context = _bulk_context()
    registry = CandidateRegistry({"details": 1}, {context.stream.stream_id: 1}, 1)
    events = []
    sessions, open_parts = _reader_contexts(context, events, failure)
    buffers = _buffered_replay(monkeypatch, events, failure)
    monkeypatch.setattr(staging, "open_segmented_parquet_parts", open_parts)
    if failure:
        with pytest.raises(ConnectionError if failure == "uncertain" else RuntimeError, match="synthetic"):
            await staging._replay_source(sessions, context.request, registry, 11, 6, None, {1: (1, 0)})
        assert "finish" not in events and "freeze" not in events
        if failure != "uncertain":
            assert "copy" not in events
    else:
        await staging._replay_source(sessions, context.request, registry, 11, 6, None, {1: (1, 0)})
        assert events == ["reader", "transaction", "session", "copy", "finish", "freeze"]
    assert len(buffers) == 1 and not buffers[0].pages and not buffers[0].verified_parts
    assert buffers[0].context is None and buffers[0].rows == buffers[0].bytes == 0


async def test_resume_compares_committed_prefix_before_buffering_suffix(monkeypatch):
    context = _bulk_context()
    prefix, suffix = _prepared_page(context), _prepared_page(context, row=1, ordinal=1)
    events = []

    async def compare(*args):
        events.append(("prefix", args[-1]))

    async def store(*args):
        events.append(("suffix", args[-1]))

    monkeypatch.setattr(staging, "_validate_replay_partition_schema", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(staging, "_source_pages", lambda *_args: (item for item in (prefix, suffix)))
    monkeypatch.setattr(staging, "_compare_committed_page", compare)
    monkeypatch.setattr(staging, "_aggregate_parquet_arrow_bytes", lambda *_args, **_kwargs: 0)
    assert (
        await staging._replay_part(
            None,
            context,
            SimpleNamespace(ordinal=1, record_count=2, capture=object()),
            _policy(),
            (1, 1, 0),
            store_page=store,
        )
        == 2
    )
    assert events == [("prefix", prefix), ("suffix", suffix)]
