# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retained decoding and driver cleanup with mocked persistence boundaries."""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import threading
from contextlib import asynccontextmanager
from dataclasses import replace
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest

from process.custom_import import https, s3, upload
from process.custom_import import prepared_source as prepared
from process.custom_import.capture import CaptureError, DecodedRecord, iter_records
from process.custom_import.object_processing_policy import ObjectProcessingPolicy
from process.custom_import.prepared_source_codec import iter_prepared_source_records
from process.custom_import.prepared_source_metadata import PreparedPartMetadata
from process.custom_import.prepared_source_progress import (
    append_progress,
    close_stream,
    empty_progress,
)
from process.custom_import.prepared_source_store import PreparedSourceState
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_capture import _capture, _stream
from tests.test_custom_import_object_processing_policy import _document
from tests.test_custom_import_object_source_binding import _definition


class _TrackedIterator:
    def __init__(self, iterator, events, name, close_failure=None):
        self.iterator = iterator
        self.events = events
        self.name = name
        self.close_failure = close_failure

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self.iterator)
        except StopIteration:
            self.events.append(f"{self.name}-iterator-exhausted")
            raise

    def close(self):
        self.events.append(f"{self.name}-close")
        self.iterator.close()
        if self.close_failure is not None:
            raise self.close_failure


def _track_decoding(monkeypatch, case):
    original_decoder = prepared._iter_verified_records
    original_parts = prepared.iter_prepared_source_parts

    def decoded(capture, stream, *, limits, _cleanup_failures):
        case.decoder_calls.append((capture, stream, limits))

        def records():
            try:
                yield from original_decoder(capture, stream, limits=limits, _cleanup_failures=_cleanup_failures)
                case.events.append("decoder-exhausted")
            except BaseException as error:
                case.decode_errors.append(error)
                raise
            finally:
                case.events.append("decoder-finalized")
                if case.reported_failure is not None:
                    _cleanup_failures.append(case.reported_failure)

        return _TrackedIterator(records(), case.events, "decoder", case.decoder_close_failure)

    def parts(*args, **kwargs):
        return _TrackedIterator(original_parts(*args, **kwargs), case.events, "parts", case.parts_close_failure)

    monkeypatch.setattr(prepared, "_iter_verified_records", decoded)
    monkeypatch.setattr(prepared, "iter_prepared_source_parts", parts)


def _mock_storage(monkeypatch, case):
    async def append(session_factory, context, *, stream_slot, part_ordinal, part):
        assert session_factory is case.sessions and context is case.context
        key = (stream_slot, part_ordinal)
        case.events.append(("append", *key))
        if key in case.committed:
            if case.committed[key] != part:
                raise CandidateRunnerError("prepared_source_part_conflict")
            case.observed.append(key)
        else:
            case.committed[key] = part
            case.written.append(key)
        if case.uncertain_ack is not None:
            failure, case.uncertain_ack = case.uncertain_ack, None
            raise failure

    async def eof(_sessions, _context, *, stream_slot):
        case.events.append(("eof", stream_slot))

    async def seal(_sessions, _context):
        case.events.append("seal")
        return case.digest

    case.load_context = AsyncMock(return_value=case.context)
    case.begin = AsyncMock(return_value=PreparedSourceState("pending", 1, empty_progress(2, case.context.policy)))
    case.append = AsyncMock(side_effect=append)
    case.eof = AsyncMock(side_effect=eof)
    case.seal = AsyncMock(side_effect=seal)
    monkeypatch.setattr(prepared, "load_prepared_source_context", case.load_context)
    monkeypatch.setattr(prepared, "begin_prepared_source", case.begin)
    monkeypatch.setattr(prepared, "append_prepared_part", case.append)
    monkeypatch.setattr(prepared, "mark_prepared_stream_eof", case.eof)
    monkeypatch.setattr(prepared, "seal_prepared_source", case.seal)


@pytest.fixture
def case(monkeypatch):
    document = _document()
    document["prepared"].update(part_row_limit=2, maximum_parts=8)
    definition = _definition()
    context = SimpleNamespace(
        execution_id=7,
        capture_bundle_id=9,
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        policy=ObjectProcessingPolicy.from_mapping(document),
        definition=definition,
        stream_slots=(("rates", 1), ("providers", 2)),
        statement_timeout_ms=1000,
    )
    captures = (
        _capture(
            b"Provider ID,Provider Name\n1003000126,One\n1003000134,Two\n1003000142,Three\n",
            definition.source_streams[0],
        ),
        _capture(gzip.compress(b""), definition.source_streams[1]),
    )
    case_state = SimpleNamespace(
        context=context,
        events=[],
        decoder_calls=[],
        decode_errors=[],
        committed={},
        observed=[],
        written=[],
        digest=b"d" * 32,
        uncertain_ack=None,
        decoder_close_failure=None,
        parts_close_failure=None,
        reported_failure=None,
        retained=AsyncMock(return_value=captures),
        session=object(),
    )

    @asynccontextmanager
    async def read_session(sessions):
        assert sessions is case_state.sessions
        case_state.events.append("originals-open")
        try:
            yield case_state.session
        finally:
            case_state.events.append("originals-close")

    case_state.sessions = Mock(side_effect=AssertionError("driver opened an unexpected session"))
    case_state.timeout = AsyncMock()
    monkeypatch.setattr(prepared, "_read_session", read_session)
    monkeypatch.setattr(prepared, "load_replayable_raw_bundle", case_state.retained)
    monkeypatch.setattr(prepared.staging, "_set_timeout", case_state.timeout)
    _mock_storage(monkeypatch, case_state)
    _track_decoding(monkeypatch, case_state)
    return case_state


async def _prepare(case):
    return await prepared.prepare_retained_source(
        case.sessions, execution_id=7, fence=11, token=b"synthetic-lease", source_permit="synthetic-permit"
    )


def _single_stream(case, payload=b"value\none\ntwo\nthree\nfour\nfive\n", *, format_name="csv"):
    stream = _stream(format_name=format_name)
    case.context.definition = SimpleNamespace(source_streams=(stream,))
    case.context.stream_slots = ((stream.stream_id, 1),)
    case.retained.return_value = (_capture(payload, stream),)
    case.begin.return_value = PreparedSourceState("pending", 1, empty_progress(1, case.context.policy))
    return stream


def _replay(case, part):
    policy = case.context.policy
    return tuple(
        iter_prepared_source_records(
            BytesIO(part.payload),
            limits=policy.capture_limits,
            part_byte_limit=policy.prepared["part_byte_limit"],
            part_row_limit=policy.prepared["part_row_limit"],
            first_source_ordinal=part.first_source_ordinal,
            record_count=part.record_count,
        )
    )


async def test_pending_decodes_retained_scope_and_appends_before_eof(case, monkeypatch):
    acquisitions = [AsyncMock(side_effect=AssertionError("source acquisition is forbidden")) for _ in range(3)]
    for module, name, blocked in zip(
        (https, s3, upload), ("acquire_https", "acquire_s3", "acquire_upload"), acquisitions, strict=True
    ):
        monkeypatch.setattr(module, name, blocked)

    assert await _prepare(case) == case.digest

    case.load_context.assert_awaited_once_with(
        case.sessions, execution_id=7, fence=11, token=b"synthetic-lease", source_permit="synthetic-permit"
    )
    case.begin.assert_awaited_once_with(case.sessions, case.context)
    case.timeout.assert_awaited_once_with(case.session, 1000)
    case.retained.assert_awaited_once_with(
        case.session,
        capture_bundle_id=9,
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        limits=case.context.policy.capture_limits,
    )
    assert case.written == [(1, 1), (2, 1), (2, 2)]
    assert case.committed[1, 1].record_count == 0 and case.committed[1, 1].payload == b""
    for slot, (capture, stream, limits) in enumerate(case.decoder_calls, 1):
        assert limits is case.context.policy.capture_limits
        decoded_records = [
            decoded_record
            for (part_slot, _), part in case.committed.items()
            if part_slot == slot
            for decoded_record in _replay(case, part)
        ]
        assert decoded_records == list(iter_records(capture, stream, limits=limits))
    assert case.events.index("originals-close") < case.events.index(("append", 1, 1))
    assert case.events[-5:] == ["parts-iterator-exhausted", "parts-close", "decoder-close", ("eof", 2), "seal"]
    assert case.eof.await_args_list == [
        call(case.sessions, case.context, stream_slot=1),
        call(case.sessions, case.context, stream_slot=2),
    ]
    for blocked in acquisitions:
        blocked.assert_not_awaited()


@pytest.mark.parametrize("state", ["closed", "sealed"])
async def test_completed_states_bypass_originals_and_decoding(case, state):
    case.begin.return_value = PreparedSourceState(state, 5, ())
    assert await _prepare(case) == case.digest
    case.retained.assert_not_awaited()
    assert case.decoder_calls == []
    case.append.assert_not_awaited()
    case.eof.assert_not_awaited()
    case.seal.assert_awaited_once_with(case.sessions, case.context)


async def test_pending_skips_persisted_eof_stream(case):
    progress = empty_progress(2, case.context.policy)
    metadata = PreparedPartMetadata(1, 1, 1, 0, 0, hashlib.sha256(b"").digest())
    progress = close_stream(append_progress(progress, metadata, case.context.policy), 1, case.context.policy)
    case.begin.return_value = PreparedSourceState("pending", 3, progress)
    assert await _prepare(case) == case.digest
    assert [stream.stream_id for _, stream, _ in case.decoder_calls] == ["providers"]
    assert case.written == [(2, 1), (2, 2)]
    case.eof.assert_awaited_once_with(case.sessions, case.context, stream_slot=2)


async def test_unknown_append_ack_replays_exact_prefix_then_suffix(case):
    _single_stream(case)
    failure = ConnectionError("synthetic uncertain acknowledgement")
    case.uncertain_ack = failure
    with pytest.raises(ConnectionError) as caught:
        await _prepare(case)
    assert caught.value is failure
    assert case.written == [(1, 1)]
    case.eof.assert_not_awaited()
    case.seal.assert_not_awaited()
    assert await _prepare(case) == case.digest
    assert case.observed == [(1, 1)]
    assert case.written == [(1, 1), (1, 2), (1, 3)]
    assert [(part.first_source_ordinal, part.record_count) for part in case.committed.values()] == [
        (1, 2),
        (3, 2),
        (5, 1),
    ]
    assert [record.ordinal for part in case.committed.values() for record in _replay(case, part)] == [1, 2, 3, 4, 5]
    assert case.events.count("decoder-exhausted") == 1


async def test_changed_prefix_conflict_stops_resume(case):
    _single_stream(case)
    case.uncertain_ack = ConnectionError("synthetic uncertain acknowledgement")
    with pytest.raises(ConnectionError):
        await _prepare(case)
    _single_stream(case, b"value\nchanged\ntwo\nthree\n")
    with pytest.raises(CandidateRunnerError, match="^prepared_source_part_conflict$"):
        await _prepare(case)
    assert case.written == [(1, 1)]
    case.eof.assert_not_awaited()
    case.seal.assert_not_awaited()


@pytest.mark.parametrize("fault", ["context", "begin", "originals"])
async def test_scope_and_retained_failures_stop_preparation(case, fault):
    failure = CandidateRunnerError("synthetic retained refusal")
    {"context": case.load_context, "begin": case.begin, "originals": case.retained}[fault].side_effect = failure
    with pytest.raises(CandidateRunnerError) as caught:
        await _prepare(case)
    assert caught.value is failure
    assert case.decoder_calls == []
    case.append.assert_not_awaited()
    case.eof.assert_not_awaited()
    case.seal.assert_not_awaited()


@pytest.mark.parametrize("damage", ["missing", "unexpected"])
async def test_original_stream_scope_mismatch_precedes_decode(case, damage):
    captures = case.retained.return_value
    unexpected = replace(captures[0], manifest=replace(captures[0].manifest, stream_id="unexpected"))
    case.retained.return_value = captures[:1] if damage == "missing" else (*captures, unexpected)
    with pytest.raises(CandidateRunnerError, match="^prepared_source_original_scope_invalid$"):
        await _prepare(case)
    assert case.decoder_calls == []
    case.append.assert_not_awaited()
    case.eof.assert_not_awaited()
    case.seal.assert_not_awaited()


@pytest.mark.parametrize("cleanup", [None, "decoder_close_failure", "reported_failure"])
async def test_decode_error_preserves_prefix_without_eof(case, cleanup):
    _single_stream(case, b'{"value":1}\n{"value":2}\n{"value":3}\ninvalid\n', format_name="ndjson")
    if cleanup is not None:
        secondary = RuntimeError("synthetic cleanup failure")
        setattr(case, cleanup, secondary)
    with pytest.raises(CaptureError) as caught:
        await _prepare(case)
    assert caught.value is case.decode_errors[0]
    if cleanup is not None:
        assert caught.value.__cause__ is secondary and caught.value._custom_import_retry_blocked
    assert case.written == [(1, 1)]
    assert case.events[-2:] == ["parts-close", "decoder-close"]
    assert "decoder-exhausted" not in case.events
    case.eof.assert_not_awaited()
    case.seal.assert_not_awaited()


@pytest.mark.parametrize("cleanup", ["decoder_close_failure", "parts_close_failure", "reported_failure"])
async def test_cleanup_failure_after_true_eof_refuses_close(case, cleanup):
    _single_stream(case)
    failure = RuntimeError("synthetic cleanup failure")
    setattr(case, cleanup, failure)
    with pytest.raises(RuntimeError) as caught:
        await _prepare(case)
    assert caught.value is failure
    assert failure._custom_import_retry_blocked
    assert case.events.count("decoder-exhausted") == 1
    assert len(case.committed) == 3
    case.eof.assert_not_awaited()
    case.seal.assert_not_awaited()


@pytest.mark.parametrize("failure_type", [RuntimeError, asyncio.CancelledError])
@pytest.mark.parametrize("cleanup", [None, "decoder_close_failure", "reported_failure"])
async def test_append_failure_remains_primary_during_cleanup(case, failure_type, cleanup):
    _single_stream(case)
    primary = failure_type("synthetic append refusal")
    case.append.side_effect = primary
    if cleanup is not None:
        secondary = RuntimeError("synthetic cleanup failure")
        setattr(case, cleanup, secondary)
    with pytest.raises(failure_type) as caught:
        await _prepare(case)
    assert caught.value is primary
    assert case.events[-3:] == ["parts-close", "decoder-close", "decoder-finalized"]
    assert "decoder-exhausted" not in case.events
    if cleanup is not None:
        assert primary.__cause__ is secondary and primary._custom_import_retry_blocked
    case.eof.assert_not_awaited()
    case.seal.assert_not_awaited()


async def test_eof_refusal_prevents_seal(case):
    _single_stream(case)
    failure = CandidateRunnerError("synthetic EOF refusal")
    case.eof.side_effect = failure
    with pytest.raises(CandidateRunnerError) as caught:
        await _prepare(case)
    assert caught.value is failure
    assert case.events[-2:] == ["parts-close", "decoder-close"]
    assert case.events.count("decoder-exhausted") == 1
    case.seal.assert_not_awaited()


async def _loop_barrier():
    completed = asyncio.get_running_loop().create_future()
    asyncio.get_running_loop().call_soon(completed.set_result, None)
    await completed


async def test_cancel_drains_worker_before_closing_decoder(case, monkeypatch):
    stream = _single_stream(case)
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    release = threading.Event()
    active = threading.Event()
    worker_finished = threading.Event()
    main_thread = threading.get_ident()

    def records():
        assert threading.get_ident() != main_thread
        active.set()
        loop.call_soon_threadsafe(started.set)
        try:
            assert release.wait(timeout=10), "worker was not released"
        finally:
            active.clear()
            worker_finished.set()
        yield DecodedRecord(1, {"value": "synthetic"})

    class Decoder(_TrackedIterator):
        def close(self):
            assert not active.is_set(), "decoder closed before worker drained"
            super().close()

    decoder = Decoder(records(), case.events, "decoder")
    monkeypatch.setattr(prepared, "_iter_verified_records", Mock(return_value=decoder))
    document = case.context.policy.to_mapping()
    document["prepared"]["part_row_limit"] = 1
    case.context.policy = ObjectProcessingPolicy.from_mapping(document)
    operation = asyncio.create_task(
        prepared._prepare_stream(case.sessions, case.context, stream, case.retained.return_value[0], 1)
    )
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        operation.cancel("synthetic first cancellation")
        await _loop_barrier()
        operation.cancel("synthetic repeated cancellation")
        await _loop_barrier()
        assert active.is_set() and not operation.done()
        assert "decoder-close" not in case.events
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError) as caught:
            await asyncio.wait_for(operation, timeout=5)
    assert caught.value.args == ("synthetic first cancellation",)
    assert caught.value._custom_import_retry_blocked
    assert worker_finished.is_set() and not active.is_set()
    assert case.events == ["parts-close", "decoder-close"]
    case.append.assert_not_awaited()
    case.eof.assert_not_awaited()
    case.seal.assert_not_awaited()
