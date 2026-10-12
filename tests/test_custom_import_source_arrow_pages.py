# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual Parquet SOURCE selection, ordered prefixes and bounded column packets."""

import time
from dataclasses import replace
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from process.custom_import import build_source as staging
from process.custom_import import bulk_page_codec, capture, source_arrow_pages, source_batch
from process.custom_import import source_column_preparation as columns
from tests import test_custom_import_source_columns as native_cases
from tests.test_custom_import_build_source import _child, _root
from tests.test_custom_import_build_source_bulk import _bulk_context
from tests.test_custom_import_snowflake_capture import _policy

_batch = native_cases._batch
encoder = native_cases.encoder
plan_factory = native_cases.plan_factory


@pytest.fixture(params=["legacy", "compiled"])
def column_mode(monkeypatch, plan_factory, request):
    monkeypatch.setattr(
        columns, "native_column_plan_factory", lambda: plan_factory if request.param == "compiled" else None
    )


def _fixture(rows, *, stream=0, page_rows=256, types=None, context=None):
    context = context or _bulk_context(stream)
    context = replace(context, request=replace(context.request, page_row_limit=page_rows, page_byte_limit=1_048_576))
    policy = _policy(part_limits=replace(_policy().part_limits, maximum_records=2048))
    batch = _batch(context, rows, types=types)
    output = BytesIO()
    pq.write_table(pa.Table.from_batches([batch]), output, row_group_size=128)
    sealed = capture.capture_stream(
        BytesIO(output.getvalue()), context.stream, source_snapshot_token="synthetic", limits=policy.part_limits
    )
    return context, SimpleNamespace(ordinal=1, record_count=len(rows), capture=sealed), policy, batch


async def _collect(context, part, policy, cursor=(1, 0, 0)):
    pages = staging._hosted_source_pages(context, part, policy, cursor)
    result, primary = [], None
    try:
        while (page := await staging._next_source_page(pages)) is not None:
            result.append(page)
    except BaseException as error:
        primary = error
    return tuple(result), await staging._close_source_pages(pages, primary)


def _landing(context, pages):
    return bulk_page_codec.encode_landing_batch(context, pages, batch_id=UUID(int=7), first_pack_ordinal=3)


def _tracked(monkeypatch, encoder):
    calls = []

    def record(descriptors):
        calls.append(descriptors[0][3])
        assert 1 <= calls[-1] <= 64

    def encode(layout, descriptors, limit, **options):
        record(descriptors)
        return encoder(layout, descriptors, limit, **options)

    monkeypatch.setattr(columns, "native_column_encoder", lambda: encode)
    factory = columns.native_column_plan_factory()
    if factory is not None:

        def compile_plan(layout):
            plan = factory(layout)

            def prepare(descriptors, limit, **options):
                record(descriptors)
                return plan.prepare(descriptors, limit, **options)

            return SimpleNamespace(prepare=prepare)

        monkeypatch.setattr(columns, "native_column_plan_factory", lambda: compile_plan)
    return calls


async def test_real_part_compiles_once_and_matches_legacy_column_abi(monkeypatch, encoder, plan_factory):
    context, part, policy, _ = _fixture([_root(score=index) for index in range(150)])
    monkeypatch.setattr(columns, "native_column_plan_factory", lambda: None)
    expected, failure = await _collect(context, part, policy)
    assert failure is None
    factory = Mock(wraps=plan_factory)
    legacy = Mock(side_effect=AssertionError("compiled part entered legacy ABI"))
    monkeypatch.setattr(columns, "native_column_encoder", lambda: legacy)
    monkeypatch.setattr(columns, "native_column_plan_factory", lambda: factory)
    for count in (1, 2):
        actual, failure = await _collect(context, part, policy)
        assert failure is None and _landing(context, actual) == _landing(context, expected)
        assert factory.call_count == count
    assert factory.call_args.args == (columns.compile_column_layout(context),)
    legacy.assert_not_called()


async def test_layout_allocation_failure_closes_decoder_without_legacy_retry(monkeypatch, encoder):
    context, part, policy, batch = _fixture([_root()] * 3)
    events = []
    failure = MemoryError("layout allocation")

    def decoded(*_args, **_kwargs):
        try:
            events.append("first row")
            yield batch.slice(0, 1)
            raise AssertionError("decoder advanced after layout allocation failure")
        finally:
            events.append("closed")

    factory = Mock(side_effect=failure)
    legacy = Mock(side_effect=AssertionError("failed layout entered legacy ABI"))
    monkeypatch.setattr(source_arrow_pages, "iter_parquet_source_batches", decoded)
    monkeypatch.setattr(columns, "native_column_plan_factory", lambda: factory)
    monkeypatch.setattr(columns, "native_column_encoder", lambda: legacy)
    pages, error = await _collect(context, part, policy)
    assert error is failure and not pages and events == ["first row", "closed"]
    factory.assert_called_once()
    legacy.assert_not_called()


@pytest.mark.parametrize("stream", [0, 1])
async def test_real_parquet_matches_existing_native_writer_input_without_python_rows(
    monkeypatch, encoder, stream, column_mode
):
    rows = [_root(score=index) if stream == 0 else _child(key=str(index), amount=index) for index in range(1000)]
    context, part, policy, _ = _fixture(rows, stream=stream)
    monkeypatch.setattr(columns, "native_column_encoder", lambda: None)
    expected, failure = await _collect(context, part, policy)
    assert failure is None
    calls = _tracked(monkeypatch, encoder)
    monkeypatch.setattr(columns, "_decoded_arrow_row", Mock(side_effect=AssertionError("Python row conversion")))
    actual, failure = await _collect(context, part, policy)
    assert failure is None and calls == [1] * 1000
    assert _landing(context, actual) == _landing(context, expected)


async def test_mixed_rejections_preserve_every_landing_column(monkeypatch, encoder, column_mode):
    context, part, policy, _ = _fixture([_root(), _root(npi=None), _root(npi="invalid"), _root(score=None)])
    monkeypatch.setattr(columns, "native_column_encoder", lambda: None)
    expected, failure = await _collect(context, part, policy)
    assert failure is None
    _tracked(monkeypatch, encoder)
    actual, failure = await _collect(context, part, policy)
    assert failure is None and _landing(context, actual) == _landing(context, expected)


async def test_resumed_prefix_uses_the_existing_serial_comparison(monkeypatch, encoder, column_mode):
    context, part, policy, _ = _fixture([_root()] * 3, page_rows=1)
    calls = _tracked(monkeypatch, encoder)
    actual, failure = await _collect(context, part, policy, cursor=(1, 1, 0))
    assert failure is None and not calls and sum(len(page.records) for page in actual) == 3


@pytest.mark.parametrize("failure", [capture.CaptureError("late decode"), MemoryError("late allocation")])
async def test_late_decoder_failure_retains_ordinary_prefix_and_fatal_allocation_policy(
    monkeypatch, encoder, failure, column_mode
):
    context, part, policy, batch = _fixture([_root()] * 3, page_rows=1)
    events = []

    def decoded(*_args, **_kwargs):
        try:
            yield from (batch.slice(index, 1) for index in range(3))
            raise failure
        finally:
            events.append("closed")

    monkeypatch.setattr(source_arrow_pages, "iter_parquet_source_batches", decoded)
    calls = _tracked(monkeypatch, encoder)
    actual, error = await _collect(context, part, policy)
    assert error is failure and events == ["closed"]
    assert len(actual) == (0 if isinstance(failure, MemoryError) else 2)
    assert calls == [1] * 3


async def test_part_eof_requires_the_actual_receipt_count(monkeypatch, encoder, column_mode):
    context, part, policy, _ = _fixture([_root()] * 3)
    part.record_count += 1
    _tracked(monkeypatch, encoder)
    _, error = await _collect(context, part, policy)
    assert isinstance(error, staging.CandidateRunnerError)
    assert str(error) == "decoded part record count differs from the sealed receipt"


@pytest.mark.parametrize("before_prefix", [False, True])
async def test_native_preparation_deadline_discards_the_batch(monkeypatch, encoder, before_prefix, column_mode):
    from tests.test_custom_import_source_batch import _CURSOR, _PERMIT

    context, part, policy, _ = _fixture([_root()])
    part.arrow_byte_count = staging._aggregate_parquet_arrow_bytes(
        part.capture, limits=policy.part_limits, decoded_bytes=0
    )
    calls = _tracked(monkeypatch, encoder)
    times = iter((60,) if before_prefix else (59, 60))
    monkeypatch.setattr(source_batch, "time", SimpleNamespace(monotonic=lambda: next(times)))
    with pytest.raises(staging.LeaseAuthorityLost, match="read deadline elapsed"):
        await source_batch._prepare_part(None, context, part, policy, _CURSOR, _PERMIT, 60)
    assert bool(calls) is not before_prefix


async def test_decimal256_preserves_the_complete_existing_serializer_route(monkeypatch, encoder, column_mode):
    context, part, policy, _ = _fixture([_root()] * 100, types={"score": pa.decimal256(50, 12)})
    monkeypatch.setattr(columns, "native_column_encoder", lambda: None)
    expected, failure = await _collect(context, part, policy)
    assert failure is None
    calls = _tracked(monkeypatch, encoder)
    actual, failure = await _collect(context, part, policy)
    assert failure is None and not calls and _landing(context, actual) == _landing(context, expected)


@pytest.mark.parametrize("stream", [0, 1])
async def test_reordered_schema_preserves_native_reader_prefix(monkeypatch, encoder, column_mode, stream):
    import inspect
    import json
    from contextlib import closing

    from process.custom_import import source_preparation
    from process.custom_import.definition import CustomImportDefinition
    from tests.test_custom_import_source_batch import _CURSOR, _PERMIT

    source_row = _root() if stream == 0 else _child()
    context, part, policy, batch = _fixture([source_row] * 3, stream=stream, page_rows=1)
    document = json.loads(context.request.definition.canonical)
    schema = document["schema"]["root"] if stream == 0 else document["schema"]["children"][0]
    schema["fields"].reverse()
    definition = CustomImportDefinition.from_mapping(document)
    context = replace(
        context, request=replace(context.request, definition=definition), stream=definition.source_streams[stream]
    )
    layout = columns.compile_column_layout(context)
    assert tuple(batch.schema.names) == tuple(
        field.field_id for field in staging._stream_fields(definition, context.stream)
    )
    assert tuple(batch.schema.names) != tuple(field[0] for field in layout[1])
    assert inspect.isbuiltin(source_preparation.native_encoder())
    original_records = staging.iter_records
    original_batches = source_arrow_pages.iter_parquet_source_batches
    failure = MemoryError("later decoder allocation")

    for use_columns in (False, True):
        reads = []

        def decoded(iterator):
            with closing(iterator):
                for ordinal, row in enumerate(iterator):
                    reads.append(ordinal)
                    if ordinal == 2:
                        raise failure
                    yield row

        monkeypatch.setattr(staging, "iter_records", lambda *args, **kwargs: decoded(original_records(*args, **kwargs)))
        monkeypatch.setattr(
            source_arrow_pages,
            "iter_parquet_source_batches",
            lambda *args, **kwargs: decoded(original_batches(*args, **kwargs)),
        )
        monkeypatch.setattr(columns, "native_column_encoder", lambda: encoder if use_columns else None)
        deadline = time.monotonic() + 60
        # A one-row fallback would stop after two reads and hide this failure.
        with pytest.raises(MemoryError) as caught:
            await source_batch._prepare_part(
                None, context, part, policy, _CURSOR, _PERMIT, deadline, budget=(1, 1_048_576, deadline)
            )
        assert caught.value is failure and reads == [0, 1, 2]


async def test_late_invalid_utf8_is_not_sent_to_native(monkeypatch, encoder, column_mode):
    context, part, policy, batch = _fixture([_root()] * 3, page_rows=1)
    bad = pa.Array.from_buffers(pa.string(), 1, [None, pa.py_buffer(b"\0\0\0\0\1\0\0\0"), pa.py_buffer(b"\xff")])
    invalid = pa.record_batch([bad, batch.column(1).slice(0, 1), batch.column(2).slice(0, 1)], names=batch.schema.names)

    def decoded(*_args, **_kwargs):
        yield batch.slice(0, 1)
        yield batch.slice(1, 1)
        yield invalid

    monkeypatch.setattr(source_arrow_pages, "iter_parquet_source_batches", decoded)
    calls = _tracked(monkeypatch, encoder)
    actual, failure = await _collect(context, part, policy)
    assert len(actual) == 1 and calls == [1, 1]
    assert isinstance(failure, capture.CaptureError) and str(failure) == "Parquet source payload is invalid"


async def test_integer_transport_error_precedes_later_decoder_oom_with_exact_page_prefix(
    monkeypatch, encoder, column_mode
):
    from process.custom_import.snowflake_bundle import SnowflakeBundleError
    from tests.test_custom_import_source_batch import _CURSOR, _PERMIT

    source_rows = [_root(score=7)] * 200
    source_rows[129] = _root(score=2**63)
    context, part, policy, batch = _fixture(
        source_rows, page_rows=2, context=native_cases._context(integer=True), types={"score": pa.decimal128(38, 0)}
    )
    page_prefixes, reader_positions = [], []
    original_next = staging._next_source_page

    for use_columns in (False, True):
        emitted_pages, reads = [], []

        def batches(*_args, **_kwargs):
            for index in range(140):
                reads.append(index)
                yield batch.slice(index, 1)
            raise MemoryError("later decode allocation")

        def records(*_args, **_kwargs):
            for index, decoded_batch in enumerate(batches()):
                yield columns._decoded_arrow_row(decoded_batch, 0, index + 1, policy.part_limits)

        async def observed_next(pages):
            page = await original_next(pages)
            if page is not None:
                emitted_pages.append(
                    tuple(staging._prepared_fingerprint(page, index) for index in range(len(page.records)))
                )
            return page

        monkeypatch.setattr(staging, "iter_records", records)
        monkeypatch.setattr(staging, "_next_source_page", observed_next)
        monkeypatch.setattr(source_arrow_pages, "iter_parquet_source_batches", batches)
        monkeypatch.setattr(columns, "native_column_encoder", lambda: encoder if use_columns else None)
        with pytest.raises(SnowflakeBundleError, match="exceeds the signed 64-bit range"):
            await source_batch._prepare_part(None, context, part, policy, _CURSOR, _PERMIT, time.monotonic() + 60)
        page_prefixes.append(emitted_pages)
        reader_positions.append(reads)
    assert page_prefixes[0] == page_prefixes[1]
    assert sum(map(len, page_prefixes[0])) == 128
    assert reader_positions[0] == reader_positions[1] == list(range(130))


@pytest.mark.parametrize("stream", [0, 1])
@pytest.mark.parametrize("scale", [-2, 0, 7, 12])
def test_native_bounds_match_existing_scheduler_and_cover_prepared_documents(encoder, stream, scale):
    from decimal import Decimal

    context = native_cases._context(stream=stream, key_decimal=True)
    decimal_value = Decimal("1E-7") if scale >= 7 else Decimal("1.20E+4")
    source_rows = (
        [_root(score=decimal_value), _root(score=None), _root(npi='Ω\n\0"\\' * 20, score=decimal_value)]
        if stream == 0
        else [
            _child(key='Ω\n\0"\\' * 20, amount=decimal_value),
            _child(key=None, amount=decimal_value),
            _child(amount=None),
        ]
    )
    batch = _batch(context, source_rows, types={"score" if stream == 0 else "amount": pa.decimal128(38, scale)})
    layout = columns.compile_column_layout(context)
    from process.custom_import import source_preparation

    old_layout = source_preparation.native_layout(context.request.definition, context.stream)
    fields = staging._stream_fields(context.request.definition, context.stream)
    outcomes = encoder(
        layout,
        columns.column_descriptors(layout, batch),
        context.request.page_byte_limit,
        include_preparation_bounds=True,
    )
    for index, (outcome, input_bytes, output_bytes) in enumerate(outcomes):
        decoded = columns._decoded_arrow_row(batch, index, 1, capture.CaptureLimits())
        original = source_preparation._native_row(context, fields, old_layout, decoded.values)
        if original is None:
            assert input_bytes == output_bytes == 0
        else:
            assert (input_bytes, output_bytes) == (original.input_bytes, original.output_bytes)
            assert outcome[8] <= output_bytes


@pytest.mark.parametrize("stage", ["validate", "native"])
async def test_safety_probe_allocation_failure_never_flushes_or_retries(monkeypatch, encoder, stage, column_mode):
    context, part, policy, batch = _fixture(
        [_root(score=7)] * 3, context=native_cases._context(integer=True), types={"score": pa.decimal128(38, 0)}
    )
    failure = pa.ArrowMemoryError("safety probe allocation")
    events = []

    def decoded(*_args, **_kwargs):
        try:
            yield batch.slice(0, 1)
            events.append("later decode")
            raise AssertionError("decoder advanced after allocation failure")
        finally:
            events.append("closed")

    if stage == "validate":
        monkeypatch.setattr(source_arrow_pages, "_is_safe_column_batch", Mock(side_effect=failure))
    else:
        monkeypatch.setattr(columns, "column_row_result", Mock(side_effect=failure))
    monkeypatch.setattr(source_arrow_pages, "iter_parquet_source_batches", decoded)
    calls = _tracked(monkeypatch, encoder)
    pages, error = await _collect(context, part, policy)
    assert error is failure and not pages and not calls and events == ["closed"]


@pytest.mark.parametrize("bound", ["capture", "raw_key", "occurrence"])
async def test_early_byte_limit_error_wins_over_later_decoder_oom(monkeypatch, encoder, bound, column_mode):
    from process.custom_import.family_raw_key import RawFamilyKeyError

    if bound == "occurrence":
        source_rows = [_child(amount="2.00")] * 200
        source_rows[129] = _child(key="x" * 25_000, amount="2.00")
        context, part, policy, batch = _fixture(source_rows, stream=1, page_rows=2, types={"amount": pa.string()})
    else:
        source_rows = [_root(score="1.00")] * 200
        source_rows[129] = _root(npi="x" * 25_000, score="1.00")
        context, part, policy, batch = _fixture(source_rows, page_rows=2, types={"score": pa.string()})
    policy = replace(policy, part_limits=replace(policy.part_limits, maximum_record_bytes=65_536))
    if bound == "capture":
        policy = replace(policy, part_limits=replace(policy.part_limits, maximum_record_bytes=256))
    else:
        context = replace(context, request=replace(context.request, page_byte_limit=20_000))
    prefixes, positions, failures = [], [], []
    for use_columns in (False, True):
        reads = []

        def decoded(*_args, **_kwargs):
            for index in range(140):
                reads.append(index)
                yield batch.slice(index, 1)
            raise MemoryError("later decode allocation")

        def records(*_args, **_kwargs):
            for index, row in enumerate(decoded()):
                yield columns._decoded_arrow_row(row, 0, index + 1, policy.part_limits)

        monkeypatch.setattr(staging, "iter_records", records)
        monkeypatch.setattr(source_arrow_pages, "iter_parquet_source_batches", decoded)
        monkeypatch.setattr(columns, "native_column_encoder", lambda: encoder if use_columns else None)
        pages, error = await _collect(context, part, policy)
        prefixes.append(
            tuple(staging._prepared_fingerprint(page, index) for page in pages for index in range(len(page.records)))
        )
        positions.append(reads)
        failures.append(error)
    expected_error = {
        "capture": capture.CaptureError,
        "raw_key": RawFamilyKeyError,
        "occurrence": staging.CandidateRunnerError,
    }[bound]
    assert type(failures[0]) is type(failures[1]) is expected_error
    assert str(failures[0]) == str(failures[1])
    assert prefixes[0] == prefixes[1] and len(prefixes[0]) == 128
    assert positions[0] == positions[1] == list(range(130))


async def test_rejection_barrier_preserves_soft_stop_before_future_decoder_failure(monkeypatch, encoder, column_mode):
    from tests.test_custom_import_source_batch import _CURSOR, _PERMIT

    context, part, policy, batch = _fixture([_root(), _root(npi="invalid"), _root()], page_rows=1)
    for use_columns in (False, True):
        reads = []

        def decoded(*_args, **_kwargs):
            for index in range(2):
                reads.append(index)
                yield batch.slice(index, 1)
            raise MemoryError("unread future decoder failure")

        def records(*_args, **_kwargs):
            for index, row in enumerate(decoded()):
                yield columns._decoded_arrow_row(row, 0, index + 1, policy.part_limits)

        monkeypatch.setattr(staging, "iter_records", records)
        monkeypatch.setattr(source_arrow_pages, "iter_parquet_source_batches", decoded)
        monkeypatch.setattr(columns, "native_column_encoder", lambda: encoder if use_columns else None)
        deadline = time.monotonic() + 60
        pages, eof = await source_batch._prepare_part(
            None, context, part, policy, _CURSOR, _PERMIT, deadline, budget=(1, 1_048_576, deadline)
        )
        assert len(pages) == 1 and len(pages[0].records) == 1 and not eof
        assert reads == [0, 1]


@pytest.mark.parametrize("compiled", [False, True])
async def test_native_handoff_drains_before_cancelled_decoder_cleanup(monkeypatch, encoder, plan_factory, compiled):
    import asyncio
    from threading import Event

    context, part, policy, batch = _fixture([_root()] * 3)
    started, release = Event(), Event()
    events = []

    def decoded(*_args, **_kwargs):
        try:
            yield from (batch.slice(index, 1) for index in range(3))
        finally:
            events.append("decoder closed")

    def blocked_call(prepare, *args, **kwargs):
        started.set()
        assert release.wait(5)
        result = prepare(*args, **kwargs)
        events.append("native complete")
        return result

    def compiled_plan(layout):
        plan = plan_factory(layout)
        return SimpleNamespace(prepare=lambda *args, **kwargs: blocked_call(plan.prepare, *args, **kwargs))

    def blocked_encoder(*args, **kwargs):
        return blocked_call(encoder, *args, **kwargs)

    monkeypatch.setattr(source_arrow_pages, "iter_parquet_source_batches", decoded)
    monkeypatch.setattr(columns, "native_column_encoder", lambda: blocked_encoder)
    monkeypatch.setattr(columns, "native_column_plan_factory", lambda: compiled_plan if compiled else None)
    task = asyncio.create_task(_collect(context, part, policy))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    pages, error = await task
    assert not pages and isinstance(error, asyncio.CancelledError)
    assert events == ["native complete"] * 3 + ["decoder closed"]


@pytest.mark.parametrize("cap", ["input", "output"])
async def test_byte_flush_preserves_original_soft_stop_reader_position(monkeypatch, encoder, cap, column_mode):
    from process.custom_import import source_preparation
    from tests.test_custom_import_source_batch import _CURSOR, _PERMIT

    source_rows = [_child(key="x" * 4000)] * 100 if cap == "input" else [_child()] * 100
    context, part, policy, batch = _fixture(source_rows, stream=1, page_rows=1)
    if cap == "output":
        monkeypatch.setattr(source_preparation, "MAX_OUTPUT_BYTES", 5000)
        monkeypatch.setattr(source_arrow_pages, "MAX_OUTPUT_BYTES", 5000)
    prefixes, positions = [], []
    for use_columns in (False, True):
        reads = []

        def decoded(*_args, **_kwargs):
            for index in range(90):
                reads.append(index)
                yield batch.slice(index, 1)
            raise MemoryError("unread future allocation")

        def records(*_args, **_kwargs):
            for index, row in enumerate(decoded()):
                yield columns._decoded_arrow_row(row, 0, index + 1, policy.part_limits)

        monkeypatch.setattr(staging, "iter_records", records)
        monkeypatch.setattr(source_arrow_pages, "iter_parquet_source_batches", decoded)
        monkeypatch.setattr(columns, "native_column_encoder", lambda: encoder if use_columns else None)
        deadline = time.monotonic() + 60
        pages, eof = await source_batch._prepare_part(
            None, context, part, policy, _CURSOR, _PERMIT, deadline, budget=(1, 1_048_576, deadline)
        )
        assert len(pages) == 1 and not eof
        prefixes.append(staging._prepared_fingerprint(pages[0], 0))
        positions.append(reads)
    assert prefixes[0] == prefixes[1] and positions[0] == positions[1]
    assert 1 < len(positions[0]) < 65


async def test_occurrence_error_retains_its_original_deferred_flush_before_later_oom(monkeypatch, encoder, column_mode):
    context, part, policy, batch = _fixture([_root()] * 3)
    context = replace(context, request=replace(context.request, page_byte_limit=250))
    for use_columns in (False, True):

        def decoded(*_args, **_kwargs):
            yield from (batch.slice(index, 1) for index in range(3))
            raise MemoryError("later allocation before the native finishing boundary")

        def records(*_args, **_kwargs):
            for index, row in enumerate(decoded()):
                yield columns._decoded_arrow_row(row, 0, index + 1, policy.part_limits)

        monkeypatch.setattr(staging, "iter_records", records)
        monkeypatch.setattr(source_arrow_pages, "iter_parquet_source_batches", decoded)
        monkeypatch.setattr(columns, "native_column_encoder", lambda: encoder if use_columns else None)
        pages, error = await _collect(context, part, policy)
        assert not pages and isinstance(error, MemoryError)
        assert str(error) == "later allocation before the native finishing boundary"


async def test_wider_column_abi_preselects_the_complete_old_route(monkeypatch, encoder, column_mode):
    import json

    from process.custom_import.definition import CustomImportDefinition

    context = _bulk_context()
    document = json.loads(context.request.definition.canonical)
    values = _root()
    for slot in range(8, 37):
        name = f"extra_{slot}"
        document["schema"]["root"]["fields"].append({"id": name, "slot": slot, "type": "string", "nullable": False})
        values[name] = "x" * 3900
    definition = CustomImportDefinition.from_mapping(document)
    context = replace(
        context, request=replace(context.request, definition=definition), stream=definition.source_streams[0]
    )
    context, part, policy, _ = _fixture([values], context=context)
    policy = replace(
        policy,
        part_limits=replace(policy.part_limits, maximum_record_bytes=262_144, maximum_decoded_bytes=8 * 1024**2),
    )
    calls = _tracked(monkeypatch, encoder)
    pages, error = await _collect(context, part, policy)
    assert error is None and not calls and len(pages) == 1
