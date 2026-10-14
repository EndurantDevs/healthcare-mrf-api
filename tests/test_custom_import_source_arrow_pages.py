# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual Parquet SOURCE selection, ordered prefixes and bounded column packets."""

import json
import time
from dataclasses import replace
from decimal import Decimal
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from process.custom_import import build_source as staging
from process.custom_import import bulk_page_codec, capture, source_arrow_pages, source_batch
from process.custom_import import source_column_preparation as columns
from process.custom_import.definition import CustomImportDefinition
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


def _fixture(rows, *, stream=0, page_rows=256, types=None, context=None, reordered=False):
    context = context or _bulk_context(stream)
    context = replace(context, request=replace(context.request, page_row_limit=page_rows, page_byte_limit=1_048_576))
    policy = _policy(part_limits=replace(_policy().part_limits, maximum_records=2048))
    batch = _batch(context, rows, types=types)
    output = BytesIO()
    pq.write_table(pa.Table.from_batches([batch]), output, row_group_size=128)
    sealed = capture.capture_stream(
        BytesIO(output.getvalue()), context.stream, source_snapshot_token="synthetic", limits=policy.part_limits
    )
    if reordered:
        document = json.loads(context.request.definition.canonical)
        schema = document["schema"]["root"] if stream == 0 else document["schema"]["children"][0]
        schema["fields"].reverse()
        definition = CustomImportDefinition.from_mapping(document)
        context = replace(
            context, request=replace(context.request, definition=definition), stream=definition.source_streams[stream]
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

    def record(layout, descriptors):
        assert tuple(column[0] for column in descriptors) == tuple(field[0] for field in layout[1])
        calls.append(descriptors[0][3])
        assert 1 <= calls[-1] <= 64

    def encode(layout, descriptors, limit, **options):
        record(layout, descriptors)
        return encoder(layout, descriptors, limit, **options)

    monkeypatch.setattr(columns, "native_column_encoder", lambda: encode)
    factory = columns.native_column_plan_factory()
    if factory is not None:

        def compile_plan(layout):
            plan = factory(layout)

            def prepare(descriptors, limit, **options):
                record(layout, descriptors)
                return plan.prepare(descriptors, limit, **options)

            return SimpleNamespace(prepare=prepare)

        monkeypatch.setattr(columns, "native_column_plan_factory", lambda: compile_plan)
    return calls


@pytest.mark.parametrize("stream", [0, 1])
@pytest.mark.parametrize("reordered", [False, True])
async def test_real_part_compiles_once_and_matches_legacy_column_abi(
    monkeypatch, encoder, plan_factory, stream, reordered
):
    rows = [_root(score=index) if stream == 0 else _child(key=str(index), amount=index) for index in range(150)]
    context, part, policy, _ = _fixture(rows, stream=stream, reordered=reordered)
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
@pytest.mark.parametrize("reordered", [False, True])
async def test_real_parquet_matches_existing_native_writer_input_without_python_rows(
    monkeypatch, encoder, stream, column_mode, reordered
):
    rows = [_root(score=index) if stream == 0 else _child(key=str(index), amount=index) for index in range(1000)]
    context, part, policy, _ = _fixture(rows, stream=stream, reordered=reordered)
    monkeypatch.setattr(columns, "native_column_encoder", lambda: None)
    expected, failure = await _collect(context, part, policy)
    assert failure is None
    calls = _tracked(monkeypatch, encoder)
    monkeypatch.setattr(columns, "_decoded_arrow_row", Mock(side_effect=AssertionError("Python row conversion")))
    actual, failure = await _collect(context, part, policy)
    assert failure is None and sum(calls) == 1000 and max(calls) > 1
    assert _landing(context, actual) == _landing(context, expected)


@pytest.mark.parametrize("stream", [0, 1])
@pytest.mark.parametrize("reordered", [False, True])
async def test_mixed_rejections_preserve_every_landing_column(monkeypatch, encoder, column_mode, stream, reordered):
    rows = (
        [_root(), _root(npi=None), _root(npi="invalid"), _root(score=None)]
        if stream == 0
        else [_child(), _child(npi=None), _child(key=None), _child(amount=None)]
    )
    context, part, policy, _ = _fixture(rows, stream=stream, reordered=reordered)
    monkeypatch.setattr(columns, "native_column_encoder", lambda: None)
    expected, failure = await _collect(context, part, policy)
    assert failure is None
    calls = _tracked(monkeypatch, encoder)
    hydrated = Mock(wraps=columns._decoded_arrow_row)
    monkeypatch.setattr(columns, "_decoded_arrow_row", hydrated)
    actual, failure = await _collect(context, part, policy)
    assert failure is None and calls == [4] and hydrated.call_count == 3
    assert _landing(context, actual) == _landing(context, expected)


@pytest.mark.parametrize("reordered", [False, True])
async def test_resumed_prefix_uses_the_existing_serial_comparison(monkeypatch, encoder, column_mode, reordered):
    context, part, policy, _ = _fixture([_root()] * 3, page_rows=1, reordered=reordered)
    calls = _tracked(monkeypatch, encoder)
    actual, failure = await _collect(context, part, policy, cursor=(1, 1, 0))
    assert failure is None and not calls and sum(len(page.records) for page in actual) == 3


def _hosted_call(monkeypatch, context, part, policy):
    from tests.test_custom_import_source_batch import _CURSOR, _PERMIT

    part.receipt = SimpleNamespace(canonical_manifest='{"part_count":1}')
    monkeypatch.setattr(
        source_batch, "_load_context", AsyncMock(return_value=(context, policy, 9, time.monotonic() + 60, False))
    )
    monkeypatch.setattr(source_batch, "_fetch_closed_part", AsyncMock(return_value=part))
    monkeypatch.setattr(source_batch, "_verify_metadata", AsyncMock())
    commit = AsyncMock()
    monkeypatch.setattr(source_batch, "_commit", commit)
    return source_batch.serve_source_batch(
        None,
        execution_id=4,
        build_id=11,
        fence=1,
        stream_slot=context.stream_slot,
        expected_cursor=_CURSOR,
        lease_token=context.request.lease_token,
        source_permit=_PERMIT,
    ), commit


@pytest.mark.parametrize("stage", ["decoder", "allocation", "native", "cleanup"])
async def test_observed_batch_failure_discards_buffered_pages_before_commit(monkeypatch, encoder, column_mode, stage):
    context, part, policy, batch = _fixture([_root()] * 70, page_rows=7)
    failure = MemoryError("late allocation") if stage == "allocation" else RuntimeError("observed batch failure")
    events, buffered_pages = [], []
    next_page = staging._next_source_page

    async def observed_page(pages):
        page = await next_page(pages)
        if page is not None:
            buffered_pages.append(page)
        return page

    def decoded(*_args, **_kwargs):
        try:
            yield batch.slice(0, 64)
            if stage in {"decoder", "allocation"}:
                raise failure
            yield batch.slice(64)
        finally:
            events.append("decoder closed")
            if stage == "cleanup":
                raise failure

    calls = _tracked(monkeypatch, encoder)
    native_call = columns._native_column_outcomes

    async def prepare(*args, **kwargs):
        if stage == "native" and calls:
            raise failure
        return await native_call(*args, **kwargs)

    monkeypatch.setattr(staging, "_next_source_page", observed_page)
    monkeypatch.setattr(source_arrow_pages, "iter_parquet_source_batches", decoded)
    monkeypatch.setattr(columns, "_native_column_outcomes", prepare)
    operation, commit = _hosted_call(monkeypatch, context, part, policy)
    with pytest.raises(type(failure)) as caught:
        await operation
    assert caught.value is failure and buffered_pages and calls[0] == 64
    assert events == ["decoder closed"]
    commit.assert_not_awaited()


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
@pytest.mark.parametrize("bound", ["rows", "bytes"])
async def test_multirow_soft_stop_preserves_logical_pages_and_committed_cursor(
    monkeypatch, encoder, column_mode, stream, bound
):
    from tests.test_custom_import_source_batch import _CURSOR, _PERMIT, _CommitHarness

    source_rows = [_root(score=index) if stream == 0 else _child(key=str(index)) for index in range(100)]
    context, part, policy, _ = _fixture(source_rows, stream=stream, page_rows=7, reordered=True)
    deadline = time.monotonic() + 60
    budget = (21, 10_485_760, deadline) if bound == "rows" else (100, context.request.page_byte_limit, deadline)
    monkeypatch.setattr(columns, "native_column_encoder", lambda: None)
    expected, expected_eof = await source_batch._prepare_part(
        None, context, part, policy, _CURSOR, _PERMIT, deadline, budget=budget
    )
    calls = _tracked(monkeypatch, encoder)
    actual, eof = await source_batch._prepare_part(
        None, context, part, policy, _CURSOR, _PERMIT, deadline, budget=budget
    )
    assert not eof and not expected_eof and calls and max(calls) > 1
    assert _landing(context, actual) == _landing(context, expected)
    row_count = sum(len(page.records) for page in actual)
    assert row_count == (21 if bound == "rows" else 7)
    receipts = []
    for pages in (expected, actual):
        harness = _CommitHarness(monkeypatch)
        harness.context = context
        receipt = await source_batch._commit(None, context, pages, (), _CURSOR, 9, _PERMIT)
        assert harness.events[-1] == "commit"
        receipts.append(receipt)
    assert receipts[0] == receipts[1]
    assert receipts[1].after == source_batch.SourceCursor(1, row_count, row_count, len(actual))


@pytest.mark.parametrize("stream", [0, 1])
async def test_declaration_order_cannot_replace_the_exact_retained_schema(monkeypatch, encoder, column_mode, stream):
    context, part, policy, batch = _fixture([_root() if stream == 0 else _child()], stream=stream, reordered=True)
    wrong_order = batch.select(tuple(reversed(range(batch.num_columns))))
    assert tuple(wrong_order.schema.names) == tuple(field[0] for field in columns.compile_column_layout(context)[1])
    monkeypatch.setattr(
        source_arrow_pages, "iter_parquet_source_batches", lambda *_args, **_kwargs: iter((wrong_order,))
    )
    calls = _tracked(monkeypatch, encoder)
    pages, failure = await _collect(context, part, policy)
    assert not pages and not calls and isinstance(failure, staging.CandidateRunnerError)
    assert str(failure) == "replayed fields differ from the retained stream"


@pytest.mark.usefixtures("column_mode")
@pytest.mark.parametrize("reordered", [False, True])
@pytest.mark.parametrize(
    "integer_values,scales,message",
    [
        ((7, 8), (0, 0), None),
        ((2**63, 2**63), (0, 0), "bundle replay integer result exceeds the signed 64-bit range"),
        ((Decimal("1.5"), 2**63), (1, 0), "bundle replay integer result is invalid"),
        ((2**63, Decimal("1.5")), (0, 1), "bundle replay integer result exceeds the signed 64-bit range"),
    ],
)
async def test_integer_transports_preserve_retained_error_order(
    monkeypatch, encoder, integer_values, scales, message, reordered
):
    from process.custom_import.snowflake_bundle import SnowflakeBundleError

    context = native_cases._context(integer=True)
    document = json.loads(context.request.definition.canonical)
    document["schema"]["root"]["fields"].append({"id": "sequence", "slot": 8, "type": "integer", "nullable": False})
    definition = CustomImportDefinition.from_mapping(document)
    context = replace(
        context, request=replace(context.request, definition=definition), stream=definition.source_streams[0]
    )
    context, part, policy, _ = _fixture(
        [dict(_root(score=integer_values[0]), sequence=integer_values[1])],
        context=context,
        types={"score": pa.decimal128(38, scales[0]), "sequence": pa.decimal128(38, scales[1])},
        reordered=reordered,
    )
    monkeypatch.setattr(columns, "native_column_encoder", lambda: None)
    expected, expected_failure = await _collect(context, part, policy)
    calls = _tracked(monkeypatch, encoder)
    use_native = not (reordered and any(scales))
    if use_native:
        monkeypatch.setattr(columns, "_decoded_arrow_row", Mock(side_effect=AssertionError("Python row conversion")))
    actual, failure = await _collect(context, part, policy)
    assert calls == ([1] if use_native else [])
    if message is None:
        assert expected_failure is failure is None and _landing(context, actual) == _landing(context, expected)
    else:
        assert type(expected_failure) is type(failure) is SnowflakeBundleError
        assert str(expected_failure) == str(failure) == message


@pytest.mark.parametrize("reordered", [False, True])
async def test_late_invalid_utf8_is_not_sent_to_native(monkeypatch, encoder, column_mode, reordered):
    context, part, policy, batch = _fixture([_root()] * 3, page_rows=1, reordered=reordered)
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


@pytest.mark.parametrize("reordered", [False, True])
async def test_integer_transport_error_precedes_later_decoder_oom_with_exact_page_prefix(
    monkeypatch, encoder, column_mode, reordered
):
    from process.custom_import.snowflake_bundle import SnowflakeBundleError
    from tests.test_custom_import_source_batch import _CURSOR, _PERMIT

    source_rows = [_root(score=7)] * 200
    source_rows[129] = _root(score=2**63)
    context, part, policy, batch = _fixture(
        source_rows,
        page_rows=2,
        context=native_cases._context(integer=True),
        types={"score": pa.decimal128(38, 0)},
        reordered=reordered,
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
        monkeypatch.setattr(columns, "column_descriptors", Mock(side_effect=failure))
    monkeypatch.setattr(source_arrow_pages, "iter_parquet_source_batches", decoded)
    calls = _tracked(monkeypatch, encoder)
    pages, error = await _collect(context, part, policy)
    assert error is failure and not pages and not calls and events == ["closed"]


@pytest.mark.parametrize("bound", ["capture", "raw_key", "occurrence"])
@pytest.mark.parametrize("reordered", [False, True])
async def test_early_byte_limit_error_wins_over_later_decoder_oom(monkeypatch, encoder, bound, column_mode, reordered):
    from process.custom_import.family_raw_key import RawFamilyKeyError

    if bound == "occurrence":
        source_rows = [_child(amount="2.00")] * 200
        source_rows[129] = _child(key="x" * 25_000, amount="2.00")
        context, part, policy, batch = _fixture(
            source_rows, stream=1, page_rows=2, types={"amount": pa.string()}, reordered=reordered
        )
    else:
        source_rows = [_root(score="1.00")] * 200
        source_rows[129] = _root(npi="x" * 25_000, score="1.00")
        context, part, policy, batch = _fixture(
            source_rows, page_rows=2, types={"score": pa.string()}, reordered=reordered
        )
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


@pytest.mark.parametrize("reordered", [False, True])
async def test_rejection_barrier_preserves_soft_stop_before_future_decoder_failure(
    monkeypatch, encoder, column_mode, reordered
):
    from tests.test_custom_import_source_batch import _CURSOR, _PERMIT

    context, part, policy, batch = _fixture([_root(), _root(npi="invalid"), _root()], page_rows=1, reordered=reordered)
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
@pytest.mark.parametrize("reordered", [False, True])
async def test_native_handoff_drains_before_cancelled_decoder_cleanup(
    monkeypatch, encoder, plan_factory, compiled, reordered
):
    import asyncio
    from threading import Event

    context, part, policy, batch = _fixture([_root()] * 3, reordered=reordered)
    started, release = Event(), Event()
    events = []

    def decoded(*_args, **_kwargs):
        try:
            yield batch
        finally:
            events.append("decoder closed")

    def blocked_call(prepare, *args, **kwargs):
        descriptors = args[0] if compiled else args[1]
        assert descriptors[0][3] == 3
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
    operation, commit = _hosted_call(monkeypatch, context, part, policy)
    task = asyncio.create_task(operation)
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert events == ["native complete", "decoder closed"]
    commit.assert_not_awaited()


@pytest.mark.parametrize("output_limited", [False, True])
def test_native_windows_split_before_input_or_escaped_output_copies(monkeypatch, encoder, output_limited):
    layout = (
        True,
        (("value", "string", False, False),),
        (("first", 0), ("second", 0), ("third", 0)),
        (0,),
        None,
    )
    batch = pa.record_batch([pa.array([chr(1) * 4096] * (30 if output_limited else 64))], names=["value"])
    with monkeypatch.context() as guarded:
        copy = Mock(side_effect=AssertionError("unadmitted native buffers copied"))
        guarded.setattr(columns, "bytes", copy, raising=False)
        assert columns.column_descriptors(layout, batch) is None
        copy.assert_not_called()
    expected = encoder(layout, columns.column_descriptors(layout, batch.slice(0, 1)), 1_048_576)[0]
    outcomes, sizes = [], []
    for offset, window, descriptors in columns._admitted_column_slices(layout, batch, None):
        sizes.append(window.num_rows)
        assert offset == len(outcomes) and 1 <= window.num_rows <= 64 and descriptors is not None
        assert columns._column_output_bound(layout, descriptors, window.num_rows) <= columns.MAX_OUTPUT_BYTES
        outcomes.extend(encoder(layout, descriptors, 1_048_576))
    assert len(sizes) > 1 and max(sizes) > 1
    assert outcomes == [expected] * batch.num_rows


@pytest.mark.parametrize("reordered", [False, True])
async def test_ordered_occurrence_error_cannot_commit_a_partial_batch(monkeypatch, encoder, column_mode, reordered):
    # Row 65 is beyond an already buffered native window but is not admitted.
    source_rows = [_child()] * 64 + [_child(key="x" * 2000)] * 6
    context, part, policy, _ = _fixture(source_rows, stream=1, page_rows=7, reordered=reordered)
    context = replace(context, request=replace(context.request, page_byte_limit=1000))
    _tracked(monkeypatch, encoder)
    operation, commit = _hosted_call(monkeypatch, context, part, policy)
    with pytest.raises(staging.CandidateRunnerError, match="occurrence exceeds"):
        await operation
    commit.assert_not_awaited()


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
