# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native decoded SOURCE parity, eligibility, failure order and bounded lifetimes."""

import inspect
import json
import weakref
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from process.custom_import import build_source as source
from process.custom_import import family_shard_facts as facts
from process.custom_import import source_column_preparation as columns
from process.custom_import import source_decoded_preparation as preparation
from process.custom_import.bulk_page_codec import encode_landing_batch
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_shard_input_records import encode_record, prepare_record
from process.custom_import.snowflake_bundle import SnowflakeBundleError
from tests.test_custom_import_build_source import _child, _root
from tests.test_custom_import_build_source_bulk import _bulk_context


@pytest.fixture
def native_calls(monkeypatch):
    encoder = columns.native_column_encoder()
    if encoder is None:
        pytest.skip("compiled source columns unavailable")
    assert inspect.isbuiltin(encoder)
    original = columns.bind_column_encoder
    calls = []

    def bind(layout):
        actual_rows = original(layout)

        def invoke(descriptors, limit):
            calls.append(descriptors[0][3])
            return actual_rows(descriptors, limit)

        return invoke

    monkeypatch.setattr(columns, "bind_column_encoder", bind)
    return calls


def _entries(context, rows, *, source_start=0, row_start=0):
    for index, values in enumerate(rows):
        record = prepare_record(
            context.request.definition,
            context.stream,
            values,
            stream_slot=context.stream_slot,
            collection_slot=context.collection_slot,
            part_ordinal=1,
            row_ordinal=row_start + index,
            source_ordinal=source_start + index,
        )
        yield record, len(encode_record(record))


def _cells(prepared):
    rejected = prepared.rejection
    return (
        replace(prepared, rejection=None),
        None if rejected is None else tuple(getattr(rejected, column.name) for column in rejected.__table__.columns),
    )


def _reordered_context(stream=0):
    context = _bulk_context(stream)
    document = json.loads(context.request.definition.canonical)
    for schema in (document["schema"]["root"], *document["schema"]["children"]):
        for field in schema["fields"]:
            field["slot"] = 3 * field["slot"] + 1
        schema["fields"].reverse()
    definition = CustomImportDefinition.from_mapping(document)
    return replace(
        context, request=replace(context.request, definition=definition), stream=definition.source_streams[stream]
    )


def _assert_rows(context, entries, actual_rows):
    fields = source._stream_fields(context.request.definition, context.stream)
    assert len(entries) == len(actual_rows)
    for (original, _), (observed, prepared) in zip(entries, actual_rows, strict=True):
        assert observed is original
        expected = source._prepare_decoded_row(context.request, context.stream, fields, dict(original.values))
        assert _cells(prepared) == _cells(expected)


@pytest.mark.parametrize("child", [False, True])
def test_real_compiled_rows_keep_exact_documents_rejections_and_large_global_ordinals(native_calls, child):
    context = _bulk_context(int(child))
    rows = []
    for index in range(65):
        values = _child(key=f'Ω-{index}\n\0"\\') if child else _root(score=Decimal("1.200"))
        if index % 13 == 0:
            values["detail_npi" if child else "npi"] = None
        rows.append(values)
    entries = list(_entries(context, rows, source_start=79_000_000, row_start=37))
    prepared = preparation.DecodedSourcePreparation(context, memory_bytes=8 * 1024**2)
    actual_rows = list(prepared.rows(entries))
    _assert_rows(context, entries, actual_rows)
    assert native_calls == [64, 1]
    assert [original.source_ordinal for original, _ in actual_rows] == list(range(79_000_000, 79_000_065))


@pytest.mark.parametrize("stream", [0, 1])
def test_reordered_sparse_fields_keep_native_documents_hashes_and_original_bytes(monkeypatch, native_calls, stream):
    context = _reordered_context(stream)
    context = replace(context, request=replace(context.request, page_byte_limit=1_048_576))
    source_rows = [
        _child(key=f'child-{index}-Ω\n\0"\\', amount=Decimal(f"{index + 2}.300"))
        if stream
        else _root(score=Decimal(f"{index + 1}.200"))
        for index in range(65)
    ]
    source_rows[0]["detail_npi" if stream else "npi"] = None
    source_rows[1]["detail_id" if stream else "enabled"] = None
    entries = list(_entries(context, source_rows, source_start=79_000_000, row_start=37))
    original_bytes = [encode_record(original) for original, _ in entries]
    fields = source._stream_fields(context.request.definition, context.stream)
    expected_rows = tuple(
        source._prepare_decoded_row(context.request, context.stream, fields, source_values_by_field)
        for source_values_by_field in source_rows
    )
    names = tuple(field.field_id for field in fields)
    hydrated_indices = []
    prepare_outcome = columns.prepared_column_outcome

    def hydrate(context, fields, batch, index, *args):
        assert tuple(batch.schema.names) == names
        hydrated_indices.append(index)
        return prepare_outcome(context, fields, batch, index, *args)

    monkeypatch.setattr(columns, "prepared_column_outcome", hydrate)
    actual_rows = list(preparation.DecodedSourcePreparation(context, memory_bytes=8 * 1024**2).rows(entries))
    _assert_rows(context, entries, actual_rows)
    assert native_calls == [64, 1] and len(hydrated_indices) == len(entries)
    assert [encode_record(original) for original, _ in actual_rows] == original_bytes
    actual_page = source._SourcePage(1, 37, 79_000_000, tuple(prepared for _, prepared in actual_rows))
    expected_page = replace(actual_page, records=expected_rows)
    assert tuple(source._prepared_fingerprint(actual_page, index) for index in range(65)) == tuple(
        source._prepared_fingerprint(expected_page, index) for index in range(65)
    )
    assert encode_landing_batch(context, (actual_page,), batch_id=UUID(int=7), first_pack_ordinal=3) == (
        encode_landing_batch(context, (expected_page,), batch_id=UUID(int=7), first_pack_ordinal=3)
    )


@pytest.mark.parametrize("stream", [0, 1])
def test_reordered_descriptor_boundary_charges_the_corresponding_field_widths(monkeypatch, stream):
    """Inspect the descriptor boundary without producing emulated native outcomes."""
    context = _reordered_context(stream)
    source_values_by_field = _child(npi="x" * 257, key="distinct-child") if stream else _root(npi="x" * 257)
    entries = list(_entries(context, [source_values_by_field]))
    decoded_inputs = [
        preparation._Input(original, dict(original.values), 1024 + 8 * size) for original, size in entries
    ]
    names = tuple(source_values_by_field)
    signature = preparation._signature(decoded_inputs, names, lambda: None)
    layout = columns.compile_column_layout(context)
    indices = tuple(names.index(field[0]) for field in layout[1])
    selected = preparation._arrow_batch(decoded_inputs, names, signature).select(indices)
    descriptors = columns.column_descriptors(layout, selected)
    assert descriptors is not None
    expected_bound = columns._column_output_bound(layout, descriptors, 1)
    assert preparation._output_bound(layout, signature, 1) < expected_bound
    original_bound = preparation._output_bound
    checked_bounds = []

    def bound(actual_layout, actual_signature, count):
        assert actual_signature == tuple(signature[index] for index in indices)
        actual = original_bound(actual_layout, actual_signature, count)
        assert actual == expected_bound
        checked_bounds.append(actual)
        return actual

    class NativeBoundaryReached(Exception):
        pass

    def boundary(actual_descriptors, _limit):
        assert actual_descriptors == descriptors
        raise NativeBoundaryReached

    monkeypatch.setattr(preparation, "_output_bound", bound)
    monkeypatch.setattr(columns, "bind_column_encoder", lambda _: boundary)
    with pytest.raises(NativeBoundaryReached):
        list(preparation.DecodedSourcePreparation(context, memory_bytes=8 * 1024**2).rows(entries))
    assert checked_bounds == [expected_bound]
    assert dict(entries[0][0].values) == source_values_by_field


@pytest.mark.parametrize(
    "values,message",
    [
        ((Decimal(2**63), Decimal("1.5")), "bundle replay integer result exceeds the signed 64-bit range"),
        ((Decimal("1.5"), Decimal(2**63)), "bundle replay integer result is invalid"),
    ],
)
def test_reordered_integer_transport_keeps_original_first_error(monkeypatch, values, message):
    context = _reordered_context()
    document = json.loads(context.request.definition.canonical)
    for field in document["schema"]["root"]["fields"]:
        if field["id"] in {"score", "enabled"}:
            field["type"] = "integer"
    definition = CustomImportDefinition.from_mapping(document)
    context = replace(
        context, request=replace(context.request, definition=definition), stream=definition.source_streams[0]
    )
    valid_by_field = dict(npi="1003000126", score=7, enabled=9)
    original, _ = next(_entries(context, [valid_by_field]))
    row = valid_by_field | dict(zip(("score", "enabled"), values, strict=True))
    original = replace(original, values=tuple(row.items()))

    def unexpected(*_args):
        pytest.fail("order-sensitive integer transport must use original Python validation")

    monkeypatch.setattr(columns, "bind_column_encoder", lambda _: unexpected)
    fields = source._stream_fields(definition, context.stream)
    with pytest.raises(SnowflakeBundleError) as expected:
        source._prepare_decoded_row(context.request, context.stream, fields, row)
    with pytest.raises(type(expected.value)) as actual:
        list(
            preparation.DecodedSourcePreparation(context, memory_bytes=8 * 1024**2).rows(
                [(original, len(encode_record(original)))]
            )
        )
    assert str(actual.value) == str(expected.value) == message


@pytest.mark.parametrize(
    "scores",
    [
        (Decimal("1.20"), Decimal("1.200")),
        (Decimal("-0.000"),),
        (datetime(2026, 1, 1, tzinfo=UTC),),
        (True, 1),
        (1.25,),
    ],
)
def test_unsupported_exact_scalar_representations_use_original_python(native_calls, scores):
    context = _bulk_context()
    entries = list(_entries(context, [_root(score=value) for value in scores]))
    actual_rows = list(preparation.DecodedSourcePreparation(context, memory_bytes=8 * 1024**2).rows(entries))
    _assert_rows(context, entries, actual_rows)
    assert native_calls == []


def test_small_resource_budget_and_absent_native_preserve_valid_serial_input(monkeypatch):
    context = _bulk_context()
    entries = list(_entries(context, [_root(), _root(score=Decimal("-0.000"))]))
    monkeypatch.setattr(
        columns, "bind_column_encoder", lambda _: pytest.fail("tiny work must not compile a native plan")
    )
    actual_rows = list(preparation.DecodedSourcePreparation(context, memory_bytes=1).rows(entries))
    _assert_rows(context, entries, actual_rows)
    monkeypatch.setattr(columns, "bind_column_encoder", lambda _: None)
    actual_rows = list(preparation.DecodedSourcePreparation(context, memory_bytes=8 * 1024**2).rows(entries))
    _assert_rows(context, entries, actual_rows)


@pytest.mark.parametrize("reordered", [False, True])
def test_preparation_failure_precedes_a_later_input_failure_and_closes_reader(native_calls, reordered):
    context = _reordered_context() if reordered else _bulk_context()
    context = replace(context, request=replace(context.request, page_byte_limit=1))
    entries = list(_entries(context, [_root()]))
    close_events = []

    def records():
        try:
            yield entries[0]
            raise ValueError("later input damage")
        finally:
            close_events.append(True)

    fields = source._stream_fields(context.request.definition, context.stream)
    with pytest.raises(Exception) as expected:
        source._prepare_decoded_row(context.request, context.stream, fields, _root())
    with pytest.raises(type(expected.value), match="raw family key") as actual_rows:
        list(preparation.DecodedSourcePreparation(context, memory_bytes=8 * 1024**2).rows(records()))
    assert str(actual_rows.value) == str(expected.value)
    assert native_calls == [1]
    assert close_events == [True]


@pytest.mark.parametrize("reordered", [False, True])
def test_stop_after_native_call_precedes_hydration_or_output(monkeypatch, native_calls, reordered):
    context = _reordered_context() if reordered else _bulk_context()
    entries = list(_entries(context, [_root()]))
    stop_events = []
    bind = columns.bind_column_encoder

    def stop_after(layout):
        native = bind(layout)

        def invoke(*args):
            outcome = native(*args)
            stop_events.append(True)
            return outcome

        return invoke

    def check():
        if stop_events:
            raise InterruptedError("stop_events after native")

    monkeypatch.setattr(columns, "bind_column_encoder", stop_after)
    emitted_rows = []
    with pytest.raises(InterruptedError, match="stop_events after native"):
        emitted_rows.extend(
            preparation.DecodedSourcePreparation(context, memory_bytes=8 * 1024**2, check=check).rows(entries)
        )
    assert emitted_rows == [] and native_calls == [1]


def test_preallocation_document_bound_matches_actual_arrow_bound():
    context = _bulk_context()
    entries = list(_entries(context, [_root(score=Decimal("1.200")), _root(npi=None, score=Decimal("1.200"))]))
    records = [preparation._Input(original, dict(original.values), 1024 + 8 * size) for original, size in entries]
    names = tuple(records[0].values)
    signature = preparation._signature(records, names, lambda: None)
    layout = columns.compile_column_layout(context)
    batch = preparation._arrow_batch(records, names, signature)
    windows = tuple(columns._column_window(name, batch.column(index)) for index, name in enumerate(names))
    assert preparation._output_bound(layout, signature, len(records)) == columns._column_output_bound(
        layout, windows, len(records)
    )


def test_arrow_allocation_failure_is_never_converted_into_serial_fallback(monkeypatch, native_calls):
    context = _bulk_context()
    entries = list(_entries(context, [_root()]))
    failure = MemoryError("synthetic allocation failure")

    def allocate(*_args):
        raise failure

    monkeypatch.setattr(preparation, "_arrow_batch", allocate)
    monkeypatch.setattr(source, "_prepare_decoded_row", lambda *_: pytest.fail("allocation failure must remain fatal"))
    with pytest.raises(MemoryError) as actual_rows:
        list(preparation.DecodedSourcePreparation(context, memory_bytes=8 * 1024**2).rows(entries))
    assert actual_rows.value is failure and native_calls == []


@pytest.mark.parametrize("reordered", [False, True])
def test_split_native_attempt_accounts_all_still_queued_originals(monkeypatch, native_calls, reordered):
    context = _reordered_context() if reordered else _bulk_context()
    entries = list(_entries(context, [_root() for _ in range(16)]))
    held = sum(1024 + 8 * size for _, size in entries)
    layout = columns.compile_column_layout(context)
    single_inputs = [preparation._Input(entries[0][0], dict(entries[0][0].values), 1024 + 8 * entries[0][1])]
    names = tuple(single_inputs[0].values)
    signature = preparation._signature(single_inputs, names, lambda: None)
    batch = preparation._arrow_batch(single_inputs, names, signature)
    batch = batch.select(tuple(names.index(field[0]) for field in layout[1]))
    descriptors = columns.column_descriptors(layout, batch)
    one_inputs = sum(len(column[-1]) + 8 + 256 for column in descriptors)
    fixed = 8192 + sum(256 + 8 * len(field[0].encode()) for field in layout[1])
    memory = held + 8 * one_inputs + 8 * columns._column_output_bound(layout, descriptors, 1) + fixed + 1024
    bind = columns.bind_column_encoder

    def bounded(layout):
        native = bind(layout)

        def invoke(descriptors, limit):
            count = descriptors[0][3]
            inputs = sum(len(column[-1]) + 4 * (count + 1) + 256 for column in descriptors)
            fixed = 8192 + sum(256 + 8 * len(field[0].encode()) for field in layout[1])
            assert held + 8 * inputs + 8 * columns._column_output_bound(layout, descriptors, count) + fixed <= memory
            return native(descriptors, limit)

        return invoke

    monkeypatch.setattr(columns, "bind_column_encoder", bounded)
    actual_rows = list(preparation.DecodedSourcePreparation(context, memory_bytes=memory).rows(entries))
    _assert_rows(context, entries, actual_rows)
    assert native_calls and sum(native_calls) == len(entries)


def test_input_allocation_failure_stops_before_pending_native_work(native_calls):
    context = _bulk_context()
    entry = next(_entries(context, [_root()]))
    failure = MemoryError("synthetic input allocation failure")

    def records():
        yield entry
        raise failure

    with pytest.raises(MemoryError) as actual_rows:
        list(preparation.DecodedSourcePreparation(context, memory_bytes=8 * 1024**2).rows(records()))
    assert actual_rows.value is failure and native_calls == []


def test_final_sort_run_payloads_are_released_before_merge_consumption(tmp_path, monkeypatch):
    class Payload(bytearray):
        pass

    values = [(index, b"synthetic" * 128) for index in range(8)]
    identities = {id(value) for value in values}
    references = []
    pack = facts._pack

    def observed(value):
        payload = Payload(pack(value))
        if id(value) in identities:
            references.append(weakref.ref(payload))
        return payload

    monkeypatch.setattr(facts, "_pack", observed)
    work = facts._Work(tmp_path / "sort", facts.SourceFactLimits(1024**2, 64 * 1024, 64 * 1024), lambda _: None)
    ordered = facts._sorted(work, iter(reversed(values)), key=lambda value: value[0])
    try:
        first = next(ordered)
        assert references and all(reference() is None for reference in references)
        assert [first, *ordered] == values
    finally:
        ordered.close()
        work.close()
