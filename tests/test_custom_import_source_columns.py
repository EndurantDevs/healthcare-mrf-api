# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compiled pre-dictionary SOURCE documents, validation and capture-byte parity."""

from __future__ import annotations

import importlib
import inspect
import json
import struct
import subprocess
import sys
from dataclasses import replace
from decimal import Decimal
from importlib.machinery import ExtensionFileLoader

import pyarrow as pa
import pytest

from process.custom_import import source_column_preparation as adapter
from process.custom_import.capture import CaptureError, CaptureLimits, _record_size
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import _is_valid_npi
from process.custom_import.family_raw_key import RawFamilyKeyError
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.snowflake_bundle import SnowflakeBundleError
from tests.test_custom_import_build_source import _child, _root
from tests.test_custom_import_build_source_bulk import _bulk_context


@pytest.fixture
def encoder():
    function = adapter.native_column_encoder()
    if function is None:
        pytest.skip("bounded source column capability is unavailable")
    assert inspect.isbuiltin(function)
    return function


@pytest.fixture
def plan_factory(encoder):
    module = importlib.import_module("ptg2_address_canon")
    factory = getattr(module, "custom_import_source_columns_plan_v1", None)
    assert inspect.isbuiltin(factory), "compiled source column layout capability is required"
    assert factory.__module__ == encoder.__module__
    native_module = importlib.import_module(encoder.__module__)
    assert isinstance(native_module.__loader__, ExtensionFileLoader)
    assert factory is native_module.custom_import_source_columns_plan_v1
    assert module._SourceColumnsPlan is native_module._SourceColumnsPlan
    assert module._SourceColumnsPlan.__module__ == module.__name__
    return factory


def _context(*, stream=0, key_decimal=False, integer=False, nullable=False, projected=False):
    context = _bulk_context(stream)
    document = json.loads(context.request.definition.canonical)
    root = document["schema"]["root"]
    if key_decimal:
        root["logical_key"] = ["score"]
        document["schema"]["children"][0]["parent_key"] = [{"child": "amount", "root": "score"}]
    root["fields"][1]["nullable"] = nullable
    if integer:
        root["fields"][1]["type"] = "integer"
    if projected:
        root["fields"][1]["projection_slot"] = 1
    definition = CustomImportDefinition.from_mapping(document)
    return replace(
        context, request=replace(context.request, definition=definition), stream=definition.source_streams[stream]
    )


def _batch(context, rows, *, types=None):
    fields = tuple(
        field for field in context.request.definition.fields if field.collection == context.stream.child_collection
    )
    types_by_kind = {
        "string": pa.string(),
        "integer": pa.int64(),
        "decimal": pa.decimal128(38, 12),
        "boolean": pa.bool_(),
    }
    return pa.record_batch(
        [
            pa.array(
                [row[field.field_id] for row in rows],
                type=(types or {}).get(field.field_id, types_by_kind[field.value_type]),
            )
            for field in fields
        ],
        names=[field.field_id for field in fields],
    )


async def _parity(context, batch, encoder, *, limits=None):
    limits = limits or CaptureLimits()
    layout = adapter.compile_column_layout(context)
    oracle_rows = await adapter.prepare_arrow_batch_oracle(context, batch, limits)
    native_outcomes = encoder(layout, adapter.column_descriptors(layout, batch), context.request.page_byte_limit)
    assert native_outcomes is not None and len(native_outcomes) == batch.num_rows
    actual_rows = await adapter.prepare_arrow_batch_columns(context, layout, batch, limits)
    assert tuple(replace(prepared_row, rejection=None) for prepared_row in actual_rows) == tuple(
        replace(prepared_row, rejection=None) for prepared_row in oracle_rows
    )
    for left, right in zip(actual_rows, oracle_rows, strict=True):
        assert (left.rejection is None) == (right.rejection is None)
        if left.rejection is not None:
            columns = left.rejection.__table__.columns
            assert tuple(getattr(left.rejection, column.name) for column in columns) == tuple(
                getattr(right.rejection, column.name) for column in columns
            )
    for index, outcome in enumerate(native_outcomes):
        values_by_label = {name: batch.column(column)[index].as_py() for column, name in enumerate(batch.schema.names)}
        assert outcome[-1] == _record_size(values_by_label)
    return oracle_rows, native_outcomes


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [0, 1])
@pytest.mark.parametrize("physical", [pa.string(), pa.large_string()])
async def test_root_child_slices_preserve_documents_hashes_and_byte_counts(encoder, stream, physical):
    context = _bulk_context(stream)
    rows = (
        [_root(), _root(score=Decimal("-0.000")), _root(npi=None), _root(score=None), _root(npi="invalid")]
        if stream == 0
        else [_child(), _child(key='Ω\n\0"\\'), _child(npi=None), _child(key=None), _child(amount=None)]
    )
    batch = _batch(context, rows, types={"npi" if stream == 0 else "detail_id": physical}).slice(1, 4)
    await _parity(context, batch, encoder)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "text", ["001.2300", "-0.000", "1000", "0.000000000001", "1.0000000000001", "1e2", "+1", "1_0", "١"]
)
async def test_lexical_decimal_preserves_raw_string_and_typed_identity(encoder, text):
    context = _context(key_decimal=True)
    batch = _batch(context, [_root(score=text)], types={"score": pa.string()})
    expected, outcomes = await _parity(context, batch, encoder)
    assert json.loads(outcomes[0][0] if len(outcomes[0]) == 10 else outcomes[0][1])["values"][0] == {
        "type": "string",
        "value": text,
    }
    assert expected[0].raw_key is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value,scale", [("1.20E+4", -2), ("0E-7", 7), ("1E-7", 7), ("0.000001", 6), ("-123.4500", 4), ("0.00", 2)]
)
async def test_decimal128_transport_spelling_and_numeric_raw_identity(encoder, value, scale):
    context = _context(key_decimal=True)
    batch = _batch(context, [_root(score=Decimal(value))], types={"score": pa.decimal128(38, scale)})
    await _parity(context, batch, encoder)


@pytest.mark.asyncio
@pytest.mark.parametrize("physical", [pa.int8(), pa.int16(), pa.int32(), pa.int64()])
async def test_signed_integer_buffers_match_integer_and_decimal_fields(encoder, physical):
    for integer in (False, True):
        context = _context(integer=integer, key_decimal=not integer)
        batch = _batch(context, [_root(score=-7), _root(score=0), _root(score=12)], types={"score": physical}).slice(
            1, 2
        )
        await _parity(context, batch, encoder)


@pytest.mark.asyncio
async def test_nullable_null_physical_column_is_accepted_without_python_rows(encoder, monkeypatch):
    context = _context(nullable=True)
    batch = _batch(context, [_root(score=None)], types={"score": pa.null()})
    expected, _ = await _parity(context, batch, encoder)
    monkeypatch.setattr(adapter, "_decoded_arrow_row", lambda *_args: pytest.fail("accepted row decoded again"))
    assert (
        await adapter.prepare_arrow_batch_columns(
            context, adapter.compile_column_layout(context), batch, CaptureLimits()
        )
        == expected
    )


@pytest.mark.asyncio
async def test_boolean_bitmap_crosses_byte_boundaries(encoder):
    context = _bulk_context()
    rows = [_root() | {"enabled": (None if index == 4 else index % 2 == 0)} for index in range(12)]
    await _parity(context, _batch(context, rows).slice(3, 8), encoder)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value,code",
    [("1.5", "integer_invalid"), ("9223372036854775808", "integer_range"), ("-9223372036854775809", "integer_range")],
)
async def test_integer_decimal_transport_error_is_ordered_data(encoder, value, code):
    context = _context(integer=True)
    batch = _batch(
        context, [_root(npi=None, score=Decimal(value))], types={"score": pa.decimal128(38, 1 if "." in value else 0)}
    )
    layout = adapter.compile_column_layout(context)
    result = encoder(layout, adapter.column_descriptors(layout, batch), context.request.page_byte_limit)
    assert result[0][:2] == ("transport-error", code)
    with pytest.raises(SnowflakeBundleError) as oracle:
        await adapter.prepare_arrow_batch_oracle(context, batch, CaptureLimits())
    with pytest.raises(type(oracle.value), match=str(oracle.value)):
        await adapter.prepare_arrow_batch_columns(context, layout, batch, CaptureLimits())
    with pytest.raises(CaptureError, match="record 1 exceeds the byte limit"):
        await adapter.prepare_arrow_batch_columns(context, layout, batch, CaptureLimits(maximum_record_bytes=1))


@pytest.mark.asyncio
async def test_current_python_npi_contract_accepts_luhn_valid_prefix_three(encoder):
    value = next("300000000" + str(digit) for digit in range(10) if _is_valid_npi("300000000" + str(digit)))
    assert value[0] == "3"  # The existing registry classifier restricts this prefix.
    expected, outcomes = await _parity(_bulk_context(), _batch(_bulk_context(), [_root(npi=value)]), encoder)
    assert expected[0].rejection is None and len(outcomes[0]) == 10


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "limit,error,message",
    [(20, RawFamilyKeyError, "raw family key exceeds"), (250, CandidateRunnerError, "one source occurrence exceeds")],
)
async def test_page_bound_outcome_preserves_capture_precedence(encoder, limit, error, message):
    context = _bulk_context()
    context = replace(context, request=replace(context.request, page_byte_limit=limit))
    batch = _batch(context, [_root()])
    layout = adapter.compile_column_layout(context)
    outcome = encoder(layout, adapter.column_descriptors(layout, batch), limit)[0]
    assert outcome[0] == "preparation-error"
    with pytest.raises(error, match=message):
        await adapter.prepare_arrow_batch_columns(context, layout, batch, CaptureLimits())
    with pytest.raises(CaptureError, match="record 1 exceeds the byte limit"):
        await adapter.prepare_arrow_batch_columns(context, layout, batch, CaptureLimits(maximum_record_bytes=1))


def test_native_descriptor_constraints_are_independent_of_python_adapter(encoder):
    context, batch = _bulk_context(), _batch(_bulk_context(), [_root()])
    layout = adapter.compile_column_layout(context)
    columns = adapter.column_descriptors(layout, batch)
    for position, value, message in [
        (0, "other", "identity"),
        (2, 8, "offset"),
        (3, 2, "row count"),
        (8, bytearray(b"mutable"), "immutable"),
    ]:
        descriptor_parts = list(columns[0])
        descriptor_parts[position] = value
        with pytest.raises(ValueError, match=message):
            encoder(layout, (tuple(descriptor_parts), *columns[1:]), context.request.page_byte_limit)
    descriptor_parts = list(columns[1])
    descriptor_parts[8] = (10**38).to_bytes(16, "little", signed=True)
    with pytest.raises(ValueError, match="precision"):
        encoder(layout, (columns[0], tuple(descriptor_parts), columns[2]), context.request.page_byte_limit)


def test_native_shape_preselection_retains_existing_caps(encoder):
    context, batch = _bulk_context(), _batch(_bulk_context(), [_root()])
    layout = adapter.compile_column_layout(context)
    columns = adapter.column_descriptors(layout, batch)
    descriptor_parts = list(columns[0])
    descriptor_parts[1] = "timestamp"
    assert encoder(layout, (tuple(descriptor_parts), *columns[1:]), context.request.page_byte_limit) is None
    descriptor_parts = list(columns[0])
    descriptor_parts[8] = b"x" * 131073
    assert encoder(layout, (tuple(descriptor_parts), *columns[1:]), context.request.page_byte_limit) is None
    descriptor_parts = list(columns[0])
    descriptor_parts[3] = 65
    assert encoder(layout, (tuple(descriptor_parts), *columns[1:]), context.request.page_byte_limit) is None


@pytest.mark.asyncio
async def test_null_string_slot_ignores_unused_invalid_utf8(encoder):
    context = _context(nullable=True)
    score = pa.Array.from_buffers(
        pa.string(), 1, [pa.py_buffer(b"\x00"), pa.py_buffer(struct.pack("<ii", 0, 1)), pa.py_buffer(b"\xff")]
    )
    batch = pa.record_batch([pa.array(["1003000126"]), score, pa.array([True])], names=["npi", "score", "enabled"])
    expected, outcomes = await _parity(context, batch, encoder)
    assert expected[0].rejection is None and len(outcomes[0]) == 10


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value", ["999999999999999999", "1000000000000000000", "0.000000000001", "0.0000000000001", "-0.0000000000000"]
)
async def test_projected_decimal_storage_bounds_keep_typed_rejection_evidence(encoder, value):
    context = _context(projected=True, key_decimal=True)
    batch = _batch(context, [_root(score=value)], types={"score": pa.string()})
    expected, _ = await _parity(context, batch, encoder)
    assert expected[0].typed_key is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["x" * 2048, "x" * 2049, "Ω" * 1024, "Ω" * 1025, "10\x003000126"])
async def test_projected_string_storage_error_precedes_entity_rule(encoder, value):
    context = _bulk_context()
    document = json.loads(context.request.definition.canonical)
    document["schema"]["root"]["fields"][0]["projection_slot"] = 1
    definition = CustomImportDefinition.from_mapping(document)
    context = replace(
        context, request=replace(context.request, definition=definition), stream=definition.source_streams[0]
    )
    await _parity(context, _batch(context, [_root(npi=value)]), encoder)


@pytest.mark.parametrize("compiled", [False, True])
def test_python_result_allocation_releases_source_slot_before_gc_reentry(encoder, plan_factory, compiled):
    # The subprocess bounds a potential non-reentrant-slot deadlock. Exact
    # immutable input is created before arming GC's nested SOURCE call.
    script = r"""
import gc, struct, sys
import ptg2_address_canon as native
layout = (False, (("npi", "string", False, False),), (("npi", 0),), (), 0)
columns = (("npi", "utf8", 0, 1, 0, 0, None, struct.pack("<ii", 0, 10), b"1003000126"),)
old_layout = (False, (("npi", "string"),), (("npi", 0),), ())
old_rows = [(("value", "1003000126"),)]
prepare = (
    native.custom_import_source_columns_plan_v1(layout).prepare
    if sys.argv[1] == "compiled"
    else lambda columns, limit: native.custom_import_source_columns_v1(layout, columns, limit)
)
armed = False
reentries = 0
def callback(phase, info):
    global reentries
    if armed and phase == "start":
        native.custom_import_source_documents_v1(old_layout, old_rows)
        reentries += 1
gc.collect()
gc.callbacks.append(callback)
gc.set_threshold(1, 1, 1)
armed = True
result = prepare(columns, 16384)
armed = False
gc.callbacks.remove(callback)
assert len(result[0]) == 10
assert reentries > 0
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, "compiled" if compiled else "legacy"],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert completed.returncode == 0, completed.stderr


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [0, 1])
async def test_composite_key_order_and_mixed_numeric_boolean_identity(encoder, stream):
    context = _bulk_context(stream)
    document = json.loads(context.request.definition.canonical)
    document["schema"]["root"]["logical_key"] = ["enabled", "score", "npi"]
    child = document["schema"]["children"][0]
    child["fields"][1]["type"] = "boolean"
    child["parent_key"] = [
        {"child": "detail_id", "root": "enabled"},
        {"child": "amount", "root": "score"},
        {"child": "detail_npi", "root": "npi"},
    ]
    child["child_key"] = ["amount", "detail_id"]
    definition = CustomImportDefinition.from_mapping(document)
    context = replace(
        context, request=replace(context.request, definition=definition), stream=definition.source_streams[stream]
    )
    rows = (
        [_root(score=1000), _root(score=Decimal("-0")) | {"enabled": False}]
        if stream == 0
        else [_child(key=True, amount=1000), _child(key=False, amount=Decimal("-0"))]
    )
    await _parity(context, _batch(context, rows), encoder)


@pytest.mark.parametrize("stream", [0, 1])
@pytest.mark.parametrize("limit", [20, 250, 1_048_576])
@pytest.mark.parametrize("bounds", [False, True])
def test_compiled_layout_matches_exact_outcomes_and_bounds(encoder, plan_factory, stream, limit, bounds):
    context = _context(stream=stream, integer=stream == 0)
    rows = (
        [_root(score=7), _root(npi="invalid", score=7), _root(score=None), _root(score=2**63)]
        if stream == 0
        else [_child(), _child(key='Ω\n\0"\\'), _child(key=None), _child(amount=None)]
    )
    batch = _batch(context, rows, types={"score": pa.decimal128(38, 0)} if stream == 0 else None)
    layout = adapter.compile_column_layout(context)
    plan = plan_factory(layout)
    assert inspect.isbuiltin(plan.prepare)
    assert type(plan) is importlib.import_module("ptg2_address_canon")._SourceColumnsPlan
    assert inspect.signature(plan.prepare).parameters["include_preparation_bounds"].default is False
    with pytest.raises(AttributeError):
        plan.layout = layout
    with pytest.raises(TypeError):
        type(plan)()
    for window in (batch, *(batch.slice(index, 1) for index in range(batch.num_rows)), batch):
        descriptors = adapter.column_descriptors(layout, window)
        expected = encoder(layout, descriptors, limit, include_preparation_bounds=bounds)
        assert expected is not None
        assert plan.prepare(descriptors, limit, include_preparation_bounds=bounds) == expected


def test_compiled_layout_retains_per_operation_input_charge(encoder, plan_factory):
    layout = (False, (("value", "string", False, False),), (("value", 0),), (), None)
    plan = plan_factory(layout)
    rows = 64
    fixed = 128 + len("value") + len("string") + 64 + len("value")
    overhead = fixed + rows * 128 + 128 + len("value") + len("utf8") + (rows + 1) * 4
    for extra in (0, 1, 0):
        size = adapter.MAX_INPUT_BYTES - overhead + extra
        offsets = struct.pack("<65i", *(size * index // rows for index in range(rows + 1)))
        descriptors = (("value", "utf8", 0, rows, 0, 0, None, offsets, b"x" * size),)
        expected = encoder(layout, descriptors, 1_048_576, include_preparation_bounds=True)
        assert (expected is None) == bool(extra)
        assert plan.prepare(descriptors, 1_048_576, include_preparation_bounds=True) == expected


def test_compiled_layout_retains_output_and_text_caps(encoder, plan_factory):
    layout = (False, (("value", "string", False, False),), (("first", 0), ("second", 0), ("third", 0)), (), None)
    plan = plan_factory(layout)
    for rows, width, declined in ((64, 1000, False), (64, 1600, True), (1, 4097, True), (1, 4096, False)):
        batch = pa.record_batch([pa.array(["x" * width] * rows)], names=["value"])
        descriptors = adapter.column_descriptors(layout, batch)
        assert descriptors is not None
        expected = encoder(layout, descriptors, 1_048_576, include_preparation_bounds=True)
        assert (expected is None) == declined
        assert plan.prepare(descriptors, 1_048_576, include_preparation_bounds=True) == expected


def test_compiled_layout_preserves_descriptor_and_page_limit_failures(encoder, plan_factory):
    context = _bulk_context()
    layout = adapter.compile_column_layout(context)
    descriptors = adapter.column_descriptors(layout, _batch(context, [_root()]))
    plan = plan_factory(layout)
    cases = [(descriptors, limit) for limit in (0, -1, True, "1024", 2**63)]
    for position, value in ((0, "other"), (2, 8), (3, 2), (8, bytearray(b"mutable"))):
        descriptor_parts = list(descriptors[0])
        descriptor_parts[position] = value
        cases.append(((tuple(descriptor_parts), *descriptors[1:]), 1_048_576))
    for columns, limit in cases:
        with pytest.raises((ValueError, TypeError, OverflowError)) as expected:
            encoder(layout, columns, limit, include_preparation_bounds=True)
        with pytest.raises(type(expected.value)) as actual:
            plan.prepare(columns, limit, include_preparation_bounds=True)
        assert str(actual.value) == str(expected.value)
    for position, value in ((1, "timestamp"), (3, 65), (8, b"x" * 131073)):
        descriptor_parts = list(descriptors[0])
        descriptor_parts[position] = value
        columns = (tuple(descriptor_parts), *descriptors[1:])
        assert encoder(layout, columns, 1_048_576) is None
        assert plan.prepare(columns, 1_048_576) is None


def test_compiled_layout_rejects_the_same_unbounded_or_malformed_metadata(encoder, plan_factory):
    layout = adapter.compile_column_layout(_bulk_context())
    cases = [layout[:-1], (0, *layout[1:]), (False, (), *layout[2:]), (*layout[:4], 99)]
    cases.append((False, layout[1] * 22, *layout[2:]))
    for changed in cases:
        with pytest.raises((ValueError, TypeError, OverflowError)) as expected:
            encoder(changed, (), 0)
        with pytest.raises(type(expected.value)) as actual:
            plan_factory(changed)
        assert str(actual.value) == str(expected.value)
