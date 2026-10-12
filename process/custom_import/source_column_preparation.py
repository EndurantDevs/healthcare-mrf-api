# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded native SOURCE preparation from typed Arrow column windows."""

from __future__ import annotations

import asyncio
import struct
import sys
from contextlib import aclosing, closing
from functools import lru_cache, partial

import pyarrow as pa

from process.custom_import.capture import CaptureError, _decoded_record
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.source_preparation import (
    MAX_FIELDS,
    MAX_INPUT_BYTES,
    MAX_OUTPUT_BYTES,
    MAX_ROWS,
    _has_allocation_failure,
    native_layout,
)


def compile_column_layout(context):
    """Compile the immutable configuration once, outside the row loop."""
    definition, stream = context.request.definition, context.stream
    layout = native_layout(definition, stream)
    if layout is None:
        return None
    fields = tuple(field for field in definition.fields if field.collection == stream.child_collection)
    names = tuple(field.field_id for field in fields)
    entity = definition.entity_field if stream.record_kind == "root" else None
    return (
        layout[0],
        tuple(
            (field.field_id, field.value_type, field.nullable, field.projection_slot is not None) for field in fields
        ),
        layout[2],
        layout[3],
        None if entity is None else names.index(entity),
    )


def _native_column_capability(name):
    import importlib

    try:
        module = importlib.import_module("ptg2_address_canon")
    except ImportError as error:
        if _has_allocation_failure(error):
            raise
        return None
    capability = getattr(module, name, None)
    return capability if callable(capability) else None


@lru_cache(maxsize=1)
def native_column_encoder():
    """Select the bounded native column capability before preparation."""
    return _native_column_capability("custom_import_source_columns_v1")


@lru_cache(maxsize=1)
def native_column_plan_factory():
    """Older extensions retain the original stateless column ABI."""
    return _native_column_capability("custom_import_source_columns_plan_v1")


def bind_column_encoder(layout):
    """Compile one immutable layout per admitted part; never cache row buffers."""
    encoder = native_column_encoder()
    if encoder is None:
        return None
    factory = native_column_plan_factory()
    return partial(encoder, layout) if factory is None else factory(layout).prepare


def _window(buffer, first, last):
    if first < 0 or last < first or (buffer is None and last) or (buffer is not None and last > buffer.size):
        raise CandidateRunnerError("source column buffer bounds differ")
    return memoryview(b"") if buffer is None else memoryview(buffer).cast("B")[first:last]


def _column_window(name, array):
    kind, width, precision, scale = None, 0, 0, 0
    physical = array.type
    if pa.types.is_null(physical):
        return name, "null", 0, len(array), 0, 0, None, None, memoryview(b"")
    if pa.types.is_signed_integer(physical):
        kind, width = f"int{physical.bit_width}", physical.bit_width // 8
    elif pa.types.is_boolean(physical):
        kind = "bool"
    elif pa.types.is_string(physical) or pa.types.is_large_string(physical):
        kind, width = ("utf8", 4) if pa.types.is_string(physical) else ("large_utf8", 8)
    elif pa.types.is_decimal128(physical):
        kind, width, precision, scale = "decimal128", 16, physical.precision, physical.scale
    else:
        return None
    offset, count = array.offset, len(array)
    buffers = array.buffers()
    bit_offset = offset % 8
    first_byte, last_byte = offset // 8, (offset + count + 7) // 8
    validity = None if buffers[0] is None else _window(buffers[0], first_byte, last_byte)
    offsets = None
    if kind in {"utf8", "large_utf8"}:
        offsets = _window(buffers[1], offset * width, (offset + count + 1) * width)
        code = "<i" if width == 4 else "<q"
        first = struct.unpack_from(code, offsets, 0)[0]
        last = struct.unpack_from(code, offsets, count * width)[0]
        data_window = _window(buffers[2], first, last)
    elif kind == "bool":
        data_window = _window(buffers[1], first_byte, last_byte)
    else:
        data_window = _window(buffers[1], offset * width, (offset + count) * width)
    return name, kind, bit_offset, count, precision, scale, validity, offsets, data_window


def column_descriptors(layout, batch):
    """Copy logical windows only after the complete native input fits."""
    if layout is None or sys.byteorder != "little" or type(batch) is not pa.RecordBatch:
        return None
    fields = layout[1]
    names = tuple(field[0] for field in fields)
    if not 1 <= batch.num_rows <= MAX_ROWS or not 1 <= len(fields) <= MAX_FIELDS:
        return None
    if tuple(batch.schema.names) != names:
        return None
    windows = tuple(_column_window(name, batch.column(index)) for index, name in enumerate(names))
    if any(column is None for column in windows):
        return None
    size = sum(128 + len(name.encode()) + len(kind.encode()) for name, kind, _nullable, _projected in fields)
    size += sum(64 + len(name.encode()) for name, _index in layout[2]) + 8 * len(layout[3])
    size += batch.num_rows * (64 + 64 * len(fields))
    for column in windows:
        size += 128 + len(column[0].encode()) + len(column[1].encode())
        size += sum(len(value) for value in column[6:] if value is not None)
    if size > MAX_INPUT_BYTES:
        return None
    return tuple(
        (*column[:6], *(None if value is None else bytes(value) for value in column[6:])) for column in windows
    )


def _decoded_arrow_row(batch, index, first_ordinal, limits):
    try:
        values_by_label = {name: batch.column(column)[index].as_py() for column, name in enumerate(batch.schema.names)}
    except MemoryError:
        raise
    except (pa.ArrowException, EOFError, OSError, OverflowError, ValueError) as exc:
        raise CaptureError("Parquet source payload is invalid") from exc
    return _decoded_record(first_ordinal + index, values_by_label, limits)


async def iter_arrow_batch_oracle(context, batch, limits, *, first_ordinal=1, compiled_layout=None):
    """Yield current preparation outcomes in order, retaining its native serializer."""
    from process.custom_import.build_source import _prepare_decoded_row, _stream_fields
    from process.custom_import.source_preparation import _ordered_rows, native_encoder

    decoded_records = (_decoded_arrow_row(batch, index, first_ordinal, limits) for index in range(batch.num_rows))
    encoder = native_encoder()
    layout = (
        compiled_layout if compiled_layout is not None else native_layout(context.request.definition, context.stream)
    )
    with closing(decoded_records):
        if encoder is None or layout is None:
            fields = _stream_fields(context.request.definition, context.stream)
            for decoded_record in decoded_records:
                yield _prepare_decoded_row(context.request, context.stream, fields, decoded_record.values)
            return
        async with aclosing(_ordered_rows(encoder, layout, context, decoded_records)) as prepared_rows:
            async for prepared_row in prepared_rows:
                yield prepared_row


async def prepare_arrow_batch_oracle(context, batch, limits, *, first_ordinal=1, compiled_layout=None):
    """Materialize one complete batch for callers that require a tuple."""
    async with aclosing(
        iter_arrow_batch_oracle(context, batch, limits, first_ordinal=first_ordinal, compiled_layout=compiled_layout)
    ) as rows:
        return tuple([row async for row in rows])


async def _native_column_outcomes(encoder, layout, columns, page_byte_limit):
    """Drain one off-loop native call before returning its bounded outcomes."""
    from process.custom_import.build_source import _drain_cleanup

    completed_batches = []

    async def complete():
        """Retain the native result while cancellation waits for completion."""
        completed_batches.append(await asyncio.to_thread(encoder, layout, columns, page_byte_limit))

    primary = await _drain_cleanup(complete(), None)
    if primary is not None:
        raise primary
    return completed_batches[0]


def _raise_column_error(outcome):
    """Raise the existing transport or preparation exception for one outcome."""
    if outcome[0] == "transport-error" and outcome[1] in {"integer_invalid", "integer_range"}:
        from process.custom_import.snowflake_bundle import SnowflakeBundleError

        message = (
            "bundle replay integer result is invalid"
            if outcome[1] == "integer_invalid"
            else "bundle replay integer result exceeds the signed 64-bit range"
        )
        raise SnowflakeBundleError(message)
    if outcome[0] == "preparation-error" and outcome[1] == "raw_key_byte_limit":
        from process.custom_import.family_raw_key import RawFamilyKeyError

        raise RawFamilyKeyError("raw family key exceeds the canonical byte limit")
    if outcome[0] == "preparation-error" and outcome[1] == "occurrence_byte_limit":
        raise CandidateRunnerError("one source occurrence exceeds the admitted page byte limit")
    raise CandidateRunnerError("source column encoder returned an invalid error")


def _rejected_column_row(context, fields, batch, index, first_ordinal, limits, outcome):
    """Hydrate a semantic rejection and compare its native identity evidence."""
    from process.custom_import.build_source import _prepare_decoded_row

    decoded_record = _decoded_arrow_row(batch, index, first_ordinal, limits)
    prepared_row = _prepare_decoded_row(context.request, context.stream, fields, decoded_record.values)
    if prepared_row.rejection is None or prepared_row.rejection.code != outcome[0]:
        raise CandidateRunnerError("source column rejection differs from the oracle")
    raw_key = None if outcome[1] is None and outcome[2] is None else (outcome[1], outcome[2])
    typed_key = None if outcome[3] is None and outcome[4] is None else (outcome[3], outcome[4])
    if (prepared_row.raw_key, prepared_row.typed_key) != (raw_key, typed_key):
        raise CandidateRunnerError("source column rejection identity differs from the oracle")
    return prepared_row


def _accepted_column_row(context, outcome):
    """Check the native document envelope without reconstructing source values."""
    from process.custom_import.build_source import _PreparedRow

    raw_key, typed_key = (outcome[0], outcome[1]), (outcome[2], outcome[3])
    for text, digest in (raw_key, typed_key, (outcome[4], outcome[5])):
        if type(text) is not str or type(digest) is not bytes or len(digest) != 32:
            raise CandidateRunnerError("source column encoder returned an invalid document")
    is_child = context.stream.record_kind == "child"
    if (outcome[6] is not None) != is_child or (outcome[7] is not None) != is_child:
        raise CandidateRunnerError("source column encoder returned an invalid child key")
    if is_child and (type(outcome[6]) is not str or type(outcome[7]) is not bytes or len(outcome[7]) != 32):
        raise CandidateRunnerError("source column encoder returned an invalid child key")
    if type(outcome[8]) is not int or not 0 <= outcome[8] <= context.request.page_byte_limit:
        raise CandidateRunnerError("source column encoder returned an invalid byte count")
    return _PreparedRow(raw_key, typed_key, outcome[4], outcome[5], outcome[6], outcome[7], None, outcome[8])


def column_row_result(encoder, layout, batch, page_byte_limit):
    """Classify one row natively before the scheduler requests its successor."""
    descriptors = column_descriptors(layout, batch)
    if descriptors is None:
        return None
    results = encoder(descriptors, page_byte_limit, include_preparation_bounds=True)
    if results is None:
        return None
    if type(results) not in (list, tuple) or len(results) != 1:
        raise CandidateRunnerError("source column encoder returned an incomplete row")
    result = results[0]
    if type(result) is not tuple or len(result) != 3:
        raise CandidateRunnerError("source column encoder returned invalid preparation bounds")
    outcome, input_bytes, output_bytes = result
    if type(input_bytes) is not int or type(output_bytes) is not int or min(input_bytes, output_bytes) < 0:
        raise CandidateRunnerError("source column encoder returned invalid preparation bounds")
    if bool(input_bytes) != bool(output_bytes) or input_bytes > MAX_INPUT_BYTES or output_bytes > MAX_OUTPUT_BYTES:
        raise CandidateRunnerError("source column encoder returned invalid preparation bounds")
    if type(outcome) is not tuple or len(outcome) not in (3, 6, 10):
        raise CandidateRunnerError("source column encoder returned an invalid row")
    is_deferred = len(outcome) == 10 or outcome[:2] == ("preparation-error", "occurrence_byte_limit")
    if bool(input_bytes) != is_deferred:
        raise CandidateRunnerError("source column encoder returned invalid preparation bounds")
    return outcome, input_bytes, output_bytes


def _validate_column_outcome(outcome, limits, ordinal):
    if type(outcome) is not tuple or len(outcome) not in (3, 6, 10) or type(outcome[-1]) is not int or outcome[-1] < 0:
        raise CandidateRunnerError("source column encoder returned an invalid row")
    if outcome[-1] > limits.maximum_record_bytes:
        raise CaptureError(f"record {ordinal} exceeds the byte limit")


def prepared_column_outcome(context, fields, batch, index, first_ordinal, limits, outcome):
    """Hydrate only a semantic rejection; accepted documents stay native."""
    _validate_column_outcome(outcome, limits, first_ordinal + index)
    if len(outcome) == 3:
        _raise_column_error(outcome)
    if len(outcome) == 6:
        return _rejected_column_row(context, fields, batch, index, first_ordinal, limits, outcome)
    return _accepted_column_row(context, outcome)


async def iter_arrow_batch_columns(context, layout, batch, limits, *, first_ordinal=1):
    """Yield ordered native outcomes after the caller validates capture/schema.

    The caller retains part counters, reader cleanup and genuine EOF checks.
    """
    from process.custom_import.build_source import _stream_fields

    is_admitted = (
        first_ordinal >= 1
        and first_ordinal + batch.num_rows - 1 <= limits.maximum_records
        and batch.num_columns <= limits.maximum_fields_per_record
    )
    columns = column_descriptors(layout, batch) if is_admitted else None
    encoder = native_column_encoder() if columns is not None else None
    native_outcomes = None
    if encoder is not None:
        native_outcomes = await _native_column_outcomes(encoder, layout, columns, context.request.page_byte_limit)
    if native_outcomes is None:
        async with aclosing(
            iter_arrow_batch_oracle(context, batch, limits, first_ordinal=first_ordinal)
        ) as prepared_rows:
            async for prepared_row in prepared_rows:
                yield prepared_row
        return
    if type(native_outcomes) not in (tuple, list) or len(native_outcomes) != batch.num_rows:
        raise CandidateRunnerError("source column encoder returned an incomplete batch")
    fields = _stream_fields(context.request.definition, context.stream)
    for index, outcome in enumerate(native_outcomes):
        yield prepared_column_outcome(context, fields, batch, index, first_ordinal, limits, outcome)


async def prepare_arrow_batch_columns(context, layout, batch, limits, *, first_ordinal=1):
    """Materialize one complete batch for callers that require a tuple."""
    async with aclosing(iter_arrow_batch_columns(context, layout, batch, limits, first_ordinal=first_ordinal)) as rows:
        return tuple([row async for row in rows])
