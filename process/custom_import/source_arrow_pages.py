# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Batch guarded one-row Arrow buffers before SOURCE scalar dictionaries exist."""

import asyncio
from contextlib import aclosing, closing
from itertools import chain

import pyarrow as pa

from process.custom_import import source_column_preparation as columns
from process.custom_import.capture import CaptureError, iter_parquet_source_batches
from process.custom_import.source_preparation import (
    MAX_INPUT_BYTES,
    MAX_OUTPUT_BYTES,
    MAX_ROWS,
    MAX_TEXT_BYTES,
    _has_allocation_failure,
    _layout_bytes,
)


def is_column_layout_bounded(layout):
    """Select the complete old route when its valid rows can exceed the column ABI."""
    fields, roots, children = layout[1:4]
    maximum = (
        sum(256 + 2 * len(name.encode()) + len(kind.encode()) + 10 + 17 + MAX_TEXT_BYTES for name, kind, *_ in fields)
        + sum(64 + len(name.encode()) for name, _index in roots)
        + 8 * len(children)
        + 64
        + 64 * len(fields)
    )
    return maximum <= MAX_INPUT_BYTES


def _next_batch(decoded):
    try:
        return next(decoded), None
    except StopIteration:
        return None, None
    except Exception as error:
        if _has_allocation_failure(error):
            raise
        return None, error


def _is_safe_column_batch(batch):
    # Invalid UTF8 selects the ordered capture oracle before native entry.
    try:
        batch.validate(full=True)
    except (pa.ArrowException, ValueError) as error:
        if _has_allocation_failure(error):
            raise
        return False
    return True


def _column_schema(schema):
    return all(
        pa.types.is_signed_integer(field.type)
        or pa.types.is_boolean(field.type)
        or pa.types.is_string(field.type)
        or pa.types.is_large_string(field.type)
        or (pa.types.is_decimal128(field.type) and abs(field.type.scale) <= 38)
        or pa.types.is_null(field.type)
        for field in schema
    )


def _next_column_preparation(encoder, context, layout, decoded, limits, ordinal):
    batch, failure = _next_batch(decoded)
    if failure is not None or batch is None:
        return batch, None, failure
    result = (
        columns.column_row_result(encoder, layout, batch, context.request.page_byte_limit)
        if _is_safe_column_batch(batch)
        else None
    )
    if result is not None:
        try:
            columns._validate_column_outcome(result[0], limits, ordinal)
        except CaptureError as error:
            return None, None, error
    return batch, result, None


async def _oracle_rows(context, decoded, limits):
    from process.custom_import import build_source as staging
    from process.custom_import.source_preparation import _ordered_rows, native_encoder, native_layout

    records = (columns._decoded_arrow_row(batch, 0, ordinal, limits) for ordinal, batch in enumerate(decoded, start=1))
    encoder = native_encoder()
    layout = native_layout(context.request.definition, context.stream)
    with closing(records):
        if encoder is None or layout is None:
            fields = staging._stream_fields(context.request.definition, context.stream)
            for row in records:
                yield staging._prepare_decoded_row(context.request, context.stream, fields, row.values)
            return
        async with aclosing(_ordered_rows(encoder, layout, context, records)) as prepared:
            async for row in prepared:
                yield row


def _column_packets(encoder, context, layout, decoded, limits):
    """Reuse old scheduling charges while accepted documents are prepared in Rust."""
    old_layout = layout[0], tuple((name, kind) for name, kind, *_ in layout[1]), layout[2], layout[3]
    fixed_bytes = _layout_bytes(old_layout)
    pending, ordinal = [], 1
    input_bytes, output_bytes = fixed_bytes, 0
    while True:
        batch, result, failure = _next_column_preparation(encoder, context, layout, decoded, limits, ordinal)
        is_eligible = result is not None and result[1] > 0
        if pending and (
            not is_eligible
            or len(pending) == MAX_ROWS
            or input_bytes + result[1] > MAX_INPUT_BYTES
            or output_bytes + result[2] > MAX_OUTPUT_BYTES
        ):
            yield tuple(pending), None, None
            pending.clear()
            input_bytes, output_bytes = fixed_bytes, 0
        if failure is not None:
            raise failure
        if batch is None:
            return
        if not is_eligible:
            yield None, batch, (ordinal, None if result is None else result[0])
        else:
            pending.append((ordinal, result[0]))
            input_bytes += result[1]
            output_bytes += result[2]
        ordinal += 1


async def _next_column_packet(packets):
    """Drain one bounded decoder/preparation handoff, including cancellation."""
    from process.custom_import import build_source as staging

    completed_packets = []

    async def complete():
        """Retain the packet while cancellation waits for its worker."""
        completed_packets.append(await asyncio.to_thread(next, packets, None))

    primary = await staging._drain_cleanup(complete(), None)
    if primary is not None:
        raise primary
    return completed_packets[0]


async def _ordered_column_rows(context, layout, decoded, limits):
    first, failure = _next_batch(decoded)
    if failure is not None:
        raise failure
    if first is None:
        return
    batches = chain((first,), decoded)
    if not _column_schema(first.schema) or tuple(first.schema.names) != tuple(field[0] for field in layout[1]):
        async with aclosing(_oracle_rows(context, batches, limits)) as prepared:
            async for prepared_row in prepared:
                yield prepared_row
        return
    from process.custom_import import build_source as staging

    fields = staging._stream_fields(context.request.definition, context.stream)
    encoder = columns.bind_column_encoder(layout)
    with closing(_column_packets(encoder, context, layout, batches, limits)) as packets:
        while (packet := await _next_column_packet(packets)) is not None:
            native_rows, batch, barrier = packet
            if native_rows is not None:
                for ordinal, outcome in native_rows:
                    yield columns.prepared_column_outcome(context, fields, None, 0, ordinal, limits, outcome)
                continue
            if barrier[1] is not None:
                yield columns.prepared_column_outcome(context, fields, batch, 0, barrier[0], limits, barrier[1])
                continue
            async with aclosing(
                columns.iter_arrow_batch_oracle(context, batch, limits, first_ordinal=barrier[0])
            ) as prepared:
                async for prepared_row in prepared:
                    yield prepared_row


async def iter_source_pages(context, layout, part, policy, cursor):
    """Retain existing ordered page admission, genuine EOF and decoder cleanup."""
    from process.custom_import import build_source as staging

    reducer = staging._SourcePageReducer(context, part.ordinal, 0, cursor[2])
    cleanup_failures = []
    decoded = iter_parquet_source_batches(
        part.capture, context.stream, limits=policy.part_limits, _cleanup_failures=cleanup_failures
    )
    prepared_rows = _ordered_column_rows(context, layout, decoded, policy.part_limits)
    primary = None
    try:
        async for prepared in prepared_rows:
            page = reducer.before_append(prepared)
            if page is not None:
                yield page
            reducer.append(prepared)
        if reducer.records:
            yield reducer.page()
        if reducer.count != part.record_count:
            raise staging.CandidateRunnerError("decoded part record count differs from the sealed receipt")
    except BaseException as error:
        primary = error
    primary = await staging._drain_cleanup(prepared_rows.aclose(), primary)
    primary = staging._close_iterator(decoded, primary)
    if cleanup_failures:
        cleanup = cleanup_failures[0]
        if primary is not cleanup and (primary is None or primary.__cause__ is not cleanup):
            primary = staging._source_cleanup_failure(primary, cleanup)
    if primary is not None:
        raise primary
