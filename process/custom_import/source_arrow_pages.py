# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Prepare guarded Arrow windows before SOURCE scalar dictionaries exist."""

import asyncio
from contextlib import aclosing

import pyarrow as pa

from process.custom_import import source_column_preparation as columns
from process.custom_import.capture import iter_parquet_source_batches
from process.custom_import.source_preparation import (
    MAX_INPUT_BYTES,
    MAX_TEXT_BYTES,
    _has_allocation_failure,
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


async def _next_column_batch(decoded):
    """Drain one bounded decoder handoff before cancellation closes its reader."""
    from process.custom_import import build_source as staging

    completed_batches = []

    async def complete():
        """Retain the decoder result while cancellation drains its worker."""
        completed_batches.append(await asyncio.to_thread(_next_batch, decoded))

    primary = await staging._drain_cleanup(complete(), None)
    if primary is not None:
        raise primary
    batch, failure = completed_batches[0]
    if failure is not None:
        raise failure
    return batch


async def _ordered_column_rows(context, layout, decoded, limits):
    from process.custom_import import build_source as staging

    first = await _next_column_batch(decoded)
    if first is None:
        return
    fields = staging._stream_fields(context.request.definition, context.stream)
    is_reordered = tuple(first.schema.names) != tuple(field[0] for field in layout[1])
    use_oracle = (
        not _column_schema(first.schema)
        or tuple(first.schema.names) != tuple(field.field_id for field in fields)
        # shortcut: reordered non-scale-zero integer transports use the oracle, widen after ordered error parity proof.
        or (
            is_reordered
            and any(
                field.value_type == "integer" and pa.types.is_decimal128(column.type) and column.type.scale != 0
                for field, column in zip(fields, first.schema, strict=True)
            )
        )
    )
    column_indices = None if not is_reordered else tuple(first.schema.get_field_index(field[0]) for field in layout[1])
    encoder = None if use_oracle else columns.bind_column_encoder(layout)
    batch, ordinal = first, 1
    del first
    while batch is not None:
        prepared_rows = (
            columns.iter_arrow_batch_oracle(context, batch, limits, first_ordinal=ordinal)
            if use_oracle or not _is_safe_column_batch(batch)
            else columns.iter_arrow_batch_columns(
                context, layout, batch, limits, first_ordinal=ordinal, prepare=encoder, column_indices=column_indices
            )
        )
        async with aclosing(prepared_rows):
            async for prepared_row in prepared_rows:
                yield prepared_row
        ordinal += batch.num_rows
        del batch
        batch = await _next_column_batch(decoded)


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
