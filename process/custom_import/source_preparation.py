# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded native SOURCE encoding; the ordered parent retains every authority."""

from __future__ import annotations

import asyncio
import importlib
from contextlib import aclosing
from dataclasses import dataclass
from decimal import Decimal
from functools import lru_cache

from process.custom_import import runner_codec
from process.custom_import.definition import MAX_DEFINITION_DEPTH, MAX_DEFINITION_NODES
from process.custom_import.family_raw_key import raw_family_key_evidence
from process.custom_import.runner_types import CandidateRunnerError

MAX_ROWS = 64
MAX_FIELDS = 64
MAX_INPUT_BYTES = 131_072
MAX_OUTPUT_BYTES = 4_194_304
MAX_TEXT_BYTES = 4_096


@lru_cache(maxsize=1)
def native_encoder():
    """Choose a compatible capability before work, never retry a rejected batch."""
    try:
        module = importlib.import_module("ptg2_address_canon")
    except ImportError:
        return None
    encoder = getattr(module, "custom_import_source_documents_v1", None)
    return encoder if callable(encoder) else None


def native_layout(definition, stream):
    """Select only the bounded shape; wider valid definitions remain serial."""
    fields = tuple(field for field in definition.fields if field.collection == stream.child_collection)
    if not 1 <= len(fields) <= MAX_FIELDS or MAX_DEFINITION_DEPTH < 4 or 3 + 6 * len(fields) > MAX_DEFINITION_NODES:
        return None
    index_by_field = {field.field_id: index for index, field in enumerate(fields)}
    is_child = stream.record_kind == "child"
    if is_child:
        collection = definition.collections_by_name[stream.child_collection]
        fields_by_id = definition.fields_by_id
        if any(
            fields_by_id[part.root_field].value_type != fields_by_id[part.child_field].value_type
            for part in collection.parent_key
        ):
            return None
        root_keys = tuple((part.root_field, index_by_field[part.child_field]) for part in collection.parent_key)
        child_keys = tuple(index_by_field[field_id] for field_id in collection.child_key)
    else:
        root_keys = tuple((field_id, index_by_field[field_id]) for field_id in definition.root_logical_key)
        child_keys = ()
    if not 1 <= len(root_keys) <= 3:
        return None
    return is_child, tuple((field.field_id, field.value_type) for field in fields), root_keys, child_keys


def _has_native_scalar_values(values):
    """Eligibility is narrower than validation; preserve the full serial domain."""
    for value in values.values():
        if type(value) not in (type(None), bool, int, str, Decimal):
            return False
        if type(value) is int and not -(1 << 63) <= value < 1 << 63:
            return False
        if type(value) is str:
            if len(value) > MAX_TEXT_BYTES:
                return False
            try:
                if len(value.encode("utf-8")) > MAX_TEXT_BYTES:
                    return False
            except UnicodeEncodeError:
                return False
        if type(value) is Decimal:
            if not value.is_finite():
                return False
            parts = value.as_tuple()
            if len(parts.digits) > 38 or abs(parts.exponent) > 38:
                return False
    return True


def _value_bytes(cell):
    state, value = cell
    if state != "value":
        return 0
    if type(value) is str:
        return len(value.encode("utf-8"))
    return 5 if type(value) is bool else 20


def _layout_bytes(layout):
    _child, fields, root_key, child_key = layout
    return (
        sum(128 + len(name) + len(kind) for name, kind in fields)
        + sum(64 + len(name) for name, _index in root_key)
        + 8 * len(child_key)
    )


def _output_bound(layout, cells):
    child, fields, root_key, child_key = layout

    def field_bytes(name, index):
        """Bound one generated field, including JSON escaping and grammar."""
        return 192 + 6 * (len(name) + _value_bytes(cells[index]))

    root_bytes = 192 + sum(field_bytes(name, index) for name, index in root_key)
    payload_bytes = 192 + sum(field_bytes(name, index) for index, (name, _kind) in enumerate(fields))
    child_bytes = 192 + sum(field_bytes(fields[index][0], index) for index in child_key) if child else 0
    return root_bytes + payload_bytes + child_bytes


@dataclass(frozen=True, slots=True)
class _NativeRow:
    raw_key: tuple[str, bytes]
    cells: tuple
    input_bytes: int
    output_bytes: int


def _native_row(context, fields, layout, values_by_field):
    from process.custom_import import build_source as staging

    if tuple(values_by_field) != tuple(field.field_id for field in fields):
        raise CandidateRunnerError("replayed fields differ from the retained stream")
    if not _has_native_scalar_values(values_by_field):
        return None
    normalized_by_field = staging._normalized_integer_replay_values(dict(values_by_field), fields)
    fields, root_key, code = staging._source_row_shape(context.request.definition, context.stream, normalized_by_field)
    if code is not None:
        return None
    raw_key = raw_family_key_evidence(root_key, maximum_canonical_bytes=context.request.page_byte_limit)
    cells = [None] * len(fields)
    # The serial path normalizes the root key before the full payload.
    for index in (*(index for _name, index in layout[2]), *range(len(fields))):
        if cells[index] is None:
            field = fields[index]
            cells[index] = (
                runner_codec._value_parts(field, normalized_by_field[field.field_id])
                if field.field_id in normalized_by_field
                else ("missing", None)
            )
    input_bytes = 64 + sum(64 + _value_bytes(cell) for cell in cells)
    output_bytes = _output_bound(layout, cells)
    if _layout_bytes(layout) + input_bytes > MAX_INPUT_BYTES or output_bytes > MAX_OUTPUT_BYTES:
        return None
    return _NativeRow(raw_key, tuple(cells), input_bytes, output_bytes)


async def _encode(encoder, layout, prepared_rows):
    """Drain the sole bounded native call before cancellation can close its inputs."""
    from process.custom_import.build_source import _drain_cleanup

    completed_batches = []

    async def complete():
        """Retain one native result only after the off-loop call settles."""
        completed_batches.append(await asyncio.to_thread(encoder, layout, [row.cells for row in prepared_rows]))

    primary = await _drain_cleanup(complete(), None)
    if primary is not None:
        raise primary
    encoded_batch = completed_batches[0]
    if type(encoded_batch) is not list or len(encoded_batch) != len(prepared_rows):
        raise CandidateRunnerError("source serializer returned an incomplete batch")
    return encoded_batch


async def _finish_native_batch(encoder, layout, context, prepared_rows):
    from process.custom_import import build_source as staging

    encoded_rows = await _encode(encoder, layout, prepared_rows)
    output_bytes = 0
    for original, documents in zip(prepared_rows, encoded_rows, strict=True):
        if type(documents) is not tuple or len(documents) != 6:
            raise CandidateRunnerError("source serializer returned a malformed row")
        root_key, root_hash, canonical_payload, payload_hash, child_key, child_hash = documents
        for index, (canonical, digest) in enumerate(
            ((root_key, root_hash), (canonical_payload, payload_hash), (child_key, child_hash))
        ):
            if index == 2 and canonical is None and digest is None and not layout[0]:
                continue
            if type(canonical) is not str or type(digest) is not bytes or len(digest) != 32:
                raise CandidateRunnerError("source serializer returned a malformed document")
            output_bytes += len(canonical.encode("utf-8"))
        if (child_key is not None) != layout[0] or output_bytes > MAX_OUTPUT_BYTES:
            raise CandidateRunnerError("source serializer returned an invalid encoding bound")
        yield staging._finish_prepared_row(
            context.request,
            context.stream,
            original.raw_key,
            (root_key, root_hash),
            (canonical_payload, payload_hash, child_key, child_hash),
            None,
        )


def _next_preparation(context, fields, layout, decoded):
    try:
        decoded_row = next(decoded)
    except StopIteration:
        return None, None, None
    except Exception as error:
        return None, None, error
    try:
        return decoded_row, _native_row(context, fields, layout, decoded_row.values), None
    except Exception as error:
        return None, None, error


async def _ordered_rows(encoder, layout, context, decoded):
    from process.custom_import import build_source as staging

    fields = staging._stream_fields(context.request.definition, context.stream)
    pending_rows = []
    input_bytes, output_bytes = _layout_bytes(layout), 0
    while True:
        decoded_row, prepared, failure = _next_preparation(context, fields, layout, decoded)
        if pending_rows and (
            prepared is None
            or len(pending_rows) == MAX_ROWS
            or input_bytes + prepared.input_bytes > MAX_INPUT_BYTES
            or output_bytes + prepared.output_bytes > MAX_OUTPUT_BYTES
        ):
            async with aclosing(_finish_native_batch(encoder, layout, context, pending_rows)) as encoded:
                async for prepared_row in encoded:
                    yield prepared_row
            pending_rows.clear()
            input_bytes, output_bytes = _layout_bytes(layout), 0
        if failure is not None:
            raise failure
        if decoded_row is None:
            return
        if prepared is None:
            yield staging._prepare_decoded_row(context.request, context.stream, fields, decoded_row.values)
        else:
            pending_rows.append(prepared)
            input_bytes += prepared.input_bytes
            output_bytes += prepared.output_bytes


async def iter_source_pages(encoder, layout, context, part, policy, cursor):
    """Share ordered page admission; native completion never establishes part EOF."""
    from process.custom_import import build_source as staging

    reducer = staging._SourcePageReducer(context, part.ordinal, 0, cursor[2])
    cleanup_failures = []
    decoded = staging.iter_records(
        part.capture, context.stream, limits=policy.part_limits, _cleanup_failures=cleanup_failures
    )
    ordered_rows = _ordered_rows(encoder, layout, context, decoded)
    primary = None
    try:
        async for prepared in ordered_rows:
            page = reducer.before_append(prepared)
            if page is not None:
                yield page
            reducer.append(prepared)
        if reducer.records:
            yield reducer.page()
        if reducer.count != part.record_count:
            raise CandidateRunnerError("decoded part record count differs from the sealed receipt")
    except BaseException as error:
        primary = error
    primary = await staging._drain_cleanup(ordered_rows.aclose(), primary)
    primary = staging._close_iterator(decoded, primary)
    if cleanup_failures:
        cleanup = cleanup_failures[0]
        if primary is not cleanup and (primary is None or primary.__cause__ is not cleanup):
            primary = staging._source_cleanup_failure(primary, cleanup)
    if primary is not None:
        raise primary
