# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Decode bounded canonical graph rows; unsupported rows keep the Python codec."""

from __future__ import annotations

import importlib
from decimal import Decimal, InvalidOperation
from functools import lru_cache

from process.custom_import import runner_codec
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.source_preparation import (
    MAX_INPUT_BYTES,
    MAX_ROWS,
    _has_allocation_failure,
    _layout_bytes,
    native_layout,
)

_CELL_TYPES = {"string": str, "integer": int, "decimal": Decimal, "boolean": bool}
_VALUE_LANES = {"string": 0, "integer": 1, "decimal": 2, "boolean": 3}
_MAX_DECIMAL_TEXT = 64


@lru_cache(maxsize=1)
def native_decoder():
    """Select the optional versioned capability without changing the serial domain."""
    try:
        module = importlib.import_module("ptg2_address_canon")
    except ImportError:
        return None
    decoder = getattr(module, "custom_import_graph_children_v1", None)
    return decoder if callable(decoder) else None


def _layout(definition, collection):
    stream = next(stream for stream in definition.source_streams if stream.child_collection == collection)
    source = native_layout(definition, stream)
    if source is None or any(kind in {"date", "timestamp"} for _name, kind in source[1]):
        return None
    fields = tuple(field for field in definition.child_fields if field.collection == collection)
    return source, tuple((field.nullable, field.field_slot, field.projection_slot) for field in fields)


def _decimal_values(layout, decoded_row):
    """Convert bounded native text with the captured class, outside native admission."""
    cells, scalars = list(decoded_row[5]), list(decoded_row[6])
    decimals_by_field = {}
    for index, ((_name, kind), binding, cell) in enumerate(zip(layout[0][1], layout[1], cells, strict=True)):
        if kind != "decimal":
            continue
        if type(cell) is not tuple or len(cell) != 2:
            raise CandidateRunnerError("native graph decoder returned a malformed decimal cell")
        if cell[0] != "value":
            continue
        text = cell[1]
        if type(text) is not str or not 1 <= len(text) <= _MAX_DECIMAL_TEXT:
            raise CandidateRunnerError("native graph decoder returned malformed decimal text")
        try:
            decimal_value = Decimal(text)
        except InvalidOperation as exc:
            raise CandidateRunnerError("native graph decoder returned malformed decimal text") from exc
        if not decimal_value.is_finite():
            raise CandidateRunnerError("native graph decoder returned malformed decimal text")
        cells[index] = ("value", decimal_value)
        decimals_by_field[binding[1]] = (text, decimal_value)
    for index, scalar in enumerate(scalars):
        if type(scalar) is not tuple or len(scalar) != 10:
            raise CandidateRunnerError("native graph projection returned a malformed scalar")
        if scalar[2:4] != ("decimal", "value"):
            continue
        if type(scalar[0]) is not int:
            raise CandidateRunnerError("native graph projection differs from its decimal field")
        expected = decimals_by_field.get(scalar[0])
        if expected is None or type(scalar[6]) is not str or scalar[6] != expected[0]:
            raise CandidateRunnerError("native graph projection differs from its decimal text")
        scalars[index] = (*scalar[:6], expected[1], *scalar[7:])
    return (*decoded_row[:5], tuple(cells), tuple(scalars), decoded_row[7])


def _decode_batch(decoder, layout, batch):
    try:
        decoded = decoder(layout, [payload for _record, payload in batch])
    except (ValueError, TypeError) as exc:
        raise CandidateRunnerError("native graph decoder rejected its bounded input") from exc
    if type(decoded) is not list or len(decoded) != len(batch):
        raise CandidateRunnerError("native graph decoder returned a different row prefix")
    for (record, _payload), row in zip(batch, decoded, strict=True):
        if row is not None and (
            type(row) is not tuple
            or len(row) != 8
            or any(type(row[index]) is not str for index in (0, 2))
            or any(type(row[index]) is not bytes or len(row[index]) != 32 for index in (1, 3, 4))
            or type(row[5]) is not tuple
            or len(row[5]) != len(layout[0][1])
            or type(row[6]) is not tuple
            or len(row[6]) > 20
            or type(row[7]) is not int
            or row[7] < 0
        ):
            raise CandidateRunnerError("native graph decoder returned a malformed row")
        yield record, None if row is None else _decimal_values(layout, row)


def decode_records(definition, collection, record_inputs):
    """Yield (original, proof) in order, deferring a later input error until drain."""
    decoder = native_decoder()
    layout = _layout(definition, collection) if decoder is not None else None
    if layout is None:
        for original_record, _payload in record_inputs:
            yield original_record, None
        return
    # The graph binding adds a fixed envelope to the existing SOURCE shape.
    base_bytes = 256 + _layout_bytes(layout[0])
    batch, input_bytes = [], base_bytes
    record_inputs = iter(record_inputs)
    while True:
        try:
            original_record, canonical_payload = next(record_inputs)
        except StopIteration:
            break
        except Exception as error:
            if _has_allocation_failure(error):
                raise
            yield from _decode_batch(decoder, layout, batch) if batch else ()
            raise
        try:
            size = (
                64 + len(canonical_payload.encode("utf-8"))
                if type(canonical_payload) is str and len(canonical_payload) < MAX_INPUT_BYTES
                else MAX_INPUT_BYTES
            )
        except UnicodeEncodeError:
            size = MAX_INPUT_BYTES
        if batch and (len(batch) == MAX_ROWS or input_bytes + size > MAX_INPUT_BYTES):
            yield from _decode_batch(decoder, layout, batch)
            batch, input_bytes = [], base_bytes
        if base_bytes + size > MAX_INPUT_BYTES:
            yield original_record, None
            continue
        batch.append((original_record, canonical_payload))
        input_bytes += size
    if batch:
        yield from _decode_batch(decoder, layout, batch)


def validated_documents(definition, collection, documents):
    """Keep the v1 family hash's full canonical validation before root writes."""
    fields = runner_codec.fields_by_collection(definition)[collection]
    records = ((document, document[2]) for document in documents)
    for document, proof in decode_records(definition, collection, records):
        if proof is None or (document[0], document[1]) != (proof[3], proof[2]):
            runner_codec._validate_child_document(definition, collection, fields, document)
        yield document


def projection_values(fields, proof, child_id):
    """Bind native scalar lanes to the definition and preserve exact byte charging."""
    values_by_field, scalars, byte_count = {}, [], 0
    native_scalars = iter(proof[6])
    for field, cell in zip(fields, proof[5], strict=True):
        if type(cell) is not tuple or len(cell) != 2 or cell[0] not in {"missing", "null", "value"}:
            raise CandidateRunnerError("native graph decoder returned a malformed cell")
        state, scalar_value = cell
        expected_type = _CELL_TYPES[field.value_type]
        if (state == "value" and type(scalar_value) is not expected_type) or (
            state != "value" and (scalar_value is not None or not field.nullable)
        ):
            raise CandidateRunnerError("native graph decoder returned a malformed cell")
        if state == "missing":
            continue
        values_by_field[field.field_id] = scalar_value
        if field.projection_slot is None:
            continue
        scalar = next(native_scalars, None)
        if (
            type(scalar) is not tuple
            or len(scalar) != 10
            or scalar[:4] != (field.field_slot, field.projection_slot, field.value_type, state)
        ):
            raise CandidateRunnerError("native graph projection differs from its field binding")
        lane = _VALUE_LANES[field.value_type]
        expected_lanes = tuple(scalar_value if index == lane else None for index in range(6))
        if any(
            type(actual) is not type(expected) or actual != expected
            for actual, expected in zip(scalar[4:], expected_lanes, strict=True)
        ):
            raise CandidateRunnerError("native graph projection differs from its decoded value")
        byte_count += len(field.value_type) + len(state)
        if scalar[4] is not None:
            byte_count += len(scalar[4].encode("utf-8"))
        scalars.append((child_id, scalar[0], *scalar[2:]))
    if next(native_scalars, None) is not None or byte_count != proof[7]:
        raise CandidateRunnerError("native graph projection accounting differs")
    return values_by_field, scalars, byte_count
