# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Decode bounded canonical graph rows; unsupported rows keep the Python codec."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
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


@dataclass(frozen=True)
class ProjectionRecord:
    """Existing immutable row inputs; no ORM object crosses the native boundary."""

    child: object
    root: object
    family: object
    root_values: dict


@dataclass(frozen=True)
class ProjectionBatch:
    """Ordered spans (or serial fallback), typed columns, and owned-memory bounds."""

    rows: tuple
    scalars: tuple
    contexts: tuple
    input_bytes: int
    workspace_bytes: int


@lru_cache(maxsize=1)
def native_projector():
    """Select full batch preparation separately from the unchanged eight-field ABI."""
    try:
        module = importlib.import_module("ptg2_address_canon")
    except ImportError:
        return None
    projector = getattr(module, "custom_import_graph_projection_columns_v1", None)
    return projector if callable(projector) else None


def _projection_layout(definition, collection_slots):
    from process.custom_import import materialization as material

    scopes = material._profile_scopes(definition, collection_slots)
    if any(type(slot) is not int or not 1 <= slot <= 32_767 for slot in collection_slots.values()):
        return None
    root_fields = tuple(definition.fields_by_id[name] for name in definition.query.root_fields)
    if any(field.value_type not in _CELL_TYPES for field in root_fields):
        return None
    collections, indices_by_slot = [], {}
    for collection, slot in collection_slots.items():
        layout = _layout(definition, collection)
        if layout is None:
            continue
        fields = tuple(field for field in definition.child_fields if field.collection == collection)
        query_names = definition.query.child_fields if collection == definition.query.child_collection else ()
        query_indices = tuple(
            next(index for index, field in enumerate(fields) if field.field_id == name) for name in query_names
        )
        query_ids = (*definition.query.root_fields, *query_names)
        profiles = tuple(
            (
                ordinal,
                profile.profile_id,
                tuple(query_ids.index(term.field_id) for term in profile.selection_terms),
                tuple(query_ids.index(name) for name in profile.context_dimensions),
            )
            for ordinal, (profile, scope) in enumerate(zip(definition.selection_profiles, scopes, strict=True), 1)
            if scope.collection_slot == slot and collection == definition.query.child_collection
        )
        indices_by_slot[slot] = len(collections)
        collections.append((slot, layout, query_indices, profiles))
    if not collections:
        return None
    return (
        (tuple((field.value_type, field.nullable, field.field_slot) for field in root_fields), tuple(collections)),
        root_fields,
        indices_by_slot,
    )


def _projection_decimal(value):
    if type(value) is not Decimal or value.__sizeof__() > 512 or not value.is_finite():
        return None
    _sign, digits, exponent = value.as_tuple()
    if not value.is_zero() and (len(digits) + exponent > 18 or exponent < -12):
        return None
    rendered = "0" if value.is_zero() else format(value, "f")
    return rendered.rstrip("0").rstrip(".") if "." in rendered else rendered


def _projection_root_cells(fields, values):
    if type(values) is not dict:
        return None
    cells = []
    for field in fields:
        if field.field_id not in values:
            cells.append(("missing", None))
            continue
        value = values[field.field_id]
        if value is None:
            cells.append(("null", None))
            continue
        if type(value) is not _CELL_TYPES[field.value_type]:
            return None
        if field.value_type == "decimal":
            value = _projection_decimal(value)
            if value is None:
                return None
        if type(value) is int and not -(1 << 63) <= value < 1 << 63:
            return None
        if type(value) is str and (len(value) > 4_096 or len(value.encode("utf-8")) > 4_096):
            return None
        cells.append(("value", value))
    return tuple(cells)


def _projection_root(record, fields):
    cells = _projection_root_cells(fields, record.root_values)
    if cells is None:
        return None
    root, family = record.root, record.family
    if (
        type(root.canonical_logical_key) is not str
        or any(
            type(value) is not bytes or len(value) != 32 for value in (root.logical_key_sha256, family.family_sha256)
        )
        or any(
            type(value) is not int or not -(1 << 63) <= value < 1 << 63
            for value in (family.entity_binding_id, family.family_revision_id)
        )
    ):
        return None
    return (
        root.canonical_logical_key,
        root.logical_key_sha256,
        family.entity_binding_id,
        family.family_revision_id,
        family.family_sha256,
        cells,
    )


def _projection_row(record, root_index, indices):
    child = record.child
    if child.collection_slot not in indices:
        return None
    if (
        type(child.collection_slot) is not int
        or type(child.child_revision_id) is not int
        or not -(1 << 63) <= child.child_revision_id < 1 << 63
    ):
        return None
    if any(
        type(value) is not bytes or len(value) != 32
        for value in (child.parent_key_sha256, child.child_key_sha256, child.payload_sha256)
    ) or any(
        type(value) is not str
        for value in (child.canonical_parent_key, child.canonical_child_key, child.canonical_payload)
    ):
        return None
    return (
        indices[child.collection_slot],
        root_index,
        child.child_revision_id,
        child.canonical_parent_key,
        child.parent_key_sha256,
        child.canonical_child_key,
        child.child_key_sha256,
        child.payload_sha256,
        child.canonical_payload,
    )


def _projection_primitive_bytes(values):
    total = 0
    for value in values:
        if type(value) is bytes and len(value) == 32:
            total += 32
        elif type(value) is int and -(1 << 63) <= value < 1 << 63:
            continue
        elif type(value) is str and len(value) <= MAX_INPUT_BYTES:
            total += len(value.encode("utf-8"))
        else:
            return None
    return total


def _projection_cell_bytes(cells):
    return sum(
        64
        + (
            len(value.encode("utf-8"))
            if type(value) is str
            else 20
            if type(value) is int
            else 5
            if type(value) is bool
            else 0
        )
        for _state, value in cells
    )


def _projection_inputs(records, fields, indices):
    roots, rows, root_indices = [], [], {}
    input_bytes = 0
    for record in records:
        root_identity = (id(record.root), id(record.family), id(record.root_values))
        if root_identity not in root_indices:
            root_indices[root_identity] = len(roots)
            root = _projection_root(record, fields)
            size = None if root is None else _projection_primitive_bytes(root[:5])
            if size is None:
                root = None
            else:
                input_bytes += size + _projection_cell_bytes(root[5])
            roots.append(root)
            input_bytes += 64
        row = _projection_row(record, root_indices[root_identity], indices)
        size = None if row is None else _projection_primitive_bytes(row)
        rows.append(row if size is not None else None)
        input_bytes += 64 + (size or 0)
    return roots, rows, input_bytes


def _projection_bounds(layout, roots, rows, record_bytes):
    roots_layout, collections = layout
    input_bytes = 256 + len(roots_layout) * 128 + record_bytes
    for _slot, graph_layout, _query, profiles in collections:
        source = graph_layout[0]
        input_bytes += 256 + sum(128 + len(name) + len(kind) for name, kind in source[1])
        input_bytes += sum(64 + len(name) for name, _index in source[2])
        input_bytes += sum(256 + len(profile[1]) for profile in profiles)
    workspace = 16_384 * len(rows)
    for row in rows:
        if row is None or roots[row[1]] is None:
            continue
        root_text = sum(len(value.encode("utf-8")) if type(value) is str else 32 for _state, value in roots[row[1]][5])
        payload_bytes = len(row[8].encode("utf-8"))
        # Keep owned UTF-8 and the worst-case four-byte PyUnicode representation
        # inside the same window, including JSON escapes and context metadata.
        workspace += 24 * payload_bytes + 5 * len(collections[row[0]][3]) * (512 + 6 * (root_text + payload_bytes))
    return input_bytes, workspace


def _projection_columns(columns, kinds, maximum, required):
    if type(columns) is not tuple or len(columns) != len(kinds):
        raise CandidateRunnerError("native graph projection column shape differs")
    count = len(columns[0]) if type(columns[0]) is tuple else maximum + 1
    if count > maximum or any(type(column) is not tuple or len(column) != count for column in columns):
        raise CandidateRunnerError("native graph projection column prefix differs")
    for index, (column, kind) in enumerate(zip(columns, kinds, strict=True)):
        if any(
            (value is None and index < required)
            or (value is not None and (type(value) is not kind or (kind is bytes and len(value) != 32)))
            for value in column
        ):
            raise CandidateRunnerError("native graph projection column type differs")
    return columns


def _projection_decimal_column(column):
    decimals = []
    for value in column:
        if value is None:
            decimals.append(None)
            continue
        if not 1 <= len(value) <= _MAX_DECIMAL_TEXT:
            raise CandidateRunnerError("native graph projection decimal text differs")
        try:
            decimal = Decimal(value)
        except InvalidOperation as error:
            raise CandidateRunnerError("native graph projection decimal text differs") from error
        if not decimal.is_finite():
            raise CandidateRunnerError("native graph projection decimal text differs")
        decimals.append(decimal)
    return tuple(decimals)


def _projection_result(encoded, count, bounds):
    if type(encoded) is not tuple or len(encoded) != 3 or type(encoded[0]) is not tuple or len(encoded[0]) != count:
        raise CandidateRunnerError("native graph projection returned a different row prefix")
    spans, scalar_columns, context_columns = encoded
    scalars = _projection_columns(
        scalar_columns, (int, int, str, str, str, int, str, bool, type(None), type(None)), MAX_ROWS * 20, 4
    )
    contexts = _projection_columns(context_columns, (int, int, str, bytes), MAX_ROWS * 4, 4)
    scalar_offset = context_offset = 0
    for span in spans:
        if span is None:
            continue
        if type(span) is not tuple or len(span) != 7 or any(type(entry) is not int or entry < 0 for entry in span):
            raise CandidateRunnerError("native graph projection span differs")
        start, scalar_count, context_start, context_count, *costs = span
        if (
            (start, context_start) != (scalar_offset, context_offset)
            or scalar_count > 20
            or context_count > 4
            or any(cost > 4_194_304 for cost in costs)
        ):
            raise CandidateRunnerError("native graph projection span bound differs")
        scalar_offset += scalar_count
        context_offset += context_count
    if (scalar_offset, context_offset) != (len(scalars[0]), len(contexts[0])):
        raise CandidateRunnerError("native graph projection spans differ from columns")
    return ProjectionBatch(
        spans, (*scalars[:6], _projection_decimal_column(scalars[6]), *scalars[7:]), contexts, *bounds
    )


def project_batch(definition, collection_slots, projection_records, *, retained_bytes=0):
    """Prepare at most 64 immutable children; None keeps the complete serial path.

    Row spans are scalar start/count, context start/count, scalar-model bytes,
    context-model bytes, and the ten-scalar/three-context SQL argument bytes.
    The caller consumes spans in order and retains all logical page/fence checks.
    Previously retained column slices share the aggregate native workspace cap.
    """
    projector = native_projector()
    if (
        projector is None
        or type(projection_records) is not tuple
        or not 1 <= len(projection_records) <= MAX_ROWS
        or type(retained_bytes) is not int
        or not 0 <= retained_bytes <= 4_194_304
    ):
        return None
    try:
        prepared = _projection_layout(definition, collection_slots)
        if prepared is None:
            return None
        layout, root_fields, indices = prepared
        roots, child_inputs, record_bytes = _projection_inputs(projection_records, root_fields, indices)
        bounds = _projection_bounds(layout, roots, child_inputs, record_bytes)
    except (ValueError, TypeError, KeyError, AttributeError, StopIteration) as error:
        if _has_allocation_failure(error):
            raise
        return None
    if bounds[0] > MAX_INPUT_BYTES or retained_bytes + bounds[1] > 4_194_304:
        return None
    try:
        encoded = projector(layout, roots, child_inputs)
    except (ValueError, TypeError) as error:
        raise CandidateRunnerError("native graph projector rejected its bounded input") from error
    return _projection_result(encoded, len(projection_records), bounds)
