# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded native preparation inside an already owned SOURCE CPU phase."""

from dataclasses import dataclass
from decimal import Decimal

import pyarrow as pa

from process.custom_import import build_source as source
from process.custom_import import source_column_preparation as columns
from process.custom_import.capture import CaptureLimits
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost
from process.custom_import.source_preparation import MAX_ROWS, MAX_TEXT_BYTES, _has_allocation_failure


def _column_signature(field_values, count):
    if not field_values:
        return "null", None, 0
    kind = type(field_values[0])
    if any(type(scalar) is not kind for scalar in field_values):
        return None
    if kind is Decimal:
        if any(not scalar.is_finite() or scalar.is_zero() and scalar.is_signed() for scalar in field_values):
            return None
        exponent = field_values[0].as_tuple().exponent
        if not -38 <= exponent <= 38 or any(
            scalar.as_tuple().exponent != exponent or len(scalar.as_tuple().digits) > 38 for scalar in field_values
        ):
            return None
        return "decimal", -exponent, 16 * count
    if kind is int:
        return ("integer", None, 8 * count) if all(-(2**63) <= scalar < 2**63 for scalar in field_values) else None
    if kind is bool:
        return "boolean", None, (count + 7) // 8
    if kind is not str:
        return None
    size = 0
    for scalar in field_values:
        if len(scalar) > MAX_TEXT_BYTES:
            return None
        try:
            width = len(scalar.encode("utf-8"))
        except UnicodeEncodeError:
            return None
        if width > MAX_TEXT_BYTES:
            return None
        size += width
    return "string", None, size


def _signature(decoded_inputs, names, check):
    """Admit exact scalar representations, without coercing the serial domain."""
    if any(tuple(decoded_input.values) != names for decoded_input in decoded_inputs):
        return None
    column_shapes = []
    for name in names:
        check()
        field_values = [
            decoded_input.values[name] for decoded_input in decoded_inputs if decoded_input.values[name] is not None
        ]
        shape = _column_signature(field_values, len(decoded_inputs))
        if shape is None:
            return None
        column_shapes.append(shape)
    return tuple(column_shapes)


def _output_bound(layout, signature, count):
    """Apply the existing column document bound before Arrow allocation."""
    widths = tuple(
        size + count if kind == "string" else 0 if kind == "null" else 80 * count for kind, _, size in signature
    )

    def field_bytes(name, index):
        """Charge the field envelope and maximally escaped UTF-8 bytes."""
        return count * (192 + 6 * len(name.encode())) + 6 * widths[index]

    fields, roots, children = layout[1:4]
    bound = count * (512 + 192 * 3)
    bound += sum(field_bytes(field[0], index) for index, field in enumerate(fields))
    bound += sum(field_bytes(name, index) for name, index in roots)
    bound += sum(192 * count + 6 * widths[index] for _name, index in roots)
    if layout[0]:
        bound += 192 * count + sum(field_bytes(fields[index][0], index) for index in children)
    return bound


def _arrow_batch(records, names, signature):
    types_by_kind = {"null": pa.null(), "string": pa.string(), "integer": pa.int64(), "boolean": pa.bool_()}
    arrays = []
    for name, (kind, scale, _size) in zip(names, signature, strict=True):
        physical = pa.decimal128(38, scale) if kind == "decimal" else types_by_kind[kind]
        arrays.append(pa.array([record.values[name] for record in records], type=physical))
    return pa.record_batch(arrays, names=names)


@dataclass(slots=True)
class _Input:
    original: object
    values: dict
    charge: int


class DecodedSourcePreparation:
    """Compile one stream plan; retain no input across part iterator lifetimes.

    Entries are (original occurrence, original encoded frame byte count). Resource
    eligibility only chooses an optimization; valid serial inputs remain valid.
    """

    def __init__(self, context, *, memory_bytes, check=None):
        self.context = context
        self.fields = source._stream_fields(context.request.definition, context.stream)
        self.names = tuple(field.field_id for field in self.fields)
        self.memory_bytes = memory_bytes
        self.check = check or (lambda: None)
        self.fixed_bytes = 8192 + sum(256 + 8 * len(name.encode()) for name in self.names)
        self.layout = None if self.fixed_bytes > memory_bytes else columns.compile_column_layout(context)
        self.column_indices = (
            None
            if self.layout is None or self.names == tuple(field[0] for field in self.layout[1])
            else tuple(self.names.index(field[0]) for field in self.layout[1])
        )
        self.native = None if self.layout is None else columns.bind_column_encoder(self.layout)

    def _serial(self, records):
        for record in records:
            self.check()
            yield (
                record.original,
                source._prepare_decoded_row(self.context.request, self.context.stream, self.fields, record.values),
            )

    def _native_signature(self, decoded_inputs):
        """Keep retained-order integer transport failures on the serial path."""
        signature = None if self.native is None else _signature(decoded_inputs, self.names, self.check)
        if (
            signature is not None
            and self.column_indices is not None
            and any(
                field.value_type == "integer" and kind == "decimal" and scale != 0
                for field, (kind, scale, _size) in zip(self.fields, signature, strict=True)
            )
        ):
            return None
        return signature

    def _prepared(self, decoded_inputs, held_bytes=None):
        """Prepare bounded rows while preserving original-order rejection values."""
        self.check()
        if held_bytes is None:
            held_bytes = sum(decoded_input.charge for decoded_input in decoded_inputs)
        signature = self._native_signature(decoded_inputs)
        if signature is None:
            yield from self._serial(decoded_inputs)
            return
        layout_signature = (
            signature if self.column_indices is None else tuple(signature[index] for index in self.column_indices)
        )
        # Charge original objects, Arrow buffers/descriptors/native copies, and
        # expanded Python/native documents together. No new singleton refusal.
        inputs = sum(size + 4 * (len(decoded_inputs) + 1) + 256 for _, _, size in signature)
        charge = (
            held_bytes
            + 8 * inputs
            + 8 * _output_bound(self.layout, layout_signature, len(decoded_inputs))
            + self.fixed_bytes
        )
        if charge > self.memory_bytes:
            if len(decoded_inputs) == 1:
                yield from self._serial(decoded_inputs)
            else:
                middle = len(decoded_inputs) // 2
                yield from self._prepared(decoded_inputs[:middle], held_bytes)
                yield from self._prepared(decoded_inputs[middle:], held_bytes)
            return
        try:
            batch = _arrow_batch(decoded_inputs, self.names, signature)
        except (pa.ArrowException, OverflowError, ValueError) as error:
            if _has_allocation_failure(error):
                raise
            yield from self._serial(decoded_inputs)
            return
        # Keep the original batch for retained-order rejection hydration.
        selected = batch if self.column_indices is None else batch.select(self.column_indices)
        descriptors = columns.column_descriptors(self.layout, selected)
        if descriptors is None:
            yield from self._serial(decoded_inputs)
            return
        self.check()
        outcomes = self.native(descriptors, self.context.request.page_byte_limit)
        self.check()
        if outcomes is None:
            yield from self._serial(decoded_inputs)
            return
        if type(outcomes) not in (list, tuple) or len(outcomes) != len(decoded_inputs):
            raise source.CandidateRunnerError("source column encoder returned an incomplete batch")
        # Original part-local ordinals, never the global SOURCE ordinal. Native
        # input admission is <128 KiB, below this existing 1 MiB decoder bound.
        first = decoded_inputs[0].original.row_ordinal + 1
        limits = CaptureLimits(maximum_records=max(1, decoded_inputs[-1].original.row_ordinal + 1))
        for index, (decoded_input, outcome) in enumerate(zip(decoded_inputs, outcomes, strict=True)):
            self.check()
            prepared = columns.prepared_column_outcome(self.context, self.fields, batch, index, first, limits, outcome)
            yield decoded_input.original, prepared

    def rows(self, entries):
        """Preserve first failure order while filling only bounded native batches."""
        entries = iter(entries)
        pending, size, failure = [], 0, None
        try:
            while True:
                self.check()
                try:
                    original, frame_bytes = next(entries)
                except StopIteration:
                    break
                except (
                    MemoryError,
                    InterruptedError,
                    CancellationRequested,
                    LeaseAuthorityLost,
                    KeyboardInterrupt,
                    SystemExit,
                ):
                    raise
                except Exception as error:
                    failure = error
                    break
                charge = 1024 + 8 * frame_bytes
                if pending and (len(pending) == MAX_ROWS or size + charge > self.memory_bytes):
                    yield from self._prepared(pending)
                    pending, size = [], 0
                decoded_input = _Input(original, dict(original.values), charge)
                if charge > self.memory_bytes or self.native is None:
                    yield from self._serial((decoded_input,))
                else:
                    pending.append(decoded_input)
                    size += charge
            if pending:
                yield from self._prepared(pending)
            if failure is not None:
                raise failure
        finally:
            close = getattr(entries, "close", None)
            if close is not None:
                close()
