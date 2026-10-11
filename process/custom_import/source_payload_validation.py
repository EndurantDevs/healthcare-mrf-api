# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded validation of landing payloads without recovering unused values."""

import importlib
from functools import lru_cache

from process.custom_import import runner_codec
from process.custom_import.runner_types import CandidateRunnerError

MAX_ROWS = 64
MAX_FIELDS = 64
MAX_INPUT_BYTES = 131_072
_LABEL = "landing source payload"


@lru_cache(maxsize=1)
def native_validator():
    """Select the optional capability before validation, never after rejection."""
    try:
        module = importlib.import_module("ptg2_address_canon")
    except ImportError:
        return None
    validator = getattr(module, "custom_import_validate_source_payloads_v1", None)
    return validator if callable(validator) else None


def _native_layout(fields):
    # Date and timestamp parsing retain the complete Python ISO input domain.
    if not 1 <= len(fields) <= MAX_FIELDS or any(
        field.value_type not in {"string", "integer", "decimal", "boolean"} for field in fields
    ):
        return None
    return tuple((field.field_id, field.value_type, field.nullable) for field in fields)


def _validate_batch(validator, layout, payloads):
    outcome = validator(layout, payloads)
    if outcome is None:
        return
    if (
        type(outcome) is not tuple
        or len(outcome) != 2
        or type(outcome[0]) is not int
        or not 0 <= outcome[0] < len(payloads)
        or type(outcome[1]) is not str
    ):
        raise CandidateRunnerError("source payload validator returned an invalid result")
    raise CandidateRunnerError(f"{_LABEL} {outcome[1]}")


def validate_landing_payloads(fields, payloads):
    """Validate ordered (text, checked canonical byte count) pairs completely."""
    layout = _native_layout(fields)
    validator = None if layout is None else native_validator()
    if validator is None:
        for payload, _byte_count in payloads:
            runner_codec.payload_values(fields, payload, label=_LABEL)
        return
    layout_bytes = sum(128 + len(name) + len(kind) for name, kind, _nullable in layout)
    pending, input_bytes = [], layout_bytes
    for payload, byte_count in payloads:
        # The caller already counted every UTF-8 document, including this one.
        row_bytes = 64 + byte_count
        # serde_json reserves these object keys for its extended scalar codecs.
        is_serial = layout_bytes + row_bytes > MAX_INPUT_BYTES or "$serde_json::private::" in payload
        if pending and (is_serial or len(pending) == MAX_ROWS or input_bytes + row_bytes > MAX_INPUT_BYTES):
            _validate_batch(validator, layout, pending)
            pending, input_bytes = [], layout_bytes
        if is_serial:
            runner_codec.payload_values(fields, payload, label=_LABEL)
        else:
            pending.append(payload)
            input_bytes += row_bytes
    if pending:
        _validate_batch(validator, layout, pending)
