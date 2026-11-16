# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded native address preparation with explicit SQL fallback markers."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from typing import Any

from process.ext import address_fast

MAX_ROWS = 5_000
# Payload bytes; MAX_ROWS separately bounds fixed-width container overhead.
MAX_INPUT_BYTES = 8 * 1024 * 1024
MAX_FIELD_BYTES = 1024 * 1024
RESULT_FIELDS = frozenset(
    {
        "address_key",
        "identity_key",
        "premise_key",
        "premise_identity_key",
        "line1_norm",
        "unit_norm",
        "city_norm",
        "state_code",
        "zip5",
        "zip4",
        "country_code",
        "formatted_address",
        "entity_street_key",
        "entity_line1_norm",
        "entity_line2_norm",
        "entity_city_norm",
        "entity_state_code",
        "entity_zip5",
        "entity_country_norm",
        "entity_keyable",
    }
)
logger = logging.getLogger(__name__)


def is_native_available() -> bool:
    """Let callers retain SQL without creating unused address scratch."""
    return callable(getattr(address_fast._fast_module(), "prepare_address_batch", None))


def _bounded_rows(rows: Iterable[Sequence[Any]]) -> list[address_fast.AddressRow]:
    prepared_rows = []
    input_bytes = 0
    for row in rows:
        if len(prepared_rows) == MAX_ROWS:
            raise ValueError("Address preparation batch exceeds row limit")
        values = address_fast._row_tuple(row)
        lengths = [len(value.encode("utf-8")) for value in values if value is not None]
        if any(length > MAX_FIELD_BYTES for length in lengths):
            raise ValueError("Address preparation field exceeds byte limit")
        input_bytes += sum(lengths)
        if input_bytes > MAX_INPUT_BYTES:
            raise ValueError("Address preparation batch exceeds byte limit")
        prepared_rows.append(values)
    return prepared_rows


def _is_valid_result(result: Any, expected_rows: int) -> bool:
    return (
        type(result) is list
        and len(result) == expected_rows
        and all(
            row is None
            or type(row) is dict
            and row.keys() == RESULT_FIELDS
            and all(value is None or type(value) is str for value in row.values())
            for row in result
        )
    )


def prepare_address_batch(rows: Iterable[Sequence[Any]]) -> list[dict[str, str | None] | None] | None:
    """Prepare each distinct tuple once; None retains the caller's SQL calculation."""
    prepared_rows = _bounded_rows(rows)
    if not prepared_rows:
        return []
    native = getattr(address_fast._fast_module(), "prepare_address_batch", None)
    if not callable(native):
        return None
    unique_rows = list(dict.fromkeys(prepared_rows))
    try:
        result = native(unique_rows)
    except Exception:
        logger.warning("Native address preparation failed; retaining SQL fallback")
        return None
    if not _is_valid_result(result, len(unique_rows)):
        logger.warning("Native address preparation output differs; retaining SQL fallback")
        return None
    by_input = dict(zip(unique_rows, result, strict=True))
    return [None if by_input[row] is None else by_input[row].copy() for row in prepared_rows]
