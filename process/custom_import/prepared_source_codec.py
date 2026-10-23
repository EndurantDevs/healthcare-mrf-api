# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lossless, bounded replay parts for already decoded source-label records."""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from decimal import Decimal, DecimalException
from typing import BinaryIO, NamedTuple

from process.custom_import.capture import DecodedRecord, _decoded_record
from process.custom_import.capture_limits import CaptureLimits

PREPARED_SOURCE_CONTRACT = "custom-import/prepared-source-rows/v1"
_MAX_PART_BYTES = 64 * 1024 * 1024
_MAX_PART_ROWS = 1_000_000


class PreparedSourceCodecError(ValueError):
    """A prepared part violates its scalar representation or admitted bounds."""


class PreparedSourcePart(NamedTuple):
    first_source_ordinal: int
    record_count: int
    payload: bytes


def prepared_source_row_byte_limit(limits: CaptureLimits) -> int:
    """Bound the tagged envelope independently of original source byte counts."""

    return 6 * limits.maximum_record_bytes + 64 * limits.maximum_fields_per_record + 128


def _part_limits(limits: CaptureLimits, part_byte_limit: int, part_row_limit: int) -> int:
    row_byte_limit = prepared_source_row_byte_limit(limits)
    if (
        type(part_byte_limit) is not int
        or not row_byte_limit <= part_byte_limit <= _MAX_PART_BYTES
        or type(part_row_limit) is not int
        or not 1 <= part_row_limit <= _MAX_PART_ROWS
    ):
        raise PreparedSourceCodecError("prepared_source_part_limits_invalid")
    return row_byte_limit


def _row_bytes(ordinal: int, entries: list) -> bytes:
    return (
        json.dumps([ordinal, entries], ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _tagged_scalar(value):
    if value is None:
        return "null", None
    if isinstance(value, str):
        return "string", value
    if isinstance(value, bool):
        return "boolean", value
    if isinstance(value, (int, Decimal)):
        return ("integer" if isinstance(value, int) else "decimal"), str(value)
    raise PreparedSourceCodecError("prepared_source_row_invalid")


def _encode_row(record: DecodedRecord, expected_ordinal: int, limits: CaptureLimits) -> bytes:
    if not isinstance(record, DecodedRecord) or not isinstance(record.values, Mapping):
        raise PreparedSourceCodecError("prepared_source_row_invalid")
    if type(record.ordinal) is not int or record.ordinal != expected_ordinal:
        raise PreparedSourceCodecError("prepared_source_ordinal_invalid")
    if record.ordinal > limits.maximum_records or len(record.values) > limits.maximum_fields_per_record:
        raise PreparedSourceCodecError("prepared_source_row_invalid")
    try:
        entries = []
        labels = set()
        for label, value in record.values.items():
            if type(label) is not str or label in labels:
                raise ValueError
            labels.add(label)
            entries.append([label, *_tagged_scalar(value)])
        return _row_bytes(record.ordinal, entries)
    except ValueError, UnicodeError, DecimalException:
        raise PreparedSourceCodecError("prepared_source_row_invalid") from None


def iter_prepared_source_parts(
    decoded_records: Iterable[DecodedRecord],
    *,
    limits: CaptureLimits,
    part_byte_limit: int,
    part_row_limit: int,
) -> Iterator[PreparedSourcePart]:
    """Encode trusted verified-decoder records, retaining a part and next row.

    The source decoder must already enforce original record-byte/label limits;
    those limits bound allocation of the single tagged serialization per row.
    The caller owns decoder cleanup and aggregate storage/part-count admission.
    Exhaust this iterator before recording true source EOF.
    """

    row_byte_limit = _part_limits(limits, part_byte_limit, part_row_limit)
    part_payload = bytearray()
    first_ordinal = expected_ordinal = 1
    record_count = 0
    for decoded_record in decoded_records:
        encoded = _encode_row(decoded_record, expected_ordinal, limits)
        if len(encoded) > row_byte_limit:
            raise PreparedSourceCodecError("prepared_source_row_byte_limit_exceeded")
        if record_count and len(part_payload) + len(encoded) > part_byte_limit:
            yield PreparedSourcePart(first_ordinal, record_count, bytes(part_payload))
            part_payload.clear()
            first_ordinal, record_count = expected_ordinal, 0
        part_payload.extend(encoded)
        record_count += 1
        expected_ordinal += 1
        if record_count == part_row_limit:
            yield PreparedSourcePart(first_ordinal, record_count, bytes(part_payload))
            part_payload.clear()
            first_ordinal, record_count = expected_ordinal, 0
    if record_count or expected_ordinal == 1:
        yield PreparedSourcePart(first_ordinal, record_count, bytes(part_payload))


def _decoded_scalar(tag, value):
    if tag == "null" and value is None:
        return None
    if tag == "string" and type(value) is str:
        return value
    if tag == "boolean" and type(value) is bool:
        return value
    if tag in ("integer", "decimal") and type(value) is str:
        scalar = int(value) if tag == "integer" else Decimal(value)
        if str(scalar) == value:
            return scalar
    raise PreparedSourceCodecError("prepared_source_row_invalid")


def _check_row_structure(raw_line: bytes, maximum_fields: int) -> None:
    """Bound container and item allocation before the JSON parser runs."""

    container_depth = array_count = comma_count = 0
    is_quoted = is_escaped = False
    for byte_value in raw_line:
        if is_quoted and is_escaped:
            is_escaped = False
            continue
        if is_quoted and byte_value == 92:
            is_escaped = True
            continue
        if byte_value == 34:
            is_quoted = not is_quoted
            continue
        if is_quoted:
            continue
        if byte_value == 91:
            container_depth += 1
            array_count += 1
            if container_depth > 3 or array_count > maximum_fields + 2:
                raise ValueError
            continue
        if byte_value == 93:
            container_depth -= 1
            if container_depth < 0:
                raise ValueError
            continue
        if byte_value in (123, 125):
            raise ValueError
        if byte_value == 44:
            comma_count += 1
            if comma_count > 3 * maximum_fields + 1:
                raise ValueError
    if is_quoted or container_depth:
        raise ValueError


def _decode_row(raw_line: bytes, expected_ordinal: int, limits: CaptureLimits) -> DecodedRecord:
    try:
        _check_row_structure(raw_line, limits.maximum_fields_per_record)
        document = json.loads(raw_line.decode("utf-8"))
        if type(document) is not list or len(document) != 2:
            raise ValueError
        ordinal, entries = document
        if type(ordinal) is not int or ordinal != expected_ordinal:
            raise PreparedSourceCodecError("prepared_source_ordinal_invalid")
        if type(entries) is not list or len(entries) > limits.maximum_fields_per_record:
            raise ValueError
        values_by_label = {}
        for entry in entries:
            if type(entry) is not list or len(entry) != 3:
                raise ValueError
            label, tag, value = entry
            if type(label) is not str or label in values_by_label or type(tag) is not str:
                raise ValueError
            values_by_label[label] = _decoded_scalar(tag, value)
        checked = _decoded_record(ordinal, values_by_label, limits)
        if _row_bytes(ordinal, entries) != raw_line:
            raise ValueError
        return checked
    except PreparedSourceCodecError:
        raise
    except ValueError, UnicodeError, DecimalException, RecursionError:
        raise PreparedSourceCodecError("prepared_source_row_invalid") from None


def iter_prepared_source_records(
    binary: BinaryIO,
    *,
    limits: CaptureLimits,
    part_byte_limit: int,
    part_row_limit: int,
    first_source_ordinal: int,
    record_count: int,
) -> Iterator[DecodedRecord]:
    """Read just one part; complete iteration validates its exact row coverage.

    Part payload/manifest digests and aggregate stream coverage belong to the
    retained reader. This function does not close its caller-owned stream.
    """

    row_byte_limit = _part_limits(limits, part_byte_limit, part_row_limit)
    if (
        type(first_source_ordinal) is not int
        or not 1 <= first_source_ordinal <= limits.maximum_records
        or type(record_count) is not int
        or not 0 <= record_count <= part_row_limit
        or first_source_ordinal + record_count - 1 > limits.maximum_records
        or (record_count == 0 and first_source_ordinal != 1)
    ):
        raise PreparedSourceCodecError("prepared_source_part_metadata_invalid")
    byte_count = row_count = 0
    while True:
        try:
            raw_line = binary.readline(min(row_byte_limit, part_byte_limit - byte_count) + 1)
        except OSError:
            raise PreparedSourceCodecError("prepared_source_part_invalid") from None
        if type(raw_line) is not bytes:
            raise PreparedSourceCodecError("prepared_source_part_invalid")
        if not raw_line:
            break
        byte_count += len(raw_line)
        if len(raw_line) > row_byte_limit or byte_count > part_byte_limit:
            raise PreparedSourceCodecError("prepared_source_part_byte_limit_exceeded")
        if row_count >= record_count:
            raise PreparedSourceCodecError("prepared_source_part_record_count_invalid")
        if not raw_line.endswith(b"\n"):
            raise PreparedSourceCodecError("prepared_source_part_framing_invalid")
        yield _decode_row(raw_line, first_source_ordinal + row_count, limits)
        row_count += 1
    if row_count != record_count:
        raise PreparedSourceCodecError("prepared_source_part_record_count_invalid")
