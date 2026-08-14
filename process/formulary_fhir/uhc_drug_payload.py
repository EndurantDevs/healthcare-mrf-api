# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure structural admission for retained UHC formulary JSON payloads."""

from __future__ import annotations

from pathlib import Path
from typing import Any, BinaryIO, Callable, Iterator

import ijson
from ijson.backends import python as strict_ijson


class UHCDrugPayloadError(ValueError):
    """Reject a payload without exposing its contents."""


MAX_JSON_NESTING_DEPTH = 32
MAX_JSON_RECORD_BYTES = 67_108_864
MAX_JSON_SCALAR_BYTES = 1_048_576
_SOURCE_JSON_ERRORS = (ijson.JSONError, ValueError, ArithmeticError, SystemError)
_SURROGATE_ESCAPE_PREFIXES = tuple(
    f"\\ud{suffix}".encode() for suffix in "89abcdef"
)


def _is_root_array_item_start(event_name: str) -> bool:
    if event_name == "end_array":
        return False
    if event_name != "start_map":
        raise UHCDrugPayloadError("array item is not an object")
    return True


def _require_unique_map_key(
    container_keys: list[set[str] | None],
    event_value: object,
) -> None:
    current_keys = container_keys[-1]
    if current_keys is None or event_value in current_keys:
        raise UHCDrugPayloadError("duplicate JSON key is invalid")
    current_keys.add(event_value)


def _close_map(container_keys: list[set[str] | None]) -> None:
    if container_keys[-1] is None:
        raise UHCDrugPayloadError("JSON nesting is invalid")
    container_keys.pop()


def _close_array(container_keys: list[set[str] | None]) -> None:
    if container_keys[-1] is not None:
        raise UHCDrugPayloadError("JSON nesting is invalid")
    container_keys.pop()


def _apply_container_event(
    container_keys: list[set[str] | None],
    event_name: str,
    event_value: object,
) -> None:
    if event_name == "map_key":
        _require_unique_map_key(container_keys, event_value)
        return
    if event_name == "start_map":
        container_keys.append(set())
        return
    if event_name == "start_array":
        container_keys.append(None)
        return
    if event_name == "end_map":
        _close_map(container_keys)
        return
    if event_name == "end_array":
        _close_array(container_keys)


def _json_event_bytes(event_name: str, event_value: object) -> int:
    if event_name in {"string", "map_key"}:
        if type(event_value) is not str:
            raise UHCDrugPayloadError("JSON scalar is invalid")
        scalar_byte_count = len(event_value.encode("utf-8"))
        if scalar_byte_count > MAX_JSON_SCALAR_BYTES:
            raise UHCDrugPayloadError("JSON scalar is too large")
        return scalar_byte_count + 2
    if event_name in {"number", "boolean", "null"}:
        try:
            return len(str(event_value).encode("utf-8")) + 1
        except (ValueError, ArithmeticError, SystemError):
            raise UHCDrugPayloadError("JSON scalar is invalid") from None
    return 1


def _object_array_item_count(
    parser: Iterator[tuple[str, Any]],
    cancel_check: Callable[[], None] | None,
) -> int:
    item_count = 0
    item_byte_count = 0
    container_keys: list[set[str] | None] = [None]
    event_index = 0
    while True:
        if cancel_check is not None and event_index > 0 and event_index % 1_024 == 0:
            cancel_check()
        try:
            event_name, event_value = next(parser)
            event_index += 1
            if len(container_keys) == 1:
                if not _is_root_array_item_start(event_name):
                    return item_count
                item_count += 1
                item_byte_count = _json_event_bytes(event_name, event_value)
                container_keys.append(set())
                continue
            item_byte_count += _json_event_bytes(event_name, event_value)
            if item_byte_count > MAX_JSON_RECORD_BYTES:
                raise UHCDrugPayloadError("JSON record is too large")
            _apply_container_event(container_keys, event_name, event_value)
            if len(container_keys) > MAX_JSON_NESTING_DEPTH:
                raise UHCDrugPayloadError("JSON nesting is invalid")
        except StopIteration:
            break
        except UHCDrugPayloadError:
            raise
        except _SOURCE_JSON_ERRORS:
            raise UHCDrugPayloadError("UHC drug JSON structure is invalid") from None
    raise UHCDrugPayloadError("root array is incomplete")


def _has_surrogate_escape(
    input_file: BinaryIO,
    cancel_check: Callable[[], None] | None,
) -> bool:
    start_offset = input_file.tell()
    overlap = b""
    try:
        while source_chunk := input_file.read(1_048_576):
            if cancel_check is not None:
                cancel_check()
            lowered_chunk = (overlap + source_chunk).lower()
            if any(prefix in lowered_chunk for prefix in _SURROGATE_ESCAPE_PREFIXES):
                return True
            overlap = lowered_chunk[-3:]
        return False
    finally:
        input_file.seek(start_offset)


def count_uhc_drug_stream_items(
    input_file: BinaryIO,
    *,
    cancel_check: Callable[[], None] | None = None,
) -> int:
    """Validate one stream and return its exact nonzero top-level item count."""

    if cancel_check is not None:
        cancel_check()
    parser_backend = (
        strict_ijson if _has_surrogate_escape(input_file, cancel_check) else ijson
    )
    parser = iter(parser_backend.basic_parse(input_file, use_float=False))
    try:
        first_event, _first_value = next(parser)
    except StopIteration:
        raise UHCDrugPayloadError("UHC drug JSON structure is invalid") from None
    except _SOURCE_JSON_ERRORS:
        raise UHCDrugPayloadError("UHC drug JSON structure is invalid") from None
    if first_event != "start_array":
        raise UHCDrugPayloadError("UHC drug JSON structure is invalid")
    try:
        item_count = _object_array_item_count(parser, cancel_check)
    except UHCDrugPayloadError:
        raise UHCDrugPayloadError("UHC drug JSON structure is invalid") from None
    try:
        trailing_event = next(parser, None)
    except _SOURCE_JSON_ERRORS:
        raise UHCDrugPayloadError("UHC drug JSON structure is invalid") from None
    if trailing_event is not None:
        raise UHCDrugPayloadError("UHC drug JSON structure is invalid")
    if item_count <= 0:
        raise UHCDrugPayloadError("UHC drug JSON structure is invalid")
    if cancel_check is not None:
        cancel_check()
    return item_count


def uhc_drug_object_array_item_count(
    source_path: Path,
    *,
    cancel_check: Callable[[], None] | None = None,
) -> int:
    """Return the nonzero item count for one complete top-level object array."""

    try:
        with source_path.open("rb") as input_file:
            return count_uhc_drug_stream_items(
                input_file,
                cancel_check=cancel_check,
            )
    except OSError:
        raise UHCDrugPayloadError("UHC drug JSON structure is invalid") from None


__all__ = (
    "MAX_JSON_RECORD_BYTES",
    "MAX_JSON_SCALAR_BYTES",
    "UHCDrugPayloadError",
    "count_uhc_drug_stream_items",
    "uhc_drug_object_array_item_count",
)
