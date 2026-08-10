# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure structural admission for retained UHC formulary JSON payloads."""

from __future__ import annotations

from pathlib import Path
from typing import Any, BinaryIO, Callable, Iterator

import ijson


class UHCDrugPayloadError(ValueError):
    """Reject a payload without exposing its contents."""


MAX_JSON_NESTING_DEPTH = 32
MAX_JSON_RECORD_BYTES = 67_108_864
MAX_JSON_SCALAR_BYTES = 1_048_576


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
        return len(str(event_value).encode("utf-8")) + 1
    return 1


def _object_array_item_count(
    parser: Iterator[tuple[str, Any]],
    cancel_check: Callable[[], None] | None,
) -> int:
    item_count = 0
    item_byte_count = 0
    container_keys: list[set[str] | None] = [None]
    for event_index, (event_name, event_value) in enumerate(parser, start=1):
        if cancel_check is not None and event_index % 1_024 == 0:
            cancel_check()
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
    raise UHCDrugPayloadError("root array is incomplete")


def count_uhc_drug_stream_items(
    input_file: BinaryIO,
    *,
    cancel_check: Callable[[], None] | None = None,
) -> int:
    """Validate one stream and return its exact nonzero top-level item count."""

    try:
        parser = iter(ijson.basic_parse(input_file, use_float=False))
        first_event, _first_value = next(parser)
        if first_event != "start_array":
            raise UHCDrugPayloadError("root is not an array")
        if cancel_check is not None:
            cancel_check()
        item_count = _object_array_item_count(parser, cancel_check)
        sentinel = object()
        if next(parser, sentinel) is not sentinel:
            raise UHCDrugPayloadError("trailing JSON value is invalid")
    except (StopIteration, ValueError, ijson.JSONError):
        raise UHCDrugPayloadError("UHC drug JSON structure is invalid") from None
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
