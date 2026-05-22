# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Streaming JSON object iterators for PTG imports."""

from __future__ import annotations

import codecs
import json
from typing import Any

import ijson
from ijson.common import ObjectBuilder

try:
    import orjson
except ImportError:  # pragma: no cover - optional acceleration
    orjson = None

from process.ptg_parts.config import (
    PTG2_FAST_JSON_LOADS_ENV,
    PTG2_JSON_DECODER_ITERATOR_ENV,
    _env_bool,
    _stream_buffer_bytes,
)


def _json_loads(value: str | bytes | bytearray) -> Any:
    if orjson is not None and _env_bool(PTG2_FAST_JSON_LOADS_ENV, True):
        return orjson.loads(value)
    return json.loads(value)


def _iter_top_level_objects(
    file_obj,
    item_prefixes: dict[str, str],
    use_float: bool = True,
    progress_callback=None,
):
    active_name = None
    active_prefix = None
    builder = None
    event_count = 0
    for prefix, event, value in ijson.parse(file_obj, use_float=use_float):
        event_count += 1
        if progress_callback is not None and event_count % 100000 == 0:
            progress_callback()
        if builder is not None:
            builder.event(event, value)
            if prefix == active_prefix and event in {"end_map", "end_array"}:
                yield active_name, builder.value
                active_name = None
                active_prefix = None
                builder = None
            continue
        if event not in {"start_map", "start_array"}:
            continue
        for name, item_prefix in item_prefixes.items():
            if prefix == item_prefix:
                active_name = name
                active_prefix = item_prefix
                builder = ObjectBuilder()
                builder.event(event, value)
                break


def _iter_top_level_object_bytes(
    file_obj,
    array_names: set[str],
    *,
    progress_callback=None,
    chunk_size: int | None = None,
):
    """
    Yield raw JSON object bytes from selected top-level arrays.

    This avoids ijson's Python ObjectBuilder for very large TiC objects while
    preserving streaming gzip reads. The scanner only recognizes top-level
    object keys and then captures complete object values inside selected arrays.
    """
    chunk_size = chunk_size or _stream_buffer_bytes()
    targets = {name.encode("utf-8"): name for name in array_names}
    depth = 0
    active_name: str | None = None
    active_array_depth = 0
    capture = bytearray()
    capture_depth = 0
    in_string = False
    escape = False
    string_buffer: bytearray | None = None
    candidate_key: bytes | None = None
    pending_key: bytes | None = None
    bytes_since_progress = 0

    while True:
        chunk = file_obj.read(chunk_size)
        if not chunk:
            break
        bytes_since_progress += len(chunk)
        if progress_callback is not None and bytes_since_progress >= 64 * 1024 * 1024:
            progress_callback()
            bytes_since_progress = 0
        for byte in chunk:
            char = byte
            if capture_depth:
                capture.append(char)
            if in_string:
                if string_buffer is not None:
                    string_buffer.append(char)
                if escape:
                    escape = False
                elif char == 0x5C:  # backslash
                    escape = True
                elif char == 0x22:  # quote
                    in_string = False
                    if string_buffer is not None:
                        candidate_key = bytes(string_buffer[:-1])
                        string_buffer = None
                continue

            if char == 0x22:  # quote
                in_string = True
                escape = False
                if depth == 1 and not active_name and not capture_depth:
                    string_buffer = bytearray()
                else:
                    string_buffer = None
                continue

            if candidate_key is not None:
                if char in (0x20, 0x09, 0x0A, 0x0D):
                    continue
                if char == 0x3A:  # colon
                    pending_key = candidate_key
                candidate_key = None
                if char == 0x3A:
                    continue

            if pending_key is not None:
                if char in (0x20, 0x09, 0x0A, 0x0D):
                    continue
                if char == 0x5B and pending_key in targets and depth == 1:  # [
                    depth += 1
                    active_name = targets[pending_key]
                    active_array_depth = depth
                    pending_key = None
                    continue
                pending_key = None

            if active_name and not capture_depth and char == 0x7B and depth == active_array_depth:  # {
                capture = bytearray(b"{")
                capture_depth = 1
                depth += 1
                continue

            if char in (0x7B, 0x5B):  # { [
                if capture_depth:
                    capture_depth += 1
                    depth += 1
                    continue
                depth += 1
                continue
            if char in (0x7D, 0x5D):  # } ]
                if capture_depth:
                    capture_depth -= 1
                    if capture_depth == 0:
                        yield active_name, bytes(capture)
                        capture = bytearray()
                        depth -= 1
                        continue
                if active_name and char == 0x5D and depth == active_array_depth:
                    active_name = None
                    active_array_depth = 0
                depth = max(depth - 1, 0)


def _skip_json_ws(buffer: str, pos: int) -> int:
    while pos < len(buffer) and buffer[pos] in " \t\r\n":
        pos += 1
    return pos


def _iter_top_level_objects_jsondecoder(
    file_obj,
    item_prefixes: dict[str, str],
    *,
    progress_callback=None,
    chunk_size: int | None = None,
    raw_object_names: set[str] | None = None,
):
    """
    Stream selected top-level array objects with JSONDecoder.raw_decode.

    The older byte scanner touched every decompressed byte in Python. This
    iterator still keeps only a bounded buffer, but lets CPython's JSON decoder
    find each object boundary while parsing the object payload.
    """
    chunk_size = chunk_size or _stream_buffer_bytes()
    array_to_name = {
        item_prefix.removesuffix(".item"): name
        for name, item_prefix in item_prefixes.items()
        if item_prefix.endswith(".item")
    }
    if not array_to_name:
        return
    key_tokens = {array_name: f'"{array_name}"' for array_name in array_to_name}
    max_key_len = max(len(token) for token in key_tokens.values())
    utf8_decoder = codecs.getincrementaldecoder("utf-8")()
    json_decoder = json.JSONDecoder()
    buffer = ""
    pos = 0
    eof = False
    active_array: str | None = None
    bytes_since_progress = 0

    def read_more() -> bool:
        nonlocal buffer, eof, bytes_since_progress
        if eof:
            return False
        chunk = file_obj.read(chunk_size)
        if not chunk:
            tail = utf8_decoder.decode(b"", final=True)
            if tail:
                buffer += tail
            eof = True
            return False
        bytes_since_progress += len(chunk)
        if progress_callback is not None and bytes_since_progress >= 64 * 1024 * 1024:
            progress_callback()
            bytes_since_progress = 0
        buffer += utf8_decoder.decode(chunk, final=False)
        return True

    def compact_buffer(force: bool = False) -> None:
        nonlocal buffer, pos
        if pos <= 0:
            return
        if force or pos > chunk_size:
            buffer = buffer[pos:]
            pos = 0

    while True:
        if active_array is None:
            while True:
                matches = [
                    (found_at, array_name, token)
                    for array_name, token in key_tokens.items()
                    for found_at in [buffer.find(token, pos)]
                    if found_at >= 0
                ]
                if matches:
                    found_at, array_name, token = min(matches, key=lambda item: item[0])
                    scan = _skip_json_ws(buffer, found_at + len(token))
                    while True:
                        if scan >= len(buffer):
                            if not read_more():
                                return
                            continue
                        if buffer[scan] != ":":
                            pos = found_at + len(token)
                            break
                        scan = _skip_json_ws(buffer, scan + 1)
                        while scan >= len(buffer):
                            if not read_more():
                                return
                            scan = _skip_json_ws(buffer, scan)
                        if buffer[scan] != "[":
                            pos = scan + 1
                            break
                        active_array = array_name
                        pos = scan + 1
                        compact_buffer(force=True)
                        break
                    if active_array is not None:
                        break
                    continue
                if eof:
                    return
                keep_from = max(len(buffer) - (max_key_len + 32), 0)
                if keep_from:
                    buffer = buffer[keep_from:]
                    pos = 0
                if not read_more() and eof:
                    return

        pos = _skip_json_ws(buffer, pos)
        while pos >= len(buffer):
            if not read_more():
                return
            pos = _skip_json_ws(buffer, pos)

        if buffer[pos] == ",":
            pos += 1
            continue
        if buffer[pos] == "]":
            pos += 1
            active_array = None
            compact_buffer(force=True)
            continue

        start_pos = pos
        try:
            payload, end_pos = json_decoder.raw_decode(buffer, pos)
        except json.JSONDecodeError:
            if eof:
                raise
            read_more()
            continue
        object_name = array_to_name[active_array]
        if raw_object_names and object_name in raw_object_names:
            yield object_name, buffer[start_pos:end_pos].encode("utf-8")
            del payload
        else:
            yield object_name, payload
        pos = end_pos
        compact_buffer()


def _iter_top_level_objects_fast(
    file_obj,
    item_prefixes: dict[str, str],
    *,
    use_float: bool = True,
    progress_callback=None,
):
    del use_float
    if _env_bool(PTG2_JSON_DECODER_ITERATOR_ENV, True):
        yield from _iter_top_level_objects_jsondecoder(
            file_obj,
            item_prefixes,
            progress_callback=progress_callback,
        )
        return
    array_names = {
        item_prefix.removesuffix(".item")
        for item_prefix in item_prefixes.values()
        if item_prefix.endswith(".item")
    }
    prefix_to_name = {item_prefix.removesuffix(".item"): name for name, item_prefix in item_prefixes.items()}
    for array_name, raw_object in _iter_top_level_object_bytes(
        file_obj,
        array_names,
        progress_callback=progress_callback,
    ):
        yield prefix_to_name[array_name], _json_loads(raw_object)
