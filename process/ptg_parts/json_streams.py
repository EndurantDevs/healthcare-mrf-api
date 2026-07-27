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
    for prefix, event, event_value in ijson.parse(file_obj, use_float=use_float):
        event_count += 1
        if progress_callback is not None and event_count % 100000 == 0:
            progress_callback()
        if builder is not None:
            builder.event(event, event_value)
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
                builder.event(event, event_value)
                break


_JSON_WHITESPACE_BYTES = (0x20, 0x09, 0x0A, 0x0D)


class _RawObjectByteScanner:
    """Track top-level arrays and capture their complete object values."""

    def __init__(self, array_names: set[str]) -> None:
        self.target_name_by_token = {
            name.encode("utf-8"): name for name in array_names
        }
        self.depth = 0
        self.active_name: str | None = None
        self.active_array_depth = 0
        self.capture = bytearray()
        self.capture_depth = 0
        self.is_inside_string = False
        self.is_escape_pending = False
        self.string_buffer: bytearray | None = None
        self.candidate_key: bytes | None = None
        self.pending_key: bytes | None = None

    def _consume_string_byte(self, char: int) -> None:
        if self.string_buffer is not None:
            self.string_buffer.append(char)
        if self.is_escape_pending:
            self.is_escape_pending = False
        elif char == 0x5C:  # backslash
            self.is_escape_pending = True
        elif char == 0x22:  # quote
            self.is_inside_string = False
            if self.string_buffer is not None:
                self.candidate_key = bytes(self.string_buffer[:-1])
                self.string_buffer = None

    def _begin_string(self) -> None:
        self.is_inside_string = True
        self.is_escape_pending = False
        if (
            self.depth == 1
            and self.active_name is None
            and self.capture_depth == 0
        ):
            self.string_buffer = bytearray()
        else:
            self.string_buffer = None

    def _has_consumed_candidate_key(self, char: int) -> bool:
        if self.candidate_key is None:
            return False
        if char in _JSON_WHITESPACE_BYTES:
            return True
        if char == 0x3A:  # colon
            self.pending_key = self.candidate_key
        self.candidate_key = None
        return char == 0x3A

    def _has_consumed_pending_key(self, char: int) -> bool:
        if self.pending_key is None:
            return False
        if char in _JSON_WHITESPACE_BYTES:
            return True
        if (
            char == 0x5B
            and self.pending_key in self.target_name_by_token
            and self.depth == 1
        ):
            self.depth += 1
            self.active_name = self.target_name_by_token[self.pending_key]
            self.active_array_depth = self.depth
            self.pending_key = None
            return True
        self.pending_key = None
        return False

    def consume(self, char: int) -> tuple[str, bytes] | None:
        """Consume one byte and return a completed selected object, if any."""

        if self.capture_depth:
            self.capture.append(char)
        if self.is_inside_string:
            self._consume_string_byte(char)
            return None
        if char == 0x22:  # quote
            self._begin_string()
            return None
        if self._has_consumed_candidate_key(char):
            return None
        if self._has_consumed_pending_key(char):
            return None
        if (
            self.active_name is not None
            and self.capture_depth == 0
            and char == 0x7B
            and self.depth == self.active_array_depth
        ):
            self.capture = bytearray(b"{")
            self.capture_depth = 1
            self.depth += 1
            return None
        if char in (0x7B, 0x5B):  # { [
            if self.capture_depth:
                self.capture_depth += 1
            self.depth += 1
            return None
        if char not in (0x7D, 0x5D):  # } ]
            return None
        if self.capture_depth:
            self.capture_depth -= 1
            if self.capture_depth == 0:
                captured_object = bytes(self.capture)
                self.capture = bytearray()
                self.depth -= 1
                return self.active_name, captured_object
        if (
            self.active_name is not None
            and char == 0x5D
            and self.depth == self.active_array_depth
        ):
            self.active_name = None
            self.active_array_depth = 0
        self.depth = max(self.depth - 1, 0)
        return None


def _iter_top_level_object_bytes(
    file_obj,
    array_names: set[str],
    *,
    progress_callback=None,
    chunk_size: int | None = None,
):
    """Yield raw object bytes from selected top-level arrays."""

    read_size = chunk_size or _stream_buffer_bytes()
    scanner = _RawObjectByteScanner(array_names)
    bytes_since_progress = 0
    while chunk := file_obj.read(read_size):
        bytes_since_progress += len(chunk)
        if (
            progress_callback is not None
            and bytes_since_progress >= 64 * 1024 * 1024
        ):
            progress_callback()
            bytes_since_progress = 0
        for byte in chunk:
            captured_object = scanner.consume(byte)
            if captured_object is not None:
                yield captured_object


def _skip_json_ws(buffer: str, pos: int) -> int:
    while pos < len(buffer) and buffer[pos] in " \t\r\n":
        pos += 1
    return pos


class _JSONTextStream:
    """Incrementally decode UTF-8 source blocks into a compact parser buffer."""

    def __init__(
        self,
        file_obj,
        chunk_size: int,
        progress_callback,
    ) -> None:
        self.file_obj = file_obj
        self.chunk_size = chunk_size
        self.progress_callback = progress_callback
        self.utf8_decoder = codecs.getincrementaldecoder("utf-8")()
        self.buffer = ""
        self.position = 0
        self.is_eof = False
        self.bytes_since_progress = 0

    def has_read_next_block(self) -> bool:
        """Decode one block and report whether source bytes were read."""

        if self.is_eof:
            return False
        chunk = self.file_obj.read(self.chunk_size)
        if not chunk:
            self.buffer += self.utf8_decoder.decode(b"", final=True)
            self.is_eof = True
            return False
        self.bytes_since_progress += len(chunk)
        if (
            self.progress_callback is not None
            and self.bytes_since_progress >= 64 * 1024 * 1024
        ):
            self.progress_callback()
            self.bytes_since_progress = 0
        self.buffer += self.utf8_decoder.decode(chunk, final=False)
        return True

    def compact(self, *, force: bool = False) -> None:
        """Discard parsed text while preserving unread JSON content."""

        if self.position <= 0:
            return
        if force or self.position > self.chunk_size:
            self.buffer = self.buffer[self.position :]
            self.position = 0


def _next_non_whitespace_position(
    stream: _JSONTextStream,
    position: int,
) -> int | None:
    while True:
        next_position = _skip_json_ws(stream.buffer, position)
        if next_position < len(stream.buffer):
            return next_position
        if not stream.has_read_next_block():
            return None


def _find_array_token(
    stream: _JSONTextStream,
    key_token_by_array: dict[str, str],
    max_key_len: int,
) -> tuple[int, str, str] | None:
    while True:
        matches = [
            (found_at, array_name, token)
            for array_name, token in key_token_by_array.items()
            for found_at in [stream.buffer.find(token, stream.position)]
            if found_at >= 0
        ]
        if matches:
            return min(matches, key=lambda match: match[0])
        if stream.is_eof:
            return None
        keep_from = max(len(stream.buffer) - (max_key_len + 32), 0)
        if keep_from:
            stream.buffer = stream.buffer[keep_from:]
            stream.position = 0
        if not stream.has_read_next_block() and stream.is_eof:
            return None


def _activate_next_array(
    stream: _JSONTextStream,
    key_token_by_array: dict[str, str],
    max_key_len: int,
) -> str | None:
    while token_match := _find_array_token(
        stream,
        key_token_by_array,
        max_key_len,
    ):
        found_at, array_name, token = token_match
        colon_position = _next_non_whitespace_position(
            stream,
            found_at + len(token),
        )
        if colon_position is None:
            return None
        if stream.buffer[colon_position] != ":":
            stream.position = found_at + len(token)
            continue
        array_position = _next_non_whitespace_position(
            stream,
            colon_position + 1,
        )
        if array_position is None:
            return None
        if stream.buffer[array_position] != "[":
            stream.position = array_position + 1
            continue
        stream.position = array_position + 1
        stream.compact(force=True)
        return array_name
    return None


def _decoder_array_maps(
    item_prefixes: dict[str, str],
) -> tuple[dict[str, str], dict[str, str], int]:
    object_name_by_array = {
        item_prefix.removesuffix(".item"): name
        for name, item_prefix in item_prefixes.items()
        if item_prefix.endswith(".item")
    }
    key_token_by_array = {
        array_name: f'"{array_name}"' for array_name in object_name_by_array
    }
    max_key_len = max(
        (len(token) for token in key_token_by_array.values()),
        default=0,
    )
    return object_name_by_array, key_token_by_array, max_key_len


def _decode_next_object(
    stream: _JSONTextStream,
    json_decoder: json.JSONDecoder,
) -> tuple[Any, int] | None:
    try:
        return json_decoder.raw_decode(stream.buffer, stream.position)
    except json.JSONDecodeError:
        if stream.is_eof:
            raise
        stream.has_read_next_block()
        return None


def _iter_top_level_objects_jsondecoder(
    file_obj,
    item_prefixes: dict[str, str],
    *,
    progress_callback=None,
    chunk_size: int | None = None,
    raw_object_names: set[str] | None = None,
):
    """Stream selected top-level array objects with JSONDecoder.raw_decode."""

    read_size = chunk_size or _stream_buffer_bytes()
    object_name_by_array, key_token_by_array, max_key_len = (
        _decoder_array_maps(item_prefixes)
    )
    if not object_name_by_array:
        return
    stream = _JSONTextStream(file_obj, read_size, progress_callback)
    json_decoder = json.JSONDecoder()
    active_array: str | None = None

    while True:
        if active_array is None:
            active_array = _activate_next_array(
                stream,
                key_token_by_array,
                max_key_len,
            )
            if active_array is None:
                return
        next_position = _next_non_whitespace_position(
            stream,
            stream.position,
        )
        if next_position is None:
            return
        stream.position = next_position
        if stream.buffer[stream.position] == ",":
            stream.position += 1
            continue
        if stream.buffer[stream.position] == "]":
            stream.position += 1
            active_array = None
            stream.compact(force=True)
            continue
        start_position = stream.position
        decoded_entry = _decode_next_object(stream, json_decoder)
        if decoded_entry is None:
            continue
        decoded_object, end_position = decoded_entry
        object_name = object_name_by_array[active_array]
        if raw_object_names and object_name in raw_object_names:
            yield (
                object_name,
                stream.buffer[start_position:end_position].encode("utf-8"),
            )
        else:
            yield object_name, decoded_object
        stream.position = end_position
        stream.compact()


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
    object_name_by_prefix = {
        item_prefix.removesuffix(".item"): name
        for name, item_prefix in item_prefixes.items()
    }
    for array_name, raw_object in _iter_top_level_object_bytes(
        file_obj,
        array_names,
        progress_callback=progress_callback,
    ):
        yield object_name_by_prefix[array_name], _json_loads(raw_object)
