# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded PostgreSQL binary-COPY filtering for immutable shared PTG blocks."""

from __future__ import annotations

import hashlib
import io
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO


_COPY_SIGNATURE = b"PGCOPY\n\xff\r\n\x00"
_COPY_HEADER = _COPY_SIGNATURE + struct.pack(">ii", 0, 0)
_COPY_TRAILER = struct.pack(">h", -1)
_FIELD_COUNT = 10
_FORMAT_VERSION = 2


def _read_exact(source: BinaryIO, size: int, *, label: str) -> bytes:
    value = source.read(size)
    if len(value) != size:
        raise RuntimeError(f"strict V3 shared-block COPY truncates {label}")
    return value


def _decode_i16(value: bytes) -> int:
    return struct.unpack(">h", value)[0]


def _decode_i32(value: bytes) -> int:
    return struct.unpack(">i", value)[0]


def _decode_i64(value: bytes) -> int:
    return struct.unpack(">q", value)[0]


def _validated_metadata(fields: tuple[bytes, ...]) -> tuple[bytes, int]:
    if len(fields) != _FIELD_COUNT - 1:
        raise RuntimeError("strict V3 shared-block COPY metadata field count changed")
    if len(fields[0]) != 32:
        raise RuntimeError("strict V3 shared-block COPY hash is not 32 bytes")
    if len(fields[1]) != 2 or _decode_i16(fields[1]) != _FORMAT_VERSION:
        raise RuntimeError("strict V3 shared-block COPY format version is incompatible")
    try:
        object_kind = fields[2].decode("utf-8")
        codec = fields[6].decode("ascii")
    except UnicodeDecodeError as exc:
        raise RuntimeError("strict V3 shared-block COPY metadata text is invalid") from exc
    if not object_kind or len(fields[2]) > 64:
        raise RuntimeError("strict V3 shared-block COPY object kind is invalid")
    if codec not in {"none", "zlib"}:
        raise RuntimeError("strict V3 shared-block COPY codec is invalid")
    integer_contract = (
        (3, 8, "block key", _decode_i64),
        (4, 4, "fragment number", _decode_i32),
        (5, 8, "entry count", _decode_i64),
        (7, 8, "raw byte count", _decode_i64),
        (8, 8, "stored byte count", _decode_i64),
    )
    decoded_integer_by_index: dict[int, int] = {}
    for field_index, expected_size, label, decoder in integer_contract:
        if len(fields[field_index]) != expected_size:
            raise RuntimeError(f"strict V3 shared-block COPY {label} width changed")
        decoded_integer_by_index[field_index] = decoder(fields[field_index])
        if decoded_integer_by_index[field_index] < 0:
            raise RuntimeError(f"strict V3 shared-block COPY {label} is negative")
    return fields[0], decoded_integer_by_index[8]


def _read_metadata_fields(source: BinaryIO) -> tuple[tuple[bytes, ...], list[bytes]]:
    fields: list[bytes] = []
    encoded_fields: list[bytes] = []
    for field_index in range(_FIELD_COUNT - 1):
        encoded_length = _read_exact(
            source,
            4,
            label=f"field {field_index} length",
        )
        field_length = _decode_i32(encoded_length)
        if field_length < 0:
            raise RuntimeError("strict V3 shared-block COPY metadata cannot be NULL")
        field = _read_exact(
            source,
            field_length,
            label=f"field {field_index}",
        )
        fields.append(field)
        encoded_fields.extend((encoded_length, field))
    return tuple(fields), encoded_fields


def _validate_header(source: BinaryIO) -> bytes:
    header = _read_exact(source, len(_COPY_HEADER), label="header")
    if header != _COPY_HEADER:
        raise RuntimeError("strict V3 shared-block COPY header is incompatible")
    return header


def _validate_trailer(source: BinaryIO) -> None:
    if source.read(1):
        raise RuntimeError("strict V3 shared-block COPY contains trailing bytes")


@dataclass(frozen=True)
class SharedBlockCopyScan:
    """Metadata collected without reading the potentially large payload fields."""

    block_hashes: frozenset[bytes]
    row_count: int
    stored_payload_bytes: int


def scan_shared_block_copy(copy_path: str | Path) -> SharedBlockCopyScan:
    """Validate a Rust block COPY and collect its exact requested content hashes."""

    path = Path(copy_path)
    file_size = path.stat().st_size
    block_hashes: set[bytes] = set()
    row_count = 0
    stored_payload_bytes = 0
    with path.open("rb") as source_stream:
        _validate_header(source_stream)
        while True:
            encoded_field_count = _read_exact(
                source_stream,
                2,
                label="row field count",
            )
            field_count = _decode_i16(encoded_field_count)
            if field_count == -1:
                _validate_trailer(source_stream)
                break
            if field_count != _FIELD_COUNT:
                raise RuntimeError("strict V3 shared-block COPY row width changed")
            fields, _encoded_fields = _read_metadata_fields(source_stream)
            block_hash, stored_byte_count = _validated_metadata(fields)
            encoded_payload_length = _read_exact(
                source_stream,
                4,
                label="payload length",
            )
            payload_length = _decode_i32(encoded_payload_length)
            if payload_length < 0 or payload_length != stored_byte_count:
                raise RuntimeError("strict V3 shared-block COPY payload length is invalid")
            payload_end = source_stream.tell() + payload_length
            if payload_end > file_size:
                raise RuntimeError("strict V3 shared-block COPY truncates payload")
            source_stream.seek(payload_length, io.SEEK_CUR)
            block_hashes.add(block_hash)
            row_count += 1
            stored_payload_bytes += payload_length
    if row_count <= 0:
        raise RuntimeError("strict V3 shared-block COPY contains no rows")
    return SharedBlockCopyScan(
        block_hashes=frozenset(block_hashes),
        row_count=row_count,
        stored_payload_bytes=stored_payload_bytes,
    )


class SelectiveSharedBlockCopyReader:
    """Emit full new rows and metadata-only rows for hashes already durable."""

    def __init__(
        self,
        source: BinaryIO,
        *,
        existing_hashes: set[bytes] | frozenset[bytes],
        expected_source_bytes: int,
        expected_source_sha256: str,
    ) -> None:
        self._source = source
        self._existing_hashes = existing_hashes
        self._expected_source_bytes = int(expected_source_bytes)
        self._expected_source_sha256 = str(expected_source_sha256)
        self._sha256 = hashlib.sha256()
        self._source_byte_count = 0
        self._output = bytearray()
        self._started = False
        self._finished = False
        self.row_count = 0
        self.reused_row_count = 0
        self.reused_payload_bytes = 0
        self.copied_payload_bytes = 0

    def _read_source(self, size: int, *, label: str) -> bytes:
        value = _read_exact(self._source, size, label=label)
        self._source_byte_count += len(value)
        self._sha256.update(value)
        return value

    def _read_metadata(self) -> tuple[tuple[bytes, ...], list[bytes]]:
        fields: list[bytes] = []
        encoded_fields: list[bytes] = []
        for field_index in range(_FIELD_COUNT - 1):
            encoded_length = self._read_source(
                4,
                label=f"field {field_index} length",
            )
            field_length = _decode_i32(encoded_length)
            if field_length < 0:
                raise RuntimeError("strict V3 shared-block COPY metadata cannot be NULL")
            field = self._read_source(field_length, label=f"field {field_index}")
            fields.append(field)
            encoded_fields.extend((encoded_length, field))
        return tuple(fields), encoded_fields

    def _verify_source(self) -> None:
        trailing = self._source.read(1)
        if trailing:
            self._source_byte_count += len(trailing)
            self._sha256.update(trailing)
            raise RuntimeError("strict V3 shared-block COPY contains trailing bytes")
        if (
            self._source_byte_count != self._expected_source_bytes
            or self._sha256.hexdigest() != self._expected_source_sha256
        ):
            raise RuntimeError(
                "strict V3 shared-block COPY content changed during publication"
            )

    def _append_next(self) -> None:
        if not self._started:
            header = self._read_source(len(_COPY_HEADER), label="header")
            if header != _COPY_HEADER:
                raise RuntimeError("strict V3 shared-block COPY header is incompatible")
            self._output.extend(header)
            self._started = True
            return
        encoded_field_count = self._read_source(2, label="row field count")
        field_count = _decode_i16(encoded_field_count)
        if field_count == -1:
            self._verify_source()
            self._output.extend(encoded_field_count)
            self._finished = True
            return
        if field_count != _FIELD_COUNT:
            raise RuntimeError("strict V3 shared-block COPY row width changed")
        fields, encoded_fields = self._read_metadata()
        block_hash, stored_byte_count = _validated_metadata(fields)
        encoded_payload_length = self._read_source(4, label="payload length")
        payload_length = _decode_i32(encoded_payload_length)
        if payload_length < 0 or payload_length != stored_byte_count:
            raise RuntimeError("strict V3 shared-block COPY payload length is invalid")
        payload_bytes = self._read_source(payload_length, label="payload")
        self._output.extend(encoded_field_count)
        self._output.extend(b"".join(encoded_fields))
        if block_hash in self._existing_hashes:
            self._output.extend(struct.pack(">i", -1))
            self.reused_row_count += 1
            self.reused_payload_bytes += payload_length
        else:
            self._output.extend(encoded_payload_length)
            self._output.extend(payload_bytes)
            self.copied_payload_bytes += payload_length
        self.row_count += 1

    def read(self, size: int = -1) -> bytes:
        """Return the next filtered COPY chunk through asyncpg's file protocol."""

        if size == 0:
            return b""
        while not self._finished and (size < 0 or len(self._output) < size):
            self._append_next()
        if not self._output:
            return b""
        take = len(self._output) if size < 0 else min(size, len(self._output))
        value = bytes(self._output[:take])
        del self._output[:take]
        return value

    @property
    def source_byte_count(self) -> int:
        """Return bytes consumed from the authenticated source COPY stream."""

        return self._source_byte_count

    @property
    def source_sha256(self) -> str:
        """Return the digest of all authenticated source COPY bytes consumed."""

        return self._sha256.hexdigest()


def binary_copy_rows(copy_payload: bytes) -> tuple[tuple[Any, ...], ...]:
    """Decode test-sized block COPY bytes without depending on PostgreSQL."""

    rows: list[tuple[Any, ...]] = []
    with io.BytesIO(copy_payload) as source:
        _validate_header(source)
        while True:
            field_count = _decode_i16(_read_exact(source, 2, label="row field count"))
            if field_count == -1:
                _validate_trailer(source)
                break
            if field_count != _FIELD_COUNT:
                raise RuntimeError("strict V3 shared-block COPY row width changed")
            values: list[bytes | None] = []
            for field_index in range(_FIELD_COUNT):
                field_length = _decode_i32(
                    _read_exact(source, 4, label=f"field {field_index} length")
                )
                values.append(
                    None
                    if field_length == -1
                    else _read_exact(source, field_length, label=f"field {field_index}")
                )
            rows.append(tuple(values))
    return tuple(rows)
