# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical JSON artifact stream helpers for PTG2 imports."""

from __future__ import annotations

import binascii
import gzip
import hashlib
import io
import json
import struct
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any

try:
    import isal.igzip as igzip
except ImportError:  # pragma: no cover - optional acceleration
    igzip = None

try:
    import inflate64
except ImportError:  # pragma: no cover - dependency is installed in deployed images
    inflate64 = None

from process.ptg_parts.config import (
    PTG2_DEFER_LOGICAL_HASH_BYTES_ENV,
    PTG2_ISAL_GZIP_ENV,
    _env_bool,
    _env_int,
    _stream_buffer_bytes,
)
from process.ptg_parts.domain import PTG2LogicalArtifact

ZIP_DEFLATE64_METHOD = 9
PTG2_MAX_DECOMPRESSED_BYTES_ENV = "HLTHPRT_PTG2_MAX_DECOMPRESSED_BYTES"
PTG2_DEFAULT_MAX_DECOMPRESSED_BYTES = 1024 * 1024 * 1024 * 1024


class DecompressedArtifactTooLargeError(RuntimeError):
    pass


def _max_decompressed_bytes() -> int:
    return max(
        _env_int(PTG2_MAX_DECOMPRESSED_BYTES_ENV, PTG2_DEFAULT_MAX_DECOMPRESSED_BYTES),
        1,
    )


class _DecompressedByteLimitReader:
    def __init__(self, source, *, limit: int, label: str):
        self._source = source
        self._limit = limit
        self._label = label
        self._total = 0

    def is_readable(self) -> bool:
        """Return whether the bounded stream accepts reads."""
        return True

    readable = is_readable

    def read(self, size: int = -1) -> bytes:
        """Read bytes while enforcing the configured expansion limit."""
        if size == 0:
            return b""
        if size is None or size < 0:
            chunks = []
            while True:
                chunk = self.read(1024 * 1024)
                if not chunk:
                    return b"".join(chunks)
                chunks.append(chunk)

        remaining = self._limit - self._total
        chunk = self._source.read(min(size, remaining + 1))
        self._total += len(chunk)
        if self._total > self._limit:
            raise DecompressedArtifactTooLargeError(
                f"Decompressed PTG2 artifact {self._label} exceeds "
                f"{PTG2_MAX_DECOMPRESSED_BYTES_ENV}={self._limit} bytes"
            )
        return chunk

    def readinto(self, buffer) -> int:
        """Fill a writable buffer without bypassing the expansion limit."""
        chunk = self.read(len(buffer))
        buffer[: len(chunk)] = chunk
        return len(chunk)


class _Utf8BomSkippingReader:
    def __init__(self, source):
        self._source = source
        self._prefix = b""
        self._checked = False

    def is_readable(self) -> bool:
        """Return whether the JSON stream accepts reads."""

        return True

    readable = is_readable

    def _ensure_checked(self) -> None:
        if self._checked:
            return
        prefix_parts = []
        prefix_length = 0
        while prefix_length < 3:
            part = self._source.read(3 - prefix_length)
            if not part:
                break
            prefix_parts.append(part)
            prefix_length += len(part)
        prefix = b"".join(prefix_parts)
        self._prefix = b"" if prefix == b"\xef\xbb\xbf" else prefix
        self._checked = True

    def read(self, size: int = -1) -> bytes:
        """Read JSON bytes after removing one leading UTF-8 BOM."""

        if size == 0:
            return b""
        self._ensure_checked()
        if size is None or size < 0:
            result = self._prefix + self._source.read()
            self._prefix = b""
            return result
        if len(self._prefix) >= size:
            result = self._prefix[:size]
            self._prefix = self._prefix[size:]
            return result
        prefix = self._prefix
        self._prefix = b""
        return prefix + self._source.read(size - len(prefix))

    def readinto(self, buffer) -> int:
        """Fill a writable buffer without exposing a leading UTF-8 BOM."""

        chunk = self.read(len(buffer))
        buffer[: len(chunk)] = chunk
        return len(chunk)


def _stream_copy_with_hash(src, dst, chunk_size: int = 1024 * 1024) -> tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    for chunk in iter(lambda: src.read(chunk_size), b""):
        digest.update(chunk)
        total += len(chunk)
        dst.write(chunk)
    return digest.hexdigest(), total


def _stream_hash(src, chunk_size: int = 1024 * 1024) -> tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    for chunk in iter(lambda: src.read(chunk_size), b""):
        digest.update(chunk)
        total += len(chunk)
    return digest.hexdigest(), total


def _materialize_stream_with_hash(src, target: Path) -> tuple[str, int]:
    try:
        with open(target, "wb") as dst:
            return _stream_copy_with_hash(src, dst)
    except BaseException:
        target.unlink(missing_ok=True)
        raise


def _raw_file_is_gzip(path: str | Path) -> bool:
    path_obj = Path(path)
    if path_obj.name.endswith(".gz"):
        return True
    try:
        with open(path_obj, "rb") as fp:
            return fp.read(2) == b"\x1f\x8b"
    except OSError:
        return False


def _first_zip_member_info(zip_ref: zipfile.ZipFile) -> zipfile.ZipInfo | None:
    for info in zip_ref.infolist():
        if not info.is_dir():
            return info
    return None


def _first_zip_member(path: str | Path) -> str | None:
    with zipfile.ZipFile(path, "r") as zip_ref:
        info = _first_zip_member_info(zip_ref)
        return info.filename if info is not None else None


def _zip_member_payload_offset(raw_fp, info: zipfile.ZipInfo) -> int:
    raw_fp.seek(info.header_offset)
    header = raw_fp.read(30)
    if len(header) != 30:
        raise RuntimeError(f"Invalid zip local header for {info.filename}")
    signature, *_fields, filename_len, extra_len = struct.unpack("<IHHHHHIIIHH", header)
    if signature != 0x04034B50:
        raise RuntimeError(f"Invalid zip local header signature for {info.filename}")
    return info.header_offset + 30 + filename_len + extra_len


def _bounded_decompressed_reader(source, *, label: str, declared_size: int | None = None):
    limit = _max_decompressed_bytes()
    if declared_size is not None and declared_size > limit:
        raise DecompressedArtifactTooLargeError(
            f"Decompressed PTG2 artifact {label} declares {declared_size} bytes, "
            f"exceeding {PTG2_MAX_DECOMPRESSED_BYTES_ENV}={limit} bytes"
        )
    return _DecompressedByteLimitReader(source, limit=limit, label=label)


class _Deflate64ZipMemberReader:
    def __init__(self, raw_fp, info: zipfile.ZipInfo, *, compressed_chunk_size: int = 256 * 1024):
        if inflate64 is None:
            raise RuntimeError("Deflate64 zip members require the inflate64 package")
        if info.flag_bits & 0x1:
            raise RuntimeError(f"Encrypted zip member is not supported: {info.filename}")
        self._raw_fp = raw_fp
        self._remaining = info.compress_size
        self._chunk_size = compressed_chunk_size
        self._inflater = inflate64.Inflater()
        self._buffer = bytearray()
        self._flushed = False
        self._filename = info.filename
        self._expected_size = info.file_size
        self._expected_crc = info.CRC
        self._decompressed_size = 0
        self._crc = 0

    def is_readable(self) -> bool:
        """Return whether this zip member stream supports reads."""
        return True

    readable = is_readable

    def _record_inflated(self, payload: bytes) -> bytes:
        if not payload:
            return payload
        self._decompressed_size += len(payload)
        self._crc = binascii.crc32(payload, self._crc) & 0xFFFFFFFF
        if self._decompressed_size > self._expected_size:
            raise zipfile.BadZipFile(
                f"Deflate64 member {self._filename} exceeds declared size {self._expected_size}"
            )
        return payload

    def _inflate_next(self) -> bytes:
        requested = min(self._chunk_size, self._remaining)
        compressed_chunk = self._raw_fp.read(requested)
        if not compressed_chunk:
            raise EOFError(f"Truncated Deflate64 compressed data for {self._filename}")
        self._remaining -= len(compressed_chunk)
        inflated = self._record_inflated(self._inflater.inflate(compressed_chunk))
        if self._inflater.eof and self._remaining:
            raise zipfile.BadZipFile(
                f"Deflate64 member {self._filename} ended before its declared compressed size"
            )
        return inflated

    def _finish(self) -> bytes:
        if self._flushed:
            return b""
        if self._remaining:
            raise EOFError(f"Truncated Deflate64 compressed data for {self._filename}")
        tail = self._record_inflated(self._inflater.inflate(b""))
        self._flushed = True
        if not self._inflater.eof:
            raise EOFError(f"Truncated Deflate64 stream for {self._filename}")
        if self._decompressed_size != self._expected_size:
            raise zipfile.BadZipFile(
                f"Deflate64 member {self._filename} has size {self._decompressed_size}, "
                f"expected {self._expected_size}"
            )
        if self._crc != self._expected_crc:
            raise zipfile.BadZipFile(
                f"Bad CRC-32 for Deflate64 member {self._filename}: "
                f"{self._crc:08x} != {self._expected_crc:08x}"
            )
        return tail

    def read(self, size: int = -1) -> bytes:
        """Read decompressed bytes from the Deflate64 zip member."""
        if size == 0:
            return b""
        if size is None or size < 0:
            decompressed_chunks = [bytes(self._buffer)]
            self._buffer.clear()
            while self._remaining > 0:
                inflated = self._inflate_next()
                if inflated:
                    decompressed_chunks.append(inflated)
            tail = self._finish()
            if tail:
                decompressed_chunks.append(tail)
            return b"".join(decompressed_chunks)

        while len(self._buffer) < size and self._remaining > 0:
            inflated = self._inflate_next()
            if inflated:
                self._buffer.extend(inflated)
        if self._remaining == 0:
            tail = self._finish()
            if tail:
                self._buffer.extend(tail)
        decompressed_result = bytes(self._buffer[:size])
        del self._buffer[:size]
        return decompressed_result


@contextmanager
def _open_zip_member_stream(path: Path, info: zipfile.ZipInfo, zip_ref: zipfile.ZipFile):
    if info.compress_type == ZIP_DEFLATE64_METHOD:
        with open(path, "rb") as raw_fp:
            raw_fp.seek(_zip_member_payload_offset(raw_fp, info))
            yield _Deflate64ZipMemberReader(raw_fp, info)
        return
    with zip_ref.open(info, "r") as fp:
        yield fp


@contextmanager
def open_json_artifact_stream(path: str | Path):
    """Yield decompressed JSON bytes from plain, gzip, or zip artifacts."""
    path_obj = Path(path)
    if _raw_file_is_gzip(path_obj):
        with open(path_obj, "rb") as raw_fp:
            gzip_cls = igzip.IGzipFile if igzip is not None and _env_bool(PTG2_ISAL_GZIP_ENV, False) else gzip.GzipFile
            with gzip_cls(fileobj=raw_fp, mode="rb") as gzip_fp:
                with io.BufferedReader(gzip_fp, buffer_size=_stream_buffer_bytes()) as buffered_fp:
                    bounded_fp = _bounded_decompressed_reader(buffered_fp, label=str(path_obj))
                    yield _Utf8BomSkippingReader(bounded_fp)
        return
    if zipfile.is_zipfile(path_obj):
        with zipfile.ZipFile(path_obj, "r") as zip_ref:
            member_info = _first_zip_member_info(zip_ref)
            if member_info is None:
                raise RuntimeError(f"No file members found in zip artifact {path_obj}")
            with _open_zip_member_stream(path_obj, member_info, zip_ref) as fp:
                bounded_fp = _bounded_decompressed_reader(
                    fp,
                    label=f"{path_obj}:{member_info.filename}",
                    declared_size=member_info.file_size,
                )
                yield _Utf8BomSkippingReader(bounded_fp)
        return
    with open(path_obj, "rb") as fp:
        yield _Utf8BomSkippingReader(fp)


def _zip_member_name(path: str | Path) -> str | None:
    with zipfile.ZipFile(path, "r") as zip_ref:
        info = _first_zip_member_info(zip_ref)
        return info.filename if info is not None else None


def _compression_for_path(raw_path: str | Path) -> tuple[str | None, str | None]:
    raw_path_obj = Path(raw_path)
    compression = "gzip" if _raw_file_is_gzip(raw_path_obj) else None
    member_name = None
    if compression is None and zipfile.is_zipfile(raw_path_obj):
        compression = "zip"
        member_name = _zip_member_name(raw_path_obj)
    return compression, member_name


def logical_artifact_identity(
    raw_path: str | Path,
    *,
    raw_sha256: str | None = None,
    raw_byte_count: int | None = None,
    allow_deferred: bool = False,
) -> PTG2LogicalArtifact:
    """Return a logical identity, or a marked raw-container identity when deferred."""
    raw_path_obj = Path(raw_path)
    compression, member_name = _compression_for_path(raw_path_obj)
    threshold = _env_int(PTG2_DEFER_LOGICAL_HASH_BYTES_ENV, 1024 * 1024 * 1024)
    if (
        allow_deferred
        and compression is not None
        and raw_sha256
        and raw_byte_count
        and threshold > 0
        and raw_byte_count >= threshold
    ):
        return PTG2LogicalArtifact(
            str(raw_path_obj),
            raw_sha256,
            raw_byte_count,
            compression=compression,
            member_name=member_name,
            logical_hash_deferred=True,
        )
    with open_json_artifact_stream(raw_path_obj) as src:
        digest, total = _stream_hash(src)
    return PTG2LogicalArtifact(str(raw_path_obj), digest, total, compression=compression, member_name=member_name)


def load_json_artifact(path: str | Path) -> Any:
    """Load and decode JSON from a supported PTG2 artifact container."""
    with open_json_artifact_stream(path) as fp:
        return json.load(fp)


def stream_logical_artifact(raw_path: str | Path, output_dir: str | Path | None = None) -> PTG2LogicalArtifact:
    """Materialize compressed JSON when needed and return its logical identity."""
    raw_path_obj = Path(raw_path)
    output_root = Path(output_dir) if output_dir else raw_path_obj.parent
    output_root.mkdir(parents=True, exist_ok=True)
    if _raw_file_is_gzip(raw_path_obj):
        logical_path = output_root / f"{raw_path_obj.stem}_logical.json"
        try:
            with open_json_artifact_stream(raw_path_obj) as src:
                digest, total = _materialize_stream_with_hash(src, logical_path)
        except BaseException:
            logical_path.unlink(missing_ok=True)
            raise
        return PTG2LogicalArtifact(str(logical_path), digest, total, compression="gzip")
    if zipfile.is_zipfile(raw_path_obj):
        with zipfile.ZipFile(raw_path_obj, "r") as zip_ref:
            member_info = _first_zip_member_info(zip_ref)
            if member_info is None:
                raise RuntimeError(f"No file members found in zip artifact {raw_path_obj}")
            logical_path = output_root / Path(member_info.filename).name
            try:
                with _open_zip_member_stream(raw_path_obj, member_info, zip_ref) as src:
                    bounded_src = _bounded_decompressed_reader(
                        src,
                        label=f"{raw_path_obj}:{member_info.filename}",
                        declared_size=member_info.file_size,
                    )
                    normalized_src = _Utf8BomSkippingReader(bounded_src)
                    digest, total = _materialize_stream_with_hash(normalized_src, logical_path)
            except BaseException:
                logical_path.unlink(missing_ok=True)
                raise
            return PTG2LogicalArtifact(
                str(logical_path),
                digest,
                total,
                compression="zip",
                member_name=member_info.filename,
            )
    with open_json_artifact_stream(raw_path_obj) as src:
        digest, total = _stream_hash(src)
    return PTG2LogicalArtifact(str(raw_path_obj), digest, total)
