# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical JSON artifact stream helpers for PTG2 imports."""

from __future__ import annotations

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

from process.ptg_parts.artifacts import sha256_file
from process.ptg_parts.config import (
    PTG2_DEFER_LOGICAL_HASH_BYTES_ENV,
    PTG2_ISAL_GZIP_ENV,
    _env_bool,
    _env_int,
    _stream_buffer_bytes,
)
from process.ptg_parts.domain import PTG2LogicalArtifact

ZIP_DEFLATE64_METHOD = 9


def _stream_copy_with_hash(src, dst, chunk_size: int = 1024 * 1024) -> tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    for chunk in iter(lambda: src.read(chunk_size), b""):
        digest.update(chunk)
        total += len(chunk)
        dst.write(chunk)
    return digest.hexdigest(), total


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

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if size == 0:
            return b""
        if size is None or size < 0:
            chunks = [bytes(self._buffer)]
            self._buffer.clear()
            while self._remaining > 0:
                chunk = self._raw_fp.read(min(self._chunk_size, self._remaining))
                if not chunk:
                    break
                self._remaining -= len(chunk)
                inflated = self._inflater.inflate(chunk)
                if inflated:
                    chunks.append(inflated)
            if not self._flushed:
                tail = self._inflater.inflate(b"")
                self._flushed = True
                if tail:
                    chunks.append(tail)
            return b"".join(chunks)

        while len(self._buffer) < size and self._remaining > 0:
            chunk = self._raw_fp.read(min(self._chunk_size, self._remaining))
            if not chunk:
                break
            self._remaining -= len(chunk)
            inflated = self._inflater.inflate(chunk)
            if inflated:
                self._buffer.extend(inflated)
        if self._remaining == 0 and not self._flushed:
            tail = self._inflater.inflate(b"")
            self._flushed = True
            if tail:
                self._buffer.extend(tail)
        result = bytes(self._buffer[:size])
        del self._buffer[:size]
        return result


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
    path_obj = Path(path)
    if _raw_file_is_gzip(path_obj):
        with open(path_obj, "rb") as raw_fp:
            gzip_cls = igzip.IGzipFile if igzip is not None and _env_bool(PTG2_ISAL_GZIP_ENV, False) else gzip.GzipFile
            with gzip_cls(fileobj=raw_fp, mode="rb") as gzip_fp:
                with io.BufferedReader(gzip_fp, buffer_size=_stream_buffer_bytes()) as buffered_fp:
                    yield buffered_fp
        return
    if zipfile.is_zipfile(path_obj):
        with zipfile.ZipFile(path_obj, "r") as zip_ref:
            member_info = _first_zip_member_info(zip_ref)
            if member_info is None:
                raise RuntimeError(f"No file members found in zip artifact {path_obj}")
            with _open_zip_member_stream(path_obj, member_info, zip_ref) as fp:
                yield fp
        return
    with open(path_obj, "rb") as fp:
        yield fp


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
    raw_path_obj = Path(raw_path)
    compression, member_name = _compression_for_path(raw_path_obj)
    threshold = _env_int(PTG2_DEFER_LOGICAL_HASH_BYTES_ENV, 1024 * 1024 * 1024)
    if allow_deferred and raw_sha256 and raw_byte_count and threshold > 0 and raw_byte_count >= threshold:
        return PTG2LogicalArtifact(
            str(raw_path_obj),
            raw_sha256,
            raw_byte_count,
            compression=compression,
            member_name=member_name,
        )
    digest = hashlib.sha256()
    total = 0
    with open_json_artifact_stream(raw_path_obj) as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            digest.update(chunk)
            total += len(chunk)
    return PTG2LogicalArtifact(str(raw_path_obj), digest.hexdigest(), total, compression=compression, member_name=member_name)


def load_json_artifact(path: str | Path) -> Any:
    with open_json_artifact_stream(path) as fp:
        return json.load(fp)


def stream_logical_artifact(raw_path: str | Path, output_dir: str | Path | None = None) -> PTG2LogicalArtifact:
    raw_path_obj = Path(raw_path)
    output_root = Path(output_dir) if output_dir else raw_path_obj.parent
    output_root.mkdir(parents=True, exist_ok=True)
    if _raw_file_is_gzip(raw_path_obj):
        target = output_root / f"{raw_path_obj.stem}_logical.json"
        with gzip.open(raw_path_obj, "rb") as src, open(target, "wb") as dst:
            digest, total = _stream_copy_with_hash(src, dst)
        return PTG2LogicalArtifact(str(target), digest, total, compression="gzip")
    if zipfile.is_zipfile(raw_path_obj):
        with zipfile.ZipFile(raw_path_obj, "r") as zip_ref:
            member_info = _first_zip_member_info(zip_ref)
            if member_info is None:
                raise RuntimeError(f"No file members found in zip artifact {raw_path_obj}")
            target = output_root / Path(member_info.filename).name
            with _open_zip_member_stream(raw_path_obj, member_info, zip_ref) as src, open(target, "wb") as dst:
                digest, total = _stream_copy_with_hash(src, dst)
            return PTG2LogicalArtifact(
                str(target),
                digest,
                total,
                compression="zip",
                member_name=member_info.filename,
            )
    digest, total = sha256_file(raw_path_obj)
    return PTG2LogicalArtifact(str(raw_path_obj), digest, total)
