# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical JSON artifact stream helpers for PTG2 imports."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import shutil
import subprocess
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any

try:
    import isal.igzip as igzip
except ImportError:  # pragma: no cover - optional acceleration
    igzip = None

from process.ptg_parts.artifacts import sha256_file
from process.ptg_parts.config import (
    PTG2_DEFER_LOGICAL_HASH_BYTES_ENV,
    PTG2_ISAL_GZIP_ENV,
    _env_bool,
    _env_int,
    _stream_buffer_bytes,
)
from process.ptg_parts.domain import PTG2LogicalArtifact


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


def _first_zip_member(path: str | Path) -> str | None:
    with zipfile.ZipFile(path, "r") as zip_ref:
        for name in zip_ref.namelist():
            if not name.endswith("/"):
                return name
    return None


def _should_fallback_to_unzip(exc: BaseException) -> bool:
    return "compression method is not supported" in str(exc).lower()


@contextmanager
def _open_zip_member_stream(path: str | Path, member_name: str):
    try:
        with zipfile.ZipFile(path, "r") as zip_ref:
            with zip_ref.open(member_name, "r") as fp:
                yield fp
        return
    except NotImplementedError as exc:
        if not _should_fallback_to_unzip(exc):
            raise

    unzip_bin = shutil.which("unzip")
    if not unzip_bin:
        raise RuntimeError(
            f"Zip member {member_name!r} in {path} uses a compression method "
            "Python cannot read and system unzip is not installed"
        )
    process = subprocess.Popen(
        [unzip_bin, "-p", str(path), member_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdout is not None
    try:
        yield process.stdout
    finally:
        if process.stdout is not None:
            process.stdout.close()
        stderr = process.stderr.read().decode("utf-8", errors="replace") if process.stderr is not None else ""
        return_code = process.wait()
        if return_code != 0:
            raise RuntimeError(
                f"unzip failed for member {member_name!r} in {path}: {stderr.strip()}"
            )


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
        member_name = _first_zip_member(path_obj)
        if not member_name:
            raise RuntimeError(f"No file members found in zip artifact {path_obj}")
        with _open_zip_member_stream(path_obj, member_name) as fp:
            yield fp
        return
    with open(path_obj, "rb") as fp:
        yield fp


def _compression_for_path(raw_path: str | Path) -> tuple[str | None, str | None]:
    raw_path_obj = Path(raw_path)
    compression = "gzip" if _raw_file_is_gzip(raw_path_obj) else None
    member_name = None
    if compression is None and zipfile.is_zipfile(raw_path_obj):
        compression = "zip"
        member_name = _first_zip_member(raw_path_obj)
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
            for name in zip_ref.namelist():
                if name.endswith("/"):
                    continue
                target = output_root / Path(name).name
                with _open_zip_member_stream(raw_path_obj, name) as src, open(target, "wb") as dst:
                    digest, total = _stream_copy_with_hash(src, dst)
                return PTG2LogicalArtifact(str(target), digest, total, compression="zip", member_name=name)
        raise RuntimeError(f"No file members found in zip artifact {raw_path_obj}")
    digest, total = sha256_file(raw_path_obj)
    return PTG2LogicalArtifact(str(raw_path_obj), digest, total)
