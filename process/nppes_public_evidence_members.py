# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Safe streaming and bounded legacy materialization for NPPES ZIP members."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import csv
from dataclasses import dataclass
import hashlib
import io
import os
from pathlib import Path
import stat
import zipfile

from aiofiles import open as async_open

from public_evidence.nppes_registry_primitives import validate_nppes_header
from process.nppes_public_evidence_archive import (
    NppesZipLayout,
    PreparedNppesArchive,
    _LEGACY_PATTERNS,
    _opened_prepared_nppes_archive,
    _zip_member,
    archive_error,
)


_LEGACY_LAYOUT_SEAL = object()
_LEGACY_KINDS = (
    "primary",
    "practice_location",
    "other_name",
    "endpoint",
)


@dataclass(frozen=True, slots=True, repr=False, init=False)
class NppesLegacyLayout:
    """Four sealed materialized files consumed by the legacy importer."""

    primary_path: Path
    practice_location_path: Path
    other_name_path: Path
    endpoint_path: Path
    _member_seals: tuple[
        tuple[str, Path, tuple[int, int, int, int, int, int, int], str],
        ...,
    ]
    _seal: object

    def __repr__(self) -> str:
        return "<nppes-legacy-layout>"


def _matching_layout(
    archive: zipfile.ZipFile,
    layout: NppesZipLayout,
) -> dict[str, zipfile.ZipInfo]:
    infos = archive.infolist()
    members = tuple(_zip_member(info, ordinal) for ordinal, info in enumerate(infos))
    if members != layout.members:
        raise archive_error()
    info_by_name = {info.filename: info for info in infos}
    if set(info_by_name) != {member.name for member in layout.members}:
        raise archive_error()
    return info_by_name


def _private_destination(destination: object) -> Path:
    if not isinstance(destination, Path) or destination.is_symlink():
        raise archive_error()
    resolved = destination.resolve()
    destination_stat = resolved.stat()
    if (
        not stat.S_ISDIR(destination_stat.st_mode)
        or stat.S_IMODE(destination_stat.st_mode) & 0o077
        or destination_stat.st_uid != os.geteuid()
    ):
        raise archive_error()
    return resolved


def _copy_zip_member(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    destination: Path,
) -> str:
    descriptor = os.open(
        destination,
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    total = 0
    digest = hashlib.sha256()
    try:
        with archive.open(info, "r") as member_stream, os.fdopen(
            descriptor, "wb", closefd=True
        ) as output:
            for chunk in iter(lambda: member_stream.read(1024 * 1024), b""):
                total += len(chunk)
                if total > info.file_size:
                    raise archive_error()
                digest.update(chunk)
                output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
        if total != info.file_size:
            raise archive_error()
    except BaseException:
        destination.unlink(missing_ok=True)
        raise
    return digest.hexdigest()


def _member_identity(file_stat: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    if not stat.S_ISREG(file_stat.st_mode):
        raise archive_error()
    return (
        file_stat.st_dev,
        file_stat.st_ino,
        file_stat.st_size,
        file_stat.st_mtime_ns,
        file_stat.st_ctime_ns,
        file_stat.st_uid,
        stat.S_IMODE(file_stat.st_mode),
    )


def _opened_sha256(descriptor: int) -> str:
    digest = hashlib.sha256()
    offset = 0
    while True:
        chunk = os.pread(descriptor, 1024 * 1024, offset)
        if not chunk:
            return digest.hexdigest()
        digest.update(chunk)
        offset += len(chunk)


def _build_legacy_layout(
    paths_by_kind: dict[str, Path],
    digests_by_kind: dict[str, str],
) -> NppesLegacyLayout:
    if set(paths_by_kind) != set(_LEGACY_KINDS) or set(digests_by_kind) != set(
        _LEGACY_KINDS
    ):
        raise archive_error()
    seals = tuple(
        (
            kind,
            paths_by_kind[kind],
            _member_identity(paths_by_kind[kind].stat()),
            digests_by_kind[kind],
        )
        for kind in _LEGACY_KINDS
    )
    materialized = object.__new__(NppesLegacyLayout)
    for kind in _LEGACY_KINDS:
        object.__setattr__(materialized, f"{kind}_path", paths_by_kind[kind])
    object.__setattr__(materialized, "_member_seals", seals)
    object.__setattr__(materialized, "_seal", _LEGACY_LAYOUT_SEAL)
    return materialized


def _validated_member_seal(
    layout: object,
    kind: object,
) -> tuple[Path, tuple[int, int, int, int, int, int, int], str]:
    if (
        type(layout) is not NppesLegacyLayout
        or layout._seal is not _LEGACY_LAYOUT_SEAL
        or type(kind) is not str
        or kind not in _LEGACY_KINDS
        or type(layout._member_seals) is not tuple
        or len(layout._member_seals) != len(_LEGACY_KINDS)
    ):
        raise archive_error()
    seal_values_by_kind = {
        member_seal[0]: member_seal[1:]
        for member_seal in layout._member_seals
    }
    if set(seal_values_by_kind) != set(_LEGACY_KINDS):
        raise archive_error()
    path, identity, digest = seal_values_by_kind[kind]
    if (
        not isinstance(path, Path)
        or getattr(layout, f"{kind}_path") != path
        or type(identity) is not tuple
        or len(identity) != 7
        or type(digest) is not str
        or len(digest) != 64
    ):
        raise archive_error()
    return path, identity, digest


def _validate_open_member(
    descriptor: int,
    identity: tuple[int, int, int, int, int, int, int],
    digest: str,
) -> None:
    if (
        _member_identity(os.fstat(descriptor)) != identity
        or _opened_sha256(descriptor) != digest
    ):
        raise archive_error()


@asynccontextmanager
async def open_verified_nppes_legacy_text(
    layout: object,
    kind: object,
):
    """Open one sealed materialized CSV and verify it before and after use."""

    descriptor = None
    try:
        path, identity, digest = _validated_member_seal(layout, kind)
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        await asyncio.to_thread(_validate_open_member, descriptor, identity, digest)
    except asyncio.CancelledError:
        if descriptor is not None:
            os.close(descriptor)
        raise
    except Exception:
        if descriptor is not None:
            os.close(descriptor)
        raise archive_error() from None
    try:
        async with async_open(
            descriptor,
            "r",
            encoding="utf-8-sig",
            errors="strict",
            newline="",
            closefd=False,
        ) as member_stream:
            yield member_stream
    except BaseException:
        raise
    else:
        try:
            await asyncio.to_thread(
                _validate_open_member,
                descriptor,
                identity,
                digest,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise archive_error() from None
    finally:
        os.close(descriptor)


def materialize_nppes_legacy_members(
    prepared: object,
    destination: object,
) -> NppesLegacyLayout:
    """Copy only the four validated importer inputs without extracting the ZIP."""

    created_paths: list[Path] = []
    try:
        destination_path = _private_destination(destination)
        with _opened_prepared_nppes_archive(prepared) as (
            fixed_prepared,
            archive_stream,
        ):
            layout = fixed_prepared.layout
            legacy_name_by_kind = dict(layout.legacy_member_names)
            if set(legacy_name_by_kind) != set(_LEGACY_PATTERNS):
                raise archive_error()
            with zipfile.ZipFile(archive_stream, "r") as archive:
                info_by_name = _matching_layout(archive, layout)
                paths_by_kind: dict[str, Path] = {}
                digests_by_kind: dict[str, str] = {}
                for kind in _LEGACY_KINDS:
                    name = legacy_name_by_kind[kind]
                    final_path = destination_path / name
                    digests_by_kind[kind] = _copy_zip_member(
                        archive,
                        info_by_name[name],
                        final_path,
                    )
                    created_paths.append(final_path)
                    paths_by_kind[kind] = final_path
        materialized = _build_legacy_layout(paths_by_kind, digests_by_kind)
    except Exception:
        for created_path in created_paths:
            created_path.unlink(missing_ok=True)
        normalized_error = archive_error()
    else:
        return materialized
    raise normalized_error


class NppesPrimaryCsvRows:
    """A single-use strict CSV iterator that must be exhausted on success."""

    def __init__(self, prepared: object) -> None:
        self._prepared = prepared
        self._archive = None
        self._archive_context = None
        self._archive_stream = None
        self._raw = None
        self._text = None
        self._reader = None
        self._finished = False
        self.header: tuple[str, ...] | None = None

    def __enter__(self) -> "NppesPrimaryCsvRows":
        try:
            if (
                type(self._prepared) is not PreparedNppesArchive
            ):
                raise archive_error()
            self._archive_context = _opened_prepared_nppes_archive(self._prepared)
            self._prepared, self._archive_stream = self._archive_context.__enter__()
            layout = self._prepared.layout
            self._archive = zipfile.ZipFile(self._archive_stream, "r")
            info_by_name = _matching_layout(self._archive, layout)
            primary_name = layout.primary_member_name
            self._raw = self._archive.open(info_by_name[primary_name], "r")
            self._text = io.TextIOWrapper(
                self._raw,
                encoding="utf-8-sig",
                errors="strict",
                newline="",
            )
            self._reader = csv.reader(self._text, strict=True)
            self.header = validate_nppes_header(tuple(next(self._reader)))
        except Exception:
            self._close()
            normalized_error = archive_error()
        else:
            return self
        raise normalized_error

    def __iter__(self) -> "NppesPrimaryCsvRows":
        return self

    def __next__(self) -> tuple[str, ...]:
        if self._reader is None or self._finished or self.header is None:
            raise StopIteration
        try:
            csv_fields = tuple(next(self._reader))
            if len(csv_fields) != len(self.header):
                raise archive_error()
        except StopIteration:
            self._finished = True
            raise
        except Exception:
            normalized_error = archive_error()
        else:
            return csv_fields
        raise normalized_error

    def _close(self) -> None:
        for stream_name in ("_text", "_raw", "_archive"):
            stream = getattr(self, stream_name)
            if stream is not None:
                try:
                    stream.close()
                except Exception:
                    stream = None
                setattr(self, stream_name, None)
        if self._archive_context is not None:
            archive_context = self._archive_context
            self._archive_context = None
            archive_context.__exit__(None, None, None)
        self._archive_stream = None

    def __exit__(self, exception_type, _exception, _traceback) -> bool:
        is_incomplete = exception_type is None and not self._finished
        self._close()
        if is_incomplete:
            raise archive_error()
        return False


__all__ = (
    "NppesLegacyLayout",
    "NppesPrimaryCsvRows",
    "materialize_nppes_legacy_members",
    "open_verified_nppes_legacy_text",
)
