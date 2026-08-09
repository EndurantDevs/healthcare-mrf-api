# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact listing and ZIP boundaries for retained NPPES evidence archives."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import stat
import zipfile

from process.nppes_public_evidence_archive_contract import (
    NppesArchiveCandidate,
    NppesPublicEvidenceArchiveError,
    archive_error,
)
from process.nppes_public_evidence_listing import (
    NPPES_LISTING_URL,
    _candidate_from_url,
    parse_official_nppes_listing,
    select_nppes_release_chain,
    validate_nppes_archive_candidate,
)
_PRIMARY_RE = re.compile(
    r"npidata_pfile_([0-9]{8})-([0-9]{8})\.csv",
    flags=re.ASCII,
)
_LEGACY_PATTERNS = {
    "primary": _PRIMARY_RE,
    "practice_location": re.compile(
        r"pl_pfile_([0-9]{8})-([0-9]{8})\.csv",
        flags=re.ASCII,
    ),
    "other_name": re.compile(
        r"othername_pfile_([0-9]{8})-([0-9]{8})\.csv",
        flags=re.ASCII,
    ),
    "endpoint": re.compile(
        r"endpoint_pfile_([0-9]{8})-([0-9]{8})\.csv",
        flags=re.ASCII,
    ),
}
_MAX_LISTING_BYTES = 4 * 1024 * 1024
_MAX_MEMBER_COUNT = 4096
_MAX_MEMBER_NAME_BYTES = 1024
_MAX_MEMBER_BYTES = 64 * 1024**3
_MAX_TOTAL_MEMBER_BYTES = 128 * 1024**3
_SUPPORTED_COMPRESSION = {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}
_SHA256_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)


@dataclass(frozen=True, slots=True, repr=False)
class RetainedNppesArchive:
    """One fully hashed content-addressed archive and HTTP observation."""

    candidate: NppesArchiveCandidate
    path: Path
    artifact_sha256: str
    artifact_byte_count: int
    listing_sha256: str
    etag: str | None
    last_modified: str | None
    acquired_at: str

    def __repr__(self) -> str:
        return "<retained-nppes-archive>"


@dataclass(frozen=True, slots=True, repr=False)
class NppesZipMember:
    """Bounded central-directory identity for one regular member."""

    ordinal: int
    name: str
    crc32: int
    compressed_size: int
    uncompressed_size: int

    def __repr__(self) -> str:
        return "<nppes-zip-member>"


@dataclass(frozen=True, slots=True, repr=False)
class NppesZipLayout:
    """Validated member census and exact legacy member mapping."""

    members: tuple[NppesZipMember, ...]
    primary_member_name: str
    primary_snapshot_date: date
    legacy_member_names: tuple[tuple[str, str], ...]

    def __repr__(self) -> str:
        return "<nppes-zip-layout>"


_PREPARED_ARCHIVE_SEAL = object()


@dataclass(frozen=True, slots=True, repr=False, init=False)
class PreparedNppesArchive:
    """One fully verified archive whose retained inode must stay unchanged."""

    retained: RetainedNppesArchive
    layout: NppesZipLayout
    _file_identity: tuple[int, int, int, int, int, int, int]
    _seal: object

    @property
    def archive_name(self) -> str:
        """Return the canonical retained archive basename."""

        return self.retained.candidate.archive_name

    def __repr__(self) -> str:
        return "<prepared-nppes-archive>"


def _safe_http_observation(value: object) -> str | None:
    if value is None:
        return None
    if (
        type(value) is not str
        or len(value.encode("utf-8")) > 4096
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise archive_error()
    return value


def _validated_retained_metadata(retained: object) -> RetainedNppesArchive:
    if type(retained) is not RetainedNppesArchive:
        raise archive_error()
    candidate = validate_nppes_archive_candidate(retained.candidate)
    if (
        not isinstance(retained.path, Path)
        or retained.path.is_symlink()
        or type(retained.artifact_sha256) is not str
        or _SHA256_RE.fullmatch(retained.artifact_sha256) is None
        or type(retained.listing_sha256) is not str
        or _SHA256_RE.fullmatch(retained.listing_sha256) is None
        or type(retained.artifact_byte_count) is not int
        or retained.artifact_byte_count <= 0
        or type(retained.acquired_at) is not str
    ):
        raise archive_error()
    datetime.strptime(retained.acquired_at, "%Y-%m-%dT%H:%M:%SZ")
    return RetainedNppesArchive(
        candidate=candidate,
        path=retained.path,
        artifact_sha256=retained.artifact_sha256,
        artifact_byte_count=retained.artifact_byte_count,
        listing_sha256=retained.listing_sha256,
        etag=_safe_http_observation(retained.etag),
        last_modified=_safe_http_observation(retained.last_modified),
        acquired_at=retained.acquired_at,
    )


def _file_identity_from_stat(
    file_stat: os.stat_result,
) -> tuple[int, int, int, int, int, int, int]:
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


def _verified_stream_digest(archive_stream: object) -> tuple[str, int]:
    archive_stream.seek(0)
    digest = hashlib.sha256()
    byte_count = 0
    for chunk in iter(lambda: archive_stream.read(1024 * 1024), b""):
        byte_count += len(chunk)
        digest.update(chunk)
    archive_stream.seek(0)
    return digest.hexdigest(), byte_count


@contextmanager
def _opened_verified_retained(retained: object):
    try:
        fixed_retained = _validated_retained_metadata(retained)
        descriptor = os.open(
            fixed_retained.path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
    except Exception:
        raise archive_error() from None
    with os.fdopen(descriptor, "rb", closefd=True) as archive_stream:
        try:
            initial_identity = _file_identity_from_stat(
                os.fstat(archive_stream.fileno())
            )
            actual_digest, actual_bytes = _verified_stream_digest(archive_stream)
            if (
                actual_digest != fixed_retained.artifact_sha256
                or actual_bytes != fixed_retained.artifact_byte_count
            ):
                raise archive_error()
        except Exception:
            raise archive_error() from None
        is_body_failed = True
        try:
            yield fixed_retained, archive_stream, initial_identity
            is_body_failed = False
        finally:
            if not is_body_failed:
                try:
                    final_identity = _file_identity_from_stat(
                        os.fstat(archive_stream.fileno())
                    )
                    path_identity = _retained_file_identity(fixed_retained.path)
                except Exception:
                    final_error = archive_error()
                else:
                    final_error = None
                if (
                    final_error is None
                    and (
                        final_identity != initial_identity
                        or path_identity != initial_identity
                    )
                ):
                    final_error = archive_error()
                if final_error is not None:
                    raise final_error


def verify_retained_nppes_archive(retained: object) -> RetainedNppesArchive:
    """Rehash one held regular file and rebuild its bounded metadata."""

    try:
        with _opened_verified_retained(retained) as (fixed_retained, _, _):
            rebuilt = fixed_retained
    except Exception:
        normalized_error = archive_error()
    else:
        return rebuilt
    raise normalized_error


def _safe_member_name(name: object) -> str:
    if type(name) is not str or not name or "\x00" in name or "\\" in name:
        raise archive_error()
    try:
        encoded = name.encode("utf-8")
    except UnicodeError:
        raise archive_error() from None
    path = PurePosixPath(name)
    if (
        len(encoded) > _MAX_MEMBER_NAME_BYTES
        or path.is_absolute()
        or len(path.parts) != 1
        or path.parts[0] in {".", ".."}
    ):
        raise archive_error()
    return name


def _member_kind(name: str) -> str | None:
    matches = [kind for kind, pattern in _LEGACY_PATTERNS.items() if pattern.fullmatch(name)]
    if len(matches) > 1:
        raise archive_error()
    return matches[0] if matches else None


def _legacy_member_layout(
    names: list[str],
) -> tuple[dict[str, str], date, date]:
    legacy_name_by_kind: dict[str, str] = {}
    for name in names:
        member_kind = _member_kind(name)
        if member_kind is None:
            continue
        if member_kind in legacy_name_by_kind:
            raise archive_error()
        legacy_name_by_kind[member_kind] = name
    if "primary" not in legacy_name_by_kind:
        raise archive_error()
    primary_match = _PRIMARY_RE.fullmatch(legacy_name_by_kind["primary"])
    if primary_match is None:
        raise archive_error()
    primary_start = datetime.strptime(primary_match.group(1), "%Y%m%d").date()
    primary_end = datetime.strptime(primary_match.group(2), "%Y%m%d").date()
    if primary_end < primary_start:
        raise archive_error()
    for kind, name in legacy_name_by_kind.items():
        member_match = _LEGACY_PATTERNS[kind].fullmatch(name)
        if member_match is None:
            raise archive_error()
        member_start = datetime.strptime(member_match.group(1), "%Y%m%d").date()
        member_end = datetime.strptime(member_match.group(2), "%Y%m%d").date()
        if member_start != primary_start or member_end != primary_end:
            raise archive_error()
    return legacy_name_by_kind, primary_start, primary_end


def _zip_member(info: zipfile.ZipInfo, ordinal: int) -> NppesZipMember:
    name = _safe_member_name(info.filename)
    unix_mode = (info.external_attr >> 16) & 0xFFFF
    file_type = stat.S_IFMT(unix_mode)
    if (
        info.is_dir()
        or info.flag_bits & 0x1
        or info.compress_type not in _SUPPORTED_COMPRESSION
        or file_type not in {0, stat.S_IFREG}
        or not 0 <= info.file_size <= _MAX_MEMBER_BYTES
        or not 0 <= info.compress_size <= _MAX_MEMBER_BYTES
    ):
        raise archive_error()
    return NppesZipMember(
        ordinal=ordinal,
        name=name,
        crc32=info.CRC,
        compressed_size=info.compress_size,
        uncompressed_size=info.file_size,
    )


def _inspect_verified_nppes_archive(
    fixed_retained: RetainedNppesArchive,
    archive_stream: object,
) -> NppesZipLayout:
    try:
        with zipfile.ZipFile(archive_stream, "r") as archive:
            infos = archive.infolist()
            if not 1 <= len(infos) <= _MAX_MEMBER_COUNT:
                raise archive_error()
            members = tuple(
                _zip_member(info, ordinal)
                for ordinal, info in enumerate(infos)
            )
            if (
                sum(zip_member.uncompressed_size for zip_member in members)
                > _MAX_TOTAL_MEMBER_BYTES
            ):
                raise archive_error()
            names = [zip_member.name for zip_member in members]
            if len(set(names)) != len(names) or len({name.casefold() for name in names}) != len(names):
                raise archive_error()
            legacy_by_kind, _, primary_end = _legacy_member_layout(names)
            candidate = fixed_retained.candidate
            if candidate.archive_kind == "monthly":
                if (
                    primary_end.year != candidate.period_start.year
                    or primary_end.month != candidate.period_start.month
                ):
                    raise archive_error()
            elif candidate.period_end != primary_end:
                raise archive_error()
            if archive.testzip() is not None:
                raise archive_error()
        layout = NppesZipLayout(
            members=members,
            primary_member_name=legacy_by_kind["primary"],
            primary_snapshot_date=primary_end,
            legacy_member_names=tuple(sorted(legacy_by_kind.items())),
        )
    except Exception:
        normalized_error = archive_error()
    else:
        return layout
    raise normalized_error


def inspect_nppes_archive(retained: object) -> NppesZipLayout:
    """Rehash one archive, then validate its directory and every member CRC."""

    try:
        with _opened_verified_retained(retained) as (
            fixed_retained,
            archive_stream,
            _,
        ):
            layout = _inspect_verified_nppes_archive(
                fixed_retained,
                archive_stream,
            )
    except Exception:
        normalized_error = archive_error()
    else:
        return layout
    raise normalized_error


def _retained_file_identity(
    path: Path,
) -> tuple[int, int, int, int, int, int, int]:
    if path.is_symlink():
        raise archive_error()
    return _file_identity_from_stat(path.stat())


def prepare_nppes_archive(retained: object) -> PreparedNppesArchive:
    """Hash and inspect one archive exactly once, then seal its inode identity."""

    try:
        with _opened_verified_retained(retained) as (
            fixed_retained,
            archive_stream,
            file_identity,
        ):
            layout = _inspect_verified_nppes_archive(
                fixed_retained,
                archive_stream,
            )
            prepared = object.__new__(PreparedNppesArchive)
            object.__setattr__(prepared, "retained", fixed_retained)
            object.__setattr__(prepared, "layout", layout)
            object.__setattr__(prepared, "_file_identity", file_identity)
            object.__setattr__(prepared, "_seal", _PREPARED_ARCHIVE_SEAL)
    except Exception:
        normalized_error = archive_error()
    else:
        return prepared
    raise normalized_error


def _validated_prepared_shape(candidate: object) -> PreparedNppesArchive:
    if (
        type(candidate) is not PreparedNppesArchive
        or candidate._seal is not _PREPARED_ARCHIVE_SEAL
        or type(candidate.retained) is not RetainedNppesArchive
        or type(candidate.layout) is not NppesZipLayout
        or type(candidate._file_identity) is not tuple
        or len(candidate._file_identity) != 7
        or any(type(value) is not int for value in candidate._file_identity)
        or _retained_file_identity(candidate.retained.path)
        != candidate._file_identity
    ):
        raise archive_error()
    return candidate


@contextmanager
def _opened_prepared_nppes_archive(candidate: object):
    prepared = _validated_prepared_shape(candidate)
    with _opened_verified_retained(prepared.retained) as (
        fixed_retained,
        archive_stream,
        file_identity,
    ):
        if fixed_retained != prepared.retained or file_identity != prepared._file_identity:
            raise archive_error()
        yield prepared, archive_stream


def validate_prepared_nppes_archive(candidate: object) -> PreparedNppesArchive:
    """Rehash one sealed archive while holding its unchanged retained inode."""

    try:
        with _opened_prepared_nppes_archive(candidate) as (prepared, _):
            rebuilt = prepared
    except Exception:
        normalized_error = archive_error()
    else:
        return rebuilt
    raise normalized_error


__all__ = (
    "NPPES_LISTING_URL",
    "NppesArchiveCandidate",
    "NppesPublicEvidenceArchiveError",
    "NppesZipLayout",
    "PreparedNppesArchive",
    "RetainedNppesArchive",
    "archive_error",
    "inspect_nppes_archive",
    "parse_official_nppes_listing",
    "prepare_nppes_archive",
    "select_nppes_release_chain",
    "validate_nppes_archive_candidate",
    "validate_prepared_nppes_archive",
    "verify_retained_nppes_archive",
)
