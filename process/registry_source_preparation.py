# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare retained CMS ZIP members without allocating or admitting snapshots.

Original distribution bytes remain retained. Preparation verifies container and
input structure; native admission still owns row semantics and resolution.
"""

from __future__ import annotations

import csv
import hashlib
import io
import os
import re
import stat
import struct
import tempfile
import zipfile
import zlib
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path

from process.cms_planfinder_workbook_input import (
    HEADERS,
    MAX_WORKBOOK_BYTES,
    iter_cms_planfinder_issuer_batches,
)
from process.registry_source_admission import _source_header_sha256
from process.registry_source_editions import RegistrySourceEditionCandidate
from process.registry_source_fetch import RegistrySourceFetchReceipt, RegistrySourceFetchResult

MAX_MLR_INPUT_BYTES = 8 * 1024 * 1024
MAX_LOOKUP_BYTES = 64 * 1024
MAX_ARCHIVE_MEMBERS = 64
_MAX_HEADER_BYTES = 64 * 1024
_HEADER_MEMBER = "MR_Submission_Template_Header.csv"
_LOOKUP_MEMBER = "MR_Submission_Sheet_Lookup.csv"
_HIOS_MEMBER = re.compile(r"(20\d{2}-\d{2}-\d{2})-HIOS\.xlsx\Z")
_PARSERS_BY_SOURCE = {"commercial-mlr": "cms-header-v1", "plan-finder": "cms-planfinder-hios-2026-08-06-v1"}


class RegistrySourcePreparationError(ValueError):
    """The retained edition has an invalid or unsupported preparation layout."""

    def __init__(self):
        super().__init__("Registry source preparation is unavailable or invalid")


@dataclass(frozen=True)
class RegistrySourcePreparedInput:
    """Real input fingerprints and original provenance, without admission authority."""

    candidate: RegistrySourceEditionCandidate
    fetch: RegistrySourceFetchResult
    input_path: Path
    input_sha256: str
    header_sha256: str
    input_bytes: int
    member_name: str
    member_date: date | None
    lookup_sha256: str | None


def _require(condition):
    if not condition:
        raise RegistrySourcePreparationError()


def _identity(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _digest(stream, maximum):
    digest = hashlib.sha256()
    size = 0
    while chunk := stream.read(64 * 1024):
        size += len(chunk)
        _require(size <= maximum)
        digest.update(chunk)
    return digest.hexdigest(), size


@contextmanager
def _verified_file(path, expected_digest, expected_size, maximum):
    path = Path(path).absolute()
    _require(path.resolve(strict=True) == path)
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "rb", buffering=0) as stream:
        initial = os.fstat(stream.fileno())
        _require(stat.S_ISREG(initial.st_mode) and 0 < initial.st_size == expected_size <= maximum)
        _require(_digest(stream, maximum) == (expected_digest, expected_size))
        stream.seek(0)
        yield stream
        _require(_identity(initial) == _identity(os.fstat(stream.fileno())) == _identity(path.lstat()))


def _validate_identity(candidate, fetched):
    _require(type(candidate) is RegistrySourceEditionCandidate and type(fetched) is RegistrySourceFetchResult)
    _require(type(fetched.receipt) is RegistrySourceFetchReceipt and type(fetched.unchanged) is bool)
    receipt = replace(fetched.receipt, spec=replace(fetched.receipt.spec))
    spec = receipt.spec
    _require(candidate.source_system == "cms" and candidate.source_id in _PARSERS_BY_SOURCE)
    _require(type(candidate.reporting_year) is int and 2010 <= candidate.reporting_year <= 2100)
    _require(candidate.artifact_sha256 is None and candidate.published_at is None)
    _require(
        (spec.source_system, spec.source_id, spec.edition_id, spec.source_url, spec.reporting_year, spec.published_at)
        == (
            candidate.source_system,
            candidate.source_id,
            candidate.edition_id,
            candidate.source_url,
            candidate.reporting_year,
            candidate.published_at,
        )
    )
    _require(spec.parser_version == _PARSERS_BY_SOURCE[candidate.source_id])
    _require(spec.expected_sha256 in (None, receipt.artifact_sha256))
    _require(Path(fetched.artifact_path).name == receipt.artifact_sha256 + ".artifact")
    if candidate.source_id == "commercial-mlr":
        _require(candidate.quarter is None and candidate.release is None)
        _require(candidate.as_of_date is None or type(candidate.as_of_date) is date)
        expected_edition = str(candidate.reporting_year)
        if candidate.as_of_date is not None:
            expected_edition += "-as-of-" + candidate.as_of_date.isoformat()
    else:
        _require(type(candidate.quarter) is int and 1 <= candidate.quarter <= 4)
        _require(candidate.release in {"final", "preliminary", "unspecified"} and candidate.as_of_date is None)
        expected_edition = f"{candidate.reporting_year}-q{candidate.quarter}-{candidate.release}"
    _require(candidate.edition_id == expected_edition)
    return receipt


def _members(archive):
    members = archive.infolist()
    _require(1 <= len(members) <= MAX_ARCHIVE_MEMBERS)
    _require(len({member.filename for member in members}) == len(members))
    for member in members:
        mode = stat.S_IFMT(member.external_attr >> 16)
        _require(
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 ._()-]{0,254}", member.filename) is not None
            and member.orig_filename == member.filename
            and not member.is_dir()
            and mode in (0, stat.S_IFREG)
            and not member.flag_bits & 1
            and member.compress_type in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)
            and 0 <= member.file_size <= 512 * 1024 * 1024
            and 0 <= member.compress_size <= 512 * 1024 * 1024
        )
    return {member.filename: member for member in members}


def _bounded_directory(stream):
    """Refuse large/ZIP64 central directories before ZipFile materializes them."""
    size = stream.seek(0, os.SEEK_END)
    stream.seek(max(0, size - 65_557))
    tail = stream.read(65_557)
    offset = tail.rfind(b"PK\x05\x06")
    _require(offset >= 0 and len(tail) - offset >= 22)
    end_record_offset = size - len(tail) + offset
    if end_record_offset >= 20:
        stream.seek(end_record_offset - 20)
        _require(stream.read(4) != b"PK\x06\x07")
    _, disk, directory_disk, disk_count, count, directory_size, directory_offset, comment_size = struct.unpack(
        "<4s4H2LH", tail[offset : offset + 22]
    )
    _require(disk == directory_disk == 0 and disk_count == count and 1 <= count <= MAX_ARCHIVE_MEMBERS)
    _require(directory_size <= MAX_ARCHIVE_MEMBERS * 1024 and directory_offset < 2**32 - 1)
    _require(directory_offset + directory_size == size - len(tail) + offset)
    _require(offset + 22 + comment_size == len(tail))
    stream.seek(0)


def _selection(candidate, members_by_name):
    if candidate.source_id == "commercial-mlr":
        _require(_HEADER_MEMBER in members_by_name and _LOOKUP_MEMBER in members_by_name)
        return members_by_name[_HEADER_MEMBER], MAX_MLR_INPUT_BYTES, None
    matches = [member for name, member in members_by_name.items() if _HIOS_MEMBER.fullmatch(name)]
    _require(len(matches) == 1)
    member = matches[0]
    member_date = date.fromisoformat(_HIOS_MEMBER.fullmatch(member.filename)[1])
    # The decoder supports this inspected inner workbook layout only.
    _require(member_date == date(2026, 8, 6))
    return member, MAX_WORKBOOK_BYTES, member_date


def _read_member(archive, member, maximum):
    _require(0 < member.file_size <= maximum)
    with archive.open(member) as stream:
        data = stream.read(maximum + 1)
        _require(len(data) == member.file_size and len(data) <= maximum)
        _require(stream.read(1) == b"")
    return data


def _lookup_digest(archive, members_by_name, year):
    data = _read_member(archive, members_by_name[_LOOKUP_MEMBER], MAX_LOOKUP_BYTES)
    reader = csv.reader(io.StringIO(data.decode("utf-8-sig"), newline=""), strict=True)
    _require(next(reader) == ["reporting_year", "lookup_cat_code", "lookup_cat_name"])
    rows = 0
    for row in reader:
        rows += 1
        _require(rows <= 128 and len(row) == 3 and row[0] == str(year))
    _require(rows > 0)
    return hashlib.sha256(data).hexdigest()


@contextmanager
def _stage_member(archive, member, root, maximum):
    _require(0 < member.file_size <= maximum)
    descriptor, name = tempfile.mkstemp(prefix=".registry-input-", dir=root)
    stage = Path(name)
    try:
        digest = hashlib.sha256()
        size = 0
        with os.fdopen(descriptor, "wb") as output, archive.open(member) as stream:
            while chunk := stream.read(64 * 1024):
                size += len(chunk)
                _require(size <= maximum)
                digest.update(chunk)
                output.write(chunk)
            _require(size == member.file_size)
            output.flush()
            os.fsync(output.fileno())
        yield stage, digest.hexdigest(), size
    finally:
        stage.unlink(missing_ok=True)


def _header_digest(stage, digest, artifact_digest, source_id):
    if source_id == "plan-finder":
        batch_count = 0
        for _ in iter_cms_planfinder_issuer_batches(
            stage, expected_workbook_sha256=digest, artifact_sha256=artifact_digest
        ):
            batch_count += 1
        _require(batch_count > 0)
        headers = HEADERS
    else:
        data = stage.read_bytes()
        headers = next(csv.reader(io.StringIO(data.decode("utf-8-sig"), newline=""), strict=True))
        _require(0 < len(headers) <= 64 and len(set(headers)) == len(headers))
        _require(all(header and "\0" not in header for header in headers))
        _require(sum(len(header.encode()) for header in headers) <= _MAX_HEADER_BYTES)
    return _source_header_sha256(headers)


def _retain(stage, root, digest, size, source_id):
    suffix = ".xlsx" if source_id == "plan-finder" else ".csv"
    final = root / (digest + suffix)
    try:
        os.link(stage, final)
    except FileExistsError:
        with _verified_file(final, digest, size, size):
            return final
    return final


def prepare_registry_source_input(candidate, fetched, input_dir):
    """Verify one retained distribution and retain its exact bounded parser input.

    Only flat MLR Header/Sheet Lookup CSVs or the inspected 2026-08-06 HIOS
    workbook are supported. RBIS is never opened. Reporting year/quarter come
    from the explicit candidate, never ZIP member dates or HTTP validators.
    The caller owns trusted directories and later native admission/review.
    """
    try:
        receipt = _validate_identity(candidate, fetched)
        root = Path(input_dir).absolute()
        root.mkdir(parents=True, exist_ok=True)
        _require(root.resolve(strict=True) == root and root.is_dir())
        with ExitStack() as cleanup:
            with _verified_file(
                fetched.artifact_path, receipt.artifact_sha256, receipt.artifact_bytes, receipt.spec.max_bytes
            ) as stream:
                _bounded_directory(stream)
                with zipfile.ZipFile(stream) as archive:
                    members_by_name = _members(archive)
                    member, maximum, member_date = _selection(candidate, members_by_name)
                    lookup_digest = (
                        _lookup_digest(archive, members_by_name, candidate.reporting_year)
                        if candidate.source_id == "commercial-mlr"
                        else None
                    )
                    stage, digest, size = cleanup.enter_context(_stage_member(archive, member, root, maximum))
                    header_digest = _header_digest(stage, digest, receipt.artifact_sha256, candidate.source_id)
                    # Finish retained-archive identity checks before publishing input.
                    stream.seek(0)
                    _require(
                        _digest(stream, receipt.spec.max_bytes) == (receipt.artifact_sha256, receipt.artifact_bytes)
                    )
            path = _retain(stage, root, digest, size, candidate.source_id)
        return RegistrySourcePreparedInput(
            candidate, fetched, path, digest, header_digest, size, member.filename, member_date, lookup_digest
        )
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        StopIteration,
        RuntimeError,
        EOFError,
        csv.Error,
        zipfile.BadZipFile,
        zlib.error,
    ) as exc:
        if type(exc) is RegistrySourcePreparationError:
            raise
        raise RegistrySourcePreparationError() from None
