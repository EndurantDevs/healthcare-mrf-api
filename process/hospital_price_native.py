# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure-Python contract around the native CMS hospital MRF parser."""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import os
import re
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, BinaryIO, Iterator
from urllib.parse import unquote, urlsplit

from support.hospital_price_native_validation import (
    HOSPITAL_MRF_BINARY_COPY_KINDS,
    HOSPITAL_MRF_COPY_COLUMNS,
    HOSPITAL_MRF_PACKED_COPY_COLUMNS,
    HOSPITAL_MRF_PARSER_CONTRACT,
    HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
    HOSPITAL_MRF_SCHEMA_REVISION,
    HOSPITAL_MRF_SUMMARY_CONTRACT,
    HOSPITAL_MRF_TEXT_COPY_COLUMNS,
    HospitalPackedRoot,
    HospitalParserArtifact,
    HospitalParserReceipt,
    _SHA256,
    _parser_artifact,
    validate_hospital_parser_summary,
)


_EIN_FILENAME = re.compile(r"^(\d{2})-?(\d{7})(?:[_-]|$)")
_ZIP_MAGICS = (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")
HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES_ENV = (
    "HLTHPRT_HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES"
)
DEFAULT_HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES = 64 * 1024**3
HOSPITAL_MRF_FORMAT_DETECTION_MAX_BYTES = 64 * 1024**2
HOSPITAL_MRF_CSV_HEADER_SCAN_MAX_RECORDS = 16
_CP1252_NBSP_SURROGATE = "\udca0"
_REQUIRED_CSV_METADATA_HEADERS = frozenset(
    {"hospital_name", "last_updated_on", "version"}
)


def hospital_price_version_id(content_sha256: str) -> str:
    """Derive one immutable content-plus-parser projection identity."""

    if _SHA256.fullmatch(content_sha256) is None:
        raise ValueError("hospital content SHA-256 is invalid")
    payload = (
        f"hospital-price-version-v1\0{content_sha256}\0"
        f"{HOSPITAL_MRF_PARSER_CONTRACT_SHA256}"
    )
    return hashlib.sha256(payload.encode("ascii")).hexdigest()


def hospital_ein_from_mrf_url(url: str) -> str | None:
    """Return only a filename-leading public EIN candidate."""

    filename = unquote(Path(urlsplit(url).path).name)
    match = _EIN_FILENAME.match(filename)
    return "".join(match.groups()) if match else None


class _BoundedPayload(io.RawIOBase):
    def __init__(self, source: BinaryIO, max_bytes: int) -> None:
        self.source, self.remaining = source, max_bytes

    def is_readable(self) -> bool:
        """Return true because the wrapper provides bounded reads."""
        return True

    readable = is_readable

    def readinto(self, buffer: Any) -> int:
        """Read into a caller buffer without crossing the configured limit."""
        if not buffer:
            return 0
        if self.remaining == 0:
            if self.source.read(1):
                raise ValueError("hospital MRF format header exceeds its read limit")
            return 0
        chunk = self.source.read(min(len(buffer), self.remaining))
        size = len(chunk)
        buffer[:size] = chunk
        self.remaining -= size
        return size


def _max_decompressed_bytes(value: int | None = None) -> int:
    raw_value: Any = value
    if raw_value is None:
        raw_value = os.getenv(HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES_ENV)
    if raw_value in (None, ""):
        return DEFAULT_HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES
    try:
        limit = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES_ENV} must be a positive integer"
        ) from exc
    if limit < 1:
        raise ValueError(
            f"{HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES_ENV} must be a positive integer"
        )
    return limit


def _is_matching_appledouble_member(sidecar_name: str, payload_name: str) -> bool:
    if payload_name.startswith("__MACOSX/"):
        return False
    payload_parent, _, payload_basename = payload_name.rpartition("/")
    sidecar_path = sidecar_name.removeprefix("__MACOSX/")
    sidecar_parent, _, sidecar_basename = sidecar_path.rpartition("/")
    return (
        sidecar_path != sidecar_name
        and bool(payload_basename)
        and sidecar_parent == payload_parent
        and sidecar_basename == f"._{payload_basename}"
    )


def _select_zip_payload_member(archive: zipfile.ZipFile) -> zipfile.ZipInfo:
    members = [member for member in archive.infolist() if not member.is_dir()]
    if any(member.flag_bits & 1 for member in members):
        raise ValueError("ZIP hospital MRF member must not be encrypted")
    if any(
        member.compress_type
        not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED, zipfile.ZIP_LZMA}
        for member in members
    ):
        raise ValueError("ZIP hospital MRF compression method is unsupported")
    if len(members) == 1:
        member = members[0]
        if member.filename.startswith("__MACOSX/") and member.filename.rsplit(
            "/", 1
        )[-1].startswith("._"):
            raise ValueError("ZIP hospital MRF must contain exactly one file")
        return member
    if len(members) == 2 and _is_matching_appledouble_member(
        members[0].filename, members[1].filename
    ):
        return members[1]
    if len(members) == 2 and _is_matching_appledouble_member(
        members[1].filename, members[0].filename
    ):
        return members[0]
    raise ValueError("ZIP hospital MRF must contain exactly one file")


@contextmanager
def _open_payload(path: Path, max_decompressed_bytes: int) -> Iterator[BinaryIO]:
    read_limit = min(
        max_decompressed_bytes, HOSPITAL_MRF_FORMAT_DETECTION_MAX_BYTES
    )
    with path.open("rb") as raw:
        magic = raw.read(4)
    if magic in _ZIP_MAGICS or path.suffix.casefold() == ".zip":
        try:
            with zipfile.ZipFile(path) as archive:
                member = _select_zip_payload_member(archive)
                if member.file_size > max_decompressed_bytes:
                    raise ValueError(
                        "ZIP hospital MRF decompressed size exceeds its configured limit"
                    )
                with archive.open(member) as member_stream, io.BufferedReader(
                    _BoundedPayload(member_stream, read_limit)
                ) as bounded:
                    yield bounded
        except zipfile.BadZipFile as exc:
            raise ValueError("ZIP hospital MRF input is invalid") from exc
        return
    if magic[:2] == b"\x1f\x8b":
        with gzip.open(path, "rb") as gzip_stream, io.BufferedReader(
            _BoundedPayload(gzip_stream, read_limit)
        ) as bounded:
            yield bounded
    else:
        with path.open("rb") as file_stream, io.BufferedReader(
            _BoundedPayload(file_stream, read_limit)
        ) as bounded:
            yield bounded


def _next_csv_data_headers(structural_rows: Iterator[list[str]]) -> list[str]:
    try:
        for _ in range(HOSPITAL_MRF_CSV_HEADER_SCAN_MAX_RECORDS):
            metadata_headers = next(structural_rows)
            normalized_headers = {
                header.replace(_CP1252_NBSP_SURROGATE, " ")
                .strip()
                .casefold()
                for header in metadata_headers
            }
            if _REQUIRED_CSV_METADATA_HEADERS <= normalized_headers:
                next(structural_rows)
                return next(structural_rows)
    except (StopIteration, csv.Error) as exc:
        raise ValueError("hospital CSV is missing its three header rows") from exc
    raise ValueError("hospital CSV metadata header exceeds its scan limit")


def detect_hospital_mrf_format(
    path: str | Path, max_decompressed_bytes: int | None = None
) -> str:
    """Detect JSON, tall CSV, or wide CSV without filename assumptions."""

    max_decompressed_bytes = _max_decompressed_bytes(max_decompressed_bytes)
    with _open_payload(Path(path), max_decompressed_bytes) as payload_stream, io.TextIOWrapper(
        payload_stream, encoding="utf-8-sig", errors="surrogateescape", newline=""
    ) as text_source:
        first = ""
        while not first:
            text = text_source.read(4096)
            if not text:
                raise ValueError("hospital MRF input is empty")
            first = text.lstrip()[:1]
    if first == "{":
        return "json"

    with _open_payload(Path(path), max_decompressed_bytes) as payload_stream, io.TextIOWrapper(
        payload_stream, encoding="utf-8-sig", errors="surrogateescape", newline=""
    ) as text_source:
        reader = csv.reader(text_source)
        structural_rows = (
            csv_record
            for csv_record in reader
            if any(
                field.replace(_CP1252_NBSP_SURROGATE, " ").strip()
                for field in csv_record
            )
        )
        headers = _next_csv_data_headers(structural_rows)
    normalized_headers = {header.strip().casefold() for header in headers}
    if "payer_name" in normalized_headers:
        return "csv-tall"
    for header in headers:
        parts = [part.strip().casefold() for part in header.split("|")]
        valid_component_counts = {
            "standard_charge": {3, 4, 5},
            "estimated_amount": {3, 4},
            "median_amount": {3, 4},
            "10th_percentile": {3, 4},
            "90th_percentile": {3, 4},
            "count": {3, 4},
            "additional_payer_notes": {3, 4},
        }.get(parts[0])
        if valid_component_counts and len(parts) in valid_component_counts:
            return "csv-wide"
    raise ValueError("hospital CSV payer layout is not CMS tall or wide")
