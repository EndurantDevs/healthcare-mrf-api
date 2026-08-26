# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure-Python contract around the native CMS v3 hospital MRF parser."""

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
                members = [
                    archive_entry
                    for archive_entry in archive.infolist()
                    if not archive_entry.is_dir()
                ]
                if len(members) != 1:
                    raise ValueError("ZIP hospital MRF must contain exactly one file")
                member = members[0]
                if member.flag_bits & 1:
                    raise ValueError("ZIP hospital MRF member must not be encrypted")
                if member.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
                    raise ValueError("ZIP hospital MRF compression method is unsupported")
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


def detect_hospital_mrf_format(
    path: str | Path, max_decompressed_bytes: int | None = None
) -> str:
    """Detect JSON, tall CSV, or wide CSV without filename assumptions."""

    max_decompressed_bytes = _max_decompressed_bytes(max_decompressed_bytes)
    with _open_payload(Path(path), max_decompressed_bytes) as payload_stream, io.TextIOWrapper(
        payload_stream, encoding="utf-8-sig", errors="strict", newline=""
    ) as text_source:
        text = text_source.read(4096)
    first = text.lstrip()[:1]
    if first == "{":
        return "json"
    if not first:
        raise ValueError("hospital MRF input is empty")

    with _open_payload(Path(path), max_decompressed_bytes) as payload_stream, io.TextIOWrapper(
        payload_stream, encoding="utf-8-sig", errors="strict", newline=""
    ) as text_source:
        reader = csv.reader(text_source)
        try:
            next(reader)
            next(reader)
            headers = next(reader)
        except (StopIteration, csv.Error) as exc:
            raise ValueError("hospital CSV is missing its three header rows") from exc
    normalized_headers = {header.strip().casefold() for header in headers}
    if "payer_name" in normalized_headers:
        return "csv-tall"
    if any(
        len(header.split("|")) in {3, 4}
        and header.strip().casefold().startswith(
            ("standard_charge|", "median_amount|", "10th_percentile|")
        )
        for header in headers
    ):
        return "csv-wide"
    raise ValueError("hospital CSV payer layout is not CMS v3 tall or wide")
