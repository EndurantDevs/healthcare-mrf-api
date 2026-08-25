# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure-Python contract around the native CMS v3 hospital MRF parser."""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import os
import re
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Iterator
from urllib.parse import unquote, urlsplit


HOSPITAL_MRF_SCHEMA_REVISION = "5333564a710f80d7740180b9ffab8dbdcba9b502"
HOSPITAL_MRF_PARSER_CONTRACT = (
    f"hospital-mrf-copy-v3-resource-bounded:{HOSPITAL_MRF_SCHEMA_REVISION}"
)
HOSPITAL_MRF_PARSER_CONTRACT_SHA256 = hashlib.sha256(
    HOSPITAL_MRF_PARSER_CONTRACT.encode("ascii")
).hexdigest()
HOSPITAL_MRF_COPY_COLUMNS = {
    "mrf": (
        "version_id", "source_hospital_name", "last_updated_on", "template_version",
        "attestation_text", "confirm_attestation", "attester_name",
        "financial_aid_policy",
    ),
    "location": (
        "version_id", "location_ordinal", "location_name", "hospital_address",
    ),
    "npi": ("version_id", "npi_ordinal", "npi"),
    "license": (
        "version_id", "license_ordinal", "license_number", "state",
    ),
    "contract_provision": (
        "version_id", "provision_ordinal", "payer_name", "plan_name",
        "provisions",
    ),
    "service": (
        "version_id", "service_ordinal", "description", "drug_unit", "drug_type",
    ),
    "code": (
        "version_id", "service_ordinal", "code_ordinal", "code_type", "code",
    ),
    "charge": (
        "version_id", "service_ordinal", "charge_ordinal", "setting",
        "modifier_codes", "gross_charge", "discounted_cash", "minimum",
        "maximum", "additional_generic_notes",
        "billing_class",
    ),
    "payer_charge": (
        "version_id", "service_ordinal", "charge_ordinal", "payer_ordinal",
        "payer_name", "plan_name", "standard_charge_dollar",
        "standard_charge_percentage", "standard_charge_algorithm",
        "median_amount", "percentile_10", "percentile_90", "allowed_count",
        "methodology", "additional_payer_notes",
    ),
    "modifier": (
        "version_id", "modifier_ordinal", "code", "description", "setting",
        "additional_generic_notes",
    ),
    "modifier_payer": (
        "version_id", "modifier_ordinal", "payer_ordinal", "payer_name",
        "plan_name", "description", "standard_charge_dollar",
        "standard_charge_percentage", "standard_charge_algorithm",
    ),
}
_REQUIRED_NONEMPTY_RELATIONS = frozenset(
    {"mrf", "location", "npi", "license", "service", "code", "charge"}
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_EIN_FILENAME = re.compile(r"^(\d{2})-?(\d{7})(?:[_-]|$)")
_ZIP_MAGICS = (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")
HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES_ENV = (
    "HLTHPRT_HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES"
)
DEFAULT_HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES = 64 * 1024**3
HOSPITAL_MRF_FORMAT_DETECTION_MAX_BYTES = 64 * 1024**2


@dataclass(frozen=True)
class HospitalParserArtifact:
    kind: str
    path: Path
    rows: int
    bytes: int
    sha256: str


@dataclass(frozen=True)
class HospitalParserReceipt:
    version_id: str
    source_format: str
    semantic_sha256: str
    max_fanout_rows: int
    max_decompressed_bytes: int
    max_output_bytes: int
    artifacts: tuple[HospitalParserArtifact, ...]

    def artifact(self, kind: str) -> HospitalParserArtifact:
        """Return the artifact for one known COPY relation."""

        return self.artifacts[tuple(HOSPITAL_MRF_COPY_COLUMNS).index(kind)]


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


def _parser_artifact(
    raw: Any, *, kind: str, output_directory: Path
) -> HospitalParserArtifact:
    if not isinstance(raw, dict) or raw.get("kind") != kind:
        raise ValueError(f"hospital parser artifact {kind} is missing")
    path = Path(str(raw.get("path") or ""))
    expected_path = (output_directory / f"{kind}.copy").resolve()
    if path.resolve() != expected_path or path.is_symlink() or not path.is_file():
        raise ValueError(f"hospital parser artifact {kind} has an unsafe path")
    rows, byte_count, digest = raw.get("rows"), raw.get("bytes"), raw.get("sha256")
    if (
        type(rows) is not int or rows < 0
        or type(byte_count) is not int or byte_count < 0
        or path.stat().st_size != byte_count
        or not isinstance(digest, str) or _SHA256.fullmatch(digest) is None
        or (kind in _REQUIRED_NONEMPTY_RELATIONS and rows == 0)
        or (rows > 0) != (byte_count > 0)
    ):
        raise ValueError(f"hospital parser artifact {kind} is invalid")
    return HospitalParserArtifact(kind, path, rows, byte_count, digest)


def _is_parser_contract_valid(
    summary: dict[str, Any], version_id: str, source_format: str,
    input_bytes: int, max_decompressed_bytes: int, max_output_bytes: int,
) -> bool:
    return (
        summary["contract"] == "hospital-mrf-copy-v3"
        and summary["version_id"] == version_id
        and summary["schema_version"] == "3.0.0"
        and summary["schema_revision"] == HOSPITAL_MRF_SCHEMA_REVISION
        and summary["format"] == source_format
        and summary["compressed_input_bytes"] == input_bytes
        and type(summary["max_fanout_rows"]) is int
        and summary["max_fanout_rows"] >= 1
        and type(summary["max_decompressed_bytes"]) is int
        and summary["max_decompressed_bytes"] == max_decompressed_bytes
        and type(max_decompressed_bytes) is int
        and max_decompressed_bytes >= 1
        and type(summary["max_output_bytes"]) is int
        and summary["max_output_bytes"] == max_output_bytes
        and type(max_output_bytes) is int
        and max_output_bytes >= 1
        and isinstance(summary["artifacts"], list)
        and len(summary["artifacts"]) == len(HOSPITAL_MRF_COPY_COLUMNS)
    )


def validate_hospital_parser_summary(
    summary_bytes: bytes, *,
    version_id: str,
    source_format: str,
    input_bytes: int,
    output_directory: str | Path,
    max_decompressed_bytes: int,
    max_output_bytes: int,
) -> HospitalParserReceipt:
    """Validate the bounded native receipt and its private COPY paths."""

    if _SHA256.fullmatch(version_id) is None:
        raise ValueError("hospital version identity is invalid")
    if len(summary_bytes) > 1_000_000:
        raise ValueError("hospital parser summary is oversized")
    try:
        summary = json.loads(summary_bytes)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("hospital parser summary is invalid JSON") from exc
    if not isinstance(summary, dict) or {
        "contract", "version_id", "schema_version", "schema_revision", "format",
        "compressed_input_bytes", "max_fanout_rows", "max_decompressed_bytes",
        "max_output_bytes", "artifacts",
    } != set(summary):
        raise ValueError("hospital parser summary shape is invalid")
    if not _is_parser_contract_valid(
        summary, version_id, source_format, input_bytes,
        max_decompressed_bytes, max_output_bytes,
    ):
        raise ValueError("hospital parser summary contract is invalid")
    output = Path(output_directory).resolve()
    artifacts = tuple(
        _parser_artifact(raw, kind=kind, output_directory=output)
        for kind, raw in zip(HOSPITAL_MRF_COPY_COLUMNS, summary["artifacts"])
    )
    if sum(artifact.bytes for artifact in artifacts) > max_output_bytes:
        raise ValueError("hospital parser artifacts exceed their output limit")
    digest = hashlib.sha256()
    for artifact in artifacts:
        digest.update(
            f"{artifact.kind}\0{artifact.rows}\0{artifact.sha256}\n".encode("ascii")
        )
    return HospitalParserReceipt(
        version_id=version_id,
        source_format=source_format.replace("-", "_"),
        semantic_sha256=digest.hexdigest(),
        max_fanout_rows=summary["max_fanout_rows"],
        max_decompressed_bytes=summary["max_decompressed_bytes"],
        max_output_bytes=max_output_bytes,
        artifacts=artifacts,
    )
