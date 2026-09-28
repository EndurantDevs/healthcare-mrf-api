# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Acquire and validate one complete CMS NPD bulk-file release."""

from __future__ import annotations

import datetime as dt
import fcntl
import hashlib
import json
import os
import re
import sqlite3
import uuid
from compression import zstd
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import httpx

SOURCE_ID = "cms-npd"
DOWNLOADS_URL = "https://directory.cms.gov/downloads"
CMS_BULK_HOST = "npd-east-prod-bulk-site.s3.amazonaws.com"
RESOURCE_FILES = (
    ("01-Organization.ndjson", "Organization"),
    ("02-Location.ndjson", "Location"),
    ("03-Endpoint.ndjson", "Endpoint"),
    ("04-HealthcareService.ndjson", "HealthcareService"),
    ("05-InsurancePlan.ndjson", "InsurancePlan"),
    ("06-Practitioner.ndjson", "Practitioner"),
    ("07-PractitionerRole.ndjson", "PractitionerRole"),
    ("08-OrganizationAffiliation.ndjson", "OrganizationAffiliation"),
)
MAX_MANIFEST_BYTES = 64 * 1024
MAX_RESOURCE_LINE_BYTES = 20 * 1024 * 1024
CHUNK_BYTES = 1024 * 1024
_CONTENT_RANGE = re.compile(r"bytes (\d+)-(\d+)/(\d+)")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_FHIR_ID = re.compile(r"[A-Za-z0-9.-]{1,64}")


class CmsNpdSourceError(ValueError):
    """One safe, stable reason why a release cannot be admitted."""


@dataclass(frozen=True)
class ManifestFile:
    name: str
    resource_type: str
    compressed_bytes: int
    original_bytes: int


@dataclass(frozen=True)
class Manifest:
    generated_at: str
    sha256: str
    files: tuple[ManifestFile, ...]
    raw: bytes = field(repr=False)


@dataclass(frozen=True)
class FileProbe:
    etag: str
    compressed_bytes: int


def _is_positive_size(value: Any) -> bool:
    return type(value) is int and 0 < value <= 2**63 - 1


def _is_nonnegative_size(value: Any) -> bool:
    return type(value) is int and 0 <= value <= 2**63 - 1


def parse_manifest(raw: bytes) -> Manifest:
    """Pin the exact eight-file, size-bound CMS manifest."""

    if not raw or len(raw) > MAX_MANIFEST_BYTES:
        raise CmsNpdSourceError("cms_npd_manifest_size_invalid")
    try:
        document = json.loads(raw)
    except (UnicodeError, ValueError) as error:
        raise CmsNpdSourceError("cms_npd_manifest_invalid") from error
    if not isinstance(document, dict) or document.get("compression_algorithm") != "zstd":
        raise CmsNpdSourceError("cms_npd_manifest_invalid")
    generated_at = document.get("generated_at")
    try:
        if not isinstance(generated_at, str) or dt.date.fromisoformat(generated_at).isoformat() != generated_at:
            raise ValueError
    except ValueError as error:
        raise CmsNpdSourceError("cms_npd_manifest_date_invalid") from error
    raw_files = document.get("files")
    if not isinstance(raw_files, dict) or set(raw_files) != {name for name, _ in RESOURCE_FILES}:
        raise CmsNpdSourceError("cms_npd_manifest_files_invalid")
    files = []
    for name, resource_type in RESOURCE_FILES:
        entry = raw_files[name]
        if (
            not isinstance(entry, dict)
            or not _is_positive_size(entry.get("compressed_bytes"))
            or not _is_nonnegative_size(entry.get("original_bytes"))
        ):
            raise CmsNpdSourceError("cms_npd_manifest_file_size_invalid")
        files.append(ManifestFile(name, resource_type, entry["compressed_bytes"], entry["original_bytes"]))
    totals = document.get("totals")
    if not isinstance(totals, dict) or any(
        not _is_nonnegative_size(totals.get(field))
        or totals[field] != sum(getattr(file_spec, field) for file_spec in files)
        for field in ("compressed_bytes", "original_bytes")
    ):
        raise CmsNpdSourceError("cms_npd_manifest_totals_invalid")
    return Manifest(generated_at, hashlib.sha256(raw).hexdigest(), tuple(files), raw)


def _manifest(client: httpx.Client, base_url: str) -> Manifest:
    with _stream(client, base_url, f"{base_url.rstrip('/')}/manifest.json") as response:
        if response.status_code != 200:
            raise CmsNpdSourceError("cms_npd_manifest_unavailable")
        raw = bytearray()
        for chunk in response.iter_bytes(CHUNK_BYTES):
            raw.extend(chunk)
            if len(raw) > MAX_MANIFEST_BYTES:
                raise CmsNpdSourceError("cms_npd_manifest_size_invalid")
    return parse_manifest(bytes(raw))


def _file_url(base_url: str, name: str) -> str:
    return f"{base_url.rstrip('/')}/{name}.zst"


@contextmanager
def _stream(
    client: httpx.Client,
    base_url: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
) -> Iterator[httpx.Response]:
    """Follow only reviewed HTTPS CMS/S3 locations without exposing signed URLs."""

    original_host = httpx.URL(base_url).host
    current = httpx.URL(url)
    with httpx.Client() as clean_client:
        default_headers = clean_client.headers.copy()
    for _ in range(4):
        if (
            client.auth is not None
            or client.headers != default_headers
            or client.cookies
            or client.params
            or client.event_hooks.get("request")
            or (headers and set(map(str.lower, headers)) - {"range", "if-range"})
        ):
            raise CmsNpdSourceError("cms_npd_credentials_not_allowed")
        is_allowed_host = current.host in (original_host, CMS_BULK_HOST)
        if (
            current.scheme != "https"
            or current.port not in (None, 443)
            or current.username
            or current.password
            or current.fragment
            or not is_allowed_host
        ):
            raise CmsNpdSourceError("cms_npd_redirect_invalid")
        with client.stream("GET", current, headers=headers, follow_redirects=False) as response:
            if response.status_code in (301, 302, 303, 307, 308):
                location = response.headers.get("location")
                if not location:
                    raise CmsNpdSourceError("cms_npd_redirect_invalid")
                current = response.url.join(location)
                continue
            yield response
            return
    raise CmsNpdSourceError("cms_npd_redirect_limit")


def _range(response: httpx.Response) -> tuple[int, int, int]:
    matched = _CONTENT_RANGE.fullmatch(response.headers.get("content-range", ""))
    if response.status_code != 206 or matched is None:
        raise CmsNpdSourceError("cms_npd_range_invalid")
    return tuple(int(value) for value in matched.groups())


def _probe(client: httpx.Client, base_url: str, item: ManifestFile) -> FileProbe:
    with _stream(client, base_url, _file_url(base_url, item.name), headers={"Range": "bytes=0-0"}) as response:
        first, last, total = _range(response)
        etag = response.headers.get("etag", "")
        if (
            (first, last, total) != (0, 0, item.compressed_bytes)
            or response.headers.get("content-encoding") not in (None, "identity")
            or not etag.startswith('"')
            or not etag.endswith('"')
        ):
            raise CmsNpdSourceError("cms_npd_file_validator_invalid")
        bytes_read = 0
        for chunk in response.iter_bytes(2):
            bytes_read += len(chunk)
            if bytes_read > 1:
                raise CmsNpdSourceError("cms_npd_file_validator_invalid")
        if bytes_read != 1:
            raise CmsNpdSourceError("cms_npd_file_validator_invalid")
    return FileProbe(etag, total)


def _atomic_bytes(path: Path, raw: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as output:
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    _atomic_bytes(path, json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8"))


@contextmanager
def _release_lock(directory: Path) -> Iterator[None]:
    descriptor = os.open(directory / ".acquire.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise CmsNpdSourceError("cms_npd_acquisition_in_progress") from error
        yield
    finally:
        os.close(descriptor)


def _append_file_range(
    client: httpx.Client,
    base_url: str,
    partial: Path,
    file_spec: ManifestFile,
    probe: FileProbe,
    offset: int,
) -> None:
    with _stream(
        client,
        base_url,
        _file_url(base_url, file_spec.name),
        headers={"Range": f"bytes={offset}-", "If-Range": probe.etag},
    ) as response:
        first, last, total = _range(response)
        if (first, last, total) != (offset, file_spec.compressed_bytes - 1, file_spec.compressed_bytes):
            raise CmsNpdSourceError("cms_npd_download_range_changed")
        if response.headers.get("etag") != probe.etag or response.headers.get("content-encoding") not in (
            None,
            "identity",
        ):
            raise CmsNpdSourceError("cms_npd_download_validator_changed")
        with partial.open("ab") as output:
            for chunk in response.iter_bytes(CHUNK_BYTES):
                output.write(chunk)
                if output.tell() > file_spec.compressed_bytes:
                    raise CmsNpdSourceError("cms_npd_download_size_invalid")
            output.flush()
            os.fsync(output.fileno())


def _download(
    client: httpx.Client,
    base_url: str,
    directory: Path,
    file_spec: ManifestFile,
    probe: FileProbe,
) -> Path:
    destination = directory / f"{file_spec.name}.zst"
    partial = directory / f"{file_spec.name}.part"
    resume = directory / f"{file_spec.name}.resume.json"
    resume_identity_by_field = {"etag": probe.etag, "compressed_bytes": probe.compressed_bytes}
    if destination.exists():
        if destination.stat().st_size != file_spec.compressed_bytes:
            raise CmsNpdSourceError("cms_npd_existing_file_size_invalid")
        return destination
    if partial.exists():
        try:
            previous_identity = json.loads(resume.read_text(encoding="utf-8"))
        except OSError, ValueError:
            previous_identity = None
        if previous_identity != resume_identity_by_field or partial.stat().st_size > file_spec.compressed_bytes:
            partial.unlink()
            resume.unlink(missing_ok=True)
    if not partial.exists():
        _atomic_json(resume, resume_identity_by_field)
    offset = partial.stat().st_size if partial.exists() else 0
    if offset < file_spec.compressed_bytes:
        _append_file_range(client, base_url, partial, file_spec, probe, offset)
    if partial.stat().st_size != file_spec.compressed_bytes:
        raise CmsNpdSourceError("cms_npd_download_incomplete")
    os.replace(partial, destination)
    resume.unlink(missing_ok=True)
    return destination


def _decoded_resource_fingerprints(path: Path, file_spec: ManifestFile) -> Iterator[tuple[str, bytes, int]]:
    """Yield validated FHIR IDs, canonical hashes, and decoded line lengths."""

    with zstd.open(path, "rb") as decoded:
        while line := decoded.readline(MAX_RESOURCE_LINE_BYTES + 1):
            if len(line) > MAX_RESOURCE_LINE_BYTES:
                raise CmsNpdSourceError("cms_npd_decoded_size_invalid")
            try:
                resource = json.loads(line.decode("utf-8"))
            except (UnicodeError, ValueError) as error:
                raise CmsNpdSourceError("cms_npd_ndjson_invalid") from error
            resource_id = resource.get("id") if isinstance(resource, dict) else None
            if (
                not isinstance(resource, dict)
                or resource.get("resourceType") != file_spec.resource_type
                or not isinstance(resource_id, str)
                or _FHIR_ID.fullmatch(resource_id) is None
            ):
                raise CmsNpdSourceError("cms_npd_resource_invalid")
            try:
                canonical_resource = json.dumps(
                    resource, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
                ).encode("utf-8")
            except ValueError as error:
                raise CmsNpdSourceError("cms_npd_ndjson_invalid") from error
            yield resource_id, hashlib.sha256(canonical_resource).digest(), len(line)


def _validate_file(path: Path, file_spec: ManifestFile, directory: Path) -> dict[str, Any]:
    """Read to zstd EOF, validate every line, and detect conflicting IDs on disk."""

    duplicate_db = directory / f".{file_spec.name}.{uuid.uuid4().hex}.sqlite"
    row_count = decoded_bytes = distinct_count = 0
    database = sqlite3.connect(duplicate_db)
    try:
        database.execute("PRAGMA journal_mode=OFF")
        database.execute("CREATE TABLE ids (id TEXT PRIMARY KEY, hash BLOB) WITHOUT ROWID")
        for resource_id, fingerprint, line_bytes in _decoded_resource_fingerprints(path, file_spec):
            decoded_bytes += line_bytes
            if decoded_bytes > file_spec.original_bytes:
                raise CmsNpdSourceError("cms_npd_decoded_size_invalid")
            row_count += 1
            insert_result = database.execute("INSERT OR IGNORE INTO ids VALUES (?, ?)", (resource_id, fingerprint))
            if insert_result.rowcount:
                distinct_count += 1
            elif database.execute("SELECT hash FROM ids WHERE id = ?", (resource_id,)).fetchone()[0] != fingerprint:
                raise CmsNpdSourceError("cms_npd_resource_id_conflict")
            if row_count % 10000 == 0:
                database.commit()
        if decoded_bytes != file_spec.original_bytes:
            raise CmsNpdSourceError("cms_npd_decoded_size_invalid")
    except (EOFError, zstd.ZstdError) as error:
        raise CmsNpdSourceError("cms_npd_zstd_incomplete") from error
    finally:
        database.close()
        duplicate_db.unlink(missing_ok=True)
    return {
        "sha256": _file_sha256(path),
        "compressed_bytes": file_spec.compressed_bytes,
        "original_bytes": decoded_bytes,
        "row_count": row_count,
        "distinct_count": distinct_count,
    }


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def _assert_vector_unchanged(
    client: httpx.Client,
    base_url: str,
    manifest: Manifest,
    probes: dict[str, FileProbe],
) -> None:
    if _manifest(client, base_url).sha256 != manifest.sha256 or any(
        _probe(client, base_url, item) != probes[item.name] for item in manifest.files
    ):
        raise CmsNpdSourceError("cms_npd_source_vector_changed")


def acquire_release(
    root: Path,
    *,
    client: httpx.Client,
    base_url: str = DOWNLOADS_URL,
) -> tuple[Path, dict[str, Any]]:
    """Seal a local release only after all eight pinned files validate."""

    if not root.is_dir():
        raise CmsNpdSourceError("cms_npd_artifact_root_missing")
    manifest = _manifest(client, base_url)
    probes_by_name = {file_spec.name: _probe(client, base_url, file_spec) for file_spec in manifest.files}
    vector_by_field = {
        "manifest_sha256": manifest.sha256,
        "files": {
            name: {"etag": probe.etag, "compressed_bytes": probe.compressed_bytes}
            for name, probe in probes_by_name.items()
        },
    }
    vector_sha256 = hashlib.sha256(json.dumps(vector_by_field, sort_keys=True).encode()).hexdigest()
    directory = root / "cms-npd" / "releases" / vector_sha256
    directory.mkdir(parents=True, exist_ok=True)
    with _release_lock(directory):
        return _acquire_pinned_release(client, base_url, directory, manifest, probes_by_name, vector_sha256)


def _assert_retained_receipt(
    directory: Path,
    receipt_by_field: dict[str, Any],
    manifest: Manifest,
    probes_by_name: dict[str, FileProbe],
    vector_sha256: str,
) -> None:
    """Refuse reuse unless the receipt binds each retained byte stream."""

    if (
        not isinstance(receipt_by_field.get("files"), dict)
        or receipt_by_field.get("source_id") != SOURCE_ID
        or receipt_by_field.get("generated_at") != manifest.generated_at
        or receipt_by_field.get("manifest_sha256") != manifest.sha256
        or receipt_by_field.get("vector_sha256") != vector_sha256
        or set(receipt_by_field["files"]) != {file_spec.name for file_spec in manifest.files}
    ):
        raise CmsNpdSourceError("cms_npd_receipt_invalid")
    for file_spec in manifest.files:
        file_path = directory / f"{file_spec.name}.zst"
        entry = receipt_by_field["files"][file_spec.name]
        if (
            not isinstance(entry, dict)
            or not isinstance(entry.get("sha256"), str)
            or _SHA256.fullmatch(entry["sha256"]) is None
            or entry.get("compressed_bytes") != file_spec.compressed_bytes
            or entry.get("original_bytes") != file_spec.original_bytes
            or not _is_nonnegative_size(entry.get("row_count"))
            or not _is_nonnegative_size(entry.get("distinct_count"))
            or entry["distinct_count"] > entry["row_count"]
            or not file_path.is_file()
            or file_path.stat().st_size != file_spec.compressed_bytes
            or entry.get("etag") != probes_by_name[file_spec.name].etag
            or _file_sha256(file_path) != entry.get("sha256")
        ):
            raise CmsNpdSourceError("cms_npd_retained_file_missing")


def _acquire_pinned_release(
    client: httpx.Client,
    base_url: str,
    directory: Path,
    manifest: Manifest,
    probes_by_name: dict[str, FileProbe],
    vector_sha256: str,
) -> tuple[Path, dict[str, Any]]:
    """Seal or verify the pinned vector while holding its writer lock."""

    manifest_path = directory / "manifest.json"
    if manifest_path.exists():
        if _file_sha256(manifest_path) != manifest.sha256:
            raise CmsNpdSourceError("cms_npd_retained_manifest_invalid")
    else:
        _atomic_bytes(manifest_path, manifest.raw)
    receipt_path = directory / "receipt.json"
    if receipt_path.exists():
        try:
            receipt_by_field = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            raise CmsNpdSourceError("cms_npd_receipt_invalid") from error
        if not isinstance(receipt_by_field, dict):
            raise CmsNpdSourceError("cms_npd_receipt_invalid")
        _assert_retained_receipt(directory, receipt_by_field, manifest, probes_by_name, vector_sha256)
        _assert_vector_unchanged(client, base_url, manifest, probes_by_name)
        return directory, receipt_by_field
    files_by_name: dict[str, Any] = {}
    for file_spec in manifest.files:
        source_path = _download(client, base_url, directory, file_spec, probes_by_name[file_spec.name])
        try:
            validated = _validate_file(source_path, file_spec, directory)
        except CmsNpdSourceError:
            source_path.unlink(missing_ok=True)
            raise
        files_by_name[file_spec.name] = {**validated, "etag": probes_by_name[file_spec.name].etag}
    _assert_vector_unchanged(client, base_url, manifest, probes_by_name)
    receipt_by_field = {
        "source_id": SOURCE_ID,
        "generated_at": manifest.generated_at,
        "manifest_sha256": manifest.sha256,
        "vector_sha256": vector_sha256,
        "files": files_by_name,
    }
    _atomic_json(receipt_path, receipt_by_field)
    return directory, receipt_by_field
