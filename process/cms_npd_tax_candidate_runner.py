# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stream admitted CMS Organizations into a pinned, private tax candidate report."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import tempfile
from collections import Counter
from collections.abc import Callable, Iterator
from compression import zstd
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncContextManager, BinaryIO

from sqlalchemy import text

from process.cms_npd_source import MAX_MANIFEST_BYTES, MAX_RESOURCE_LINE_BYTES, RESOURCE_FILES, parse_manifest
from process.cms_npd_tax_candidate_lookup import lookup_pinned_tax_candidates
from process.cms_npd_tax_candidate_report import CmsTaxCandidateOrganization, build_cms_tax_candidate_report
from process.provider_directory_identifier_policy import CMS_NPD_PSEUDO_EIN_SYSTEM
from process.tin_npi_connector_extract import _identifier_cutoff, _select_effective_identifiers
from process.tin_npi_connector_policy import FhirTinNpiIdentifierPolicy, FhirTinNpiIdentifierRule
from process.tin_npi_connector_support import FhirOrganizationEvidenceState

_ORGANIZATION_FILE = RESOURCE_FILES[0][0]
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_FHIR_ID = re.compile(r"[A-Za-z0-9.-]{1,64}\Z")
_NPI_SYSTEMS = (
    "http://hl7.org/fhir/sid/us-npi",
    "http://terminology.hl7.org/NamingSystem/npi",
)
_MAX_NPIS_PER_LOOKUP = 256

# The EIN selector is deliberately unreachable: this policy class requires one,
# while this candidate lane never selects or projects a tax identifier.
CMS_NPD_NPI_ONLY_POLICY = FhirTinNpiIdentifierPolicy(
    policy_id="cms-npd-npi-only-candidates-v1",
    rules=(
        FhirTinNpiIdentifierRule(
            rule_id="cms-npd-exact-npi-v1",
            source_id="cms-npd",
            endpoint_id="cms-npd-bulk",
            npi_systems=_NPI_SYSTEMS,
            npi_type_codings=(),
            ein_systems=("urn:cms-npd:candidate-lane-never-ein",),
            ein_type_codings=(),
        ),
    ),
)


@dataclass(frozen=True)
class CmsTaxCandidateReportResult:
    sha256: str
    candidate_organization_count: int


def _canonical_raw_hash(resource: dict[str, Any]) -> bytes:
    """Match the CMS acquisition row hash, including Unicode and separators."""

    encoded = json.dumps(resource, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).digest()


def _release_witness(directory: Path) -> tuple[Path, dict[str, Any], str]:
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("CMS tax candidate release is unavailable")
    manifest_path = directory / "manifest.json"
    receipt_path = directory / "receipt.json"
    if manifest_path.is_symlink() or receipt_path.is_symlink():
        raise ValueError("CMS tax candidate release witness is invalid")
    with manifest_path.open("rb") as manifest_stream:
        manifest = parse_manifest(manifest_stream.read(MAX_MANIFEST_BYTES + 1))
    if receipt_path.stat().st_size > 64 * 1024:
        raise ValueError("CMS tax candidate release witness is invalid")
    receipt = json.loads(receipt_path.read_bytes())
    files = receipt.get("files") if isinstance(receipt, dict) else None
    if (
        not isinstance(files, dict)
        or receipt.get("source_id") != "cms-npd"
        or receipt.get("generated_at") != manifest.generated_at
        or receipt.get("manifest_sha256") != manifest.sha256
        or set(files) != {name for name, _ in RESOURCE_FILES}
        or type(receipt.get("vector_sha256")) is not str
        or _SHA256.fullmatch(receipt["vector_sha256"]) is None
        or directory.name != receipt["vector_sha256"]
    ):
        raise ValueError("CMS tax candidate release witness is invalid")
    source_vector_by_field = {
        "manifest_sha256": manifest.sha256,
        "files": {
            file_spec.name: {
                "etag": files[file_spec.name].get("etag") if isinstance(files[file_spec.name], dict) else None,
                "compressed_bytes": file_spec.compressed_bytes,
            }
            for file_spec in manifest.files
        },
    }
    vector_sha256 = hashlib.sha256(json.dumps(source_vector_by_field, sort_keys=True).encode()).hexdigest()
    if vector_sha256 != receipt["vector_sha256"]:
        raise ValueError("CMS tax candidate release vector changed")
    for file_spec in manifest.files:
        entry = files[file_spec.name]
        if (
            not isinstance(entry, dict)
            or type(entry.get("sha256")) is not str
            or _SHA256.fullmatch(entry["sha256"]) is None
            or type(entry.get("etag")) is not str
            or not entry["etag"]
            or entry.get("compressed_bytes") != file_spec.compressed_bytes
            or entry.get("original_bytes") != file_spec.original_bytes
            or type(entry.get("row_count")) is not int
            or type(entry.get("distinct_count")) is not int
            or not 0 <= entry["distinct_count"] <= entry["row_count"]
        ):
            raise ValueError("CMS tax candidate release witness is invalid")
    path = directory / f"{_ORGANIZATION_FILE}.zst"
    entry = files[_ORGANIZATION_FILE]
    if path.is_symlink() or not path.is_file() or path.stat().st_size != entry["compressed_bytes"]:
        raise ValueError("CMS tax candidate Organization file is unavailable")
    return path, entry, receipt["vector_sha256"]


def _extract_npi_only(resource: dict[str, Any], cutoff: Any):
    if resource.get("active") is False:
        return None, "inactive"
    identifiers = resource.get("identifier")
    if not isinstance(identifiers, list) or not identifiers:
        return None, "missing_identifiers"
    if any(
        not isinstance(identifier, dict) or identifier.get("system") not in (*_NPI_SYSTEMS, CMS_NPD_PSEUDO_EIN_SYSTEM)
        for identifier in identifiers
    ):
        return None, "unreviewed_identifier_system"
    selected = _select_effective_identifiers(
        identifiers,
        identifier_rule=CMS_NPD_NPI_ONLY_POLICY.rules[0],
        evidence_cutoff=cutoff,
        source_id="cms-npd",
    )
    if selected.state is FhirOrganizationEvidenceState.MISSING_EIN and selected.npi_candidates:
        return selected, None
    return None, selected.state.value


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class _ReportConfig:
    session_factory: Callable[[], AsyncContextManager[Any]]
    schema_name: str
    dataset_id: str
    release_id: str
    snapshot_key: int
    manifest_sha256: str
    canonical_cutoff: str
    cutoff: Any

    def report_kwargs_by_name(self) -> dict[str, Any]:
        """Bind immutable source, snapshot, and extraction identities."""
        return {
            "dataset_id": self.dataset_id,
            "release_id": self.release_id,
            "tax_snapshot_key": self.snapshot_key,
            "tax_manifest_sha256": self.manifest_sha256,
            "extraction_policy_sha256": CMS_NPD_NPI_ONLY_POLICY.descriptor_sha256,
            "extraction_cutoff": self.canonical_cutoff,
        }


@dataclass
class _ReportCounts:
    raw_count: int = 0
    distinct_count: int = 0
    decoded_bytes: int = 0
    candidate_count: int = 0
    skipped_by_reason: Counter[str] = field(default_factory=Counter)


async def _lookup_candidates(config: _ReportConfig, npis: tuple[int, ...]) -> dict[int, Any]:
    """Use a short, read-only transaction for each bounded graph lookup."""
    async with config.session_factory() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        return await lookup_pinned_tax_candidates(
            session,
            schema_name=config.schema_name,
            snapshot_key=config.snapshot_key,
            manifest_sha256=config.manifest_sha256,
            npis=npis,
        )


class _CandidateRows:
    def __init__(self, output: BinaryIO, config: _ReportConfig, counts: _ReportCounts) -> None:
        self.output = output
        self.config = config
        self.counts = counts
        self.pending_organizations: list[CmsTaxCandidateOrganization] = []
        self.pending_npis: set[int] = set()
        self.is_first_row = True

    async def add(self, organization: CmsTaxCandidateOrganization) -> None:
        """Queue one organization, flushing before the NPI bound is exceeded."""
        organization_npis = set(organization.extraction.npi_candidates)
        if len(organization_npis) > _MAX_NPIS_PER_LOOKUP:
            raise ValueError("CMS tax candidate Organization NPI set exceeds lookup bound")
        if self.pending_organizations and (
            len(self.pending_organizations) >= 256 or len(self.pending_npis | organization_npis) > _MAX_NPIS_PER_LOOKUP
        ):
            await self.flush()
        self.pending_organizations.append(organization)
        self.pending_npis.update(organization_npis)

    async def flush(self) -> None:
        """Write complete candidate rows for one bounded batch."""
        if not self.pending_organizations:
            return
        matches_by_npi = await _lookup_candidates(self.config, tuple(sorted(self.pending_npis)))
        batch_by_field = json.loads(
            build_cms_tax_candidate_report(
                **self.config.report_kwargs_by_name(),
                organizations=self.pending_organizations,
                candidates_by_npi=matches_by_npi,
            )
        )
        for organization_row in batch_by_field["organizations"]:
            if not self.is_first_row:
                self.output.write(b",")
            self.output.write(json.dumps(organization_row, sort_keys=True, separators=(",", ":")).encode("utf-8"))
            self.is_first_row = False
            self.counts.candidate_count += 1
        self.pending_organizations.clear()
        self.pending_npis.clear()


def _report_header(output: BinaryIO, config: _ReportConfig, source_entry: dict[str, Any]) -> None:
    empty_report = json.loads(
        build_cms_tax_candidate_report(**config.report_kwargs_by_name(), organizations=(), candidates_by_npi={})
    )
    metadata = {key: field_value for key, field_value in empty_report.items() if key != "organizations"}
    metadata.update(
        {
            "extraction_policy_id": CMS_NPD_NPI_ONLY_POLICY.policy_id,
            "source_file_sha256": source_entry["sha256"],
            "source_file_row_count": source_entry["row_count"],
            "source_file_distinct_count": source_entry["distinct_count"],
        }
    )
    output.write(json.dumps(metadata, sort_keys=True, separators=(",", ":"))[:-1].encode("utf-8"))
    output.write(b',"organizations":[')


def _parse_organization_line(line: bytes) -> tuple[str, bytes, dict[str, Any]]:
    organization_by_field = json.loads(line)
    resource_id = organization_by_field.get("id") if isinstance(organization_by_field, dict) else None
    if (
        not isinstance(organization_by_field, dict)
        or organization_by_field.get("resourceType") != "Organization"
        or type(resource_id) is not str
        or _FHIR_ID.fullmatch(resource_id) is None
    ):
        raise ValueError("CMS tax candidate Organization witness is invalid")
    return resource_id, _canonical_raw_hash(organization_by_field), organization_by_field


def _unique_organizations(
    source_path: Path,
    source_entry: dict[str, Any],
    seen_database: sqlite3.Connection,
    counts: _ReportCounts,
) -> Iterator[tuple[str, bytes, dict[str, Any]]]:
    with zstd.open(source_path, "rb") as decoded:
        while line := decoded.readline(MAX_RESOURCE_LINE_BYTES + 1):
            if len(line) > MAX_RESOURCE_LINE_BYTES:
                raise ValueError("CMS tax candidate Organization line is too large")
            counts.raw_count += 1
            counts.decoded_bytes += len(line)
            if counts.decoded_bytes > source_entry["original_bytes"]:
                raise ValueError("CMS tax candidate Organization bytes changed")
            resource_id, payload_hash, organization_by_field = _parse_organization_line(line)
            prior_digest = seen_database.execute("SELECT digest FROM seen WHERE id = ?", (resource_id,)).fetchone()
            if prior_digest is not None:
                if prior_digest[0] != payload_hash:
                    raise ValueError("CMS tax candidate Organization ID changed")
                continue
            seen_database.execute("INSERT INTO seen VALUES (?, ?)", (resource_id, payload_hash))
            counts.distinct_count += 1
            if counts.distinct_count % 10000 == 0:
                seen_database.commit()
            yield resource_id, payload_hash, organization_by_field


def _verify_source_counts(source_path: Path, source_entry: dict[str, Any], counts: _ReportCounts) -> None:
    if (
        counts.raw_count != source_entry["row_count"]
        or counts.distinct_count != source_entry["distinct_count"]
        or counts.decoded_bytes != source_entry["original_bytes"]
        or _file_sha256(source_path) != source_entry["sha256"]
        or counts.candidate_count + sum(counts.skipped_by_reason.values()) != counts.distinct_count
    ):
        raise ValueError("CMS tax candidate Organization witness changed")


async def _write_report_content(
    output: BinaryIO,
    config: _ReportConfig,
    source_path: Path,
    source_entry: dict[str, Any],
    seen_database: sqlite3.Connection,
) -> int:
    counts = _ReportCounts()
    candidate_rows = _CandidateRows(output, config, counts)
    _report_header(output, config, source_entry)
    for resource_id, payload_hash, organization_by_field in _unique_organizations(
        source_path, source_entry, seen_database, counts
    ):
        extraction, skip_reason = _extract_npi_only(organization_by_field, config.cutoff)
        if extraction is None:
            counts.skipped_by_reason[skip_reason] += 1
            continue
        await candidate_rows.add(CmsTaxCandidateOrganization(resource_id, payload_hash.hex(), extraction))
    await candidate_rows.flush()
    _verify_source_counts(source_path, source_entry, counts)
    output.write(b'],"candidate_organization_count":')
    output.write(str(counts.candidate_count).encode("ascii"))
    output.write(b',"skipped_distinct_organizations":')
    output.write(json.dumps(dict(sorted(counts.skipped_by_reason.items())), separators=(",", ":")).encode("utf-8"))
    output.write(b"}\n")
    output.flush()
    os.fsync(output.fileno())
    return counts.candidate_count


@contextmanager
def _report_workspace(parent: Path) -> Iterator[tuple[Path, sqlite3.Connection, BinaryIO]]:
    report_descriptor, report_name = tempfile.mkstemp(prefix=".cms-tax-", dir=parent)
    seen_name: str | None = None
    try:
        seen_descriptor, seen_name = tempfile.mkstemp(prefix=".cms-tax-seen-", suffix=".sqlite", dir=parent)
        os.close(seen_descriptor)
        with closing(sqlite3.connect(seen_name)) as seen_database:
            seen_database.execute("PRAGMA journal_mode=OFF")
            seen_database.execute("CREATE TABLE seen (id TEXT PRIMARY KEY, digest BLOB NOT NULL) WITHOUT ROWID")
            with os.fdopen(report_descriptor, "wb") as output:
                report_descriptor = None
                yield Path(report_name), seen_database, output
    finally:
        if report_descriptor is not None:
            os.close(report_descriptor)
        if seen_name is not None:
            Path(seen_name).unlink(missing_ok=True)
        Path(report_name).unlink(missing_ok=True)


def _retain_report(temporary_path: Path, output_path: Path, digest: str) -> None:
    try:
        os.link(temporary_path, output_path)
        parent_fd = os.open(output_path.parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    except FileExistsError:
        if output_path.is_symlink() or output_path.stat().st_mode & 0o077 or _file_sha256(output_path) != digest:
            raise ValueError("CMS tax candidate report path already has different evidence") from None


async def run_admitted_cms_tax_candidate_report(
    session_factory: Callable[[], AsyncContextManager[Any]],
    *,
    release_directory: Path,
    output_path: Path,
    schema_name: str,
    dataset_id: str,
    snapshot_key: int,
    manifest_sha256: str,
    evidence_as_of: str,
) -> CmsTaxCandidateReportResult:
    """Stream admitted Organizations into a pinned report with bounded reads.

    A failed attempt leaves no report. Retrying the same published CMS release
    rechecks its retained bytes and reuses only the exact immutable report.
    """
    source_path, source_entry, release_id = _release_witness(release_directory)
    if not output_path.parent.is_dir() or output_path.parent.is_symlink():
        raise ValueError("CMS tax candidate report directory is invalid")
    canonical_cutoff, cutoff = _identifier_cutoff(evidence_as_of)
    if canonical_cutoff != evidence_as_of:
        raise ValueError("CMS tax candidate extraction cutoff is invalid")
    config = _ReportConfig(
        session_factory, schema_name, dataset_id, release_id, snapshot_key, manifest_sha256, canonical_cutoff, cutoff
    )
    await _lookup_candidates(config, ())
    if _file_sha256(source_path) != source_entry["sha256"]:
        raise ValueError("CMS tax candidate Organization file changed")
    with _report_workspace(output_path.parent) as (temporary_path, seen_database, output):
        candidate_count = await _write_report_content(output, config, source_path, source_entry, seen_database)
        digest = _file_sha256(temporary_path)
        _retain_report(temporary_path, output_path, digest)
    return CmsTaxCandidateReportResult(digest, candidate_count)
