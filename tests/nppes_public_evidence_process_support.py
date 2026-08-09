# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic retained NPPES files for process-boundary tests."""

from __future__ import annotations

import csv
import hashlib
import io
from pathlib import Path
import stat
import zipfile

from process.nppes_public_evidence_acquisition import NppesListingSnapshot
from process.nppes_public_evidence_archive import (
    RetainedNppesArchive,
    parse_official_nppes_listing,
    prepare_nppes_archive,
)
from process.nppes_public_evidence_import import build_prepared_nppes_release_chain
from process.ptg_parts.artifacts import sha256_file
from tests.public_evidence_nppes_registry_support import HEADER


def _synthetic_npi(ordinal: int) -> str:
    """Return one deterministic CMS-range NPI with a valid Luhn check digit."""

    if type(ordinal) is not int or not 0 <= ordinal < 100_000_000:
        raise ValueError("synthetic NPI ordinal is outside the bounded range")
    prefix = f"{100_000_000 + ordinal:09d}"
    checksum_total = 0
    for digit_index, digit_text in enumerate("80840" + prefix):
        digit = int(digit_text)
        if digit_index % 2:
            doubled = digit * 2
            digit = doubled // 10 + doubled % 10
        checksum_total += digit
    return prefix + str((-checksum_total) % 10)


def _scale_row(source_row_ordinal: int) -> tuple[str, ...]:
    """Build the benchmark's 98 percent projected, two-reason row mix."""

    ordinal_modulo = source_row_ordinal % 100
    npi = _synthetic_npi(source_row_ordinal)
    entity_type = str(1 + source_row_ordinal % 2)
    if ordinal_modulo == 98:
        return (npi, "", "05/23/2005", "05/01/2026", "", "")
    if ordinal_modulo == 99:
        return (npi, entity_type, "", "", "", "")
    return (npi, entity_type, "05/23/2005", "05/01/2026", "", "")


def _zip_member(member_name: str) -> zipfile.ZipInfo:
    """Return one deterministic regular-file ZIP member descriptor."""

    member = zipfile.ZipInfo(
        member_name,
        date_time=(2026, 7, 13, 9, 10, 32),
    )
    member.compress_type = zipfile.ZIP_DEFLATED
    member.create_system = 3
    member.external_attr = (stat.S_IFREG | 0o600) << 16
    return member


def _write_scale_primary_member(
    archive: zipfile.ZipFile,
    primary_name: str,
    row_count: int,
) -> None:
    """Stream one bounded synthetic primary CSV directly into its ZIP member."""

    with archive.open(_zip_member(primary_name), "w", force_zip64=True) as raw_stream:
        with io.TextIOWrapper(raw_stream, encoding="utf-8", newline="") as text_stream:
            csv_writer = csv.writer(text_stream, lineterminator="\n")
            csv_writer.writerow(HEADER)
            for source_row_ordinal in range(row_count):
                csv_writer.writerow(_scale_row(source_row_ordinal))


def prepared_sized_archive(
    root: Path,
    archive_name: str,
    primary_end: str,
    row_count: int,
):
    """Create a streaming scale archive without retaining its CSV in memory."""

    primary_name = f"npidata_pfile_20050523-{primary_end}.csv"
    archive_path = root / archive_name
    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
        _write_scale_primary_member(archive, primary_name, row_count)
        archive.writestr(_zip_member("readme.pdf"), b"synthetic scale archive")
    artifact_sha256, artifact_byte_count = sha256_file(archive_path)
    candidate = parse_official_nppes_listing(
        f'<a href="./{archive_name}">archive</a>'.encode()
    )[0]
    retained = RetainedNppesArchive(
        candidate=candidate,
        path=archive_path,
        artifact_sha256=artifact_sha256,
        artifact_byte_count=artifact_byte_count,
        listing_sha256="cd" * 32,
        etag='"synthetic-scale"',
        last_modified="Mon, 13 Jul 2026 09:10:32 GMT",
        acquired_at="2026-08-08T00:00:00Z",
    )
    return prepare_nppes_archive(retained)


def prepared_archive(
    root: Path,
    archive_name: str,
    primary_end: str,
    csv_rows: tuple[tuple[str, ...], ...],
    *,
    listing_sha256: str = "ab" * 32,
):
    """Create one sealed synthetic retained archive for process-level tests."""

    primary_name = f"npidata_pfile_20050523-{primary_end}.csv"
    csv_body = ",".join(HEADER) + "\n"
    csv_body += "".join(",".join(csv_row) + "\n" for csv_row in csv_rows)
    archive_path = root / archive_name
    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for member_name, member_bytes in (
            (primary_name, csv_body.encode("utf-8")),
            ("readme.pdf", b"synthetic"),
        ):
            member = zipfile.ZipInfo(
                member_name,
                date_time=(2026, 7, 13, 9, 10, 32),
            )
            member.compress_type = zipfile.ZIP_DEFLATED
            member.create_system = 3
            member.external_attr = (stat.S_IFREG | 0o600) << 16
            archive.writestr(member, member_bytes)
    archive_bytes = archive_path.read_bytes()
    candidate = parse_official_nppes_listing(
        f'<a href="./{archive_name}">archive</a>'.encode()
    )[0]
    retained = RetainedNppesArchive(
        candidate=candidate,
        path=archive_path,
        artifact_sha256=hashlib.sha256(archive_bytes).hexdigest(),
        artifact_byte_count=len(archive_bytes),
        listing_sha256=listing_sha256,
        etag='"synthetic"',
        last_modified="Mon, 13 Jul 2026 09:10:32 GMT",
        acquired_at="2026-08-08T00:00:00Z",
    )
    return prepare_nppes_archive(retained)


def prepared_release_chain(
    root: Path,
    archive_specs: tuple[
        tuple[str, str, tuple[tuple[str, ...], ...]],
        ...,
    ],
    *,
    listing_names: tuple[str, ...] | None = None,
):
    """Create a sealed synthetic listing and its exact selected archive chain."""

    candidate_names = listing_names or tuple(spec[0] for spec in archive_specs)
    raw_listing = "".join(
        f'<a href="./{archive_name}">archive</a>'
        for archive_name in candidate_names
    ).encode()
    listing_path = root / "NPI_Files.html"
    listing_path.write_bytes(raw_listing)
    listing_sha256 = hashlib.sha256(raw_listing).hexdigest()
    listing = NppesListingSnapshot(
        path=listing_path,
        listing_sha256=listing_sha256,
        byte_count=len(raw_listing),
        candidates=parse_official_nppes_listing(raw_listing),
        etag='"listing"',
        last_modified="Mon, 13 Jul 2026 09:10:32 GMT",
        acquired_at="2026-08-08T00:00:00Z",
    )
    archives = tuple(
        prepared_archive(
            root,
            archive_name,
            primary_end,
            csv_rows,
            listing_sha256=listing_sha256,
        )
        for archive_name, primary_end, csv_rows in archive_specs
    )
    return build_prepared_nppes_release_chain(listing, archives)
