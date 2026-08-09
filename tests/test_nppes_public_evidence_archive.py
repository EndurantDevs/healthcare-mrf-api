# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Proof for deterministic NPPES listing and safe ZIP traversal."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
import hashlib
import os
from pathlib import Path
import zipfile

import pytest

import process.nppes_public_evidence_archive as archive_contract
from process.nppes_public_evidence_archive import (
    NppesPublicEvidenceArchiveError,
    PreparedNppesArchive,
    RetainedNppesArchive,
    inspect_nppes_archive,
    parse_official_nppes_listing,
    prepare_nppes_archive,
    select_nppes_release_chain,
    validate_nppes_archive_candidate,
)
from process.nppes_public_evidence_members import (
    NppesPrimaryCsvRows,
    materialize_nppes_legacy_members,
    open_verified_nppes_legacy_text,
)


MONTHLY = "NPPES_Data_Dissemination_July_2026_V2.zip"
WEEKLY_NAMES = (
    "NPPES_Data_Dissemination_070626_071226_Weekly_V2.zip",
    "NPPES_Data_Dissemination_071326_071926_Weekly_V2.zip",
    "NPPES_Data_Dissemination_072026_072626_Weekly_V2.zip",
    "NPPES_Data_Dissemination_072726_080226_Weekly_V2.zip",
)
HEADER = (
    "NPI",
    "Entity Type Code",
    "Provider Enumeration Date",
    "Last Update Date",
    "NPI Deactivation Date",
    "NPI Reactivation Date",
)


def _listing(*names: str) -> bytes:
    links = "".join(f'<a href="./{name}">{index}</a>' for index, name in enumerate(names))
    return f"<html><body>{links}</body></html>".encode()


def _candidate(name: str):
    return parse_official_nppes_listing(_listing(name))[0]


def _retained(path: Path, archive_name: str = MONTHLY) -> RetainedNppesArchive:
    payload = path.read_bytes()
    return RetainedNppesArchive(
        candidate=_candidate(archive_name),
        path=path,
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
        artifact_byte_count=len(payload),
        listing_sha256="ab" * 32,
        etag='"opaque"',
        last_modified="Mon, 13 Jul 2026 09:10:32 GMT",
        acquired_at="2026-08-08T00:00:00Z",
    )


def _write_zip(path: Path, primary: str, body: str, *extra_names: str) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(primary, body)
        for name in extra_names:
            archive.writestr(name, "x")


def test_listing_order_is_deterministic_and_selects_post_monthly_chain() -> None:
    candidates = parse_official_nppes_listing(
        _listing(WEEKLY_NAMES[3], WEEKLY_NAMES[0], MONTHLY, *WEEKLY_NAMES[1:3])
    )
    selected = select_nppes_release_chain(candidates, date(2026, 7, 12))
    assert [item.archive_name for item in selected] == [MONTHLY, *WEEKLY_NAMES[1:]]
    assert selected[0].archive_kind == "monthly"
    assert all(item.archive_kind == "weekly" for item in selected[1:])
    assert "NPPES" not in repr(selected[0])


def test_listing_deduplicates_exact_links_and_ignores_unrelated_links() -> None:
    raw_html = (
        _listing(MONTHLY, MONTHLY).decode()
        + '<a href="https://example.test/NPPES_Data_Dissemination_July_2026_V2.zip">x</a>'
    ).encode()
    assert parse_official_nppes_listing(raw_html) == (_candidate(MONTHLY),)


def test_listing_parser_ignores_non_href_attributes_before_href() -> None:
    raw_html = f'<a class="download" href="./{MONTHLY}">monthly</a>'.encode()
    assert parse_official_nppes_listing(raw_html) == (_candidate(MONTHLY),)


@pytest.mark.parametrize(
    "raw_html",
    (
        b"",
        b"<html><a href='https://example.test/no.zip'>x</a></html>",
        b"\xff",
    ),
)
def test_listing_rejects_empty_off_origin_or_invalid_utf8(raw_html: bytes) -> None:
    with pytest.raises(NppesPublicEvidenceArchiveError) as caught:
        parse_official_nppes_listing(raw_html)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


@pytest.mark.parametrize(
    "archive_name",
    (
        "NPPES_Data_Dissemination_July_0000_V2.zip",
        "NPPES_Data_Dissemination_023126_030126_Weekly_V2.zip",
        "NPPES_Data_Dissemination_071926_071326_Weekly_V2.zip",
    ),
)
def test_listing_rejects_invalid_archive_calendar_period(archive_name: str) -> None:
    with pytest.raises(NppesPublicEvidenceArchiveError):
        parse_official_nppes_listing(_listing(archive_name))


def test_candidate_validator_rejects_wrong_outer_type() -> None:
    with pytest.raises(NppesPublicEvidenceArchiveError):
        validate_nppes_archive_candidate(object())


def test_release_chain_rejects_a_weekly_gap() -> None:
    candidates = parse_official_nppes_listing(_listing(MONTHLY, WEEKLY_NAMES[2]))
    with pytest.raises(NppesPublicEvidenceArchiveError):
        select_nppes_release_chain(candidates, date(2026, 7, 12))


def test_release_chain_rebuilds_each_exact_candidate() -> None:
    candidate = _candidate(MONTHLY)
    forged = replace(candidate, source_url="https://download.cms.gov/nppes/forged.zip")
    with pytest.raises(NppesPublicEvidenceArchiveError):
        select_nppes_release_chain((forged,), date(2026, 7, 13))


@pytest.mark.parametrize(
    ("candidates", "snapshot_date"),
    (
        ([], date(2026, 7, 13)),
        ((_candidate(WEEKLY_NAMES[0]),), date(2026, 7, 12)),
        ((_candidate(MONTHLY),), date(2026, 6, 30)),
    ),
)
def test_release_chain_rejects_wrong_shape_or_missing_monthly_base(
    candidates,
    snapshot_date: date,
) -> None:
    with pytest.raises(NppesPublicEvidenceArchiveError):
        select_nppes_release_chain(candidates, snapshot_date)


def test_valid_zip_inventory_and_complete_csv_stream(tmp_path: Path) -> None:
    path = tmp_path / MONTHLY
    primary = "npidata_pfile_20050523-20260713.csv"
    body = "\ufeff" + ",".join(HEADER) + "\r\n1003000100,1,05/23/2005,07/01/2026,,\r\n"
    _write_zip(path, primary, body, "endpoint_pfile_20050523-20260713.csv")
    prepared = prepare_nppes_archive(_retained(path))
    layout = prepared.layout
    assert layout.primary_member_name == primary
    assert layout.primary_snapshot_date == date(2026, 7, 13)
    assert len(layout.members) == 2
    with NppesPrimaryCsvRows(prepared) as rows:
        assert rows.header == HEADER
        assert list(rows) == [
            ("1003000100", "1", "05/23/2005", "07/01/2026", "", "")
        ]


def test_monthly_archive_rejects_a_primary_snapshot_from_another_month(
    tmp_path: Path,
) -> None:
    path = tmp_path / MONTHLY
    primary = "npidata_pfile_20050523-20260802.csv"
    _write_zip(path, primary, ",".join(HEADER) + "\n")
    with pytest.raises(NppesPublicEvidenceArchiveError):
        prepare_nppes_archive(_retained(path))


@pytest.mark.parametrize(
    "ancillary_name",
    (
        "pl_pfile_20050523-20260712.csv",
        "pl_pfile_20260714-20260713.csv",
    ),
)
def test_archive_rejects_mismatched_ancillary_member_period(
    tmp_path: Path,
    ancillary_name: str,
) -> None:
    path = tmp_path / MONTHLY
    primary = "npidata_pfile_20050523-20260713.csv"
    _write_zip(path, primary, ",".join(HEADER) + "\n", ancillary_name)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        prepare_nppes_archive(_retained(path))


def test_csv_stream_must_reach_eof_on_normal_exit(tmp_path: Path) -> None:
    path = tmp_path / MONTHLY
    primary = "npidata_pfile_20050523-20260713.csv"
    body = ",".join(HEADER) + "\n1003000100,1,05/23/2005,07/01/2026,,\n"
    _write_zip(path, primary, body)
    prepared = prepare_nppes_archive(_retained(path))
    with pytest.raises(NppesPublicEvidenceArchiveError):
        with NppesPrimaryCsvRows(prepared) as rows:
            assert rows.header == HEADER


def test_only_four_validated_legacy_members_are_materialized(tmp_path: Path) -> None:
    path = tmp_path / MONTHLY
    primary = "npidata_pfile_20050523-20260713.csv"
    legacy_names = (
        "pl_pfile_20050523-20260713.csv",
        "othername_pfile_20050523-20260713.csv",
        "endpoint_pfile_20050523-20260713.csv",
    )
    _write_zip(path, primary, ",".join(HEADER) + "\n", *legacy_names, "readme.pdf")
    prepared = prepare_nppes_archive(_retained(path))
    destination = tmp_path / "private"
    destination.mkdir(mode=0o700)
    materialized = materialize_nppes_legacy_members(prepared, destination)
    assert {
        materialized.primary_path.name,
        materialized.practice_location_path.name,
        materialized.other_name_path.name,
        materialized.endpoint_path.name,
    } == {primary, *legacy_names}
    assert {item.name for item in destination.iterdir()} == {primary, *legacy_names}


@pytest.mark.asyncio
async def test_materialized_primary_swap_cannot_diverge_from_evidence(
    tmp_path: Path,
) -> None:
    path = tmp_path / MONTHLY
    primary = "npidata_pfile_20050523-20260713.csv"
    legacy_names = (
        "pl_pfile_20050523-20260713.csv",
        "othername_pfile_20050523-20260713.csv",
        "endpoint_pfile_20050523-20260713.csv",
    )
    _write_zip(path, primary, ",".join(HEADER) + "\n", *legacy_names)
    prepared = prepare_nppes_archive(_retained(path))
    destination = tmp_path / "private"
    destination.mkdir(mode=0o700)
    materialized = materialize_nppes_legacy_members(prepared, destination)
    replacement = destination / "replacement.csv"
    replacement.write_bytes(materialized.primary_path.read_bytes())
    os.replace(replacement, materialized.primary_path)

    with pytest.raises(NppesPublicEvidenceArchiveError):
        async with open_verified_nppes_legacy_text(
            materialized,
            "primary",
        ) as member_stream:
            await member_stream.read()


def test_sealed_archive_rejects_inode_drift_before_replay(tmp_path: Path) -> None:
    path = tmp_path / MONTHLY
    primary = "npidata_pfile_20050523-20260713.csv"
    _write_zip(path, primary, ",".join(HEADER) + "\n")
    prepared = prepare_nppes_archive(_retained(path))
    path.write_bytes(path.read_bytes() + b"drift")
    with pytest.raises(NppesPublicEvidenceArchiveError):
        with NppesPrimaryCsvRows(prepared) as rows:
            list(rows)


def test_sealed_archive_rejects_same_size_rewrite_with_restored_mtime(
    tmp_path: Path,
) -> None:
    path = tmp_path / MONTHLY
    primary = "npidata_pfile_20050523-20260713.csv"
    _write_zip(path, primary, ",".join(HEADER) + "\n")
    prepared = prepare_nppes_archive(_retained(path))
    original_stat = path.stat()
    mutated_bytes = bytearray(path.read_bytes())
    mutated_bytes[-1] ^= 1
    path.write_bytes(mutated_bytes)
    os.utime(
        path,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        with NppesPrimaryCsvRows(prepared) as rows:
            list(rows)


def test_primary_rows_normalize_retained_path_failure_during_exit(
    tmp_path: Path,
) -> None:
    path = tmp_path / MONTHLY
    primary = "npidata_pfile_20050523-20260713.csv"
    _write_zip(path, primary, ",".join(HEADER) + "\n")
    prepared = prepare_nppes_archive(_retained(path))

    with pytest.raises(NppesPublicEvidenceArchiveError) as caught:
        with NppesPrimaryCsvRows(prepared) as rows:
            assert list(rows) == []
            path.unlink()

    assert str(caught.value) == "nppes_public_evidence_archive_invalid"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_prepare_rejects_atomic_path_swap_during_inspection(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / MONTHLY
    primary = "npidata_pfile_20050523-20260713.csv"
    _write_zip(path, primary, ",".join(HEADER) + "\n")
    original_inspection = archive_contract._inspect_verified_nppes_archive

    def swap_after_inspection(fixed_retained, archive_stream):
        layout = original_inspection(fixed_retained, archive_stream)
        replacement = tmp_path / "replacement.zip"
        _write_zip(replacement, primary, ",".join(HEADER) + "\n1003000100,1,,,,\n")
        os.replace(replacement, path)
        return layout

    monkeypatch.setattr(
        archive_contract,
        "_inspect_verified_nppes_archive",
        swap_after_inspection,
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        prepare_nppes_archive(_retained(path))


def test_csv_stream_rejects_an_exact_type_with_a_forged_seal(tmp_path: Path) -> None:
    path = tmp_path / MONTHLY
    primary = "npidata_pfile_20050523-20260713.csv"
    _write_zip(path, primary, ",".join(HEADER) + "\n")
    prepared = prepare_nppes_archive(_retained(path))
    forged = object.__new__(PreparedNppesArchive)
    object.__setattr__(forged, "retained", prepared.retained)
    object.__setattr__(forged, "layout", prepared.layout)
    object.__setattr__(forged, "_file_identity", prepared._file_identity)
    object.__setattr__(forged, "_seal", object())
    with pytest.raises(NppesPublicEvidenceArchiveError):
        with NppesPrimaryCsvRows(forged) as rows:
            list(rows)


@pytest.mark.parametrize(
    "member_names",
    (
        ("../npidata_pfile_20050523-20260713.csv",),
        (
            "npidata_pfile_20050523-20260713.csv",
            "npidata_pfile_20050524-20260713.csv",
        ),
    ),
)
def test_zip_rejects_unsafe_or_ambiguous_primary_members(
    tmp_path: Path, member_names: tuple[str, ...]
) -> None:
    path = tmp_path / MONTHLY
    with zipfile.ZipFile(path, "w") as archive:
        for name in member_names:
            archive.writestr(name, ",".join(HEADER) + "\n")
    with pytest.raises(NppesPublicEvidenceArchiveError):
        inspect_nppes_archive(_retained(path))


def test_csv_rejects_bad_header_and_row_width(tmp_path: Path) -> None:
    for index, body in enumerate(
        (
            "NPI,Entity Type Code\n1003000100,1\n",
            ",".join(HEADER) + "\n1003000100,1\n",
        )
    ):
        path = tmp_path / f"{index}-{MONTHLY}"
        _write_zip(path, "npidata_pfile_20050523-20260713.csv", body)
        prepared = prepare_nppes_archive(_retained(path))
        with pytest.raises(NppesPublicEvidenceArchiveError):
            with NppesPrimaryCsvRows(prepared) as rows:
                list(rows)
