# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic archives prove bounded preparation, never source coverage."""

import hashlib
import io
import stat
import struct
import warnings
import zipfile
from dataclasses import replace
from datetime import date, datetime, timezone

import pytest

from process import registry_source_preparation as preparation
from process.registry_source_admission import _source_header_sha256
from process.registry_source_editions import RegistrySourceEditionCandidate
from process.registry_source_fetch import (
    RegistrySourceFetchReceipt,
    RegistrySourceFetchResult,
    RegistrySourceFetchSpec,
    fetch_registry_source,
)
from tests.test_cms_planfinder_workbook_input import _workbook
from tests.test_registry_source_fetch import _source_http

_HEADERS = (
    "mr_submission_template_id",
    "business_state",
    "group_affiliation",
    "company_pk",
    "hios_issuer_id",
    "company_name",
    "company_address",
    "domiciliary_state",
    "naic_group_code",
    "naic_company_code",
    "federal_ein",
    "am_best_number",
    "dba_marketing_name",
    "not_for_profit",
    "created_date",
    "merge_markets_ind_small_grp",
    "fit_exempt",
)
_HEADER = b"\xef\xbb\xbf" + (",".join(_HEADERS) + "\r\n" + ",".join(["synthetic"] * len(_HEADERS)) + "\r\n").encode()
_LOOKUP = b"reporting_year,lookup_cat_code,lookup_cat_name\r\n2024,1,Example\r\n"


def _candidate(source_id="commercial-mlr", **changes):
    candidate = RegistrySourceEditionCandidate(
        source_id,
        "2024-as-of-2025-09-12",
        "https://www.cms.gov/files/zip/synthetic.zip",
        2024,
        None,
        None,
        date(2025, 9, 12),
    )
    if source_id == "plan-finder":
        candidate = replace(
            candidate, edition_id="2026-q4-final", reporting_year=2026, quarter=4, release="final", as_of_date=None
        )
    return replace(candidate, **changes)


def _zip(members, *, compression=zipfile.ZIP_DEFLATED):
    output = io.BytesIO()
    with warnings.catch_warnings(), zipfile.ZipFile(output, "w", compression=compression) as archive:
        warnings.simplefilter("ignore", UserWarning)
        for name, content in members:
            archive.writestr(name, content)
    return output.getvalue()


def _mlr_members():
    return [(preparation._HEADER_MEMBER, _HEADER), (preparation._LOOKUP_MEMBER, _LOOKUP), ("unused.csv", b"unused")]


def _fetch_result(tmp_path, archive_bytes, candidate):
    digest = hashlib.sha256(archive_bytes).hexdigest()
    path = tmp_path / (digest + ".artifact")
    path.write_bytes(archive_bytes)
    spec = RegistrySourceFetchSpec(
        "cms",
        candidate.source_id,
        candidate.edition_id,
        candidate.source_url,
        preparation._PARSERS_BY_SOURCE[candidate.source_id],
        candidate.reporting_year,
        None,
        4 * 1024 * 1024,
    )
    receipt = RegistrySourceFetchReceipt(spec, digest, len(archive_bytes), datetime.now(timezone.utc), '"validator"')
    return RegistrySourceFetchResult(receipt, path, False)


def _prepare(tmp_path, members=None, candidate=None, *, archive_bytes=None):
    candidate = candidate or _candidate()
    archive_bytes = archive_bytes if archive_bytes is not None else _zip(members or _mlr_members())
    fetched = _fetch_result(tmp_path, archive_bytes, candidate)
    return preparation.prepare_registry_source_input(candidate, fetched, tmp_path / "inputs"), fetched


def test_mlr_exact_bytes_header_and_lookup_provenance_are_replay_stable(tmp_path):
    prepared, fetched = _prepare(tmp_path)
    replay = preparation.prepare_registry_source_input(prepared.candidate, fetched, tmp_path / "inputs")
    assert replay == prepared and prepared.input_path.read_bytes() == _HEADER
    assert prepared.input_sha256 == hashlib.sha256(_HEADER).hexdigest()
    assert prepared.header_sha256 == _source_header_sha256(_HEADERS)
    assert prepared.lookup_sha256 == hashlib.sha256(_LOOKUP).hexdigest()
    assert prepared.fetch == fetched and fetched.artifact_path.read_bytes() == _zip(_mlr_members())
    assert prepared.member_date is None and prepared.candidate.reporting_year == 2024
    assert list((tmp_path / "inputs").iterdir()) == [prepared.input_path]


@pytest.mark.asyncio
async def test_genuine_fetch_receipt_and_conditional_replay_prepare_same_bytes(tmp_path):
    candidate = _candidate()
    body = _zip(_mlr_members())
    synthetic = _fetch_result(tmp_path, body, candidate)
    async with _source_http(body) as (state, transport):
        state.headers = {"ETag": '"same"'}
        state.conditional = True
        first = await fetch_registry_source(synthetic.receipt.spec, tmp_path / "raw", _test_transport=transport)
        second = await fetch_registry_source(
            synthetic.receipt.spec, tmp_path / "raw", previous=first.receipt, _test_transport=transport
        )
    prepared = preparation.prepare_registry_source_input(candidate, first, tmp_path / "inputs")
    replay = preparation.prepare_registry_source_input(candidate, second, tmp_path / "inputs")
    assert second.unchanged and prepared.input_path == replay.input_path
    assert replay.candidate.published_at is None and replay.fetch.receipt.etag == '"same"'


@pytest.mark.parametrize(
    "field,changed",
    [
        ("source_id", "plan-finder"),
        ("edition_id", "different"),
        ("reporting_year", 2023),
        ("source_url", "https://www.cms.gov/files/zip/other.zip"),
        ("quarter", 2),
        ("release", "final"),
    ],
)
def test_candidate_receipt_identity_mismatch_refuses_before_output(tmp_path, field, changed):
    candidate = _candidate()
    fetched = _fetch_result(tmp_path, _zip(_mlr_members()), candidate)
    with pytest.raises(preparation.RegistrySourcePreparationError):
        preparation.prepare_registry_source_input(replace(candidate, **{field: changed}), fetched, tmp_path / "inputs")
    assert not (tmp_path / "inputs").exists()


@pytest.mark.parametrize("kind", ["hash", "size", "pin", "parser", "symlink", "basename"])
def test_retained_artifact_and_receipt_tampering_refuses(tmp_path, kind):
    candidate = _candidate()
    fetched = _fetch_result(tmp_path, _zip(_mlr_members()), candidate)
    if kind == "hash":
        original = fetched.artifact_path.read_bytes()
        fetched.artifact_path.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))
    elif kind == "size":
        fetched = replace(fetched, receipt=replace(fetched.receipt, artifact_bytes=fetched.receipt.artifact_bytes - 1))
    elif kind in {"pin", "parser"}:
        changes_by_name = {"expected_sha256": "a" * 64} if kind == "pin" else {"parser_version": "unsupported"}
        fetched = replace(
            fetched, receipt=replace(fetched.receipt, spec=replace(fetched.receipt.spec, **changes_by_name))
        )
    else:
        moved = tmp_path / "moved"
        fetched.artifact_path.rename(moved)
        if kind == "symlink":
            fetched.artifact_path.symlink_to(moved)
        else:
            fetched = replace(fetched, artifact_path=moved)
    with pytest.raises(preparation.RegistrySourcePreparationError):
        preparation.prepare_registry_source_input(candidate, fetched, tmp_path / "inputs")


@pytest.mark.parametrize("name", ["../escape.csv", "/absolute.csv", "sub/file.csv", "back\\slash.csv", "nul\0tail"])
def test_path_hazards_in_any_member_refuse_without_extraction(tmp_path, name):
    archive_bytes = _zip(_mlr_members() + [(name.replace("\0", "X"), b"ignored")])
    if "\0" in name:
        archive_bytes = archive_bytes.replace(b"nulXtail", b"nul\0tail")
    with pytest.raises(preparation.RegistrySourcePreparationError):
        _prepare(tmp_path, archive_bytes=archive_bytes)
    assert not (tmp_path / "escape.csv").exists()


@pytest.mark.parametrize("kind", ["duplicate", "symlink", "compression", "encryption", "directory"])
def test_unsafe_or_ambiguous_archive_members_refuse(tmp_path, kind):
    members = _mlr_members()
    if kind == "duplicate":
        members.append(members[0])
    else:
        info = zipfile.ZipInfo("extra.csv" if kind != "directory" else "nested/")
        if kind == "symlink":
            info.create_system = 3
            info.external_attr = (stat.S_IFLNK | 0o777) << 16
        if kind == "compression":
            info.compress_type = zipfile.ZIP_BZIP2
        members.append((info, b"ignored"))
    archive_bytes = _zip(members)
    if kind == "encryption":
        archive_bytes = archive_bytes.replace(
            b"PK\x01\x02\x14\x03\x14\x00\x00\x00", b"PK\x01\x02\x14\x03\x14\x00\x01\x00", 1
        )
    with pytest.raises(preparation.RegistrySourcePreparationError):
        _prepare(tmp_path, archive_bytes=archive_bytes)


@pytest.mark.parametrize(
    "kind",
    [
        "missing",
        "wrong-year",
        "mixed-year",
        "lookup-header",
        "lookup-limit",
        "input-limit",
        "duplicate-header",
        "empty-header",
        "crc",
        "directory-limit",
        "malformed",
    ],
)
def test_mlr_complete_lookup_limits_header_crc_and_container_refusals(tmp_path, monkeypatch, kind):
    members = _mlr_members()
    if kind == "missing":
        members = members[1:]
    elif kind in {"wrong-year", "mixed-year", "lookup-header"}:
        lookup = _LOOKUP.replace(b"2024", b"2023") if kind == "wrong-year" else _LOOKUP
        lookup += b"2023,2,Example\r\n" if kind == "mixed-year" else b""
        lookup = lookup.replace(b"reporting_year", b"unknown") if kind == "lookup-header" else lookup
        members[1] = (preparation._LOOKUP_MEMBER, lookup)
    elif kind in {"lookup-limit", "input-limit"}:
        monkeypatch.setattr(preparation, "MAX_LOOKUP_BYTES" if kind == "lookup-limit" else "MAX_MLR_INPUT_BYTES", 8)
    elif kind in {"duplicate-header", "empty-header"}:
        members[0] = (preparation._HEADER_MEMBER, b"a,a\n" if kind == "duplicate-header" else b"\n")
    archive_bytes = _zip(members, compression=zipfile.ZIP_STORED)
    if kind == "crc":
        archive_bytes = archive_bytes.replace(_HEADER, _HEADER.replace(b"synthetic", b"tampering", 1), 1)
    elif kind == "directory-limit":
        archive_bytes = bytearray(archive_bytes)
        offset = archive_bytes.rfind(b"PK\x05\x06")
        struct.pack_into("<HH", archive_bytes, offset + 8, 65, 65)
        archive_bytes = bytes(archive_bytes)
    elif kind == "malformed":
        archive_bytes = b"not a zip"
    with pytest.raises(preparation.RegistrySourcePreparationError):
        _prepare(tmp_path, archive_bytes=archive_bytes)
    assert not list((tmp_path / "inputs").glob("*"))


def test_planfinder_uses_real_decoder_preserves_raw_workbook_and_period(tmp_path, monkeypatch):
    workbook, digest = _workbook(tmp_path, count=5001)
    members = [("2026-08-06-HIOS.xlsx", workbook.read_bytes()), ("2026-08-06-RBIS.zip", b"never open this")]
    original_open = zipfile.ZipFile.open

    def guarded_open(archive, name, *args, **kwargs):
        if kwargs.get("mode", "r") == "r":
            assert "RBIS" not in (name.filename if isinstance(name, zipfile.ZipInfo) else name)
        return original_open(archive, name, *args, **kwargs)

    monkeypatch.setattr(zipfile.ZipFile, "open", guarded_open)
    prepared, _ = _prepare(tmp_path, members, _candidate("plan-finder"))
    assert prepared.input_path.read_bytes() == workbook.read_bytes() and prepared.input_sha256 == digest
    assert prepared.header_sha256 == _source_header_sha256(preparation.HEADERS)
    assert prepared.member_date == date(2026, 8, 6) and prepared.candidate.quarter == 4
    assert prepared.candidate.reporting_year == 2026 and prepared.candidate.published_at is None
    assert prepared.lookup_sha256 is None


@pytest.mark.parametrize("kind", ["ambiguous", "missing", "unsupported-date", "late-malformed", "size"])
def test_planfinder_ambiguity_layout_and_late_decoder_failure_leave_no_input(tmp_path, monkeypatch, kind):
    def corrupt_last_row(documents_by_path):
        sheet = documents_by_path["xl/worksheets/sheet1.xml"]
        sheet[-1][-1].set("r", "99999")

    workbook, _ = _workbook(
        tmp_path,
        count=5001 if kind == "late-malformed" else 1,
        mutation=corrupt_last_row if kind == "late-malformed" else None,
    )
    name = "2026-08-07-HIOS.xlsx" if kind == "unsupported-date" else "2026-08-06-HIOS.xlsx"
    members = [(name, workbook.read_bytes())]
    if kind == "ambiguous":
        members.append(("2026-08-05-HIOS.xlsx", workbook.read_bytes()))
    elif kind == "missing":
        members = [("2026-08-06-RBIS.zip", b"ignored")]
    elif kind == "size":
        monkeypatch.setattr(preparation, "MAX_WORKBOOK_BYTES", 8)
    with pytest.raises(preparation.RegistrySourcePreparationError):
        _prepare(tmp_path, members, _candidate("plan-finder"))
    assert not list((tmp_path / "inputs").glob("*"))


@pytest.mark.parametrize("kind", ["corrupt-existing", "symlink-existing", "symlink-root"])
def test_prepared_input_reuse_never_overwrites_untrusted_existing_paths(tmp_path, kind):
    candidate = _candidate()
    fetched = _fetch_result(tmp_path, _zip(_mlr_members()), candidate)
    root = tmp_path / "inputs"
    root.mkdir()
    final = root / (hashlib.sha256(_HEADER).hexdigest() + ".csv")
    if kind == "corrupt-existing":
        final.write_bytes(b"preserve")
    elif kind == "symlink-existing":
        final.symlink_to(fetched.artifact_path)
    else:
        root.rmdir()
        root.symlink_to(tmp_path / "actual", target_is_directory=True)
        (tmp_path / "actual").mkdir()
    with pytest.raises(preparation.RegistrySourcePreparationError):
        preparation.prepare_registry_source_input(candidate, fetched, root)
    if kind == "corrupt-existing":
        assert final.read_bytes() == b"preserve"


@pytest.mark.parametrize("changes_by_name", [{"quarter": 3}, {"release": "preliminary"}, {"reporting_year": 2025}])
def test_planfinder_candidate_period_must_match_its_explicit_edition_label(tmp_path, changes_by_name):
    candidate = _candidate("plan-finder")
    fetched = _fetch_result(tmp_path, _zip([("2026-08-06-HIOS.xlsx", b"not opened")]), candidate)
    changed = replace(candidate, **changes_by_name)
    # Change the receipt year too: inconsistent candidate identity must still fail.
    fetched = replace(
        fetched,
        receipt=replace(fetched.receipt, spec=replace(fetched.receipt.spec, reporting_year=changed.reporting_year)),
    )
    with pytest.raises(preparation.RegistrySourcePreparationError):
        preparation.prepare_registry_source_input(changed, fetched, tmp_path / "inputs")
    assert not (tmp_path / "inputs").exists()


def test_directory_limits_are_checked_before_zipfile_materialization(tmp_path, monkeypatch):
    candidate = _candidate()
    archive_bytes = bytearray(_zip(_mlr_members()))
    offset = archive_bytes.rfind(b"PK\x05\x06")
    struct.pack_into("<L", archive_bytes, offset + 12, 64 * 1024 + 1)
    fetched = _fetch_result(tmp_path, bytes(archive_bytes), candidate)

    def refuse_zip_construction(*args, **kwargs):
        raise AssertionError("Oversized directory must not be materialized")

    monkeypatch.setattr(preparation.zipfile, "ZipFile", refuse_zip_construction)
    with pytest.raises(preparation.RegistrySourcePreparationError):
        preparation.prepare_registry_source_input(candidate, fetched, tmp_path / "inputs")


def _zip64_directory_bypass(comment_size):
    archive_bytes = _zip([(f"entry-{index:04d}.csv", b"x") for index in range(1500)], compression=zipfile.ZIP_STORED)
    offset = archive_bytes.rfind(b"PK\x05\x06")
    _, _, _, _, count, directory_size, directory_offset, _ = struct.unpack("<4s4H2LH", archive_bytes[offset:])
    record = struct.pack("<4sQ2H2L4Q", b"PK\x06\x06", 44, 45, 45, 0, 0, count, count, directory_size, directory_offset)
    locator = struct.pack("<4sLQL", b"PK\x06\x07", 0, offset, 1)
    forged = struct.pack("<4s4H2LH", b"PK\x05\x06", 0, 0, 2, 2, 0, offset + len(record) + len(locator), comment_size)
    return archive_bytes[:offset] + record + locator + forged + b"x" * comment_size


@pytest.mark.parametrize("comment_size", [0, 65535])
def test_zip64_locator_refuses_before_constructor_even_with_maximum_comment(tmp_path, monkeypatch, comment_size):
    candidate = _candidate()
    archive_bytes = _zip64_directory_bypass(comment_size)
    # The standard directory fields conceal the larger ZIP64 directory.
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        assert len(archive.infolist()) == 1500
    fetched = _fetch_result(tmp_path, archive_bytes, candidate)

    def refuse_zip_construction(*args, **kwargs):
        raise AssertionError("ZIP64 directory must not be materialized by preparation")

    monkeypatch.setattr(preparation.zipfile, "ZipFile", refuse_zip_construction)
    with pytest.raises(preparation.RegistrySourcePreparationError):
        preparation.prepare_registry_source_input(candidate, fetched, tmp_path / "inputs")
    assert not list((tmp_path / "inputs").iterdir())


@pytest.mark.parametrize("member_name", [preparation._HEADER_MEMBER, preparation._LOOKUP_MEMBER])
def test_malformed_quoted_csv_is_sanitized_and_preserves_original_bytes(tmp_path, member_name):
    candidate = _candidate()
    malformed = b'"unterminated'
    if member_name == preparation._LOOKUP_MEMBER:
        malformed = b'reporting_year,lookup_cat_code,lookup_cat_name\n2024,1,"unterminated'
    members = [(name, malformed if name == member_name else content) for name, content in _mlr_members()]
    archive_bytes = _zip(members)
    fetched = _fetch_result(tmp_path, archive_bytes, candidate)
    with pytest.raises(
        preparation.RegistrySourcePreparationError, match="^Registry source preparation is unavailable or invalid$"
    ):
        preparation.prepare_registry_source_input(candidate, fetched, tmp_path / "inputs")
    assert fetched.artifact_path.read_bytes() == archive_bytes
    assert not list((tmp_path / "inputs").iterdir())


@pytest.mark.parametrize("kind", ["in-place", "replacement"])
def test_retained_archive_change_during_preparation_refuses_and_cleans_stage(tmp_path, monkeypatch, kind):
    candidate = _candidate()
    fetched = _fetch_result(tmp_path, _zip(_mlr_members()), candidate)
    original_header_digest = preparation._header_digest

    def mutate_retained_archive(*args):
        digest = original_header_digest(*args)
        if kind == "in-place":
            fetched.artifact_path.write_bytes(b"changed while preparing")
        else:
            replacement = tmp_path / "replacement"
            replacement.write_bytes(fetched.artifact_path.read_bytes())
            replacement.replace(fetched.artifact_path)
        return digest

    monkeypatch.setattr(preparation, "_header_digest", mutate_retained_archive)
    with pytest.raises(preparation.RegistrySourcePreparationError):
        preparation.prepare_registry_source_input(candidate, fetched, tmp_path / "inputs")
    assert not list((tmp_path / "inputs").iterdir())
