# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import csv
import hashlib
import importlib
from pathlib import Path
import shutil
import zipfile
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.nppes_public_evidence_acquisition import NppesListingSnapshot
from process.nppes_public_evidence_archive import (
    RetainedNppesArchive,
    parse_official_nppes_listing,
    prepare_nppes_archive,
)
from process.nppes_public_evidence_import import (
    NPPES_RIGHTS_PROOF_SHA256,
    NppesEvidenceRuntimeConfig,
    NppesPublicEvidenceArchiveReceipt,
    _finished_chain_receipt,
    build_prepared_nppes_release_chain,
)


@pytest.fixture
def npi_module():
    return importlib.import_module("process.npi")


def _minimal_npi_row() -> dict[str, str]:
    npi_csv_row_map = {
        "NPI": "1215387113",
        "Entity Type Code": "2",
        "Provider Organization Name (Legal Business Name)": "Example Org",
        "Provider First Line Business Practice Location Address": "123 Main St",
        "Provider Second Line Business Practice Location Address": "",
        "Provider Business Practice Location Address City Name": "Austin",
        "Provider Business Practice Location Address State Name": "TX",
        "Provider Business Practice Location Address Postal Code": "78701",
        "Provider Business Practice Location Address Country Code (If outside U.S.)": "US",
        "Provider Business Practice Location Address Telephone Number": "5125550100",
        "Provider Business Practice Location Address Fax Number": "",
        "Provider First Line Business Mailing Address": "PO Box 1",
        "Provider Second Line Business Mailing Address": "",
        "Provider Business Mailing Address City Name": "Austin",
        "Provider Business Mailing Address State Name": "TX",
        "Provider Business Mailing Address Postal Code": "78702",
        "Provider Business Mailing Address Country Code (If outside U.S.)": "US",
        "Provider Business Mailing Address Telephone Number": "5125550199",
        "Provider Business Mailing Address Fax Number": "",
        "Provider Enumeration Date": "05/23/2005",
        "Last Update Date": "2024-01-15",
        "NPI Deactivation Date": "",
        "NPI Reactivation Date": "",
    }
    for idx in range(1, 16):
        npi_csv_row_map[f"Healthcare Provider Taxonomy Code_{idx}"] = ""
        npi_csv_row_map[f"Provider License Number_{idx}"] = ""
        npi_csv_row_map[f"Provider License Number State Code_{idx}"] = ""
        npi_csv_row_map[f"Healthcare Provider Primary Taxonomy Switch_{idx}"] = ""
        npi_csv_row_map[f"Healthcare Provider Taxonomy Group_{idx}"] = ""
    npi_csv_row_map["Healthcare Provider Taxonomy Code_1"] = "207Q00000X"
    npi_csv_row_map["Provider License Number_1"] = "TX123"
    npi_csv_row_map["Provider License Number State Code_1"] = "TX"
    npi_csv_row_map["Healthcare Provider Primary Taxonomy Switch_1"] = "Y"
    npi_csv_row_map["Healthcare Provider Taxonomy Group_1"] = "Allopathic & Osteopathic Physicians"

    for idx in range(1, 51):
        npi_csv_row_map[f"Other Provider Identifier_{idx}"] = ""
        npi_csv_row_map[f"Other Provider Identifier Type Code_{idx}"] = ""
        npi_csv_row_map[f"Other Provider Identifier State_{idx}"] = ""
        npi_csv_row_map[f"Other Provider Identifier Issuer_{idx}"] = ""
    npi_csv_row_map["Other Provider Identifier_1"] = "ALT123"
    npi_csv_row_map["Other Provider Identifier Type Code_1"] = "05"
    npi_csv_row_map["Other Provider Identifier State_1"] = "TX"
    npi_csv_row_map["Other Provider Identifier Issuer_1"] = "Issuer"
    return npi_csv_row_map


def _write_csv(path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _build_nppes_zip(tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_csv(
        source_dir / "npidata_pfile_20050523-20260331.csv",
        [_minimal_npi_row()],
    )
    _write_csv(
        source_dir / "pl_pfile_20050523-20260331.csv",
        [
            {
                "NPI": "1215387113",
                "Provider Secondary Practice Location Address- Address Line 1": "456 Side St",
                "Provider Secondary Practice Location Address-  Address Line 2": "",
                "Provider Secondary Practice Location Address - City Name": "Austin",
                "Provider Secondary Practice Location Address - State Name": "TX",
                "Provider Secondary Practice Location Address - Postal Code": "78703",
                "Provider Secondary Practice Location Address - Country Code (If outside U.S.)": "US",
                "Provider Secondary Practice Location Address - Telephone Number": "5125550111",
                "Provider Practice Location Address - Fax Number": "",
            }
        ],
    )
    _write_csv(
        source_dir / "othername_pfile_20050523-20260331.csv",
        [
            {
                "NPI": "1215387113",
                "Provider Other Organization Name": "Example DBA",
                "Provider Other Organization Name Type Code": "3",
            }
        ],
    )
    _write_csv(
        source_dir / "endpoint_pfile_20050523-20260331.csv",
        [{"NPI": "1215387113"}],
    )

    zip_path = tmp_path / "NPPES_Data_Dissemination_March_2026_V2.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        for file_path in source_dir.iterdir():
            archive.write(file_path, arcname=file_path.name)
    return zip_path


def _prepared_chain(tmp_path: Path, zip_path: Path):
    """Bind the synthetic legacy archive to one sealed listing snapshot."""

    archive_name = zip_path.name
    listing_bytes = f'<a href="./{archive_name}">archive</a>'.encode()
    listing_path = tmp_path / "NPI_Files.html"
    listing_path.write_bytes(listing_bytes)
    listing_sha256 = hashlib.sha256(listing_bytes).hexdigest()
    candidate = parse_official_nppes_listing(listing_bytes)[0]
    archive_bytes = zip_path.read_bytes()
    retained = RetainedNppesArchive(
        candidate=candidate,
        path=zip_path,
        artifact_sha256=hashlib.sha256(archive_bytes).hexdigest(),
        artifact_byte_count=len(archive_bytes),
        listing_sha256=listing_sha256,
        etag='"synthetic"',
        last_modified="Tue, 31 Mar 2026 00:00:00 GMT",
        acquired_at="2026-04-01T00:00:00Z",
    )
    listing = NppesListingSnapshot(
        path=listing_path,
        listing_sha256=listing_sha256,
        byte_count=len(listing_bytes),
        candidates=(candidate,),
        etag='"listing"',
        last_modified="Tue, 31 Mar 2026 00:00:00 GMT",
        acquired_at="2026-04-01T00:00:00Z",
    )
    return build_prepared_nppes_release_chain(
        listing,
        (prepare_nppes_archive(retained),),
    )


def _chain_receipt(prepared_chain) -> object:
    """Create one valid value-safe receipt for the synthetic archive."""

    archive = prepared_chain.archives[0]
    archive_receipt = NppesPublicEvidenceArchiveReceipt(
        archive_name=archive.archive_name,
        snapshot_at="2026-03-31T00:00:00Z",
        admission_ref=f"penpa1_{'A' * 43}",
        source_release_ref=f"perel1_{'B' * 43}",
        artifact_sha256=archive.retained.artifact_sha256,
        manifest_sha256="cd" * 32,
        source_record_count=1,
        projected_record_count=1,
        excluded_record_count=0,
        write_state="inserted",
    )
    listing = prepared_chain.listing
    return _finished_chain_receipt(
        listing.listing_sha256,
        listing.byte_count,
        tuple(candidate.archive_name for candidate in listing.candidates),
        (archive_receipt,),
    )


def _install_test_mode_dependencies(
    monkeypatch,
    npi_module,
    zip_path: Path,
    captured_payloads: list[dict[str, object]],
) -> AsyncMock:
    async def fake_download(_url):
        return '<a href="NPPES_Data_Dissemination_20260301_20260331_V2.zip">current</a>'

    async def fake_download_and_save(_url, target, **_kwargs):
        shutil.copyfile(zip_path, target)

    async def fake_unzip(source, target, **_kwargs):
        with zipfile.ZipFile(source) as archive:
            archive.extractall(target)

    async def fake_save(_ctx, npi_payload):
        captured_payloads.append(npi_payload)

    monkeypatch.setattr(npi_module, "download_it", fake_download)
    monkeypatch.setattr(npi_module, "download_it_and_save", fake_download_and_save)
    monkeypatch.setattr(npi_module, "unzip", fake_unzip)
    for dependency_name in (
        "ensure_database",
        "_ensure_required_extensions",
        "_assert_nucc_ready",
        "_assert_nppes_canonical_ready",
        "_acquire_npi_import_lease",
        "_assert_npi_import_lease",
        "_release_npi_import_lease",
    ):
        monkeypatch.setattr(npi_module, dependency_name, AsyncMock())
    monkeypatch.setattr(
        npi_module,
        "_load_nucc_taxonomy_int_code_map",
        AsyncMock(return_value={}),
    )
    staging_reset = AsyncMock()
    monkeypatch.setattr(npi_module, "_prepare_npi_staging", staging_reset)
    monkeypatch.setattr(npi_module, "save_npi_data", fake_save)
    return staging_reset


@pytest.mark.asyncio
async def test_process_data_test_mode_imports_nppes_zip(monkeypatch, tmp_path, npi_module):
    """Import a representative NPPES archive in bounded test mode."""

    zip_path = _build_nppes_zip(tmp_path)
    captured_payloads = []
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_DIR", "https://example.com/")
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_FILE", "feed.html")
    staging_reset = _install_test_mode_dependencies(
        monkeypatch,
        npi_module,
        zip_path,
        captured_payloads,
    )
    worker_context_map = {
        "context": {},
        "redis": SimpleNamespace(enqueue_job=AsyncMock()),
        "import_date": "20260331",
    }
    await npi_module.process_data(worker_context_map, {"test_mode": True})
    staging_reset.assert_awaited_once_with("20260331", "mrf")
    assert worker_context_map["context"]["run"] == 1
    assert worker_context_map["context"]["test_mode"] is True
    npi_payload = next(
        candidate_npi_payload
        for candidate_npi_payload in captured_payloads
        if candidate_npi_payload.get("npi_obj_list")
    )
    assert npi_payload["npi_obj_list"][0]["npi"] == 1215387113
    assert npi_payload["npi_taxonomy_list"][0]["healthcare_provider_taxonomy_code"] == "207Q00000X"
    assert npi_payload["npi_taxonomy_group_list"][0]["healthcare_provider_taxonomy_group"]
    assert npi_payload["npi_other_id_list"][0]["other_provider_identifier"] == "ALT123"
    assert {address["type"] for address in npi_payload["npi_address_list"]} == {"primary", "mail"}
    secondary_payload = next(
        candidate_secondary_payload
        for candidate_secondary_payload in captured_payloads
        if candidate_secondary_payload.get("npi_address_list")
        and candidate_secondary_payload["npi_address_list"][0]["type"] == "secondary"
    )
    assert secondary_payload["npi_address_list"][0]["city_name"] == "AUSTIN"
    other_name_payload = next(
        candidate_other_name_payload
        for candidate_other_name_payload in captured_payloads
        if candidate_other_name_payload.get("npi_other_id_list")
        and candidate_other_name_payload["npi_other_id_list"][0]["other_provider_identifier"] == "Example DBA"
    )
    assert other_name_payload["npi_other_id_list"][0]["other_provider_identifier_type_code"] == "3"
