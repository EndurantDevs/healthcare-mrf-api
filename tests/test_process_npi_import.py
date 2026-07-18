# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import csv
import importlib
import shutil
import zipfile
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.fixture
def npi_module():
    return importlib.import_module("process.npi")


def _minimal_npi_row() -> dict[str, str]:
    npi_csv_row = {
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
        "Last Update Date": "2024-01-15",
    }
    for idx in range(1, 16):
        npi_csv_row[f"Healthcare Provider Taxonomy Code_{idx}"] = ""
        npi_csv_row[f"Provider License Number_{idx}"] = ""
        npi_csv_row[f"Provider License Number State Code_{idx}"] = ""
        npi_csv_row[f"Healthcare Provider Primary Taxonomy Switch_{idx}"] = ""
        npi_csv_row[f"Healthcare Provider Taxonomy Group_{idx}"] = ""
    npi_csv_row["Healthcare Provider Taxonomy Code_1"] = "207Q00000X"
    npi_csv_row["Provider License Number_1"] = "TX123"
    npi_csv_row["Provider License Number State Code_1"] = "TX"
    npi_csv_row["Healthcare Provider Primary Taxonomy Switch_1"] = "Y"
    npi_csv_row["Healthcare Provider Taxonomy Group_1"] = "Allopathic & Osteopathic Physicians"

    for idx in range(1, 51):
        npi_csv_row[f"Other Provider Identifier_{idx}"] = ""
        npi_csv_row[f"Other Provider Identifier Type Code_{idx}"] = ""
        npi_csv_row[f"Other Provider Identifier State_{idx}"] = ""
        npi_csv_row[f"Other Provider Identifier Issuer_{idx}"] = ""
    npi_csv_row["Other Provider Identifier_1"] = "ALT123"
    npi_csv_row["Other Provider Identifier Type Code_1"] = "05"
    npi_csv_row["Other Provider Identifier State_1"] = "TX"
    npi_csv_row["Other Provider Identifier Issuer_1"] = "Issuer"
    return npi_csv_row


def _write_csv(path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _build_nppes_zip(tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_csv(source_dir / "npi_sample.csv", [_minimal_npi_row()])
    _write_csv(
        source_dir / "pl_pfile_sample.csv",
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
        source_dir / "other_sample.csv",
        [
            {
                "NPI": "1215387113",
                "Provider Other Organization Name": "Example DBA",
                "Provider Other Organization Name Type Code": "3",
            }
        ],
    )
    _write_csv(source_dir / "endpoint_sample.csv", [{"NPI": "1215387113"}])

    zip_path = tmp_path / "NPPES_Data_Dissemination_20260301_20260331_V2.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        for file_path in source_dir.iterdir():
            archive.write(file_path, arcname=file_path.name)
    return zip_path


@pytest.mark.asyncio
async def test_process_data_test_mode_imports_nppes_zip(monkeypatch, tmp_path, npi_module):
    """Import a representative NPPES archive in bounded test mode."""

    zip_path = _build_nppes_zip(tmp_path)
    captured_payloads = []

    async def fake_download(_url):
        return '<a href="NPPES_Data_Dissemination_20260301_20260331_V2.zip">current</a>'

    async def fake_download_and_save(_url, target, **_kwargs):
        shutil.copyfile(zip_path, target)

    async def fake_unzip(source, target, **_kwargs):
        with zipfile.ZipFile(source) as archive:
            archive.extractall(target)

    async def fake_save(_ctx, payload):
        captured_payloads.append(payload)

    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_DIR", "https://example.com/")
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_FILE", "feed.html")
    monkeypatch.setattr(npi_module, "download_it", fake_download)
    monkeypatch.setattr(npi_module, "download_it_and_save", fake_download_and_save)
    monkeypatch.setattr(npi_module, "unzip", fake_unzip)
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(npi_module, "_ensure_required_extensions", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_nucc_ready", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_nppes_canonical_ready", AsyncMock())
    monkeypatch.setattr(npi_module, "_load_nucc_taxonomy_int_code_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi_module, "save_npi_data", fake_save)

    ctx = {
        "context": {},
        "redis": SimpleNamespace(enqueue_job=AsyncMock()),
        "import_date": "20260331",
    }

    await npi_module.process_data(ctx, {"test_mode": True})

    assert ctx["context"]["run"] == 1
    assert ctx["context"]["test_mode"] is True

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
