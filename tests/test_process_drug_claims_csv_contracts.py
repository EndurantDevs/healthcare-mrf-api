# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import csv
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "process" / "drug_claims.py"
MODULE_SPEC = spec_from_file_location("drug_claims_csv_contracts", MODULE_PATH)
drug_claims = module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
sys.modules["drug_claims_csv_contracts"] = drug_claims
MODULE_SPEC.loader.exec_module(drug_claims)


def _write_csv(path: Path, fieldnames: list[str], csv_rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_rows)


def test_provider_bucket_count_is_bounded(tmp_path, monkeypatch):
    source_path = tmp_path / "provider.csv"
    source_path.write_bytes(b"x" * 101)
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_CHUNK_TARGET_BYTES", 25)
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_PROVIDER_DRUG_MAX_BUCKETS", 3)
    assert drug_claims._provider_drug_bucket_count(str(source_path)) == 3

    source_path.write_bytes(b"")
    assert drug_claims._provider_drug_bucket_count(str(source_path)) == 1


@pytest.mark.asyncio
async def test_provider_split_test_mode_manifest_contract(tmp_path, monkeypatch):
    source_path = tmp_path / "provider.csv"
    provider_rows = [
        {
            "Prscrbr_NPI": str(1000000000 + row_number),
            "Gnrc_Name": f"Generic {row_number}",
            "Tot_Clms": str(row_number),
        }
        for row_number in range(1, 23)
    ]
    provider_rows[10]["Prscrbr_NPI"] = "invalid"
    _write_csv(source_path, ["Prscrbr_NPI", "Gnrc_Name", "Tot_Clms"], provider_rows)
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_CHUNK_TARGET_BYTES", 10)
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_PROVIDER_DRUG_MAX_BUCKETS", 4)
    monkeypatch.setattr(drug_claims, "TEST_PROVIDER_DRUG_ROW_LIMIT", 1)

    chunks = await drug_claims._split_provider_drug_into_chunks(
        str(source_path),
        tmp_path / "chunks",
        test_mode=True,
    )

    assert len(chunks) == 1
    assert chunks[0]["parsed_rows"] == 22
    assert chunks[0]["accepted_rows"] == 1
    assert chunks[0]["rows_in_bucket"] == 1
    with open(chunks[0]["chunk_path"], encoding="utf-8") as chunk_file:
        claim_rows = list(csv.DictReader(chunk_file))
    assert claim_rows[0]["Prscrbr_NPI"] == str(1000000022)


@pytest.mark.asyncio
async def test_provider_split_empty_input_is_explicit(tmp_path, capsys):
    source_path = tmp_path / "provider.csv"
    source_path.write_text("Prscrbr_NPI,Gnrc_Name\n", encoding="utf-8")
    chunks = await drug_claims._split_provider_drug_into_chunks(
        str(source_path),
        tmp_path / "chunks",
        test_mode=False,
    )
    assert chunks == []
    assert "no chunks generated for provider_drug" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_spending_split_preserves_header_and_boundaries(tmp_path, monkeypatch):
    source_path = tmp_path / "spending.csv"
    source_path.write_bytes(
        b"Gnrc_Name,Tot_Clms\n"
        b"Generic A,1\n"
        b"Generic B,2\n"
        b"Generic C,3\n"
    )
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_CHUNK_TARGET_BYTES", 25)

    chunks = await drug_claims._split_source_into_chunks(
        "drug_spending",
        str(source_path),
        tmp_path / "chunks",
        test_mode=False,
    )

    assert len(chunks) == 3
    assert [chunk["chunk_index"] for chunk in chunks] == [0, 1, 2]
    assert all(Path(chunk["chunk_path"]).read_bytes().startswith(b"Gnrc_Name") for chunk in chunks)
    assert chunks[-1]["accepted_rows"] == 3


@pytest.mark.asyncio
async def test_spending_split_test_limit_and_empty_contract(tmp_path, monkeypatch, capsys):
    source_path = tmp_path / "spending.csv"
    spending_rows = [
        {"Gnrc_Name": f"Generic {row_number}", "Tot_Clms": str(row_number)}
        for row_number in range(1, 23)
    ]
    _write_csv(source_path, ["Gnrc_Name", "Tot_Clms"], spending_rows)
    monkeypatch.setitem(
        drug_claims.DATASET_BY_KEY,
        "drug_spending",
        drug_claims.DatasetConfig("drug_spending", "unused", 1),
    )
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_CHUNK_TARGET_BYTES", 1000)

    chunks = await drug_claims._split_source_into_chunks(
        "drug_spending",
        str(source_path),
        tmp_path / "limited",
        test_mode=True,
    )
    assert len(chunks) == 1
    assert chunks[0]["parsed_rows"] == 22
    assert chunks[0]["accepted_rows"] == 1

    header_only_path = tmp_path / "header-only.csv"
    header_only_path.write_text("Gnrc_Name,Tot_Clms\n", encoding="utf-8")
    empty_chunks = await drug_claims._split_source_into_chunks(
        "drug_spending",
        str(header_only_path),
        tmp_path / "empty",
        test_mode=False,
    )
    assert empty_chunks == []
    assert "no chunks generated for drug_spending" in capsys.readouterr().out


_PROVIDER_LOADER_FIELDNAMES = [
    "Prscrbr_NPI",
    "Gnrc_Name",
    "Brnd_Name",
    "Prscrbr_Last_Org_Name",
    "Prscrbr_First_Name",
    "Prscrbr_State_Abrvtn",
    "Tot_Clms",
    "Tot_Drug_Cst",
]


def _provider_loader_rows() -> list[dict]:
    return [
        {
            "Prscrbr_NPI": "1000000001",
            "Gnrc_Name": "Generic A",
            "Brnd_Name": "Brand A",
            "Prscrbr_Last_Org_Name": "Practice",
            "Prscrbr_First_Name": "Ava",
            "Prscrbr_State_Abrvtn": "AA",
            "Tot_Clms": "2",
            "Tot_Drug_Cst": "10.5",
        },
        {
            "Prscrbr_NPI": "1000000001",
            "Gnrc_Name": "Generic A",
            "Brnd_Name": "Brand A",
            "Prscrbr_Last_Org_Name": "Practice",
            "Prscrbr_First_Name": "Ava",
            "Prscrbr_State_Abrvtn": "AA",
            "Tot_Clms": "3",
            "Tot_Drug_Cst": "",
        },
        {"Prscrbr_NPI": "invalid", "Gnrc_Name": "Ignored", "Tot_Clms": "9"},
        {
            "Prscrbr_NPI": "1000000002",
            "Gnrc_Name": "",
            "Brnd_Name": "",
            "Tot_Clms": "4",
        },
    ]


@pytest.mark.asyncio
async def test_provider_loader_aggregates_exact_staging_rows(tmp_path, monkeypatch):
    source_path = tmp_path / "provider.csv"
    _write_csv(source_path, _PROVIDER_LOADER_FIELDNAMES, _provider_loader_rows())
    push_batches = AsyncMock()
    monkeypatch.setattr(drug_claims, "_push_objects_with_retry", push_batches)
    monkeypatch.setattr(drug_claims, "ROW_PROGRESS_INTERVAL_SECONDS", 9999)

    await drug_claims._load_provider_drug_rows(
        str(source_path),
        SimpleNamespace(__tablename__="provider_stage"),
        2023,
        test_mode=False,
    )

    push_batches.assert_awaited_once()
    staged_rows = push_batches.await_args.args[0]
    assert len(staged_rows) == 1
    assert staged_rows[0] == _expected_provider_stage_row(
        drug_claims._rx_code_from_names("Generic A", "Brand A")
    )


def _expected_provider_stage_row(rx_code: str) -> dict:
    return {
        "npi": 1000000001,
        "year": 2023,
        "rx_code_system": "HP_RX_CODE",
        "rx_code": rx_code,
        "rx_name": "Generic A",
        "generic_name": "Generic A",
        "brand_name": "Brand A",
        "provider_name": "Practice, Ava",
        "provider_type": None,
        "city": None,
        "state": "AA",
        "zip5": None,
        "country": "US",
        "total_claims": 5.0,
        "total_30day_fills": None,
        "total_day_supply": None,
        "total_drug_cost": 10.5,
        "total_benes": None,
        "ge65_total_claims": None,
        "ge65_total_30day_fills": None,
        "ge65_total_day_supply": None,
        "ge65_total_drug_cost": None,
        "ge65_total_benes": None,
    }


@pytest.mark.asyncio
async def test_provider_loader_test_sampling_contract(tmp_path, monkeypatch):
    source_path = tmp_path / "provider.csv"
    provider_rows = [
        {"Prscrbr_NPI": str(1000000000 + row_number), "Gnrc_Name": "Generic A"}
        for row_number in range(1, 23)
    ]
    _write_csv(source_path, ["Prscrbr_NPI", "Gnrc_Name"], provider_rows)
    push_batches = AsyncMock()
    monkeypatch.setattr(drug_claims, "_push_objects_with_retry", push_batches)
    monkeypatch.setattr(drug_claims, "TEST_PROVIDER_DRUG_ROW_LIMIT", 1)

    await drug_claims._load_provider_drug_rows(
        str(source_path),
        object(),
        2023,
        test_mode=True,
    )
    staged_rows = push_batches.await_args.args[0]
    assert [prescription["npi"] for prescription in staged_rows] == [1000000011]


@pytest.mark.asyncio
async def test_spending_loader_aggregates_and_rewrites(tmp_path, monkeypatch):
    source_path = tmp_path / "spending.csv"
    spending_rows = [
        {
            "Gnrc_Name": "Generic A",
            "Brnd_Name": "Brand A",
            "Tot_Clms": "2",
            "Tot_Spndng": "10",
        },
        {
            "Gnrc_Name": "Generic A",
            "Brnd_Name": "Brand A",
            "Tot_Clms": "3",
            "Tot_Spndng": "4",
        },
        {"Gnrc_Name": "", "Brnd_Name": "", "Tot_Clms": "99"},
    ]
    _write_csv(
        source_path,
        ["Gnrc_Name", "Brnd_Name", "Tot_Clms", "Tot_Spndng"],
        spending_rows,
    )
    push_batches = AsyncMock()
    monkeypatch.setattr(drug_claims, "_push_objects_with_retry", push_batches)

    await drug_claims._load_drug_spending_rows(
        str(source_path),
        SimpleNamespace(__tablename__="prescription_stage"),
        2024,
        test_mode=False,
    )

    staged_rows = push_batches.await_args.args[0]
    assert staged_rows[0]["total_claims"] == 5.0
    assert staged_rows[0]["total_drug_cost"] == 14.0
    assert staged_rows[0]["source_year"] == 2024
    assert push_batches.await_args.kwargs == {"rewrite": True, "use_copy": False}


def test_merge_helpers_fill_only_missing_names():
    existing_prescription_by_field = {
        "rx_name": None,
        "generic_name": "Generic A",
        "brand_name": None,
        "total_claims": None,
        "source_year": 2022,
    }
    new_prescription_by_field = {
        "rx_name": "Display A",
        "generic_name": "Replacement",
        "brand_name": "Brand A",
        "total_claims": 3.0,
        "source_year": 2023,
    }
    drug_claims._merge_prescription_names(
        existing_prescription_by_field,
        new_prescription_by_field,
    )
    drug_claims._merge_prescription_metrics(
        existing_prescription_by_field,
        new_prescription_by_field,
        ("total_claims",),
    )
    assert existing_prescription_by_field["rx_name"] == "Display A"
    assert existing_prescription_by_field["generic_name"] == "Generic A"
    assert existing_prescription_by_field["brand_name"] == "Brand A"
    assert existing_prescription_by_field["total_claims"] == 3.0


def test_row_normalizers_reject_missing_drug_names():
    assert (
        drug_claims._provider_prescription_from_row(
            {"Gnrc_Name": "", "Brnd_Name": ""},
            1000000001,
            2023,
        )
        is None
    )
    assert drug_claims._spending_prescription_from_row({}, 2023) is None
