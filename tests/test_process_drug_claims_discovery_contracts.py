# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "process" / "drug_claims.py"
MODULE_SPEC = spec_from_file_location("drug_claims_discovery_contracts", MODULE_PATH)
drug_claims = module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
sys.modules["drug_claims_discovery_contracts"] = drug_claims
MODULE_SPEC.loader.exec_module(drug_claims)


def _synthetic_catalog() -> dict:
    return {
        "dataset": [
            {
                "landingPage": "https://catalog.invalid/provider/data",
                "title": "Synthetic provider claims",
                "distribution": [
                    {
                        "downloadURL": "https://files.invalid/provider_DY23.csv",
                        "mediaType": "text/csv",
                        "modified": "2024-01-01",
                    },
                    {
                        "downloadURL": "https://files.invalid/provider_DY23_new.csv",
                        "format": "CSV",
                        "modified": "2024-02-01",
                    },
                    {
                        "downloadURL": "https://files.invalid/provider_DY22.csv",
                        "mediaType": "text/csv",
                    },
                    {
                        "downloadURL": "https://files.invalid/provider_DY23.json",
                        "mediaType": "application/json",
                    },
                    {"mediaType": "text/csv"},
                ],
            },
            {
                "landingPage": "https://catalog.invalid/spending",
                "title": "Synthetic drug spending",
                "distribution": [
                    {
                        "downloadURL": "https://files.invalid/spending_D23.csv.gz",
                        "issued": "2024-01-01",
                    },
                    {
                        "downloadURL": "https://files.invalid/spending_D22.csv",
                        "mediaType": "text/csv",
                    },
                ],
            },
        ]
    }


def test_distribution_selection_and_test_mode_contract(monkeypatch):
    dataset_configs = (
        drug_claims.DatasetConfig("provider_drug", "https://catalog.invalid/provider", 10),
        drug_claims.DatasetConfig("drug_spending", "https://catalog.invalid/spending", 5),
    )
    monkeypatch.setattr(drug_claims, "DATASETS", dataset_configs)
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_YEAR_WINDOW", (2022, 2023))

    production_sources = drug_claims._resolve_sources(_synthetic_catalog())
    test_sources = drug_claims._resolve_sources(_synthetic_catalog(), test_mode=True)

    assert [entry["reporting_year"] for entry in production_sources["provider_drug"]] == [
        2022,
        2023,
    ]
    assert production_sources["provider_drug"][1]["url"].endswith("provider_DY23_new.csv")
    assert test_sources == {
        "provider_drug": [
            {
                "url": "https://files.invalid/provider_DY23_new.csv",
                "reporting_year": 2023,
                "dataset_title": "Synthetic provider claims",
            }
        ],
        "drug_spending": [
            {
                "url": "https://files.invalid/spending_D23.csv.gz",
                "reporting_year": 2023,
                "dataset_title": "Synthetic drug spending",
            }
        ],
    }


def test_source_resolution_reports_missing_contracts(monkeypatch):
    dataset_configs = (
        drug_claims.DatasetConfig("provider_drug", "https://catalog.invalid/provider", 10),
    )
    monkeypatch.setattr(drug_claims, "DATASETS", dataset_configs)
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_YEAR_WINDOW", (2021, 2023))

    with pytest.raises(LookupError, match=r"years=\[2021\]"):
        drug_claims._resolve_sources(_synthetic_catalog())
    with pytest.raises(LookupError, match="dataset not found"):
        drug_claims._find_dataset(_synthetic_catalog(), "https://catalog.invalid/missing")

    monkeypatch.setattr(
        drug_claims,
        "_select_csv_distributions_by_year",
        lambda dataset, years: {2021: {}},
    )
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_YEAR_WINDOW", (2021,))
    with pytest.raises(LookupError, match="Missing downloadURL"):
        drug_claims._resolve_sources(_synthetic_catalog())


@pytest.mark.parametrize(
    ("download_url", "reporting_year"),
    [
        ("https://files.invalid/claims_DY19.csv", 2019),
        ("https://files.invalid/claims_D24.csv", 2024),
        ("https://files.invalid/claims.csv", -1),
    ],
)
def test_reporting_year_contract(download_url, reporting_year):
    assert drug_claims._extract_reporting_year(download_url) == reporting_year


def test_distribution_filter_accepts_supported_csv_shapes():
    dataset_by_field = {
        "distribution": [
            {"downloadURL": "https://files.invalid/a", "mediaType": "TEXT/CSV"},
            {"downloadURL": "https://files.invalid/b", "format": "csv"},
            {"downloadURL": "https://files.invalid/c.csv"},
            {"downloadURL": "https://files.invalid/d.csv.gz"},
            {"downloadURL": "https://files.invalid/e.json", "format": "json"},
            {"mediaType": "text/csv"},
        ]
    }
    csv_distributions = drug_claims._csv_distributions(dataset_by_field)
    assert [entry["downloadURL"] for entry in csv_distributions] == [
        "https://files.invalid/a",
        "https://files.invalid/b",
        "https://files.invalid/c.csv",
        "https://files.invalid/d.csv.gz",
    ]
    assert drug_claims._parse_modified({"issued": "2024-01-01"}) == "2024-01-01"
    assert drug_claims._parse_modified({}) == ""


class _CatalogClient:
    def __init__(self, response_text):
        self.response_text = response_text
        self.requests = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def get(self, url, timeout):
        self.requests.append((url, timeout))
        return SimpleNamespace(text=AsyncMock(return_value=self.response_text))


@pytest.mark.asyncio
async def test_catalog_fetch_retries_invalid_json(monkeypatch):
    invalid_client = _CatalogClient("not-json")
    valid_client = _CatalogClient(json.dumps({"dataset": []}))
    clients = iter([invalid_client, valid_client])
    sleep = AsyncMock()
    monkeypatch.setattr(
        drug_claims,
        "get_http_client",
        AsyncMock(side_effect=lambda **kwargs: next(clients)),
    )
    monkeypatch.setattr(drug_claims.asyncio, "sleep", sleep)
    monkeypatch.setattr(drug_claims, "DOWNLOAD_RETRIES", 2)

    assert await drug_claims._fetch_catalog() == {"dataset": []}
    sleep.assert_awaited_once_with(3)


@pytest.mark.asyncio
async def test_catalog_fetch_surfaces_transport_error(monkeypatch):
    transport_error = RuntimeError("catalog offline")
    monkeypatch.setattr(
        drug_claims,
        "get_http_client",
        AsyncMock(side_effect=transport_error),
    )
    monkeypatch.setattr(drug_claims.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(drug_claims, "DOWNLOAD_RETRIES", 1)

    with pytest.raises(RuntimeError, match="catalog offline"):
        await drug_claims._fetch_catalog()


class _ResponseContent:
    def __init__(self, chunks):
        self.chunks = chunks

    async def iter_chunked(self, chunk_size):
        for response_chunk in self.chunks:
            yield response_chunk


@pytest.mark.asyncio
async def test_response_head_stops_at_exact_byte_limit(tmp_path):
    response = SimpleNamespace(content=_ResponseContent([b"abc", b"", b"ignored"]))
    empty_path = tmp_path / "empty.csv"
    await drug_claims._write_response_head(response, str(empty_path), 10)
    assert empty_path.read_bytes() == b"abc"

    response = SimpleNamespace(content=_ResponseContent([b"abcd", b"efgh"]))
    bounded_path = tmp_path / "bounded.csv"
    await drug_claims._write_response_head(response, str(bounded_path), 6)
    assert bounded_path.read_bytes() == b"abcdef"


@pytest.mark.asyncio
async def test_download_retries_and_preserves_mode_contract(tmp_path, monkeypatch):
    retry_error = drug_claims.Retry(defer=1)
    production_download = AsyncMock(side_effect=[retry_error, None])
    sleep = AsyncMock()
    monkeypatch.setattr(drug_claims, "download_it_and_save", production_download)
    monkeypatch.setattr(drug_claims.asyncio, "sleep", sleep)
    monkeypatch.setattr(drug_claims, "DOWNLOAD_RETRIES", 2)
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_PREFER_STREAM_DOWNLOAD", False)

    downloaded_path = await drug_claims._download_source_file(
        "drug_spending",
        {"url": "https://files.invalid/spending.csv"},
        str(tmp_path),
        test_mode=False,
        reporting_year=2023,
    )
    assert downloaded_path.endswith("drug_spending_2023.csv")
    assert production_download.await_args.kwargs == {"prefer_stream": False}
    sleep.assert_awaited_once_with(5)

    test_download = AsyncMock()
    monkeypatch.setattr(drug_claims, "_download_csv_head", test_download)
    await drug_claims._download_source_file(
        "provider_drug",
        {"url": "https://files.invalid/provider.csv"},
        str(tmp_path),
        test_mode=True,
    )
    test_download.assert_awaited_once()


@pytest.mark.asyncio
async def test_download_surfaces_final_generic_error(tmp_path, monkeypatch):
    monkeypatch.setattr(
        drug_claims,
        "download_it_and_save",
        AsyncMock(side_effect=RuntimeError("download failed")),
    )
    monkeypatch.setattr(drug_claims.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(drug_claims, "DOWNLOAD_RETRIES", 1)

    with pytest.raises(RuntimeError, match="download failed"):
        await drug_claims._download_source_file(
            "drug_spending",
            {"url": "https://files.invalid/spending.csv"},
            str(tmp_path),
            test_mode=False,
        )


@pytest.mark.parametrize(
    ("raw_npi", "normalized_npi"),
    [
        (None, None),
        ("", None),
        ("1234567890.0", 1234567890),
        ("12x", None),
        ("0", None),
        ("10000000000", None),
        ("1,234", 1234),
    ],
)
def test_npi_normalization_contract(raw_npi, normalized_npi):
    assert drug_claims._to_npi(raw_npi) == normalized_npi


def test_row_and_api_normalization_contract(monkeypatch):
    assert drug_claims._to_float(None) is None
    assert drug_claims._to_float("1,234.5") == 1234.5
    assert drug_claims._to_float("bad") is None
    assert drug_claims._row_value({"A": 1}, "B", "A") == 1
    assert drug_claims._row_value({"A": 1}, "B") is None
    assert drug_claims._provider_name("Practice", "Ava") == "Practice, Ava"
    assert drug_claims._provider_name("Practice", None) == "Practice"
    assert drug_claims._provider_name(None, "Ava") == "Ava"
    monkeypatch.setattr(drug_claims, "RX_CROSSWALK_LIVE_BASE_URL", "http://drug.invalid/api/v1/drug")
    assert drug_claims._drug_api_url("/name") == "http://drug.invalid/api/v1/drug/name"
    monkeypatch.setattr(drug_claims, "RX_CROSSWALK_LIVE_BASE_URL", "http://drug.invalid/api/v1")
    assert drug_claims._drug_api_url("/name") == "http://drug.invalid/api/v1/drug/name"
    monkeypatch.setattr(drug_claims, "RX_CROSSWALK_LIVE_BASE_URL", "http://drug.invalid")
    assert drug_claims._drug_api_url("/name") == "http://drug.invalid/api/v1/drug/name"


def test_product_ndc_payload_contract():
    assert drug_claims._extract_product_ndcs(None) == []
    assert drug_claims._extract_product_ndcs({"generic": "invalid"}) == []
    assert drug_claims._extract_product_ndcs(
        {
            "generic": [None, {"product_ndc": ""}, {"product_ndc": "0001-0001"}],
            "brand": [{"product_ndc": "0001-0001"}, {"product_ndc": "0002-0002"}],
        }
    ) == ["0001-0001", "0002-0002"]
