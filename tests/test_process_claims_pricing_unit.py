# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os
import sys
import tempfile
import csv
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "process" / "claims_pricing.py"
MODULE_SPEC = spec_from_file_location("claims_pricing_unit", MODULE_PATH)
claims_pricing = module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
sys.modules["claims_pricing_unit"] = claims_pricing
MODULE_SPEC.loader.exec_module(claims_pricing)

_find_dataset = claims_pricing._find_dataset
_resolve_sources = claims_pricing._resolve_sources
_row_allowed_for_test = claims_pricing._row_allowed_for_test
_row_value = claims_pricing._row_value
_normalize_state = claims_pricing._normalize_state
_normalize_zip5 = claims_pricing._normalize_zip5
_normalize_state_fips = claims_pricing._normalize_state_fips
_normalize_service_code = claims_pricing._normalize_service_code
_env_bool = claims_pricing._env_bool
_select_csv_distribution = claims_pricing._select_csv_distribution
_download_sources = claims_pricing._download_sources
_download_source_file = claims_pricing._download_source_file
_split_source_into_chunks = claims_pricing._split_source_into_chunks
_push_objects_with_retry = claims_pricing._push_objects_with_retry
CLAIMS_YEAR_WINDOW = claims_pricing.CLAIMS_YEAR_WINDOW


def test_find_dataset_normalizes_landing_page():
    catalog = {
        "dataset": [
            {
                "landingPage": "https://data.cms.gov/x/y/data",
                "title": "dataset-1",
                "distribution": [],
            }
        ]
    }

    result = _find_dataset(catalog, "https://data.cms.gov/x/y")
    assert result["title"] == "dataset-1"


def test_select_csv_distribution_prefers_latest_reporting_year():
    dataset = {
        "title": "demo",
        "distribution": [
            {"downloadURL": "https://example.com/file_DY21.csv", "mediaType": "text/csv", "modified": "2025-01-01"},
            {"downloadURL": "https://example.com/file_DY23.csv", "mediaType": "text/csv", "modified": "2024-01-01"},
            {"downloadURL": "https://example.com/file_DY22.csv", "mediaType": "text/csv", "modified": "2026-01-01"},
        ],
    }

    selected = _select_csv_distribution(dataset)
    assert selected["downloadURL"].endswith("DY23.csv")


def test_resolve_sources_extracts_urls_and_years():
    catalog = {
        "dataset": [
            {
                "landingPage": "https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider",
                "title": "providers",
                "distribution": [
                    {"downloadURL": "https://example.com/providers_DY21.csv", "mediaType": "text/csv"},
                    {"downloadURL": "https://example.com/providers_DY22.csv", "mediaType": "text/csv"},
                    {"downloadURL": "https://example.com/providers_DY23.csv", "mediaType": "text/csv"},
                ],
            },
            {
                "landingPage": "https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service",
                "title": "provider_service",
                "distribution": [
                    {"downloadURL": "https://example.com/provider_service_DY21.csv", "mediaType": "text/csv"},
                    {"downloadURL": "https://example.com/provider_service_DY22.csv", "mediaType": "text/csv"},
                    {"downloadURL": "https://example.com/provider_service_DY23.csv", "mediaType": "text/csv"},
                ],
            },
            {
                "landingPage": "https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-geography-and-service",
                "title": "geo_service",
                "distribution": [
                    {"downloadURL": "https://example.com/geo_service_DY21.csv", "mediaType": "text/csv"},
                    {"downloadURL": "https://example.com/geo_service_DY22.csv", "mediaType": "text/csv"},
                    {"downloadURL": "https://example.com/geo_service_DY23.csv", "mediaType": "text/csv"},
                ],
            },
        ]
    }

    resolved = _resolve_sources(catalog)
    assert [entry["reporting_year"] for entry in resolved["provider"]] == list(CLAIMS_YEAR_WINDOW)
    assert resolved["provider_service"][0]["url"].startswith("https://example.com/")
    assert resolved["geo_service"][-1]["dataset_title"] == "geo_service"


def test_row_allowed_for_test_is_deterministic_sparse_sample():
    hits = [idx for idx in range(1, 50) if _row_allowed_for_test(idx)]
    assert hits == [11, 22, 33, 44]


def test_row_value_supports_multiple_header_variants():
    row = {"Prscrbr_NPI": "12345"}
    assert _row_value(row, "PRSCRBR_NPI", "Prscrbr_NPI") == "12345"


def test_normalize_state_and_zip5_helpers():
    assert _normalize_state("in") == "IN"
    assert _normalize_state(" IN ") == "IN"
    assert _normalize_state("Jill") is None
    assert _normalize_state(None) is None

    assert _normalize_zip5("47501") == "47501"
    assert _normalize_zip5("47501-1234") == "47501"
    assert _normalize_zip5("FNP-C") is None
    assert _normalize_zip5(None) is None

    assert _normalize_state_fips("18") == "18"
    assert _normalize_state_fips("018") == "18"
    assert _normalize_state_fips("18.0") == "18"
    assert _normalize_state_fips("US-18") == "18"
    assert _normalize_state_fips("12345") is None
    assert _normalize_state_fips(None) is None


def test_normalize_service_code_filters_invalid_and_recovers_embedded_code():
    assert _normalize_service_code("99213") == "99213"
    assert _normalize_service_code(" j1030 ") == "J1030"
    assert _normalize_service_code("0124A") == "0124A"
    assert _normalize_service_code("0241u") == "0241U"
    assert _normalize_service_code("Code: J1030 (Injection)") == "J1030"
    assert _normalize_service_code("ESTABLISHED PATIENT OFFICE OR OTHER OUTPATIENT VISIT") is None
    assert _normalize_service_code("Y") is None
    assert _normalize_service_code(None) is None


def test_env_bool_parsing(monkeypatch):
    monkeypatch.delenv("HLTHPRT_FAKE_BOOL", raising=False)
    assert _env_bool("HLTHPRT_FAKE_BOOL", default=True) is True
    monkeypatch.setenv("HLTHPRT_FAKE_BOOL", "false")
    assert _env_bool("HLTHPRT_FAKE_BOOL", default=True) is False
    monkeypatch.setenv("HLTHPRT_FAKE_BOOL", "YES")
    assert _env_bool("HLTHPRT_FAKE_BOOL", default=False) is True


@pytest.mark.asyncio
async def test_download_sources_uses_partial_download_in_test_mode(monkeypatch):
    calls = {"head": 0, "full": 0}

    async def fake_head(url, path, max_bytes):
        calls["head"] += 1
        assert max_bytes == claims_pricing.TEST_MAX_DOWNLOAD_BYTES
        with open(path, "wb") as handle:
            handle.write(b"header1,header2\n")

    async def fake_full(url, path):
        calls["full"] += 1
        raise AssertionError("download_it_and_save must not be called in --test mode")

    monkeypatch.setattr(claims_pricing, "_download_csv_head", fake_head)
    monkeypatch.setattr(claims_pricing, "download_it_and_save", fake_full)

    sources = {"provider": {"url": "https://example.com/providers.csv"}}
    with tempfile.TemporaryDirectory() as tmpdir:
        result = await _download_sources(sources, tmpdir, test_mode=True)
        assert os.path.exists(result["provider"])
    assert calls["head"] == 1
    assert calls["full"] == 0


@pytest.mark.asyncio
async def test_download_sources_uses_full_download_in_non_test_mode(monkeypatch):
    calls = {"head": 0, "full": 0}

    async def fake_head(url, path, max_bytes):
        calls["head"] += 1
        raise AssertionError("_download_csv_head must not be called in non-test mode")

    async def fake_full(url, path):
        calls["full"] += 1
        with open(path, "wb") as handle:
            handle.write(b"header1,header2\n")

    monkeypatch.setattr(claims_pricing, "_download_csv_head", fake_head)
    monkeypatch.setattr(claims_pricing, "download_it_and_save", fake_full)

    sources = {"provider": {"url": "https://example.com/providers.csv"}}
    with tempfile.TemporaryDirectory() as tmpdir:
        result = await _download_sources(sources, tmpdir, test_mode=False)
        assert os.path.exists(result["provider"])
    assert calls["head"] == 0
    assert calls["full"] == 1


@pytest.mark.asyncio
async def test_download_source_file_creates_missing_directory(monkeypatch):
    calls = {"full": 0}

    async def fake_full(url, path):
        calls["full"] += 1
        with open(path, "wb") as handle:
            handle.write(b"col\n")

    monkeypatch.setattr(claims_pricing, "download_it_and_save", fake_full)
    with tempfile.TemporaryDirectory() as tmpdir:
        nested_dir = Path(tmpdir) / "a" / "b" / "c"
        output_path = await _download_source_file(
            "provider",
            {"url": "https://example.com/providers.csv"},
            str(nested_dir),
            test_mode=False,
        )
        assert Path(output_path).exists()
    assert calls["full"] == 1


@pytest.mark.asyncio
async def test_main_enqueues_claims_start(monkeypatch):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(claims_pricing, "create_pool", AsyncMock(return_value=fake_pool))

    result = await claims_pricing.main(test_mode=True, import_id="dev1")

    assert result["ok"] is True
    assert result["queued"] is True
    fake_pool.enqueue_job.assert_awaited_once()
    enqueue_call = fake_pool.enqueue_job.await_args
    assert enqueue_call.args[0] == "claims_pricing_start"
    assert enqueue_call.kwargs["_queue_name"] == claims_pricing.CLAIMS_QUEUE_NAME
    assert enqueue_call.kwargs["_job_id"].startswith("claims_start_")
    assert enqueue_call.args[1]["test_mode"] is True
    assert enqueue_call.args[1]["import_id"] == "dev1"


@pytest.mark.asyncio
async def test_finish_main_enqueues_finalize(monkeypatch):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(claims_pricing, "create_pool", AsyncMock(return_value=fake_pool))

    result = await claims_pricing.finish_main(import_id="dev1", run_id="run_a", test_mode=True)

    assert result["ok"] is True
    assert result["queued"] is True
    assert result["run_id"] == "run_a"
    assert result["import_id"] == "dev1"
    assert result["stage_suffix"]
    fake_pool.enqueue_job.assert_awaited_once()
    enqueue_call = fake_pool.enqueue_job.await_args
    assert enqueue_call.args[0] == "claims_pricing_finalize"
    assert enqueue_call.kwargs["_queue_name"] == claims_pricing.CLAIMS_FINISH_QUEUE_NAME
    assert enqueue_call.kwargs["_job_id"] == "claims_finalize_run_a"
    assert enqueue_call.args[1]["test_mode"] is True


@pytest.mark.asyncio
async def test_split_provider_service_chunks_partition_by_npi(monkeypatch):
    monkeypatch.setattr(claims_pricing, "CLAIMS_CHUNK_TARGET_BYTES", 64)
    monkeypatch.setattr(claims_pricing, "CLAIMS_PROVIDER_DRUG_MAX_BUCKETS", 8)

    with tempfile.TemporaryDirectory() as tmpdir:
        source_path = Path(tmpdir) / "provider_service.csv"
        chunks_dir = Path(tmpdir) / "chunks"
        with open(source_path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["Rndrng_NPI", "HCPCS_Cd", "HCPCS_Desc", "Rndrng_Prvdr_City", "Rndrng_Prvdr_State_Abrvtn"],
            )
            writer.writeheader()
            writer.writerow({"Rndrng_NPI": "1000000001", "HCPCS_Cd": "99213", "HCPCS_Desc": "A", "Rndrng_Prvdr_City": "X", "Rndrng_Prvdr_State_Abrvtn": "CA"})
            writer.writerow({"Rndrng_NPI": "1000000002", "HCPCS_Cd": "99214", "HCPCS_Desc": "B", "Rndrng_Prvdr_City": "X", "Rndrng_Prvdr_State_Abrvtn": "CA"})
            writer.writerow({"Rndrng_NPI": "1000000001", "HCPCS_Cd": "99215", "HCPCS_Desc": "C", "Rndrng_Prvdr_City": "Y", "Rndrng_Prvdr_State_Abrvtn": "CA"})
            writer.writerow({"Rndrng_NPI": "1000000003", "HCPCS_Cd": "G0008", "HCPCS_Desc": "D", "Rndrng_Prvdr_City": "Z", "Rndrng_Prvdr_State_Abrvtn": "NY"})

        chunks = await _split_source_into_chunks(
            dataset_key="provider_service",
            source_path=str(source_path),
            chunks_dir=chunks_dir,
            test_mode=False,
        )
        assert chunks

        npi_to_chunk = {}
        for chunk in chunks:
            with open(chunk["chunk_path"], "r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    npi = row["Rndrng_NPI"]
                    existing = npi_to_chunk.get(npi)
                    if existing is None:
                        npi_to_chunk[npi] = chunk["chunk_path"]
                    else:
                        assert existing == chunk["chunk_path"]

        assert "1000000001" in npi_to_chunk
        assert "1000000002" in npi_to_chunk
        assert "1000000003" in npi_to_chunk


@pytest.mark.asyncio
async def test_push_objects_with_retry_retries_deadlock(monkeypatch):
    attempts = {"count": 0}

    async def fake_push(*_args, **_kwargs):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("deadlock detected")
        return None

    monkeypatch.setattr(claims_pricing, "push_objects", fake_push)
    monkeypatch.setattr(claims_pricing, "CLAIMS_DB_DEADLOCK_RETRIES", 4)
    monkeypatch.setattr(claims_pricing.asyncio, "sleep", AsyncMock())

    await _push_objects_with_retry([{"provider_key": 1}], SimpleNamespace(__tablename__="pricing_provider"))
    assert attempts["count"] == 3
