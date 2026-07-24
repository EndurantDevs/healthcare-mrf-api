# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from tests.claims_pricing_contract_fakes import (
    CatalogHttpClient,
    DownloadHttpClient,
    FakeHttpResponse,
)


claims_pricing = importlib.import_module("process.claims_pricing")


def test_scalar_normalizers_cover_import_boundaries():
    assert claims_pricing._safe_int(None, 7) == 7
    assert claims_pricing._safe_int(b"12") == 12
    assert claims_pricing._safe_int("bad", 9) == 9
    assert claims_pricing._to_float("1,234.5") == 1234.5
    assert claims_pricing._to_float("*") is None
    assert claims_pricing._to_float("bad") is None
    assert claims_pricing._to_int("1,234.9") == 1234
    assert claims_pricing._to_int("NA") is None
    assert claims_pricing._to_int(object()) is None


@pytest.mark.parametrize(
    ("raw_npi", "expected_npi"),
    [
        (None, None),
        ("", None),
        ("1000000001.0", 1000000001),
        ("abc", None),
        ("0", None),
        ("10000000000", None),
        ("1000000001", 1000000001),
    ],
)
def test_npi_normalization_enforces_database_range(raw_npi, expected_npi):
    assert claims_pricing._to_npi(raw_npi) == expected_npi


def test_small_collection_helpers_preserve_import_semantics():
    duplicate_rows = [{"key": 1, "name": "first"}, {"key": 1, "name": "last"}]
    assert claims_pricing._dedupe_rows([], ("key",)) == []
    assert claims_pricing._dedupe_rows(duplicate_rows, ("key",)) == [{"key": 1, "name": "last"}]
    assert claims_pricing._chunk_rows([], 2) == []
    assert claims_pricing._chunk_rows([1, 2, 3], 2) == [[1, 2], [3]]
    assert claims_pricing._sum_optional(None, None) is None
    assert claims_pricing._sum_optional(None, 4.0) == 4.0
    assert claims_pricing._provider_name("Example", "A") == "Example, A"
    assert claims_pricing._provider_name(None, "A") == "A"


def test_run_identifiers_are_safe_and_stable(monkeypatch):
    monkeypatch.setattr(claims_pricing.secrets, "token_hex", lambda _size: "feedbeef")
    assert claims_pricing._normalize_run_id(" run/id ") == "run_id"
    assert claims_pricing._normalize_run_id("***").endswith("_feedbeef")
    assert claims_pricing._normalize_run_id(None).endswith("_feedbeef")
    assert claims_pricing._normalize_import_id("day/one") == "day_one"
    assert claims_pricing._normalize_import_id("***") != "***"
    assert claims_pricing._build_stage_suffix("day/one", "run") == claims_pricing._build_stage_suffix(
        "day/one", "run"
    )


def test_csv_distribution_filters_and_tie_breaks():
    dataset_by_field = {
        "title": "synthetic",
        "distribution": [
            {"downloadURL": "", "mediaType": "text/csv"},
            {"downloadURL": "https://example.test/a.json", "mediaType": "application/json"},
            {"downloadURL": "https://example.test/a_DY23.csv", "mediaType": "text/csv", "modified": "1"},
            {"downloadURL": "https://example.test/b_DY23.csv", "format": "csv", "modified": "2"},
            {"downloadURL": "https://example.test/c_DY22.csv.gz", "modified": "9"},
        ],
    }
    candidates = claims_pricing._csv_distributions(dataset_by_field)
    assert [candidate["downloadURL"] for candidate in candidates] == [
        "https://example.test/a_DY23.csv",
        "https://example.test/b_DY23.csv",
        "https://example.test/c_DY22.csv.gz",
    ]
    selected_by_year = claims_pricing._select_csv_distributions_by_year(dataset_by_field, {2022, 2023})
    assert selected_by_year[2023]["downloadURL"].endswith("b_DY23.csv")
    assert claims_pricing._select_csv_distribution_for_test(dataset_by_field)["downloadURL"].endswith(
        "c_DY22.csv.gz"
    )


def test_catalog_selection_reports_missing_contracts():
    empty_dataset_by_field = {"title": "none", "distribution": []}
    with pytest.raises(LookupError, match="No CSV distribution"):
        claims_pricing._select_csv_distribution(empty_dataset_by_field)
    with pytest.raises(LookupError, match="No CSV distribution"):
        claims_pricing._select_csv_distribution_for_test(empty_dataset_by_field)
    with pytest.raises(LookupError, match="CMS dataset not found"):
        claims_pricing._find_dataset({"dataset": []}, "https://example.test/missing")


def test_resolve_sources_rejects_incomplete_year_window(monkeypatch):
    monkeypatch.setattr(claims_pricing, "CLAIMS_YEAR_WINDOW", (2022, 2023))
    monkeypatch.setattr(
        claims_pricing,
        "DATASETS",
        (claims_pricing.DatasetConfig("provider", "https://example.test/provider", 10),),
    )
    catalog_by_field = {
        "dataset": [
            {
                "landingPage": "https://example.test/provider",
                "title": "provider",
                "distribution": [
                    {"downloadURL": "https://example.test/provider_DY23.csv", "mediaType": "text/csv"}
                ],
            }
        ]
    }
    with pytest.raises(LookupError, match=r"years=\[2022\]"):
        claims_pricing._resolve_sources(catalog_by_field)


def test_resolve_sources_test_mode_selects_latest_year(monkeypatch):
    monkeypatch.setattr(claims_pricing, "CLAIMS_YEAR_WINDOW", (2022, 2023))
    monkeypatch.setattr(
        claims_pricing,
        "DATASETS",
        (claims_pricing.DatasetConfig("provider", "https://example.test/provider", 10),),
    )
    catalog_by_field = {
        "dataset": [
            {
                "landingPage": "https://example.test/provider/data/",
                "title": "provider",
                "distribution": [
                    {"downloadURL": f"https://example.test/provider_DY{year % 100}.csv", "format": "csv"}
                    for year in (2022, 2023)
                ],
            }
        ]
    }
    sources_by_dataset = claims_pricing._resolve_sources(catalog_by_field, test_mode=True)
    assert sources_by_dataset == {
        "provider": [
            {
                "url": "https://example.test/provider_DY23.csv",
                "reporting_year": 2023,
                "dataset_title": "provider",
            }
        ]
    }


@pytest.mark.asyncio
async def test_bounded_writer_truncates_at_exact_limit(tmp_path):
    destination = tmp_path / "head.csv"
    response = FakeHttpResponse(chunks=[b"header\n", b"abcdef", b"ignored"])
    await claims_pricing._write_bounded_csv_content(response, str(destination), 10)
    assert destination.read_bytes() == b"header\nabc"


@pytest.mark.asyncio
async def test_bounded_writer_stops_on_empty_chunk(tmp_path):
    destination = tmp_path / "head.csv"
    response = FakeHttpResponse(chunks=[b"one", b"", b"two"])
    await claims_pricing._write_bounded_csv_content(response, str(destination), 100)
    assert destination.read_bytes() == b"one"


@pytest.mark.asyncio
async def test_bounded_writer_accepts_zero_byte_budget(tmp_path):
    destination = tmp_path / "head.csv"
    response = FakeHttpResponse(chunks=[b"header"])
    await claims_pricing._write_bounded_csv_content(response, str(destination), 0)
    assert destination.read_bytes() == b""


@pytest.mark.asyncio
async def test_csv_head_validates_http_before_writing(monkeypatch, tmp_path):
    response = FakeHttpResponse(chunks=[b"a,b\n1,2\n"])
    client = DownloadHttpClient(response)
    monkeypatch.setattr(claims_pricing, "get_http_client", AsyncMock(return_value=client))
    destination = tmp_path / "head.csv"
    await claims_pricing._download_csv_head("https://example.test/file.csv", str(destination), 4)
    assert destination.read_bytes() == b"a,b\n"
    assert response.status_checks == 1
    assert client.requests[0][0] == "https://example.test/file.csv"


@pytest.mark.asyncio
async def test_csv_head_propagates_http_failure(monkeypatch, tmp_path):
    response = FakeHttpResponse(status_error=RuntimeError("status failed"))
    monkeypatch.setattr(
        claims_pricing,
        "get_http_client",
        AsyncMock(return_value=DownloadHttpClient(response)),
    )
    with pytest.raises(RuntimeError, match="status failed"):
        await claims_pricing._download_csv_head("https://example.test/file.csv", str(tmp_path / "x"), 4)


@pytest.mark.asyncio
async def test_download_retries_arq_signal_then_returns(monkeypatch, tmp_path):
    download_attempt = AsyncMock(side_effect=[claims_pricing.Retry(defer=1), None])
    monkeypatch.setattr(claims_pricing, "download_it_and_save", download_attempt)
    monkeypatch.setattr(claims_pricing.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(claims_pricing, "DOWNLOAD_RETRIES", 2)
    downloaded_path = await claims_pricing._download_source_file(
        "provider",
        {"url": "https://example.test/provider.csv"},
        str(tmp_path),
        test_mode=False,
        reporting_year=2023,
    )
    assert downloaded_path.endswith("provider_2023.csv")
    assert download_attempt.await_count == 2


@pytest.mark.asyncio
async def test_download_retries_generic_failure_then_returns(monkeypatch, tmp_path):
    partial_download = AsyncMock(side_effect=[OSError("transient"), None])
    monkeypatch.setattr(claims_pricing, "_download_csv_head", partial_download)
    monkeypatch.setattr(claims_pricing.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(claims_pricing, "DOWNLOAD_RETRIES", 2)
    downloaded_path = await claims_pricing._download_source_file(
        "provider",
        {"url": "https://example.test/provider.csv"},
        str(tmp_path),
        test_mode=True,
    )
    assert downloaded_path.endswith("provider.csv")
    assert partial_download.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "terminal_error",
    [claims_pricing.Retry(defer=1), OSError("unavailable")],
)
async def test_download_propagates_terminal_failure(monkeypatch, tmp_path, terminal_error):
    monkeypatch.setattr(claims_pricing, "download_it_and_save", AsyncMock(side_effect=terminal_error))
    monkeypatch.setattr(claims_pricing, "DOWNLOAD_RETRIES", 1)
    with pytest.raises(type(terminal_error)):
        await claims_pricing._download_source_file(
            "provider",
            {"url": "https://example.test/provider.csv"},
            str(tmp_path),
            test_mode=False,
        )


@pytest.mark.asyncio
async def test_catalog_fetch_returns_decoded_manifest(monkeypatch):
    client = CatalogHttpClient([FakeHttpResponse(text_payload='{"dataset": []}')])
    monkeypatch.setattr(claims_pricing, "get_http_client", AsyncMock(return_value=client))
    assert await claims_pricing._fetch_catalog() == {"dataset": []}
    assert client.requests[0][0] == claims_pricing.CATALOG_URL


@pytest.mark.asyncio
async def test_catalog_fetch_retries_invalid_json(monkeypatch):
    client = CatalogHttpClient(
        [
            FakeHttpResponse(text_payload="{"),
            FakeHttpResponse(text_payload='{"dataset": [1]}'),
        ]
    )
    monkeypatch.setattr(claims_pricing, "get_http_client", AsyncMock(return_value=client))
    monkeypatch.setattr(claims_pricing.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(claims_pricing, "DOWNLOAD_RETRIES", 2)
    assert await claims_pricing._fetch_catalog() == {"dataset": [1]}


@pytest.mark.asyncio
async def test_catalog_fetch_reports_invalid_terminal_payload(monkeypatch):
    client = CatalogHttpClient([FakeHttpResponse(text_payload="{")])
    monkeypatch.setattr(claims_pricing, "get_http_client", AsyncMock(return_value=client))
    monkeypatch.setattr(claims_pricing.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(claims_pricing, "DOWNLOAD_RETRIES", 1)
    with pytest.raises(RuntimeError, match="Invalid CMS catalog payload"):
        await claims_pricing._fetch_catalog()


@pytest.mark.asyncio
async def test_catalog_fetch_propagates_terminal_transport_error(monkeypatch):
    client = CatalogHttpClient([OSError("offline")])
    monkeypatch.setattr(claims_pricing, "get_http_client", AsyncMock(return_value=client))
    monkeypatch.setattr(claims_pricing.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(claims_pricing, "DOWNLOAD_RETRIES", 1)
    with pytest.raises(OSError, match="offline"):
        await claims_pricing._fetch_catalog()


@pytest.mark.asyncio
async def test_generic_split_honors_byte_boundary(monkeypatch, tmp_path):
    source_path = tmp_path / "provider.csv"
    source_path.write_bytes(b"a,b\n1,2\n3,4\n")
    monkeypatch.setattr(claims_pricing, "CLAIMS_CHUNK_TARGET_BYTES", 8)
    chunk_entries = await claims_pricing._split_source_into_chunks(
        "provider",
        str(source_path),
        tmp_path / "chunks",
        test_mode=False,
    )
    assert [entry["chunk_id"] for entry in chunk_entries] == ["provider:0", "provider:1"]
    assert [Path(entry["chunk_path"]).read_bytes() for entry in chunk_entries] == [
        b"a,b\n1,2\n",
        b"a,b\n3,4\n",
    ]
    assert chunk_entries[-1]["accepted_rows"] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("source_bytes", [b"", b"a,b\n"])
async def test_generic_split_returns_no_chunks_without_rows(tmp_path, source_bytes):
    source_path = tmp_path / "provider.csv"
    source_path.write_bytes(source_bytes)
    chunk_entries = await claims_pricing._split_source_into_chunks(
        "provider",
        str(source_path),
        tmp_path / "chunks",
        test_mode=False,
    )
    assert chunk_entries == []


@pytest.mark.asyncio
async def test_generic_split_test_mode_samples_then_stops(monkeypatch, tmp_path):
    source_path = tmp_path / "provider.csv"
    source_lines = [b"a,b\n", *[f"{number},x\n".encode() for number in range(1, 25)]]
    source_path.write_bytes(b"".join(source_lines))
    monkeypatch.setitem(
        claims_pricing.DATASET_BY_KEY,
        "provider",
        claims_pricing.DatasetConfig("provider", "https://example.test/provider", 1),
    )
    chunk_entries = await claims_pricing._split_source_into_chunks(
        "provider",
        str(source_path),
        tmp_path / "chunks",
        test_mode=True,
    )
    assert len(chunk_entries) == 1
    assert Path(chunk_entries[0]["chunk_path"]).read_text().splitlines() == ["a,b", "11,x"]


@pytest.mark.asyncio
async def test_generic_split_delegates_provider_service(monkeypatch, tmp_path):
    delegated_chunks = [{"dataset_key": "provider_service", "chunk_id": "provider_service:0"}]
    split_provider = AsyncMock(return_value=delegated_chunks)
    monkeypatch.setattr(claims_pricing, "_split_provider_service_into_chunks", split_provider)
    assert await claims_pricing._split_source_into_chunks(
        "provider_service",
        str(tmp_path / "source.csv"),
        tmp_path / "chunks",
        test_mode=True,
    ) == delegated_chunks


@pytest.mark.asyncio
async def test_provider_bucket_split_skips_invalid_npi(tmp_path):
    source_path = tmp_path / "provider_service.csv"
    source_path.write_text("Rndrng_NPI,HCPCS_Cd\nbad,99213\n", encoding="utf-8")
    chunk_entries = await claims_pricing._split_provider_service_into_chunks(
        str(source_path),
        tmp_path / "chunks",
        test_mode=False,
    )
    assert chunk_entries == []


@pytest.mark.asyncio
async def test_provider_bucket_split_honors_test_limit(monkeypatch, tmp_path):
    source_path = tmp_path / "provider_service.csv"
    source_lines = ["Rndrng_NPI,HCPCS_Cd\n"]
    source_lines.extend(f"1000000{number:03d},99213\n" for number in range(1, 35))
    source_path.write_text("".join(source_lines), encoding="utf-8")
    monkeypatch.setattr(claims_pricing, "TEST_PROVIDER_SERVICE_ROW_LIMIT", 1)
    chunk_entries = await claims_pricing._split_provider_service_into_chunks(
        str(source_path),
        tmp_path / "chunks",
        test_mode=True,
    )
    assert sum(entry["rows_in_bucket"] for entry in chunk_entries) == 1
    assert chunk_entries[0]["accepted_rows"] == 1


def test_byte_chunk_writer_close_without_open_is_safe(tmp_path):
    chunk_entries = []
    chunk_writer = claims_pricing._ByteChunkWriter("provider", tmp_path, b"h\n", chunk_entries)
    chunk_writer.close_chunk(0, 0)
    assert chunk_entries == []


def test_io_contract_artifacts_are_json_serializable(tmp_path):
    manifest_by_field = {
        "chunks": [{"chunk_path": str(tmp_path / "chunk.csv")}],
        "total_chunks": 1,
    }
    assert json.loads(json.dumps(manifest_by_field))["total_chunks"] == 1
