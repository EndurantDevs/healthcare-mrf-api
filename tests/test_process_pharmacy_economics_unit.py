# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
from pathlib import Path

import pytest


class _CatalogResponse:
    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def json(self, **_kwargs):
        return self.payload


class _CatalogClient:
    def __init__(self, payload):
        self.payload = payload

    def get(self, *_args, **_kwargs):
        return _CatalogResponse(self.payload)


@pytest.fixture
def econ_module():
    return importlib.import_module("process.pharmacy_economics")


@pytest.mark.asyncio
async def test_source_catalog_selects_latest_reviewed_csvs(monkeypatch, econ_module):
    for variable_name in (
        "HLTHPRT_PHARMACY_ECON_SDUD_URL",
        "HLTHPRT_PHARMACY_ECON_NADAC_URL",
        "HLTHPRT_PHARMACY_ECON_FUL_URL",
    ):
        monkeypatch.delenv(variable_name, raising=False)
    catalog_payload_by_key = {
        "dataset": [
            {
                "title": "Unrelated dataset",
                "distribution": [{"downloadURL": "https://example.invalid/ignore.csv"}],
            },
            {
                "title": "State Drug Utilization Data 2023",
                "distribution": [
                    {"downloadURL": "https://example.invalid/readme.json"},
                    {"accessURL": "https://example.invalid/sdud-2023.csv"},
                ],
            },
            {
                "title": "State Drug Utilization Data 2025",
                "distribution": [{"downloadURL": "https://example.invalid/sdud-2025.csv"}],
            },
            {
                "title": "NADAC (National Average Drug Acquisition Cost) 2024",
                "distribution": [{"downloadURL": "https://example.invalid/nadac-2024.csv"}],
            },
            {
                "title": "NADAC (National Average Drug Acquisition Cost) 2026",
                "distribution": [{"accessURL": "https://example.invalid/nadac-2026.csv"}],
            },
            {
                "title": "ACA Federal Upper Limits",
                "distribution": [
                    {"downloadURL": "https://example.invalid/ful.json"},
                    {"accessURL": "https://example.invalid/ful.csv"},
                ],
            },
        ]
    }

    resolved_source_urls = await econ_module._resolve_source_urls(
        _CatalogClient(catalog_payload_by_key)
    )

    assert resolved_source_urls == (
        "https://example.invalid/sdud-2025.csv",
        "https://example.invalid/nadac-2026.csv",
        "https://example.invalid/ful.csv",
    )
    assert econ_module._extract_trailing_year("other 2026", "expected ") is None
    assert econ_module._extract_trailing_year("expected current", "expected ") is None
    assert econ_module._extract_trailing_year("expected 2026", "expected ") == 2026


@pytest.mark.asyncio
async def test_source_catalog_fallbacks_overrides_and_missing_data(
    monkeypatch, econ_module
):
    merged_catalog_payload_by_key = {
        "dataset": [
            {
                "title": "SDUD",
                "distribution": [{"downloadURL": "https://example.invalid/sdud.csv"}],
            },
            {
                "title": "NADAC (National Average Drug Acquisition Cost) 2025",
                "distribution": [{"downloadURL": "https://example.invalid/nadac.csv"}],
            },
            {
                "title": "ACA Federal Upper Limits",
                "distribution": [{"downloadURL": "https://example.invalid/ful.csv"}],
            },
        ]
    }
    resolved_source_urls = await econ_module._resolve_source_urls(
        _CatalogClient(merged_catalog_payload_by_key)
    )
    assert resolved_source_urls[0] == "https://example.invalid/sdud.csv"

    monkeypatch.setenv(
        "HLTHPRT_PHARMACY_ECON_SDUD_URL",
        " https://override.invalid/sdud.csv ",
    )
    monkeypatch.setenv(
        "HLTHPRT_PHARMACY_ECON_NADAC_URL",
        "https://override.invalid/nadac.csv",
    )
    monkeypatch.setenv(
        "HLTHPRT_PHARMACY_ECON_FUL_URL",
        "https://override.invalid/ful.csv",
    )
    assert await econ_module._resolve_source_urls(_CatalogClient({})) == (
        "https://override.invalid/sdud.csv",
        "https://override.invalid/nadac.csv",
        "https://override.invalid/ful.csv",
    )

    for variable_name in (
        "HLTHPRT_PHARMACY_ECON_SDUD_URL",
        "HLTHPRT_PHARMACY_ECON_NADAC_URL",
        "HLTHPRT_PHARMACY_ECON_FUL_URL",
    ):
        monkeypatch.delenv(variable_name)
    with pytest.raises(ValueError, match="Could not resolve"):
        await econ_module._resolve_source_urls(_CatalogClient({}))


def test_source_value_parsing_and_database_identifiers(monkeypatch, econ_module):
    monkeypatch.setattr(
        econ_module.datetime,
        "datetime",
        type(
            "_FixedDateTime",
            (),
            {"now": staticmethod(lambda: type("_Now", (), {"strftime": lambda _self, _fmt: "20260727"})())},
        ),
    )
    assert econ_module._normalize_import_id(" import-2026/07 ") == "import202607"
    assert econ_module._normalize_import_id("---") == "20260727"
    assert econ_module._parse_int("1,234.0") == 1234
    assert econ_module._parse_int("invalid") == 0
    assert econ_module._parse_float("$1,234.50") == 1234.5
    assert econ_module._parse_float("invalid") is None
    assert econ_module._validate_schema_name("_reviewed_2026") == "_reviewed_2026"
    for schema_name in ("", "9invalid", "invalid-name"):
        with pytest.raises(ValueError, match="Invalid schema name"):
            econ_module._validate_schema_name(schema_name)
    short_name = econ_module._archived_identifier("profile_idx")
    long_name = econ_module._archived_identifier("profile_" + ("x" * 80))
    assert short_name == "profile_idx_old"
    assert len(long_name) <= econ_module.POSTGRES_IDENTIFIER_MAX_LENGTH
    assert long_name.endswith("_old")


@pytest.mark.asyncio
async def test_fetch_sdud_uses_prescription_counts_only(monkeypatch, econ_module):
    csv_payload = (
        "state,ndc,product_name,number_of_prescriptions,total_amount_reimbursed\n"
        "IL,12345-6789-01,Drug A,150,99999\n"
        "IL,12345-6789-02,Drug B,,120000\n"
        "XX,12345-6789-03,Drug C,111,11111\n"
    )

    async def _fake_download(_client, _url, tmp_dir, _file_name):
        path = Path(tmp_dir) / "sdud.csv"
        path.write_text(csv_payload, encoding="utf-8")
        return str(path)

    monkeypatch.setattr(econ_module, "_download_to_temp_csv", _fake_download)

    result = await econ_module._fetch_sdud(client=None, sdud_url="https://example.invalid/sdud.csv")
    assert "IL" in result
    assert "12345678901" in result["IL"]
    assert result["IL"]["12345678901"]["volume"] == 150
    assert "12345678902" not in result["IL"]
    assert "XX" not in result


def test_pharmacy_economics_rows_preserve_margin_inputs(econ_module):
    updated_at = econ_module.datetime.datetime(2026, 8, 11, 12, 30)
    summary_rows = list(
        econ_module._pharmacy_economics_rows(
            {
                "UT": {
                    "12345678901": {"name": "Example A", "volume": 12},
                    "12345678902": {"name": "Example B", "volume": 3},
                }
            },
            {"12345678901": 2.0},
            {"12345678901": 1.5},
            updated_at,
        )
    )

    assert summary_rows == [
        {
            "state": "UT",
            "ndc11": "12345678901",
            "drug_name": "Example A",
            "sdud_volume": 12,
            "nadac_per_unit": 2.0,
            "ful_per_unit": 1.5,
            "medicaid_dispensing_fee": econ_module.STATE_DISPENSING_FEES["UT"],
            "estimated_gross_margin": -4.77,
            "updated_at": updated_at,
        }
    ]
