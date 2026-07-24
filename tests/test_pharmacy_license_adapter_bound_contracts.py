"""Explicit row and page bounds for pharmacy-license adapters."""

import importlib
import io
import json
import zipfile
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


pharmacy_license = importlib.import_module("process.pharmacy_license")


def _state(state_code: str = "TX") -> pharmacy_license.StateSource:
    return pharmacy_license.StateSource(
        state_code=state_code,
        state_name={"TX": "Texas", "FL": "Florida", "CO": "Colorado"}.get(
            state_code,
            "Sample State",
        ),
        board_url=f"https://licenses.example.test/{state_code.lower()}",
    )


@pytest.mark.asyncio
async def test_socrata_adapter_marks_short_page_page_limit_and_wa_mapping(monkeypatch):
    wa_payload = (
        b"credentialnumber,lastname,firstname,state\n"
        b"WA-1,Pharmacy,Sample,WA\n"
    )
    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=("https://data.example.test/licenses.csv", "text/csv", wa_payload)),
    )
    license_rows, _source_url, metadata, error = await pharmacy_license._load_rows_from_socrata_source(
        object(),
        _state("WA"),
        "https://data.example.test/licenses.csv",
        select_columns=("credentialnumber",),
        where_clause="true",
        page_size=2,
    )
    assert error is None
    assert license_rows[0]["License Number"] == "WA-1"
    assert metadata["pages_fetched"] == 1
    assert "page_limit_reached" not in metadata

    monkeypatch.setattr(pharmacy_license, "PHARM_LICENSE_STATE_ADAPTER_MAX_PAGES", 1)
    license_rows, _source_url, metadata, error = await pharmacy_license._load_rows_from_socrata_source(
        object(),
        _state("ZZ"),
        "https://data.example.test/licenses.csv",
        select_columns=("credentialnumber",),
        where_clause="true",
        page_size=1,
    )
    assert len(license_rows) == 1
    assert metadata["page_limit_reached"] is True
    assert error is None


@pytest.mark.asyncio
async def test_rosa_adapter_enforces_bounds_and_rejects_non_list_content(monkeypatch):
    monkeypatch.setattr(pharmacy_license, "PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS", 1)
    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(
            return_value=(
                "https://api.example.test/licenses",
                "application/json",
                b'{"content":[{"registrationNumber":"A"}]}',
            )
        ),
    )
    license_rows, _source_url, metadata, error = await pharmacy_license._load_rows_from_ny_rosa_source(
        object(),
        _state("NY"),
        base_url="https://api.example.test/licenses",
        api_key="secret",
        query_terms=("one", "two"),
        page_size=10,
    )
    assert len(license_rows) == 1
    assert metadata["row_limit_reached"] is True
    assert error is None

    monkeypatch.setattr(pharmacy_license, "PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS", 10)
    monkeypatch.setattr(pharmacy_license, "PHARM_LICENSE_STATE_ADAPTER_MAX_PAGES", 1)
    license_rows, _source_url, metadata, error = await pharmacy_license._load_rows_from_ny_rosa_source(
        object(),
        _state("NY"),
        base_url="https://api.example.test/licenses",
        api_key="secret",
        query_terms=("one",),
        page_size=1,
    )
    assert len(license_rows) == 1
    assert metadata["page_limit_reached"] is True
    assert error is None

    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=("https://api.example.test/licenses", "application/json", b'{"content":{}}')),
    )
    license_rows, _source_url, metadata, error = await pharmacy_license._load_rows_from_ny_rosa_source(
        object(),
        _state("NY"),
        base_url="https://api.example.test/licenses",
        api_key="secret",
        query_terms=("one",),
        page_size=10,
    )
    assert license_rows == []
    assert metadata["rows_loaded"] == 0
    assert error is None


@pytest.mark.asyncio
async def test_export_adapter_stops_at_the_configured_facility_row_bound(monkeypatch):
    monkeypatch.setattr(pharmacy_license, "PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS", 1)
    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=("https://exports.example.test/board", "application/zip", b"zip")),
    )
    monkeypatch.setattr(
        pharmacy_license,
        "_parse_zip_records",
        lambda _raw: [
            {"License Type": "Retail Pharmacy License", "License Number": "A"},
            {"License Type": "Retail Pharmacy License", "License Number": "B"},
        ],
    )

    license_rows, _source_url, metadata, error = await pharmacy_license._load_rows_from_ma_export_source(
        object(),
        _state("MA"),
        base_url="https://exports.example.test",
        board_id="board",
    )

    assert [license_record["License Number"] for license_record in license_rows] == ["A"]
    assert metadata["row_limit_reached"] is True
    assert error is None
