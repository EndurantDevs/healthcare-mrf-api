"""State pharmacy-license adapter and registry contracts."""

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
async def test_schema_discovery_prefers_configured_then_standard_schemas(monkeypatch):
    pharmacy_license._TABLE_SCHEMA_CACHE.clear()
    rows_by_table = {
        "preferred": [("public",), ("analytics",)],
        "mrf_first": [("public",), ("mrf",)],
        "public_first": [("archive",), ("public",)],
        "fallback": [("archive",)],
        "missing": [],
    }

    async def find_schemas(_sql, *, table_name):
        return rows_by_table[table_name]

    monkeypatch.setattr(pharmacy_license.db, "all", find_schemas)

    assert await pharmacy_license._find_table_schema("preferred", "analytics") == "analytics"
    assert await pharmacy_license._find_table_schema("mrf_first") == "mrf"
    assert await pharmacy_license._find_table_schema("public_first") == "public"
    assert await pharmacy_license._find_table_schema("fallback") == "archive"
    assert await pharmacy_license._find_table_schema("missing") is None

    rows_by_table["preferred"] = []
    assert await pharmacy_license._find_table_schema("preferred", "analytics") == "analytics"


@pytest.mark.asyncio
async def test_state_resolver_uses_unambiguous_registry_evidence_and_quality_gated_names(monkeypatch):
    """Prefer unique state evidence and expose the Part D quality decision."""
    async def find_schema(_table_name, _preferred_schema=None):
        return "mrf"
    async def load_registry_rows(sql, **_params):
        if "provider_license_number\n" in sql:
            return [
                None,
                (None, "IGNORED"),
                (1111111111, None),
                (1111111111, "TX-ONE"),
                (2222222222, "TX-DUP"),
                (3333333333, "TX-DUP"),
            ]
        if "other_provider_identifier" in sql:
            return [
                None,
                (None, "TX-OTHER", "STATE LICENSE"),
                (1111111111, "IGNORED", "PLAN IDENTIFIER"),
                (2222222222, None, "STATE LICENSE"),
                (2222222222, "TX-OTHER", "STATE LICENSE"),
                (3333333333, "12-003", "STATE LICENSE"),
            ]
        if "provider_organization_name" in sql:
            return [
                None,
                (None, "Austin", "Ignored", None, None),
                (1111111111, None, "Ignored", None, None),
                (1111111111, "Austin", "Sample Pharmacy", None, "Sample DBA"),
            ]
        if "pharmacy_name" in sql:
            return [
                None,
                (None, "Ignored", "Austin", "78701"),
                (1111111111, None, None, None),
                (1111111111, "Sample Pharmacy", "Austin", "78701"),
                (2222222222, "Other Pharmacy", None, "78702"),
            ]
        raise AssertionError(sql)

    monkeypatch.setattr(pharmacy_license, "_find_table_schema", find_schema)
    monkeypatch.setattr(pharmacy_license.db, "all", load_registry_rows)
    monkeypatch.setattr(
        pharmacy_license,
        "_is_partd_name_fallback_acceptable",
        lambda **_counts: True,
    )

    resolver = await pharmacy_license._build_state_npi_resolver(" tx ")

    assert resolver is not None
    assert resolver.state_code == "TX"
    assert resolver.by_license == {"TXONE": 1111111111}
    assert resolver.by_other_identifier == {"TXOTHER": 2222222222, "12003": 3333333333}
    assert resolver.by_other_identifier_digits == {"12003": 3333333333}
    assert resolver.by_registry_name_city[("samplepharmacy", "austin")] == 1111111111
    assert resolver.partd_name_fallback_enabled is True
    assert resolver.partd_quality["quality_ok"] is True

    assert await pharmacy_license._build_state_npi_resolver(" ") is None


@pytest.mark.asyncio
async def test_state_resolver_returns_none_when_no_index_has_unique_evidence(monkeypatch):
    monkeypatch.setattr(pharmacy_license, "_find_table_schema", AsyncMock(return_value=None))

    assert await pharmacy_license._build_state_npi_resolver("TX") is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state_code", "header", "expected_name"),
    [
        ("TX", "LIC_NBR,PHARMACY_NAME\n1,Sample TX Pharmacy\n", "Sample TX Pharmacy"),
        ("FL", "License Number, Org Name\n2,Sample FL Pharmacy\n", "Sample FL Pharmacy"),
        ("ZZ", "License Number,Entity Name\n3,Sample Pharmacy\n", "Sample Pharmacy"),
    ],
)
async def test_direct_csv_adapter_preserves_state_specific_mapping(
    monkeypatch,
    state_code,
    header,
    expected_name,
):
    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=("https://files.example.test/licenses.csv", "text/csv", header.encode())),
    )

    license_rows, source_url, metadata, error = await pharmacy_license._load_rows_from_direct_csv_source(
        object(),
        _state(state_code),
        "https://files.example.test/licenses.csv",
    )

    assert error is None
    assert source_url == "https://files.example.test/licenses.csv"
    assert license_rows[0]["Entity Name"] == expected_name
    assert metadata["rows_loaded"] == 1


@pytest.mark.asyncio
async def test_direct_csv_adapter_distinguishes_transport_and_parse_failures(monkeypatch):
    monkeypatch.setattr(pharmacy_license, "_fetch_bytes", AsyncMock(side_effect=TimeoutError("slow")))
    license_rows, source_url, _metadata, error = await pharmacy_license._load_rows_from_direct_csv_source(
        object(),
        _state(),
        "https://files.example.test/licenses.csv",
    )
    assert license_rows == []
    assert source_url is None
    assert error == "adapter_fetch_failed:slow"

    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=("https://files.example.test/licenses.csv", "text/csv", b"bad")),
    )
    monkeypatch.setattr(pharmacy_license, "_parse_csv_records", lambda _raw: (_ for _ in ()).throw(ValueError("bad csv")))
    license_rows, source_url, _metadata, error = await pharmacy_license._load_rows_from_direct_csv_source(
        object(),
        _state(),
        "https://files.example.test/licenses.csv",
    )
    assert license_rows == []
    assert source_url.endswith("licenses.csv")
    assert error == "adapter_parse_failed:bad csv"


@pytest.mark.asyncio
async def test_socrata_adapter_pages_until_short_page_and_maps_configured_state(monkeypatch):
    payloads = [
        b"licensenumber,lastname,firstname,state\n1,Pharmacy,Alpha,CO\n",
        b"licensenumber,lastname,firstname,state\n2,Pharmacy,Beta,CO\n",
        b"licensenumber,lastname,firstname,state\n",
    ]
    requested_urls = []

    async def fetch_page(_session, url, **_kwargs):
        requested_urls.append(url)
        return url, "text/csv", payloads.pop(0)

    monkeypatch.setattr(pharmacy_license, "_fetch_bytes", fetch_page)
    license_rows, source_url, metadata, error = await pharmacy_license._load_rows_from_socrata_source(
        object(),
        _state("CO"),
        "https://data.example.test/resource.csv",
        select_columns=("licensenumber", "lastname", "firstname", "state"),
        where_clause="active=true",
        page_size=1,
    )

    assert error is None
    assert [license_record["License Number"] for license_record in license_rows] == ["1", "2"]
    assert metadata == {
        "adapter": pharmacy_license._STATE_ADAPTER_SOCRATA,
        "source_url": "https://data.example.test/resource.csv",
        "pages_fetched": 3,
        "rows_loaded": 2,
    }
    assert "%24offset=1" in requested_urls[1]
    assert source_url == requested_urls[-1]


@pytest.mark.asyncio
async def test_socrata_adapter_reports_fetch_parse_and_row_limits(monkeypatch):
    monkeypatch.setattr(pharmacy_license, "_fetch_bytes", AsyncMock(side_effect=OSError("offline")))
    license_rows, source_url, _metadata, error = await pharmacy_license._load_rows_from_socrata_source(
        object(),
        _state("WA"),
        "https://data.example.test/licenses.csv",
        select_columns=("credentialnumber",),
        where_clause="active=true",
        page_size=10,
    )
    assert (license_rows, source_url, error) == ([], None, "adapter_fetch_failed:offline")

    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=("https://data.example.test/licenses.csv", "text/csv", b"broken")),
    )
    monkeypatch.setattr(pharmacy_license, "_parse_csv_records", lambda _raw: (_ for _ in ()).throw(ValueError("shape")))
    license_rows, source_url, _metadata, error = await pharmacy_license._load_rows_from_socrata_source(
        object(),
        _state("WA"),
        "https://data.example.test/licenses.csv",
        select_columns=("credentialnumber",),
        where_clause="active=true",
        page_size=10,
    )
    assert license_rows == []
    assert source_url.endswith("licenses.csv")
    assert error == "adapter_parse_failed:shape"

    monkeypatch.setattr(pharmacy_license, "_parse_csv_records", lambda _raw: [{"raw": "row"}])
    monkeypatch.setattr(pharmacy_license, "PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS", 1)
    license_rows, _source_url, metadata, error = await pharmacy_license._load_rows_from_socrata_source(
        object(),
        _state("ZZ"),
        "https://data.example.test/licenses.csv",
        select_columns=("raw",),
        where_clause="true",
        page_size=1,
    )
    assert license_rows == [{"raw": "row"}]
    assert metadata["row_limit_reached"] is True
    assert error is None


@pytest.mark.asyncio
async def test_rosa_adapter_deduplicates_registration_records_across_terms(monkeypatch):
    responses = [
        {
            "content": [
                "ignore",
                {},
                {"registrationNumber": "A", "legalName": {"value": "Alpha"}},
            ]
        },
        {"content": [{"registrationNumber": "A"}, {"registrationNumber": "B"}]},
    ]

    async def fetch_page(_session, url, **kwargs):
        assert kwargs["headers"] == {"x-oapi-key": "secret"}
        return url, "application/json", json.dumps(responses.pop(0)).encode()

    monkeypatch.setattr(pharmacy_license, "_fetch_bytes", fetch_page)
    license_rows, source_url, metadata, error = await pharmacy_license._load_rows_from_ny_rosa_source(
        object(),
        _state("NY"),
        base_url="https://api.example.test/licenses",
        api_key="secret",
        query_terms=("pharmacy", "drug"),
        page_size=10,
    )

    assert error is None
    assert {license_record["License Number"] for license_record in license_rows} == {"A", "B"}
    assert metadata["requests_made"] == 2
    assert metadata["rows_loaded"] == 2
    assert source_url.startswith("https://api.example.test/licenses?")


@pytest.mark.asyncio
async def test_rosa_adapter_explains_missing_credentials_and_bad_responses(monkeypatch):
    license_rows, source_url, _metadata, error = await pharmacy_license._load_rows_from_ny_rosa_source(
        object(),
        _state("NY"),
        base_url="https://api.example.test/licenses",
        api_key="",
        query_terms=("pharmacy",),
        page_size=10,
    )
    assert (license_rows, source_url, error) == ([], None, "missing_ny_rosa_api_key")

    monkeypatch.setattr(pharmacy_license, "_fetch_bytes", AsyncMock(side_effect=OSError("offline")))
    license_rows, _source_url, _metadata, error = await pharmacy_license._load_rows_from_ny_rosa_source(
        object(),
        _state("NY"),
        base_url="https://api.example.test/licenses",
        api_key="secret",
        query_terms=("pharmacy",),
        page_size=10,
    )
    assert license_rows == []
    assert error == "adapter_fetch_failed:offline"

    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=("https://api.example.test/licenses", "application/json", b"{")),
    )
    license_rows, source_url, _metadata, error = await pharmacy_license._load_rows_from_ny_rosa_source(
        object(),
        _state("NY"),
        base_url="https://api.example.test/licenses",
        api_key="secret",
        query_terms=("pharmacy",),
        page_size=10,
    )
    assert license_rows == []
    assert source_url == "https://api.example.test/licenses"
    assert error.startswith("adapter_parse_failed:")


@pytest.mark.asyncio
async def test_export_adapter_keeps_facilities_and_reports_source_failures(monkeypatch):
    license_rows, source_url, _metadata, error = await pharmacy_license._load_rows_from_ma_export_source(
        object(),
        _state("MA"),
        base_url="https://exports.example.test",
        board_id=" ",
    )
    assert (license_rows, source_url, error) == ([], None, "missing_ma_export_board_id")

    monkeypatch.setattr(pharmacy_license, "_fetch_bytes", AsyncMock(side_effect=OSError("offline")))
    license_rows, source_url, _metadata, error = await pharmacy_license._load_rows_from_ma_export_source(
        object(),
        _state("MA"),
        base_url="https://exports.example.test",
        board_id="board",
    )
    assert (license_rows, source_url, error) == ([], None, "adapter_fetch_failed:offline")

    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=("https://exports.example.test/board", "application/zip", b"zip")),
    )
    monkeypatch.setattr(
        pharmacy_license,
        "_parse_zip_records",
        lambda _raw: [
            "ignore",
            {"License Type": "Pharmacist License", "License Number": "P"},
            {"License Type": "Retail Pharmacy License", "License Number": "F"},
        ],
    )
    license_rows, source_url, metadata, error = await pharmacy_license._load_rows_from_ma_export_source(
        object(),
        _state("MA"),
        base_url="https://exports.example.test/",
        board_id=" board ",
    )
    assert error is None
    assert source_url.endswith("/board")
    assert [license_record["License Number"] for license_record in license_rows] == ["F"]
    assert metadata["records_parsed"] == 3

    monkeypatch.setattr(pharmacy_license, "_parse_zip_records", lambda _raw: (_ for _ in ()).throw(ValueError("zip")))
    license_rows, source_url, _metadata, error = await pharmacy_license._load_rows_from_ma_export_source(
        object(),
        _state("MA"),
        base_url="https://exports.example.test",
        board_id="board",
    )
    assert license_rows == []
    assert source_url.endswith("/board")
    assert error == "adapter_parse_failed:zip"


@pytest.mark.asyncio
async def test_configured_source_dispatches_each_explicit_adapter(monkeypatch):
    direct_loader = AsyncMock(return_value=([{"kind": "direct"}], "direct", {"adapter": "direct"}, None))
    socrata_loader = AsyncMock(return_value=([{"kind": "socrata"}], "socrata", {"adapter": "socrata"}, None))
    rosa_loader = AsyncMock(return_value=([{"kind": "rosa"}], "rosa", {"adapter": "rosa"}, None))
    export_loader = AsyncMock(return_value=([{"kind": "export"}], "export", {"adapter": "export"}, None))
    monkeypatch.setattr(pharmacy_license, "_load_rows_from_direct_csv_source", direct_loader)
    monkeypatch.setattr(pharmacy_license, "_load_rows_from_socrata_source", socrata_loader)
    monkeypatch.setattr(pharmacy_license, "_load_rows_from_ny_rosa_source", rosa_loader)
    monkeypatch.setattr(pharmacy_license, "_load_rows_from_ma_export_source", export_loader)
    monkeypatch.setattr(pharmacy_license, "_STATE_STATIC_UNSUPPORTED_CONFIG", {"ZZ": "manual_only"})
    monkeypatch.setattr(pharmacy_license, "_STATE_DIRECT_CSV_CONFIG", {"TX": {"source_url": "direct"}})
    monkeypatch.setattr(
        pharmacy_license,
        "_STATE_SOCRATA_CONFIG",
        {"CO": {"source_url": "socrata", "columns": ("id",), "where": "true"}},
    )

    static_result = await pharmacy_license._load_rows_from_configured_source(object(), _state("ZZ"))
    assert static_result[0] is True
    assert static_result[-1] == "manual_only"
    assert static_result[3]["terminal_error"] is True

    assert (await pharmacy_license._load_rows_from_configured_source(object(), _state("TX")))[1][0]["kind"] == "direct"
    assert (await pharmacy_license._load_rows_from_configured_source(object(), _state("CO")))[1][0]["kind"] == "socrata"
    assert (await pharmacy_license._load_rows_from_configured_source(object(), _state("NY")))[1][0]["kind"] == "rosa"
    assert (await pharmacy_license._load_rows_from_configured_source(object(), _state("MA")))[1][0]["kind"] == "export"
    assert await pharmacy_license._load_rows_from_configured_source(object(), _state("AK")) == (
        False,
        [],
        None,
        {},
        None,
    )
