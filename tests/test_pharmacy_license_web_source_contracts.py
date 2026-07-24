"""Web discovery and browser-style pharmacy-license source contracts."""

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


class _ResponseContext:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, *_exc):
        return False


class _PostSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def post(self, url, **kwargs):
        self.requests.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return _ResponseContext(response)


@pytest.mark.asyncio
async def test_aspnet_adapter_submits_filters_pages_and_deduplicates(monkeypatch):
    search_html = """
    <form action="results.aspx">
      <input type="hidden" name="__VIEWSTATE" value="token">
      <select name="t_web_lookup__profession_name">
        <option value="RX">Pharmacy</option>
      </select>
      <select name="t_web_lookup__license_type_name">
        <option value="FACILITY">Pharmacy</option>
      </select>
    </form>
    """
    first_page = SimpleNamespace(url="https://licenses.example.test/results", page="first")
    second_page = SimpleNamespace(url="https://licenses.example.test/results", page="second")
    session = _PostSession([first_page, second_page])
    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=("https://licenses.example.test/search", "text/html", search_html.encode())),
    )
    monkeypatch.setattr(pharmacy_license, "_read_response_bytes", AsyncMock(side_effect=[b"first", b"second"]))
    monkeypatch.setattr(pharmacy_license, "_decode_text", lambda payload: payload.decode() if isinstance(payload, bytes) else payload)
    license_rows_by_page = {
        "first": [
            {"Name": "Sample Pharmacy", "License #": "A", "State": "TX"},
            {"Name": "Sample Pharmacy", "License #": "A", "State": "TX"},
        ],
        "second": [{"Name": "Second Pharmacy", "License Number": "B"}],
    }
    monkeypatch.setattr(pharmacy_license, "_parse_datagrid_rows", lambda page: license_rows_by_page[page])
    monkeypatch.setattr(pharmacy_license, "_extract_postback_targets", lambda page: {2: "next"} if page == "first" else {})
    monkeypatch.setattr(pharmacy_license, "_extract_hidden_fields", lambda _page: {"__VIEWSTATE": "token"})

    license_rows, source_url, metadata, error = await pharmacy_license._load_rows_from_aspnet_search_state(
        session,
        _state(),
        pharmacy_license.AspNetStateAdapterSpec("TX", "https://licenses.example.test/search"),
    )

    assert error is None
    assert [license_record["License #"] if "License #" in license_record else license_record["License Number"] for license_record in license_rows] == ["A", "B"]
    assert source_url == "https://licenses.example.test/results"
    assert metadata["pages_fetched"] == 2
    assert metadata["rows_loaded"] == 2
    assert session.requests[0][1]["data"]["t_web_lookup__profession_name"] == "RX"
    assert session.requests[1][1]["data"]["__EVENTTARGET"] == "next"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("search_page", "post_response", "expected_error"),
    [
        (b"please solve the captcha", None, "captcha_required"),
        (b"<form><input name='unrelated'></form>", None, "state_adapter_no_pharmacy_filter"),
        (b"<form><input name='t_web_lookup__full_name'></form>", OSError("blocked"), "adapter_search_failed:blocked"),
    ],
)
async def test_aspnet_adapter_explains_pre_result_failures(
    monkeypatch,
    search_page,
    post_response,
    expected_error,
):
    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=("https://licenses.example.test/search", "text/html", search_page)),
    )
    session = _PostSession([] if post_response is None else [post_response])

    license_rows, _source_url, _metadata, error = await pharmacy_license._load_rows_from_aspnet_search_state(
        session,
        _state(),
        pharmacy_license.AspNetStateAdapterSpec("TX", "https://licenses.example.test/search"),
    )
    assert license_rows == []
    assert error == expected_error


@pytest.mark.asyncio
async def test_aspnet_adapter_distinguishes_captcha_empty_grid_and_pagination_failure(monkeypatch):
    search_html = b"<form><input name='t_web_lookup__doing_business_as'></form>"
    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=("https://licenses.example.test/search", "text/html", search_html)),
    )

    response = SimpleNamespace(url="https://licenses.example.test/results")
    session = _PostSession([response])
    monkeypatch.setattr(
        pharmacy_license,
        "_read_response_bytes",
        AsyncMock(return_value=b"please solve the captcha"),
    )
    license_rows, _source_url, _metadata, error = await pharmacy_license._load_rows_from_aspnet_search_state(
        session,
        _state(),
        pharmacy_license.AspNetStateAdapterSpec("TX", "https://licenses.example.test/search"),
    )
    assert license_rows == []
    assert error == "captcha_required"

    session = _PostSession([response])
    monkeypatch.setattr(pharmacy_license, "_read_response_bytes", AsyncMock(return_value=b"empty grid"))
    monkeypatch.setattr(pharmacy_license, "_parse_datagrid_rows", lambda _page: [])
    license_rows, _source_url, _metadata, error = await pharmacy_license._load_rows_from_aspnet_search_state(
        session,
        _state(),
        pharmacy_license.AspNetStateAdapterSpec("TX", "https://licenses.example.test/search"),
    )
    assert license_rows == []
    assert error == "state_adapter_no_results_grid"

    session = _PostSession([response, OSError("next blocked")])
    monkeypatch.setattr(pharmacy_license, "_read_response_bytes", AsyncMock(return_value=b"first"))
    monkeypatch.setattr(
        pharmacy_license,
        "_parse_datagrid_rows",
        lambda _page: [{"Name": "Sample Pharmacy", "License #": "A", "State": "TX"}],
    )
    monkeypatch.setattr(pharmacy_license, "_extract_postback_targets", lambda _page: {2: "next"})
    license_rows, _source_url, metadata, error = await pharmacy_license._load_rows_from_aspnet_search_state(
        session,
        _state(),
        pharmacy_license.AspNetStateAdapterSpec("TX", "https://licenses.example.test/search"),
    )
    assert len(license_rows) == 1
    assert metadata["pagination_error"] == "next blocked"
    assert error is None


def test_json_record_extraction_preserves_only_object_records():
    assert pharmacy_license._extract_records_from_json([{"id": 1}, "ignore"]) == [{"id": 1}]
    assert pharmacy_license._extract_records_from_json({"items": [{"id": 2}, None]}) == [{"id": 2}]
    assert pharmacy_license._extract_records_from_json({"a": {"id": 3}, "b": {"id": 4}}) == [
        {"id": 3},
        {"id": 4},
    ]
    assert pharmacy_license._extract_records_from_json({"a": {"id": 3}, "b": "mixed"}) == []
    assert pharmacy_license._extract_records_from_json("invalid") == []


class _ChunkedContent:
    def __init__(self, chunks):
        self.chunks = chunks

    async def iter_chunked(self, _size):
        for chunk in self.chunks:
            yield chunk


@pytest.mark.asyncio
async def test_response_reader_ignores_empty_chunks_and_enforces_size_limit():
    response = SimpleNamespace(content=_ChunkedContent([b"a", b"", b"bc"]))
    assert await pharmacy_license._read_response_bytes(response, max_bytes=3) == b"abc"

    response = SimpleNamespace(content=_ChunkedContent([b"ab", b"cd"]))
    with pytest.raises(RuntimeError, match="payload_too_large:3"):
        await pharmacy_license._read_response_bytes(response, max_bytes=3)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("board_url", "final_url", "content_type", "raw", "expected"),
    [
        ("https://files.example.test/licenses.csv", None, None, None, ["https://files.example.test/licenses.csv"]),
        ("https://licenses.example.test/search", "https://files.example.test/export.zip", "application/octet-stream", b"", ["https://files.example.test/export.zip"]),
        ("https://licenses.example.test/search", "https://licenses.example.test/results", "application/json", b"{}", ["https://licenses.example.test/results"]),
        ("https://licenses.example.test/search", "https://licenses.example.test/results", "text/csv", b"id\n1\n", ["https://licenses.example.test/results"]),
        ("https://licenses.example.test/search", "https://licenses.example.test/results", "text/html", b"<a href='/files/licenses.csv'>file</a>", ["https://licenses.example.test/files/licenses.csv"]),
    ],
)
async def test_source_discovery_honors_direct_redirect_content_type_and_html_links(
    monkeypatch,
    board_url,
    final_url,
    content_type,
    raw,
    expected,
):
    if final_url is not None:
        monkeypatch.setattr(
            pharmacy_license,
            "_fetch_bytes",
            AsyncMock(return_value=(final_url, content_type, raw)),
        )
    sources, error = await pharmacy_license._discover_machine_readable_sources(
        object(),
        pharmacy_license.StateSource("ZZ", "Sample State", board_url),
    )
    assert sources == expected
    assert error is None


@pytest.mark.asyncio
async def test_source_discovery_reports_transport_and_missing_link_failures(monkeypatch):
    monkeypatch.setattr(pharmacy_license, "_fetch_bytes", AsyncMock(side_effect=OSError("offline")))
    sources, error = await pharmacy_license._discover_machine_readable_sources(object(), _state())
    assert sources == []
    assert error == "board_fetch_failed:offline"

    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=("https://licenses.example.test/results", "text/html", b"<html />")),
    )
    sources, error = await pharmacy_license._discover_machine_readable_sources(object(), _state())
    assert sources == []
    assert error == "no_machine_readable_link"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source_url", "content_type", "payload", "expected"),
    [
        ("https://files.example.test/licenses.csv", "application/octet-stream", b"id\n1\n", [{"id": "1"}]),
        ("https://files.example.test/licenses", "application/json", b'{"items":[{"id":2}]}', [{"id": 2}]),
    ],
)
async def test_record_loader_selects_parser_from_url_or_content_type(
    monkeypatch,
    source_url,
    content_type,
    payload,
    expected,
):
    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=(source_url, content_type, payload)),
    )
    license_rows, error = await pharmacy_license._load_records_from_source(object(), source_url)
    assert license_rows == expected
    assert error is None


@pytest.mark.asyncio
async def test_record_loader_reports_transport_parse_and_format_failures(monkeypatch):
    monkeypatch.setattr(pharmacy_license, "_fetch_bytes", AsyncMock(side_effect=OSError("offline")))
    assert await pharmacy_license._load_records_from_source(object(), "https://files.example.test/a") == (
        [],
        "source_fetch_failed:offline",
    )

    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=("https://files.example.test/a.json", "application/json", b"{")),
    )
    license_rows, error = await pharmacy_license._load_records_from_source(object(), "https://files.example.test/a.json")
    assert license_rows == []
    assert error.startswith("source_parse_failed:")

    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=("https://files.example.test/a.bin", "application/octet-stream", b"raw")),
    )
    assert await pharmacy_license._load_records_from_source(object(), "https://files.example.test/a.bin") == (
        [],
        "unsupported_source_format",
    )


@pytest.mark.asyncio
async def test_aspnet_source_error_takes_precedence_over_generic_discovery_failure(monkeypatch):
    monkeypatch.setattr(
        pharmacy_license,
        "_load_rows_from_configured_source",
        AsyncMock(return_value=(False, [], None, {}, None)),
    )
    monkeypatch.setattr(
        pharmacy_license,
        "_create_aspnet_adapter_spec",
        lambda _source: pharmacy_license.AspNetStateAdapterSpec("TX", "https://licenses.example.test/search"),
    )
    monkeypatch.setattr(
        pharmacy_license,
        "_load_rows_from_aspnet_search_state",
        AsyncMock(return_value=([], "https://licenses.example.test/results", {"adapter": "aspnet"}, "captcha_required")),
    )
    monkeypatch.setattr(
        pharmacy_license,
        "_discover_machine_readable_sources",
        AsyncMock(return_value=([], "no_machine_readable_link")),
    )

    stats = await pharmacy_license._import_state_source(
        object(),
        _state(),
        run_id="run",
        snapshot_id="snapshot",
        test_mode=False,
    )

    assert stats.status == "unsupported"
    assert stats.unsupported_reason == "captcha_required"
    assert stats.metadata["adapter_source_url"].endswith("results")


@pytest.mark.asyncio
async def test_aspnet_adapter_distinguishes_no_records_second_empty_page_and_adapter_bounds(monkeypatch):
    search_html = b"<form><input name='t_web_lookup__full_name'></form>"
    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=("https://licenses.example.test/search", "text/html", search_html)),
    )
    response = SimpleNamespace(url="https://licenses.example.test/results")

    session = _PostSession([response])
    monkeypatch.setattr(pharmacy_license, "_read_response_bytes", AsyncMock(return_value=b"no records found"))
    monkeypatch.setattr(pharmacy_license, "_parse_datagrid_rows", lambda _page: [])
    license_rows, _source_url, metadata, error = await pharmacy_license._load_rows_from_aspnet_search_state(
        session,
        _state(),
        pharmacy_license.AspNetStateAdapterSpec("TX", "https://licenses.example.test/search"),
    )
    assert license_rows == []
    assert metadata["pages_fetched"] == 1
    assert error is None

    session = _PostSession([response, response])
    monkeypatch.setattr(pharmacy_license, "_read_response_bytes", AsyncMock(side_effect=[b"first", b"second"]))
    monkeypatch.setattr(
        pharmacy_license,
        "_parse_datagrid_rows",
        lambda page: [{"Name": "Sample", "License #": "A"}] if page == "first" else [],
    )
    monkeypatch.setattr(pharmacy_license, "_extract_postback_targets", lambda page: {2: "next"} if page == "first" else {})
    license_rows, _source_url, metadata, error = await pharmacy_license._load_rows_from_aspnet_search_state(
        session,
        _state(),
        pharmacy_license.AspNetStateAdapterSpec("TX", "https://licenses.example.test/search"),
    )
    assert len(license_rows) == 1
    assert metadata["pages_fetched"] == 2
    assert error is None

    session = _PostSession([response])
    monkeypatch.setattr(pharmacy_license, "_read_response_bytes", AsyncMock(return_value=b"bounded"))
    monkeypatch.setattr(
        pharmacy_license,
        "_parse_datagrid_rows",
        lambda _page: [{"Name": "Sample", "License #": "A"}],
    )
    monkeypatch.setattr(pharmacy_license, "_extract_postback_targets", lambda _page: {2: ""})
    monkeypatch.setattr(pharmacy_license, "PHARM_LICENSE_STATE_ADAPTER_MAX_PAGES", 1)
    monkeypatch.setattr(pharmacy_license, "PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS", 1)
    license_rows, _source_url, metadata, error = await pharmacy_license._load_rows_from_aspnet_search_state(
        session,
        _state(),
        pharmacy_license.AspNetStateAdapterSpec("TX", "https://licenses.example.test/search"),
    )
    assert len(license_rows) == 1
    assert metadata["page_limit_reached"] is True
    assert metadata["row_limit_reached"] is True
    assert error is None


def test_nested_archive_parser_ignores_bad_archives_and_falls_back_to_metadata():
    archive_buffer = io.BytesIO()
    with zipfile.ZipFile(archive_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("broken.zip", b"not a zip")
        archive.writestr("notes.txt", "ignore")
        archive.writestr("empty.csv", "")
        archive.writestr("records.json", '{"records":[{"License Number":"A"}]}')
        archive.writestr("metadata.csv", "Column,Value\nignored,true\n")
    assert pharmacy_license._parse_zip_records(archive_buffer.getvalue()) == [{"License Number": "A"}]

    metadata_buffer = io.BytesIO()
    with zipfile.ZipFile(metadata_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("metadata.csv", "Column,Value\nLicense Number,A\n")
    assert pharmacy_license._parse_zip_records(metadata_buffer.getvalue()) == [
        {"Column": "License Number", "Value": "A"}
    ]


@pytest.mark.asyncio
async def test_record_loader_supports_zip_content_type(monkeypatch):
    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=("https://files.example.test/archive", "application/zip", b"zip")),
    )
    monkeypatch.setattr(pharmacy_license, "_parse_zip_records", lambda _raw: [{"id": 1}])
    assert await pharmacy_license._load_records_from_source(
        object(),
        "https://files.example.test/archive",
    ) == ([{"id": 1}], None)


@pytest.mark.asyncio
async def test_aspnet_adapter_can_be_disabled_with_a_zero_page_bound(monkeypatch):
    search_html = b"<form><input name='t_web_lookup__full_name'></form>"
    response = SimpleNamespace(url="https://licenses.example.test/results")
    monkeypatch.setattr(
        pharmacy_license,
        "_fetch_bytes",
        AsyncMock(return_value=("https://licenses.example.test/search", "text/html", search_html)),
    )
    monkeypatch.setattr(pharmacy_license, "_read_response_bytes", AsyncMock(return_value=b"results"))
    monkeypatch.setattr(pharmacy_license, "PHARM_LICENSE_STATE_ADAPTER_MAX_PAGES", 0)

    license_rows, source_url, metadata, error = await pharmacy_license._load_rows_from_aspnet_search_state(
        _PostSession([response]),
        _state(),
        pharmacy_license.AspNetStateAdapterSpec("TX", "https://licenses.example.test/search"),
    )

    assert license_rows == []
    assert source_url.endswith("results")
    assert metadata["pages_fetched"] == 0
    assert metadata["page_limit_reached"] is True
    assert error is None
