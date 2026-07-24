# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
from unittest.mock import AsyncMock, call

import pytest

code_sets = importlib.import_module("process.code_sets")


POS_HTML = """
<table>
  <tr><th>Place of Service Code(s)</th><th>Place of Service Name</th><th>Place of Service Description</th></tr>
  <tr><th scope="row">23</th><td>Emergency Room - Hospital</td><td>A portion of a hospital where emergency care is provided.</td></tr>
  <tr><th scope="row">28-30</th><td>Unassigned</td><td>N/A</td></tr>
</table>
"""


RC_HTML = """
<table>
  <tr><th>Code</th><th>Display</th></tr>
  <tr><td>0450</td><td>EMERGENCY ROOM - GENERAL CLASSIFICATION</td></tr>
  <tr><td>0981</td><td>PROFESSIONAL FEES - EMERGENCY ROOM SERVICES</td></tr>
</table>
"""


def _pos_html_for_codes(codes: list[str]) -> str:
    body = "".join(
        f"<tr><td>{code}</td><td>POS {code}</td><td>Description {code}</td></tr>"
        for code in codes
    )
    return f"<table>{body}</table>"


def _revenue_html_for_codes(codes: list[str]) -> str:
    body = "".join(
        f"<tr><td>{code}</td><td>Revenue {code}</td></tr>"
        for code in codes
    )
    return f"<table>{body}</table>"


def _stub_import_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    *,
    pos_html: str,
    rc_html: str,
) -> tuple[AsyncMock, AsyncMock, AsyncMock]:
    ensure_database = AsyncMock()
    ensure_catalog = AsyncMock()
    upsert_rows = AsyncMock(side_effect=lambda _schema, code_rows: len(code_rows))
    source_html_by_url = {
        code_sets.DEFAULT_POS_URL: pos_html,
        code_sets.DEFAULT_RC_URL: rc_html,
    }

    monkeypatch.setattr(code_sets, "ensure_database", ensure_database)
    monkeypatch.setattr(code_sets, "_ensure_code_catalog", ensure_catalog)
    monkeypatch.setattr(code_sets, "_upsert_code_rows", upsert_rows)
    monkeypatch.setattr(
        code_sets,
        "_download_text",
        lambda url: source_html_by_url[url],
    )
    return ensure_database, ensure_catalog, upsert_rows


def test_parse_pos_rows_expand_ranges_and_normalize_html_text():
    source_html = """
    <table>
      <tr>
        <td>7-8</td>
        <td>Office &amp; Clinic<br>After-hours&nbsp;unit</td>
        <td>Professional &amp; technical<br/>services</td>
      </tr>
    </table>
    """

    code_rows = code_sets.parse_pos_code_rows(source_html)

    assert [code_row.code for code_row in code_rows] == ["07", "08"]
    assert code_rows[0].display_name == "Office & Clinic After-hours unit"
    assert code_rows[0].short_description == code_rows[0].display_name
    assert code_rows[0].long_description == "Professional & technical services"
    assert code_rows[0].source == code_sets.SOURCE_POS


def test_parse_pos_rows_skip_malformed_header_blank_code_and_blank_name():
    source_html = """
    <td>outside a row</td>
    <table>
      <tr></tr>
      <tr><td>one cell</td></tr>
      <tr><td>11</td><td>Place of Service Name</td><td>Description</td></tr>
      <tr><td>&nbsp;</td><td>Blank code</td><td>Description</td></tr>
      <tr><td>12</td><td>&nbsp;</td><td>Description</td></tr>
      <tr><td>13</td><td>Valid facility</td><td>&nbsp;</td></tr>
    </table>
    """

    assert code_sets.parse_pos_code_rows(source_html) == [
        code_sets.CodeSetRow(
            code_system="POS",
            code="13",
            display_name="Valid facility",
            short_description="Valid facility",
            long_description=None,
            source=code_sets.SOURCE_POS,
        )
    ]


def test_parse_revenue_rows_normalize_text_and_skip_non_data_rows():
    source_html = """
    <table>
      <tr><td>one cell</td></tr>
      <tr><td>Revenue Code 0001</td><td>Header</td></tr>
      <tr><td>&nbsp;</td><td>Blank code</td></tr>
      <tr><td>0002</td><td>&nbsp;</td></tr>
      <tr><td>0003</td><td>Facility &amp; service<br>line</td></tr>
    </table>
    """

    assert code_sets.parse_revenue_code_rows(source_html) == [
        code_sets.CodeSetRow(
            code_system="RC",
            code="0003",
            display_name="Facility & service line",
            short_description="Facility & service line",
            long_description=None,
            source=code_sets.SOURCE_RC,
        )
    ]


@pytest.mark.parametrize(
    ("raw_code", "width", "expected_codes"),
    [
        ("28 - 30", 2, ["28", "29", "30"]),
        ("30-28", 2, []),
        ("1-102", 2, []),
        ("Code 7", 2, ["07"]),
        ("not assigned", 4, []),
        ("0450", 4, ["0450"]),
    ],
)
def test_expand_code_range_accepts_numeric_codes_and_rejects_invalid_ranges(
    raw_code,
    width,
    expected_codes,
):
    assert code_sets._expand_code_range(raw_code, width) == expected_codes


def test_parse_pos_code_rows_expands_ranges():
    code_rows = code_sets.parse_pos_code_rows(POS_HTML)

    code_row_by_code = {code_row.code: code_row for code_row in code_rows}
    assert code_row_by_code["23"].display_name == "Emergency Room - Hospital"
    assert code_row_by_code["23"].source == code_sets.SOURCE_POS
    assert code_row_by_code["28"].display_name == "Unassigned"
    assert code_row_by_code["30"].display_name == "Unassigned"


def test_parse_revenue_code_rows():
    code_rows = code_sets.parse_revenue_code_rows(RC_HTML)

    code_row_by_code = {code_row.code: code_row for code_row in code_rows}
    assert code_row_by_code["0450"].display_name == "EMERGENCY ROOM - GENERAL CLASSIFICATION"
    assert code_row_by_code["0981"].source == code_sets.SOURCE_RC


def test_modifier_code_rows_include_pricing_api_contract_codes():
    code_rows = code_sets.modifier_code_rows()

    code_row_by_code = {code_row.code: code_row for code_row in code_rows}
    assert set(code_row_by_code) == {"26", "95", "GQ", "NU", "QW", "RR", "TC", "UE"}
    assert code_row_by_code["26"].display_name == "Professional component"
    assert code_row_by_code["TC"].display_name == "Technical component"
    assert code_row_by_code["TC"].source == code_sets.SOURCE_MODIFIER


class _FakeHeaders:
    def __init__(self, charset):
        self.charset = charset

    def get_content_charset(self):
        return self.charset


class _FakeResponse:
    def __init__(self, payload: bytes, charset: str | None):
        self.payload = payload
        self.headers = _FakeHeaders(charset)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return self.payload


@pytest.mark.parametrize(
    ("payload", "charset", "expected_text"),
    [
        ("café".encode(), None, "café"),
        ("café".encode("latin-1"), "latin-1", "café"),
    ],
)
def test_download_text_honors_response_charset_with_utf8_fallback(
    monkeypatch,
    payload,
    charset,
    expected_text,
):
    observed_requests = []

    def fake_urlopen(request, timeout):
        observed_requests.append((request, timeout))
        return _FakeResponse(payload, charset)

    monkeypatch.setattr(code_sets.urllib.request, "urlopen", fake_urlopen)

    assert code_sets._download_text("https://example.test/codes") == expected_text
    request, timeout = observed_requests[0]
    assert request.full_url == "https://example.test/codes"
    assert request.get_header("User-agent") == "HealthPorta code-set importer"
    assert timeout == 120


@pytest.mark.asyncio
async def test_ensure_code_catalog_creates_and_normalizes_catalog(monkeypatch):
    create_table = AsyncMock()
    execute_status = AsyncMock()
    monkeypatch.setattr(code_sets.db, "create_table", create_table)
    monkeypatch.setattr(code_sets.db, "status", execute_status)

    await code_sets._ensure_code_catalog("catalog_schema")

    create_table.assert_awaited_once_with(
        code_sets.CodeCatalog.__table__,
        checkfirst=True,
    )
    sql = execute_status.await_args.args[0]
    assert "ALTER TABLE catalog_schema.code_catalog" in sql
    assert "ALTER COLUMN code TYPE VARCHAR(128)" in sql


@pytest.mark.asyncio
async def test_upsert_code_rows_preserves_first_duplicate_per_system_and_code(
    monkeypatch,
):
    execute_status = AsyncMock()
    monkeypatch.setattr(code_sets.db, "status", execute_status)
    code_rows = [
        code_sets.CodeSetRow("RC", "0450", "First display", source="first"),
        code_sets.CodeSetRow("RC", "0450", "Later display", source="later"),
        code_sets.CodeSetRow("POS", "0450", "Different system", source="pos"),
    ]

    inserted_count = await code_sets._upsert_code_rows("mrf", code_rows)

    assert inserted_count == 2
    assert execute_status.await_count == 2
    first_params = execute_status.await_args_list[0].kwargs
    second_params = execute_status.await_args_list[1].kwargs
    assert first_params["display_name"] == "First display"
    assert first_params["source"] == "first"
    assert second_params["code_system"] == "POS"
    assert "ON CONFLICT (code_system, code) DO UPDATE" in (
        execute_status.await_args_list[0].args[0]
    )


@pytest.mark.asyncio
async def test_upsert_code_rows_does_not_write_an_empty_collection(monkeypatch):
    execute_status = AsyncMock()
    monkeypatch.setattr(code_sets.db, "status", execute_status)

    assert await code_sets._upsert_code_rows("mrf", []) == 0
    execute_status.assert_not_awaited()


@pytest.mark.asyncio
async def test_import_code_sets_writes_all_sources_and_reports_counts(
    monkeypatch,
    capsys,
):
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    ensure_database, ensure_catalog, upsert_rows = _stub_import_dependencies(
        monkeypatch,
        pos_html=POS_HTML,
        rc_html=RC_HTML,
    )

    import_report = await code_sets.import_code_sets()

    ensure_database.assert_awaited_once_with(False)
    ensure_catalog.assert_awaited_once_with("mrf")
    assert [entry.args[0] for entry in upsert_rows.await_args_list] == [
        "mrf",
        "mrf",
        "mrf",
    ]
    assert import_report == {
        "pos_rows": 4,
        "rc_rows": 2,
        "modifier_rows": 8,
        "pos_url": code_sets.DEFAULT_POS_URL,
        "rc_url": code_sets.DEFAULT_RC_URL,
    }
    assert "POS=4 RC=2 MODIFIER=8" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_import_code_sets_test_mode_prefers_representative_codes(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "code_schema")
    _, ensure_catalog, upsert_rows = _stub_import_dependencies(
        monkeypatch,
        pos_html=_pos_html_for_codes(["01", "21", "22", "23", "24"]),
        rc_html=_revenue_html_for_codes(["0001", "0450", "0981", "0999"]),
    )

    await code_sets.import_code_sets(test_mode=True)

    ensure_catalog.assert_awaited_once_with("code_schema")
    pos_rows = upsert_rows.await_args_list[0].args[1]
    rc_rows = upsert_rows.await_args_list[1].args[1]
    assert [code_row.code for code_row in pos_rows] == ["21", "22", "23"]
    assert [code_row.code for code_row in rc_rows] == ["0450", "0981"]


@pytest.mark.asyncio
async def test_import_code_sets_test_mode_falls_back_to_first_ten_rows(
    monkeypatch,
):
    _, _, upsert_rows = _stub_import_dependencies(
        monkeypatch,
        pos_html=_pos_html_for_codes([f"{number:02}" for number in range(1, 13)]),
        rc_html=_revenue_html_for_codes(
            [f"{number:04}" for number in range(1, 13)]
        ),
    )

    await code_sets.import_code_sets(test_mode=True)

    pos_rows = upsert_rows.await_args_list[0].args[1]
    rc_rows = upsert_rows.await_args_list[1].args[1]
    assert [code_row.code for code_row in pos_rows] == [
        f"{number:02}" for number in range(1, 11)
    ]
    assert [code_row.code for code_row in rc_rows] == [
        f"{number:04}" for number in range(1, 11)
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("pos_html", "rc_html", "expected_message"),
    [
        ("<table></table>", RC_HTML, "CMS POS source produced no code rows"),
        (POS_HTML, "<table></table>", "CMS Blue Button revenue-code source"),
    ],
)
async def test_import_code_sets_rejects_empty_sources_before_catalog_writes(
    monkeypatch,
    pos_html,
    rc_html,
    expected_message,
):
    _, ensure_catalog, upsert_rows = _stub_import_dependencies(
        monkeypatch,
        pos_html=pos_html,
        rc_html=rc_html,
    )

    with pytest.raises(RuntimeError, match=expected_message):
        await code_sets.import_code_sets()

    ensure_catalog.assert_not_awaited()
    upsert_rows.assert_not_awaited()


@pytest.mark.asyncio
async def test_main_returns_import_report_and_disconnects(monkeypatch):
    import_report_map = {"pos_rows": 1}
    initialize_database = AsyncMock()
    import_all_code_sets = AsyncMock(return_value=import_report_map)
    disconnect_database = AsyncMock()
    monkeypatch.setattr(code_sets, "init_db", initialize_database)
    monkeypatch.setattr(code_sets, "import_code_sets", import_all_code_sets)
    monkeypatch.setattr(code_sets.db, "disconnect", disconnect_database)

    assert await code_sets.main(test_mode=True) is import_report_map
    initialize_database.assert_awaited_once_with(code_sets.db)
    import_all_code_sets.assert_awaited_once_with(test_mode=True)
    disconnect_database.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_main_disconnects_when_import_fails(monkeypatch):
    import_failure = RuntimeError("source unavailable")
    initialize_database = AsyncMock()
    import_all_code_sets = AsyncMock(side_effect=import_failure)
    disconnect_database = AsyncMock()
    monkeypatch.setattr(code_sets, "init_db", initialize_database)
    monkeypatch.setattr(code_sets, "import_code_sets", import_all_code_sets)
    monkeypatch.setattr(code_sets.db, "disconnect", disconnect_database)

    with pytest.raises(RuntimeError, match="source unavailable"):
        await code_sets.main()

    assert initialize_database.await_args == call(code_sets.db)
    disconnect_database.assert_awaited_once_with()
