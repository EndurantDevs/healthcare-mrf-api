"""Pharmacy-license normalization and per-state staging contracts."""

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
async def test_state_import_stages_matches_and_exposes_drop_and_resolver_evidence(monkeypatch):
    state_source = _state()
    license_rows = [
        {"License Number": "TX-ONE", "Entity Name": "Sample Pharmacy", "City": "Austin"},
        {"License Number": "TX-MISSING"},
    ]
    resolver = pharmacy_license.StateNpiResolver(state_code="TX", by_license={"TXONE": 1111111111})
    flushed_rows = []

    monkeypatch.setattr(
        pharmacy_license,
        "_load_rows_from_configured_source",
        AsyncMock(return_value=(True, license_rows, "https://files.example.test/licenses.csv", {"adapter": "direct"}, None)),
    )
    monkeypatch.setattr(pharmacy_license, "_build_state_npi_resolver", AsyncMock(return_value=resolver))

    async def flush_batch(batch_rows):
        flushed_rows.extend(batch_rows)
        batch_rows.clear()

    monkeypatch.setattr(pharmacy_license, "_flush_stage_batch", flush_batch)

    stats = await pharmacy_license._import_state_source(
        object(),
        state_source,
        run_id="run-one",
        snapshot_id="snapshot-one",
        test_mode=False,
    )

    assert stats.status == "completed"
    assert stats.row_count_parsed == 2
    assert stats.row_count_matched == 1
    assert stats.row_count_dropped == 1
    assert stats.row_count_inserted == 1
    assert stats.metadata["drop_reasons"] == {"missing_npi": 1}
    assert stats.metadata["npi_match_stats"] == {"license": 1}
    assert stats.metadata["npi_resolver"]["license_keys"] == 1
    assert flushed_rows[0]["npi"] == 1111111111


@pytest.mark.asyncio
async def test_state_import_terminal_source_and_fallback_failures_are_explicit(monkeypatch):
    monkeypatch.setattr(
        pharmacy_license,
        "_load_rows_from_configured_source",
        AsyncMock(return_value=(True, [], None, {"terminal_error": True}, "manual_only")),
    )
    terminal = await pharmacy_license._import_state_source(
        object(),
        _state("ZZ"),
        run_id="run",
        snapshot_id="snapshot",
        test_mode=False,
    )
    assert terminal.status == "unsupported"
    assert terminal.unsupported_reason == "manual_only"

    monkeypatch.setattr(
        pharmacy_license,
        "_load_rows_from_configured_source",
        AsyncMock(return_value=(False, [], None, {}, None)),
    )
    monkeypatch.setattr(pharmacy_license, "_create_aspnet_adapter_spec", lambda _source: None)
    monkeypatch.setattr(
        pharmacy_license,
        "_discover_machine_readable_sources",
        AsyncMock(return_value=(["https://files.example.test/a.csv"], None)),
    )
    monkeypatch.setattr(
        pharmacy_license,
        "_load_records_from_source",
        AsyncMock(return_value=([], "unsupported_source_format")),
    )
    fallback = await pharmacy_license._import_state_source(
        object(),
        _state("ZZ"),
        run_id="run",
        snapshot_id="snapshot",
        test_mode=False,
    )
    assert fallback.status == "unsupported"
    assert fallback.unsupported_reason == "no_parseable_machine_readable_source:unsupported_source_format"
    assert fallback.metadata["source_errors"][0]["url"].endswith("a.csv")


@pytest.mark.asyncio
async def test_state_import_test_mode_returns_completed_no_match_without_writes(monkeypatch):
    monkeypatch.setattr(
        pharmacy_license,
        "_test_rows_for_state",
        lambda _source: [{"License Number": "NO-NPI"}],
    )
    flush = AsyncMock()
    monkeypatch.setattr(pharmacy_license, "_flush_stage_batch", flush)

    stats = await pharmacy_license._import_state_source(
        object(),
        _state(),
        run_id="run",
        snapshot_id="snapshot",
        test_mode=True,
    )

    assert stats.status == "completed_no_match"
    assert stats.unsupported_reason == "no_npi_matchable_rows"
    assert stats.row_count_dropped == 1
    flush.assert_not_awaited()


def test_row_identifier_fallbacks_reject_status_fields_and_accept_labeled_values():
    assert pharmacy_license._pick_license_number({"licensenumber": " "}) is None
    assert pharmacy_license._pick_license_number(
        {
            "name": "Sample Pharmacy",
            "licensestatus": "Active",
            "permitdate": "2026-01-01",
            "permitnumber": " PERMIT-42 ",
        }
    ) == "PERMIT-42"
    assert pharmacy_license._pick_license_number({"name": "Sample Pharmacy"}) is None

    assert pharmacy_license._pick_npi({"npi": "invalid", "name": "Sample", "backupnpi": "1111111111"}) == 1111111111
    assert pharmacy_license._pick_npi({"name": "Sample", "npi": "invalid"}) is None


def test_tabular_parser_skips_empty_malformed_and_pager_rows(monkeypatch):
    class Parser:
        def __init__(self, _table_id):
            self.rows = []

        def feed(self, _html):
            self.page_html = _html

    monkeypatch.setattr(pharmacy_license, "_HtmlTableParser", Parser)
    assert pharmacy_license._parse_datagrid_rows("empty") == []

    Parser.rows = [[]]
    monkeypatch.setattr(Parser, "__init__", lambda self, _table_id: setattr(self, "rows", [[]]))
    assert pharmacy_license._parse_datagrid_rows("empty header") == []

    license_rows = [
        ["Name", "License #"],
        ["", ""],
        ["wrong width"],
        ["2", ""],
        ["Next", ""],
        ["Sample Pharmacy", "A-1"],
    ]
    monkeypatch.setattr(Parser, "__init__", lambda self, _table_id: setattr(self, "rows", license_rows))
    assert pharmacy_license._parse_datagrid_rows("mixed") == [
        {"Name": "Next", "License #": ""},
        {"Name": "Sample Pharmacy", "License #": "A-1"},
    ]


@pytest.mark.parametrize(
    ("address", "expected"),
    [
        (None, (None, None, None)),
        ("not parseable", (None, None, None)),
        ("TX 78701", (None, "TX", "78701")),
        (",,, TX 78701", (None, "TX", "78701")),
        ("100 Main Street New Sample City TX 78701", ("Sample City", "TX", "78701")),
        ("100 Main Street Austin TX 78701", ("Austin", "TX", "78701")),
        ("100 200 Street TX 78701", (None, "TX", "78701")),
    ],
)
def test_freeform_address_tail_uses_only_plausible_city_tokens(address, expected):
    assert pharmacy_license._extract_city_state_zip_from_freeform(address) == expected


def test_fda_and_form_parsers_ignore_unknown_duplicate_and_empty_targets():
    html = """
    Board of Pharmacy License Databases by State
    <a href="https://licenses.example.test/tx">Texas</a>
    <a href="https://licenses.example.test/duplicate">Texas</a>
    <a href="">Florida</a>
    <a href="https://licenses.example.test/unknown">Atlantis</a>
    </ul>
    """
    assert [state_source.board_url for state_source in pharmacy_license._parse_fda_state_sources(html)] == [
        "https://licenses.example.test/tx"
    ]
    assert pharmacy_license._extract_form_action(
        "<form action=''>",
        "https://licenses.example.test/search",
    ) == "https://licenses.example.test/search"
    assert pharmacy_license._extract_postback_targets(
        '<a href="javascript:__doPostBack(bad)">2</a>'
        '<a href="javascript:__doPostBack(\'target\',\'\')">Next</a>'
    ) == {}


@pytest.mark.asyncio
async def test_fallback_source_wins_after_configured_adapter_and_first_candidate_fail(monkeypatch):
    valid_record_by_field = {"NPI": "1111111111", "License Number": "TX-ONE"}
    monkeypatch.setattr(
        pharmacy_license,
        "_load_rows_from_configured_source",
        AsyncMock(return_value=(True, [], None, {"adapter": "direct"}, "configured offline")),
    )
    monkeypatch.setattr(
        pharmacy_license,
        "_discover_machine_readable_sources",
        AsyncMock(
            return_value=(
                ["https://files.example.test/first.csv", "https://files.example.test/second.json"],
                None,
            )
        ),
    )
    monkeypatch.setattr(
        pharmacy_license,
        "_load_records_from_source",
        AsyncMock(side_effect=[([], "bad csv"), ([valid_record_by_field], None)]),
    )
    monkeypatch.setattr(pharmacy_license, "_build_state_npi_resolver", AsyncMock(return_value=None))
    flushed_rows = []

    async def flush(batch_rows):
        flushed_rows.extend(batch_rows)
        batch_rows.clear()

    monkeypatch.setattr(pharmacy_license, "_flush_stage_batch", flush)
    stats = await pharmacy_license._import_state_source(
        object(),
        _state(),
        run_id="run",
        snapshot_id="snapshot",
        test_mode=False,
    )

    assert stats.status == "completed"
    assert stats.source_url.endswith("second.json")
    assert stats.metadata["adapter_error"] == "configured offline"
    assert stats.metadata["source_errors"] == [
        {"url": "https://files.example.test/first.csv", "error": "bad csv"}
    ]
    assert stats.metadata["source_adapter"] == pharmacy_license._STATE_ADAPTER_FALLBACK_MACHINE_READABLE
    assert flushed_rows[0]["license_number"] == "TX-ONE"


@pytest.mark.asyncio
async def test_test_mode_honors_row_and_batch_bounds_during_staging(monkeypatch):
    monkeypatch.setattr(pharmacy_license, "PHARM_LICENSE_TEST_MAX_ROWS_PER_STATE", 1)
    monkeypatch.setattr(pharmacy_license, "PHARM_LICENSE_BATCH_SIZE", 1)
    monkeypatch.setattr(
        pharmacy_license,
        "_test_rows_for_state",
        lambda _source: [
            {"NPI": "1111111111", "License Number": "TX-ONE"},
            {"NPI": "2222222222", "License Number": "TX-TWO"},
        ],
    )
    flushed_rows = []

    async def flush(batch_rows):
        flushed_rows.extend(batch_rows)
        batch_rows.clear()

    monkeypatch.setattr(pharmacy_license, "_flush_stage_batch", flush)
    stats = await pharmacy_license._import_state_source(
        object(),
        _state(),
        run_id="run",
        snapshot_id="snapshot",
        test_mode=True,
    )

    assert stats.row_count_parsed == 1
    assert stats.row_count_inserted == 1
    assert [license_record["license_number"] for license_record in flushed_rows] == ["TX-ONE"]


def test_remaining_parser_fallbacks_are_explicit_and_non_lossy(monkeypatch):
    resolver = pharmacy_license.StateNpiResolver(
        state_code="TX",
        partd_name_fallback_enabled=True,
    )
    assert resolver.resolve(
        license_number="TX-UNKNOWN",
        entity_name="Unknown Pharmacy",
        dba_name=None,
        city="Austin",
        zip_code="78701",
    ) is None
    assert resolver.resolve(
        license_number="TX-123",
        entity_name=None,
        dba_name=None,
        city=None,
        zip_code=None,
    ) is None
    monkeypatch.setattr(pharmacy_license, "_name_candidates_for_match", lambda *_args: [""])
    assert resolver.resolve(
        license_number=None,
        entity_name="ignored",
        dba_name=None,
        city=None,
        zip_code=None,
    ) is None
    assert pharmacy_license._normalize_license_status("novel board value") == "unknown"
    assert pharmacy_license._extract_lookup_field_names(
        "<select name='unrelated'><option>Value</option></select>"
    ) == set()
    assert pharmacy_license._extract_select_options(
        "<select name='kind'><option value='RX'>Pharmacy</option><input name='next'>",
        "kind",
    ) == [("RX", "Pharmacy")]
    assert pharmacy_license._pick_pharmacy_option(
        [("PHARMACIST", "Pharmacist")],
        prefer_facility=True,
    ) == "PHARMACIST"
    assert pharmacy_license._extract_form_action(
        "<form action='   '>",
        "https://licenses.example.test/search",
    ) == "https://licenses.example.test/search"
    assert pharmacy_license._is_ma_pharmacy_facility_license(None) is False
    assert pharmacy_license._parse_fda_state_sources(
        '<a href="https://licenses.example.test/tx">Texas</a>'
    )[0].state_code == "TX"
    parser = pharmacy_license._HtmlTableParser("datagrid_results")
    parser.feed("<table id='datagrid_results'><tr><td> </td></tr></table>")
    assert parser.rows == []


@pytest.mark.asyncio
async def test_resolver_disables_partd_names_when_source_quality_is_insufficient(monkeypatch):
    async def find_schema(table_name, _preferred_schema=None):
        if table_name == pharmacy_license.PartDPharmacyActivity.__tablename__:
            return "mrf"
        return None

    monkeypatch.setattr(pharmacy_license, "_find_table_schema", find_schema)
    monkeypatch.setattr(
        pharmacy_license.db,
        "all",
        AsyncMock(return_value=[(1111111111, "Sample Pharmacy", "Austin", "78701")]),
    )
    monkeypatch.setattr(
        pharmacy_license,
        "_is_partd_name_fallback_acceptable",
        lambda **_counts: False,
    )

    assert await pharmacy_license._build_state_npi_resolver("TX") is None


def test_stage_normalization_reports_missing_license_and_respects_explicit_discipline_flag():
    state_source = _state()
    missing, reason = pharmacy_license._normalize_stage_row(
        {"NPI": "1111111111"},
        run_id="run",
        snapshot_id="snapshot",
        state_source=state_source,
        source_url=state_source.board_url,
        imported_at=pharmacy_license.datetime.datetime(2026, 7, 24),
    )
    assert missing is None
    assert reason == "missing_license_number"

    normalized, reason = pharmacy_license._normalize_stage_row(
        {
            "NPI": "1111111111",
            "License Number": "TX-ONE",
            "Disciplinary Flag": "false",
            "License Status": "Active",
        },
        run_id="run",
        snapshot_id="snapshot",
        state_source=state_source,
        source_url=state_source.board_url,
        imported_at=pharmacy_license.datetime.datetime(2026, 7, 24),
    )
    assert reason is None
    assert normalized["disciplinary_flag"] is False
