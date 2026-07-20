import importlib
import datetime
import io
import zipfile
from unittest.mock import AsyncMock

import pytest

pharmacy_license = importlib.import_module("process.pharmacy_license")


def test_parse_fda_state_sources_extracts_known_states():
    html = """
    <h2>Board of Pharmacy License Databases by State</h2>
    <ul>
      <li><a href=\"https://example.com/tx\">Texas</a></li>
      <li><a href=\"https://example.com/nm\">New Mexico </a></li>
      <li><a href=\"https://example.com/ok\">Oklahoma</a></li>
    </ul>
    """

    sources = pharmacy_license._parse_fda_state_sources(html)

    assert [item.state_code for item in sources] == ["NM", "OK", "TX"]
    assert sources[0].board_url == "https://example.com/nm"


def test_normalize_license_status_maps_known_values():
    assert pharmacy_license._normalize_license_status("ACTIVE") == "active"
    assert pharmacy_license._normalize_license_status("License suspended") == "suspended"
    assert pharmacy_license._normalize_license_status("revoked by board") == "revoked"
    assert pharmacy_license._normalize_license_status("expired") == "expired"
    assert pharmacy_license._normalize_license_status("Clear") == "active"
    assert pharmacy_license._normalize_license_status("Null And Void") == "inactive"
    assert pharmacy_license._normalize_license_status(None) == "unknown"


@pytest.mark.asyncio
async def test_materialize_snapshot_aborts_on_canonical_address_failure(monkeypatch):
    monkeypatch.setattr(pharmacy_license, "source_enabled", lambda source: source == "pharmacy_license")

    async def fail_stamp(*_args, **_kwargs):
        raise RuntimeError("collision")

    monkeypatch.setattr(pharmacy_license, "stamp_address_keys", fail_stamp)

    with pytest.raises(
        pharmacy_license.PharmacyLicenseCanonicalAddressError,
        match="canonical address resolve failed",
    ):
        await pharmacy_license._materialize_snapshot("mrf", "snapshot_1", "run_1")


@pytest.mark.asyncio
async def test_start_marks_run_failed_on_canonical_address_failure(monkeypatch):
    """Verify canonical-address failures terminalize all license run state."""

    run_updates = []
    snapshot_updates = []
    coverage_updates = []
    control_updates = []

    async def noop(*_args, **_kwargs):
        return None

    async def fake_download(*_args, **_kwargs):
        return """
        <h2>Board of Pharmacy License Databases by State</h2>
        <a href="https://example.com/tx">Texas</a>
        """

    async def fake_import_state(*_args, **_kwargs):
        return pharmacy_license.StateImportStats(
            supported=True,
            status="completed",
            source_url="https://example.com/tx.csv",
            unsupported_reason=None,
            error_text=None,
            row_count_parsed=1,
            row_count_matched=1,
            row_count_dropped=0,
            row_count_inserted=0,
            metadata={},
        )

    async def fail_materialize(*_args, **_kwargs):
        raise pharmacy_license.PharmacyLicenseCanonicalAddressError("canonical collision")

    class FakeClientSession:
        def __init__(self, *_args, **_kwargs):
            self.closed = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return False

    async def fake_upsert_run(payload):
        run_updates.append(payload)

    async def fake_upsert_snapshot(payload):
        snapshot_updates.append(payload)

    async def fake_upsert_coverage(payload):
        coverage_updates.append(payload)

    async def fake_mark_control_run(run_id, **payload):
        control_updates.append({"run_id": run_id, **payload})

    monkeypatch.setattr(pharmacy_license, "ensure_database", noop)
    monkeypatch.setattr(pharmacy_license, "_ensure_tables", AsyncMock(return_value="mrf"))
    monkeypatch.setattr(pharmacy_license, "_truncate_stage_table", noop)
    monkeypatch.setattr(pharmacy_license, "_drop_secondary_indexes", noop)
    monkeypatch.setattr(pharmacy_license, "_ensure_secondary_indexes", noop)
    monkeypatch.setattr(pharmacy_license, "_analyze_tables", noop)
    monkeypatch.setattr(pharmacy_license, "download_it", fake_download)
    monkeypatch.setattr(pharmacy_license, "_import_state_source", fake_import_state)
    monkeypatch.setattr(pharmacy_license, "_materialize_snapshot", fail_materialize)
    monkeypatch.setattr(pharmacy_license, "_upsert_run", fake_upsert_run)
    monkeypatch.setattr(pharmacy_license, "_upsert_snapshot", fake_upsert_snapshot)
    monkeypatch.setattr(pharmacy_license, "_upsert_coverage", fake_upsert_coverage)
    monkeypatch.setattr(pharmacy_license, "mark_control_run", fake_mark_control_run)
    monkeypatch.setattr(pharmacy_license, "enqueue_live_progress", lambda **_payload: None)
    monkeypatch.setattr(pharmacy_license.aiohttp, "ClientSession", FakeClientSession)

    with pytest.raises(pharmacy_license.PharmacyLicenseCanonicalAddressError, match="canonical collision"):
        await pharmacy_license.pharmacy_license_start(
            {},
            {"run_id": "run_1", "import_id": "import_1", "test_mode": True},
        )

    assert run_updates[-1]["status"] == "failed"
    assert run_updates[-1]["error_text"] == "canonical collision"
    assert snapshot_updates[-1]["status"] == "failed"
    assert coverage_updates[-1]["status"] == "failed"
    assert control_updates[-1]["status"] == "failed"
    assert control_updates[-1]["error"]["message"] == "canonical collision"


def test_normalize_stage_row_drops_missing_npi():
    source = pharmacy_license.StateSource(state_code="TX", state_name="Texas", board_url="https://example.com/tx")
    row_map = {
        "License Number": "TX-1234",
        "License Status": "Active",
    }

    payload, reason = pharmacy_license._normalize_stage_row(
        row_map,
        run_id="run_1",
        snapshot_id="snap_1",
        state_source=source,
        source_url="https://example.com/feed.csv",
        imported_at=datetime.datetime(2026, 3, 10, 0, 0, 0),
    )

    assert payload is None
    assert reason == "missing_npi"


def test_normalize_stage_row_maps_interesting_fields():
    state_source = pharmacy_license.StateSource(
        state_code="TX",
        state_name="Texas",
        board_url="https://example.com/tx",
    )
    license_field_map = {
        "NPI": "1518379601",
        "License Number": "TX-PH-00001",
        "License Type": "Pharmacy",
        "License Status": "Active",
        "Expiration Date": "2027-01-31",
        "Issue Date": "2019-05-01",
        "Entity Name": "Sample Pharmacy",
        "DBA": "Sample RX",
        "Address": "100 Main",
        "City": "Austin",
        "State": "TX",
        "Zip": "78701",
        "Phone": "555-555-5555",
        "Disciplinary Summary": "",
        "Last Updated": "2026-02-15",
    }

    stage_payload, reason = pharmacy_license._normalize_stage_row(
        license_field_map,
        run_id="run_1",
        snapshot_id="snap_1",
        state_source=state_source,
        source_url="https://example.com/feed.csv",
        imported_at=datetime.datetime(2026, 3, 10, 0, 0, 0),
    )

    assert reason is None
    assert stage_payload is not None
    assert stage_payload["npi"] == 1518379601
    assert stage_payload["license_number"] == "TX-PH-00001"
    assert stage_payload["license_status"] == "active"
    assert stage_payload["license_expiration_date"].isoformat() == "2027-01-31"
    assert stage_payload["state_code"] == "TX"


def test_extract_candidate_file_links_filters_noise():
    html = """
    <a href=\"/export/pharmacy.csv\">CSV</a>
    <a href=\"https://example.com/sitemap.xml\">Sitemap</a>
    <a href=\"/download/pharmacy.json\">JSON</a>
    """

    links = pharmacy_license._extract_candidate_file_links(html, "https://state.example.com/lookup")

    assert "https://state.example.com/export/pharmacy.csv" in links
    assert "https://state.example.com/download/pharmacy.json" in links
    assert all("sitemap" not in link for link in links)


def test_parse_datagrid_rows_handles_nested_aspnet_cells():
    html = """
    <table id="datagrid_results">
      <tr>
        <th>Name</th><th>License #</th><th>License Type</th><th>Status</th><th>Address</th>
      </tr>
      <tr>
        <td>
          <table role="presentation">
            <tr><td><a href="Details.aspx?result=abc">2200 PHARMACY INC</a></td></tr>
            <tr><td></td></tr>
          </table>
        </td>
        <td><span>60002818A</span></td>
        <td><span>Pharmacy</span></td>
        <td><span>Expired</span></td>
        <td><span>GARY IN 46404</span></td>
      </tr>
    </table>
    """

    parsed_rows = pharmacy_license._parse_datagrid_rows(html)

    assert len(parsed_rows) == 1
    assert parsed_rows[0]["Name"] == "2200 PHARMACY INC"
    assert parsed_rows[0]["License #"] == "60002818A"
    assert parsed_rows[0]["Address"] == "GARY IN 46404"

    hydrated = pharmacy_license._hydrate_row_with_address_parts(parsed_rows[0])
    assert hydrated["City"] == "GARY"
    assert hydrated["State"] == "IN"
    assert hydrated["Zip"] == "46404"


def test_extract_postback_targets_parses_numeric_pager_links():
    html = """
    <a href="javascript:__doPostBack(&#39;datagrid_results$_ctl44$_ctl1&#39;,&#39;&#39;)"><font>2</font></a>
    <a href="javascript:__doPostBack(&#39;datagrid_results$_ctl44$_ctl2&#39;,&#39;&#39;)"><font>3</font></a>
    <a href="javascript:__doPostBack(&#39;datagrid_results$_ctl44$_ctl3&#39;,&#39;&#39;)"><font>Next</font></a>
    """

    targets = pharmacy_license._extract_postback_targets(html)

    assert targets == {
        2: "datagrid_results$_ctl44$_ctl1",
        3: "datagrid_results$_ctl44$_ctl2",
    }


def test_normalize_stage_row_uses_npi_resolver_when_npi_missing():
    resolver = pharmacy_license.StateNpiResolver(state_code="IN")
    resolver.by_license = {"60002818A": 1518379601}
    source = pharmacy_license.StateSource(state_code="IN", state_name="Indiana", board_url="https://example.com")
    row_map = {
        "Name": "2200 PHARMACY INC",
        "License #": "60002818A",
        "License Type": "Pharmacy",
        "Status": "Active",
        "Address": "GARY IN 46404",
        "City": "GARY",
        "State": "IN",
        "Zip": "46404",
    }

    payload, reason = pharmacy_license._normalize_stage_row(
        row_map,
        run_id="run_1",
        snapshot_id="snap_1",
        state_source=source,
        source_url="https://example.com/results",
        imported_at=datetime.datetime(2026, 3, 10, 0, 0, 0),
        npi_resolver=resolver,
    )

    assert reason is None
    assert payload is not None
    assert payload["npi"] == 1518379601


def test_normalize_stage_row_uses_other_identifier_resolver_when_npi_missing():
    resolver = pharmacy_license.StateNpiResolver(state_code="MA")
    resolver.by_other_identifier = {"MAPH00123": 1518379602}
    source = pharmacy_license.StateSource(state_code="MA", state_name="Massachusetts", board_url="https://example.com")
    row_map = {
        "License Number": "MA-PH-00123",
        "License Type": "Pharmacy",
        "License Status": "Active",
        "Entity Name": "Sample Pharmacy",
        "City": "Boston",
        "State": "MA",
        "Zip": "02108",
    }

    payload, reason = pharmacy_license._normalize_stage_row(
        row_map,
        run_id="run_1",
        snapshot_id="snap_1",
        state_source=source,
        source_url="https://example.com/results",
        imported_at=datetime.datetime(2026, 3, 10, 0, 0, 0),
        npi_resolver=resolver,
    )

    assert reason is None
    assert payload is not None
    assert payload["npi"] == 1518379602
    assert resolver.stats == {"other_identifier": 1}


def test_normalize_stage_row_uses_other_identifier_digits_resolver_when_needed():
    resolver = pharmacy_license.StateNpiResolver(state_code="MA")
    resolver.by_other_identifier_digits = {"1200345": 1518379603}
    source = pharmacy_license.StateSource(state_code="MA", state_name="Massachusetts", board_url="https://example.com")
    row_map = {
        "License Number": "12-00345",
        "License Type": "Pharmacy",
        "License Status": "Active",
        "Entity Name": "Digits Pharmacy",
        "City": "Boston",
        "State": "MA",
        "Zip": "02109",
    }

    payload, reason = pharmacy_license._normalize_stage_row(
        row_map,
        run_id="run_1",
        snapshot_id="snap_1",
        state_source=source,
        source_url="https://example.com/results",
        imported_at=datetime.datetime(2026, 3, 10, 0, 0, 0),
        npi_resolver=resolver,
    )

    assert reason is None
    assert payload is not None
    assert payload["npi"] == 1518379603
    assert resolver.stats == {"other_identifier_digits": 1}


def test_state_npi_resolver_name_fallback_requires_partd_quality_gate():
    resolver = pharmacy_license.StateNpiResolver(state_code="NJ")
    resolver.by_name_zip = {("samplepharmacy", "07001"): 1518379604}

    blocked = resolver.resolve(
        license_number=None,
        entity_name="Sample Pharmacy",
        dba_name=None,
        city="Newark",
        zip_code="07001",
    )
    assert blocked is None

    resolver.partd_name_fallback_enabled = True
    allowed = resolver.resolve(
        license_number=None,
        entity_name="Sample Pharmacy",
        dba_name=None,
        city="Newark",
        zip_code="07001",
    )
    assert allowed == 1518379604
    assert resolver.stats["name_zip"] == 1


def test_state_npi_resolver_uses_registry_name_city_before_partd_fallback():
    resolver = pharmacy_license.StateNpiResolver(state_code="NJ")
    resolver.by_registry_name_city = {("acmepharmacy", "newark"): 1518379605}
    resolver.by_name_city = {("acmepharmacy", "newark"): 1518379999}
    resolver.partd_name_fallback_enabled = True

    mapped = resolver.resolve(
        license_number=None,
        entity_name="Acme Pharmacy",
        dba_name=None,
        city="Newark",
        zip_code=None,
    )

    assert mapped == 1518379605
    assert resolver.stats == {"registry_name_city": 1}


def test_name_candidates_for_match_handles_dba_and_department_suffix():
    keys = pharmacy_license._name_candidates_for_match(
        "ACME MARKETS, INC., D/B/A ACME PHARMACY DEPT. 1054",
        None,
    )

    assert "acmemarketsincdbaacmepharmacydept1054" in keys
    assert "acmepharmacy" in keys


def test_license_like_identifier_issuer_filters_non_license_values():
    assert pharmacy_license._is_license_like_identifier_issuer("STATE LICENSE") is True
    assert pharmacy_license._is_license_like_identifier_issuer("Medical License") is True
    assert pharmacy_license._is_license_like_identifier_issuer("AETNA") is False
    assert pharmacy_license._is_license_like_identifier_issuer(None) is False


def test_partd_quality_gate_requires_rows_name_and_location():
    assert (
        pharmacy_license._partd_name_fallback_quality_ok(total_rows=10, named_rows=10, city_rows=10, zip_rows=10)
        is False
    )
    assert (
        pharmacy_license._partd_name_fallback_quality_ok(total_rows=50, named_rows=0, city_rows=50, zip_rows=50)
        is False
    )
    assert (
        pharmacy_license._partd_name_fallback_quality_ok(total_rows=50, named_rows=10, city_rows=0, zip_rows=0)
        is False
    )
    assert (
        pharmacy_license._partd_name_fallback_quality_ok(total_rows=50, named_rows=10, city_rows=5, zip_rows=0)
        is True
    )


def test_create_aspnet_adapter_spec_for_supported_state():
    source = pharmacy_license.StateSource(
        state_code="NJ",
        state_name="New Jersey",
        board_url="https://newjersey.mylicense.com/verification/",
    )

    spec = pharmacy_license._create_aspnet_adapter_spec(source)

    assert spec is not None
    assert "newjersey.mylicense.com" in spec.search_url


def test_to_date_supports_us_date_formats():
    assert pharmacy_license._to_date("08/31/2026").isoformat() == "2026-08-31"
    assert pharmacy_license._to_date("08-31-2026").isoformat() == "2026-08-31"


def test_map_tx_csv_row_maps_key_fields():
    row_map = {
        "LIC_NBR": "33377",
        "ENTITY_NBR": "1102384",
        "PHARMACY_NAME": "AVITA PHARMACY 1034",
        "ADDRESS1": "2800 S IH35 FRONTAGE ROAD SUITE 105",
        "ADDRESS2": "",
        "CITY": "AUSTIN",
        "STATE": "TX",
        "ZIP": "78704",
        "PHONE": "(512) 213-4030",
        "LIC_STATUS": "Active",
        "LIC_EXPR_DATE": "08/31/2026",
        "LIC_ORIG_DATE": "08/24/2020",
        "DISP ACTN": "N",
        "PHY TYPE": "Community Independent",
        "CLASS": "Community Pharmacy",
    }

    mapped = pharmacy_license._map_tx_csv_row(row_map)

    assert mapped["License Number"] == "33377"
    assert mapped["Entity Name"] == "AVITA PHARMACY 1034"
    assert mapped["License Status"] == "Active"
    assert mapped["License Type"] == "Community Independent"
    assert mapped["State"] == "TX"


def test_map_fl_csv_row_maps_key_fields():
    row_map = {
        "License Number": "PH4",
        " Profession ": "Pharmacy",
        " Org Name": "LANIER PHARMACY, INC",
        " DBA Name": "",
        " Address": "45 AVENUE D",
        " City": "APALACHICOLA",
        " License Status": "Closed",
    }

    mapped = pharmacy_license._map_fl_csv_row(row_map)

    assert mapped["License Number"] == "PH4"
    assert mapped["Entity Name"] == "LANIER PHARMACY, INC"
    assert mapped["License Type"] == "Pharmacy"
    assert mapped["License Status"] == "Closed"
    assert mapped["City"] == "APALACHICOLA"
    assert mapped["State"] == "FL"


def test_map_co_socrata_row_maps_license_fields():
    row_map = {
        "licensetype": "PDO",
        "lastname": "",
        "firstname": "",
        "middlename": "",
        "licensenumber": "1680000102",
        "city": "Norwood",
        "state": "CO",
        "mailzipcode": "81423",
        "licensestatusdescription": "Active",
        "licenseexpirationdate": "2026-10-31T00:00:00.000",
        "licensefirstissuedate": "2024-02-01T00:00:00.000",
        "licenselastreneweddate": "2024-10-31T00:00:00.000",
    }

    mapped = pharmacy_license._map_co_socrata_row(row_map)

    assert mapped["License Number"] == "1680000102"
    assert mapped["License Type"] == "PDO"
    assert mapped["License Status"] == "Active"
    assert mapped["City"] == "Norwood"


def test_map_wa_socrata_row_maps_license_fields():
    row_map = {
        "credentialnumber": "VA1234567",
        "lastname": "Pharmacy",
        "firstname": "Sample",
        "middlename": "WA",
        "credentialtype": "Pharmacist License",
        "status": "ACTIVE",
        "firstissuedate": "20160229",
        "lastissuedate": "20250201",
        "expirationdate": "20260501",
        "actiontaken": "No",
    }

    mapped = pharmacy_license._map_wa_socrata_row(row_map)

    assert mapped["License Number"] == "VA1234567"
    assert mapped["Entity Name"] == "Sample WA Pharmacy"
    assert mapped["License Type"] == "Pharmacist License"
    assert mapped["State"] == "WA"


def test_map_ny_rosa_row_maps_address_and_discipline():
    row_map = {
        "registrationNumber": "000001",
        "type": {"value": "Pharmacy", "label": "Type"},
        "legalName": {"value": "J. LEON LASCOFF & SON INC.", "label": "Legal Name"},
        "tradeName": {"value": "LASCOFF RX", "label": "Trade Name"},
        "status": {"value": "Registered", "label": "Status"},
        "address": {"value": "1209 LEXINGTON AVE. NEW YORK NY 10028", "label": "Street Address"},
        "dateFirstRegistered": {"value": "August 11, 1931"},
        "dateRegistrationBegins": {"value": "January 01, 2026"},
        "dateRegisteredThrough": {"value": "December 31, 2027"},
        "enforcementActions": [{"action": {"value": "Consent order"}}],
    }

    mapped = pharmacy_license._map_ny_rosa_row(row_map)

    assert mapped["License Number"] == "000001"
    assert mapped["Entity Name"] == "J. LEON LASCOFF & SON INC."
    assert mapped["DBA"] == "LASCOFF RX"
    assert mapped["City"] == "NEW YORK"
    assert mapped["State"] == "NY"
    assert mapped["Zip"] == "10028"
    assert mapped["Disciplinary Flag"] is True


def test_ma_license_type_is_pharmacy_facility_filters_individuals():
    assert pharmacy_license._ma_license_type_is_pharmacy_facility("Retail Pharmacy License") is True
    assert pharmacy_license._ma_license_type_is_pharmacy_facility("Pharmacist License") is False
    assert pharmacy_license._ma_license_type_is_pharmacy_facility("Pharmacy Technician License") is False


def test_parse_zip_records_handles_nested_archives_and_skips_metadata_files():
    inner_buf = io.BytesIO()
    with zipfile.ZipFile(inner_buf, "w", compression=zipfile.ZIP_DEFLATED) as inner:
        inner.writestr(
            "Retail_Pharmacy_Data.csv",
            "License Number,License Type,License Status,Organization Name,State\n"
            "MA123,Retail Pharmacy License,Current,Sample Pharmacy,MA\n",
        )
        inner.writestr(
            "Retail_Pharmacy_Metadata.csv",
            "Column Name,Data Type,Description\nLicense Number,String,Identifier\n",
        )

    outer_buf = io.BytesIO()
    with zipfile.ZipFile(outer_buf, "w", compression=zipfile.ZIP_DEFLATED) as outer:
        outer.writestr("Board_of_Registration_in_Pharmacy_Export.zip", inner_buf.getvalue())

    rows = pharmacy_license._parse_zip_records(outer_buf.getvalue())

    assert len(rows) == 1
    assert rows[0]["License Number"] == "MA123"
    assert rows[0]["Organization Name"] == "Sample Pharmacy"


def test_identifier_and_source_normalizers_cover_supported_edge_cases():
    generated_run_id = pharmacy_license._normalize_run_id(None)
    punctuation_only_run_id = pharmacy_license._normalize_run_id("***")

    assert pharmacy_license._normalize_run_id(" run/id ") == "run_id"
    assert len(generated_run_id) == 23
    assert generated_run_id[14] == "_"
    assert len(punctuation_only_run_id) == 23
    assert pharmacy_license._normalize_import_id(None).isdigit()
    assert pharmacy_license._normalize_import_id("monthly/2026-07") == "monthly_2026_07"
    assert pharmacy_license._normalize_import_id("***") == "___"
    assert pharmacy_license._normalize_key(" License Number ") == "licensenumber"
    assert pharmacy_license._safe_text(None) is None
    assert pharmacy_license._safe_text("   ") is None
    assert pharmacy_license._safe_text(" value ") == "value"
    assert pharmacy_license._strip_html_tags("<b>A &amp; B</b>") == "A & B"
    assert pharmacy_license._normalize_license_for_match(None) is None
    assert pharmacy_license._normalize_license_for_match(" ma-ph 12 ") == "MAPH12"
    assert pharmacy_license._normalize_zip_for_match("12") is None
    assert pharmacy_license._normalize_zip_for_match("02108-1234") == "02108"
    assert pharmacy_license._normalize_digits_for_match(None) is None
    assert pharmacy_license._normalize_digits_for_match("MA-12-03") == "1203"
    assert pharmacy_license._normalize_city_for_match(None) is None
    assert pharmacy_license._normalize_city_for_match("  New-York  ") == "new york"
    assert pharmacy_license._normalize_name_for_match(None) is None
    assert pharmacy_license._normalize_name_for_match("Acme Pharmacy, Inc.") == "acmepharmacyinc"
    assert (
        pharmacy_license._normalize_name_for_match("Acme Pharmacy, Inc.", loose=True)
        == "acmepharmacy"
    )
    assert pharmacy_license._iter_name_variants_for_match(None) == []
    assert pharmacy_license._unique_mapping({"A": {1}, "B": {2, 3}}) == {"A": 1}
    assert pharmacy_license._state_code_for_name("  New   York ") == "NY"
    assert pharmacy_license._state_code_for_name("Atlantis") is None

    snapshot_id = pharmacy_license._hash_snapshot_id("run", "MA", "https://example.com")
    assert snapshot_id.startswith("run:MA:")
    assert len(snapshot_id) == len("run:MA:") + 12
    assert pharmacy_license._entry_extensions(None) == ""
    assert pharmacy_license._entry_extensions("https://example.com/a.CSV?x=1") == "csv"
    assert pharmacy_license._entry_extensions("https://example.com/a.json") == "json"
    assert pharmacy_license._entry_extensions("https://example.com/a.zip") == "zip"
    assert pharmacy_license._entry_extensions("https://example.com/a.xml") == "xml"
    assert pharmacy_license._entry_extensions("https://example.com/a.txt") == ""
    assert pharmacy_license._is_noise_link("https://example.com/sitemap.xml") is True
    assert pharmacy_license._is_noise_link("https://example.com/licenses.csv") is False


def test_date_boolean_and_npi_normalizers_cover_invalid_and_wrapped_values():
    assert pharmacy_license._to_date(None) is None
    assert pharmacy_license._to_date("not-a-date") is None
    assert pharmacy_license._to_date("recorded 2026-02-28") == datetime.date(2026, 2, 28)
    assert pharmacy_license._to_date("recorded 2026-02-30") is None
    assert pharmacy_license._to_date("08/31/26") == datetime.date(2026, 8, 31)
    assert pharmacy_license._to_date("08-31-26") == datetime.date(2026, 8, 31)

    assert pharmacy_license._to_bool(None) is None
    assert pharmacy_license._to_bool(True) is True
    assert pharmacy_license._to_bool(" ") is None
    assert pharmacy_license._to_bool("YES") is True
    assert pharmacy_license._to_bool("closed") is False
    assert pharmacy_license._to_bool("maybe") is None

    assert pharmacy_license._to_npi("1-1234567890-0") == 1234567890
    assert pharmacy_license._to_npi("1-2345678901") == 2345678901
    assert pharmacy_license._to_npi("123") is None
    assert pharmacy_license._to_npi("0000000000") is None


def test_state_npi_resolver_uses_each_partd_name_fallback_level():
    resolver = pharmacy_license.StateNpiResolver(
        state_code="MA",
        by_license={"MAPH1": 1111111111},
        by_name_city={("cityrx", "boston"): 2222222222},
        by_name={"namerx": 3333333333},
        partd_name_fallback_enabled=True,
    )

    assert resolver.resolve(
        license_number="MA-PH-1",
        entity_name=None,
        dba_name=None,
        city=None,
        zip_code=None,
    ) == 1111111111
    assert resolver.resolve(
        license_number=None,
        entity_name="City RX",
        dba_name=None,
        city="Boston",
        zip_code=None,
    ) == 2222222222
    assert resolver.resolve(
        license_number=None,
        entity_name="Name RX",
        dba_name=None,
        city=None,
        zip_code=None,
    ) == 3333333333
    assert resolver.resolve(
        license_number=None,
        entity_name="Missing RX",
        dba_name=None,
        city=None,
        zip_code=None,
    ) is None
    assert resolver.stats == {"license": 1, "name_city": 1, "name": 1}


def test_aspnet_form_helpers_cover_empty_and_fallback_paths():
    page_html = """
    <form action="results.aspx">
      <input type="hidden" name="ignored" value="x">
      <input type="hidden" name="__VIEWSTATE" value="a&amp;b">
      <input name="t_web_lookup__full_name">
      <select name="t_web_lookup__profession_name">
        <option value="">Select</option>
        <option value="PH">Pharmacy</option>
      </select>
    </form>
    """

    assert pharmacy_license._extract_hidden_fields(page_html) == {"__VIEWSTATE": "a&b"}
    assert pharmacy_license._extract_lookup_field_names(page_html) == {
        "t_web_lookup__full_name",
        "t_web_lookup__profession_name",
    }
    options = pharmacy_license._extract_select_options(
        page_html,
        "t_web_lookup__profession_name",
    )
    assert options == [("", "Select"), ("PH", "Pharmacy")]
    assert pharmacy_license._extract_select_options(page_html, "missing") == []
    assert pharmacy_license._pick_pharmacy_option([]) is None
    assert pharmacy_license._pick_pharmacy_option(options) == "PH"
    assert pharmacy_license._pick_pharmacy_option(
        [("TECH", "Pharmacy Technician"), ("RX", "Retail Pharmacy")],
        prefer_facility=True,
    ) == "RX"
    assert pharmacy_license._pick_pharmacy_option([("GRAD", "Graduate Pharmacist")]) == "GRAD"
    assert pharmacy_license._pick_pharmacy_option([("MD", "Physician")]) is None
    assert pharmacy_license._pick_exact_option(options, "") is None
    assert pharmacy_license._pick_exact_option(options, "pharmacy") == "PH"
    assert pharmacy_license._pick_exact_option(options, "pharmacist") is None
    assert pharmacy_license._extract_form_action("<div />", "https://example.com/search") == (
        "https://example.com/search"
    )
    assert pharmacy_license._extract_form_action(
        "<form action='results.aspx'>",
        "https://example.com/search",
    ) == "https://example.com/results.aspx"
    assert pharmacy_license._hydrate_row_with_address_parts({"Name": "A"}) == {"Name": "A"}
    assert pharmacy_license._hydrate_row_with_address_parts(
        {"Address": "not a city-state-zip"}
    ) == {"Address": "not a city-state-zip"}
    assert pharmacy_license._hydrate_row_with_address_parts(
        {"Address": "Boston MA 02108", "City": "Boston", "State": "MA"}
    ) == {"Address": "Boston MA 02108", "City": "Boston", "State": "MA"}
    assert pharmacy_license._is_captcha_page("Please solve the CAPTCHA") is True
    assert pharmacy_license._is_captcha_page("license results") is False
