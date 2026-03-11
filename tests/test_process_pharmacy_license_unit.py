import importlib
import datetime
import io
import zipfile

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


def test_normalize_stage_row_drops_missing_npi():
    source = pharmacy_license.StateSource(state_code="TX", state_name="Texas", board_url="https://example.com/tx")
    row = {
        "License Number": "TX-1234",
        "License Status": "Active",
    }

    payload, reason = pharmacy_license._normalize_stage_row(
        row,
        run_id="run_1",
        snapshot_id="snap_1",
        state_source=source,
        source_url="https://example.com/feed.csv",
        imported_at=datetime.datetime(2026, 3, 10, 0, 0, 0),
    )

    assert payload is None
    assert reason == "missing_npi"


def test_normalize_stage_row_maps_interesting_fields():
    source = pharmacy_license.StateSource(state_code="TX", state_name="Texas", board_url="https://example.com/tx")
    row = {
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

    payload, reason = pharmacy_license._normalize_stage_row(
        row,
        run_id="run_1",
        snapshot_id="snap_1",
        state_source=source,
        source_url="https://example.com/feed.csv",
        imported_at=datetime.datetime(2026, 3, 10, 0, 0, 0),
    )

    assert reason is None
    assert payload is not None
    assert payload["npi"] == 1518379601
    assert payload["license_number"] == "TX-PH-00001"
    assert payload["license_status"] == "active"
    assert payload["license_expiration_date"].isoformat() == "2027-01-31"
    assert payload["state_code"] == "TX"


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

    rows = pharmacy_license._parse_datagrid_rows(html)

    assert len(rows) == 1
    assert rows[0]["Name"] == "2200 PHARMACY INC"
    assert rows[0]["License #"] == "60002818A"
    assert rows[0]["Address"] == "GARY IN 46404"

    hydrated = pharmacy_license._hydrate_row_with_address_parts(rows[0])
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
    row = {
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
        row,
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
    row = {
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

    mapped = pharmacy_license._map_tx_csv_row(row)

    assert mapped["License Number"] == "33377"
    assert mapped["Entity Name"] == "AVITA PHARMACY 1034"
    assert mapped["License Status"] == "Active"
    assert mapped["License Type"] == "Community Independent"
    assert mapped["State"] == "TX"


def test_map_fl_csv_row_maps_key_fields():
    row = {
        "License Number": "PH4",
        " Profession ": "Pharmacy",
        " Org Name": "LANIER PHARMACY, INC",
        " DBA Name": "",
        " Address": "45 AVENUE D",
        " City": "APALACHICOLA",
        " License Status": "Closed",
    }

    mapped = pharmacy_license._map_fl_csv_row(row)

    assert mapped["License Number"] == "PH4"
    assert mapped["Entity Name"] == "LANIER PHARMACY, INC"
    assert mapped["License Type"] == "Pharmacy"
    assert mapped["License Status"] == "Closed"
    assert mapped["City"] == "APALACHICOLA"
    assert mapped["State"] == "FL"


def test_map_co_socrata_row_maps_license_fields():
    row = {
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

    mapped = pharmacy_license._map_co_socrata_row(row)

    assert mapped["License Number"] == "1680000102"
    assert mapped["License Type"] == "PDO"
    assert mapped["License Status"] == "Active"
    assert mapped["City"] == "Norwood"


def test_map_wa_socrata_row_maps_license_fields():
    row = {
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

    mapped = pharmacy_license._map_wa_socrata_row(row)

    assert mapped["License Number"] == "VA1234567"
    assert mapped["Entity Name"] == "Sample WA Pharmacy"
    assert mapped["License Type"] == "Pharmacist License"
    assert mapped["State"] == "WA"


def test_map_ny_rosa_row_maps_address_and_discipline():
    row = {
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

    mapped = pharmacy_license._map_ny_rosa_row(row)

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
