import importlib
import datetime
import io
import zipfile
from unittest.mock import AsyncMock

import pytest

pharmacy_license = importlib.import_module("process.pharmacy_license")
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
