# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib

import pytest


@pytest.fixture
def anchors_module():
    return importlib.import_module("process.facility_anchors")


def test_parse_lat_lng_reads_wkt_location(anchors_module):
    location_by_field = {"Latitude": "", "Longitude": "", "Location": "POINT (-87.623177 41.881832)"}
    lat, lng = anchors_module._parse_lat_lng(location_by_field)
    assert lat == pytest.approx(41.881832)
    assert lng == pytest.approx(-87.623177)


def test_parse_lat_lng_prefers_explicit_columns(anchors_module):
    location_by_field = {"Latitude": "40.7128", "Longitude": "-74.0060", "Location": "POINT (-1.0 2.0)"}
    lat, lng = anchors_module._parse_lat_lng(location_by_field)
    assert lat == pytest.approx(40.7128)
    assert lng == pytest.approx(-74.0060)


def test_normalize_npi_accepts_only_ten_digits(anchors_module):
    assert anchors_module._normalize_npi(" 1234567890 ") == 1234567890
    assert anchors_module._normalize_npi("NPI: 123-456-7890") == 1234567890
    assert anchors_module._normalize_npi("12345") is None
    assert anchors_module._normalize_npi("") is None


def test_normalize_medicare_ccn_keeps_alphanumeric_codes(anchors_module):
    assert anchors_module._normalize_medicare_ccn(" 12-345F ") == "12345F"
    assert anchors_module._normalize_medicare_ccn("") is None


def test_normalize_phone_accepts_ten_digit_us_numbers(anchors_module):
    assert anchors_module._normalize_phone("(312) 555-0199") == "3125550199"
    assert anchors_module._normalize_phone("1-312-555-0199") == "3125550199"
    assert anchors_module._normalize_phone("312") is None
    assert anchors_module._normalize_phone("") is None


def test_stable_hrsa_site_id_prefers_location_identifier(anchors_module):
    site_by_field = {
        "BHCMIS Organization Identification Number": "ORG-1",
        "Health Center Number": "HC-22",
        "Health Center Location Identification Number": " 12345 ",
        "Site Name": "First Name",
        "Site Address": "10 Main St",
        "Site City": "Boston",
        "Site State Abbreviation": "MA",
        "Site Postal Code": "02108",
    }
    changed_site_by_field = {
        **site_by_field,
        "Site Name": "Changed Name",
        "Site Address": "Changed Address",
    }

    assert anchors_module._stable_hrsa_site_id(site_by_field) == anchors_module._stable_hrsa_site_id(changed_site_by_field)
    assert anchors_module._stable_hrsa_site_id(site_by_field) != anchors_module._stable_hrsa_site_id(
        {**site_by_field, "Health Center Location Identification Number": "12346"}
    )
    assert anchors_module._stable_hrsa_site_id(site_by_field) != anchors_module._stable_hrsa_site_id(
        {**site_by_field, "BHCMIS Organization Identification Number": "ORG-2"}
    )


def test_stable_hrsa_site_id_falls_back_to_normalized_source_fields(anchors_module):
    site_by_field = {
        "BHCMIS Organization Identification Number": "ORG-1",
        "Health Center Number": "HC-22",
        "Site Name": "North Clinic",
        "Site Address": "10 Main St.",
        "Site City": "Boston",
        "Site State Abbreviation": "MA",
        "Site Postal Code": "02108-1234",
    }
    equivalent_site_by_field = {
        "BHCMIS Organization Identification Number": "org 1",
        "Health Center Number": "HC22",
        "Site Name": " north clinic ",
        "Site Address": "10 MAIN ST",
        "Site City": "BOSTON",
        "Site State Abbreviation": "ma",
        "Site Postal Code": "02108",
    }

    assert anchors_module._stable_hrsa_site_id(site_by_field) == anchors_module._stable_hrsa_site_id(equivalent_site_by_field)


@pytest.mark.asyncio
async def test_backfill_hospital_coordinates_uses_existing_live_table(monkeypatch, anchors_module):
    calls = []

    async def has_table_column(schema, table, column):
        calls.append(("has_column", schema, table, column))
        return True

    class FakeDb:
        async def status(self, sql):
            calls.append(("status", sql))
            return 7

    monkeypatch.setattr(anchors_module, "_table_has_column", has_table_column)
    monkeypatch.setattr(anchors_module, "db", FakeDb())

    updated = await anchors_module._backfill_hospital_coordinates_from_existing_live(
        "facility_anchor_20260615",
        "mrf",
    )

    assert updated == 7
    sql = calls[-1][1]
    assert "UPDATE mrf.facility_anchor_20260615 AS stage" in sql
    assert "FROM mrf.facility_anchor AS live" in sql
    assert "live.id = stage.id" in sql
    assert "stage.facility_type = 'Hospital'" in sql
