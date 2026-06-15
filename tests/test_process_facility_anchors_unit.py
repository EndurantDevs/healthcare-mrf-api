# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib

import pytest


@pytest.fixture
def anchors_module():
    return importlib.import_module("process.facility_anchors")


def test_parse_lat_lng_reads_wkt_location(anchors_module):
    row = {"Latitude": "", "Longitude": "", "Location": "POINT (-87.623177 41.881832)"}
    lat, lng = anchors_module._parse_lat_lng(row)
    assert lat == pytest.approx(41.881832)
    assert lng == pytest.approx(-87.623177)


def test_parse_lat_lng_prefers_explicit_columns(anchors_module):
    row = {"Latitude": "40.7128", "Longitude": "-74.0060", "Location": "POINT (-1.0 2.0)"}
    lat, lng = anchors_module._parse_lat_lng(row)
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
