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
