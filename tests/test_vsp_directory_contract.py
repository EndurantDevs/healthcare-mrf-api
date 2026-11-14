# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic source-error, identity and complete geographic coverage checks."""

import math

import pytest

from process.vsp_directory_contract import (
    API_BASE,
    SEARCH_URL,
    VSPDirectoryError,
    office_position,
    search_result,
    validated_url,
)
from process.vsp_directory_geometry import GeoTile, census_request, distance_miles, world_tiles


def test_source_error_inside_success_response_is_never_empty_completion():
    with pytest.raises(VSPDirectoryError, match="vsp_search_source_error"):
        search_result({"totalCounts": 0, "locations": [], "errors": [{"code": "B003"}]})
    with pytest.raises(VSPDirectoryError, match="vsp_search_response_incomplete"):
        search_result({"totalCounts": 4, "locations": []})
    with pytest.raises(VSPDirectoryError, match="vsp_search_response_incomplete"):
        search_result({"totalCounts": 50, "locations": []})
    for payload in ({"totalCounts": 1}, {"totalCounts": 0, "locations": None}, {"totalCounts": False}):
        with pytest.raises(VSPDirectoryError, match="vsp_search_response_invalid"):
            search_result(payload)


@pytest.mark.parametrize(
    "url",
    [
        "https://other.example/provider-web/publicdoctorprofile?officepublicid=1&asofdate=2035-01-01",
        API_BASE + "unrelated",
        API_BASE + "provider-web/%2e%2e/private",
        API_BASE + "provider-web/publicdoctorprofile?officepublicid=1&asofdate=2035-01-01&extra=1",
        API_BASE + "provider-web/publicdoctorprofile?officepublicid=1&asofdate=invalid",
        API_BASE + "provider-web/publicdoctorprofile?officepublicid=1&officepublicid=2&asofdate=2035-01-01",
    ],
)
def test_linked_requests_cannot_escape_source_or_change_identity(url):
    with pytest.raises((VSPDirectoryError, ValueError)):
        validated_url(url, method="GET")
    assert validated_url(SEARCH_URL, method="POST") == SEARCH_URL


def test_world_tiles_cover_boundaries_and_spherical_query_circles():
    tiles = world_tiles()
    assert len(tiles) == 36 and len({tile.key for tile in tiles}) == 36
    for latitude in range(-90, 91, 5):
        for longitude in range(-180, 181, 5):
            assert any(tile.has_point(latitude, longitude) for tile in tiles)
    for tile in tiles:
        request = tile.request("Signature")
        radius = float(request["radius"])
        center = tile.center
        for i in range(7):
            for j in range(7):
                lat = tile.south + (tile.north - tile.south) * i / 6
                lon = tile.west + (tile.east - tile.west) * j / 6
                assert distance_miles(*center, lat, lon) <= radius
    assert float(census_request("Choice")["radius"]) > math.pi * 3958.7613


def test_invalid_publisher_coordinates_cannot_become_discovery_points():
    for bad in ("not-a-coordinate", "NaN", "181"):
        with pytest.raises(VSPDirectoryError, match="^vsp_office_position_invalid$"):
            office_position({"addr": {"latitude": 0, "longitude": bad}})


@pytest.mark.parametrize(
    "tile,children",
    [(GeoTile(30, 30.1, -110, -100), 2), (GeoTile(30, 40, -100, -99.9), 2), (GeoTile(30, 31, -101, -100), 4)],
)
def test_adaptive_splits_preserve_every_part_of_parent(tile, children):
    descendants = tile.split()
    assert len(descendants) == children
    for i in range(11):
        for j in range(11):
            lat = tile.south + (tile.north - tile.south) * i / 10
            lon = tile.west + (tile.east - tile.west) * j / 10
            assert any(child.has_point(lat, lon) for child in descendants)
