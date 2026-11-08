# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete spherical tile coverage for count-reconciled directory searches."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass

from process.vsp_directory_contract import NETWORKS, canonical_json, require

EARTH_MILES = 3958.7613


def distance_miles(latitude_a, longitude_a, latitude_b, longitude_b):
    """Return great-circle distance, including across the date line."""
    lat_a, lat_b = math.radians(latitude_a), math.radians(latitude_b)
    delta_lat = lat_b - lat_a
    delta_lon = math.radians(longitude_b - longitude_a)
    haversine = math.sin(delta_lat / 2) ** 2 + math.cos(lat_a) * math.cos(lat_b) * math.sin(delta_lon / 2) ** 2
    return EARTH_MILES * 2 * math.asin(math.sqrt(min(1.0, max(0.0, haversine))))


@dataclass(frozen=True)
class GeoTile:
    """One closed latitude/longitude rectangle with a covering query circle."""

    south: float
    north: float
    west: float
    east: float
    depth: int = 0

    def __post_init__(self):
        require(
            all(type(x) in (int, float) and math.isfinite(x) for x in (self.south, self.north, self.west, self.east)),
            "vsp_tile_invalid",
        )
        require(-90 <= self.south < self.north <= 90 and -180 <= self.west < self.east <= 180, "vsp_tile_invalid")
        require(self.north - self.south <= 30 and self.east - self.west <= 60, "vsp_tile_exceeds_geometry_contract")
        require(type(self.depth) is int and 0 <= self.depth <= 24, "vsp_tile_invalid")

    @property
    def bounds(self):
        """Return exact resumable bounds without dropping boundary offices."""
        return (self.south, self.north, self.west, self.east)

    @property
    def key(self):
        """Use source-independent geometry identity for shared capture checkpoints."""
        return hashlib.sha256(canonical_json(self.bounds).encode()).hexdigest()

    @property
    def center(self):
        """Return the center of the rectangle, not a ZIP-code centroid."""
        return (self.south + self.north) / 2, (self.west + self.east) / 2

    def has_point(self, latitude, longitude):
        """Keep boundary overlap explicit; office IDs handle deduplication."""
        return self.south <= latitude <= self.north and self.west <= longitude <= self.east

    def request(self, network):
        """Cover every corner, rounding radius outward rather than clipping it."""
        require(network in NETWORKS.values(), "vsp_network_invalid")
        latitude, longitude = self.center
        radius = max(
            distance_miles(latitude, longitude, lat, lon)
            for lat in (self.south, self.north)
            for lon in (self.west, self.east)
        )
        radius = max(0.05, math.ceil(radius * 1.01 * 100) / 100)
        return {
            "searchType": "RADIUS",
            "latitude": str(latitude),
            "longitude": str(longitude),
            "radius": f"{radius:.2f}",
            "networkNames": [network],
            "locationType": ["VSP", "Affiliate"],
            "includePremierFields": False,
            "offset": "1",
            "limit": "50",
        }

    def split(self):
        """Use two children for thin tiles and four otherwise, with no coverage gap."""
        require(self.depth < 24, "vsp_spatial_depth_exhausted")
        lat, lon = self.center
        height = distance_miles(self.south, lon, self.north, lon)
        width = distance_miles(lat, self.west, lat, self.east)
        child_bounds = []
        if width > 2 * height:
            child_bounds = [(self.south, self.north, self.west, lon), (self.south, self.north, lon, self.east)]
        elif height > 2 * width:
            child_bounds = [(self.south, lat, self.west, self.east), (lat, self.north, self.west, self.east)]
        else:
            child_bounds = [
                (south, north, west, east)
                for south, north in ((self.south, lat), (lat, self.north))
                for west, east in ((self.west, lon), (lon, self.east))
            ]
        return tuple(GeoTile(*bounds, depth=self.depth + 1) for bounds in child_bounds)


def world_tiles():
    """Cover the full globe, including territories, poles and both date-line sides."""
    tiles = (
        GeoTile(south, south + 30, west, west + 60) for south in range(-90, 90, 30) for west in range(-180, 180, 60)
    )
    return tuple(sorted(tiles, key=lambda tile: distance_miles(40, -100, *tile.center)))


def census_request(network):
    """Obtain a publisher count from a circle larger than half Earth's circumference."""
    require(network in NETWORKS.values(), "vsp_network_invalid")
    return {
        "searchType": "RADIUS",
        "latitude": "40.0",
        "longitude": "-100.0",
        "radius": "13000",
        "networkNames": [network],
        "locationType": ["VSP", "Affiliate"],
        "includePremierFields": False,
        "offset": "1",
        "limit": "1",
    }
