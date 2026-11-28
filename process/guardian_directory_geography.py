# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate survey rectangles against independent state boundaries and postal assignments."""

import csv
import gzip
import json
from collections import defaultdict
from functools import cache
from pathlib import Path

from shapely import box, union_all
from shapely.geometry import shape

JURISDICTIONS = frozenset(
    "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM "
    "NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY AS GU MP PR VI".split()
)
_REFERENCE = Path(__file__).resolve().parents[1] / "support" / "zip"


@cache
def _read_reference(directory):
    """Load the packaged polygons and physical ZIP/state pairs without contacting the finder."""
    boundary_by_state = {}
    with gzip.open(directory / "census-state-boundaries-2025.geojson.gz", "rt", encoding="utf-8") as stream:
        for feature in json.load(stream)["features"]:
            state = feature["properties"]["STUSPS"]
            geometry = shape(feature["geometry"])
            if (
                state in boundary_by_state
                or geometry.geom_type not in {"Polygon", "MultiPolygon"}
                or geometry.is_empty
                or not geometry.is_valid
            ):
                raise ValueError("guardian_jurisdiction_reference_invalid")
            boundary_by_state[state] = geometry
    if boundary_by_state.keys() != JURISDICTIONS:
        raise ValueError("guardian_jurisdiction_reference_incomplete")
    with (directory / "zip_codes_list.csv").open(encoding="utf-8", newline="") as stream:
        postal_pairs = {(row["PHYSICAL STATE"], row["PHYSICAL ZIP"]) for row in csv.DictReader(stream)}
    return boundary_by_state, postal_pairs


def validate_region_coverage(regions):
    """Require each state's own rectangles to cover every part of its reference geometry."""
    boundaries, postal_pairs = _read_reference(_REFERENCE)
    rectangles = defaultdict(list)
    for region in regions:
        if (region.state, region.zip_code) not in postal_pairs:
            raise ValueError("guardian_region_postal_state_invalid")
        tile = region.tile
        rectangles[region.state].append(box(tile.west, tile.south, tile.east, tile.north))
    for state, boundary in boundaries.items():
        if not union_all(rectangles[state]).covers(boundary):
            raise ValueError("guardian_jurisdiction_coverage_incomplete")
