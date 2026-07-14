"""Exact coordinate provenance checks for Provider Directory API evidence."""

from __future__ import annotations

import math
from typing import Any, Mapping, Sequence

from scripts.research.provider_directory_api_evidence_models import OverlaySample
from scripts.research.provider_directory_api_evidence_typed import (
    matching_source_summary_maps,
)


def has_detail_geo_source(
    address_row_list: Sequence[Any],
    source_id: str,
    sample: OverlaySample,
) -> bool:
    """Return whether exact detail exposes the selected sourced coordinates."""
    if sample.address_key is None or sample.latitude is None or sample.longitude is None:
        return False
    for address_map in address_row_list:
        if not isinstance(address_map, Mapping):
            continue
        if str(address_map.get("address_key") or "") != sample.address_key:
            continue
        try:
            latitude = float(address_map.get("lat"))
            longitude = float(address_map.get("long"))
        except (TypeError, ValueError):
            continue
        has_coordinates = math.isclose(
            latitude, sample.latitude, rel_tol=0.0, abs_tol=1e-6
        ) and math.isclose(
            longitude, sample.longitude, rel_tol=0.0, abs_tol=1e-6
        )
        if has_coordinates and matching_source_summary_maps(address_map, source_id):
            return True
    return False
