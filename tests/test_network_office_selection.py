# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact office/geo request boundaries without native authority or I/O."""

import asyncio
from types import SimpleNamespace
from uuid import UUID

import pytest

from api.endpoint.network_providers import _parameters
from api.network_provider_read import (
    NetworkOfficeSelection,
    NetworkProviderReadError,
    office_selection,
    read_network_provider_page,
)

LOCATION = "10000000-0000-0000-0000-000000000001"


@pytest.mark.parametrize("is_detail", [False, True])
def test_exact_office_geo_parameters_round_trip(is_detail):
    request = SimpleNamespace(
        body=b"",
        headers={},
        query_string=f"network_ids=7&network_generation=3&location_id={LOCATION}&lat=34&long=-118&radius_miles=1",
    )
    result = _parameters(request, is_detail=is_detail)
    assert result["office_filters"] == {"location_id": LOCATION, "lat": 34.0, "long": -118.0, "radius_miles": 1.0}
    assert result["network_ids"] == (7,) and result["generation_id"] == 3
    assert office_selection(**result["office_filters"]) == (UUID(LOCATION), 34.0, -118.0, 1.0)


@pytest.mark.parametrize(
    "query",
    [
        "lat=34",
        "lat=34&long=-118",
        "lat=NaN&long=-118&radius_miles=1",
        "lat=34&long=inf&radius_miles=1",
        "lat=91&long=-118&radius_miles=1",
        "lat=34&long=-181&radius_miles=1",
        "lat=34&long=-118&radius_miles=0",
        "lat=34&long=-118&radius_miles=101",
        "location_id=bad",
        "location_id=00000000-0000-0000-0000-000000000000",
        f"location_id={LOCATION}&location_id={LOCATION}",
        "lat=34&lat=34&long=-118&radius_miles=1",
    ],
)
def test_invalid_or_duplicate_office_queries_refuse_before_database(query):
    request = SimpleNamespace(body=b"", headers={}, query_string="network_ids=7&" + query)
    with pytest.raises(ValueError):
        _parameters(request, is_detail=False)


@pytest.mark.parametrize(
    "filters",
    [
        {"location_id": "bad"},
        {"location_id": UUID(LOCATION)},
        {"lat": True, "long": 1, "radius_miles": 1},
        {"lat": 1},
        {"lat": float("nan"), "long": 1, "radius_miles": 1},
    ],
)
def test_invalid_typed_office_filters_refuse_without_connection(filters):
    with pytest.raises(NetworkProviderReadError):
        asyncio.run(read_network_provider_page(None, None, office_filters=NetworkOfficeSelection(**filters)))


def test_legacy_network_member_request_keeps_empty_office_filter():
    request = SimpleNamespace(body=b"", headers={}, query_string="network_ids=7")
    result = _parameters(request, is_detail=False)
    assert result["office_filters"] == {} and result["limit"] == 50 and result["offset"] == 0
