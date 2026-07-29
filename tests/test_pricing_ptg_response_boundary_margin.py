# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pricing query and geographic fallback boundary coverage."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import InvalidUsage

from api.endpoint import pricing


@pytest.fixture(autouse=True)
def _clear_zip_radius_cache():
    pricing._ZIP_RADIUS_ROWS_CACHE.clear()
    yield
    pricing._ZIP_RADIUS_ROWS_CACHE.clear()


def test_pricing_query_scalars_reject_malformed_and_out_of_range_values():
    assert pricing._parse_int([None, "", "7"], "limit", minimum=1) == 7
    assert pricing._parse_int("[]", "limit") is None
    assert pricing._parse_zip_radius_miles(
        "1000",
        param="zip_radius_miles",
        default=10.0,
    ) == pricing.PROCEDURE_ZIP_MAX_RADIUS_MILES

    with pytest.raises(InvalidUsage, match="must be an integer"):
        pricing._parse_int("many", "limit")
    with pytest.raises(InvalidUsage, match="must be >= 1"):
        pricing._parse_int("0", "limit", minimum=1)
    with pytest.raises(InvalidUsage, match="must be numeric"):
        pricing._parse_float("near", "radius")
    with pytest.raises(InvalidUsage, match="must be >= 0"):
        pricing._parse_float("-0.1", "radius", minimum=0)
    with pytest.raises(InvalidUsage, match="must be numeric"):
        pricing._parse_zip_radius_miles(
            "near",
            param="zip_radius_miles",
            default=10.0,
        )
    with pytest.raises(InvalidUsage, match="must be >= 0"):
        pricing._parse_zip_radius_miles(
            "-1",
            param="zip_radius_miles",
            default=10.0,
        )
    assert pricing._normalize_zip5("12-3") is None


def test_default_year_and_distance_boundaries_fail_closed(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PRICING_DEFAULT_YEAR", "not-a-year")
    with pytest.raises(RuntimeError, match="Invalid"):
        pricing._parse_pricing_default_year()

    monkeypatch.setenv("HLTHPRT_PRICING_DEFAULT_YEAR", "2012")
    with pytest.raises(RuntimeError, match="must be >= 2013"):
        pricing._parse_pricing_default_year()

    assert pricing._distance_bucket(None) is None
    assert pricing._distance_bucket(0.0) == "zip_exact"
    assert pricing._distance_bucket(15.0) == "within_20mi"
    assert pricing._distance_bucket(250.0).endswith("mi_plus")


def test_zip_radius_cache_evicts_the_oldest_entry(monkeypatch):
    monkeypatch.setattr(pricing, "_ZIP_RADIUS_ROWS_CACHE_TTL_SECONDS", 30.0)
    monkeypatch.setattr(pricing, "_ZIP_RADIUS_ROWS_CACHE_MAX_KEYS", 1)
    monkeypatch.setattr(pricing.time, "monotonic", lambda: 10.0)
    first_key = ("10001", 5.0, "NY", 10)
    second_key = ("10002", 5.0, "NY", 10)

    pricing._zip_radius_rows_cache_put(first_key, [{"zip5": "10001"}])
    pricing._zip_radius_rows_cache_put(second_key, [{"zip5": "10002"}])

    assert pricing._zip_radius_rows_cache_get(first_key) is None
    assert pricing._zip_radius_rows_cache_get(second_key) == [
        {"zip5": "10002"}
    ]


@pytest.mark.asyncio
async def test_missing_zip_context_returns_and_caches_anchor_fallback(
    monkeypatch,
):
    monkeypatch.setattr(pricing, "_ZIP_RADIUS_ROWS_CACHE_TTL_SECONDS", 30.0)
    monkeypatch.setattr(pricing, "_ZIP_RADIUS_ROWS_CACHE_MAX_KEYS", 4)
    lookup_context = AsyncMock(return_value=None)
    monkeypatch.setattr(pricing, "_lookup_zip_context", lookup_context)
    session = object()

    observed = await pricing._zip_radius_rows(
        session,
        zip5="10001-1234",
        radius_miles=10.0,
        state_hint=" ny ",
        limit=8,
    )

    assert observed == [
        {
            "zip5": "10001",
            "state": "NY",
            "city_lower": None,
            "distance_miles": 0.0,
            "is_anchor": True,
        }
    ]
    lookup_context.assert_awaited_once_with(session, "10001")
    assert len(pricing._ZIP_RADIUS_ROWS_CACHE) == 1


@pytest.mark.asyncio
async def test_incomplete_zip_context_uses_canonical_anchor_only(monkeypatch):
    monkeypatch.setattr(pricing, "_ZIP_RADIUS_ROWS_CACHE_TTL_SECONDS", 30.0)
    monkeypatch.setattr(pricing, "_ZIP_RADIUS_ROWS_CACHE_MAX_KEYS", 4)
    monkeypatch.setattr(
        pricing,
        "_lookup_zip_context",
        AsyncMock(
            return_value={
                "zip5": "10001",
                "state": "ny",
                "city_lower": "new york",
                "latitude": None,
                "longitude": -73.99,
            }
        ),
    )

    observed = await pricing._zip_radius_rows(
        object(),
        zip5="10001",
        radius_miles=10.0,
    )

    assert observed[0]["state"] == "NY"
    assert observed[0]["city_lower"] == "new york"
    assert len(pricing._ZIP_RADIUS_ROWS_CACHE) == 1


@pytest.mark.asyncio
async def test_zip_radius_rows_ignore_bad_duplicates_and_restore_anchor(
    monkeypatch,
):
    lookup_context = AsyncMock(
        return_value={
            "zip5": "10001",
            "state": "",
            "city_lower": "new york",
            "latitude": 40.75,
            "longitude": -73.99,
        }
    )
    monkeypatch.setattr(pricing, "_lookup_zip_context", lookup_context)
    execute = AsyncMock(
        return_value=[
            {"zip5": "bad", "distance_miles": 1.0},
            {
                "zip5": "10002",
                "state": " ny ",
                "city_lower": " New York ",
                "distance_miles": 2.5,
            },
            {
                "zip5": "10002",
                "state": "NY",
                "city_lower": "duplicate",
                "distance_miles": 3.0,
            },
        ]
    )
    supplied_anchor_map = {
        "zip5": "99999",
        "latitude": 1.0,
        "longitude": 2.0,
    }

    observed = await pricing._zip_radius_rows(
        SimpleNamespace(execute=execute),
        zip5="10001",
        radius_miles=5.0,
        anchor_context=supplied_anchor_map,
    )

    assert [zip_radius_entry["zip5"] for zip_radius_entry in observed] == [
        "10001",
        "10002",
    ]
    assert observed[1]["state"] == "NY"
    assert observed[1]["city_lower"] == "new york"
    assert observed[1]["is_anchor"] is False
    lookup_context.assert_awaited_once()
    execute.assert_awaited_once()
