# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unique-provider page proof at the E&M location-window boundary."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api.endpoint import pricing
from api.plan_pricing_em_distance import search_plan_pricing_em_distance
from api.plan_release_serving import PlanReleaseServingSelection
from api.ptg2_serving import PTG2LocationScopeError
from tests.test_plan_pricing_em_distance import (
    _EM_DISTANCE_RELEASE_BINDINGS,
)


class _LocationWindowSession:
    def __init__(self, candidate_count, unique_count):
        self.candidate_count = candidate_count
        self.unique_count = unique_count
        self.windows = []

    async def execute(self, _statement, parameters):
        window = parameters["candidate_limit"]
        self.windows.append(window)
        unique_count = self.unique_count if window == 8192 else 1
        metadata = {
            "projection_ready": True,
            "candidate_count": min(window, self.candidate_count),
            "unique_count": unique_count,
        }
        rows = [
            {
                **metadata,
                "npi": 1003000000 + ordinal,
                "distance_miles": float(ordinal),
                "minimum_rates": [10] * 6,
                "maximum_rates": [20] * 6,
                "rate_counts": [2] * 6,
            }
            for ordinal in range(
                parameters["offset"],
                min(unique_count, parameters["offset"] + parameters["page_limit"]),
            )
        ] or [{**metadata, "npi": None}]
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))


def _release_selection():
    return PlanReleaseServingSelection(
        serving_revision_id="hpserve_" + "3" * 26,
        plan_release_id="hprelease_" + "0" * 26,
        healthporta_plan_id="hpplan_" + "1" * 26,
        plan_version_id=None,
        release_month="2026-08",
        release_status="published",
        binding_set_digest="a" * 64,
        bindings=_EM_DISTANCE_RELEASE_BINDINGS,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("candidate_count", "unique_count", "expected"),
    [
        (8192, 1, None),
        (8192, 26, None),
        (8192, 27, (25, False, True, 27)),
        (8191, 1, (0, True, False, 1)),
    ],
)
async def test_em_distance_page_boundaries(
    candidate_count, unique_count, expected
):
    session = _LocationWindowSession(candidate_count, unique_count)
    request = search_plan_pricing_em_distance(
        session,
        _release_selection(),
        {
            "code": "99213",
            "view": "card",
            "order_by": "distance",
            "zip5": "60611",
        },
        SimpleNamespace(limit=25, offset=1, page=1),
    )
    if expected is None:
        with pytest.raises(PTG2LocationScopeError) as failure:
            await request
        assert failure.value.allows_distance_retry is False
    else:
        response = await request
        pagination = response["pagination"]
        assert (
            len(response["items"]),
            pagination["total_is_exact"],
            pagination["has_more"],
            pagination["total_lower_bound"],
        ) == expected
    assert session.windows == [512, 1024, 2048, 4096, 8192]


@pytest.mark.asyncio
async def test_em_cap_refusal_has_no_retry(monkeypatch):
    selection = _release_selection()
    monkeypatch.setattr(
        pricing, "resolve_plan_release_guard_selection", AsyncMock(return_value=selection)
    )
    monkeypatch.setattr(
        pricing, "resolve_plan_release_serving", AsyncMock(return_value=selection)
    )
    monkeypatch.setattr(
        pricing, "is_em_distance_projection_ready", AsyncMock(return_value=True)
    )
    request = SimpleNamespace(
        args={
            "plan_release_id": selection.plan_release_id,
            "code": "99213",
            "view": "card",
            "order_by": "distance",
            "zip5": "60611",
            "zip_radius_miles": "0",
            "offset": "1",
            "limit": "25",
        },
        ctx=SimpleNamespace(sa_session=_LocationWindowSession(8192, 1)),
    )

    response = await pricing.list_providers_by_procedure(request)

    response_by_field = json.loads(response.body)
    assert response.status == 422
    assert response_by_field["code"] == "ptg2_location_scope_too_broad"
    assert response_by_field["fix_it"]["retry_options"] == []
