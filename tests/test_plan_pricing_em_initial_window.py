# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Smaller probes preserve provider pages while reporting honest count bounds."""

from types import SimpleNamespace

import pytest

from api import plan_pricing_em_distance as distance
from tests.test_plan_pricing_em_distance import _DistanceProjectionSession
from tests.test_plan_pricing_em_distance_pagination import (
    _release_selection,
)


async def _search_page(session, *, limit=25, offset=0):
    return await distance.search_plan_pricing_em_distance(
        session,
        _release_selection(),
        {"code": "99213", "view": "card", "order_by": "distance", "zip5": "60611"},
        SimpleNamespace(limit=limit, offset=offset, page=offset // limit + 1),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("limit", "offset", "expected_window"),
    [(25, 0, 128), (100, 0, 404), (25, 100, 504), (100, 100, 804)],
)
async def test_initial_probe_covers_requested_page(limit, offset, expected_window):
    session = _DistanceProjectionSession(ready=True)

    response = await _search_page(session, limit=limit, offset=offset)

    assert [parameters["candidate_limit"] for parameters in session.parameters] == [
        expected_window
    ]
    assert response["items"] == []
    assert response["pagination"]["total_is_exact"] is True
    assert response["pagination"]["has_more"] is False


class _PageProofSession:
    """Supply fixed SQL prefix counts and ordered cards, including distance ties."""

    def __init__(self, observations_by_window):
        self.observations_by_window = observations_by_window
        self.windows = []

    async def execute(self, _statement, parameters):
        window = parameters["candidate_limit"]
        self.windows.append(window)
        candidate_count, unique_count = self.observations_by_window[window]
        assert parameters["offset"] == 0
        assert parameters["page_limit"] == 26
        metadata = {
            "projection_ready": True,
            "candidate_count": candidate_count,
            "unique_count": unique_count,
        }
        cards = [
            {
                **metadata,
                "npi": 1003000000 + ordinal,
                "distance_miles": float(ordinal // 2),
                "minimum_rates": [10 + ordinal] * 6,
                "maximum_rates": [20 + ordinal] * 6,
                "rate_counts": [2 + ordinal] * 6,
            }
            for ordinal in range(min(unique_count, 26))
        ]
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: cards))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("observations_by_window", "baseline_count", "candidate_count", "candidate_windows"),
    [
        pytest.param(
            {128: (10, 10), 512: (10, 10)},
            (10, True), (10, True), [128], id="sparse-exact",
        ),
        pytest.param(
            {128: (128, 128), 512: (200, 200)},
            (200, True), (128, False), [128], id="mid-size-lower-bound",
        ),
        pytest.param(
            {128: (128, 128), 512: (512, 512)},
            (512, False), (128, False), [128], id="dense-lower-bound",
        ),
        pytest.param(
            {128: (128, 8), 256: (256, 20), 512: (320, 30)},
            (30, True), (30, True), [128, 256, 512], id="duplicate-heavy",
        ),
        pytest.param(
            {128: (128, 128), 512: (128, 128)},
            (128, True), (128, False), [128], id="full-probe-not-exhaustion",
        ),
    ],
)
async def test_smaller_probe_preserves_page_semantics(
    monkeypatch, observations_by_window, baseline_count, candidate_count, candidate_windows
):
    responses = []
    for floor, expected_windows, expected_count in (
        (512, [512], baseline_count),
        (128, candidate_windows, candidate_count),
    ):
        monkeypatch.setattr(distance, "_INITIAL_LOCATION_WINDOW", floor)
        session = _PageProofSession(observations_by_window)
        response = await _search_page(session)
        assert session.windows == expected_windows
        pagination = response["pagination"]
        assert (pagination["total"], pagination["total_is_exact"]) == expected_count
        assert pagination["total_lower_bound"] == expected_count[0]
        responses.append(response)
    baseline, candidate = responses
    assert candidate["items"] == baseline["items"]
    expected_items = min(baseline_count[0], 25)
    assert [card["npi"] for card in candidate["items"]] == list(
        range(1003000000, 1003000000 + expected_items)
    )
    assert [card["minimum_negotiated_rate"] for card in candidate["items"]] == list(
        range(10, 10 + expected_items)
    )
    assert [card["distance_miles"] for card in candidate["items"]] == [
        float(ordinal // 2) for ordinal in range(expected_items)
    ]
    for key in ("has_more", "limit", "offset", "page"):
        assert candidate["pagination"][key] == baseline["pagination"][key]
    assert candidate["pagination"]["has_more"] is (baseline_count[0] > 25)
    assert {
        key: field_content
        for key, field_content in candidate.items()
        if key != "pagination"
    } == {
        key: field_content
        for key, field_content in baseline.items()
        if key != "pagination"
    }
