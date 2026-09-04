# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Adaptive fixed-work pages for the release-bound state scan."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_state_scan as scan
from api.endpoint.pagination import PaginationParams
from tests.test_plan_pricing_state_scan import (
    _args,
    _keyring,
    _occurrence,
    _selection,
    _Session,
)


def _provider_page(npis: tuple[int, ...], has_more: bool = False):
    return npis, {npi: b"frozen-provider" for npi in npis}, has_more


def _install_cursor(monkeypatch) -> None:
    monkeypatch.setattr(scan, "_cursor_keyring", _keyring)
    monkeypatch.setattr(scan.time, "time", lambda: 2_000_000_000)


@pytest.mark.asyncio
async def test_response_is_shaped_before_size_budget(monkeypatch):
    _install_cursor(monkeypatch)
    monkeypatch.setattr(scan, "STATE_SCAN_RESPONSE_BYTE_LIMIT", 4096)
    monkeypatch.setattr(
        scan,
        "_hydrate_selected_groups",
        AsyncMock(
            return_value=[
                {
                    "npi": 1000000001,
                    "price_set_hash": "diagnostic",
                    "source_trace": "x" * 10_000,
                }
            ]
        ),
    )

    response = await scan.search_plan_pricing_state_scan(
        _Session([1000000001], [_occurrence(1000000001)]),
        _selection(),
        _args(),
        PaginationParams(page=1, limit=1, offset=0, source="page"),
    )

    assert response["items"] == [{"npi": 1000000001}]
    assert "source" not in response["query"]


@pytest.mark.asyncio
async def test_adaptive_prefix_is_bounded(monkeypatch):
    _install_cursor(monkeypatch)
    npis = tuple(range(1000000001, 1000000201))
    provider_reader = AsyncMock(return_value=_provider_page(npis))
    occurrence_sizes: list[int] = []
    hydration_sizes: list[int] = []

    async def read_occurrences(_session, _projection, _system, _code, selected, limit):
        occurrence_sizes.append(len(selected))
        assert limit == len(selected)
        return [_occurrence(npi) for npi in selected]

    async def hydrate(_session, _selection_value, _request, _occurrences, fragments):
        hydration_sizes.append(len(fragments))
        if len(fragments) > 1:
            raise scan.PlanPricingStateScanBudgetExceeded()
        return [{"npi": npi} for npi in fragments]

    monkeypatch.setattr(scan, "_read_state_npis", provider_reader)
    monkeypatch.setattr(scan, "_read_projected_occurrences", read_occurrences)
    monkeypatch.setattr(scan, "_hydrate_selected_groups", hydrate)
    response = await scan.search_plan_pricing_state_scan(
        object(), _selection(), _args(),
        PaginationParams(page=1, limit=200, offset=0, source="page"),
    )

    expected_sizes = [200, 100, 50, 25, 12, 6, 3, 1]
    assert occurrence_sizes == expected_sizes
    assert hydration_sizes == expected_sizes
    assert provider_reader.await_count == 1
    assert response["pagination"] | {"next_cursor": True} == {
        "total": 1,
        "total_is_exact": False,
        "total_lower_bound": 1,
        "limit": 200,
        "offset": 0,
        "page": 1,
        "has_more": True,
        "next_cursor": True,
        "scanned_npi_count": 1,
    }


@pytest.mark.asyncio
async def test_adaptive_cursor_resumes_suffix(monkeypatch):
    _install_cursor(monkeypatch)
    npis = tuple(range(1000000001, 1000000005))
    provider_afters: list[int] = []

    async def read_providers(_session, _projection, _state, after_npi, limit):
        provider_afters.append(after_npi)
        remaining_npis = tuple(npi for npi in npis if npi > after_npi)
        return _provider_page(remaining_npis[:limit], len(remaining_npis) > limit)

    async def read_occurrences(_session, _projection, _system, _code, selected, _limit):
        return [_occurrence(npi) for npi in selected]

    async def hydrate(_session, _selection_value, _request, _occurrences, fragments):
        if len(fragments) > 2:
            raise scan.PlanPricingStateScanBudgetExceeded()
        return [{"npi": npi} for npi in fragments]

    monkeypatch.setattr(scan, "_read_state_npis", read_providers)
    monkeypatch.setattr(scan, "_read_projected_occurrences", read_occurrences)
    monkeypatch.setattr(scan, "_hydrate_selected_groups", hydrate)
    pagination = PaginationParams(page=1, limit=4, offset=0, source="page")
    first = await scan.search_plan_pricing_state_scan(
        object(), _selection(), _args(), pagination,
    )
    cursor_args = _args(cursor=first["pagination"]["next_cursor"])
    cursor_scope = scan._cursor_scope(_selection(), _args(), "CPT", "93320", "MI", 4)
    repeated_pages = [
        await scan.search_plan_pricing_state_scan(
            object(), _selection(), cursor_args, pagination,
        )
        for _ in range(2)
    ]

    assert provider_afters == [0, 1000000002, 1000000002]
    assert scan._open_position(
        cursor_args["cursor"], keyring=_keyring(), trusted_now=2_000_000_000,
        scope=cursor_scope,
    ) == (1000000002, 2, 2, 1)
    assert first["pagination"]["scanned_npi_count"] == 2
    assert first["pagination"]["page"] == 1
    assert [page["items"] for page in repeated_pages] == [
        [{"npi": 1000000003}, {"npi": 1000000004}],
        [{"npi": 1000000003}, {"npi": 1000000004}],
    ]
    assert all(page["pagination"]["page"] == 2 for page in repeated_pages)
    assert all(page["pagination"]["scanned_npi_count"] == 4 for page in repeated_pages)
    assert all(page["pagination"]["next_cursor"] is None for page in repeated_pages)


@pytest.mark.asyncio
async def test_single_npi_overflow_is_terminal(monkeypatch):
    _install_cursor(monkeypatch)
    npi = 1000000001
    provider_reader = AsyncMock(return_value=_provider_page((npi,)))
    occurrence_reader = AsyncMock(return_value=[_occurrence(npi)])
    hydration = AsyncMock(side_effect=scan.PlanPricingStateScanBudgetExceeded())
    monkeypatch.setattr(scan, "_read_state_npis", provider_reader)
    monkeypatch.setattr(scan, "_read_projected_occurrences", occurrence_reader)
    monkeypatch.setattr(scan, "_hydrate_selected_groups", hydration)

    with pytest.raises(scan.PlanPricingStateScanBudgetExceeded):
        await scan.search_plan_pricing_state_scan(
            object(), _selection(), _args(),
            PaginationParams(page=1, limit=1, offset=0, source="page"),
        )
    provider_reader.assert_awaited_once()
    occurrence_reader.assert_awaited_once()
    hydration.assert_awaited_once()
