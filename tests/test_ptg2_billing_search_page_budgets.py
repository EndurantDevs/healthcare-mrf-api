# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded work tests for exact billing provider-page hydration."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_billing_search_page as page
from api.ptg2_billing_search_contract import (
    BillingSearchServingUnavailableError,
)
from tests.billing_search_page_support import (
    NPI_VALUES,
    candidate,
    hydrated_price,
)


@pytest.mark.asyncio
async def test_aggregate_scoped_price_key_budget_splits_without_skips(monkeypatch):
    candidates = (
        candidate(
            npi=NPI_VALUES[0],
            distance=1.0,
            binding_ordinal=0,
            price_keys=(10,),
        ),
        candidate(
            npi=NPI_VALUES[1],
            distance=2.0,
            binding_ordinal=1,
            price_keys=(11,),
        ),
    )
    hydrate = AsyncMock(
        side_effect=lambda _session, _tables, *, geo_witnesses, **_kwargs: tuple(
            hydrated_price(witness) for witness in geo_witnesses
        )
    )
    monkeypatch.setattr(page, "MAX_PRICE_KEYS", 1)
    monkeypatch.setattr(
        page.ptg2_billing_price_reader,
        "hydrate_exact_billing_geo_prices",
        hydrate,
    )

    provider_page = await page.hydrate_billing_search_page(
        object(),
        candidates=candidates,
        after_sort_key=None,
        limit=2,
        price_filter_args={},
    )

    assert [
        provider.candidate.address.npi for provider in provider_page.providers
    ] == list(NPI_VALUES[:2])
    assert hydrate.await_count == 2


@pytest.mark.asyncio
async def test_aggregate_atom_budget_fails_across_bindings(monkeypatch):
    candidates = (
        candidate(npi=NPI_VALUES[0], binding_ordinal=0),
        candidate(npi=NPI_VALUES[1], binding_ordinal=1),
    )
    hydrate = AsyncMock(
        side_effect=lambda _session, _tables, *, geo_witnesses, **_kwargs: tuple(
            hydrated_price(witness) for witness in geo_witnesses
        )
    )
    monkeypatch.setattr(page, "MAX_PRICE_ATOMS", 1)
    monkeypatch.setattr(
        page.ptg2_billing_price_reader,
        "hydrate_exact_billing_geo_prices",
        hydrate,
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await page.hydrate_billing_search_page(
            object(),
            candidates=candidates,
            after_sort_key=None,
            limit=2,
            price_filter_args={},
        )
    assert hydrate.await_count == 2
    assert hydrate.await_args_list[0].kwargs["atom_budget"] == 1
    assert hydrate.await_args_list[1].kwargs["atom_budget"] == 0


@pytest.mark.asyncio
async def test_page_hydration_call_budget_fails_before_unbounded_sparse_scan(
    monkeypatch,
):
    candidates = tuple(
        candidate(npi=npi, distance=float(position))
        for position, npi in enumerate(NPI_VALUES[:3])
    )
    hydrate = AsyncMock(return_value=())
    monkeypatch.setattr(page, "MAX_HYDRATION_CANDIDATES", 1)
    monkeypatch.setattr(page, "MAX_PAGE_HYDRATION_CALLS", 2)
    monkeypatch.setattr(
        page.ptg2_billing_price_reader,
        "hydrate_exact_billing_geo_prices",
        hydrate,
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await page.hydrate_billing_search_page(
            object(),
            candidates=candidates,
            after_sort_key=None,
            limit=1,
            price_filter_args={},
        )
    assert hydrate.await_count == 2


@pytest.mark.asyncio
async def test_page_scoped_key_budget_accumulates_across_chunks(monkeypatch):
    candidates = (
        candidate(npi=NPI_VALUES[0], price_keys=(10,)),
        candidate(npi=NPI_VALUES[1], price_keys=(11,)),
    )
    hydrate = AsyncMock(return_value=())
    monkeypatch.setattr(page, "MAX_HYDRATION_CANDIDATES", 1)
    monkeypatch.setattr(page, "MAX_PAGE_SCOPED_PRICE_KEYS", 1)
    monkeypatch.setattr(
        page.ptg2_billing_price_reader,
        "hydrate_exact_billing_geo_prices",
        hydrate,
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await page.hydrate_billing_search_page(
            object(),
            candidates=candidates,
            after_sort_key=None,
            limit=1,
            price_filter_args={},
        )
    assert hydrate.await_count == 1


@pytest.mark.asyncio
async def test_oversized_single_candidate_price_scope_fails_without_hydration(
    monkeypatch,
):
    selected_candidate = candidate(price_keys=(10, 11))
    hydrate = AsyncMock()
    monkeypatch.setattr(page, "MAX_PRICE_KEYS", 1)
    monkeypatch.setattr(
        page.ptg2_billing_price_reader,
        "hydrate_exact_billing_geo_prices",
        hydrate,
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await page.hydrate_billing_search_page(
            object(),
            candidates=(selected_candidate,),
            after_sort_key=None,
            limit=1,
            price_filter_args={},
        )
    hydrate.assert_not_awaited()
