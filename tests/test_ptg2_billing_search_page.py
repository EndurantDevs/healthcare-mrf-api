# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider-level exact billing page grouping and hydration tests."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_billing_search_page as page
from api.ptg2_billing_search_contract import BillingSearchServingUnavailableError
from tests.billing_search_page_support import (
    GROUP_B,
    NPI_VALUES,
    address,
    binding,
    candidate as _candidate,
    code_witness,
    geo_witness,
    hydrated_price,
    serving_tables,
)


def test_grouping_retains_multiple_exact_groups_in_one_provider_candidate():
    selected_address = address(NPI_VALUES[0], distance=1.5)
    witnesses = (
        geo_witness(selected_address=selected_address),
        geo_witness(
            selected_address=selected_address,
            source_key=1,
            group_ref=GROUP_B,
            occurrence_ordinal=1,
        ),
    )

    candidates = page.group_billing_geo_candidates(
        binding=binding(),
        serving_tables=serving_tables(),
        code_witnesses=(code_witness(),),
        geo_witnesses=witnesses,
    )

    assert len(candidates) == 1
    assert candidates[0].geo_witnesses == witnesses
    assert [
        witness.provider_rate.provider_group_ref
        for witness in candidates[0].geo_witnesses
    ] == ["aa" * 16, GROUP_B]


def test_same_snapshot_bindings_remain_distinct_candidates():
    first = _candidate(binding_ordinal=0)
    second = _candidate(binding_ordinal=1)

    assert first.snapshot_id == second.snapshot_id
    assert first.address == second.address
    assert first.sort_key != second.sort_key
    assert first.binding_ordinal == 0
    assert second.binding_ordinal == 1


@pytest.mark.parametrize(
    ("serving_override", "value"),
    [
        ("plan_id", "different-plan"),
        ("plan_market_type", "individual"),
        ("source_key", "different-source"),
    ],
)
def test_grouping_rejects_same_snapshot_with_different_binding_scope(
    serving_override,
    value,
):
    with pytest.raises(BillingSearchServingUnavailableError):
        page.group_billing_geo_candidates(
            binding=binding(),
            serving_tables=serving_tables(**{serving_override: value}),
            code_witnesses=(code_witness(),),
            geo_witnesses=(geo_witness(),),
        )


@pytest.mark.parametrize(
    "sort_key",
    [
        (0, 1.0, 0, "ptg2:synthetic-page", NPI_VALUES[0], "bad", "0" * 64),
        (
            1,
            2.0,
            0,
            "ptg2:synthetic-page",
            NPI_VALUES[0],
            "00000000-0000-0000-0000-000000000001",
            "0" * 64,
        ),
        (
            0,
            float("inf"),
            0,
            "ptg2:synthetic-page",
            NPI_VALUES[0],
            "00000000-0000-0000-0000-000000000001",
            "0" * 64,
        ),
        (
            0,
            1.0,
            0,
            "ptg2:synthetic-page",
            1000000000,
            "00000000-0000-0000-0000-000000000001",
            "0" * 64,
        ),
    ],
)
def test_sort_key_validation_fails_closed(sort_key):
    with pytest.raises(BillingSearchServingUnavailableError):
        page.validate_billing_search_sort_key(sort_key)


@pytest.mark.asyncio
async def test_hydration_partitions_same_snapshot_by_binding(monkeypatch):
    first = _candidate(binding_ordinal=0, distance=1.0)
    second = _candidate(binding_ordinal=1, distance=1.0)
    hydrate = AsyncMock(
        side_effect=lambda _session, _tables, *, geo_witnesses, **_kwargs: tuple(
            hydrated_price(witness) for witness in geo_witnesses
        )
    )
    monkeypatch.setattr(
        page.ptg2_billing_price_reader,
        "hydrate_exact_billing_geo_prices",
        hydrate,
    )

    provider_page = await page.hydrate_billing_search_page(
        object(),
        candidates=tuple(sorted((second, first), key=lambda item: item.sort_key)),
        after_sort_key=None,
        limit=2,
        price_filter_args={},
    )

    assert len(provider_page.providers) == 2
    assert hydrate.await_count == 2
    assert [
        provider.candidate.binding_ordinal for provider in provider_page.providers
    ] == [0, 1]


@pytest.mark.asyncio
async def test_partition_cap_stops_after_page_lookahead(monkeypatch):
    candidates = tuple(
        _candidate(
            npi=NPI_VALUES[ordinal],
            distance=float(ordinal),
            binding_ordinal=ordinal,
        )
        for ordinal in range(3)
    )
    hydrate = AsyncMock(
        side_effect=lambda _session, _tables, *, geo_witnesses, **_kwargs: tuple(
            hydrated_price(witness) for witness in geo_witnesses
        )
    )
    monkeypatch.setattr(page, "MAX_HYDRATION_PARTITIONS", 1)
    monkeypatch.setattr(
        page.ptg2_billing_price_reader,
        "hydrate_exact_billing_geo_prices",
        hydrate,
    )

    provider_page = await page.hydrate_billing_search_page(
        object(),
        candidates=candidates,
        after_sort_key=None,
        limit=1,
        price_filter_args={},
    )

    assert provider_page.has_more is True
    assert hydrate.await_count == 2


@pytest.mark.asyncio
async def test_partition_cap_preserves_interleaved_scope_order_across_pages(
    monkeypatch,
):
    candidates = tuple(
        _candidate(
            npi=npi,
            distance=float(position),
            binding_ordinal=binding_ordinal,
        )
        for position, (npi, binding_ordinal) in enumerate(
            zip(NPI_VALUES[:3], (0, 1, 0), strict=True),
            start=1,
        )
    )

    async def hydrate(_session, _tables, *, geo_witnesses, **_kwargs):
        return tuple(hydrated_price(witness) for witness in geo_witnesses)

    monkeypatch.setattr(page, "MAX_HYDRATION_PARTITIONS", 1)
    monkeypatch.setattr(
        page.ptg2_billing_price_reader,
        "hydrate_exact_billing_geo_prices",
        hydrate,
    )
    first_page = await page.hydrate_billing_search_page(
        object(),
        candidates=candidates,
        after_sort_key=None,
        limit=2,
        price_filter_args={},
    )
    second_page = await page.hydrate_billing_search_page(
        object(),
        candidates=candidates,
        after_sort_key=first_page.next_sort_key,
        limit=2,
        price_filter_args={},
    )

    returned_npis = [
        provider.candidate.address.npi
        for provider in first_page.providers + second_page.providers
    ]
    assert returned_npis == list(NPI_VALUES[:3])
    assert first_page.has_more is True
    assert second_page.has_more is False


@pytest.mark.asyncio
async def test_two_pages_have_no_provider_duplication_or_skip(monkeypatch):
    candidates = tuple(
        _candidate(npi=npi, distance=float(position))
        for position, npi in enumerate(NPI_VALUES[:3], start=1)
    )

    async def hydrate(_session, _tables, *, geo_witnesses, **_kwargs):
        return tuple(hydrated_price(witness) for witness in geo_witnesses)

    monkeypatch.setattr(
        page.ptg2_billing_price_reader,
        "hydrate_exact_billing_geo_prices",
        hydrate,
    )
    first_page = await page.hydrate_billing_search_page(
        object(),
        candidates=candidates,
        after_sort_key=None,
        limit=2,
        price_filter_args={},
    )
    second_page = await page.hydrate_billing_search_page(
        object(),
        candidates=candidates,
        after_sort_key=first_page.next_sort_key,
        limit=2,
        price_filter_args={},
    )

    first_npis = [provider.candidate.address.npi for provider in first_page.providers]
    second_npis = [provider.candidate.address.npi for provider in second_page.providers]
    assert first_page.has_more is True
    assert second_page.has_more is False
    assert first_npis + second_npis == list(NPI_VALUES[:3])
    assert set(first_npis).isdisjoint(second_npis)


@pytest.mark.asyncio
async def test_sparse_filtering_scans_multiple_chunks_to_fill_page(monkeypatch):
    candidates = tuple(
        _candidate(npi=npi, distance=float(position), price_keys=(10 + position,))
        for position, npi in enumerate(NPI_VALUES[:5])
    )
    hydrate = AsyncMock()

    async def sparse_hydrate(_session, _tables, *, geo_witnesses, **_kwargs):
        return tuple(
            hydrated_price(witness)
            for witness in geo_witnesses
            if witness.provider_rate.price_key in {12, 13, 14}
        )

    hydrate.side_effect = sparse_hydrate
    monkeypatch.setattr(page, "MAX_HYDRATION_CANDIDATES", 2)
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
        price_filter_args={"modifiers": ("AA",)},
    )

    assert provider_page.has_more is True
    assert [
        provider.candidate.address.npi for provider in provider_page.providers
    ] == list(NPI_VALUES[2:4])
    assert hydrate.await_count == 3


@pytest.mark.asyncio
async def test_cursor_must_identify_existing_immutable_candidate(monkeypatch):
    candidate = _candidate(distance=1.0)
    impossible_sort_key_values = tuple(
        2.0 if position == 1 else value
        for position, value in enumerate(candidate.sort_key)
    )
    hydrate = AsyncMock()
    monkeypatch.setattr(
        page.ptg2_billing_price_reader,
        "hydrate_exact_billing_geo_prices",
        hydrate,
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await page.hydrate_billing_search_page(
            object(),
            candidates=(candidate,),
            after_sort_key=impossible_sort_key_values,
            limit=1,
            price_filter_args={},
        )
    hydrate.assert_not_awaited()

