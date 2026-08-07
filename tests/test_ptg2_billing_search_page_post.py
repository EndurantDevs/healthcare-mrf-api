# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider grouping and bounded hydration tests for billing-search POST."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_billing_search_page as page
from api.ptg2_billing_geo_contract import (
    BillingProviderGeoPriceWitness,
    BillingProviderGeoWitness,
)
from api.ptg2_billing_search_contract import (
    BillingSearchBindingPin,
    BillingSearchServingUnavailableError,
)
from tests.billing_search_post_support import (
    address,
    billing_entity_ref,
    binding,
    code_witness,
    provider_rate,
    serving_tables,
)


def _two_exact_geo_witnesses() -> tuple[BillingProviderGeoWitness, ...]:
    selected_address = address()
    first_rate = provider_rate()
    second_rate = replace(
        first_rate,
        source_key=1,
        source_record_ordinal=1,
        provider_group_ref="bb" * 16,
        occurrence_ordinal=1,
    )
    return (
        BillingProviderGeoWitness(first_rate, selected_address),
        BillingProviderGeoWitness(second_rate, selected_address),
    )


def test_grouping_retains_multiple_exact_groups_for_one_provider_address() -> None:
    tables = serving_tables()
    witnesses = _two_exact_geo_witnesses()

    candidates = page.group_billing_geo_candidates(
        binding_pin=BillingSearchBindingPin(binding(), tables),
        billing_entity_ref=billing_entity_ref(),
        code_witnesses=(code_witness(),),
        geo_witnesses=witnesses,
    )

    assert len(candidates) == 1
    assert candidates[0].geo_witnesses == witnesses
    assert [
        witness.provider_rate.provider_group_ref
        for witness in candidates[0].geo_witnesses
    ] == ["aa" * 16, "bb" * 16]


@pytest.mark.asyncio
async def test_hydration_preserves_duplicate_price_atoms_per_exact_witness(
    monkeypatch,
) -> None:
    tables = serving_tables()
    candidates = page.group_billing_geo_candidates(
        binding_pin=BillingSearchBindingPin(binding(), tables),
        billing_entity_ref=billing_entity_ref(),
        code_witnesses=(code_witness(),),
        geo_witnesses=_two_exact_geo_witnesses(),
    )
    hydrate = AsyncMock(
        side_effect=lambda _session, _tables, *, geo_witnesses, **_kwargs: tuple(
            BillingProviderGeoPriceWitness(
                witness,
                ({"negotiated_rate": 20},),
            )
            for witness in geo_witnesses
        )
    )
    monkeypatch.setattr(
        page.ptg2_billing_price_reader,
        "hydrate_exact_billing_geo_prices",
        hydrate,
    )

    provider_page = await page.hydrate_billing_search_page(
        object(),
        candidates=candidates,
        after_sort_key=None,
        limit=200,
        price_filter_args={},
    )

    assert len(provider_page.providers) == 1
    assert len(provider_page.providers[0].price_witnesses) == 2
    assert provider_page.providers[0].price_atom_count == 2
    assert hydrate.await_args.kwargs["atom_budget"] == 256


@pytest.mark.asyncio
async def test_cursor_must_name_an_existing_candidate(monkeypatch) -> None:
    tables = serving_tables()
    candidate = page.group_billing_geo_candidates(
        binding_pin=BillingSearchBindingPin(binding(), tables),
        billing_entity_ref=billing_entity_ref(),
        code_witnesses=(code_witness(),),
        geo_witnesses=(_two_exact_geo_witnesses()[0],),
    )[0]
    invalid_sort_key_values = tuple(
        1.0 if index == 1 else value for index, value in enumerate(candidate.sort_key)
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
            after_sort_key=invalid_sort_key_values,
            limit=1,
            price_filter_args={},
        )
    hydrate.assert_not_awaited()
