# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed corruption and work-bound tests for billing page hydration."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_billing_search_page as page
from api.ptg2_billing_search_contract import BillingSearchServingUnavailableError
from tests.billing_search_page_support import (
    GROUP_B,
    NPI_VALUES,
    address,
    binding,
    candidate,
    code_witness,
    geo_witness,
    serving_tables,
)


@pytest.mark.parametrize(
    "sort_key",
    [
        (),
        (
            0,
            1.0,
            0,
            "ptg2:synthetic-page",
            NPI_VALUES[0],
            "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA",
            "0" * 64,
        ),
    ],
)
def test_sort_key_rejects_wrong_shape_and_noncanonical_uuid(sort_key):
    with pytest.raises(BillingSearchServingUnavailableError):
        page.validate_billing_search_sort_key(sort_key)


@pytest.mark.parametrize(
    "code_witnesses",
    [(object(),), (code_witness(), code_witness())],
)
def test_grouping_rejects_untyped_or_duplicate_code_witnesses(code_witnesses):
    with pytest.raises(BillingSearchServingUnavailableError):
        page.group_billing_geo_candidates(
            binding=binding(),
            serving_tables=serving_tables(),
            code_witnesses=code_witnesses,
            geo_witnesses=(geo_witness(),),
        )


def test_grouping_rejects_untyped_geo_witness() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        page.group_billing_geo_candidates(
            binding=binding(),
            serving_tables=serving_tables(),
            code_witnesses=(code_witness(),),
            geo_witnesses=(object(),),
        )


def test_grouping_rejects_conflicting_addresses_for_one_location() -> None:
    first_address = address(NPI_VALUES[0], distance=1.0)
    conflicting_address = address(NPI_VALUES[0], distance=2.0)
    witnesses = (
        geo_witness(selected_address=first_address),
        geo_witness(
            selected_address=conflicting_address,
            source_key=1,
            group_ref=GROUP_B,
        ),
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        page.group_billing_geo_candidates(
            binding=binding(),
            serving_tables=serving_tables(),
            code_witnesses=(code_witness(),),
            geo_witnesses=witnesses,
        )


def test_grouping_rejects_geo_witness_without_code_evidence() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        page.group_billing_geo_candidates(
            binding=binding(),
            serving_tables=serving_tables(),
            code_witnesses=(code_witness(6),),
            geo_witnesses=(geo_witness(),),
        )


def test_candidate_validation_rejects_untyped_and_duplicate_rows() -> None:
    selected_candidate = candidate()
    with pytest.raises(BillingSearchServingUnavailableError):
        page._validated_candidates((object(),))
    with pytest.raises(BillingSearchServingUnavailableError):
        page._validated_candidates((selected_candidate, selected_candidate))


def test_hydration_chunk_rejects_invalid_partition_limit(monkeypatch) -> None:
    monkeypatch.setattr(page, "MAX_HYDRATION_PARTITIONS", 0)
    with pytest.raises(BillingSearchServingUnavailableError):
        page._next_hydration_chunk((candidate(),), 0, 1)


def test_partition_rejects_conflicting_tables_and_excess_scopes(monkeypatch) -> None:
    first = candidate(binding_ordinal=0)
    conflicting_tables = replace(
        first.serving_tables,
        price_dictionary_item_count=999,
    )
    conflicting = replace(first, serving_tables=conflicting_tables)
    with pytest.raises(BillingSearchServingUnavailableError):
        page._partition_candidates((first, conflicting))

    monkeypatch.setattr(page, "MAX_HYDRATION_PARTITIONS", 1)
    with pytest.raises(BillingSearchServingUnavailableError):
        page._partition_candidates(
            (first, candidate(npi=NPI_VALUES[1], binding_ordinal=1))
        )


@pytest.mark.asyncio
async def test_partition_rejects_one_witness_owned_by_two_candidates() -> None:
    first = candidate()
    second = replace(first, binding_ordinal=1)
    with pytest.raises(BillingSearchServingUnavailableError):
        await page._hydrate_partition(
            object(),
            (first, second),
            {},
            atom_budget=1,
        )


@pytest.mark.parametrize("hydrated", [[], (object(),)])
@pytest.mark.asyncio
async def test_partition_rejects_malformed_hydrator_output(monkeypatch, hydrated):
    monkeypatch.setattr(
        page.ptg2_billing_price_reader,
        "hydrate_exact_billing_geo_prices",
        AsyncMock(return_value=hydrated),
    )
    with pytest.raises(BillingSearchServingUnavailableError):
        await page._hydrate_partition(
            object(),
            (candidate(),),
            {},
            atom_budget=1,
        )


@pytest.mark.asyncio
async def test_hydration_rejects_negative_atom_budget() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        await page._hydrate_chunk(
            object(),
            (candidate(),),
            {},
            atom_budget=-1,
        )


@pytest.mark.parametrize(
    ("limit", "price_filter_args"),
    [(0, {}), (1, [])],
)
@pytest.mark.asyncio
async def test_page_rejects_invalid_public_bounds(limit, price_filter_args) -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        await page.hydrate_billing_search_page(
            object(),
            candidates=(candidate(),),
            after_sort_key=None,
            limit=limit,
            price_filter_args=price_filter_args,
        )


@pytest.mark.asyncio
async def test_page_rejects_invalid_runtime_work_budget(monkeypatch) -> None:
    monkeypatch.setattr(page, "MAX_PAGE_HYDRATION_CALLS", 0)
    with pytest.raises(BillingSearchServingUnavailableError):
        await page.hydrate_billing_search_page(
            object(),
            candidates=(candidate(),),
            after_sort_key=None,
            limit=1,
            price_filter_args={},
        )
