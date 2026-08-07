# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Adversarial coverage for bounded billing-search page hydration."""

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
from api.ptg2_billing_search_result import (
    BillingSearchMatchedProvider,
    BillingSearchProviderCandidate,
)
from tests.billing_search_post_support import (
    address,
    billing_entity_ref,
    binding,
    code_witness,
    matched_result,
    provider_rate,
    serving_tables,
)


def _pin(*, scope_index: int = 0, table_delta: int = 0) -> BillingSearchBindingPin:
    snapshot_id = (
        serving_tables().snapshot_id
        if scope_index == 0
        else f"ptg2:synthetic-billing-search-{scope_index}"
    )
    source_key = "synthetic-network" if scope_index == 0 else f"network-{scope_index}"
    selected_binding = replace(
        binding(),
        binding_ordinal=scope_index,
        snapshot_id=snapshot_id,
        source_key=source_key,
    )
    selected_tables = replace(
        serving_tables(),
        snapshot_id=snapshot_id,
        source_key=source_key,
        price_dictionary_item_count=128 + table_delta,
    )
    return BillingSearchBindingPin(selected_binding, selected_tables)


def _candidate(
    *,
    address_index: int = 1,
    price_key: int = 10,
    selected_pin: BillingSearchBindingPin | None = None,
) -> BillingSearchProviderCandidate:
    selected_address = replace(
        address(),
        address_key=f"00000000-0000-0000-0000-{address_index:012x}",
        location_key=f"{(address_index + 5) % 10}" * 64,
    )
    selected_rate = replace(
        provider_rate(),
        price_key=price_key,
        source_record_ordinal=address_index,
        occurrence_ordinal=address_index,
    )
    geo_witness = BillingProviderGeoWitness(selected_rate, selected_address)
    return BillingSearchProviderCandidate(
        selected_pin or _pin(),
        billing_entity_ref(),
        selected_address,
        (geo_witness,),
        ((5, code_witness()),),
    )


def _multi_price_candidate() -> BillingSearchProviderCandidate:
    candidate = _candidate()
    second_rate = replace(
        candidate.geo_witnesses[0].provider_rate,
        price_key=11,
        source_record_ordinal=2,
        occurrence_ordinal=2,
    )
    second_geo = BillingProviderGeoWitness(second_rate, candidate.address)
    return replace(
        candidate,
        geo_witnesses=tuple(
            sorted(
                (candidate.geo_witnesses[0], second_geo),
                key=lambda witness: witness.stable_sort_key,
            )
        ),
    )


def _matched(candidate: BillingSearchProviderCandidate) -> BillingSearchMatchedProvider:
    price_witness = BillingProviderGeoPriceWitness(
        candidate.geo_witnesses[0],
        ({"negotiated_rate": 10},),
    )
    return BillingSearchMatchedProvider(candidate, (price_witness,))


@pytest.mark.parametrize("invalid_key", ((), [0] * 6))
def test_sort_key_rejects_invalid_container_shape(invalid_key) -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        page.validate_billing_search_sort_key(invalid_key)


@pytest.mark.parametrize(
    "address_key",
    (
        "not-a-uuid",
        "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA",
    ),
)
def test_sort_key_rejects_invalid_or_noncanonical_address_uuid(address_key) -> None:
    sort_key_parts = list(_candidate().sort_key)
    sort_key_parts[5] = address_key

    with pytest.raises(BillingSearchServingUnavailableError):
        page.validate_billing_search_sort_key(sort_key_parts)


def test_code_witness_map_rejects_foreign_and_duplicate_entries() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        page._validated_code_witnesses((object(),))
    with pytest.raises(BillingSearchServingUnavailableError):
        page._validated_code_witnesses((code_witness(), code_witness()))


def test_geo_grouping_rejects_foreign_witness_and_conflicting_address() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        page._geo_witnesses_by_provider((object(),))

    first_address = address()
    changed_display_by_name = dict(first_address.display)
    changed_display_by_name["second_line"] = "Suite 3"
    second_address = replace(first_address, display=changed_display_by_name)
    first_geo = BillingProviderGeoWitness(provider_rate(), first_address)
    second_geo = BillingProviderGeoWitness(
        replace(provider_rate(), source_record_ordinal=2, occurrence_ordinal=2),
        second_address,
    )
    with pytest.raises(BillingSearchServingUnavailableError):
        page._geo_witnesses_by_provider((first_geo, second_geo))


def test_candidate_grouping_rejects_foreign_pin_and_unknown_code_key() -> None:
    candidate = _candidate()
    with pytest.raises(BillingSearchServingUnavailableError):
        page.group_billing_geo_candidates(
            binding_pin=object(),
            billing_entity_ref=billing_entity_ref(),
            code_witnesses=(code_witness(),),
            geo_witnesses=candidate.geo_witnesses,
        )
    with pytest.raises(BillingSearchServingUnavailableError):
        page.group_billing_geo_candidates(
            binding_pin=_pin(),
            billing_entity_ref=billing_entity_ref(),
            code_witnesses=(replace(code_witness(), code_key=6),),
            geo_witnesses=candidate.geo_witnesses,
        )


def test_candidate_validation_rejects_foreign_and_duplicate_rows() -> None:
    candidate = _candidate()
    with pytest.raises(BillingSearchServingUnavailableError):
        page._validated_candidates((object(),))
    with pytest.raises(BillingSearchServingUnavailableError):
        page._validated_candidates((candidate, candidate))


def test_candidate_window_rejects_unknown_authenticated_sort_key() -> None:
    candidate = _candidate()
    unknown_key = _candidate(address_index=2).sort_key

    with pytest.raises(BillingSearchServingUnavailableError):
        page._candidate_window((candidate,), unknown_key)


def test_next_chunk_rejects_invalid_limits(monkeypatch) -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        page._next_hydration_chunk((_candidate(),), 0, 0)
    monkeypatch.setattr(page, "MAX_HYDRATION_PARTITIONS", 0)
    with pytest.raises(BillingSearchServingUnavailableError):
        page._next_hydration_chunk((_candidate(),), 0, 1)


def test_next_chunk_stops_before_new_partition_cap(monkeypatch) -> None:
    first = _candidate(selected_pin=_pin(scope_index=0))
    second = _candidate(address_index=2, selected_pin=_pin(scope_index=1))
    monkeypatch.setattr(page, "MAX_HYDRATION_PARTITIONS", 1)

    chunk, next_index = page._next_hydration_chunk((first, second), 0, 10)

    assert chunk == (first,)
    assert next_index == 1


def test_next_chunk_rejects_first_candidate_over_price_key_cap(monkeypatch) -> None:
    monkeypatch.setattr(page, "MAX_PRICE_KEYS", 1)

    with pytest.raises(BillingSearchServingUnavailableError):
        page._next_hydration_chunk((_multi_price_candidate(),), 0, 10)


def test_next_chunk_stops_after_prior_candidate_reaches_price_cap(monkeypatch) -> None:
    first = _candidate(price_key=10)
    second = _candidate(address_index=2, price_key=11)
    monkeypatch.setattr(page, "MAX_PRICE_KEYS", 1)

    chunk, next_index = page._next_hydration_chunk((first, second), 0, 10)

    assert chunk == (first,)
    assert next_index == 1


def test_partitioning_rejects_conflicting_pin_and_partition_overflow(
    monkeypatch,
) -> None:
    first = _candidate(selected_pin=_pin())
    conflicting = _candidate(address_index=2, selected_pin=_pin(table_delta=1))
    with pytest.raises(BillingSearchServingUnavailableError):
        page._partition_candidates((first, conflicting))

    second_scope = _candidate(address_index=2, selected_pin=_pin(scope_index=1))
    monkeypatch.setattr(page, "MAX_HYDRATION_PARTITIONS", 1)
    with pytest.raises(BillingSearchServingUnavailableError):
        page._partition_candidates((first, second_scope))


@pytest.mark.asyncio
async def test_partition_hydration_rejects_duplicate_and_malformed_output(
    monkeypatch,
) -> None:
    candidate = _candidate()
    with pytest.raises(BillingSearchServingUnavailableError):
        await page._hydrate_partition(
            object(), (candidate, candidate), {}, atom_budget=2
        )

    hydrate = AsyncMock(return_value=[])
    monkeypatch.setattr(
        page.ptg2_billing_price_reader,
        "hydrate_exact_billing_geo_prices",
        hydrate,
    )
    with pytest.raises(BillingSearchServingUnavailableError):
        await page._hydrate_partition(object(), (candidate,), {}, atom_budget=2)

    hydrate.return_value = (object(),)
    with pytest.raises(BillingSearchServingUnavailableError):
        await page._hydrate_partition(object(), (candidate,), {}, atom_budget=2)


@pytest.mark.asyncio
async def test_chunk_hydration_rejects_invalid_and_exceeded_atom_budget(
    monkeypatch,
) -> None:
    candidate = _candidate()
    with pytest.raises(BillingSearchServingUnavailableError):
        await page._hydrate_chunk(object(), (candidate,), {}, atom_budget=-1)

    monkeypatch.setattr(page, "_partition_candidates", lambda candidates: (candidates,))
    monkeypatch.setattr(
        page,
        "_hydrate_partition",
        AsyncMock(
            return_value={
                0: (
                    BillingProviderGeoPriceWitness(
                        candidate.geo_witnesses[0],
                        ({"negotiated_rate": 10},),
                    ),
                )
            }
        ),
    )
    with pytest.raises(BillingSearchServingUnavailableError):
        await page._hydrate_chunk(object(), (candidate,), {}, atom_budget=0)


def test_page_work_admission_enforces_call_and_key_caps(monkeypatch) -> None:
    monkeypatch.setattr(page, "MAX_PAGE_HYDRATION_CALLS", 0)
    with pytest.raises(BillingSearchServingUnavailableError):
        page._admit_chunk_work(
            (_candidate(),),
            prior_call_count=0,
            prior_scoped_price_keys=set(),
        )


@pytest.mark.asyncio
async def test_page_match_scan_stops_after_limit_is_known(monkeypatch) -> None:
    candidates = (
        _candidate(),
        _candidate(address_index=2),
        _candidate(address_index=3),
    )
    first_candidates_chunk = candidates[:2]
    matched_providers = tuple(
        _matched(candidate) for candidate in first_candidates_chunk
    )
    chunk_start_indexes: list[int] = []

    def next_candidates_chunk(_candidates, start_index, _maximum):
        assert _candidates is candidates
        chunk_start_indexes.append(start_index)
        if start_index != 0:
            pytest.fail("page scan continued after limit plus one matches")
        return first_candidates_chunk, len(first_candidates_chunk)

    monkeypatch.setattr(page, "_next_hydration_chunk", next_candidates_chunk)
    monkeypatch.setattr(
        page,
        "_admit_chunk_work",
        lambda _chunk, **_kwargs: (1, set()),
    )
    hydrate_chunk = AsyncMock(return_value=matched_providers)
    monkeypatch.setattr(page, "_hydrate_chunk", hydrate_chunk)

    retained = await page._hydrated_page_matches(
        object(), candidates=candidates, limit=1, price_filter_args={}
    )

    assert retained == matched_providers
    assert chunk_start_indexes == [0]
    hydrate_chunk.assert_awaited_once()


@pytest.mark.asyncio
async def test_public_page_hydration_rejects_request_and_limit_configuration(
    monkeypatch,
) -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        await page.hydrate_billing_search_page(
            object(), candidates=(), after_sort_key=None, limit=0, price_filter_args={}
        )
    with pytest.raises(BillingSearchServingUnavailableError):
        await page.hydrate_billing_search_page(
            object(), candidates=(), after_sort_key=None, limit=1, price_filter_args=[]
        )

    monkeypatch.setattr(page, "MAX_PAGE_HYDRATION_CALLS", 0)
    with pytest.raises(BillingSearchServingUnavailableError):
        await page.hydrate_billing_search_page(
            object(), candidates=(), after_sort_key=None, limit=1, price_filter_args={}
        )
