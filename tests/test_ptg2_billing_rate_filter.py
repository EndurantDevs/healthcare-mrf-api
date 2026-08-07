# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pre-expansion negotiated-price filter tests."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_billing_price_reader as price_reader
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_ptg2_billing_price_reader import GROUP_B, _provider_rate, _tables


def _cross_product_filter_prices_by_key() -> dict[int, list[dict[str, object]]]:
    return {
        10: [
            {
                "negotiated_rate": 20,
                "billing_code_modifier": ["AA"],
                "service_code": ["11"],
            }
        ],
        11: [
            {
                "negotiated_rate": 30,
                "billing_code_modifier": ["AA"],
                "service_code": ["22"],
            },
            {
                "negotiated_rate": 40,
                "billing_code_modifier": ["BB"],
                "service_code": ["11"],
            },
        ],
    }


@pytest.mark.asyncio
async def test_rate_filter_without_selectors_has_no_price_io(monkeypatch) -> None:
    rate_witness = _provider_rate().rate_occurrence
    hydrate = AsyncMock()
    monkeypatch.setattr(
        price_reader.ptg2_serving,
        "_version_three_bounded_prices_by_key",
        hydrate,
    )

    retained = await price_reader.filter_exact_billing_rate_occurrences(
        object(),
        _tables(),
        rate_witnesses=(rate_witness,),
        price_filter_args={"modifiers": (), "place_of_service": ()},
    )

    assert retained == (rate_witness,)
    assert retained[0] is rate_witness
    hydrate.assert_not_awaited()


@pytest.mark.asyncio
async def test_rate_filter_preserves_matching_occurrences_without_atom_cross_product(
    monkeypatch,
) -> None:
    first_matching_rate = _provider_rate().rate_occurrence
    second_matching_rate = _provider_rate(
        source_key=1,
        group_ref=GROUP_B,
    ).rate_occurrence
    nonmatching_rate = replace(
        second_matching_rate,
        provider_set_key=4,
        price_key=11,
        occurrence_ordinal=1,
    )
    rate_witnesses = (
        first_matching_rate,
        second_matching_rate,
        nonmatching_rate,
    )
    hydrate = AsyncMock(return_value=_cross_product_filter_prices_by_key())
    monkeypatch.setattr(
        price_reader.ptg2_serving,
        "_version_three_bounded_prices_by_key",
        hydrate,
    )

    retained = await price_reader.filter_exact_billing_rate_occurrences(
        object(),
        _tables(),
        rate_witnesses=rate_witnesses,
        price_filter_args={
            "modifiers": ("AA",),
            "place_of_service": ("11",),
        },
    )

    assert retained == rate_witnesses[:2]
    assert retained[0] is first_matching_rate
    assert retained[1] is second_matching_rate
    assert hydrate.await_args.args[2] == (10, 11)
    assert hydrate.await_args.kwargs == {"maximum_atom_count": 4096}


@pytest.mark.asyncio
async def test_selective_rate_filter_reduces_above_page_price_key_cap(
    monkeypatch,
) -> None:
    first_rate = _provider_rate().rate_occurrence
    rate_witnesses = tuple(
        replace(first_rate, price_key=price_key, occurrence_ordinal=price_key)
        for price_key in range(price_reader.MAX_PRICE_KEYS + 1)
    )
    selected_price_key = price_reader.MAX_PRICE_KEYS
    hydrate = AsyncMock(
        return_value={
            price_key: [
                {
                    "negotiated_rate": price_key,
                    "billing_code_modifier": [
                        "AA" if price_key == selected_price_key else "BB"
                    ],
                    "service_code": ["11"],
                }
            ]
            for price_key in range(price_reader.MAX_PRICE_KEYS + 1)
        }
    )
    monkeypatch.setattr(
        price_reader.ptg2_serving,
        "_version_three_bounded_prices_by_key",
        hydrate,
    )

    retained = await price_reader.filter_exact_billing_rate_occurrences(
        object(),
        _tables(),
        rate_witnesses=rate_witnesses,
        price_filter_args={"modifiers": ("AA",)},
    )

    assert retained == (rate_witnesses[-1],)
    assert len(hydrate.await_args.args[2]) == price_reader.MAX_PRICE_KEYS + 1
    assert hydrate.await_args.kwargs == {"maximum_atom_count": 4096}


@pytest.mark.asyncio
async def test_rate_filter_rejects_price_key_scan_over_cap_before_io(
    monkeypatch,
) -> None:
    first_rate = _provider_rate().rate_occurrence
    rate_witnesses = (
        first_rate,
        replace(first_rate, price_key=11, occurrence_ordinal=1),
    )
    hydrate = AsyncMock()
    monkeypatch.setattr(price_reader, "MAX_RATE_FILTER_PRICE_KEYS", 1)
    monkeypatch.setattr(
        price_reader.ptg2_serving,
        "_version_three_bounded_prices_by_key",
        hydrate,
    )

    with pytest.raises(PTG2ManifestArtifactError, match="rate filter.*key limit"):
        await price_reader.filter_exact_billing_rate_occurrences(
            object(),
            _tables(),
            rate_witnesses=rate_witnesses,
            price_filter_args={"modifiers": ("AA",)},
        )

    hydrate.assert_not_awaited()
