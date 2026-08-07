# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Gateway-aligned resource budgets for billing-search responses."""

from __future__ import annotations

from decimal import Decimal

import pytest

from api.billing_search_response import shape_billing_search_response
from api.ptg2_billing_search_contract import BillingSearchServingUnavailableError
from tests.test_billing_search_endpoint_access import TRUSTED_NOW
from tests.test_billing_search_response import (
    _endpoint_access,
    _matched_result,
    _price,
)


def _shape_prices(*prices: dict[str, object]):
    endpoint_access = _endpoint_access()
    return shape_billing_search_response(
        endpoint_access,
        _matched_result(endpoint_access, *prices),
        trusted_now=TRUSTED_NOW,
    )


def test_exact_gateway_price_atom_limit_is_accepted() -> None:
    payload = _shape_prices(*(_price() for _index in range(256)))

    assert len(payload["items"][0]["rate_occurrences"][0]["prices"]) == 256


def test_price_atom_overflow_fails_without_partial_output() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        _shape_prices(*(_price() for _index in range(257)))


@pytest.mark.parametrize(
    "price_overrides",
    [
        {"billing_class": "x" * 1025},
        {"service_code": [f"{index:02d}" for index in range(33)]},
        {"billing_code_modifier": ["x" * 17]},
    ],
)
def test_scalar_and_array_budgets_match_the_gateway(
    price_overrides: dict[str, object],
) -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        _shape_prices(_price(**price_overrides))


def test_aggregate_text_budget_is_enforced_before_emission() -> None:
    verbose_prices = tuple(
        _price(billing_class="x" * 1024) for _price_ordinal in range(65)
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        _shape_prices(*verbose_prices)


@pytest.mark.parametrize(
    "negotiated_rate",
    [Decimal("1e64"), Decimal("1e-63"), float("inf"), "01.00"],
)
def test_numeric_wire_expansion_is_bounded(negotiated_rate: object) -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        _shape_prices(_price(negotiated_rate=negotiated_rate))


def test_maximum_plain_decimal_expansion_is_accepted() -> None:
    payload = _shape_prices(_price(negotiated_rate=Decimal("1e63")))

    assert payload["items"][0]["rate_occurrences"][0]["prices"]
