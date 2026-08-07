# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused validation and cursor-boundary tests for billing-search responses."""

from __future__ import annotations

from dataclasses import replace

import pytest

from api import billing_search_response_validation
from api.billing_search_response import shape_billing_search_response
from api.ptg2_billing_search_contract import BillingSearchServingUnavailableError
from tests.test_billing_search_endpoint_access import TRUSTED_NOW
from tests.test_billing_search_response import (
    _endpoint_access,
    _matched_result,
    _price,
)
from tests.test_billing_search_response_boundary import CURSOR_KEYRING, _cursor_result


def _assert_unavailable(callback) -> None:
    with pytest.raises(
        BillingSearchServingUnavailableError,
        match="^billing_search_serving_generation_unavailable$",
    ) as captured:
        callback()
    assert captured.value.__cause__ is None


def test_price_atom_budget_rejects_the_first_excess_atom() -> None:
    budget = billing_search_response_validation._PublicResponseBudget()
    budget.price_atom_count = billing_search_response_validation._MAX_PUBLIC_PRICE_ATOMS

    _assert_unavailable(budget.retain_price_atom)


@pytest.mark.parametrize("value", [object(), "line\u0000break"])
def test_public_text_rejects_untyped_or_control_text(value: object) -> None:
    budget = billing_search_response_validation._PublicResponseBudget()

    _assert_unavailable(
        lambda: billing_search_response_validation._public_text(value, budget)
    )


@pytest.mark.parametrize("rate_value", [True, 1 << 513])
def test_public_rate_rejects_boolean_or_unbounded_integer(rate_value: object) -> None:
    _assert_unavailable(
        lambda: billing_search_response_validation._validate_public_rate_value(
            rate_value
        )
    )


@pytest.mark.parametrize(
    "timestamp",
    [
        "synthetic-invalid-time",
        "2026-02-31T00:00:00+00:00",
        "2026-08-07",
    ],
)
def test_public_timestamp_requires_real_rfc3339_time(timestamp: str) -> None:
    budget = billing_search_response_validation._PublicResponseBudget()

    _assert_unavailable(
        lambda: billing_search_response_validation._public_timestamp(
            timestamp,
            budget,
        )
    )


def test_recursive_response_text_budget_rejects_oversized_output() -> None:
    oversized_document_by_field = {
        "synthetic": [
            "x" * (billing_search_response_validation._MAX_PUBLIC_TOTAL_TEXT_BYTES + 1)
        ]
    }

    _assert_unavailable(
        lambda: billing_search_response_validation._validate_total_text_budget(
            oversized_document_by_field
        )
    )


def test_cursor_binding_coordinates_are_reproved_before_authentication() -> None:
    endpoint_access = _endpoint_access(limit="1")
    result = _cursor_result(endpoint_access)
    assert result.cursor_binding is not None
    mismatched_binding = replace(
        result.cursor_binding,
        request_fingerprint_sha256="f" * 64,
    )

    _assert_unavailable(
        lambda: shape_billing_search_response(
            endpoint_access,
            replace(result, cursor_binding=mismatched_binding),
            cursor_keyring=CURSOR_KEYRING,
            trusted_now=TRUSTED_NOW,
        )
    )


def test_cursor_response_requires_a_cursor_keyring() -> None:
    endpoint_access = _endpoint_access(limit="1")
    result = _cursor_result(endpoint_access)

    _assert_unavailable(
        lambda: shape_billing_search_response(
            endpoint_access,
            result,
            trusted_now=TRUSTED_NOW,
        )
    )


def test_cursor_coordinate_validator_requires_binding_and_selection() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())

    _assert_unavailable(
        lambda: billing_search_response_validation._validated_expected_cursor_coordinates(
            endpoint_access,
            result,
            trusted_now=TRUSTED_NOW,
        )
    )


def test_cursor_authenticator_requires_a_sealed_cursor_object() -> None:
    endpoint_access = _endpoint_access(limit="1")
    result = _cursor_result(endpoint_access)
    object.__setattr__(result, "next_cursor", object())

    _assert_unavailable(
        lambda: billing_search_response_validation._authenticated_public_cursor(
            endpoint_access,
            result,
            CURSOR_KEYRING,
            trusted_now=TRUSTED_NOW,
        )
    )


def test_provider_page_order_rejects_duplicate_sort_keys() -> None:
    endpoint_access = _endpoint_access(limit="2")
    result = _matched_result(endpoint_access, _price())
    provider = result.providers[0]
    object.__setattr__(result, "providers", (provider, provider))

    _assert_unavailable(
        lambda: billing_search_response_validation._validate_provider_page_order(result)
    )


def test_public_page_rejects_more_providers_than_requested() -> None:
    endpoint_access = _endpoint_access(limit="1")
    result = _matched_result(endpoint_access, _price())
    provider = result.providers[0]
    object.__setattr__(result, "providers", (provider, provider))

    _assert_unavailable(
        lambda: billing_search_response_validation._validate_public_page(
            endpoint_access,
            result,
            cursor_keyring=None,
            trusted_now=TRUSTED_NOW,
        )
    )
