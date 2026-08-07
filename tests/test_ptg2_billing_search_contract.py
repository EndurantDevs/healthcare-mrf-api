# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed invariants for exact billing service results."""

import pytest

from api.ptg2_billing_search_contract import (
    BILLING_SEARCH_RESULT_NO_SNAPSHOT,
    BillingSearchProviderPage,
    BillingSearchServiceResult,
    BillingSearchServingUnavailableError,
)


def test_page_rejects_more_results_without_a_returned_provider():
    with pytest.raises(BillingSearchServingUnavailableError):
        BillingSearchProviderPage(
            providers=(),
            has_more=True,
            next_sort_key=None,
        )


def test_no_snapshot_state_requires_selection_to_be_none():
    with pytest.raises(BillingSearchServingUnavailableError):
        BillingSearchServiceResult(
            state=BILLING_SEARCH_RESULT_NO_SNAPSHOT,
            providers=(),
            next_cursor=None,
            has_more=False,
            selection=object(),
            endpoint_access_state_sha256="a" * 64,
        )
