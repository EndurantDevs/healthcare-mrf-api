# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed invariants for exact billing service results."""

from dataclasses import replace

import pytest

from api import billing_search_cursor as cursor
from api.ptg2_billing_search_contract import (
    BILLING_SEARCH_RESULT_MATCHED,
    BILLING_SEARCH_RESULT_NO_SNAPSHOT,
    BillingSearchMatchedProvider,
    BillingSearchProviderPage,
    BillingSearchResourceNotFoundError,
    BillingSearchServiceResult,
    BillingSearchServingUnavailableError,
    resource_not_found,
    validate_service_result,
)
from tests.billing_search_page_support import (
    NPI_VALUES,
    address,
    candidate,
    code_witness,
    geo_witness,
    hydrated_price,
)
from tests.billing_search_service_support import (
    CURSOR_BINDING,
    selection,
    selector_resolution,
)


def _matched_provider():
    selected_candidate = candidate()
    return BillingSearchMatchedProvider(
        selected_candidate,
        tuple(hydrated_price(witness) for witness in selected_candidate.geo_witnesses),
    )


def _no_snapshot_result():
    return BillingSearchServiceResult(
        state=BILLING_SEARCH_RESULT_NO_SNAPSHOT,
        providers=(),
        next_cursor=None,
        has_more=False,
        selection=None,
        endpoint_access_state_sha256="a" * 64,
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


def test_provider_candidate_revalidates_structure_witness_and_code_scope():
    selected_candidate = candidate()

    with pytest.raises(BillingSearchServingUnavailableError):
        replace(selected_candidate, binding_ordinal=-1)

    mismatched_witness = geo_witness(
        selected_address=address(NPI_VALUES[0], address_suffix=2),
    )
    with pytest.raises(BillingSearchServingUnavailableError):
        replace(selected_candidate, geo_witnesses=(mismatched_witness,))

    with pytest.raises(BillingSearchServingUnavailableError):
        replace(
            selected_candidate,
            code_witnesses_by_key=((6, code_witness(6)),),
        )


def test_matched_provider_rejects_empty_or_foreign_price_witnesses():
    selected_candidate = candidate()
    with pytest.raises(BillingSearchServingUnavailableError):
        BillingSearchMatchedProvider(selected_candidate, ())

    foreign_price = hydrated_price(geo_witness(npi=NPI_VALUES[1]))
    with pytest.raises(BillingSearchServingUnavailableError):
        BillingSearchMatchedProvider(selected_candidate, (foreign_price,))


def test_provider_page_rejects_untyped_and_duplicate_providers():
    matched_provider = _matched_provider()
    with pytest.raises(BillingSearchServingUnavailableError):
        BillingSearchProviderPage((object(),), False, None)
    with pytest.raises(BillingSearchServingUnavailableError):
        BillingSearchProviderPage(
            (matched_provider, matched_provider),
            False,
            None,
        )


def test_nested_cursor_binding_is_revalidated_by_service_result():
    matched_provider = _matched_provider()
    release_selection = selection()
    tampered_binding = replace(CURSOR_BINDING)
    object.__setattr__(tampered_binding, "trusted_now", -1)
    sealed_cursor = cursor._mint_billing_search_sealed_page_cursor(
        "opaque-token",
        object(),
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        BillingSearchServiceResult(
            state=BILLING_SEARCH_RESULT_MATCHED,
            providers=(matched_provider,),
            next_cursor=sealed_cursor,
            has_more=True,
            selection=release_selection,
            endpoint_access_state_sha256="a" * 64,
            selector_resolution=selector_resolution(release_selection),
            cursor_binding=tampered_binding,
        )


def test_nested_selector_resolution_is_revalidated_by_service_result():
    matched_provider = _matched_provider()
    release_selection = selection()
    tampered_selector = selector_resolution(release_selection)
    object.__setattr__(tampered_selector, "selector_scope_sha256", "0" * 64)

    with pytest.raises(BillingSearchServingUnavailableError):
        BillingSearchServiceResult(
            state=BILLING_SEARCH_RESULT_MATCHED,
            providers=(matched_provider,),
            next_cursor=None,
            has_more=False,
            selection=release_selection,
            endpoint_access_state_sha256="a" * 64,
            selector_resolution=tampered_selector,
        )


def test_service_result_rejects_duplicate_provider_coordinates():
    matched_provider = _matched_provider()
    release_selection = selection()
    with pytest.raises(BillingSearchServingUnavailableError):
        BillingSearchServiceResult(
            state=BILLING_SEARCH_RESULT_MATCHED,
            providers=(matched_provider, matched_provider),
            next_cursor=None,
            has_more=False,
            selection=release_selection,
            endpoint_access_state_sha256="a" * 64,
            selector_resolution=selector_resolution(release_selection),
        )


def test_contract_reprs_are_bounded_and_scope_redacted():
    matched_provider = _matched_provider()
    provider_page = BillingSearchProviderPage((matched_provider,), False, None)
    service_result = _no_snapshot_result()

    assert "scope=<redacted>" in repr(matched_provider.candidate)
    assert repr(matched_provider).startswith("<billing-search-matched-provider ")
    assert repr(provider_page) == (
        "<billing-search-provider-page provider_count=1 has_more=False>"
    )
    assert repr(service_result) == (
        "<billing-search-service-result " "state=no_snapshot_for_plan provider_count=0>"
    )


def test_resource_error_and_service_validator_remain_generic():
    missing = resource_not_found()
    assert type(missing) is BillingSearchResourceNotFoundError
    assert str(missing) == "billing_search_resource_not_found"

    valid_result = _no_snapshot_result()
    assert validate_service_result(valid_result) is valid_result
    with pytest.raises(BillingSearchServingUnavailableError):
        validate_service_result(object())
