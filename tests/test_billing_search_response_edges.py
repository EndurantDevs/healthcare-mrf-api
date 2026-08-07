# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed edge coverage for billing-search response shaping."""

from __future__ import annotations

from dataclasses import replace

import pytest

from api import billing_search_response, billing_search_response_fields
from api.billing_search_response import shape_billing_search_response
from api.billing_search_response_validation import _PublicResponseBudget
from api.billing_search_selector_contract import (
    BILLING_SELECTOR_NO_MATCH,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    BillingSearchSelectorResolution,
    BillingSearchSelectorScope,
)
from api.ptg2_billing_search_contract import (
    BILLING_SEARCH_RESULT_NO_MATCHING_RATES,
    BillingSearchMatchedProvider,
    BillingSearchServingUnavailableError,
)
from tests.billing_search_entity_ref_support import billing_entity_reference
from tests.billing_search_service_support import selection, selector_resolution
from tests.test_billing_search_endpoint_access import (
    TRUSTED_NOW,
    _authorize,
    _query_pairs,
    _signed_headers,
)
from tests.test_billing_search_response import (
    _endpoint_access,
    _matched_result,
    _price,
)


def _assert_unavailable(callback) -> None:
    with pytest.raises(
        BillingSearchServingUnavailableError,
        match="^billing_search_serving_generation_unavailable$",
    ) as captured:
        callback()
    assert captured.value.__cause__ is None


def _radius_endpoint_access(*, radius_miles: str = "25"):
    query_pairs = tuple(
        pair
        for pair in _query_pairs(
            billing_entity_ref=billing_entity_reference(),
            code="99213",
            lat="38.0",
            long="-82.0",
            radius_miles=radius_miles,
        )
        if pair[0] != "zip5"
    )
    return _authorize(
        parameters=dict(query_pairs),
        headers=_signed_headers(query_pairs),
    )


def test_selector_coordinates_must_equal_the_release_binding() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    resolution = result.selector_resolution
    assert resolution is not None
    mismatched_binding = replace(
        resolution.selector_scope.bindings[0],
        snapshot_id="ptg2:synthetic-other",
    )
    mismatched_resolution = BillingSearchSelectorResolution(
        BillingSearchSelectorScope(
            selector_kind="billing_entity_ref",
            bindings=(mismatched_binding,),
        ),
        resolution.selector_scope_sha256,
    )

    _assert_unavailable(
        lambda: shape_billing_search_response(
            endpoint_access,
            replace(result, selector_resolution=mismatched_resolution),
            trusted_now=TRUSTED_NOW,
        )
    )


def test_selector_binding_validation_requires_a_resolution() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    assert result.selection is not None
    object.__setattr__(result, "selector_resolution", None)

    _assert_unavailable(
        lambda: billing_search_response._validated_selector_bindings(
            result.selection,
            result,
        )
    )


def test_selector_resolution_is_revalidated_before_binding_use() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    assert result.selection is not None
    assert result.selector_resolution is not None
    object.__setattr__(
        result.selector_resolution,
        "selector_scope_sha256",
        "0" * 64,
    )

    _assert_unavailable(
        lambda: billing_search_response._validated_selector_bindings(
            result.selection,
            result,
        )
    )


def test_malformed_selector_binding_is_revalidated_before_source_use() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    resolution = result.selector_resolution
    assert resolution is not None
    object.__setattr__(
        resolution.selector_scope.bindings[0],
        "state",
        "synthetic-invalid-state",
    )

    _assert_unavailable(
        lambda: shape_billing_search_response(
            endpoint_access,
            result,
            trusted_now=TRUSTED_NOW,
        )
    )


def test_selector_state_must_cohere_with_the_public_result_state() -> None:
    endpoint_access = _endpoint_access()
    selected_release = selection(binding_count=2)
    matched_result = _matched_result(
        endpoint_access,
        _price(),
        release_selection=selected_release,
    )
    incoherent_resolution = selector_resolution(
        selected_release,
        states=(BILLING_SELECTOR_PROJECTION_UNAVAILABLE, BILLING_SELECTOR_NO_MATCH),
    )
    incoherent_result = replace(
        matched_result,
        state=BILLING_SEARCH_RESULT_NO_MATCHING_RATES,
        providers=(),
        selector_resolution=incoherent_resolution,
    )

    _assert_unavailable(
        lambda: shape_billing_search_response(
            endpoint_access,
            incoherent_result,
            trusted_now=TRUSTED_NOW,
        )
    )


def test_non_snapshot_result_requires_a_release_selection() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    object.__setattr__(result, "selection", None)

    _assert_unavailable(
        lambda: billing_search_response._validated_selector_source_groups(
            endpoint_access.request,
            result,
        )
    )


def test_selection_must_retain_exact_serving_tables() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    assert result.selection is not None
    incomplete_selection = replace(result.selection, _validated_serving_tables=())

    _assert_unavailable(
        lambda: shape_billing_search_response(
            endpoint_access,
            replace(result, selection=incomplete_selection),
            trusted_now=TRUSTED_NOW,
        )
    )


def test_public_payload_requires_a_typed_release_selection() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    object.__setattr__(result, "selection", object())

    _assert_unavailable(
        lambda: billing_search_response._public_response_payload(
            endpoint_access,
            result,
            {},
            None,
        )
    )


def test_provider_tables_must_equal_the_selected_tables() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    matched_provider = result.providers[0]
    mismatched_tables = replace(
        matched_provider.candidate.serving_tables,
        plan_id="synthetic-other-plan",
    )
    mismatched_candidate = replace(
        matched_provider.candidate,
        serving_tables=mismatched_tables,
    )
    mismatched_provider = BillingSearchMatchedProvider(
        mismatched_candidate,
        matched_provider.price_witnesses,
    )

    _assert_unavailable(
        lambda: shape_billing_search_response(
            endpoint_access,
            replace(result, providers=(mismatched_provider,)),
            trusted_now=TRUSTED_NOW,
        )
    )


def test_selection_release_must_equal_the_authenticated_release() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    assert result.selection is not None
    mismatched_selection = replace(
        result.selection,
        plan_release_id="hprelease_01K123456789ABCDEFGHJKMNR",
    )

    _assert_unavailable(
        lambda: shape_billing_search_response(
            endpoint_access,
            replace(result, selection=mismatched_selection),
            trusted_now=TRUSTED_NOW,
        )
    )


def test_unexpected_shaper_failure_is_sanitized(monkeypatch) -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())

    def raise_unexpected(*_args, **_kwargs):
        raise RuntimeError("synthetic internal detail")

    monkeypatch.setattr(
        billing_search_response,
        "_validated_response_inputs",
        raise_unexpected,
    )

    _assert_unavailable(
        lambda: shape_billing_search_response(
            endpoint_access,
            result,
            trusted_now=TRUSTED_NOW,
        )
    )


def test_malformed_address_provenance_fails_closed() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    selected_address = result.providers[0].candidate.address
    malformed_provenance = replace(selected_address.provenance[0], source_id=99)
    object.__setattr__(selected_address, "provenance", (malformed_provenance,))

    _assert_unavailable(
        lambda: shape_billing_search_response(
            endpoint_access,
            result,
            trusted_now=TRUSTED_NOW,
        )
    )


def test_duplicate_address_provenance_fails_closed() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    selected_address = result.providers[0].candidate.address
    provenance = selected_address.provenance[0]
    object.__setattr__(selected_address, "provenance", (provenance, provenance))

    _assert_unavailable(
        lambda: shape_billing_search_response(
            endpoint_access,
            result,
            trusted_now=TRUSTED_NOW,
        )
    )


def test_numeric_distance_is_emitted_only_after_validation() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    object.__setattr__(result.providers[0].candidate.address, "distance_miles", 4)

    payload = shape_billing_search_response(
        endpoint_access,
        result,
        trusted_now=TRUSTED_NOW,
    )

    assert payload["items"][0]["distance_miles"] == 4.0


def test_radius_search_requires_a_measured_distance() -> None:
    endpoint_access = _radius_endpoint_access()
    result = _matched_result(endpoint_access, _price())

    _assert_unavailable(
        lambda: shape_billing_search_response(
            endpoint_access,
            result,
            trusted_now=TRUSTED_NOW,
        )
    )


def test_radius_search_rejects_distance_outside_the_radius() -> None:
    endpoint_access = _radius_endpoint_access(radius_miles="5")
    result = _matched_result(endpoint_access, _price())
    object.__setattr__(result.providers[0].candidate.address, "distance_miles", 6.0)

    _assert_unavailable(
        lambda: shape_billing_search_response(
            endpoint_access,
            result,
            trusted_now=TRUSTED_NOW,
        )
    )


def test_distance_rejects_boolean_values_at_the_public_boundary() -> None:
    endpoint_access = _endpoint_access()
    provider = _matched_result(endpoint_access, _price()).providers[0]
    object.__setattr__(provider.candidate.address, "distance_miles", True)

    _assert_unavailable(
        lambda: billing_search_response_fields._public_distance(
            endpoint_access.request,
            provider,
        )
    )


def test_rate_witness_is_revalidated_before_source_intersection() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    provider_rate = result.providers[0].price_witnesses[0].geo_witness.provider_rate
    object.__setattr__(provider_rate, "occurrence_ordinal", -1)

    _assert_unavailable(
        lambda: shape_billing_search_response(
            endpoint_access,
            result,
            trusted_now=TRUSTED_NOW,
        )
    )


def test_code_witness_is_reproved_against_the_request() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    code_witness = result.providers[0].candidate.code_witnesses_by_key[0][1]
    object.__setattr__(code_witness, "code", "00001")

    _assert_unavailable(
        lambda: shape_billing_search_response(
            endpoint_access,
            result,
            trusted_now=TRUSTED_NOW,
        )
    )


@pytest.mark.parametrize("price_witnesses", [(), (object(),)])
def test_occurrence_list_rejects_missing_or_untyped_witnesses(
    price_witnesses: tuple[object, ...],
) -> None:
    endpoint_access = _endpoint_access()
    provider = _matched_result(endpoint_access, _price()).providers[0]
    object.__setattr__(provider, "price_witnesses", price_witnesses)

    _assert_unavailable(
        lambda: billing_search_response_fields._public_rate_occurrences(
            endpoint_access.request,
            provider,
            {},
            _PublicResponseBudget(),
        )
    )


def test_provider_payload_rejects_an_untyped_provider() -> None:
    endpoint_access = _endpoint_access()

    _assert_unavailable(
        lambda: billing_search_response_fields.public_provider_payload(
            endpoint_access.request,
            object(),
            {},
            _PublicResponseBudget(),
        )
    )


def test_provenance_canonicalizer_rejects_an_untyped_entry() -> None:
    _assert_unavailable(
        lambda: billing_search_response_fields._canonical_address_provenance(object())
    )
