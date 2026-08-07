# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed public-envelope tests for exact billing-identity search."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import orjson
import pytest

from api.billing_search_endpoint_access import (
    BillingSearchEndpointAccess,
    validate_billing_search_endpoint_access_state,
)
from api.billing_search_response import shape_billing_search_response
from api.billing_search_selector_contract import BILLING_SELECTOR_NO_MATCH
from api.plan_release_serving import PlanReleaseServingSelection
from api.ptg2_billing_geo_contract import BillingProviderGeoPriceWitness
from api.ptg2_billing_search_contract import (
    BILLING_SEARCH_RESULT_MATCHED,
    BILLING_SEARCH_RESULT_NO_MATCHING_RATES,
    BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY,
    BILLING_SEARCH_RESULT_NO_SNAPSHOT,
    BillingSearchMatchedProvider,
    BillingSearchServiceResult,
    BillingSearchServingUnavailableError,
)
from api.ptg2_billing_search_page import group_billing_geo_candidates
from tests.billing_search_entity_ref_support import billing_entity_reference
from tests.billing_search_page_support import (
    GROUP_A,
    NPI_VALUES,
    address,
    code_witness,
    geo_witness,
)
from tests.billing_search_service_support import selection, selector_resolution
from tests.test_billing_search_endpoint_access import (
    TRUSTED_NOW,
    _authorize,
    _query_pairs,
    _signed_headers,
)


def _endpoint_access(
    *,
    include_evidence: bool = False,
    **query_overrides: str,
) -> BillingSearchEndpointAccess:
    query_by_name = {
        "billing_entity_ref": billing_entity_reference(),
        "code": "99213",
        "zip5": "25000",
        **query_overrides,
    }
    if include_evidence:
        query_by_name["include_evidence"] = "true"
    query_pairs = _query_pairs(**query_by_name)
    capabilities = ["pricing:billing-search"]
    if include_evidence:
        capabilities.append("pricing:billing-search:provenance")
    return _authorize(
        parameters=dict(query_pairs),
        headers=_signed_headers(query_pairs, capabilities=capabilities),
    )


def _price(**overrides: object) -> dict[str, object]:
    price_by_field: dict[str, object] = {
        "negotiated_rate": "123.4567890123456789",
        "negotiated_type": "negotiated",
        "expiration_date": None,
        "service_code": ["11"],
        "billing_class": "professional",
        "setting": "office",
        "billing_code_modifier": [],
        "additional_information": None,
    }
    price_by_field.update(overrides)
    return price_by_field


def _matched_result(
    endpoint_access: BillingSearchEndpointAccess,
    *price_rows: Mapping[str, Any],
    release_selection: PlanReleaseServingSelection | None = None,
    npi: int = NPI_VALUES[0],
    source_key: int = 0,
    source_record_ordinal: int = 0,
    group_ref: str = GROUP_A,
) -> BillingSearchServiceResult:
    """Build one internally valid matched result around exact synthetic proof."""

    selected_release = release_selection or selection()
    matched_provider = _matched_provider(
        selected_release,
        *price_rows,
        npi=npi,
        source_key=source_key,
        source_record_ordinal=source_record_ordinal,
        group_ref=group_ref,
    )
    endpoint_digest = validate_billing_search_endpoint_access_state(
        endpoint_access,
        trusted_now=TRUSTED_NOW,
    )[1]
    return BillingSearchServiceResult(
        state=BILLING_SEARCH_RESULT_MATCHED,
        providers=(matched_provider,),
        next_cursor=None,
        has_more=False,
        selection=selected_release,
        endpoint_access_state_sha256=endpoint_digest,
        selector_resolution=selector_resolution(selected_release),
    )


def _matched_provider(
    selected_release: PlanReleaseServingSelection,
    *price_rows: Mapping[str, Any],
    npi: int,
    source_key: int,
    source_record_ordinal: int,
    group_ref: str,
) -> BillingSearchMatchedProvider:
    """Build one provider whose rate and address witnesses share identity."""

    release_binding = selected_release.in_network_bindings[0]
    serving_tables = selected_release.serving_tables_for_snapshot(
        release_binding.snapshot_id
    )
    assert serving_tables is not None
    selected_address = address(npi)
    selected_geo_witness = geo_witness(
        npi=npi,
        source_key=source_key,
        group_ref=group_ref,
        selected_address=selected_address,
        snapshot_key=serving_tables.shared_snapshot_key,
    )
    provider_candidate = group_billing_geo_candidates(
        binding=release_binding,
        serving_tables=serving_tables,
        code_witnesses=(code_witness(),),
        geo_witnesses=(selected_geo_witness,),
    )[0]
    # The support constructor uses source_key as its source-record ordinal.
    # Rebuild only for boundary tests that intentionally choose another ordinal.
    if source_record_ordinal != source_key:
        from dataclasses import replace

        selected_rate = replace(
            selected_geo_witness.provider_rate,
            source_record_ordinal=source_record_ordinal,
        )
        selected_geo_witness = replace(
            selected_geo_witness,
            provider_rate=selected_rate,
        )
        provider_candidate = group_billing_geo_candidates(
            binding=release_binding,
            serving_tables=serving_tables,
            code_witnesses=(code_witness(),),
            geo_witnesses=(selected_geo_witness,),
        )[0]
    public_prices = tuple(price_rows) or (_price(),)
    matched_provider = BillingSearchMatchedProvider(
        provider_candidate,
        (BillingProviderGeoPriceWitness(selected_geo_witness, public_prices),),
    )
    return matched_provider


def _empty_result(
    endpoint_access: BillingSearchEndpointAccess,
    state: str,
) -> BillingSearchServiceResult:
    endpoint_digest = validate_billing_search_endpoint_access_state(
        endpoint_access,
        trusted_now=TRUSTED_NOW,
    )[1]
    if state == BILLING_SEARCH_RESULT_NO_SNAPSHOT:
        return BillingSearchServiceResult(
            state,
            (),
            None,
            False,
            None,
            endpoint_digest,
        )
    selected_release = selection()
    states = (
        (BILLING_SELECTOR_NO_MATCH,)
        if state == BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY
        else None
    )
    return BillingSearchServiceResult(
        state,
        (),
        None,
        False,
        selected_release,
        endpoint_digest,
        selector_resolution(selected_release, states=states),
    )


def _all_keys(value: object) -> set[str]:
    if type(value) is dict:
        return set(value) | {
            nested_key
            for nested_value in value.values()
            for nested_key in _all_keys(nested_value)
        }
    if type(value) in {list, tuple}:
        return {
            nested_key
            for nested_value in value
            for nested_key in _all_keys(nested_value)
        }
    return set()


def test_response_matches_the_closed_gateway_wire_envelope() -> None:
    endpoint_access = _endpoint_access(include_evidence=True)

    response_body = shape_billing_search_response(
        endpoint_access,
        _matched_result(endpoint_access, _price()),
        trusted_now=TRUSTED_NOW,
    )

    assert set(response_body) == {
        "billing_association_scope",
        "billing_entity_ref",
        "geo_match_scope",
        "items",
        "pagination",
        "plan_release_id",
        "pricing_scope",
        "procedure",
        "result_state",
    }
    provider_body = response_body["items"][0]
    assert set(provider_body) == {
        "address",
        "address_evidence",
        "billing_entity_ref",
        "distance_miles",
        "npi",
        "rate_occurrences",
    }
    assert set(provider_body["address"]) == {
        "address_kind",
        "city",
        "country_code",
        "first_line",
        "postal_code",
        "second_line",
        "state",
    }
    assert provider_body["address_evidence"] == {
        "evidence_level": "nppes_registry_address",
        "selection_contract": "ptg2_billing_provider_address_selection_v1",
        "sources": [
            {
                "dataset": "cms_nppes_registry",
                "retrieved_at": "2026-01-01T00:00:00+00:00",
            }
        ],
    }


def test_response_recursively_excludes_internal_coordinates() -> None:
    endpoint_access = _endpoint_access()
    payload = shape_billing_search_response(
        endpoint_access,
        _matched_result(endpoint_access, _price()),
        trusted_now=TRUSTED_NOW,
    )

    assert _all_keys(payload).isdisjoint(
        {
            "address_key",
            "address_site_key",
            "binding_ordinal",
            "code_key",
            "generation_bundle_sha256",
            "location_key",
            "price_key",
            "provider_group_ref",
            "provider_set_key",
            "snapshot_id",
            "snapshot_key",
            "source_key",
            "source_record_id",
            "source_record_ordinal",
        }
    )


def test_arbitrary_tic_additional_information_is_never_forwarded() -> None:
    endpoint_access = _endpoint_access()
    private_source_text = "synthetic-private-tax-identity-12-3456789"

    payload = shape_billing_search_response(
        endpoint_access,
        _matched_result(
            endpoint_access,
            _price(additional_information=private_source_text),
        ),
        trusted_now=TRUSTED_NOW,
    )

    assert (
        payload["items"][0]["rate_occurrences"][0]["prices"][0][
            "additional_information"
        ]
        is None
    )
    assert private_source_text.encode() not in orjson.dumps(payload)


def test_negotiated_rate_preserves_its_exact_json_number() -> None:
    endpoint_access = _endpoint_access()
    payload = shape_billing_search_response(
        endpoint_access,
        _matched_result(endpoint_access, _price()),
        trusted_now=TRUSTED_NOW,
    )

    assert b'"negotiated_rate":123.4567890123456789' in orjson.dumps(payload)


@pytest.mark.parametrize(
    "state",
    [
        BILLING_SEARCH_RESULT_NO_MATCHING_RATES,
        BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY,
        BILLING_SEARCH_RESULT_NO_SNAPSHOT,
    ],
)
def test_empty_states_keep_the_fixed_envelope(state: str) -> None:
    endpoint_access = _endpoint_access()

    payload = shape_billing_search_response(
        endpoint_access,
        _empty_result(endpoint_access, state),
        trusted_now=TRUSTED_NOW,
    )

    assert payload["result_state"] == state
    assert payload["items"] == []
    assert payload["pagination"] == {
        "limit": 25,
        "has_more": False,
        "next_cursor": None,
    }


def test_unknown_price_atom_field_fails_closed() -> None:
    endpoint_access = _endpoint_access()

    with pytest.raises(BillingSearchServingUnavailableError):
        shape_billing_search_response(
            endpoint_access,
            _matched_result(
                endpoint_access,
                _price(internal_price_key=17),
            ),
            trusted_now=TRUSTED_NOW,
        )
