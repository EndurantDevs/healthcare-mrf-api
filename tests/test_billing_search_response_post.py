# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed public response tests for billing-search POST."""

from __future__ import annotations

from dataclasses import replace
import json

import orjson
import pytest

from api.billing_search_response import shape_billing_search_response
from api.ptg2_billing_search_contract import (
    BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY,
    BILLING_SELECTOR_NO_MATCH,
    BillingSearchBindingPin,
    BillingSearchSelectorBindingScope,
    BillingSearchSelectorScope,
    BillingSearchServingUnavailableError,
)
from api.ptg2_billing_search_result import BillingSearchServiceResult
from tests.billing_search_post_support import (
    SNAPSHOT_ID,
    binding,
    matched_result,
    query,
    selection,
    serving_tables,
)


def _wire_value(value: object) -> dict[str, object]:
    return json.loads(orjson.dumps(value))


def _all_keys(value: object) -> set[str]:
    pending_values = [value]
    keys: set[str] = set()
    while pending_values:
        member = pending_values.pop()
        if type(member) is dict:
            keys.update(member)
            pending_values.extend(member.values())
        elif type(member) in {list, tuple}:
            pending_values.extend(member)
    return keys


def test_matched_response_is_closed_and_site_claim_remains_unknown() -> None:
    payload = _wire_value(shape_billing_search_response(matched_result()))

    assert payload["result_state"] == "matched"
    assert payload["pricing_scope"] == "plan_scoped_ptg_tax_identity"
    assert payload["billing_identity"]["tax_identity_type"] == "ein"
    assert len(payload["billing_identity"]["matched_billing_entity_refs"]) == 1
    provider = payload["items"][0]
    assert provider["npi"] == 1000000004
    assert provider["billing_witness_scope"] == ("exact_tax_identity_group_rate_npi")
    assert provider["billing_entity_site_match"] == {
        "classification": "not_comparable",
        "confidence": "unknown",
    }
    assert "address_evidence" not in provider
    assert provider["rate_occurrences"][0]["prices"][0]["negotiated_rate"] == 20.5
    assert not {
        "provider_group_ref",
        "provider_set_key",
        "price_key",
        "source_key",
        "source_record_ordinal",
        "address_site_key",
        "location_key",
        "tin_hmac_sha256",
    }.intersection(_all_keys(payload))


def test_evidence_opt_in_exposes_only_bounded_address_lineage() -> None:
    payload = _wire_value(
        shape_billing_search_response(matched_result(include_evidence=True))
    )

    evidence = payload["items"][0]["address_evidence"]
    assert evidence == {
        "evidence_level": "nppes_registry_address",
        "selection_contract": "ptg2_billing_provider_address_selection_v1",
        "sources": [
            {
                "dataset": "cms_nppes_registry",
                "retrieved_at": "2026-08-01T00:00:00+00:00",
            }
        ],
    }
    assert "source_record_id" not in _all_keys(payload)


def test_no_match_state_returns_no_identity_reference_or_items() -> None:
    tables = serving_tables()
    scope = BillingSearchSelectorScope(
        "tax_identity",
        (
            BillingSearchSelectorBindingScope(
                0,
                SNAPSHOT_ID,
                BILLING_SELECTOR_NO_MATCH,
            ),
        ),
    )
    service_result = BillingSearchServiceResult(
        state=BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY,
        request=query(),
        selection=selection(tables=tables),
        selector_scope=scope,
        binding_pins=(BillingSearchBindingPin(binding(), tables),),
        providers=(),
        has_more=False,
        next_sort_key=None,
    )

    response_by_field = _wire_value(shape_billing_search_response(service_result))

    assert response_by_field["items"] == []
    assert response_by_field["billing_identity"]["matched_billing_entity_refs"] == []
    assert response_by_field["pagination"] == {
        "limit": 25,
        "has_more": False,
        "next_cursor": None,
    }


def test_central_cursor_is_required_exactly_when_page_has_more() -> None:
    terminal_result = matched_result()
    with pytest.raises(BillingSearchServingUnavailableError):
        shape_billing_search_response(terminal_result, next_cursor="cursor-v1")

    provider = terminal_result.providers[0]
    continued_result = replace(
        terminal_result,
        request=query(limit=1),
        has_more=True,
        next_sort_key=provider.candidate.sort_key,
    )
    payload = _wire_value(
        shape_billing_search_response(
            continued_result,
            next_cursor="cursor-v1",
        )
    )

    assert payload["pagination"] == {
        "limit": 1,
        "has_more": True,
        "next_cursor": "cursor-v1",
    }
