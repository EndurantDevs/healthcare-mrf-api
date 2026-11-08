# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent public-response boundaries for composed billing searches."""

from dataclasses import replace

import pytest

from api.billing_search_import_contract import _new_billing_search_composed_order
from api.billing_search_pagination import (
    BillingSearchGenerationPin,
    billing_search_snapshot_set_sha256,
    build_billing_search_cursor_binding,
    seal_billing_search_page_cursor,
)
from api.billing_search_response import shape_billing_search_response
from api.ptg2_billing_search_contract import BillingSearchServingUnavailableError
from tests.billing_search_page_support import NPI_VALUES
from tests.test_billing_search_endpoint_access import TRUSTED_NOW
from tests.test_billing_search_import_contract import _scope
from tests.test_billing_search_response import _endpoint_access, _matched_result
from tests.test_billing_search_response_boundary import CURSOR_KEYRING


def _composed_result():
    access = _endpoint_access(limit="2")
    first = _matched_result(access, npi=NPI_VALUES[0])
    second = _matched_result(access, npi=NPI_VALUES[1])
    providers = (second.providers[0], first.providers[0])
    scope = _scope()
    order = _new_billing_search_composed_order(tuple(provider.candidate.sort_key for provider in providers), scope)
    return access, replace(first, providers=providers, import_scope=scope, composed_order=order), scope


def test_public_response_accepts_sealed_composed_order_and_preserves_nested_rates():
    access, result, scope = _composed_result()
    payload = shape_billing_search_response(access, result, trusted_now=TRUSTED_NOW, import_scope=scope)
    assert [item["npi"] for item in payload["items"]] == [NPI_VALUES[1], NPI_VALUES[0]]
    assert all(item["rate_occurrences"] for item in payload["items"])
    assert "custom_import" not in str(payload)


@pytest.mark.parametrize("scope_override", [None, _scope(query_fingerprint_sha256="9" * 64)])
def test_terminal_public_page_requires_independently_verified_import_scope(scope_override):
    access, result, _scope_value = _composed_result()
    with pytest.raises(BillingSearchServingUnavailableError):
        shape_billing_search_response(access, result, trusted_now=TRUSTED_NOW, import_scope=scope_override)


def test_public_response_rejects_tampered_order_seal():
    access, result, scope = _composed_result()
    object.__setattr__(result.composed_order, "candidate_keys", result.composed_order.candidate_keys[::-1])
    with pytest.raises(BillingSearchServingUnavailableError):
        shape_billing_search_response(access, result, trusted_now=TRUSTED_NOW, import_scope=scope)


@pytest.mark.parametrize("boundary", ["constructor", "public_response"])
def test_matched_import_result_requires_order_proof_even_when_native_sorted(boundary):
    access, result, scope = _composed_result()
    native_sorted_providers = result.providers[::-1]
    if boundary == "constructor":
        with pytest.raises(BillingSearchServingUnavailableError):
            replace(result, providers=native_sorted_providers, composed_order=None)
    else:
        object.__setattr__(result, "providers", native_sorted_providers)
        object.__setattr__(result, "composed_order", None)
        with pytest.raises(BillingSearchServingUnavailableError):
            shape_billing_search_response(access, result, trusted_now=TRUSTED_NOW, import_scope=scope)


def test_public_cursor_reauthenticates_composed_query_authority_and_generation():
    access, result, scope = _composed_result()
    pin = BillingSearchGenerationPin(billing_search_snapshot_set_sha256(result.selection), "8" * 64, 1001, 1002)
    binding = build_billing_search_cursor_binding(
        access.request, access.authorization_context, pin, trusted_now=TRUSTED_NOW, import_scope=scope
    )
    sealed = seal_billing_search_page_cursor(
        result.providers[-1].candidate.sort_key, keyring=CURSOR_KEYRING, binding=binding
    )
    with_cursor = replace(result, next_cursor=sealed, has_more=True, cursor_binding=binding)
    payload = shape_billing_search_response(
        access, with_cursor, trusted_now=TRUSTED_NOW, cursor_keyring=CURSOR_KEYRING, import_scope=scope
    )
    assert payload["pagination"]["next_cursor"]
    forged_binding = replace(binding, request_fingerprint_sha256="9" * 64)
    with pytest.raises(BillingSearchServingUnavailableError):
        shape_billing_search_response(
            access,
            replace(with_cursor, cursor_binding=forged_binding),
            trusted_now=TRUSTED_NOW,
            cursor_keyring=CURSOR_KEYRING,
            import_scope=scope,
        )
