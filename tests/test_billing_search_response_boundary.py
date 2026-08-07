# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cross-layer proof tests for billing-search response emission."""

from __future__ import annotations

from dataclasses import replace

import pytest

from api import billing_search_response_fields
from api.billing_search_cursor import (
    BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS,
    BillingSearchCursorKeyring,
    BillingSearchCursorState,
    _new_sealed_page_cursor,
    seal_billing_search_cursor,
)
from api.billing_search_cursor_authentication import (
    authenticate_billing_search_sealed_page_cursor,
)
from api.billing_search_pagination import (
    BillingSearchCursorBinding,
    billing_search_authorization_scope_sha256,
    billing_search_snapshot_set_sha256,
    seal_billing_search_page_cursor,
)
from api.billing_search_response import shape_billing_search_response
from api.billing_search_selector_contract import (
    BillingSearchSelectorResolution,
    BillingSearchSelectorScope,
)
from api.billing_search_transport_contract import _canonical_utc
from api.ptg2_billing_entity_refs import encode_billing_entity_ref
from api.ptg2_billing_search_contract import (
    BillingSearchMatchedProvider,
    BillingSearchServingUnavailableError,
)
from tests.billing_search_entity_ref_support import (
    resolved_source_scope,
    source_publication,
)
from tests.billing_search_page_support import GROUP_B
from tests.billing_search_service_support import selector_resolution
from tests.test_billing_search_endpoint_access import TRUSTED_NOW
from tests.test_billing_search_response import (
    _endpoint_access,
    _matched_result,
    _price,
)

CURSOR_KEYRING = BillingSearchCursorKeyring(
    active_key_id="cursor-v1",
    keys_by_id={"cursor-v1": b"c" * 32},
)
_CURSOR_GENERATION = "a" * 64


def _assert_unavailable(endpoint_access, result, **shape_overrides: object) -> None:
    arguments_by_name = {
        "trusted_now": TRUSTED_NOW,
        **shape_overrides,
    }
    with pytest.raises(
        BillingSearchServingUnavailableError,
        match="^billing_search_serving_generation_unavailable$",
    ) as captured:
        shape_billing_search_response(
            endpoint_access,
            result,
            **arguments_by_name,
        )
    assert captured.value.__cause__ is None


@pytest.mark.parametrize(
    "result_overrides",
    [
        {"source_key": 1},
        {"source_record_ordinal": 1},
        {"group_ref": GROUP_B},
    ],
)
def test_each_rate_requires_its_exact_source_scope_triple(
    result_overrides: dict[str, object],
) -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(
        endpoint_access,
        _price(),
        **result_overrides,
    )

    _assert_unavailable(endpoint_access, result)


def test_endpoint_access_digest_is_revalidated_against_the_result() -> None:
    endpoint_access = _endpoint_access()
    result = replace(
        _matched_result(endpoint_access, _price()),
        endpoint_access_state_sha256="f" * 64,
    )

    _assert_unavailable(endpoint_access, result)


def test_selector_reference_must_equal_the_authenticated_request() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    resolution = result.selector_resolution
    assert resolution is not None
    mismatched_ref = encode_billing_entity_ref(
        snapshot_key=17,
        tin_id_128=b"y" * 16,
        tin_hmac_sha256=b"y" * 32,
    )
    first_binding = replace(
        resolution.selector_scope.bindings[0],
        billing_entity_ref=mismatched_ref,
    )
    mismatched_resolution = BillingSearchSelectorResolution(
        BillingSearchSelectorScope(
            selector_kind="billing_entity_ref",
            bindings=(first_binding,),
        ),
        resolution.selector_scope_sha256,
    )

    _assert_unavailable(
        endpoint_access,
        replace(result, selector_resolution=mismatched_resolution),
    )


@pytest.mark.parametrize("scope_mismatch", ["snapshot", "publication"])
def test_selector_scope_must_match_the_exact_serving_binding(
    scope_mismatch: str,
) -> None:
    endpoint_access = _endpoint_access()
    service_result = _matched_result(endpoint_access, _price())
    selected_release = service_result.selection
    assert selected_release is not None
    serving_tables = selected_release.serving_tables_for_snapshot(
        selected_release.in_network_bindings[0].snapshot_id
    )
    assert serving_tables is not None
    mismatched_source_scope = (
        resolved_source_scope(snapshot_key=serving_tables.shared_snapshot_key + 1)
        if scope_mismatch == "snapshot"
        else resolved_source_scope(
            publication=source_publication(content_digest="7" * 64),
            snapshot_key=serving_tables.shared_snapshot_key,
        )
    )
    mismatched_resolution = selector_resolution(
        selected_release,
        source_scopes=(mismatched_source_scope,),
    )

    _assert_unavailable(
        endpoint_access,
        replace(service_result, selector_resolution=mismatched_resolution),
    )


def test_source_unaware_selection_cannot_emit_even_an_empty_state() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    assert result.selection is not None
    source_unaware_selection = replace(
        result.selection,
        _includes_billing_tax_identity_source=False,
    )

    _assert_unavailable(
        endpoint_access,
        replace(result, selection=source_unaware_selection),
    )


def test_provider_binding_must_exist_in_the_selected_release() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    matched_provider = result.providers[0]
    mismatched_candidate = replace(
        matched_provider.candidate,
        binding_ordinal=99,
    )
    mismatched_provider = BillingSearchMatchedProvider(
        mismatched_candidate,
        matched_provider.price_witnesses,
    )

    _assert_unavailable(
        endpoint_access,
        replace(result, providers=(mismatched_provider,)),
    )


def test_requested_price_filters_are_reproved_before_emission() -> None:
    endpoint_access = _endpoint_access(modifiers="59", place_of_service="22")
    result = _matched_result(
        endpoint_access,
        _price(billing_code_modifier=[], service_code=["11"]),
    )

    _assert_unavailable(endpoint_access, result)


def test_public_npi_range_is_checked_in_addition_to_the_shared_validator(
    monkeypatch,
) -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    monkeypatch.setattr(
        billing_search_response_fields,
        "validated_provider_npi",
        lambda _value: 3_000_000_006,
    )

    _assert_unavailable(endpoint_access, result)


def test_mutated_address_payload_fails_before_public_shaping() -> None:
    endpoint_access = _endpoint_access()
    result = _matched_result(endpoint_access, _price())
    selected_address = result.providers[0].candidate.address
    object.__setattr__(selected_address, "display", {"postal_code": "25000"})

    _assert_unavailable(endpoint_access, result)


def test_address_provenance_is_revalidated_when_not_publicly_requested() -> None:
    endpoint_access = _endpoint_access()
    service_result = _matched_result(endpoint_access, _price())
    selected_address = service_result.providers[0].candidate.address
    object.__setattr__(selected_address, "provenance", ())

    _assert_unavailable(endpoint_access, service_result)


def _cursor_result(endpoint_access):
    result = _matched_result(endpoint_access, _price())
    selected_release = result.selection
    assert selected_release is not None
    _, trusted_time = _canonical_utc(TRUSTED_NOW)
    cursor_binding = BillingSearchCursorBinding(
        request_fingerprint_sha256=(endpoint_access.request.request_fingerprint_sha256),
        authorization_scope_sha256=billing_search_authorization_scope_sha256(
            endpoint_access.authorization_context,
            trusted_now=TRUSTED_NOW,
        ),
        generation_bundle_sha256=_CURSOR_GENERATION,
        snapshot_set_sha256=billing_search_snapshot_set_sha256(selected_release),
        trusted_now=int(trusted_time.timestamp()),
    )
    sealed_cursor = seal_billing_search_page_cursor(
        result.providers[-1].candidate.sort_key,
        keyring=CURSOR_KEYRING,
        binding=cursor_binding,
    )
    return replace(
        result,
        next_cursor=sealed_cursor,
        has_more=True,
        cursor_binding=cursor_binding,
    )


def test_next_cursor_is_reauthenticated_and_only_the_token_is_emitted() -> None:
    endpoint_access = _endpoint_access(limit="1")
    result = _cursor_result(endpoint_access)
    binding = result.cursor_binding
    assert binding is not None
    _, expected_token = authenticate_billing_search_sealed_page_cursor(
        result.next_cursor,
        keyring=CURSOR_KEYRING,
        trusted_now=binding.trusted_now,
        request_fingerprint_sha256=binding.request_fingerprint_sha256,
        authorization_context_sha256=binding.authorization_scope_sha256,
        generation_bundle_sha256=binding.generation_bundle_sha256,
        snapshot_set_sha256=binding.snapshot_set_sha256,
    )

    payload = shape_billing_search_response(
        endpoint_access,
        result,
        cursor_keyring=CURSOR_KEYRING,
        trusted_now=TRUSTED_NOW,
    )

    assert payload["pagination"] == {
        "limit": 1,
        "has_more": True,
        "next_cursor": expected_token,
    }
    assert expected_token.startswith("bsc1_cursor-v1_")


def test_cursor_rejects_a_different_generation_binding() -> None:
    endpoint_access = _endpoint_access(limit="1")
    result = _cursor_result(endpoint_access)
    assert result.cursor_binding is not None
    mismatched_binding = replace(
        result.cursor_binding,
        generation_bundle_sha256="b" * 64,
    )

    _assert_unavailable(
        endpoint_access,
        replace(result, cursor_binding=mismatched_binding),
        cursor_keyring=CURSOR_KEYRING,
    )


def test_cursor_rejects_mutated_ciphertext() -> None:
    endpoint_access = _endpoint_access(limit="1")
    result = _cursor_result(endpoint_access)
    object.__setattr__(
        result.next_cursor,
        "_BillingSearchSealedPageCursor__token",
        "bsc1_cursor-v1_" + "A" * 40,
    )

    _assert_unavailable(
        endpoint_access,
        result,
        cursor_keyring=CURSOR_KEYRING,
    )


def test_cursor_issued_at_must_equal_the_single_trusted_now() -> None:
    endpoint_access = _endpoint_access(limit="1")
    service_result = _cursor_result(endpoint_access)
    cursor_binding = service_result.cursor_binding
    assert cursor_binding is not None
    stale_state = BillingSearchCursorState(
        request_fingerprint_sha256=cursor_binding.request_fingerprint_sha256,
        authorization_context_sha256=cursor_binding.authorization_scope_sha256,
        generation_bundle_sha256=cursor_binding.generation_bundle_sha256,
        snapshot_set_sha256=cursor_binding.snapshot_set_sha256,
        sort_key=service_result.providers[-1].candidate.sort_key,
        issued_at=cursor_binding.trusted_now - 1,
        expires_at=(
            cursor_binding.trusted_now - 1 + BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS
        ),
    )
    stale_cursor = _new_sealed_page_cursor(
        seal_billing_search_cursor(
            stale_state,
            keyring=CURSOR_KEYRING,
            trusted_now=cursor_binding.trusted_now,
        ),
        stale_state,
    )

    _assert_unavailable(
        endpoint_access,
        replace(service_result, next_cursor=stale_cursor),
        cursor_keyring=CURSOR_KEYRING,
    )


def test_cursorless_response_does_not_require_a_cursor_keyring() -> None:
    endpoint_access = _endpoint_access()

    payload = shape_billing_search_response(
        endpoint_access,
        _matched_result(endpoint_access, _price()),
        trusted_now=TRUSTED_NOW,
    )

    assert payload["pagination"]["next_cursor"] is None
