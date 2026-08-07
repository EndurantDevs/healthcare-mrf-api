# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stable selector and plan scope for billing-search cursor chains."""

from __future__ import annotations

import hashlib
import hmac

from api.billing_search_cursor import BillingSearchCursorKeyring, _cursor_parts
from api.billing_search_post_request import (
    BillingSearchPostRequest,
    apply_entitled_billing_search_tax_identity,
    bind_billing_search_post_request_fingerprint,
    validate_billing_search_post_request,
)
from api.plan_release_serving import normalize_plan_release_id
from api.ptg2_billing_search_contract import serving_unavailable

_STABLE_SELECTOR_HMAC_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_STABLE_SELECTOR_HMAC_V1\x00"


def _framed_hmac_sha256(
    key: bytes,
    domain: bytes,
    *values: bytes,
) -> str:
    digest = hmac.new(key, digestmod=hashlib.sha256)
    digest.update(domain)
    for value in values:
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)
    return digest.hexdigest()


def select_billing_search_cursor_chain_keyring(
    cursor_token: object | None,
    *,
    keyring: BillingSearchCursorKeyring,
) -> BillingSearchCursorKeyring:
    """Select the active first-page key or a continuation's retained key."""

    if type(keyring) is not BillingSearchCursorKeyring:
        raise serving_unavailable()
    selected_key_id = (
        keyring.active_key_id
        if cursor_token is None
        else _cursor_parts(cursor_token)[0]
    )
    selected_key = keyring.key_for(selected_key_id)
    return BillingSearchCursorKeyring(
        active_key_id=selected_key_id,
        keys_by_id={selected_key_id: selected_key},
    )


def billing_search_stable_request_fingerprint(
    request: BillingSearchPostRequest,
    *,
    plan_release_id: str,
    chain_keyring: BillingSearchCursorKeyring,
) -> str:
    """Bind stable request and plan lineage to a keyed selector component."""

    validated_request = validate_billing_search_post_request(request)
    normalized_release_id = normalize_plan_release_id(plan_release_id)
    if (
        normalized_release_id is None
        or normalized_release_id != plan_release_id
        or type(chain_keyring) is not BillingSearchCursorKeyring
    ):
        raise serving_unavailable()
    key_id = chain_keyring.active_key_id
    key = chain_keyring.key_for(key_id)
    common_components = (
        key_id.encode("ascii"),
        plan_release_id.encode("ascii"),
        validated_request.healthporta_plan_id.encode("ascii"),
        validated_request.selector_kind.encode("ascii"),
    )
    if validated_request.selector_kind == "tax_identity":
        selector_component_sha256 = apply_entitled_billing_search_tax_identity(
            validated_request,
            lambda identity_type, identity_value: _framed_hmac_sha256(
                key,
                _STABLE_SELECTOR_HMAC_DOMAIN,
                *common_components,
                identity_type.encode("ascii"),
                identity_value.encode("ascii"),
            ),
        )
    else:
        billing_entity_ref = validated_request.billing_entity_ref
        if type(billing_entity_ref) is not str:
            raise serving_unavailable()
        selector_component_sha256 = _framed_hmac_sha256(
            key,
            _STABLE_SELECTOR_HMAC_DOMAIN,
            *common_components,
            billing_entity_ref.encode("ascii"),
        )
    return bind_billing_search_post_request_fingerprint(
        validated_request,
        selector_scope_sha256=selector_component_sha256,
    )


__all__ = [
    "billing_search_stable_request_fingerprint",
    "select_billing_search_cursor_chain_keyring",
]
