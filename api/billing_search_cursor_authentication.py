# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""AEAD reauthentication for sealed billing cursors before public emission."""

from __future__ import annotations

import hashlib
import hmac
import json

from api.billing_search_access_contract import (
    BillingSearchAuthorizationContext,
    validate_billing_search_authorization_context,
)

from api.billing_search_cursor import (
    BillingSearchCursorKeyring,
    BillingSearchCursorState,
    BillingSearchSealedPageCursor,
    _canonical_json_bytes,
    _fail,
    _validated_state,
    open_billing_search_cursor,
)

_AUTHORIZATION_SCOPE_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_CURSOR_AUTH_SCOPE_V1\x00"


def billing_search_authorization_scope_sha256(
    authorization_context: BillingSearchAuthorizationContext,
    *,
    trusted_now: str,
) -> str:
    """Digest stable authority while excluding issue and expiry timestamps."""

    validated = validate_billing_search_authorization_context(
        authorization_context,
        trusted_now=trusted_now,
    )
    stable_scope_by_name = {
        "audit_scope_sha256": validated.audit_scope_sha256,
        "capabilities": validated.capabilities,
        "plan_entitlement_sha256": validated.plan_entitlement_sha256,
        "principal_scope_sha256": validated.principal_scope_sha256,
        "quota_scope_sha256": validated.quota_scope_sha256,
        "tenant_scope_sha256": validated.tenant_scope_sha256,
    }
    encoded_scope = json.dumps(
        stable_scope_by_name,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    digest = hashlib.sha256()
    digest.update(_AUTHORIZATION_SCOPE_DOMAIN)
    digest.update(len(encoded_scope).to_bytes(8, "big"))
    digest.update(encoded_scope)
    return digest.hexdigest()


def authenticate_billing_search_sealed_page_cursor(
    cursor: object,
    *,
    keyring: BillingSearchCursorKeyring,
    trusted_now: int,
    request_fingerprint_sha256: str,
    authorization_context_sha256: str,
    generation_bundle_sha256: str,
    snapshot_set_sha256: str,
) -> tuple[BillingSearchCursorState, str]:
    """Authenticate an outgoing cursor before crossing the public boundary."""

    if (
        type(cursor) is not BillingSearchSealedPageCursor
        or type(keyring) is not BillingSearchCursorKeyring
    ):
        raise _fail()
    try:
        claimed_state = object.__getattribute__(
            cursor,
            "_BillingSearchSealedPageCursor__state",
        )
        token = object.__getattribute__(
            cursor,
            "_BillingSearchSealedPageCursor__token",
        )
    except (AttributeError, TypeError):
        raise _fail() from None
    claimed_fields = _validated_state(claimed_state)
    authenticated_state = open_billing_search_cursor(
        token,
        keyring=keyring,
        trusted_now=trusted_now,
        request_fingerprint_sha256=request_fingerprint_sha256,
        authorization_context_sha256=authorization_context_sha256,
        generation_bundle_sha256=generation_bundle_sha256,
        snapshot_set_sha256=snapshot_set_sha256,
    )
    authenticated_fields = _validated_state(authenticated_state)
    if not hmac.compare_digest(
        _canonical_json_bytes(authenticated_fields),
        _canonical_json_bytes(claimed_fields),
    ):
        raise _fail()
    return authenticated_state, token


__all__ = [
    "authenticate_billing_search_sealed_page_cursor",
    "billing_search_authorization_scope_sha256",
]
