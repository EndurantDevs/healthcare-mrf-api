# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""AEAD reauthentication for sealed billing cursors before public emission."""

from __future__ import annotations

import hmac

from api.billing_search_cursor import (
    BillingSearchCursorKeyring,
    BillingSearchCursorState,
    BillingSearchSealedPageCursor,
    _canonical_json_bytes,
    _fail,
    _validated_state,
    open_billing_search_cursor,
)


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
    if not hmac.compare_digest(
        _canonical_json_bytes(_validated_state(authenticated_state)),
        _canonical_json_bytes(claimed_fields),
    ):
        raise _fail()
    return authenticated_state, token


__all__ = ["authenticate_billing_search_sealed_page_cursor"]
