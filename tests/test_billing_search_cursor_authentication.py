# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Outgoing billing-cursor AEAD authentication tests."""

from __future__ import annotations

import pytest

from api.billing_search_cursor import (
    BillingSearchCursorError,
    BillingSearchCursorGenerationExpired,
    BillingSearchCursorKeyring,
    BillingSearchCursorState,
)
from api.billing_search_cursor_authentication import (
    authenticate_billing_search_sealed_page_cursor,
)
from api.billing_search_pagination import (
    BillingSearchCursorBinding,
    seal_billing_search_page_cursor,
)

KEYRING = BillingSearchCursorKeyring(
    active_key_id="cursor-v1",
    keys_by_id={"cursor-v1": b"c" * 32},
)
BINDING = BillingSearchCursorBinding(
    request_fingerprint_sha256="1" * 64,
    authorization_scope_sha256="2" * 64,
    generation_bundle_sha256="3" * 64,
    snapshot_set_sha256="4" * 64,
    trusted_now=1_800_000_100,
)
SORT_KEY = (0, 1.25, 0, "ptg2:synthetic", 1234567893, "address-key")


def _authenticate(cursor, **overrides):
    expected_by_name = {
        "keyring": KEYRING,
        "trusted_now": BINDING.trusted_now,
        "request_fingerprint_sha256": BINDING.request_fingerprint_sha256,
        "authorization_context_sha256": BINDING.authorization_scope_sha256,
        "generation_bundle_sha256": BINDING.generation_bundle_sha256,
        "snapshot_set_sha256": BINDING.snapshot_set_sha256,
    }
    expected_by_name.update(overrides)
    return authenticate_billing_search_sealed_page_cursor(
        cursor,
        **expected_by_name,
    )


def test_outgoing_cursor_reauthenticates_exact_state_and_token() -> None:
    sealed = seal_billing_search_page_cursor(
        SORT_KEY,
        keyring=KEYRING,
        binding=BINDING,
    )

    authenticated_state, token = _authenticate(sealed)
    repeated_state, repeated_token = _authenticate(sealed)

    assert authenticated_state.sort_key == SORT_KEY
    assert repeated_state == authenticated_state
    assert repeated_token == token
    assert token.startswith("bsc1_cursor-v1_")
    assert not hasattr(sealed, "token")
    assert not hasattr(sealed, "sort_key")


def test_outgoing_cursor_rejects_mutated_claimed_state() -> None:
    sealed = seal_billing_search_page_cursor(
        SORT_KEY,
        keyring=KEYRING,
        binding=BINDING,
    )
    forged_state = BillingSearchCursorState(
        request_fingerprint_sha256=BINDING.request_fingerprint_sha256,
        authorization_context_sha256=BINDING.authorization_scope_sha256,
        generation_bundle_sha256=BINDING.generation_bundle_sha256,
        snapshot_set_sha256=BINDING.snapshot_set_sha256,
        sort_key=(1,),
        issued_at=BINDING.trusted_now,
        expires_at=BINDING.trusted_now + 1,
    )
    object.__setattr__(
        sealed,
        "_BillingSearchSealedPageCursor__state",
        forged_state,
    )

    with pytest.raises(BillingSearchCursorError):
        _authenticate(sealed)


def test_outgoing_cursor_rejects_mutated_token() -> None:
    sealed = seal_billing_search_page_cursor(
        SORT_KEY,
        keyring=KEYRING,
        binding=BINDING,
    )
    object.__setattr__(
        sealed,
        "_BillingSearchSealedPageCursor__token",
        "bsc1_cursor-v1_" + "A" * 40,
    )

    with pytest.raises(BillingSearchCursorError):
        _authenticate(sealed)


def test_outgoing_cursor_uses_expected_generation_not_claimed_generation() -> None:
    sealed = seal_billing_search_page_cursor(
        SORT_KEY,
        keyring=KEYRING,
        binding=BINDING,
    )

    with pytest.raises(BillingSearchCursorGenerationExpired):
        _authenticate(sealed, generation_bundle_sha256="5" * 64)
