# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Outgoing sealed-cursor authentication tests."""

from __future__ import annotations

import copy
import pickle

import pytest

from api import billing_search_cursor as cursor
from api import billing_search_cursor_authentication as authentication
from api import billing_search_sealed_cursor as sealed

REQUEST_DIGEST = "1" * 64
AUTH_DIGEST = "2" * 64
GENERATION_DIGEST = "3" * 64
SNAPSHOT_DIGEST = "4" * 64
SORT_KEY = (
    0,
    1.25,
    0,
    "ptg2:203101:synthetic",
    1234567893,
    "00000000-0000-4000-8000-000000000001",
    "ab" * 32,
)
KEYRING = cursor.BillingSearchCursorKeyring(
    active_key_id="cursor-v1",
    keys_by_id={"cursor-v1": b"c" * 32},
)


def _state(**overrides):
    state_values_by_name = {
        "request_fingerprint_sha256": REQUEST_DIGEST,
        "authorization_context_sha256": AUTH_DIGEST,
        "generation_bundle_sha256": GENERATION_DIGEST,
        "snapshot_set_sha256": SNAPSHOT_DIGEST,
        "sort_key": SORT_KEY,
        "issued_at": 1_800_000_000,
        "expires_at": 1_800_000_600,
    }
    state_values_by_name.update(overrides)
    return cursor.BillingSearchCursorState(**state_values_by_name)


def _sealed(monkeypatch):
    monkeypatch.setattr(cursor.secrets, "token_bytes", lambda size: b"n" * size)
    state = _state()
    token = cursor.seal_billing_search_cursor(
        state,
        keyring=KEYRING,
        trusted_now=1_800_000_100,
    )
    return cursor._new_sealed_page_cursor(token, state)


def _authenticate(sealed_cursor, **overrides):
    cursor_arguments_by_name = {
        "keyring": KEYRING,
        "trusted_now": 1_800_000_100,
        "request_fingerprint_sha256": REQUEST_DIGEST,
        "authorization_context_sha256": AUTH_DIGEST,
        "generation_bundle_sha256": GENERATION_DIGEST,
        "snapshot_set_sha256": SNAPSHOT_DIGEST,
    }
    cursor_arguments_by_name.update(overrides)
    return authentication.authenticate_billing_search_sealed_page_cursor(
        sealed_cursor,
        **cursor_arguments_by_name,
    )


def test_outgoing_cursor_is_reauthenticated_before_emission(monkeypatch) -> None:
    sealed_cursor = _sealed(monkeypatch)

    authenticated_state, token = _authenticate(sealed_cursor)

    assert authenticated_state == _state()
    assert token == sealed_cursor.token
    assert repr(sealed_cursor) == "<redacted-billing-search-cursor>"


def test_reauthentication_rejects_forged_claimed_state(monkeypatch) -> None:
    authentic_cursor = _sealed(monkeypatch)
    forged_claim = _state(sort_key=(*SORT_KEY[:-1], "cd" * 32))
    forged_cursor = sealed._mint_billing_search_sealed_page_cursor(
        authentic_cursor.token,
        forged_claim,
    )

    with pytest.raises(cursor.BillingSearchCursorError):
        _authenticate(forged_cursor)


def test_reauthentication_rejects_wrong_scope_and_expired_generation(
    monkeypatch,
) -> None:
    sealed_cursor = _sealed(monkeypatch)

    with pytest.raises(cursor.BillingSearchCursorError):
        _authenticate(sealed_cursor, request_fingerprint_sha256="5" * 64)
    with pytest.raises(cursor.BillingSearchCursorGenerationExpired):
        _authenticate(sealed_cursor, generation_bundle_sha256="5" * 64)


def test_sealed_cursor_cannot_be_constructed_mutated_or_serialized(
    monkeypatch,
) -> None:
    sealed_cursor = _sealed(monkeypatch)

    with pytest.raises(ValueError):
        cursor.BillingSearchSealedPageCursor()
    with pytest.raises(TypeError):
        sealed_cursor._BillingSearchSealedPageCursor__token = "replacement"
    with pytest.raises(ValueError):
        pickle.dumps(sealed_cursor)

    assert copy.copy(sealed_cursor) is sealed_cursor
    assert copy.deepcopy(sealed_cursor) is sealed_cursor
