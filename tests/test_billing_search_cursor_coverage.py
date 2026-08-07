# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed boundary coverage for billing-search cursor components."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from api import billing_search_cursor as cursor
from api import billing_search_cursor_authentication as authentication
from api import billing_search_cursor_keys as cursor_keys
from api import billing_search_cursor_scope as cursor_scope
from api import billing_search_sealed_cursor as sealed_cursor
from api.billing_search_post_request import parse_billing_search_post_request
from api.ptg2_billing_search_contract import BillingSearchServingUnavailableError

REQUEST_DIGEST = "1" * 64
AUTHORIZATION_DIGEST = "2" * 64
GENERATION_DIGEST = "3" * 64
SNAPSHOT_DIGEST = "4" * 64
KEY = b"k" * 32
SORT_KEY = (
    0,
    1.25,
    0,
    "ptg2:203101:synthetic",
    1234567893,
    "00000000-0000-4000-8000-000000000001",
    "ab" * 32,
)
PLAN_RELEASE_ID = "hprelease_01K123456789ABCDEFGHJKMNPQ"


def _keyring() -> cursor.BillingSearchCursorKeyring:
    return cursor.BillingSearchCursorKeyring(
        active_key_id="cursor-v1",
        keys_by_id={"cursor-v1": KEY},
    )


def _state() -> cursor.BillingSearchCursorState:
    return cursor.BillingSearchCursorState(
        request_fingerprint_sha256=REQUEST_DIGEST,
        authorization_context_sha256=AUTHORIZATION_DIGEST,
        generation_bundle_sha256=GENERATION_DIGEST,
        snapshot_set_sha256=SNAPSHOT_DIGEST,
        sort_key=SORT_KEY,
        issued_at=1_800_000_000,
        expires_at=1_800_000_600,
    )


def _authenticated_token(plaintext: bytes) -> str:
    nonce = b"n" * cursor._NONCE_BYTES
    ciphertext = cursor.AESGCM(KEY).encrypt(
        nonce,
        plaintext,
        cursor._aad("cursor-v1"),
    )
    return "bsc1_cursor-v1_" + cursor._base64url_encode(nonce + ciphertext)


def _open(token: object) -> cursor.BillingSearchCursorState:
    return cursor.open_billing_search_cursor(
        token,
        keyring=_keyring(),
        trusted_now=1_800_000_100,
        request_fingerprint_sha256=REQUEST_DIGEST,
        authorization_context_sha256=AUTHORIZATION_DIGEST,
        generation_bundle_sha256=GENERATION_DIGEST,
        snapshot_set_sha256=SNAPSHOT_DIGEST,
    )


def _sealed_proof(monkeypatch) -> cursor.BillingSearchSealedPageCursor:
    monkeypatch.setattr(cursor.secrets, "token_bytes", lambda size: b"n" * size)
    state = _state()
    token = cursor.seal_billing_search_cursor(
        state,
        keyring=_keyring(),
        trusted_now=1_800_000_100,
    )
    return cursor._new_sealed_page_cursor(token, state)


def _authenticate(proof: object) -> tuple[cursor.BillingSearchCursorState, str]:
    return authentication.authenticate_billing_search_sealed_page_cursor(
        proof,
        keyring=_keyring(),
        trusted_now=1_800_000_100,
        request_fingerprint_sha256=REQUEST_DIGEST,
        authorization_context_sha256=AUTHORIZATION_DIGEST,
        generation_bundle_sha256=GENERATION_DIGEST,
        snapshot_set_sha256=SNAPSHOT_DIGEST,
    )


def test_cursor_proofs_and_keyrings_reject_deletion(monkeypatch) -> None:
    keyring = _keyring()
    proof = _sealed_proof(monkeypatch)

    with pytest.raises(TypeError, match="billing_search_cursor_invalid"):
        del keyring._BillingSearchCursorKeyring__active_key_id
    with pytest.raises(TypeError, match="billing_search_cursor_invalid"):
        del proof._BillingSearchSealedPageCursor__token

    assert proof.sort_key == SORT_KEY


def test_cursor_state_validation_rejects_wrong_and_damaged_instances() -> None:
    with pytest.raises(cursor.BillingSearchCursorError):
        cursor._validated_state(object())

    damaged_state = _state()
    object.__delattr__(damaged_state, "contract")
    with pytest.raises(cursor.BillingSearchCursorError) as failure:
        cursor._validated_state(damaged_state)

    assert failure.value.__cause__ is None


def test_cursor_envelope_rejects_noncanonical_and_short_payloads() -> None:
    with pytest.raises(cursor.BillingSearchCursorError):
        cursor._base64url_decode("AB")

    short_payload = cursor._base64url_encode(b"x" * (cursor._NONCE_BYTES + 16))
    with pytest.raises(cursor.BillingSearchCursorError):
        _open(f"bsc1_cursor-v1_{short_payload}")


@pytest.mark.parametrize(
    "replacement",
    [
        b'"sort_key":[NaN,1.25,',
        b'"sort_key":[0,1e999,',
    ],
)
def test_authenticated_json_rejects_constants_and_nonfinite_floats(
    replacement,
) -> None:
    plaintext = cursor._canonical_json_bytes(cursor._state_values(_state()))
    malformed_plaintext = plaintext.replace(
        b'"sort_key":[0,1.25,',
        replacement,
        1,
    )

    with pytest.raises(cursor.BillingSearchCursorError):
        _open(_authenticated_token(malformed_plaintext))


def test_cursor_sealing_rejects_time_keyring_nonce_and_size_guards(
    monkeypatch,
) -> None:
    state = _state()

    with pytest.raises(cursor.BillingSearchCursorError):
        cursor.seal_billing_search_cursor(
            state,
            keyring=_keyring(),
            trusted_now=state.issued_at - 1,
        )
    with pytest.raises(cursor.BillingSearchCursorError):
        cursor.seal_billing_search_cursor(
            state,
            keyring=object(),
            trusted_now=state.issued_at,
        )

    monkeypatch.setattr(cursor.secrets, "token_bytes", lambda _size: b"short")
    with pytest.raises(cursor.BillingSearchCursorError):
        cursor.seal_billing_search_cursor(
            state,
            keyring=_keyring(),
            trusted_now=state.issued_at,
        )

    monkeypatch.setattr(cursor.secrets, "token_bytes", lambda size: b"n" * size)
    monkeypatch.setattr(cursor, "BILLING_SEARCH_CURSOR_MAX_CHARACTERS", 32)
    with pytest.raises(cursor.BillingSearchCursorError):
        cursor.seal_billing_search_cursor(
            state,
            keyring=_keyring(),
            trusted_now=state.issued_at,
        )


def test_cursor_decoder_sanitizes_unexpected_state_constructor_failure(
    monkeypatch,
) -> None:
    plaintext = cursor._canonical_json_bytes(cursor._state_values(_state()))
    token = _authenticated_token(plaintext)

    def fail_state_construction(**_fields):
        raise RuntimeError("synthetic-constructor-detail")

    monkeypatch.setattr(cursor, "BillingSearchCursorState", fail_state_construction)
    with pytest.raises(cursor.BillingSearchCursorError) as failure:
        _open(token)

    assert failure.value.__cause__ is None
    assert "synthetic-constructor-detail" not in str(failure.value)


def test_cursor_open_rejects_wrong_keyring_type() -> None:
    with pytest.raises(cursor.BillingSearchCursorError):
        cursor._open_plaintext("unused", object())


def test_outgoing_cursor_authentication_rejects_wrong_and_damaged_proofs(
    monkeypatch,
) -> None:
    with pytest.raises(cursor.BillingSearchCursorError):
        _authenticate(object())

    damaged_proof = _sealed_proof(monkeypatch)
    object.__delattr__(
        damaged_proof,
        "_BillingSearchSealedPageCursor__token",
    )
    with pytest.raises(cursor.BillingSearchCursorError) as failure:
        _authenticate(damaged_proof)

    assert failure.value.__cause__ is None


def test_sealed_cursor_mint_rejects_nontext_token() -> None:
    with pytest.raises(ValueError, match="billing_search_cursor_invalid"):
        sealed_cursor._mint_billing_search_sealed_page_cursor(b"token", _state())


@pytest.mark.parametrize(
    "key_entry",
    [
        None,
        {
            "key_id": "cursor-v1",
            "key_base64url": "AA",
            "unexpected": True,
        },
    ],
)
def test_cursor_keyring_rejects_nonobject_and_open_key_entries(key_entry) -> None:
    assert cursor_keys._key_values(key_entry) is None


def test_cursor_keyring_rejects_nonmapping_environment() -> None:
    with pytest.raises(cursor_keys.BillingSearchCursorKeyringError):
        cursor_keys.load_billing_search_cursor_keyring(object())


def test_cursor_scope_rejects_wrong_keyring_and_release(monkeypatch) -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        cursor_scope.select_billing_search_cursor_chain_keyring(
            None,
            keyring=object(),
        )

    monkeypatch.setattr(
        cursor_scope,
        "validate_billing_search_post_request",
        lambda _request: SimpleNamespace(),
    )
    with pytest.raises(BillingSearchServingUnavailableError):
        cursor_scope.billing_search_stable_request_fingerprint(
            object(),
            plan_release_id="invalid-release",
            chain_keyring=_keyring(),
        )


def test_cursor_scope_rejects_missing_opaque_reference(monkeypatch) -> None:
    forged_request = SimpleNamespace(
        healthporta_plan_id="hpplan_01K123456789ABCDEFGHJKMNPQ",
        selector_kind="billing_entity_ref",
        billing_entity_ref=None,
    )
    monkeypatch.setattr(
        cursor_scope,
        "validate_billing_search_post_request",
        lambda _request: forged_request,
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        cursor_scope.billing_search_stable_request_fingerprint(
            object(),
            plan_release_id=PLAN_RELEASE_ID,
            chain_keyring=_keyring(),
        )


def test_cursor_scope_fingerprints_canonical_opaque_reference() -> None:
    request = parse_billing_search_post_request(
        {
            "healthporta_plan_id": "hpplan_01K123456789ABCDEFGHJKMNPQ",
            "billing_identity": {"billing_entity_ref": "be1_" + "a" * 64},
            "procedure": {
                "code_system": "CPT",
                "code": "00000",
                "modifiers": [],
                "place_of_service": [],
            },
            "geo": {"zip5": "00000", "radius_miles": 0},
            "page": {"limit": 25, "cursor": None},
        }
    )

    fingerprint = cursor_scope.billing_search_stable_request_fingerprint(
        request,
        plan_release_id=PLAN_RELEASE_ID,
        chain_keyring=_keyring(),
    )

    assert len(fingerprint) == 64
    assert set(fingerprint) <= set("0123456789abcdef")
