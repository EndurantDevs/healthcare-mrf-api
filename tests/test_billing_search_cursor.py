# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Confidential billing-search cursor contract tests."""

from __future__ import annotations

import base64
import copy
from dataclasses import asdict
import json
import pickle

import pytest

from api import billing_search_cursor as cursor

REQUEST_DIGEST = "1" * 64
AUTH_DIGEST = "2" * 64
GENERATION_DIGEST = "3" * 64
SNAPSHOT_DIGEST = "4" * 64
GROUP_REF = "ab" * 16
KEY_ONE = b"1" * 32
KEY_TWO = b"2" * 32


def _keyring(*, active: str = "k1", retain_old: bool = True):
    key_material_by_id = {"k1": KEY_ONE, "k2": KEY_TWO}
    if not retain_old:
        key_material_by_id = {active: key_material_by_id[active]}
    return cursor.BillingSearchCursorKeyring(
        active_key_id=active,
        keys_by_id=key_material_by_id,
    )


def _state(**overrides):
    state_fields_by_name = {
        "request_fingerprint_sha256": REQUEST_DIGEST,
        "authorization_context_sha256": AUTH_DIGEST,
        "generation_bundle_sha256": GENERATION_DIGEST,
        "snapshot_set_sha256": SNAPSHOT_DIGEST,
        "sort_key": (1, 0.0, 1000000004, 5, 7, 11, 0, 2, 3, GROUP_REF),
        "issued_at": 1_800_000_000,
        "expires_at": 1_800_000_600,
    }
    state_fields_by_name.update(overrides)
    return cursor.BillingSearchCursorState(**state_fields_by_name)


def _seal(monkeypatch, state=None, *, keyring=None):
    monkeypatch.setattr(cursor.secrets, "token_bytes", lambda size: b"n" * size)
    return cursor.seal_billing_search_cursor(
        state or _state(),
        keyring=keyring or _keyring(),
        trusted_now=1_800_000_100,
    )


def _open(token, *, keyring=None, now=1_800_000_100, **overrides):
    binding_digest_by_name = {
        "request_fingerprint_sha256": REQUEST_DIGEST,
        "authorization_context_sha256": AUTH_DIGEST,
        "generation_bundle_sha256": GENERATION_DIGEST,
        "snapshot_set_sha256": SNAPSHOT_DIGEST,
    }
    binding_digest_by_name.update(overrides)
    return cursor.open_billing_search_cursor(
        token,
        keyring=keyring or _keyring(),
        trusted_now=now,
        **binding_digest_by_name,
    )


def _authenticated_token_for_plaintext(plaintext: bytes) -> str:
    nonce = b"n" * cursor._NONCE_BYTES
    ciphertext = cursor.AESGCM(KEY_ONE).encrypt(
        nonce,
        plaintext,
        cursor._aad("k1"),
    )
    encoded_payload = cursor._base64url_encode(nonce + ciphertext)
    return f"bsc1_k1_{encoded_payload}"


def test_cursor_round_trip_is_confidential_and_redacted(monkeypatch) -> None:
    state = _state()
    token = _seal(monkeypatch, state)

    assert token.startswith("bsc1_k1_")
    assert REQUEST_DIGEST not in token
    assert GROUP_REF not in token
    assert "1000000004" not in token
    assert repr(state) == "<redacted-billing-search-cursor>"
    assert repr(_keyring()) == "<redacted-billing-search-cursor>"
    assert _open(token) == state


def test_cursor_keyring_is_frozen_after_construction() -> None:
    keyring = _keyring()

    with pytest.raises((AttributeError, TypeError)):
        keyring._BillingSearchCursorKeyring__active_key_id = "k2"
    with pytest.raises((AttributeError, TypeError)):
        del keyring._BillingSearchCursorKeyring__keys

    assert keyring.active_key_id == "k1"
    assert keyring.key_for("k1") == KEY_ONE


def test_cursor_keyring_blocks_generic_secret_serialization_and_copying() -> None:
    keyring = _keyring()

    with pytest.raises(TypeError):
        asdict(keyring)
    with pytest.raises(cursor.BillingSearchCursorError) as failure:
        pickle.dumps(keyring)

    assert copy.copy(keyring) is keyring
    assert copy.deepcopy(keyring) is keyring
    assert KEY_ONE not in repr(failure.value).encode("ascii")


def test_cursor_rejects_authenticated_noncanonical_json() -> None:
    canonical_plaintext = cursor._canonical_json_bytes(cursor._state_values(_state()))
    duplicate_plaintext = canonical_plaintext.replace(
        b'{"authorization_context_sha256":"' + AUTH_DIGEST.encode("ascii") + b'",',
        b'{"authorization_context_sha256":"'
        + AUTH_DIGEST.encode("ascii")
        + b'","authorization_context_sha256":"'
        + AUTH_DIGEST.encode("ascii")
        + b'",',
        1,
    )
    spaced_plaintext = canonical_plaintext.replace(b'","contract"', b'", "contract"')

    for plaintext in (duplicate_plaintext, spaced_plaintext):
        with pytest.raises(
            cursor.BillingSearchCursorError,
            match="^billing_search_cursor_invalid$",
        ):
            _open(_authenticated_token_for_plaintext(plaintext))


def test_cursor_parse_failure_does_not_retain_plaintext_in_exception_chain() -> None:
    secret_plaintext = b'{"sort_key":"' + GROUP_REF.encode("ascii")
    token = _authenticated_token_for_plaintext(secret_plaintext)

    with pytest.raises(cursor.BillingSearchCursorError) as failure:
        _open(token)

    assert failure.value.__cause__ is None
    assert failure.value.__context__ is None
    assert GROUP_REF not in repr(failure.value)


def test_cursor_rejects_authenticated_overflow_float_without_exception_leak() -> None:
    canonical_plaintext = cursor._canonical_json_bytes(cursor._state_values(_state()))
    overflow_plaintext = canonical_plaintext.replace(b"0.0", b"1e999", 1)

    with pytest.raises(cursor.BillingSearchCursorError) as failure:
        _open(_authenticated_token_for_plaintext(overflow_plaintext))

    assert str(failure.value) == "billing_search_cursor_invalid"
    assert failure.value.__cause__ is None
    assert failure.value.__context__ is None


def test_cursor_ciphertext_is_not_plain_base64_json(monkeypatch) -> None:
    token = _seal(monkeypatch)
    encoded_payload = token.split("_", 2)[2]
    decoded_payload = base64.urlsafe_b64decode(
        encoded_payload + "=" * (-len(encoded_payload) % 4)
    )

    with pytest.raises((UnicodeDecodeError, json.JSONDecodeError)):
        json.loads(decoded_payload.decode("utf-8"))


@pytest.mark.parametrize(
    ("binding_name", "binding_value"),
    [
        ("request_fingerprint_sha256", "5" * 64),
        ("authorization_context_sha256", "5" * 64),
        ("generation_bundle_sha256", "5" * 64),
        ("snapshot_set_sha256", "5" * 64),
    ],
)
def test_cursor_rejects_every_binding_mismatch(
    monkeypatch,
    binding_name,
    binding_value,
) -> None:
    token = _seal(monkeypatch)

    expected_exception = (
        cursor.BillingSearchCursorGenerationExpired
        if binding_name in {"generation_bundle_sha256", "snapshot_set_sha256"}
        else cursor.BillingSearchCursorError
    )
    with pytest.raises(expected_exception):
        _open(token, **{binding_name: binding_value})


def test_cursor_rejects_tampering_and_wrong_key(monkeypatch) -> None:
    token = _seal(monkeypatch)
    replacement = "A" if token[-1] != "A" else "B"

    with pytest.raises(
        cursor.BillingSearchCursorError, match="^billing_search_cursor_invalid$"
    ):
        _open(token[:-1] + replacement)
    with pytest.raises(
        cursor.BillingSearchCursorError, match="^billing_search_cursor_invalid$"
    ):
        _open(
            token,
            keyring=cursor.BillingSearchCursorKeyring(
                active_key_id="k1", keys_by_id={"k1": b"x" * 32}
            ),
        )


def test_cursor_key_rotation_reads_retained_old_key(monkeypatch) -> None:
    old_token = _seal(monkeypatch, keyring=_keyring(active="k1"))
    rotated_keyring = _keyring(active="k2")
    new_token = _seal(monkeypatch, keyring=rotated_keyring)

    assert old_token.startswith("bsc1_k1_")
    assert new_token.startswith("bsc1_k2_")
    assert _open(old_token, keyring=rotated_keyring) == _state()
    with pytest.raises(cursor.BillingSearchCursorError):
        _open(old_token, keyring=_keyring(active="k2", retain_old=False))


@pytest.mark.parametrize("now", [1_799_999_999, 1_800_000_600])
def test_cursor_rejects_time_outside_closed_validity(monkeypatch, now) -> None:
    token = _seal(monkeypatch)

    with pytest.raises(
        cursor.BillingSearchCursorError, match="^billing_search_cursor_invalid$"
    ):
        _open(token, now=now)


@pytest.mark.parametrize(
    "state_overrides",
    [
        {"issued_at": True},
        {"expires_at": 1_800_000_000},
        {
            "expires_at": (
                1_800_000_000 + cursor.BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS + 1
            )
        },
        {"sort_key": ()},
        {"sort_key": [1, 2]},
        {"sort_key": (float("nan"),)},
        {"sort_key": (True,)},
        {"sort_key": ("\N{SNOWMAN}",)},
        {"request_fingerprint_sha256": "0" * 64},
    ],
)
def test_cursor_state_rejects_noncanonical_values(state_overrides) -> None:
    with pytest.raises(
        cursor.BillingSearchCursorError, match="^billing_search_cursor_invalid$"
    ):
        _state(**state_overrides)


@pytest.mark.parametrize(
    ("active_key_id", "keys_by_id"),
    [
        ("missing", {"k1": KEY_ONE}),
        ("K1", {"K1": KEY_ONE}),
        ("k1", {"k1": b"short"}),
        ("k1", {"k1": KEY_ONE, "k2": KEY_ONE}),
        ("k1", {}),
    ],
)
def test_keyring_rejects_invalid_or_ambiguous_keys(
    active_key_id,
    keys_by_id,
) -> None:
    with pytest.raises(
        cursor.BillingSearchCursorError, match="^billing_search_cursor_invalid$"
    ):
        cursor.BillingSearchCursorKeyring(
            active_key_id=active_key_id,
            keys_by_id=keys_by_id,
        )


@pytest.mark.parametrize(
    "token",
    [
        None,
        "",
        "bsc1",
        "bsc1_K1_AA",
        "bsc1_k1_@@",
        "bsc1_k1_A",
        "bsc1_k1_" + "A" * 2048,
    ],
)
def test_cursor_rejects_malformed_envelopes(token) -> None:
    with pytest.raises(
        cursor.BillingSearchCursorError, match="^billing_search_cursor_invalid$"
    ):
        _open(token)


def test_cursor_sealing_requires_current_state(monkeypatch) -> None:
    with pytest.raises(
        cursor.BillingSearchCursorError, match="^billing_search_cursor_invalid$"
    ):
        _seal(monkeypatch, _state(issued_at=1_800_000_200))
    with pytest.raises(
        cursor.BillingSearchCursorError, match="^billing_search_cursor_invalid$"
    ):
        _seal(monkeypatch, _state(expires_at=1_800_000_100))


def test_sealed_cursor_is_opaque_immutable_and_not_serializable() -> None:
    sealed_cursor = cursor._mint_billing_search_sealed_page_cursor(
        "opaque-token",
        object(),
    )

    with pytest.raises(ValueError, match="^billing_search_cursor_invalid$"):
        cursor.BillingSearchSealedPageCursor()
    with pytest.raises(TypeError, match="^billing_search_cursor_invalid$"):
        sealed_cursor.claimed_state = object()
    with pytest.raises(TypeError, match="^billing_search_cursor_invalid$"):
        del sealed_cursor.claimed_state
    with pytest.raises(ValueError, match="^billing_search_cursor_invalid$"):
        pickle.dumps(sealed_cursor)

    assert repr(sealed_cursor) == "<redacted-billing-search-cursor>"
    assert str(sealed_cursor) == "<redacted-billing-search-cursor>"
    assert copy.copy(sealed_cursor) is sealed_cursor
    assert copy.deepcopy(sealed_cursor) is sealed_cursor

    with pytest.raises(ValueError, match="^billing_search_cursor_invalid$"):
        cursor._mint_billing_search_sealed_page_cursor(object(), object())


def test_cursor_internal_envelopes_fail_closed_on_forged_shapes() -> None:
    with pytest.raises(cursor.BillingSearchCursorError):
        cursor._validated_state(object())

    missing_state = object.__new__(cursor.BillingSearchCursorState)
    with pytest.raises(cursor.BillingSearchCursorError):
        cursor._validated_state(missing_state)

    with pytest.raises(cursor.BillingSearchCursorError):
        cursor._base64url_decode("AB")

    state = _state()
    for token in (None, "wrong_k1_AA", "bsc1_k1_AA"):
        with pytest.raises(cursor.BillingSearchCursorError):
            cursor._new_sealed_page_cursor(token, state)

    assert cursor._parse_authenticated_json(b"NaN") is cursor._INVALID_JSON


def test_cursor_sealing_rejects_bad_nonce_and_oversized_envelope(monkeypatch) -> None:
    monkeypatch.setattr(cursor.secrets, "token_bytes", lambda _size: b"short")
    with pytest.raises(cursor.BillingSearchCursorError):
        cursor.seal_billing_search_cursor(
            _state(),
            keyring=_keyring(),
            trusted_now=1_800_000_100,
        )

    monkeypatch.setattr(
        cursor.secrets,
        "token_bytes",
        lambda size: b"n" * size,
    )
    monkeypatch.setattr(cursor, "BILLING_SEARCH_CURSOR_MAX_CHARACTERS", 1)
    with pytest.raises(cursor.BillingSearchCursorError):
        cursor.seal_billing_search_cursor(
            _state(),
            keyring=_keyring(),
            trusted_now=1_800_000_100,
        )


def test_cursor_open_rejects_authenticated_payload_shorter_than_aead_tag() -> None:
    with pytest.raises(cursor.BillingSearchCursorError):
        _open("bsc1_k1_AA")
