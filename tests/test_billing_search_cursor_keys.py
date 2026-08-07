# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Billing-search cursor keyring configuration tests."""

from __future__ import annotations

from collections.abc import Mapping
import json

import pytest

from api import billing_search_cursor_keys as keys

KEY_ONE = bytes(range(32))
KEY_TWO = bytes(reversed(range(32)))
KEY_ONE_BASE64URL = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
KEY_TWO_BASE64URL = "Hx4dHBsaGRgXFhUUExIREA8ODQwLCgkIBwYFBAMCAQA"
SENSITIVE_SENTINEL = "SECRET-MUST-NOT-ESCAPE"


class _SecretRaisingEnvironment(Mapping):
    def __getitem__(self, key):
        del key
        raise RuntimeError(SENSITIVE_SENTINEL)

    def __iter__(self):
        return iter((keys.BILLING_SEARCH_CURSOR_KEYRING_ENV,))

    def __len__(self):
        return 1


def _document(*, active_key_id="cursor-v1", key_entries=None):
    return {
        "contract": keys.BILLING_SEARCH_CURSOR_KEYRING_CONTRACT,
        "active_key_id": active_key_id,
        "keys": (
            [
                {
                    "key_id": "cursor-v1",
                    "key_base64url": KEY_ONE_BASE64URL,
                }
            ]
            if key_entries is None
            else key_entries
        ),
    }


def _load(document=None):
    return keys.load_billing_search_cursor_keyring(
        {
            keys.BILLING_SEARCH_CURSOR_KEYRING_ENV: json.dumps(
                document or _document(),
                separators=(",", ":"),
                sort_keys=True,
            )
        }
    )


def test_cursor_keyring_loads_exact_rotation_set() -> None:
    keyring = _load(
        _document(
            active_key_id="cursor-v2",
            key_entries=[
                {"key_id": "cursor-v1", "key_base64url": KEY_ONE_BASE64URL},
                {"key_id": "cursor-v2", "key_base64url": KEY_TWO_BASE64URL},
            ],
        )
    )

    assert keyring.active_key_id == "cursor-v2"
    assert keyring.key_for("cursor-v1") == KEY_ONE
    assert keyring.key_for("cursor-v2") == KEY_TWO
    assert KEY_ONE_BASE64URL not in repr(keyring)


@pytest.mark.parametrize(
    "environment_map",
    [
        {},
        {keys.BILLING_SEARCH_CURSOR_KEYRING_ENV: ""},
        {keys.BILLING_SEARCH_CURSOR_KEYRING_ENV: "not-json"},
        {keys.BILLING_SEARCH_CURSOR_KEYRING_ENV: '{"contract":1}'},
        {
            keys.BILLING_SEARCH_CURSOR_KEYRING_ENV: (
                '{"active_key_id":"cursor-v1",'
                '"active_key_id":"cursor-v2","contract":'
                '"healthporta.billing-search-cursor-keyring.v1","keys":[]}'
            )
        },
        {keys.BILLING_SEARCH_CURSOR_KEYRING_ENV: "\N{SNOWMAN}"},
        {
            keys.BILLING_SEARCH_CURSOR_KEYRING_ENV: "x"
            * (keys.BILLING_SEARCH_CURSOR_MAX_KEYRING_BYTES + 1)
        },
    ],
)
def test_cursor_keyring_rejects_missing_malformed_or_unbounded_documents(
    environment_map,
) -> None:
    with pytest.raises(
        keys.BillingSearchCursorKeyringError,
        match="^billing_search_cursor_keyring_invalid$",
    ):
        keys.load_billing_search_cursor_keyring(environment_map)


@pytest.mark.parametrize(
    "document",
    [
        {**_document(), "extra": "field"},
        {**_document(), "contract": "wrong"},
        _document(active_key_id="missing"),
        _document(key_entries=[]),
        _document(
            key_entries=[{"key_id": "Cursor-v1", "key_base64url": KEY_ONE_BASE64URL}]
        ),
        _document(key_entries=[{"key_id": "cursor-v1", "key_base64url": "short"}]),
        _document(
            key_entries=[
                {
                    "key_id": "cursor-v1",
                    "key_base64url": KEY_ONE_BASE64URL + "=",
                }
            ]
        ),
        _document(
            key_entries=[
                {"key_id": "cursor-v1", "key_base64url": KEY_ONE_BASE64URL},
                {"key_id": "cursor-v1", "key_base64url": KEY_TWO_BASE64URL},
            ]
        ),
    ],
)
def test_cursor_keyring_rejects_invalid_closed_shapes(document) -> None:
    with pytest.raises(keys.BillingSearchCursorKeyringError):
        _load(document)


def test_cursor_keyring_sanitizes_secret_bearing_mapping_exceptions() -> None:
    with pytest.raises(keys.BillingSearchCursorKeyringError) as failure:
        keys.load_billing_search_cursor_keyring(_SecretRaisingEnvironment())

    assert failure.value.__cause__ is None
    assert failure.value.__context__ is None
    assert SENSITIVE_SENTINEL not in repr(failure.value)
