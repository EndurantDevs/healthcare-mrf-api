# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import base64
import json

import pytest

from api import billing_search_transport_keys


def _encoded_key(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _environment(**updates):
    document_by_field = {
        "active_key_id": "synthetic-a",
        "contract": (
            billing_search_transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_CONTRACT
        ),
        "keys": [
            {
                "key_base64url": _encoded_key(bytes(range(32))),
                "key_id": "synthetic-a",
            }
        ],
    }
    document_by_field.update(updates)
    return {
        billing_search_transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_ENV: (
            json.dumps(
                document_by_field,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
    }


def test_keyring_loads_one_redacted_rotation_set():
    keyring = billing_search_transport_keys.load_billing_search_transport_keyring(
        _environment()
    )

    assert keyring.active_key_id == "synthetic-a"
    assert keyring.key_for("synthetic-a") == bytes(range(32))
    assert repr(keyring) == "<redacted-billing-search-transport-keyring>"


@pytest.mark.parametrize(
    "environment",
    [
        {},
        {billing_search_transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_ENV: ("{}")},
        {
            billing_search_transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_ENV: (
                '{"active_key_id":"synthetic-a","active_key_id":"duplicate",'
                '"contract":"healthporta.billing-search-transport-keyring.v1",'
                '"keys":[]}'
            )
        },
    ],
)
def test_keyring_fails_closed_for_invalid_documents(environment):
    with pytest.raises(
        billing_search_transport_keys.BillingSearchTransportKeyringError,
        match="^billing_search_transport_keyring_invalid$",
    ):
        billing_search_transport_keys.load_billing_search_transport_keyring(environment)


def test_keyring_accepts_retained_rotation_key():
    retained_key = bytes(reversed(range(32)))
    environment = _environment(
        keys=[
            {
                "key_base64url": _encoded_key(bytes(range(32))),
                "key_id": "synthetic-a",
            },
            {
                "key_base64url": _encoded_key(retained_key),
                "key_id": "synthetic-b",
            },
        ]
    )

    keyring = billing_search_transport_keys.load_billing_search_transport_keyring(
        environment
    )

    assert keyring.key_for("synthetic-b") == retained_key


def test_keyring_rejects_duplicate_key_material():
    duplicate_key = _encoded_key(bytes(range(32)))
    environment = _environment(
        keys=[
            {"key_base64url": duplicate_key, "key_id": "synthetic-a"},
            {"key_base64url": duplicate_key, "key_id": "synthetic-b"},
        ]
    )

    with pytest.raises(
        billing_search_transport_keys.BillingSearchTransportKeyringError
    ):
        billing_search_transport_keys.load_billing_search_transport_keyring(environment)
