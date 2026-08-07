# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import base64
import copy
import json
import pickle
from types import MappingProxyType

import pytest

from api import billing_search_post_transport as post_transport
from api import billing_search_transport_keys as transport_keys

KEY_ID = "synthetic-edge"
KEY = bytes(range(32))


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _keyring() -> transport_keys.BillingSearchTransportKeyring:
    return transport_keys.BillingSearchTransportKeyring(
        active_key_id=KEY_ID,
        keys_by_id={KEY_ID: KEY},
    )


def test_post_transport_accepts_a_finite_json_float_before_schema_validation() -> None:
    assert post_transport._finite_json_float("1.25") == 1.25


@pytest.mark.parametrize(
    "constructor_options",
    [
        {"active_key_id": "INVALID", "keys_by_id": {KEY_ID: KEY}},
        {"active_key_id": KEY_ID, "keys_by_id": {KEY_ID: b"short"}},
        {
            "active_key_id": KEY_ID,
            "keys_by_id": MappingProxyType({KEY_ID: KEY}),
        },
        {
            "active_key_id": "synthetic-missing",
            "keys_by_id": {KEY_ID: KEY},
        },
    ],
)
def test_keyring_constructor_rejects_invalid_closed_shapes(
    constructor_options: dict[str, object],
) -> None:
    with pytest.raises(transport_keys.BillingSearchTransportKeyringError):
        transport_keys.BillingSearchTransportKeyring(**constructor_options)


def test_keyring_object_protocol_and_unknown_key_are_closed() -> None:
    keyring = _keyring()
    assert copy.copy(keyring) is keyring
    assert copy.deepcopy(keyring) is keyring
    with pytest.raises(TypeError):
        keyring.synthetic = b"blocked"
    with pytest.raises(TypeError):
        del keyring.synthetic
    with pytest.raises(transport_keys.BillingSearchTransportKeyringError):
        keyring.key_for("synthetic-missing")
    with pytest.raises(transport_keys.BillingSearchTransportKeyringError):
        pickle.dumps(keyring)


def _load_keyring_document(document: object) -> None:
    encoded_document = (
        document
        if isinstance(document, str)
        else json.dumps(document, separators=(",", ":"), sort_keys=True)
    )
    with pytest.raises(transport_keys.BillingSearchTransportKeyringError):
        transport_keys.load_billing_search_transport_keyring(
            {transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_ENV: encoded_document}
        )


def test_keyring_loader_rejects_number_unicode_and_nonmapping_documents() -> None:
    _load_keyring_document("1")
    _load_keyring_document("\N{SNOWMAN}")
    with pytest.raises(transport_keys.BillingSearchTransportKeyringError):
        transport_keys.load_billing_search_transport_keyring([])


def test_keyring_loader_rejects_bad_entry_shape_and_material() -> None:
    base_document_by_field = {
        "active_key_id": KEY_ID,
        "contract": transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_CONTRACT,
    }
    _load_keyring_document({**base_document_by_field, "keys": [{"key_id": KEY_ID}]})
    _load_keyring_document(
        {
            **base_document_by_field,
            "keys": [{"key_base64url": "A", "key_id": KEY_ID}],
        }
    )
    _load_keyring_document(
        {
            **base_document_by_field,
            "keys": [
                {"key_base64url": _base64url(KEY), "key_id": KEY_ID},
                {
                    "key_base64url": _base64url(bytes(reversed(KEY))),
                    "key_id": KEY_ID,
                },
            ],
        }
    )


@pytest.mark.parametrize("encoded", [None, "A", "AB"])
def test_keyring_base64url_decoder_rejects_closed_edges(encoded: object) -> None:
    assert transport_keys._decoded_base64url(encoded) is None
