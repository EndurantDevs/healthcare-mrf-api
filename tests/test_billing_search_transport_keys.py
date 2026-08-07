# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import base64
import copy
import json
import pickle
from types import MappingProxyType

import pytest

from api import billing_search_transport_keys

KEY_ID = "synthetic-edge"
KEY = bytes(range(32))


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
            json.dumps(document_by_field, separators=(",", ":"), sort_keys=True)
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


def _keyring() -> billing_search_transport_keys.BillingSearchTransportKeyring:
    return billing_search_transport_keys.BillingSearchTransportKeyring(
        active_key_id=KEY_ID,
        keys_by_id={KEY_ID: KEY},
    )


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
    with pytest.raises(
        billing_search_transport_keys.BillingSearchTransportKeyringError
    ):
        billing_search_transport_keys.BillingSearchTransportKeyring(
            **constructor_options
        )


def test_keyring_constructor_failure_does_not_retain_raw_key_material() -> None:
    sensitive_key = b"synthetic-key-material-marker-12"
    with pytest.raises(
        billing_search_transport_keys.BillingSearchTransportKeyringError
    ) as captured:
        billing_search_transport_keys.BillingSearchTransportKeyring(
            active_key_id=KEY_ID,
            keys_by_id={KEY_ID: sensitive_key, "synthetic-second": sensitive_key},
        )

    traceback = captured.value.__traceback__
    constructor_frames = []
    while traceback is not None:
        if traceback.tb_frame.f_code.co_name == "__init__":
            constructor_frames.append(traceback.tb_frame.f_locals)
        traceback = traceback.tb_next
    assert constructor_frames
    assert all(
        repr(sensitive_key) not in repr(local_values)
        for local_values in constructor_frames
    )


def test_keyring_object_protocol_and_unknown_key_are_closed() -> None:
    keyring = _keyring()
    assert copy.copy(keyring) is keyring
    assert copy.deepcopy(keyring) is keyring
    with pytest.raises(TypeError) as set_error:
        keyring.synthetic = b"blocked"
    with pytest.raises(TypeError) as delete_error:
        del keyring.synthetic
    key_lookup_frames = []
    for rejected_key_id in ("synthetic-missing", "INVALID"):
        with pytest.raises(
            billing_search_transport_keys.BillingSearchTransportKeyringError
        ) as captured:
            keyring.key_for(rejected_key_id)
        traceback = captured.value.__traceback__
        while traceback is not None:
            if traceback.tb_frame.f_code.co_name == "key_for":
                key_lookup_frames.append(traceback.tb_frame.f_locals)
            traceback = traceback.tb_next
    assert key_lookup_frames
    assert all("self" not in local_values for local_values in key_lookup_frames)
    assert all(
        repr(KEY) not in repr(local_values) for local_values in key_lookup_frames
    )
    with pytest.raises(
        billing_search_transport_keys.BillingSearchTransportKeyringError
    ) as pickle_error:
        pickle.dumps(keyring)
    protocol_errors = (set_error.value, delete_error.value, pickle_error.value)
    protocol_frames = []
    for protocol_error in protocol_errors:
        traceback = protocol_error.__traceback__
        while traceback is not None:
            if traceback.tb_frame.f_globals.get("__name__") == (
                "api.billing_search_transport_keys"
            ):
                protocol_frames.append(traceback.tb_frame.f_locals)
            traceback = traceback.tb_next
    assert protocol_frames
    assert all("self" not in local_values for local_values in protocol_frames)


def _load_keyring_document(document: object) -> None:
    encoded_document = (
        document
        if isinstance(document, str)
        else json.dumps(document, separators=(",", ":"), sort_keys=True)
    )
    with pytest.raises(
        billing_search_transport_keys.BillingSearchTransportKeyringError
    ):
        billing_search_transport_keys.load_billing_search_transport_keyring(
            {
                billing_search_transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_ENV: (
                    encoded_document
                )
            }
        )


def test_keyring_loader_rejects_number_unicode_and_nonmapping_documents() -> None:
    _load_keyring_document("1")
    _load_keyring_document("\N{SNOWMAN}")
    with pytest.raises(
        billing_search_transport_keys.BillingSearchTransportKeyringError
    ):
        billing_search_transport_keys.load_billing_search_transport_keyring([])


def test_failed_keyring_load_does_not_retain_the_environment_document() -> None:
    sensitive_marker = "synthetic-key-document-marker"
    environment_by_name = {
        billing_search_transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_ENV: (
            sensitive_marker
        )
    }
    with pytest.raises(
        billing_search_transport_keys.BillingSearchTransportKeyringError
    ) as captured:
        billing_search_transport_keys.load_billing_search_transport_keyring(
            environment_by_name
        )

    traceback = captured.value.__traceback__
    loader_frames = []
    while traceback is not None:
        if traceback.tb_frame.f_code.co_name == (
            "load_billing_search_transport_keyring"
        ):
            loader_frames.append(traceback.tb_frame.f_locals)
        traceback = traceback.tb_next
    assert loader_frames
    assert all(
        sensitive_marker not in repr(local_values) for local_values in loader_frames
    )


def test_keyring_loader_rejects_bad_entry_shape_and_material() -> None:
    base_document_by_field = {
        "active_key_id": KEY_ID,
        "contract": (
            billing_search_transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_CONTRACT
        ),
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
                {"key_base64url": _encoded_key(KEY), "key_id": KEY_ID},
                {
                    "key_base64url": _encoded_key(bytes(reversed(KEY))),
                    "key_id": KEY_ID,
                },
            ],
        }
    )


@pytest.mark.parametrize("encoded", [None, "A", "AB"])
def test_keyring_base64url_decoder_rejects_closed_edges(encoded: object) -> None:
    assert billing_search_transport_keys._decoded_base64url(encoded) is None
