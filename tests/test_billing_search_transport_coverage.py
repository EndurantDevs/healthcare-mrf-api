# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed-boundary coverage for billing-search signing and key rotation."""

from __future__ import annotations

import base64
import copy
import json

import pytest

from api import billing_search_post_gateway_transport as gateway
from api import billing_search_post_transport as post_transport
from api import billing_search_transport_keys as transport_keys
from tests.test_billing_search_post_gateway_transport import (
    BODY,
    KEY_ID,
    NOW,
    _headers,
    _keyring,
)
from tests.test_billing_search_transport_keys import _encoded_key, _environment


def _encoded(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


@pytest.mark.parametrize(
    ("active_key_id", "keys_by_id"),
    [
        ("INVALID", {"synthetic-a": b"a" * 32}),
        ("synthetic-a", {"synthetic-a": b"short"}),
        ("synthetic-a", {}),
        ("synthetic-a", {"synthetic-b": b"b" * 32}),
    ],
)
def test_keyring_constructor_rejects_noncanonical_rotation_sets(
    active_key_id: str,
    keys_by_id: dict[str, bytes],
) -> None:
    with pytest.raises(transport_keys.BillingSearchTransportKeyringError):
        transport_keys.BillingSearchTransportKeyring(
            active_key_id=active_key_id,
            keys_by_id=keys_by_id,
        )


def test_keyring_is_immutable_nonserializable_and_closed_for_unknown_ids() -> None:
    keyring = transport_keys.BillingSearchTransportKeyring(
        active_key_id="synthetic-a",
        keys_by_id={"synthetic-a": b"a" * 32},
    )

    with pytest.raises(TypeError):
        keyring.active_key_id = "synthetic-b"
    with pytest.raises(TypeError):
        del keyring.active_key_id
    with pytest.raises(transport_keys.BillingSearchTransportKeyringError):
        keyring.key_for("synthetic-b")
    with pytest.raises(transport_keys.BillingSearchTransportKeyringError):
        keyring.__reduce_ex__(4)

    assert copy.copy(keyring) is keyring
    assert copy.deepcopy(keyring) is keyring


def test_keyring_json_and_base64_helpers_reject_ambiguous_encodings() -> None:
    assert transport_keys._parse_json_bytes(b"1") is transport_keys._INVALID_JSON
    assert transport_keys._decoded_base64url(None) is None
    assert transport_keys._decoded_base64url("A") is None
    assert transport_keys._decoded_base64url("Zh") is None
    assert transport_keys._ascii_document("non-ascii-\N{SNOWMAN}") is None


@pytest.mark.parametrize(
    "environment",
    [
        object(),
        {transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_ENV: "non-ascii-\N{SNOWMAN}"},
        _environment(keys=[{"key_id": "synthetic-a"}]),
        _environment(
            keys=[{"key_base64url": _encoded_key(b"short"), "key_id": "synthetic-a"}]
        ),
        _environment(
            keys=[
                {
                    "key_base64url": _encoded_key(b"a" * 32),
                    "key_id": "synthetic-a",
                },
                {
                    "key_base64url": _encoded_key(b"b" * 32),
                    "key_id": "synthetic-a",
                },
            ]
        ),
    ],
)
def test_keyring_loader_closes_every_malformed_document(environment: object) -> None:
    assert transport_keys._keyring_from_environment(environment) is None


@pytest.mark.parametrize("value", [None, "0" * 64, "A" * 64])
def test_gateway_rejects_noncanonical_scope_digests(value: object) -> None:
    with pytest.raises(gateway.BillingSearchPostTransportAuthenticationError):
        gateway._canonical_sha256(value)


def test_gateway_rejects_invalid_uuid_time_json_and_release_values() -> None:
    with pytest.raises(gateway.BillingSearchPostTransportAuthenticationError):
        gateway._canonical_uuid4("not-a-uuid")
    with pytest.raises(gateway.BillingSearchPostTransportAuthenticationError):
        gateway._canonical_utc(None)
    with pytest.raises(gateway.BillingSearchPostTransportAuthenticationError):
        gateway._canonical_utc("2026-02-31T00:00:00Z")
    with pytest.raises(gateway.BillingSearchPostTransportAuthenticationError):
        gateway._canonical_json_bytes({object()})
    with pytest.raises(gateway.BillingSearchPostTransportAuthenticationError):
        gateway.billing_search_plan_entitlement_sha256(None)
    with pytest.raises(gateway.BillingSearchPostTransportAuthenticationError):
        gateway.billing_search_plan_entitlement_sha256("hprelease_invalid")


@pytest.mark.parametrize("encoded", [None, "", "*", "A", "Zh"])
def test_gateway_base64_decoder_accepts_only_canonical_values(encoded: object) -> None:
    with pytest.raises(gateway.BillingSearchPostTransportAuthenticationError):
        gateway._decoded_base64url(encoded, maximum_characters=64)


@pytest.mark.parametrize(
    "context_bytes",
    [
        b"x" * (gateway.BILLING_SEARCH_TRANSPORT_MAX_CONTEXT_BYTES + 1),
        b"1",
        b'{"duplicate":true,"duplicate":false}',
        b"{}",
    ],
)
def test_gateway_context_parser_rejects_oversize_or_ambiguous_json(
    context_bytes: bytes,
) -> None:
    with pytest.raises(gateway.BillingSearchPostTransportAuthenticationError):
        gateway._parsed_context(_encoded(context_bytes))


def test_gateway_context_parser_rejects_noncanonical_json_bytes() -> None:
    context, _key_id, _signature = _headers()
    decoded = json.loads(base64.urlsafe_b64decode(context + "=" * (-len(context) % 4)))
    noncanonical = json.dumps(decoded, indent=1).encode("ascii")

    with pytest.raises(gateway.BillingSearchPostTransportAuthenticationError):
        gateway._parsed_context(_encoded(noncanonical))


def test_gateway_signature_helpers_reject_unbounded_or_wrongly_typed_inputs() -> None:
    with pytest.raises(gateway.BillingSearchPostTransportAuthenticationError):
        gateway._signature_message("x" * 65_536, b"context", b"body")
    with pytest.raises(gateway.BillingSearchPostTransportAuthenticationError):
        gateway._verified_signature(
            keyring=object(),
            key_id=KEY_ID,
            encoded_signature=_encoded(b"s" * 32),
            context_bytes=b"context",
            body_bytes=BODY,
        )
    with pytest.raises(gateway.BillingSearchPostTransportAuthenticationError):
        gateway._verified_signature(
            keyring=_keyring(),
            key_id=KEY_ID,
            encoded_signature=_encoded(b"short"),
            context_bytes=b"context",
            body_bytes=BODY,
        )


@pytest.mark.parametrize(
    "capabilities",
    [None, ["pricing:billing-search", 1], [], ["pricing:unknown"]],
)
def test_gateway_accepts_only_frozen_capability_sets(capabilities: object) -> None:
    with pytest.raises(gateway.BillingSearchPostTransportAuthenticationError):
        gateway._capabilities(capabilities)


def test_gateway_wraps_unexpected_verifier_failure_without_value_leak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        gateway,
        "_parsed_context",
        lambda _header: (_ for _ in ()).throw(RuntimeError("synthetic-detail")),
    )

    with pytest.raises(
        gateway.BillingSearchPostTransportAuthenticationError,
        match="^billing_search_post_transport_authentication_invalid$",
    ) as captured:
        gateway.verify_billing_search_post_transport(
            "context",
            KEY_ID,
            "signature",
            body_bytes=BODY,
            keyring=_keyring(),
            trusted_now=NOW,
        )

    assert captured.value.__cause__ is None


def test_post_transport_parses_finite_fraction_and_rejects_nonfinite_fraction() -> None:
    payload = json.loads(BODY)
    payload["geo"]["radius_miles"] = 1.5
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("ascii")

    request = post_transport.parse_billing_search_post_transport(
        body,
        method=post_transport.BILLING_SEARCH_POST_METHOD,
        path=post_transport.BILLING_SEARCH_POST_PATH,
        media_type=post_transport.BILLING_SEARCH_POST_MEDIA_TYPE,
    )

    assert request.radius_miles == 1.5
    with pytest.raises(ValueError):
        post_transport._finite_json_float("1e400")
