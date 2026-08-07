# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import base64
import hashlib
import hmac
import json

import pytest

from api import billing_search_post_gateway_transport as transport
from api.billing_search_transport_keys import BillingSearchTransportKeyring


PLAN_RELEASE_ID = "hprelease_" + "0" * 26
SYNTHETIC_EIN = "99-" + "9" * 7
BODY = json.dumps(
    {
        "billing_identity": {
            "tax_identity": {"type": "ein", "value": SYNTHETIC_EIN}
        },
        "geo": {"radius_miles": 0, "zip5": "00000"},
        "healthporta_plan_id": "hpplan_" + "0" * 26,
        "procedure": {
            "code": "00000",
            "code_system": "CPT",
            "modifiers": [],
            "place_of_service": [],
        },
    },
    separators=(",", ":"),
    sort_keys=True,
).encode("ascii")
KEY = bytes(range(32))
KEY_ID = "synthetic-a"
NOW = "2026-08-07T10:00:10Z"


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _context(**updates):
    plan_entitlement = transport.billing_search_plan_entitlement_sha256(
        PLAN_RELEASE_ID
    )
    context_by_field = {
        "audience": transport.BILLING_SEARCH_TRANSPORT_AUDIENCE,
        "audit_scope_sha256": _sha("audit"),
        "capabilities": ["pricing:billing-search"],
        "contract": transport.BILLING_SEARCH_POST_TRANSPORT_CONTRACT,
        "expires_at": "2026-08-07T10:01:00Z",
        "issued_at": "2026-08-07T10:00:00Z",
        "issuer": transport.BILLING_SEARCH_TRANSPORT_ISSUER,
        "media_type": "application/json",
        "metering_receipt_sha256": "0" * 64,
        "metering_request_id": "00000000-0000-4000-8000-000000000000",
        "method": "POST",
        "path": "/api/v1/pricing/providers/search-by-procedure",
        "plan_entitlement_sha256": plan_entitlement,
        "plan_release_id": PLAN_RELEASE_ID,
        "principal_scope_sha256": _sha("principal"),
        "quota_scope_sha256": _sha("quota"),
        "request_shape_sha256": _sha("request-shape"),
        "tenant_scope_sha256": _sha("tenant"),
    }
    context_by_field.update(updates)
    context_by_field["metering_receipt_sha256"] = (
        transport._metering_receipt_sha256(context_by_field)
    )
    return context_by_field


def _headers(body: bytes = BODY, **context_updates):
    context = _context(**context_updates)
    context_bytes = transport._canonical_json_bytes(context)
    signature = hmac.new(
        KEY,
        transport._signature_message(KEY_ID, context_bytes, body),
        hashlib.sha256,
    ).digest()
    return (
        base64.urlsafe_b64encode(context_bytes).rstrip(b"=").decode("ascii"),
        KEY_ID,
        base64.urlsafe_b64encode(signature).rstrip(b"=").decode("ascii"),
    )


def _keyring():
    return BillingSearchTransportKeyring(
        active_key_id=KEY_ID,
        keys_by_id={KEY_ID: KEY},
    )


def test_transport_authenticates_exact_body_without_retaining_body_digest():
    headers = _headers()

    verified = transport.verify_billing_search_post_transport(
        *headers,
        body_bytes=BODY,
        keyring=_keyring(),
        trusted_now=NOW,
    )

    assert verified.plan_release_id == PLAN_RELEASE_ID
    assert verified.request_shape_sha256 == _sha("request-shape")
    assert verified.authorization_context.capabilities == (
        "pricing:billing-search",
    )
    assert SYNTHETIC_EIN not in repr(verified)
    decoded_context = json.loads(
        base64.urlsafe_b64decode(headers[0] + "=" * (-len(headers[0]) % 4))
    )
    assert all("body" not in field_name for field_name in decoded_context)
    assert SYNTHETIC_EIN not in repr(decoded_context)


@pytest.mark.parametrize(
    ("body", "context_updates"),
    [
        (BODY + b" ", {}),
        (BODY, {"method": "GET"}),
        (BODY, {"media_type": "application/json; charset=utf-8"}),
        (BODY, {"expires_at": "2026-08-07T10:02:00Z"}),
    ],
)
def test_transport_rejects_body_or_context_substitution(body, context_updates):
    headers = _headers(**context_updates)

    with pytest.raises(
        transport.BillingSearchPostTransportAuthenticationError,
        match="^billing_search_post_transport_authentication_invalid$",
    ):
        transport.verify_billing_search_post_transport(
            *headers,
            body_bytes=body,
            keyring=_keyring(),
            trusted_now=NOW,
        )


def test_transport_accepts_the_stronger_provenance_capability():
    headers = _headers(
        capabilities=[
            "pricing:billing-search",
            "pricing:billing-search:provenance",
        ]
    )

    verified = transport.verify_billing_search_post_transport(
        *headers,
        body_bytes=BODY,
        keyring=_keyring(),
        trusted_now=NOW,
    )

    assert verified.authorization_context.capabilities[-1].endswith(
        ":provenance"
    )


def test_transport_exposes_one_stable_signed_id_for_the_external_replay_gate():
    headers = _headers()

    first = transport.verify_billing_search_post_transport(
        *headers,
        body_bytes=BODY,
        keyring=_keyring(),
        trusted_now=NOW,
    )
    second = transport.verify_billing_search_post_transport(
        *headers,
        body_bytes=BODY,
        keyring=_keyring(),
        trusted_now=NOW,
    )

    assert first.metering_request_id == "00000000-0000-4000-8000-000000000000"
    assert second.metering_request_id == first.metering_request_id
    assert second.verified_state_sha256 == first.verified_state_sha256


@pytest.mark.parametrize(
    "trusted_now",
    ["2026-08-07T09:59:59Z", "2026-08-07T10:01:00Z"],
)
def test_transport_rejects_not_yet_valid_or_expired_context(trusted_now):
    headers = _headers()

    with pytest.raises(transport.BillingSearchPostTransportAuthenticationError):
        transport.verify_billing_search_post_transport(
            *headers,
            body_bytes=BODY,
            keyring=_keyring(),
            trusted_now=trusted_now,
        )


def test_transport_rejects_a_signed_plan_entitlement_substitution():
    headers = _headers(plan_entitlement_sha256=_sha("different-release"))

    with pytest.raises(transport.BillingSearchPostTransportAuthenticationError):
        transport.verify_billing_search_post_transport(
            *headers,
            body_bytes=BODY,
            keyring=_keyring(),
            trusted_now=NOW,
        )


def test_transport_error_never_echoes_sensitive_body():
    context, key_id, signature = _headers()

    with pytest.raises(
        transport.BillingSearchPostTransportAuthenticationError
    ) as error:
        transport.verify_billing_search_post_transport(
            context,
            key_id,
            signature[:-1] + ("A" if signature[-1] != "A" else "B"),
            body_bytes=BODY,
            keyring=_keyring(),
            trusted_now=NOW,
        )

    assert SYNTHETIC_EIN not in str(error.value)
    assert repr(error.value) == (
        "BillingSearchPostTransportAuthenticationError("
        "'billing_search_post_transport_authentication_invalid')"
    )
