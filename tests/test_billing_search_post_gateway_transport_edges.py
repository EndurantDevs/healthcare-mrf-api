# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import base64
import hashlib
import json

import pytest

from api import billing_search_post_gateway_transport as gateway_transport
from api import billing_search_post_transport as post_transport
from api import billing_search_transport_keys as transport_keys

PLAN_RELEASE_ID = "hprelease_" + "0" * 26
KEY_ID = "synthetic-edge"
KEY = bytes(range(32))
NOW = "2026-08-07T10:00:10Z"


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _body() -> bytes:
    return json.dumps(
        {
            "billing_identity": {
                "tax_identity": {"type": "ein", "value": "12-3333333"}
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


def _context() -> dict[str, object]:
    body = _body()
    request = post_transport.parse_billing_search_post_transport(
        body,
        method=post_transport.BILLING_SEARCH_POST_METHOD,
        path=post_transport.BILLING_SEARCH_POST_PATH,
        media_type=post_transport.BILLING_SEARCH_POST_MEDIA_TYPE,
    )
    context_by_field: dict[str, object] = {
        "audience": gateway_transport.BILLING_SEARCH_TRANSPORT_AUDIENCE,
        "audit_scope_sha256": _sha("audit-edge"),
        "capabilities": ["pricing:billing-search"],
        "contract": gateway_transport.BILLING_SEARCH_POST_TRANSPORT_CONTRACT,
        "expires_at": "2026-08-07T10:01:00Z",
        "issued_at": "2026-08-07T10:00:00Z",
        "issuer": gateway_transport.BILLING_SEARCH_TRANSPORT_ISSUER,
        "media_type": post_transport.BILLING_SEARCH_POST_MEDIA_TYPE,
        "metering_receipt_sha256": "0" * 64,
        "metering_request_id": "00000000-0000-4000-8000-000000000000",
        "method": post_transport.BILLING_SEARCH_POST_METHOD,
        "path": post_transport.BILLING_SEARCH_POST_PATH,
        "plan_entitlement_sha256": (
            gateway_transport.billing_search_plan_entitlement_sha256(PLAN_RELEASE_ID)
        ),
        "plan_release_id": PLAN_RELEASE_ID,
        "principal_scope_sha256": _sha("principal-edge"),
        "quota_scope_sha256": _sha("quota-edge"),
        "request_shape_sha256": request.request_shape_sha256,
        "tenant_scope_sha256": _sha("tenant-edge"),
    }
    context_by_field["metering_receipt_sha256"] = (
        gateway_transport._metering_receipt_sha256(context_by_field)
    )
    return context_by_field


def _keyring() -> transport_keys.BillingSearchTransportKeyring:
    return transport_keys.BillingSearchTransportKeyring(
        active_key_id=KEY_ID,
        keys_by_id={KEY_ID: KEY},
    )


@pytest.mark.parametrize(
    ("function", "argument"),
    [
        (gateway_transport._canonical_sha256, None),
        (gateway_transport._canonical_uuid4, "not-a-uuid"),
        (gateway_transport._canonical_utc, None),
        (gateway_transport._canonical_json_bytes, object()),
        (gateway_transport.billing_search_plan_entitlement_sha256, None),
        (
            gateway_transport.billing_search_plan_entitlement_sha256,
            "not-a-plan-release",
        ),
    ],
)
def test_gateway_canonical_validators_fail_closed(function, argument: object) -> None:
    with pytest.raises(gateway_transport.BillingSearchPostTransportAuthenticationError):
        function(argument)


def test_gateway_rejects_an_impossible_calendar_timestamp() -> None:
    with pytest.raises(gateway_transport.BillingSearchPostTransportAuthenticationError):
        gateway_transport._canonical_utc("2026-02-31T10:00:00Z")


@pytest.mark.parametrize(
    "context_bytes",
    [
        b'{"duplicate":"a","duplicate":"b"}',
        b"1",
        b"\xff",
        b"{}",
    ],
)
def test_gateway_rejects_noncanonical_context_documents(
    context_bytes: bytes,
) -> None:
    with pytest.raises(gateway_transport.BillingSearchPostTransportAuthenticationError):
        gateway_transport._parsed_context(_base64url(context_bytes))


@pytest.mark.parametrize("encoded", ["=", "A", "AB"])
def test_gateway_rejects_invalid_or_noncanonical_base64url(encoded: str) -> None:
    with pytest.raises(gateway_transport.BillingSearchPostTransportAuthenticationError):
        gateway_transport._decoded_base64url(encoded, maximum_characters=64)


def test_gateway_rejects_decoded_context_over_its_byte_limit() -> None:
    encoded = _base64url(
        b"x" * (gateway_transport.BILLING_SEARCH_TRANSPORT_MAX_CONTEXT_BYTES + 1)
    )
    with pytest.raises(gateway_transport.BillingSearchPostTransportAuthenticationError):
        gateway_transport._parsed_context(encoded)


def test_gateway_rejects_valid_but_noncanonical_json_encoding() -> None:
    noncanonical = json.dumps(
        _context(),
        ensure_ascii=True,
        sort_keys=False,
    ).encode("ascii")
    with pytest.raises(gateway_transport.BillingSearchPostTransportAuthenticationError):
        gateway_transport._parsed_context(_base64url(noncanonical))


def test_gateway_signature_and_capability_edges_fail_closed() -> None:
    with pytest.raises(gateway_transport.BillingSearchPostTransportAuthenticationError):
        gateway_transport._signature_message("x" * 65_536, b"", b"")

    with pytest.raises(gateway_transport.BillingSearchPostTransportAuthenticationError):
        gateway_transport._verified_signature(
            keyring=object(),
            key_id=KEY_ID,
            encoded_signature=_base64url(b"x" * 32),
            context_bytes=b"{}",
            body_bytes=b"x",
        )
    with pytest.raises(gateway_transport.BillingSearchPostTransportAuthenticationError):
        gateway_transport._verified_signature(
            keyring=_keyring(),
            key_id=KEY_ID,
            encoded_signature=_base64url(b"x"),
            context_bytes=b"{}",
            body_bytes=b"x",
        )

    for capabilities in ("pricing:billing-search", ["unsupported"]):
        with pytest.raises(
            gateway_transport.BillingSearchPostTransportAuthenticationError
        ):
            gateway_transport._capabilities(capabilities)


def test_gateway_verified_transport_rejects_wrong_runtime_type() -> None:
    with pytest.raises(gateway_transport.BillingSearchPostTransportAuthenticationError):
        gateway_transport.validate_billing_search_post_verified_transport(
            object(),
            trusted_now=NOW,
        )


def test_gateway_wraps_unexpected_verification_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _unexpected_failure(_encoded_context: object) -> object:
        raise RuntimeError

    monkeypatch.setattr(
        gateway_transport,
        "_parsed_context",
        _unexpected_failure,
    )
    with pytest.raises(
        gateway_transport.BillingSearchPostTransportAuthenticationError
    ) as captured:
        gateway_transport.verify_billing_search_post_transport(
            "synthetic-context",
            KEY_ID,
            "synthetic-signature",
            body_bytes=b"{}",
            keyring=_keyring(),
            trusted_now=NOW,
        )
    assert captured.value.__cause__ is None
