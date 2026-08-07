# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed gateway-to-healthcare billing-search transport tests."""

from __future__ import annotations

import hashlib
import json

import pytest

from api import billing_search_gateway_transport as transport
from api import billing_search_transport_contract as contract
from api.billing_search_transport_keys import BillingSearchTransportKeyring
from tests.test_billing_search_transport_contract import (
    BILLING_ENTITY_REF,
    METERING_RECEIPT_SHA256,
    PLAN_ENTITLEMENT_SHA256,
    PLAN_RELEASE_ID,
    QUERY_PAIRS,
    QUERY_SHA256,
    QUOTA_SHA256,
    REQUEST_ID,
)

KEY_ID = "k2031-a"
KEY_BYTES = bytes(range(32))
GOLDEN_SIGNATURE = "kvgD1xm6pDnE5egIYoJBxgHhC33C55simNWc2GJGbyw"
PRINCIPAL_SHA256 = hashlib.sha256(b"synthetic-principal-scope").hexdigest()
TENANT_SHA256 = hashlib.sha256(b"synthetic-tenant-scope").hexdigest()
AUDIT_SHA256 = hashlib.sha256(b"synthetic-audit-scope").hexdigest()
GOLDEN_PAYLOAD_BYTES = (
    b'{"audience":"healthcare-mrf-api","audit_scope_sha256":"'
    + AUDIT_SHA256.encode("ascii")
    + b'","capabilities":["pricing:billing-search"],"contract":"healthporta.billing-search-transport.v1","expires_at":"2031-01-02T03:04:55Z","issued_at":"2031-01-02T03:03:55Z","issuer":"healthporta-billing-search-gateway","metering_receipt_sha256":"'
    + METERING_RECEIPT_SHA256.encode("ascii")
    + b'","metering_request_id":"123e4567-e89b-42d3-a456-426614174000","method":"GET","path":"/api/v1/pricing/providers/search-by-procedure","plan_entitlement_sha256":"'
    + PLAN_ENTITLEMENT_SHA256.encode("ascii")
    + b'","plan_release_id":"hprelease_01K123456789ABCDEFGHJKMNPQ","principal_scope_sha256":"'
    + PRINCIPAL_SHA256.encode("ascii")
    + b'","query_sha256":"'
    + QUERY_SHA256.encode("ascii")
    + b'","quota_scope_sha256":"'
    + QUOTA_SHA256.encode("ascii")
    + b'","tenant_scope_sha256":"'
    + TENANT_SHA256.encode("ascii")
    + b'"}'
)


def _keyring(*, include_old=True):
    key_material_by_id = {KEY_ID: KEY_BYTES}
    if include_old:
        key_material_by_id["old-v1"] = b"o" * 32
    return BillingSearchTransportKeyring(
        active_key_id=KEY_ID,
        keys_by_id=key_material_by_id,
    )


def _binding(**overrides):
    binding_fields_by_name = {
        "method": "GET",
        "path": contract.BILLING_SEARCH_TRANSPORT_PATH,
        "query_pairs": QUERY_PAIRS,
        "plan_release_id": PLAN_RELEASE_ID,
        "trusted_now": "2031-01-02T03:04:05Z",
    }
    binding_fields_by_name.update(overrides)
    return contract.BillingSearchTransportRequestBinding(**binding_fields_by_name)


def _payload(**overrides):
    payload_by_field = json.loads(GOLDEN_PAYLOAD_BYTES.decode("ascii"))
    payload_by_field.update(overrides)
    return payload_by_field


def _headers_for_bytes(
    context_bytes,
    *,
    key_id=KEY_ID,
    keyring=None,
):
    signing_keyring = keyring or _keyring()
    return (
        transport._base64url_encode(context_bytes),
        key_id,
        transport._base64url_encode(
            transport._signature_bytes(signing_keyring, key_id, context_bytes)
        ),
    )


def _headers(payload=None, **header_overrides):
    context_bytes = contract._canonical_json_bytes(payload or _payload())
    headers = list(_headers_for_bytes(context_bytes))
    header_index_by_name = {"context": 0, "key_id": 1, "signature": 2}
    for header_name, header_value in header_overrides.items():
        headers[header_index_by_name[header_name]] = header_value
    return tuple(headers)


def _verify(headers=None, *, binding=None, keyring=None):
    return transport.verify_billing_search_transport(
        *(headers or _headers()),
        keyring=keyring or _keyring(),
        binding=binding or _binding(),
    )


def test_cross_language_signature_and_payload_golden_vector() -> None:
    assert len(GOLDEN_PAYLOAD_BYTES) == 1058
    assert contract._canonical_json_bytes(_payload()) == GOLDEN_PAYLOAD_BYTES
    context_header, key_id, signature_header = _headers_for_bytes(GOLDEN_PAYLOAD_BYTES)

    assert key_id == KEY_ID
    assert signature_header == GOLDEN_SIGNATURE
    assert "=" not in context_header
    assert "=" not in signature_header


def test_transport_round_trip_authenticates_closed_access_context() -> None:
    verified = _verify()

    assert verified.metering_request_id == REQUEST_ID
    assert verified.metering_receipt_sha256 == METERING_RECEIPT_SHA256
    assert verified.query_sha256 == QUERY_SHA256
    assert verified.issued_at == "2031-01-02T03:03:55Z"
    assert verified.expires_at == "2031-01-02T03:04:55Z"
    assert verified.authorization_context.capabilities == ("pricing:billing-search",)
    assert (
        verified.authorization_context.plan_entitlement_sha256
        == PLAN_ENTITLEMENT_SHA256
    )
    assert repr(verified) == "<redacted-billing-search-transport>"
    assert BILLING_ENTITY_REF not in repr(verified)
    assert (
        transport.validate_verified_billing_search_transport(
            verified,
            trusted_now="2031-01-02T03:04:05Z",
        )
        is verified
    )


def test_verified_transport_rejects_direct_construction_and_field_tampering() -> None:
    with pytest.raises(
        contract.BillingSearchTransportError,
        match="^billing_search_transport_invalid$",
    ):
        transport.VerifiedBillingSearchTransport()

    tampering_by_field = {
        "authorization_context": object(),
        "metering_request_id": "223e4567-e89b-42d3-a456-426614174000",
        "metering_receipt_sha256": "1" * 64,
        "query_sha256": "1" * 64,
        "transport_context_sha256": "1" * 64,
        "issued_at": "2031-01-02T03:03:54Z",
        "expires_at": "2031-01-02T03:04:54Z",
        "verified_state_sha256": "1" * 64,
    }
    for field_name, field_value in tampering_by_field.items():
        verified = _verify()
        object.__setattr__(verified, field_name, field_value)
        with pytest.raises(
            contract.BillingSearchTransportError,
            match="^billing_search_transport_invalid$",
        ):
            transport.validate_verified_billing_search_transport(
                verified,
                trusted_now="2031-01-02T03:04:05Z",
            )


def test_verified_transport_revalidation_enforces_expiry() -> None:
    verified = _verify()

    with pytest.raises(contract.BillingSearchTransportError):
        transport.validate_verified_billing_search_transport(
            verified,
            trusted_now=verified.expires_at,
        )


@pytest.mark.parametrize(
    "payload_overrides",
    [
        {"audience": "different-service"},
        {"issuer": "different-gateway"},
        {"contract": "different-contract"},
        {"method": "POST"},
        {"path": "/api/v1/pricing/providers/by-service"},
        {"plan_release_id": "hprelease_01K123456789ABCDEFGHJKMNP0"},
        {"plan_release_id": f" {PLAN_RELEASE_ID}"},
        {"plan_release_id": f"{PLAN_RELEASE_ID}\n"},
        {"plan_entitlement_sha256": "1" * 64},
        {"query_sha256": "1" * 64},
        {"metering_receipt_sha256": "1" * 64},
        {"metering_request_id": "123e4567-e89b-12d3-a456-426614174000"},
        {"quota_scope_sha256": "0" * 64},
        {"capabilities": []},
        {"capabilities": ["pricing:billing-search:provenance"]},
        {"capabilities": ["pricing:billing-search", "pricing:anything"]},
    ],
)
def test_transport_rejects_signed_but_inconsistent_claims(
    payload_overrides,
) -> None:
    with pytest.raises(
        contract.BillingSearchTransportError,
        match="^billing_search_transport_invalid$",
    ):
        _verify(_headers(_payload(**payload_overrides)))


@pytest.mark.parametrize(
    "time_overrides",
    [
        {"issued_at": "2031-01-02T03:04:06Z"},
        {"expires_at": "2031-01-02T03:04:05Z"},
        {
            "issued_at": "2031-01-02T03:03:54Z",
            "expires_at": "2031-01-02T03:04:55Z",
        },
        {"expires_at": "2031-02-30T03:04:55Z"},
        {"expires_at": "2031-01-02T03:04:55+00:00"},
        {"issued_at": 1_925_086_635},
    ],
)
def test_transport_rejects_noncurrent_or_noncanonical_time(time_overrides) -> None:
    with pytest.raises(contract.BillingSearchTransportError):
        _verify(_headers(_payload(**time_overrides)))


def test_transport_accepts_exact_provenance_capability_pair() -> None:
    verified = _verify(
        _headers(
            _payload(
                capabilities=[
                    "pricing:billing-search",
                    "pricing:billing-search:provenance",
                ]
            )
        )
    )

    assert verified.authorization_context.capabilities == (
        "pricing:billing-search",
        "pricing:billing-search:provenance",
    )


def test_transport_rejects_every_request_binding_mismatch() -> None:
    wrong_bindings = (
        _binding(query_pairs=QUERY_PAIRS[:-1] + (("zip5", "25702"),)),
        _binding(plan_release_id="hprelease_01K123456789ABCDEFGHJKMNP0"),
    )

    for wrong_binding in wrong_bindings:
        with pytest.raises(contract.BillingSearchTransportError):
            _verify(binding=wrong_binding)


def test_transport_rejects_tampered_or_unknown_signatures() -> None:
    context_header, key_id, signature_header = _headers()
    replacement = "A" if signature_header[-1] != "A" else "B"

    for headers in (
        (context_header, key_id, signature_header[:-1] + replacement),
        (context_header, "unknown", signature_header),
        (context_header + "=", key_id, signature_header),
        (context_header, key_id, signature_header + "="),
        (context_header, key_id, "short"),
    ):
        with pytest.raises(contract.BillingSearchTransportError):
            _verify(headers)


def test_transport_rejects_wrong_runtime_dependency_types() -> None:
    with pytest.raises(contract.BillingSearchTransportError):
        _verify(keyring=object())

    with pytest.raises(contract.BillingSearchTransportError):
        _verify(binding=object())


def test_transport_rejects_noncanonical_key_id_type() -> None:
    context_header, _key_id, signature_header = _headers()

    with pytest.raises(contract.BillingSearchTransportError):
        _verify((context_header, object(), signature_header))


def test_verified_transport_revalidation_rejects_wrong_runtime_type() -> None:
    with pytest.raises(contract.BillingSearchTransportError):
        transport.validate_verified_billing_search_transport(
            object(),
            trusted_now="2031-01-02T03:04:05Z",
        )


def test_transport_reads_retained_rotation_key() -> None:
    keyring = _keyring()
    old_payload = contract._canonical_json_bytes(_payload())
    old_headers = _headers_for_bytes(
        old_payload,
        key_id="old-v1",
        keyring=keyring,
    )

    assert _verify(old_headers, keyring=keyring).query_sha256 == QUERY_SHA256
    with pytest.raises(contract.BillingSearchTransportError):
        _verify(old_headers, keyring=_keyring(include_old=False))


def test_transport_rejects_authenticated_duplicate_and_noncanonical_json() -> None:
    duplicate_context = GOLDEN_PAYLOAD_BYTES.replace(
        b'{"audience":"healthcare-mrf-api",',
        b'{"audience":"healthcare-mrf-api","audience":"healthcare-mrf-api",',
        1,
    )
    spaced_context = GOLDEN_PAYLOAD_BYTES.replace(
        b'","audit_scope_sha256"',
        b'", "audit_scope_sha256"',
        1,
    )

    for context_bytes in (duplicate_context, spaced_context):
        with pytest.raises(contract.BillingSearchTransportError) as failure:
            _verify(_headers_for_bytes(context_bytes))
        assert failure.value.__cause__ is None
        assert failure.value.__context__ is None


def test_transport_rejects_unknown_field_without_leaking_its_value() -> None:
    sensitive_sentinel = "SENSITIVE-OPAQUE-SELECTOR-MUST-NOT-ESCAPE"
    headers = _headers(_payload(extra_selector=sensitive_sentinel))

    with pytest.raises(contract.BillingSearchTransportError) as failure:
        _verify(headers)

    assert str(failure.value) == "billing_search_transport_invalid"
    assert sensitive_sentinel not in repr(failure.value)
    assert failure.value.__cause__ is None
    assert failure.value.__context__ is None
