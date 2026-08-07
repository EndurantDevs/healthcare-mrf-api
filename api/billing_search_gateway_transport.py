# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify the signed gateway-to-healthcare billing-search transport.

One context is intentionally reusable by the gateway's bounded upstream retry;
it is not a public bearer credential or a single-use receipt. Confidentiality
and replay containment therefore require TLS and a restricted service network,
in addition to this signature and its 60-second validity ceiling.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
from typing import Any

from api.billing_search_access_contract import (
    BILLING_SEARCH_CAPABILITY,
    BILLING_SEARCH_PROVENANCE_CAPABILITY,
    BillingSearchAuthorizationContext,
    build_billing_search_authorization_context,
)
from api.billing_search_transport_contract import (
    BILLING_SEARCH_TRANSPORT_AUDIENCE,
    BILLING_SEARCH_TRANSPORT_CONTRACT,
    BILLING_SEARCH_TRANSPORT_ISSUER,
    BILLING_SEARCH_TRANSPORT_MAX_CONTEXT_BYTES,
    BILLING_SEARCH_TRANSPORT_MAX_CONTEXT_CHARACTERS,
    BILLING_SEARCH_TRANSPORT_MAX_TTL_SECONDS,
    BillingSearchTransportError,
    BillingSearchTransportRequestBinding,
    _canonical_json_bytes,
    _canonical_plan_release_id,
    _canonical_sha256,
    _canonical_utc,
    _canonical_uuid4,
    _fail,
    billing_search_metering_receipt_sha256,
)
from api.billing_search_transport_keys import (
    BillingSearchTransportKeyring,
    BillingSearchTransportKeyringError,
)
from api.billing_search_verified_transport import (
    VerifiedBillingSearchTransport,
    _new_verified_transport,
    validate_verified_billing_search_transport,
)

_SIGNATURE_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_TRANSPORT_V1\x00"
_REDACTED = "<redacted-billing-search-transport>"
_INVALID_JSON = object()
_SIGNATURE_BYTES = 32
_SHA256_FIELDS = (
    "audit_scope_sha256",
    "metering_receipt_sha256",
    "plan_entitlement_sha256",
    "principal_scope_sha256",
    "query_sha256",
    "quota_scope_sha256",
    "tenant_scope_sha256",
)
_KEY_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9-]{0,31}", flags=re.ASCII)
_BASE64URL_PATTERN = re.compile(r"[A-Za-z0-9_-]+", flags=re.ASCII)
_CAPABILITY_TUPLES = (
    (BILLING_SEARCH_CAPABILITY,),
    (BILLING_SEARCH_CAPABILITY, BILLING_SEARCH_PROVENANCE_CAPABILITY),
)
_CONTEXT_FIELDS = frozenset(
    {
        "audience",
        "audit_scope_sha256",
        "capabilities",
        "contract",
        "expires_at",
        "issued_at",
        "issuer",
        "metering_receipt_sha256",
        "metering_request_id",
        "method",
        "path",
        "plan_entitlement_sha256",
        "plan_release_id",
        "principal_scope_sha256",
        "query_sha256",
        "quota_scope_sha256",
        "tenant_scope_sha256",
    }
)


def _canonical_key_id(value: object) -> str:
    if type(value) is not str or _KEY_ID_PATTERN.fullmatch(value) is None:
        raise _fail()
    return value


def _unique_json_object(
    member_pairs: list[tuple[str, object]],
) -> dict[str, object]:
    json_object_by_name: dict[str, object] = {}
    for member_name, member_value in member_pairs:
        if member_name in json_object_by_name:
            raise ValueError
        json_object_by_name[member_name] = member_value
    return json_object_by_name


def _reject_json_number(_encoded_number: str) -> None:
    raise ValueError


def _parse_json_bytes(encoded_json: bytes) -> object:
    try:
        return json.loads(
            encoded_json.decode("ascii"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_number,
            parse_float=_reject_json_number,
            parse_int=_reject_json_number,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return _INVALID_JSON


def _base64url_encode(encoded_bytes: bytes) -> str:
    return base64.urlsafe_b64encode(encoded_bytes).rstrip(b"=").decode("ascii")


def _base64url_decode(encoded_text: object) -> bytes:
    if (
        type(encoded_text) is not str
        or not encoded_text
        or _BASE64URL_PATTERN.fullmatch(encoded_text) is None
    ):
        raise _fail()
    try:
        decoded_bytes = base64.b64decode(
            encoded_text + "=" * (-len(encoded_text) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (binascii.Error, ValueError):
        decoded_bytes = b""
    if not decoded_bytes or not hmac.compare_digest(
        _base64url_encode(decoded_bytes),
        encoded_text,
    ):
        raise _fail()
    return decoded_bytes


def _signature_message(key_id: str, context_bytes: bytes) -> bytes:
    encoded_key_id = key_id.encode("ascii")
    return b"".join(
        (
            _SIGNATURE_DOMAIN,
            len(encoded_key_id).to_bytes(2, "big"),
            encoded_key_id,
            len(context_bytes).to_bytes(8, "big"),
            context_bytes,
        )
    )


def _signature_bytes(
    keyring: BillingSearchTransportKeyring,
    key_id: str,
    context_bytes: bytes,
) -> bytes:
    return hmac.new(
        keyring.key_for(key_id),
        _signature_message(key_id, context_bytes),
        hashlib.sha256,
    ).digest()


def _expected_signature(
    keyring: BillingSearchTransportKeyring,
    key_id: str,
    context_bytes: bytes,
) -> bytes | None:
    try:
        return _signature_bytes(keyring, key_id, context_bytes)
    except BillingSearchTransportKeyringError:
        return None


def _signature_from_header(signature_header: object) -> bytes:
    signature_bytes = _base64url_decode(signature_header)
    if len(signature_bytes) != _SIGNATURE_BYTES:
        raise _fail()
    return signature_bytes


def _context_bytes_from_header(context_header: object) -> bytes:
    if (
        type(context_header) is not str
        or not 1
        <= len(context_header)
        <= BILLING_SEARCH_TRANSPORT_MAX_CONTEXT_CHARACTERS
    ):
        raise _fail()
    context_bytes = _base64url_decode(context_header)
    if not 1 <= len(context_bytes) <= BILLING_SEARCH_TRANSPORT_MAX_CONTEXT_BYTES:
        raise _fail()
    return context_bytes


def _canonical_capabilities(value: object) -> tuple[str, ...]:
    if type(value) is not list:
        raise _fail()
    capabilities = tuple(value)
    if capabilities not in _CAPABILITY_TUPLES:
        raise _fail()
    return capabilities


def _validated_time_fields(
    raw_context: dict[str, Any],
    binding: BillingSearchTransportRequestBinding,
) -> tuple[str, str]:
    issued_at, issued_time = _canonical_utc(raw_context.get("issued_at"))
    expires_at, expires_time = _canonical_utc(raw_context.get("expires_at"))
    _, trusted_time = _canonical_utc(binding.trusted_now)
    validity_seconds = (expires_time - issued_time).total_seconds()
    if (
        not 0 < validity_seconds <= BILLING_SEARCH_TRANSPORT_MAX_TTL_SECONDS
        or trusted_time < issued_time
        or trusted_time >= expires_time
    ):
        raise _fail()
    return issued_at, expires_at


def _base_context_fields(
    raw_context: dict[str, Any],
    binding: BillingSearchTransportRequestBinding,
) -> dict[str, Any]:
    """Normalize every closed field before exact binding checks."""

    issued_at, expires_at = _validated_time_fields(raw_context, binding)
    normalized_fields_by_name = {
        field_name: _canonical_sha256(raw_context.get(field_name))
        for field_name in _SHA256_FIELDS
    }
    normalized_fields_by_name.update(
        {
            "audience": raw_context.get("audience"),
            "capabilities": _canonical_capabilities(raw_context.get("capabilities")),
            "contract": raw_context.get("contract"),
            "expires_at": expires_at,
            "issued_at": issued_at,
            "issuer": raw_context.get("issuer"),
            "metering_request_id": _canonical_uuid4(
                raw_context.get("metering_request_id")
            ),
            "method": raw_context.get("method"),
            "path": raw_context.get("path"),
            "plan_release_id": _canonical_plan_release_id(
                raw_context.get("plan_release_id")
            ),
        }
    )
    return normalized_fields_by_name


def _require_request_binding(
    context_fields_by_name: dict[str, Any],
    binding: BillingSearchTransportRequestBinding,
) -> None:
    expected_pairs = (
        (context_fields_by_name["audience"], BILLING_SEARCH_TRANSPORT_AUDIENCE),
        (context_fields_by_name["contract"], BILLING_SEARCH_TRANSPORT_CONTRACT),
        (context_fields_by_name["issuer"], BILLING_SEARCH_TRANSPORT_ISSUER),
        (context_fields_by_name["method"], binding.method),
        (context_fields_by_name["path"], binding.path),
        (context_fields_by_name["query_sha256"], binding.query_sha256),
        (context_fields_by_name["plan_release_id"], binding.plan_release_id),
        (
            context_fields_by_name["plan_entitlement_sha256"],
            binding.plan_entitlement_sha256,
        ),
    )
    if any(
        type(actual) is not str or not hmac.compare_digest(actual, expected)
        for actual, expected in expected_pairs
    ):
        raise _fail()


def _require_metering_receipt(
    context_fields_by_name: dict[str, Any],
) -> None:
    expected_receipt = billing_search_metering_receipt_sha256(
        method=context_fields_by_name["method"],
        path=context_fields_by_name["path"],
        plan_entitlement_sha256=context_fields_by_name["plan_entitlement_sha256"],
        query_sha256=context_fields_by_name["query_sha256"],
        quota_scope_sha256=context_fields_by_name["quota_scope_sha256"],
        request_id=context_fields_by_name["metering_request_id"],
    )
    if not hmac.compare_digest(
        context_fields_by_name["metering_receipt_sha256"],
        expected_receipt,
    ):
        raise _fail()


def _normalized_context_fields(
    raw_context: object,
    binding: BillingSearchTransportRequestBinding,
) -> dict[str, Any]:
    """Validate the closed schema, request binding, and metering receipt."""

    if type(raw_context) is not dict or frozenset(raw_context) != _CONTEXT_FIELDS:
        raise _fail()
    context_fields_by_name = _base_context_fields(raw_context, binding)
    _require_request_binding(context_fields_by_name, binding)
    _require_metering_receipt(context_fields_by_name)
    return context_fields_by_name


def _authorization_context(
    context_fields_by_name: dict[str, Any],
    trusted_now: str,
) -> BillingSearchAuthorizationContext:
    authorization_claims_by_name = {
        "principal_scope_sha256": context_fields_by_name["principal_scope_sha256"],
        "tenant_scope_sha256": context_fields_by_name["tenant_scope_sha256"],
        "plan_entitlement_sha256": context_fields_by_name["plan_entitlement_sha256"],
        "audit_scope_sha256": context_fields_by_name["audit_scope_sha256"],
        "quota_scope_sha256": context_fields_by_name["quota_scope_sha256"],
        "capabilities": context_fields_by_name["capabilities"],
        "issued_at": context_fields_by_name["issued_at"],
        "expires_at": context_fields_by_name["expires_at"],
    }
    return build_billing_search_authorization_context(
        authorization_claims_by_name,
        trusted_now=trusted_now,
    )


def _verified_authorization_context(
    context_fields_by_name: dict[str, Any],
    trusted_now: str,
) -> BillingSearchAuthorizationContext | None:
    try:
        return _authorization_context(context_fields_by_name, trusted_now)
    except Exception:
        return None


def verify_billing_search_transport(
    context_header: object,
    key_id_header: object,
    signature_header: object,
    *,
    keyring: BillingSearchTransportKeyring,
    binding: BillingSearchTransportRequestBinding,
) -> VerifiedBillingSearchTransport:
    """Authenticate one exact, short-lived, already-metered gateway request."""

    try:
        if type(keyring) is not BillingSearchTransportKeyring:
            raise _fail()
        if type(binding) is not BillingSearchTransportRequestBinding:
            raise _fail()
        key_id = _canonical_key_id(key_id_header)
        context_bytes = _context_bytes_from_header(context_header)
        supplied_signature = _signature_from_header(signature_header)
        expected_signature = _expected_signature(keyring, key_id, context_bytes)
        if expected_signature is None or not hmac.compare_digest(
            supplied_signature,
            expected_signature,
        ):
            raise _fail()
        raw_context = _parse_json_bytes(context_bytes)
        if raw_context is _INVALID_JSON or not hmac.compare_digest(
            _canonical_json_bytes(raw_context),
            context_bytes,
        ):
            raise _fail()
        context_fields_by_name = _normalized_context_fields(raw_context, binding)
        authorization_context = _verified_authorization_context(
            context_fields_by_name,
            binding.trusted_now,
        )
        if authorization_context is None:
            raise _fail()
        return _new_verified_transport(
            context_fields_by_name,
            authorization_context,
            context_bytes=context_bytes,
            trusted_now=binding.trusted_now,
        )
    except BillingSearchTransportError:
        raise
    except Exception:
        raise _fail() from None
