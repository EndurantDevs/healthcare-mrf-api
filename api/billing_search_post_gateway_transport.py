# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate gateway context and exact POST bytes without body digests.

Verification binds and returns a metering request ID but never consumes it;
the serving boundary must enforce one-time replay protection in shared state.
"""

from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import re
import struct
from typing import Any

from api.billing_search_access_contract import (
    BILLING_SEARCH_CAPABILITY,
    BILLING_SEARCH_PROVENANCE_CAPABILITY,
    BillingSearchAuthorizationContext,
    build_billing_search_authorization_context,
    validate_billing_search_authorization_context,
)
from api.billing_search_post_transport import (
    BILLING_SEARCH_POST_MAX_BODY_BYTES,
    BILLING_SEARCH_POST_MEDIA_TYPE,
    BILLING_SEARCH_POST_METHOD,
    BILLING_SEARCH_POST_PATH,
)
from api.billing_search_transport_keys import BillingSearchTransportKeyring
from api.plan_release_serving import normalize_plan_release_id

BILLING_SEARCH_POST_TRANSPORT_CONTRACT = "healthporta.billing-search-post-transport.v2"
BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER = "X-HealthPorta-Billing-Search-Context"
BILLING_SEARCH_TRANSPORT_KEY_ID_HEADER = "X-HealthPorta-Billing-Search-Key-Id"
BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER = "X-HealthPorta-Billing-Search-Signature"
BILLING_SEARCH_TRANSPORT_ISSUER = "healthporta-billing-search-gateway"
BILLING_SEARCH_TRANSPORT_AUDIENCE = "healthcare-mrf-api"
BILLING_SEARCH_TRANSPORT_MAX_TTL_SECONDS = 60
BILLING_SEARCH_TRANSPORT_MAX_CONTEXT_BYTES = 2048
BILLING_SEARCH_TRANSPORT_MAX_CONTEXT_CHARACTERS = 3072

_PLAN_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_PLAN_ENTITLEMENT_V1\x00"
_METER_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_POST_METER_RECEIPT_V2\x00"
_SIGNATURE_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_POST_TRANSPORT_V2\x00"
_VERIFIED_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_POST_VERIFIED_V2\x00"
_INVALID = "billing_search_post_transport_authentication_invalid"
_REDACTED = "<redacted-billing-search-post-verified-transport>"
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_KEY_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9-]{0,31}", flags=re.ASCII)
_UUID4_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
    flags=re.ASCII,
)
_UTC_PATTERN = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z",
    flags=re.ASCII,
)
_BASE64URL_PATTERN = re.compile(r"[A-Za-z0-9_-]+", flags=re.ASCII)
_CAPABILITY_SETS = (
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
        "media_type",
        "metering_receipt_sha256",
        "metering_request_id",
        "method",
        "path",
        "plan_entitlement_sha256",
        "plan_release_id",
        "principal_scope_sha256",
        "quota_scope_sha256",
        "request_shape_sha256",
        "tenant_scope_sha256",
    }
)


class BillingSearchPostTransportAuthenticationError(RuntimeError):
    """Value-free failure at the Python gateway trust boundary."""


def _fail() -> BillingSearchPostTransportAuthenticationError:
    return BillingSearchPostTransportAuthenticationError(_INVALID)


def _canonical_sha256(value: object) -> str:
    if (
        type(value) is not str
        or _SHA256_PATTERN.fullmatch(value) is None
        or value == "0" * 64
    ):
        raise _fail()
    return value


def _canonical_uuid4(value: object) -> str:
    if type(value) is not str or _UUID4_PATTERN.fullmatch(value) is None:
        raise _fail()
    return value


def _canonical_utc(value: object) -> tuple[str, datetime]:
    if type(value) is not str or _UTC_PATTERN.fullmatch(value) is None:
        raise _fail()
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        raise _fail() from None
    return value, parsed


def _canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError):
        raise _fail() from None


def _framed_sha256(domain: bytes, value: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(domain)
    digest.update(len(value).to_bytes(8, "big"))
    digest.update(value)
    return digest.hexdigest()


def billing_search_plan_entitlement_sha256(plan_release_id: object) -> str:
    """Return the frozen cross-service release-entitlement digest."""

    if type(plan_release_id) is not str:
        raise _fail()
    normalized = normalize_plan_release_id(plan_release_id)
    if normalized is None or normalized != plan_release_id:
        raise _fail()
    return _framed_sha256(_PLAN_DOMAIN, normalized.encode("ascii"))


def _unique_json_object(
    member_pairs: list[tuple[str, object]],
) -> dict[str, object]:
    value_by_name: dict[str, object] = {}
    for name, value in member_pairs:
        if name in value_by_name:
            raise ValueError
        value_by_name[name] = value
    return value_by_name


def _reject_json_number(_encoded_number: str) -> None:
    raise ValueError


def _decoded_base64url(value: object, *, maximum_characters: int) -> bytes:
    if (
        type(value) is not str
        or not 1 <= len(value) <= maximum_characters
        or _BASE64URL_PATTERN.fullmatch(value) is None
    ):
        raise _fail()
    try:
        decoded = base64.b64decode(
            value + "=" * (-len(value) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (binascii.Error, ValueError):
        raise _fail() from None
    canonical = base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii")
    if not hmac.compare_digest(canonical, value):
        raise _fail()
    return decoded


def _parsed_context(encoded_context: object) -> tuple[dict[str, Any], bytes]:
    context_bytes = _decoded_base64url(
        encoded_context,
        maximum_characters=BILLING_SEARCH_TRANSPORT_MAX_CONTEXT_CHARACTERS,
    )
    if not 1 <= len(context_bytes) <= BILLING_SEARCH_TRANSPORT_MAX_CONTEXT_BYTES:
        raise _fail()
    try:
        context = json.loads(
            context_bytes.decode("ascii"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_number,
            parse_float=_reject_json_number,
            parse_int=_reject_json_number,
        )
    except (UnicodeError, ValueError, json.JSONDecodeError):
        raise _fail() from None
    if type(context) is not dict or frozenset(context) != _CONTEXT_FIELDS:
        raise _fail()
    if not hmac.compare_digest(_canonical_json_bytes(context), context_bytes):
        raise _fail()
    return context, context_bytes


def _signature_message(
    key_id: str,
    context_bytes: bytes,
    body_bytes: bytes,
) -> bytes:
    key_id_bytes = key_id.encode("ascii")
    if len(key_id_bytes) > 0xFFFF:
        raise _fail()
    return b"".join(
        (
            _SIGNATURE_DOMAIN,
            struct.pack(">H", len(key_id_bytes)),
            key_id_bytes,
            struct.pack(">Q", len(context_bytes)),
            context_bytes,
            struct.pack(">Q", len(body_bytes)),
            body_bytes,
        )
    )


def _verified_signature(
    *,
    keyring: BillingSearchTransportKeyring,
    key_id: object,
    encoded_signature: object,
    context_bytes: bytes,
    body_bytes: object,
) -> str:
    try:
        if (
            type(keyring) is not BillingSearchTransportKeyring
            or type(key_id) is not str
            or _KEY_ID_PATTERN.fullmatch(key_id) is None
            or type(body_bytes) is not bytes
            or not 1 <= len(body_bytes) <= BILLING_SEARCH_POST_MAX_BODY_BYTES
        ):
            raise _fail()
        signature = _decoded_base64url(
            encoded_signature,
            maximum_characters=64,
        )
        if len(signature) != hashlib.sha256().digest_size:
            raise _fail()
        expected = hmac.new(
            keyring.key_for(key_id),
            _signature_message(key_id, context_bytes, body_bytes),
            hashlib.sha256,
        ).digest()
        if not hmac.compare_digest(signature, expected):
            raise _fail()
        return key_id
    finally:
        del body_bytes, keyring


def _capabilities(value: object) -> tuple[str, ...]:
    if type(value) is not list or any(type(item) is not str for item in value):
        raise _fail()
    candidate_capabilities = tuple(value)
    if candidate_capabilities not in _CAPABILITY_SETS:
        raise _fail()
    return candidate_capabilities


def _metering_receipt_sha256(context: dict[str, Any]) -> str:
    receipt_by_field = {
        "method": BILLING_SEARCH_POST_METHOD,
        "path": BILLING_SEARCH_POST_PATH,
        "plan_entitlement_sha256": context["plan_entitlement_sha256"],
        "quota_scope_sha256": context["quota_scope_sha256"],
        "request_id": context["metering_request_id"],
        "request_shape_sha256": context["request_shape_sha256"],
    }
    return _framed_sha256(
        _METER_DOMAIN,
        _canonical_json_bytes(receipt_by_field),
    )


def _validated_context(
    context: dict[str, Any],
    *,
    trusted_now: object,
) -> tuple[dict[str, object], str, str]:
    canonical_now, now = _canonical_utc(trusted_now)
    issued_at, issued = _canonical_utc(context.get("issued_at"))
    expires_at, expires = _canonical_utc(context.get("expires_at"))
    plan_release_id = context.get("plan_release_id")
    plan_entitlement = billing_search_plan_entitlement_sha256(plan_release_id)
    capabilities = _capabilities(context.get("capabilities"))
    if (
        context.get("contract") != BILLING_SEARCH_POST_TRANSPORT_CONTRACT
        or context.get("issuer") != BILLING_SEARCH_TRANSPORT_ISSUER
        or context.get("audience") != BILLING_SEARCH_TRANSPORT_AUDIENCE
        or context.get("method") != BILLING_SEARCH_POST_METHOD
        or context.get("path") != BILLING_SEARCH_POST_PATH
        or context.get("media_type") != BILLING_SEARCH_POST_MEDIA_TYPE
        or not 0
        < (expires - issued).total_seconds()
        <= BILLING_SEARCH_TRANSPORT_MAX_TTL_SECONDS
        or now < issued
        or now >= expires
        or not hmac.compare_digest(
            _canonical_sha256(context.get("plan_entitlement_sha256")),
            plan_entitlement,
        )
        or not hmac.compare_digest(
            _canonical_sha256(context.get("metering_receipt_sha256")),
            _metering_receipt_sha256(context),
        )
    ):
        raise _fail()
    claims_by_field = {
        "principal_scope_sha256": _canonical_sha256(
            context.get("principal_scope_sha256")
        ),
        "tenant_scope_sha256": _canonical_sha256(context.get("tenant_scope_sha256")),
        "plan_entitlement_sha256": plan_entitlement,
        "audit_scope_sha256": _canonical_sha256(context.get("audit_scope_sha256")),
        "quota_scope_sha256": _canonical_sha256(context.get("quota_scope_sha256")),
        "capabilities": capabilities,
        "issued_at": issued_at,
        "expires_at": expires_at,
    }
    _canonical_uuid4(context.get("metering_request_id"))
    return claims_by_field, plan_release_id, canonical_now


@dataclass(frozen=True, slots=True, repr=False)
class VerifiedBillingSearchPostTransport:
    """Authenticated plan authority and value-safe request coordinates."""

    authorization_context: BillingSearchAuthorizationContext
    plan_release_id: str
    request_shape_sha256: str
    metering_request_id: str
    trusted_now: str
    verified_state_sha256: str

    def __repr__(self) -> str:
        return _REDACTED


def _verified_state_sha256(
    authorization_context: BillingSearchAuthorizationContext,
    *,
    plan_release_id: str,
    request_shape_sha256: str,
    metering_request_id: str,
    trusted_now: str,
) -> str:
    state_by_field = {
        "authorization_context_sha256": authorization_context.context_sha256,
        "metering_request_id": metering_request_id,
        "plan_release_id": plan_release_id,
        "request_shape_sha256": request_shape_sha256,
        "trusted_now": trusted_now,
    }
    return _framed_sha256(
        _VERIFIED_DOMAIN,
        _canonical_json_bytes(state_by_field),
    )


def validate_billing_search_post_verified_transport(
    transport: object,
    *,
    trusted_now: object,
) -> VerifiedBillingSearchPostTransport:
    """Revalidate every value-safe signed coordinate at the current time."""

    try:
        if type(transport) is not VerifiedBillingSearchPostTransport:
            raise _fail()
        original_trusted_now, _ = _canonical_utc(transport.trusted_now)
        authorization_context = validate_billing_search_authorization_context(
            transport.authorization_context,
            trusted_now=original_trusted_now,
        )
        validate_billing_search_authorization_context(
            authorization_context,
            trusted_now=trusted_now,
        )
        plan_release_id = normalize_plan_release_id(transport.plan_release_id)
        request_shape_sha256 = _canonical_sha256(transport.request_shape_sha256)
        metering_request_id = _canonical_uuid4(transport.metering_request_id)
        if (
            plan_release_id is None
            or plan_release_id != transport.plan_release_id
            or not hmac.compare_digest(
                authorization_context.plan_entitlement_sha256,
                billing_search_plan_entitlement_sha256(plan_release_id),
            )
            or not hmac.compare_digest(
                _canonical_sha256(transport.verified_state_sha256),
                _verified_state_sha256(
                    authorization_context,
                    plan_release_id=plan_release_id,
                    request_shape_sha256=request_shape_sha256,
                    metering_request_id=metering_request_id,
                    trusted_now=original_trusted_now,
                ),
            )
        ):
            raise _fail()
        return transport
    except BillingSearchPostTransportAuthenticationError:
        raise
    except Exception:
        raise _fail() from None


def verify_billing_search_post_transport(
    context_header: object,
    key_id_header: object,
    signature_header: object,
    *,
    body_bytes: object,
    keyring: BillingSearchTransportKeyring,
    trusted_now: object,
) -> VerifiedBillingSearchPostTransport:
    """Verify exact bytes and return the signed ID for a later replay gate."""

    try:
        context, context_bytes = _parsed_context(context_header)
        _verified_signature(
            keyring=keyring,
            key_id=key_id_header,
            encoded_signature=signature_header,
            context_bytes=context_bytes,
            body_bytes=body_bytes,
        )
        claims_by_field, plan_release_id, canonical_now = _validated_context(
            context,
            trusted_now=trusted_now,
        )
        authorization_context = build_billing_search_authorization_context(
            claims_by_field,
            trusted_now=canonical_now,
        )
        request_shape_sha256 = _canonical_sha256(context.get("request_shape_sha256"))
        metering_request_id = _canonical_uuid4(context.get("metering_request_id"))
        return VerifiedBillingSearchPostTransport(
            authorization_context=authorization_context,
            plan_release_id=plan_release_id,
            request_shape_sha256=request_shape_sha256,
            metering_request_id=metering_request_id,
            trusted_now=canonical_now,
            verified_state_sha256=_verified_state_sha256(
                authorization_context,
                plan_release_id=plan_release_id,
                request_shape_sha256=request_shape_sha256,
                metering_request_id=metering_request_id,
                trusted_now=canonical_now,
            ),
        )
    except BillingSearchPostTransportAuthenticationError:
        raise
    except Exception:
        raise _fail() from None
    finally:
        del body_bytes, keyring


__all__ = [
    "BILLING_SEARCH_POST_TRANSPORT_CONTRACT",
    "BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER",
    "BILLING_SEARCH_TRANSPORT_KEY_ID_HEADER",
    "BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER",
    "BillingSearchPostTransportAuthenticationError",
    "VerifiedBillingSearchPostTransport",
    "billing_search_plan_entitlement_sha256",
    "validate_billing_search_post_verified_transport",
    "verify_billing_search_post_transport",
]
