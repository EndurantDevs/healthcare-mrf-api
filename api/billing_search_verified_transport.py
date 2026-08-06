# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Integrity-checked state returned by billing-search transport verification."""

from __future__ import annotations

from dataclasses import dataclass
import hmac
from typing import Any

from api.billing_search_access_contract import (
    BillingSearchAuthorizationContext,
    validate_billing_search_authorization_context,
)
from api.billing_search_transport_contract import (
    BILLING_SEARCH_TRANSPORT_PATH,
    _canonical_json_bytes,
    _canonical_sha256,
    _canonical_utc,
    _canonical_uuid4,
    _fail,
    _framed_sha256,
    billing_search_metering_receipt_sha256,
)

_CONTEXT_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_TRANSPORT_CONTEXT_V1\x00"
_VERIFIED_STATE_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_VERIFIED_STATE_V1\x00"
_REDACTED = "<redacted-billing-search-transport>"


@dataclass(frozen=True, slots=True, repr=False, init=False)
class VerifiedBillingSearchTransport:
    """Verified gateway receipt plus its non-authenticating access context."""

    authorization_context: BillingSearchAuthorizationContext
    metering_request_id: str
    metering_receipt_sha256: str
    query_sha256: str
    transport_context_sha256: str
    issued_at: str
    expires_at: str
    verified_state_sha256: str

    def __init__(self, *constructor_args, **constructor_fields_by_name) -> None:
        del constructor_args, constructor_fields_by_name
        raise _fail()

    def __repr__(self) -> str:
        return _REDACTED

    __str__ = __repr__


def _verified_state_fields(
    verified: VerifiedBillingSearchTransport,
    *,
    trusted_now: str,
) -> dict[str, Any]:
    """Rebuild every state field and its independently checkable relations."""

    authorization_context = validate_billing_search_authorization_context(
        verified.authorization_context,
        trusted_now=trusted_now,
    )
    state_fields_by_name = {
        "authorization_context_sha256": _canonical_sha256(
            authorization_context.context_sha256
        ),
        "expires_at": _canonical_utc(verified.expires_at)[0],
        "issued_at": _canonical_utc(verified.issued_at)[0],
        "metering_receipt_sha256": _canonical_sha256(verified.metering_receipt_sha256),
        "metering_request_id": _canonical_uuid4(verified.metering_request_id),
        "query_sha256": _canonical_sha256(verified.query_sha256),
        "transport_context_sha256": _canonical_sha256(
            verified.transport_context_sha256
        ),
    }
    if (
        state_fields_by_name["issued_at"] != authorization_context.issued_at
        or state_fields_by_name["expires_at"] != authorization_context.expires_at
    ):
        raise _fail()
    expected_receipt = billing_search_metering_receipt_sha256(
        method="GET",
        path=BILLING_SEARCH_TRANSPORT_PATH,
        plan_entitlement_sha256=authorization_context.plan_entitlement_sha256,
        query_sha256=state_fields_by_name["query_sha256"],
        quota_scope_sha256=authorization_context.quota_scope_sha256,
        request_id=state_fields_by_name["metering_request_id"],
    )
    if not hmac.compare_digest(
        state_fields_by_name["metering_receipt_sha256"],
        expected_receipt,
    ):
        raise _fail()
    return state_fields_by_name


def _verified_state_digest(state_fields_by_name: dict[str, Any]) -> str:
    return _framed_sha256(
        _VERIFIED_STATE_DOMAIN,
        _canonical_json_bytes(state_fields_by_name),
    )


def _validated_verified_transport(
    verified: object,
    *,
    trusted_now: str,
) -> VerifiedBillingSearchTransport | None:
    try:
        if type(verified) is not VerifiedBillingSearchTransport:
            return None
        state_fields_by_name = _verified_state_fields(
            verified,
            trusted_now=trusted_now,
        )
        if not hmac.compare_digest(
            _canonical_sha256(verified.verified_state_sha256),
            _verified_state_digest(state_fields_by_name),
        ):
            return None
        return verified
    except Exception:
        return None


def validate_verified_billing_search_transport(
    verified: object,
    *,
    trusted_now: str,
) -> VerifiedBillingSearchTransport:
    """Revalidate every returned field before downstream use."""

    validated = _validated_verified_transport(verified, trusted_now=trusted_now)
    if validated is None:
        raise _fail()
    return validated


def _new_verified_transport(
    context_fields_by_name: dict[str, Any],
    authorization_context: BillingSearchAuthorizationContext,
    *,
    context_bytes: bytes,
    trusted_now: str,
) -> VerifiedBillingSearchTransport:
    verified = object.__new__(VerifiedBillingSearchTransport)
    initial_fields_by_name = {
        "authorization_context": authorization_context,
        "metering_request_id": context_fields_by_name["metering_request_id"],
        "metering_receipt_sha256": context_fields_by_name["metering_receipt_sha256"],
        "query_sha256": context_fields_by_name["query_sha256"],
        "transport_context_sha256": _framed_sha256(
            _CONTEXT_DOMAIN,
            context_bytes,
        ),
        "issued_at": context_fields_by_name["issued_at"],
        "expires_at": context_fields_by_name["expires_at"],
    }
    for field_name, field_value in initial_fields_by_name.items():
        object.__setattr__(verified, field_name, field_value)
    object.__setattr__(
        verified,
        "verified_state_sha256",
        _verified_state_digest(
            _verified_state_fields(verified, trusted_now=trusted_now)
        ),
    )
    return validate_verified_billing_search_transport(
        verified,
        trusted_now=trusted_now,
    )
