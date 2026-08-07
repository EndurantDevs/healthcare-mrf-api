# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared canonical request and digest contract for billing-search transport."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import re

BILLING_SEARCH_TRANSPORT_CONTRACT = "healthporta.billing-search-transport.v1"
BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER = "X-HealthPorta-Billing-Search-Context"
BILLING_SEARCH_TRANSPORT_KEY_ID_HEADER = "X-HealthPorta-Billing-Search-Key-Id"
BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER = "X-HealthPorta-Billing-Search-Signature"
BILLING_SEARCH_TRANSPORT_PATH = "/api/v1/pricing/providers/search-by-procedure"
BILLING_SEARCH_TRANSPORT_ISSUER = "healthporta-billing-search-gateway"
BILLING_SEARCH_TRANSPORT_AUDIENCE = "healthcare-mrf-api"
BILLING_SEARCH_TRANSPORT_MAX_TTL_SECONDS = 60
BILLING_SEARCH_TRANSPORT_MAX_QUERY_BYTES = 8192
BILLING_SEARCH_TRANSPORT_MAX_QUERY_PAIRS = 32
BILLING_SEARCH_TRANSPORT_MAX_CONTEXT_BYTES = 2048
BILLING_SEARCH_TRANSPORT_MAX_CONTEXT_CHARACTERS = 3072

_QUERY_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_QUERY_V1\x00"
_PLAN_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_PLAN_ENTITLEMENT_V1\x00"
_METER_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_METER_RECEIPT_V1\x00"
_INVALID = "billing_search_transport_invalid"
_REDACTED = "<redacted-billing-search-transport>"
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_UTC_PATTERN = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z",
    flags=re.ASCII,
)
_UUID4_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
    flags=re.ASCII,
)


class BillingSearchTransportError(RuntimeError):
    """Value-free transport verification failure."""


def _fail() -> BillingSearchTransportError:
    return BillingSearchTransportError(_INVALID)


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
        parsed_time = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        parsed_time = None
    if parsed_time is None:
        raise _fail()
    return value, parsed_time


def _canonical_json_bytes(json_object: object) -> bytes:
    return json.dumps(
        json_object,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _framed_sha256(domain: bytes, encoded_value: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(domain)
    digest.update(len(encoded_value).to_bytes(8, "big"))
    digest.update(encoded_value)
    return digest.hexdigest()


def _canonical_query_member(value: object, *, field_name: str) -> str:
    if type(value) is not str or not value or not value.isascii():
        raise _fail()
    if any(not character.isprintable() for character in value):
        raise _fail()
    character_limit = 64 if field_name == "key" else 2048
    if len(value) > character_limit:
        raise _fail()
    return value


def normalize_billing_search_query_pairs(
    query_pairs: object,
) -> tuple[tuple[str, str], ...]:
    """Return sorted unique ASCII query pairs or fail closed."""

    if (
        type(query_pairs) is not tuple
        or not 1 <= len(query_pairs) <= BILLING_SEARCH_TRANSPORT_MAX_QUERY_PAIRS
    ):
        raise _fail()
    normalized_pairs: list[tuple[str, str]] = []
    for query_pair in query_pairs:
        if type(query_pair) is not tuple or len(query_pair) != 2:
            raise _fail()
        normalized_pairs.append(
            (
                _canonical_query_member(query_pair[0], field_name="key"),
                _canonical_query_member(query_pair[1], field_name="value"),
            )
        )
    if len({query_key for query_key, _value in normalized_pairs}) != len(
        normalized_pairs
    ):
        raise _fail()
    return tuple(sorted(normalized_pairs))


def billing_search_query_sha256(query_pairs: object) -> str:
    """Digest the complete normalized internal query as canonical pairs."""

    normalized_pairs = normalize_billing_search_query_pairs(query_pairs)
    encoded_pairs = _canonical_json_bytes(normalized_pairs)
    if len(encoded_pairs) > BILLING_SEARCH_TRANSPORT_MAX_QUERY_BYTES:
        raise _fail()
    return _framed_sha256(_QUERY_DOMAIN, encoded_pairs)


def _canonical_plan_release_id(plan_release_id: object) -> str:
    from api.plan_release_serving import normalize_plan_release_id

    if type(plan_release_id) is not str:
        raise _fail()
    normalized_release_id = normalize_plan_release_id(plan_release_id)
    if normalized_release_id is None or normalized_release_id != plan_release_id:
        raise _fail()
    return normalized_release_id


def billing_search_plan_entitlement_sha256(plan_release_id: object) -> str:
    """Derive the exact release entitlement digest shared by both services."""

    canonical_release_id = _canonical_plan_release_id(plan_release_id)
    return _framed_sha256(_PLAN_DOMAIN, canonical_release_id.encode("ascii"))


def billing_search_metering_receipt_sha256(
    *,
    method: object,
    path: object,
    plan_entitlement_sha256: object,
    query_sha256: object,
    quota_scope_sha256: object,
    request_id: object,
) -> str:
    """Digest the exact metered request coordinates attested by the gateway."""

    if type(method) is not str or method != "GET":
        raise _fail()
    if type(path) is not str or path != BILLING_SEARCH_TRANSPORT_PATH:
        raise _fail()
    receipt_fields_by_name = {
        "method": method,
        "path": path,
        "plan_entitlement_sha256": _canonical_sha256(plan_entitlement_sha256),
        "query_sha256": _canonical_sha256(query_sha256),
        "quota_scope_sha256": _canonical_sha256(quota_scope_sha256),
        "request_id": _canonical_uuid4(request_id),
    }
    return _framed_sha256(
        _METER_DOMAIN,
        _canonical_json_bytes(receipt_fields_by_name),
    )


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchTransportRequestBinding:
    """Redacted exact downstream request coordinates verified by healthcare."""

    method: str
    path: str
    query_pairs: tuple[tuple[str, str], ...]
    plan_release_id: str
    trusted_now: str

    def __post_init__(self) -> None:
        if (
            type(self.method) is not str
            or self.method != "GET"
            or type(self.path) is not str
            or self.path != BILLING_SEARCH_TRANSPORT_PATH
        ):
            raise _fail()
        _canonical_plan_release_id(self.plan_release_id)
        normalize_billing_search_query_pairs(self.query_pairs)
        _canonical_utc(self.trusted_now)

    @property
    def query_sha256(self) -> str:
        """Return the digest of the complete normalized internal query."""

        return billing_search_query_sha256(self.query_pairs)

    @property
    def plan_entitlement_sha256(self) -> str:
        """Return the exact canonical release entitlement digest."""

        return billing_search_plan_entitlement_sha256(self.plan_release_id)

    def __repr__(self) -> str:
        return _REDACTED

    __str__ = __repr__
