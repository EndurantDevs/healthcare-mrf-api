# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict byte transport for the billing-identity procedure-search POST.

An external gateway must authenticate the bounded request bytes before calling
this module. This parser intentionally creates no raw-body or selector digest.
"""

from __future__ import annotations

import json
import math

from api.billing_search_post_request import (
    BillingSearchPostRequest,
    parse_billing_search_post_request,
)

BILLING_SEARCH_POST_METHOD = "POST"
BILLING_SEARCH_POST_PATH = "/api/v1/pricing/providers/search-by-procedure"
BILLING_SEARCH_POST_MEDIA_TYPE = "application/json"
BILLING_SEARCH_POST_MAX_BODY_BYTES = 16_384

_INVALID = "billing_search_post_transport_invalid"


class BillingSearchPostTransportError(ValueError):
    """One value-free transport rejection safe for the public boundary."""


def _transport_failure() -> BillingSearchPostTransportError:
    return BillingSearchPostTransportError(_INVALID)


def _unique_json_object(
    member_pairs: list[tuple[str, object]],
) -> dict[str, object]:
    value_by_name: dict[str, object] = {}
    for member_name, member_value in member_pairs:
        if member_name in value_by_name:
            raise ValueError
        value_by_name[member_name] = member_value
    return value_by_name


def _reject_json_constant(_constant: str) -> None:
    raise ValueError


def _finite_json_float(encoded_number: str) -> float:
    decoded_number = float(encoded_number)
    if not math.isfinite(decoded_number):
        raise ValueError
    return decoded_number


def _parsed_request_or_none(body: object) -> BillingSearchPostRequest | None:
    try:
        if (
            type(body) is not bytes
            or not 1 <= len(body) <= BILLING_SEARCH_POST_MAX_BODY_BYTES
        ):
            return None
        payload = json.loads(
            body.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
            parse_float=_finite_json_float,
        )
        return parse_billing_search_post_request(payload)
    except Exception:
        return None


def parse_billing_search_post_transport(
    body: object,
    *,
    method: object,
    path: object,
    media_type: object,
) -> BillingSearchPostRequest:
    """Parse authenticated bytes after exact route/media normalization upstream.

    ``media_type`` is the normalized media type, not a raw Content-Type header.
    Gateway signature/HMAC verification and selector resolution are deliberately
    outside this transport foundation.
    """

    is_exact_transport = (
        type(method) is str
        and method == BILLING_SEARCH_POST_METHOD
        and type(path) is str
        and path == BILLING_SEARCH_POST_PATH
        and type(media_type) is str
        and media_type == BILLING_SEARCH_POST_MEDIA_TYPE
    )
    parsed = _parsed_request_or_none(body) if is_exact_transport else None
    del body
    if parsed is None:
        raise _transport_failure()
    return parsed


__all__ = [
    "BILLING_SEARCH_POST_MAX_BODY_BYTES",
    "BILLING_SEARCH_POST_MEDIA_TYPE",
    "BILLING_SEARCH_POST_METHOD",
    "BILLING_SEARCH_POST_PATH",
    "BillingSearchPostTransportError",
    "parse_billing_search_post_transport",
]
