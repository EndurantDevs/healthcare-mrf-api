# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict immutable request state for billing-identity pricing search."""

from __future__ import annotations

import hmac
from typing import Any

from api.billing_search_request_values import (
    BILLING_SEARCH_MAX_LIMIT,
    BILLING_SEARCH_MAX_RADIUS_MILES,
    BillingSearchRequestError,
    normalized_billing_search_request_fields,
    request_failure,
)
from api.billing_search_transport_contract import (
    _canonical_json_bytes,
    _framed_sha256,
)

_REQUEST_STATE_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_REQUEST_STATE_V1\x00"
_REDACTED = "<redacted-billing-search-request>"


class BillingSearchRequest:
    """Immutable redacted exact billing-search request."""

    __slots__ = (
        "__billing_entity_ref",
        "__code",
        "__code_system",
        "__cursor",
        "__include_evidence",
        "__latitude",
        "__limit",
        "__longitude",
        "__modifiers",
        "__place_of_service",
        "__plan_release_id",
        "__provider_npi",
        "__query_pairs",
        "__radius_miles",
        "__request_fingerprint_sha256",
        "__state_sha256",
        "__zip5",
    )

    def __init__(self, *constructor_args, **constructor_fields_by_name: Any) -> None:
        del constructor_args, constructor_fields_by_name
        raise request_failure()

    def __setattr__(self, attribute_name: str, attribute_value: object) -> None:
        del attribute_name, attribute_value
        raise TypeError("billing_search_request_invalid")

    def __delattr__(self, attribute_name: str) -> None:
        del attribute_name
        raise TypeError("billing_search_request_invalid")

    def __repr__(self) -> str:
        return _REDACTED

    __str__ = __repr__

    def __copy__(self) -> BillingSearchRequest:
        return self

    def __deepcopy__(self, memo: dict[int, object]) -> BillingSearchRequest:
        memo[id(self)] = self
        return self

    def __reduce_ex__(self, protocol: int) -> object:
        del protocol
        raise request_failure()

    billing_entity_ref = property(lambda self: self.__billing_entity_ref)
    plan_release_id = property(lambda self: self.__plan_release_id)
    code_system = property(lambda self: self.__code_system)
    code = property(lambda self: self.__code)
    zip5 = property(lambda self: self.__zip5)
    latitude = property(lambda self: self.__latitude)
    longitude = property(lambda self: self.__longitude)
    radius_miles = property(lambda self: self.__radius_miles)
    provider_npi = property(lambda self: self.__provider_npi)
    modifiers = property(lambda self: self.__modifiers)
    place_of_service = property(lambda self: self.__place_of_service)
    include_evidence = property(lambda self: self.__include_evidence)
    limit = property(lambda self: self.__limit)
    cursor = property(lambda self: self.__cursor)
    query_pairs = property(lambda self: self.__query_pairs)
    request_fingerprint_sha256 = property(
        lambda self: self.__request_fingerprint_sha256
    )

    @property
    def geo_args(self) -> dict[str, Any]:
        """Return the validated provider-address GEO selector."""

        if self.__zip5 is not None:
            return {"zip5": self.__zip5}
        return {
            "lat": self.__latitude,
            "long": self.__longitude,
            "radius_miles": self.__radius_miles,
        }

    @property
    def price_filter_args(self) -> dict[str, Any]:
        """Return exact optional modifier and place-of-service filters."""

        return {
            "modifiers": self.__modifiers,
            "place_of_service": self.__place_of_service,
        }


def _request_state_sha256(request_fields_by_name: dict[str, Any]) -> str:
    return _framed_sha256(
        _REQUEST_STATE_DOMAIN,
        _canonical_json_bytes(request_fields_by_name),
    )


def _new_billing_search_request(
    request_fields_by_name: dict[str, Any],
) -> BillingSearchRequest:
    parsed_request = object.__new__(BillingSearchRequest)
    for field_name, field_value in request_fields_by_name.items():
        object.__setattr__(
            parsed_request,
            f"_BillingSearchRequest__{field_name}",
            field_value,
        )
    object.__setattr__(
        parsed_request,
        "_BillingSearchRequest__state_sha256",
        _request_state_sha256(request_fields_by_name),
    )
    return parsed_request


def _validated_request_or_none(request: object) -> BillingSearchRequest | None:
    try:
        if type(request) is not BillingSearchRequest:
            return None
        request_fields_by_name = normalized_billing_search_request_fields(
            dict(request.query_pairs)
        )
        if request_fields_by_name is None:
            return None
        for field_name, expected_value in request_fields_by_name.items():
            actual_value = getattr(request, field_name)
            if (
                type(actual_value) is not type(expected_value)
                or actual_value != expected_value
            ):
                return None
        supplied_state_sha256 = request._BillingSearchRequest__state_sha256
        if type(supplied_state_sha256) is not str or not hmac.compare_digest(
            supplied_state_sha256,
            _request_state_sha256(request_fields_by_name),
        ):
            return None
        return request
    except Exception:
        return None


def validate_billing_search_request(request: object) -> BillingSearchRequest:
    """Revalidate every field and relation before runtime or database use."""

    validated_request = _validated_request_or_none(request)
    if validated_request is None:
        raise request_failure()
    return validated_request


def parse_billing_search_request(
    parameters: Any,
) -> BillingSearchRequest:
    """Parse one closed gateway-normalized query without retaining aliases."""

    request_fields_by_name = normalized_billing_search_request_fields(parameters)
    if request_fields_by_name is None:
        raise request_failure()
    return validate_billing_search_request(
        _new_billing_search_request(request_fields_by_name)
    )


__all__ = [
    "BILLING_SEARCH_MAX_LIMIT",
    "BILLING_SEARCH_MAX_RADIUS_MILES",
    "BillingSearchRequest",
    "BillingSearchRequestError",
    "parse_billing_search_request",
    "validate_billing_search_request",
]
