# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable non-sensitive projection of a billing-search POST request."""

from __future__ import annotations

import hmac
from typing import Any, Mapping

from api.billing_search_post_request_values import request_failure

_REDACTED_QUERY = "<redacted-billing-search-post-service-query>"
_SERVICE_QUERY_FIELDS = (
    "healthporta_plan_id",
    "selector_kind",
    "tax_identity_type",
    "code_system",
    "code",
    "modifiers",
    "place_of_service",
    "zip5",
    "radius_miles",
    "provider_npi",
    "include_evidence",
    "limit",
    "cursor",
)


class BillingSearchPostServiceQuery:
    """Redacted immutable service view containing no selector payload."""

    __slots__ = tuple(f"__{field_name}" for field_name in _SERVICE_QUERY_FIELDS) + (
        "__request_shape_sha256",
    )

    def __init__(self, *_args, **_fields_by_name: Any) -> None:
        del self, _args, _fields_by_name
        raise request_failure()

    def __repr__(self) -> str:
        return _REDACTED_QUERY

    __str__ = __repr__

    def __setattr__(self, name: str, value: object) -> None:
        del self, name, value
        raise TypeError("billing_search_post_request_invalid")

    def __delattr__(self, name: str) -> None:
        del self, name
        raise TypeError("billing_search_post_request_invalid")

    def __copy__(self):
        return self

    def __deepcopy__(self, memo: dict[int, object]):
        memo[id(self)] = self
        return self

    def __reduce_ex__(self, protocol: int) -> object:
        del self, protocol
        raise request_failure()

    @property
    def healthporta_plan_id(self) -> str:
        """Return the entitled public plan identifier supplied by the client."""
        return self.__healthporta_plan_id

    @property
    def selector_kind(self) -> str:
        """Return which one-selector union arm was supplied."""
        return self.__selector_kind

    @property
    def tax_identity_type(self) -> str | None:
        """Return the typed-identity kind without returning its value."""
        return self.__tax_identity_type

    @property
    def code_system(self) -> str:
        """Return the canonical procedure code system."""
        return self.__code_system

    @property
    def code(self) -> str:
        """Return the canonical exact procedure code."""
        return self.__code

    @property
    def modifiers(self) -> tuple[str, ...]:
        """Return sorted unique exact procedure modifiers."""
        return self.__modifiers

    @property
    def place_of_service(self) -> tuple[str, ...]:
        """Return sorted unique exact place-of-service filters."""
        return self.__place_of_service

    @property
    def zip5(self) -> str:
        """Return the required five-digit geographic anchor."""
        return self.__zip5

    @property
    def radius_miles(self) -> float:
        """Return the validated radius in miles."""
        return self.__radius_miles

    @property
    def provider_npi(self) -> int | None:
        """Return the optional exact same-group provider filter."""
        return self.__provider_npi

    @property
    def include_evidence(self) -> bool:
        """Return whether stronger-capability provenance was requested."""
        return self.__include_evidence

    @property
    def limit(self) -> int:
        """Return the bounded keyset page size."""
        return self.__limit

    @property
    def cursor(self) -> str | None:
        """Return the optional sealed continuation cursor."""
        return self.__cursor

    @property
    def request_shape_sha256(self) -> str:
        """Return the selector-value-safe normalized request-shape digest."""
        return self.__request_shape_sha256

    @property
    def procedure_args(self) -> dict[str, object]:
        """Return exact code filters in the serving reader's vocabulary."""
        return {
            "code_system": self.__code_system,
            "code": self.__code,
            "modifiers": self.__modifiers,
            "place_of_service": self.__place_of_service,
        }

    @property
    def geo_args(self) -> dict[str, object]:
        """Return the exact-ZIP anchor and explicitly bounded radius."""
        return {"zip5": self.__zip5, "radius_miles": self.__radius_miles}

    @property
    def page_args(self) -> dict[str, object]:
        """Return bounded keyset-page inputs; no offset field can be produced."""
        return {"limit": self.__limit, "cursor": self.__cursor}


def _new_service_query(
    fields_by_name: Mapping[str, Any],
    request_shape_sha256: str,
) -> BillingSearchPostServiceQuery:
    query = object.__new__(BillingSearchPostServiceQuery)
    for field_name in _SERVICE_QUERY_FIELDS:
        object.__setattr__(
            query,
            f"_BillingSearchPostServiceQuery__{field_name}",
            fields_by_name[field_name],
        )
    object.__setattr__(
        query,
        "_BillingSearchPostServiceQuery__request_shape_sha256",
        request_shape_sha256,
    )
    return query


def _is_service_query_matching(
    query: object,
    fields_by_name: Mapping[str, Any],
    request_shape_sha256: str,
) -> bool:
    if type(query) is not BillingSearchPostServiceQuery:
        return False
    for field_name in _SERVICE_QUERY_FIELDS:
        actual = object.__getattribute__(
            query,
            f"_BillingSearchPostServiceQuery__{field_name}",
        )
        expected = fields_by_name[field_name]
        if type(actual) is not type(expected) or actual != expected:
            return False
    query_shape_sha256 = query.request_shape_sha256
    return type(query_shape_sha256) is str and hmac.compare_digest(
        query_shape_sha256,
        request_shape_sha256,
    )


__all__ = ["BillingSearchPostServiceQuery"]
