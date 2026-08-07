# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable sensitive request state for billing-identity pricing POSTs."""

from __future__ import annotations

import hmac
from typing import Any, Callable, TypeVar

from api.billing_search_post_request_projection import (
    BillingSearchPostServiceQuery,
    _is_service_query_matching,
    _new_service_query,
)
from api.billing_search_post_request_values import (
    BILLING_SEARCH_POST_DEFAULT_LIMIT,
    BILLING_SEARCH_POST_MAX_CURSOR_CHARACTERS,
    BILLING_SEARCH_POST_MAX_LIMIT,
    BILLING_SEARCH_POST_MAX_RADIUS_MILES,
    BILLING_SEARCH_POST_REQUEST_CONTRACT,
    BillingSearchPostRequestError,
    _bound_request_fingerprint_sha256,
    _normalized_request_fields_or_none,
    _request_shape_sha256,
    request_failure,
)

_REDACTED_REQUEST = "<redacted-billing-search-post-request>"
_REQUEST_VALUE_FIELDS = (
    "healthporta_plan_id",
    "selector_kind",
    "tax_identity_type",
    "tax_identity_value",
    "billing_entity_ref",
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
_Result = TypeVar("_Result")


class _RedactedImmutable:
    __slots__ = ()

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


class BillingSearchPostRequest(_RedactedImmutable):
    """Sensitive selector state retained only for entitlement-gated resolution."""

    __slots__ = tuple(f"__{field_name}" for field_name in _REQUEST_VALUE_FIELDS) + (
        "__request_shape_sha256",
        "__service_query",
    )

    def __init__(self, *_args, **_fields_by_name: Any) -> None:
        del self, _args, _fields_by_name
        raise request_failure()

    def __repr__(self) -> str:
        return _REDACTED_REQUEST

    __str__ = __repr__

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
    def billing_entity_ref(self) -> str | None:
        """Return the response-visible opaque selector for the central resolver."""

        return self.__billing_entity_ref

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
        """Return a value-safe shape digest that omits selector values and cursor."""

        return self.__request_shape_sha256

    @property
    def service_query(self) -> BillingSearchPostServiceQuery:
        """Return the immutable service view with no raw selector or opaque ref."""

        return self.__service_query


def _new_request(fields_by_name: dict[str, Any]) -> BillingSearchPostRequest:
    request = object.__new__(BillingSearchPostRequest)
    for field_name in _REQUEST_VALUE_FIELDS:
        object.__setattr__(
            request,
            f"_BillingSearchPostRequest__{field_name}",
            fields_by_name[field_name],
        )
    shape_sha256 = _request_shape_sha256(fields_by_name)
    object.__setattr__(
        request,
        "_BillingSearchPostRequest__request_shape_sha256",
        shape_sha256,
    )
    object.__setattr__(
        request,
        "_BillingSearchPostRequest__service_query",
        _new_service_query(fields_by_name, shape_sha256),
    )
    return request


def _request_payload(request: BillingSearchPostRequest) -> dict[str, Any]:
    if request.selector_kind == "tax_identity":
        billing_identity_by_field = {
            "tax_identity": {
                "type": request.tax_identity_type,
                "value": object.__getattribute__(
                    request,
                    "_BillingSearchPostRequest__tax_identity_value",
                ),
            }
        }
    else:
        billing_identity_by_field = {"billing_entity_ref": request.billing_entity_ref}
    payload_by_field: dict[str, Any] = {
        "healthporta_plan_id": request.healthporta_plan_id,
        "billing_identity": billing_identity_by_field,
        "procedure": {
            "code_system": request.code_system,
            "code": request.code,
            "modifiers": list(request.modifiers),
            "place_of_service": list(request.place_of_service),
        },
        "geo": {"zip5": request.zip5, "radius_miles": request.radius_miles},
        "page": {"limit": request.limit, "cursor": request.cursor},
    }
    if request.provider_npi is not None:
        payload_by_field["provider_npi"] = str(request.provider_npi)
    if request.include_evidence:
        payload_by_field["include_evidence"] = True
    return payload_by_field


def _validated_request_or_none(request: object) -> BillingSearchPostRequest | None:
    try:
        if type(request) is not BillingSearchPostRequest:
            return None
        normalized_fields = _normalized_request_fields_or_none(
            _request_payload(request)
        )
        if normalized_fields is None:
            return None
        for field_name in _REQUEST_VALUE_FIELDS:
            actual = object.__getattribute__(
                request,
                f"_BillingSearchPostRequest__{field_name}",
            )
            expected = normalized_fields[field_name]
            if type(actual) is not type(expected) or actual != expected:
                return None
        expected_shape = _request_shape_sha256(normalized_fields)
        supplied_shape = request.request_shape_sha256
        if type(supplied_shape) is not str or not hmac.compare_digest(
            supplied_shape,
            expected_shape,
        ):
            return None
        service_query = request.service_query
        if not _is_service_query_matching(
            service_query,
            normalized_fields,
            supplied_shape,
        ):
            return None
        return request
    except Exception:
        return None


def validate_billing_search_post_request(
    request: object,
) -> BillingSearchPostRequest:
    """Revalidate the canonical request before selector resolution or serving."""

    validated = _validated_request_or_none(request)
    del request
    if validated is None:
        raise request_failure()
    return validated


def parse_billing_search_post_request(payload: object) -> BillingSearchPostRequest:
    """Parse one decoded closed JSON object without exposing its sensitive value."""

    fields_by_name = _normalized_request_fields_or_none(payload)
    del payload
    if fields_by_name is None:
        raise request_failure()
    candidate = _new_request(fields_by_name)
    validated = _validated_request_or_none(candidate)
    del candidate, fields_by_name
    if validated is None:
        raise request_failure()
    return validated


def _applied_tax_identity_or_failure(
    request: BillingSearchPostRequest,
    transformer: Callable[[str, str], _Result],
) -> tuple[bool, _Result | None]:
    try:
        tin_type = request.tax_identity_type
        if request.selector_kind != "tax_identity" or tin_type is None:
            return False, None
        sensitive_value = object.__getattribute__(
            request,
            "_BillingSearchPostRequest__tax_identity_value",
        )
        return True, transformer(tin_type, sensitive_value)
    except Exception:
        return False, None


def apply_entitled_billing_search_tax_identity(
    request: object,
    transformer: Callable[[str, str], _Result],
) -> _Result:
    """Apply a trusted entitlement-gated transformer to the transient typed value.

    This function performs no entitlement check and provides no token resolver.
    Its caller must complete those gates first; transformer failures are redacted.
    """

    validated = _validated_request_or_none(request)
    del request
    if validated is None:
        del transformer
        raise request_failure()
    if not callable(transformer):
        del validated, transformer
        raise request_failure()
    succeeded, result = _applied_tax_identity_or_failure(validated, transformer)
    del validated, transformer
    if not succeeded:
        raise request_failure()
    return result


def bind_billing_search_post_request_fingerprint(
    request: object,
    *,
    selector_scope_sha256: object,
) -> str:
    """Bind the request shape to an entitlement-derived pseudonymous selector.

    The caller must supply a server-derived scope from an authenticated ``be1_``
    reference or a policy HMAC. This function never derives it from raw input.
    """

    validated = _validated_request_or_none(request)
    del request
    if validated is None:
        raise request_failure()
    request_shape_sha256 = validated.request_shape_sha256
    del validated
    return _bound_request_fingerprint_sha256(
        request_shape_sha256=request_shape_sha256,
        selector_scope_sha256=selector_scope_sha256,
    )


__all__ = [
    "BILLING_SEARCH_POST_DEFAULT_LIMIT",
    "BILLING_SEARCH_POST_MAX_CURSOR_CHARACTERS",
    "BILLING_SEARCH_POST_MAX_LIMIT",
    "BILLING_SEARCH_POST_MAX_RADIUS_MILES",
    "BILLING_SEARCH_POST_REQUEST_CONTRACT",
    "BillingSearchPostRequest",
    "BillingSearchPostRequestError",
    "BillingSearchPostServiceQuery",
    "apply_entitled_billing_search_tax_identity",
    "bind_billing_search_post_request_fingerprint",
    "parse_billing_search_post_request",
    "validate_billing_search_post_request",
]
