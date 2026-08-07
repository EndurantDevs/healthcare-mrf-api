# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed HTTP trust boundary for authenticated billing-search GET requests."""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import hmac
import json
import os
import secrets
from typing import Any

from api.billing_search_access_contract import (
    BillingSearchAuthorizationContext,
    require_billing_search_access,
)
from api.billing_search_gateway_transport import verify_billing_search_transport
from api.billing_search_request import (
    BillingSearchRequest,
    parse_billing_search_request,
    validate_billing_search_request,
)
from api.billing_search_transport_contract import (
    BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER,
    BILLING_SEARCH_TRANSPORT_KEY_ID_HEADER,
    BILLING_SEARCH_TRANSPORT_PATH,
    BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER,
    BillingSearchTransportRequestBinding,
    _canonical_utc,
)
from api.billing_search_transport_keys import (
    BillingSearchTransportKeyring,
    load_billing_search_transport_keyring,
)
from api.billing_search_verified_transport import (
    VerifiedBillingSearchTransport,
    validate_verified_billing_search_transport,
)

_STATE_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_ENDPOINT_ACCESS_V1\x00"
_STATE_AUTH_KEY = secrets.token_bytes(32)
_HEADER_PREFIX = "x-healthporta-billing-search-"
_HEADER_NAMES = (
    BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER,
    BILLING_SEARCH_TRANSPORT_KEY_ID_HEADER,
    BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER,
)
_ALLOWED_HEADER_NAMES = frozenset(name.lower() for name in _HEADER_NAMES)
_INVALID = "billing_search_endpoint_access_invalid"
_REDACTED = "<redacted-billing-search-endpoint-access>"


class BillingSearchEndpointAccessError(RuntimeError):
    """Value-free failure at the healthcare HTTP trust boundary."""


def _fail() -> BillingSearchEndpointAccessError:
    return BillingSearchEndpointAccessError(_INVALID)


def _mapping_items(mapping: Mapping[str, Any]) -> list[tuple[Any, Any]]:
    items = getattr(mapping, "items", None)
    if not callable(items):
        raise _fail()
    try:
        return list(items(multi=True))
    except TypeError:
        try:
            return list(items())
        except Exception:
            raise _fail() from None
    except Exception:
        raise _fail() from None


def _accessor_values(
    headers: Mapping[str, Any],
    header_name: str,
) -> list[Any] | None:
    for accessor_name in ("getall", "getlist"):
        accessor = getattr(headers, accessor_name, None)
        if not callable(accessor):
            continue
        try:
            return list(accessor(header_name))
        except (KeyError, TypeError):
            continue
        except Exception:
            raise _fail() from None
    return None


def _closed_header_values(headers: Mapping[str, Any]) -> tuple[str, str, str]:
    if not isinstance(headers, Mapping):
        raise _fail()
    header_items = _mapping_items(headers)
    for candidate_name, _candidate_value in header_items:
        if not isinstance(candidate_name, str):
            raise _fail()
        lowered_name = candidate_name.lower()
        if (
            lowered_name.startswith(_HEADER_PREFIX)
            and lowered_name not in _ALLOWED_HEADER_NAMES
        ):
            raise _fail()

    normalized_values: list[str] = []
    for header_name in _HEADER_NAMES:
        matching_values = [
            candidate_value
            for candidate_name, candidate_value in header_items
            if isinstance(candidate_name, str)
            and candidate_name.lower() == header_name.lower()
        ]
        accessor_values = _accessor_values(headers, header_name)
        if accessor_values is not None:
            if len(accessor_values) != 1 or (
                matching_values and matching_values != accessor_values
            ):
                raise _fail()
            if not matching_values:
                matching_values = accessor_values
        if len(matching_values) != 1:
            raise _fail()
        header_value = matching_values[0]
        if (
            type(header_value) is not str
            or not header_value
            or header_value != header_value.strip()
            or not header_value.isascii()
            or not header_value.isprintable()
        ):
            raise _fail()
        normalized_values.append(header_value)
    return normalized_values[0], normalized_values[1], normalized_values[2]


def _request_keyring(
    keyring: BillingSearchTransportKeyring | None,
    environment_map: Mapping[str, str] | None,
) -> BillingSearchTransportKeyring:
    try:
        if keyring is None:
            return load_billing_search_transport_keyring(
                os.environ if environment_map is None else environment_map
            )
        if environment_map is not None:
            raise _fail()
        return keyring
    finally:
        del environment_map, keyring


def _state_hmac(
    request: BillingSearchRequest,
    verified_transport: VerifiedBillingSearchTransport,
) -> bytes:
    encoded_state = json.dumps(
        {
            "authorization_context_sha256": (
                verified_transport.authorization_context.context_sha256
            ),
            "metering_receipt_sha256": (verified_transport.metering_receipt_sha256),
            "metering_request_id": verified_transport.metering_request_id,
            "query_sha256": verified_transport.query_sha256,
            "request_fingerprint_sha256": request.request_fingerprint_sha256,
            "transport_context_sha256": (verified_transport.transport_context_sha256),
            "verified_state_sha256": verified_transport.verified_state_sha256,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    state_message = b"".join(
        (
            _STATE_DOMAIN,
            id(request).to_bytes(16, "big"),
            id(verified_transport).to_bytes(16, "big"),
            len(encoded_state).to_bytes(8, "big"),
            encoded_state,
        )
    )
    return hmac.new(_STATE_AUTH_KEY, state_message, hashlib.sha256).digest()


def _validated_components(
    request: object,
    verified_transport: object,
    *,
    trusted_now: object,
) -> tuple[BillingSearchRequest, VerifiedBillingSearchTransport]:
    canonical_now = _canonical_utc(trusted_now)[0]
    validated_request = validate_billing_search_request(request)
    validated_transport = validate_verified_billing_search_transport(
        verified_transport,
        trusted_now=canonical_now,
    )
    expected_binding = BillingSearchTransportRequestBinding(
        method="GET",
        path=BILLING_SEARCH_TRANSPORT_PATH,
        query_pairs=validated_request.query_pairs,
        plan_release_id=validated_request.plan_release_id,
        trusted_now=canonical_now,
    )
    if not hmac.compare_digest(
        validated_transport.query_sha256,
        expected_binding.query_sha256,
    ):
        raise _fail()
    require_billing_search_access(
        validated_transport.authorization_context,
        requested_plan_entitlement_sha256=(expected_binding.plan_entitlement_sha256),
        detailed_provenance=validated_request.include_evidence,
        trusted_now=canonical_now,
    )
    return validated_request, validated_transport


class BillingSearchEndpointAccess:
    """Factory-created request plus authenticated gateway authority."""

    __slots__ = ("__request", "__state_hmac", "__verified_transport")

    def __init__(self, *_args, **_kwargs) -> None:
        del self, _args, _kwargs
        raise _fail()

    @property
    def request(self) -> BillingSearchRequest:
        """Return the validated internal GET request."""

        return self.__request

    @property
    def verified_transport(self) -> VerifiedBillingSearchTransport:
        """Return the authenticated gateway transport receipt."""

        return self.__verified_transport

    @property
    def authorization_context(self) -> BillingSearchAuthorizationContext:
        """Return the already authenticated authorization context."""

        return self.__verified_transport.authorization_context

    def __repr__(self) -> str:
        return _REDACTED

    __str__ = __repr__

    def __setattr__(self, attribute_name: str, attribute_value: object) -> None:
        del self, attribute_name, attribute_value
        raise TypeError(_INVALID)

    def __delattr__(self, attribute_name: str) -> None:
        del self, attribute_name
        raise TypeError(_INVALID)

    def __copy__(self) -> BillingSearchEndpointAccess:
        return self

    def __deepcopy__(
        self,
        memo: dict[int, object],
    ) -> BillingSearchEndpointAccess:
        memo[id(self)] = self
        return self

    def __reduce_ex__(self, protocol: int) -> object:
        del self, protocol
        raise _fail()


def _new_access(
    request: BillingSearchRequest,
    verified_transport: VerifiedBillingSearchTransport,
) -> BillingSearchEndpointAccess:
    access = object.__new__(BillingSearchEndpointAccess)
    object.__setattr__(
        access,
        "_BillingSearchEndpointAccess__request",
        request,
    )
    object.__setattr__(
        access,
        "_BillingSearchEndpointAccess__verified_transport",
        verified_transport,
    )
    object.__setattr__(
        access,
        "_BillingSearchEndpointAccess__state_hmac",
        _state_hmac(request, verified_transport),
    )
    return access


def _validated_access_or_none(
    access: object,
    *,
    trusted_now: object,
) -> BillingSearchEndpointAccess | None:
    try:
        if type(access) is not BillingSearchEndpointAccess:
            return None
        request, verified_transport = _validated_components(
            access.request,
            access.verified_transport,
            trusted_now=trusted_now,
        )
        supplied_state = object.__getattribute__(
            access,
            "_BillingSearchEndpointAccess__state_hmac",
        )
        expected_state = _state_hmac(request, verified_transport)
        if type(supplied_state) is not bytes or not hmac.compare_digest(
            supplied_state,
            expected_state,
        ):
            return None
        return access
    except Exception:
        return None


def validate_billing_search_endpoint_access(
    access: object,
    *,
    trusted_now: object,
) -> BillingSearchEndpointAccess:
    """Revalidate the complete access state at a fresh server-clock time."""

    validated = _validated_access_or_none(access, trusted_now=trusted_now)
    del access, trusted_now
    if validated is None:
        raise _fail()
    return validated


def _authorized_endpoint_or_none(
    parameters: Mapping[str, Any],
    headers: Mapping[str, Any],
    *,
    method: object,
    path: object,
    trusted_now: object,
    environment_map: Mapping[str, str] | None = None,
    keyring: BillingSearchTransportKeyring | None = None,
) -> BillingSearchEndpointAccess | None:
    """Return sealed access or no value after containing boundary failures."""

    request = binding = verified_transport = access = None
    context_header = key_id_header = signature_header = None
    try:
        request = parse_billing_search_request(parameters)
        canonical_now = _canonical_utc(trusted_now)[0]
        binding = BillingSearchTransportRequestBinding(
            method=method,
            path=path,
            query_pairs=request.query_pairs,
            plan_release_id=request.plan_release_id,
            trusted_now=canonical_now,
        )
        context_header, key_id_header, signature_header = _closed_header_values(headers)
        keyring = _request_keyring(keyring, environment_map)
        verified_transport = verify_billing_search_transport(
            context_header,
            key_id_header,
            signature_header,
            keyring=keyring,
            binding=binding,
        )
        access = _new_access(request, verified_transport)
        return validate_billing_search_endpoint_access(
            access,
            trusted_now=canonical_now,
        )
    except Exception:
        return None
    finally:
        del (
            access,
            binding,
            context_header,
            environment_map,
            headers,
            key_id_header,
            keyring,
            method,
            parameters,
            path,
            request,
            signature_header,
            trusted_now,
            verified_transport,
        )


def authorize_billing_search_endpoint(
    parameters: Mapping[str, Any],
    headers: Mapping[str, Any],
    *,
    method: object,
    path: object,
    trusted_now: object,
    environment_map: Mapping[str, str] | None = None,
    keyring: BillingSearchTransportKeyring | None = None,
) -> BillingSearchEndpointAccess:
    """Authenticate and authorize one exact gateway-normalized GET request."""

    access = _authorized_endpoint_or_none(
        parameters,
        headers,
        method=method,
        path=path,
        trusted_now=trusted_now,
        environment_map=environment_map,
        keyring=keyring,
    )
    del (
        environment_map,
        headers,
        keyring,
        method,
        parameters,
        path,
        trusted_now,
    )
    if access is None:
        raise _fail()
    return access


__all__ = [
    "BillingSearchEndpointAccess",
    "BillingSearchEndpointAccessError",
    "authorize_billing_search_endpoint",
    "validate_billing_search_endpoint_access",
]
