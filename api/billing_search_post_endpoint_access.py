# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed Python HTTP trust boundary for billing-search POST requests."""

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
from api.billing_search_post_gateway_transport import (
    BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER,
    BILLING_SEARCH_TRANSPORT_KEY_ID_HEADER,
    BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER,
    VerifiedBillingSearchPostTransport,
    billing_search_plan_entitlement_sha256,
    validate_billing_search_post_verified_transport,
    verify_billing_search_post_transport,
)
from api.billing_search_post_request import (
    BillingSearchPostRequest,
    validate_billing_search_post_request,
)
from api.billing_search_post_transport import (
    BillingSearchPostTransportError,
    parse_billing_search_post_transport,
)
from api.billing_search_transport_keys import (
    BillingSearchTransportKeyring,
    load_billing_search_transport_keyring,
)

_STATE_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_POST_ENDPOINT_ACCESS_V1\x00"
_STATE_AUTH_KEY = secrets.token_bytes(32)
_HEADER_PREFIX = "x-healthporta-billing-search-"
_HEADER_NAMES = (
    BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER,
    BILLING_SEARCH_TRANSPORT_KEY_ID_HEADER,
    BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER,
)
_ALLOWED_HEADER_NAMES = frozenset(name.lower() for name in _HEADER_NAMES)
_INVALID = "billing_search_post_endpoint_access_invalid"
_REDACTED = "<redacted-billing-search-post-endpoint-access>"


class BillingSearchPostEndpointAccessError(RuntimeError):
    """Value-free authenticated-boundary failure."""


def _fail() -> BillingSearchPostEndpointAccessError:
    return BillingSearchPostEndpointAccessError(_INVALID)


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
    header_values: list[str] = []
    for header_name in _HEADER_NAMES:
        matching_values = [
            listed_header_value
            for listed_header_name, listed_header_value in header_items
            if isinstance(listed_header_name, str)
            and listed_header_name.lower() == header_name.lower()
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
        exact_header_value = matching_values[0]
        if (
            type(exact_header_value) is not str
            or not exact_header_value
            or exact_header_value != exact_header_value.strip()
            or not exact_header_value.isascii()
            or not exact_header_value.isprintable()
        ):
            raise _fail()
        header_values.append(exact_header_value)
    return header_values[0], header_values[1], header_values[2]


def _request_keyring(
    keyring: BillingSearchTransportKeyring | None,
    environment_map: Mapping[str, str] | None,
) -> BillingSearchTransportKeyring:
    if keyring is None:
        return load_billing_search_transport_keyring(
            os.environ if environment_map is None else environment_map
        )
    if environment_map is not None:
        raise _fail()
    return keyring


def _state_hmac(
    request: BillingSearchPostRequest,
    transport: VerifiedBillingSearchPostTransport,
) -> bytes:
    encoded = json.dumps(
        {
            "authorization_context_sha256": (
                transport.authorization_context.context_sha256
            ),
            "metering_request_id": transport.metering_request_id,
            "plan_release_id": transport.plan_release_id,
            "request_shape_sha256": request.request_shape_sha256,
            "transport_request_shape_sha256": transport.request_shape_sha256,
            "transport_trusted_now": transport.trusted_now,
            "transport_state_sha256": transport.verified_state_sha256,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    message = b"".join(
        (
            _STATE_DOMAIN,
            id(request).to_bytes(16, "big"),
            id(transport).to_bytes(16, "big"),
            len(encoded).to_bytes(8, "big"),
            encoded,
        )
    )
    return hmac.new(_STATE_AUTH_KEY, message, hashlib.sha256).digest()


class BillingSearchPostEndpointAccess:
    """Factory-created request plus authenticated gateway authority."""

    __slots__ = ("__request", "__state_hmac", "__transport")

    def __init__(self, *_args, **_kwargs) -> None:
        raise _fail()

    @property
    def request(self) -> BillingSearchPostRequest:
        """Return the validated request while retaining selector redaction."""

        return self.__request

    @property
    def transport(self) -> VerifiedBillingSearchPostTransport:
        """Return the authenticated gateway transport receipt."""

        return self.__transport

    @property
    def authorization_context(self) -> BillingSearchAuthorizationContext:
        """Return the pseudonymous authorization capability context."""

        return self.__transport.authorization_context

    @property
    def plan_release_id(self) -> str:
        """Return the exact plan release authorized by the gateway."""

        return self.__transport.plan_release_id

    def __repr__(self) -> str:
        return _REDACTED

    __str__ = __repr__

    def __setattr__(self, name: str, value: object) -> None:
        del name, value
        raise TypeError(_INVALID)

    def __copy__(self):
        return self

    def __deepcopy__(self, memo: dict[int, object]):
        memo[id(self)] = self
        return self

    def __reduce_ex__(self, protocol: int) -> object:
        del protocol
        raise _fail()


def _new_access(
    request: BillingSearchPostRequest,
    transport: VerifiedBillingSearchPostTransport,
) -> BillingSearchPostEndpointAccess:
    access = object.__new__(BillingSearchPostEndpointAccess)
    object.__setattr__(
        access,
        "_BillingSearchPostEndpointAccess__request",
        request,
    )
    object.__setattr__(
        access,
        "_BillingSearchPostEndpointAccess__transport",
        transport,
    )
    object.__setattr__(
        access,
        "_BillingSearchPostEndpointAccess__state_hmac",
        _state_hmac(request, transport),
    )
    return access


def _validated_access_or_none(
    access: object,
    *,
    trusted_now: object,
) -> BillingSearchPostEndpointAccess | None:
    try:
        if type(access) is not BillingSearchPostEndpointAccess:
            return None
        request = validate_billing_search_post_request(access.request)
        transport = validate_billing_search_post_verified_transport(
            access.transport,
            trusted_now=trusted_now,
        )
        if not hmac.compare_digest(
            request.request_shape_sha256,
            transport.request_shape_sha256,
        ):
            return None
        entitlement = billing_search_plan_entitlement_sha256(transport.plan_release_id)
        require_billing_search_access(
            transport.authorization_context,
            requested_plan_entitlement_sha256=entitlement,
            detailed_provenance=request.include_evidence,
            trusted_now=trusted_now,
        )
        expected_state = _state_hmac(request, transport)
        supplied_state = object.__getattribute__(
            access,
            "_BillingSearchPostEndpointAccess__state_hmac",
        )
        if type(supplied_state) is not bytes or not hmac.compare_digest(
            supplied_state,
            expected_state,
        ):
            return None
        return access
    except Exception:
        return None


def validate_billing_search_post_endpoint_access(
    access: object,
    *,
    trusted_now: object,
) -> BillingSearchPostEndpointAccess:
    """Revalidate one factory-created access object without selector exposure."""

    validated = _validated_access_or_none(
        access,
        trusted_now=trusted_now,
    )
    if validated is None:
        raise _fail()
    return validated


def authorize_billing_search_post_endpoint(
    body_bytes: object,
    headers: Mapping[str, Any],
    *,
    method: object,
    path: object,
    media_type: object,
    trusted_now: object,
    environment_map: Mapping[str, str] | None = None,
    keyring: BillingSearchTransportKeyring | None = None,
) -> BillingSearchPostEndpointAccess:
    """Verify gateway authority, exact bytes, request shape, and capability.

    The caller remains responsible for atomically consuming the authenticated
    metering request ID before serving, because this boundary owns no replay
    store or metering mutation.
    """

    try:
        context_header, key_id_header, signature_header = _closed_header_values(headers)
        keyring = _request_keyring(keyring, environment_map)
        transport = verify_billing_search_post_transport(
            context_header,
            key_id_header,
            signature_header,
            body_bytes=body_bytes,
            keyring=keyring,
            trusted_now=trusted_now,
        )
        request = parse_billing_search_post_transport(
            body_bytes,
            method=method,
            path=path,
            media_type=media_type,
        )
        if not hmac.compare_digest(
            request.request_shape_sha256,
            transport.request_shape_sha256,
        ):
            raise _fail()
        access = _new_access(request, transport)
        validated = _validated_access_or_none(access, trusted_now=trusted_now)
        if validated is None:
            raise _fail()
        return validated
    except BillingSearchPostEndpointAccessError:
        raise
    except BillingSearchPostTransportError:
        raise _fail() from None
    except Exception:
        raise _fail() from None
    finally:
        del body_bytes


__all__ = [
    "BillingSearchPostEndpointAccess",
    "BillingSearchPostEndpointAccessError",
    "authorize_billing_search_post_endpoint",
    "validate_billing_search_post_endpoint_access",
]
