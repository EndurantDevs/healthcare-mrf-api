# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded scalar and cursor validation for public billing-search responses."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
import hmac
import re

from api.billing_search_cursor import (
    BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS,
    BillingSearchCursorKeyring,
    BillingSearchSealedPageCursor,
)
from api.billing_search_cursor_authentication import (
    authenticate_billing_search_sealed_page_cursor,
)
from api.billing_search_endpoint_access import BillingSearchEndpointAccess
from api.billing_search_pagination import (
    BillingSearchCursorBinding,
    billing_search_authorization_scope_sha256,
    billing_search_snapshot_set_sha256,
)
from api.billing_search_transport_contract import _canonical_utc
from api.plan_release_serving import PlanReleaseServingSelection
from api.ptg2_billing_search_contract import (
    BillingSearchServiceResult,
    serving_unavailable,
)
from api.ptg2_billing_search_page import validate_billing_search_sort_key

_MAX_PUBLIC_PRICE_ATOMS = 256
_MAX_PUBLIC_TOTAL_TEXT_BYTES = 64 * 1024
_MAX_PUBLIC_TEXT_BYTES = 1024
_MAX_PUBLIC_ARRAY_VALUES = 32
_MAX_PUBLIC_ARRAY_MEMBER_BYTES = 16
_MAX_PUBLIC_NUMERIC_BYTES = 64
_PLAIN_DECIMAL_PATTERN = re.compile(
    r"^-?(?:0|[1-9]\d*)(?:\.\d+)?$",
    flags=re.ASCII,
)
_RFC3339_TIMESTAMP_PATTERN = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})",
    flags=re.ASCII,
)


class _PublicResponseBudget:
    __slots__ = ("price_atom_count", "source_text_bytes")

    def __init__(self) -> None:
        self.price_atom_count = 0
        self.source_text_bytes = 0

    def retain_price_atom(self) -> None:
        """Consume one exact atom without permitting partial output."""

        self.price_atom_count += 1
        if self.price_atom_count > _MAX_PUBLIC_PRICE_ATOMS:
            raise serving_unavailable()

    def retain_text(self, value: str, *, maximum_bytes: int) -> None:
        """Consume one bounded source string and the aggregate text budget."""

        encoded_size = len(value.encode("utf-8"))
        self.source_text_bytes += encoded_size
        if (
            encoded_size > maximum_bytes
            or self.source_text_bytes > _MAX_PUBLIC_TOTAL_TEXT_BYTES
        ):
            raise serving_unavailable()


def _public_text(
    value: object,
    budget: _PublicResponseBudget,
    *,
    optional: bool = True,
    maximum_bytes: int = _MAX_PUBLIC_TEXT_BYTES,
) -> str | None:
    if optional and (value is None or value == ""):
        return None
    if type(value) is not str or not value or not value.isprintable():
        raise serving_unavailable()
    budget.retain_text(value, maximum_bytes=maximum_bytes)
    return value


def _public_text_array(
    value: object,
    budget: _PublicResponseBudget,
) -> list[str]:
    if type(value) not in {list, tuple} or len(value) > _MAX_PUBLIC_ARRAY_VALUES:
        raise serving_unavailable()
    normalized_values = [
        _public_text(
            member,
            budget,
            optional=False,
            maximum_bytes=_MAX_PUBLIC_ARRAY_MEMBER_BYTES,
        )
        for member in value
    ]
    if any(member is None for member in normalized_values):
        raise serving_unavailable()
    return [str(member) for member in normalized_values]


def _projected_plain_decimal_characters(decimal_rate: Decimal) -> int:
    decimal_tuple = decimal_rate.as_tuple()
    digit_count = len(decimal_tuple.digits)
    exponent = decimal_tuple.exponent
    if type(exponent) is not int:
        raise serving_unavailable()
    if exponent >= 0:
        character_count = digit_count + exponent
    elif digit_count + exponent > 0:
        character_count = digit_count + 1
    else:
        character_count = 2 - (digit_count + exponent) + digit_count
    return character_count + int(bool(decimal_tuple.sign))


def _validate_public_rate_value(rate_value: object) -> None:
    """Reject rates that cannot become one bounded exact JSON number."""

    if type(rate_value) not in {Decimal, float, int, str} or isinstance(
        rate_value, bool
    ):
        raise serving_unavailable()
    if type(rate_value) is int and abs(rate_value).bit_length() > 512:
        raise serving_unavailable()
    encoded_rate = str(rate_value)
    if (
        not encoded_rate
        or not encoded_rate.isascii()
        or len(encoded_rate) > _MAX_PUBLIC_NUMERIC_BYTES
        or (
            type(rate_value) is str
            and _PLAIN_DECIMAL_PATTERN.fullmatch(encoded_rate) is None
        )
    ):
        raise serving_unavailable()
    try:
        decimal_rate = Decimal(encoded_rate)
    except (InvalidOperation, TypeError, ValueError):
        raise serving_unavailable() from None
    if not decimal_rate.is_finite():
        raise serving_unavailable()
    if _projected_plain_decimal_characters(decimal_rate) > _MAX_PUBLIC_NUMERIC_BYTES:
        raise serving_unavailable()


def _public_timestamp(
    value: object,
    budget: _PublicResponseBudget,
) -> str:
    retained_timestamp = _public_text(
        value,
        budget,
        optional=False,
        maximum_bytes=64,
    )
    if (
        retained_timestamp is None
        or _RFC3339_TIMESTAMP_PATTERN.fullmatch(retained_timestamp) is None
    ):
        raise serving_unavailable()
    parsed_value = (
        retained_timestamp[:-1] + "+00:00"
        if retained_timestamp.endswith("Z")
        else retained_timestamp
    )
    try:
        retrieval_time = datetime.fromisoformat(parsed_value)
    except ValueError:
        raise serving_unavailable() from None
    if retrieval_time.utcoffset() is None:
        raise serving_unavailable()
    return retained_timestamp


def _validate_total_text_budget(value: object) -> None:
    retained_text_bytes = 0
    pending_values = [value]
    while pending_values:
        current_value = pending_values.pop()
        if type(current_value) is str:
            retained_text_bytes += len(current_value.encode("utf-8"))
            if retained_text_bytes > _MAX_PUBLIC_TOTAL_TEXT_BYTES:
                raise serving_unavailable()
        elif type(current_value) is dict:
            pending_values.extend(current_value.values())
        elif type(current_value) in {list, tuple}:
            pending_values.extend(current_value)


def _validated_expected_cursor_coordinates(
    endpoint_access: BillingSearchEndpointAccess,
    service_result: BillingSearchServiceResult,
    *,
    trusted_now: object,
) -> tuple[BillingSearchCursorBinding, int, str, str]:
    request = endpoint_access.request
    _, trusted_time = _canonical_utc(trusted_now)
    trusted_timestamp = int(trusted_time.timestamp())
    cursor_binding = service_result.cursor_binding
    selection = service_result.selection
    if (
        type(cursor_binding) is not BillingSearchCursorBinding
        or type(selection) is not PlanReleaseServingSelection
    ):
        raise serving_unavailable()
    cursor_binding.__post_init__()
    authorization_scope_sha256 = billing_search_authorization_scope_sha256(
        endpoint_access.authorization_context,
        trusted_now=trusted_now,
    )
    snapshot_set_sha256 = billing_search_snapshot_set_sha256(selection)
    if (
        cursor_binding.trusted_now != trusted_timestamp
        or not hmac.compare_digest(
            cursor_binding.request_fingerprint_sha256,
            request.request_fingerprint_sha256,
        )
        or not hmac.compare_digest(
            cursor_binding.authorization_scope_sha256,
            authorization_scope_sha256,
        )
        or not hmac.compare_digest(
            cursor_binding.snapshot_set_sha256,
            snapshot_set_sha256,
        )
    ):
        raise serving_unavailable()
    return (
        cursor_binding,
        trusted_timestamp,
        authorization_scope_sha256,
        snapshot_set_sha256,
    )


def _authenticated_public_cursor(
    endpoint_access: BillingSearchEndpointAccess,
    service_result: BillingSearchServiceResult,
    cursor_keyring: BillingSearchCursorKeyring,
    *,
    trusted_now: object,
) -> str:
    next_cursor = service_result.next_cursor
    if type(next_cursor) is not BillingSearchSealedPageCursor:
        raise serving_unavailable()
    try:
        (
            cursor_binding,
            trusted_timestamp,
            authorization_scope_sha256,
            snapshot_set_sha256,
        ) = _validated_expected_cursor_coordinates(
            endpoint_access,
            service_result,
            trusted_now=trusted_now,
        )
        authenticated_state, authenticated_token = (
            authenticate_billing_search_sealed_page_cursor(
                next_cursor,
                keyring=cursor_keyring,
                trusted_now=trusted_timestamp,
                request_fingerprint_sha256=(
                    endpoint_access.request.request_fingerprint_sha256
                ),
                authorization_context_sha256=authorization_scope_sha256,
                generation_bundle_sha256=(cursor_binding.generation_bundle_sha256),
                snapshot_set_sha256=snapshot_set_sha256,
            )
        )
    except Exception:
        raise serving_unavailable() from None
    if (
        authenticated_state.issued_at != trusted_timestamp
        or authenticated_state.expires_at
        != trusted_timestamp + BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS
        or authenticated_state.sort_key
        != service_result.providers[-1].candidate.sort_key
    ):
        raise serving_unavailable()
    return authenticated_token


def _validate_provider_page_order(
    service_result: BillingSearchServiceResult,
) -> None:
    provider_keys = tuple(
        provider.candidate.sort_key for provider in service_result.providers
    )
    validated_keys = tuple(
        validate_billing_search_sort_key(provider_key) for provider_key in provider_keys
    )
    if provider_keys != validated_keys or provider_keys != tuple(
        sorted(set(provider_keys))
    ):
        raise serving_unavailable()


def _validate_public_page(
    endpoint_access: BillingSearchEndpointAccess,
    service_result: BillingSearchServiceResult,
    *,
    cursor_keyring: BillingSearchCursorKeyring | None,
    trusted_now: object,
) -> str | None:
    """Validate one bounded page and authenticate its optional cursor."""

    provider_count = len(service_result.providers)
    if provider_count > endpoint_access.request.limit or (
        service_result.has_more and provider_count != endpoint_access.request.limit
    ):
        raise serving_unavailable()
    _validate_provider_page_order(service_result)
    if service_result.next_cursor is None:
        return None
    if type(cursor_keyring) is not BillingSearchCursorKeyring:
        raise serving_unavailable()
    return _authenticated_public_cursor(
        endpoint_access,
        service_result,
        cursor_keyring,
        trusted_now=trusted_now,
    )


__all__ = [
    "_MAX_PUBLIC_PRICE_ATOMS",
    "_PublicResponseBudget",
    "_public_text",
    "_public_text_array",
    "_public_timestamp",
    "_validate_public_page",
    "_validate_public_rate_value",
    "_validate_total_text_budget",
]
