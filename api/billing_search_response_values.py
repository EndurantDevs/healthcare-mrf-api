# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded scalar validation for public billing-search responses."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
import re

from api.ptg2_billing_price_reader import MAX_PRICE_ATOMS
from api.ptg2_billing_search_contract import serving_unavailable
from api.ptg2_billing_search_result import (
    BillingSearchServiceResult,
    validate_service_result,
)

MAX_PUBLIC_PRICE_ATOMS = MAX_PRICE_ATOMS
MAX_PUBLIC_TOTAL_TEXT_BYTES = 64 * 1024
MAX_PUBLIC_TEXT_BYTES = 4096
MAX_PUBLIC_ARRAY_VALUES = 32
MAX_PUBLIC_ARRAY_MEMBER_BYTES = 64
MAX_PUBLIC_CURSOR_CHARACTERS = 2048
MAX_PUBLIC_NUMERIC_BYTES = 64
PUBLIC_RELEASE_METADATA_FIELDS = (
    "healthporta_plan_id",
    "plan_release_id",
    "plan_version_id",
    "serving_revision_id",
    "release_month",
    "release_status",
    "is_current",
    "binding_set_digest",
)

_PLAIN_DECIMAL_PATTERN = re.compile(
    r"^-?(?:0|[1-9]\d*)(?:\.\d+)?$",
    flags=re.ASCII,
)
_RFC3339_TIMESTAMP_PATTERN = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}" r"(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})",
    flags=re.ASCII,
)


class PublicResponseBudget:
    """Bound exact atoms and all source-derived response text."""

    __slots__ = ("price_atom_count", "source_text_bytes")

    def __init__(self) -> None:
        self.price_atom_count = 0
        self.source_text_bytes = 0

    def retain_price_atom(self) -> None:
        """Consume one exact atom without permitting partial output."""

        self.price_atom_count += 1
        if self.price_atom_count > MAX_PUBLIC_PRICE_ATOMS:
            raise serving_unavailable()

    def retain_text(self, value: str, *, maximum_bytes: int) -> None:
        """Consume one bounded source string and aggregate text budget."""

        encoded_size = len(value.encode("utf-8"))
        self.source_text_bytes += encoded_size
        if (
            encoded_size > maximum_bytes
            or self.source_text_bytes > MAX_PUBLIC_TOTAL_TEXT_BYTES
        ):
            raise serving_unavailable()


def public_text(
    value: object,
    budget: PublicResponseBudget,
    *,
    optional: bool = True,
    maximum_bytes: int = MAX_PUBLIC_TEXT_BYTES,
) -> str | None:
    """Return one bounded printable string or an allowed null."""

    if optional and (value is None or value == ""):
        return None
    if type(value) is not str or not value or not value.isprintable():
        raise serving_unavailable()
    budget.retain_text(value, maximum_bytes=maximum_bytes)
    return value


def public_text_array(
    value: object,
    budget: PublicResponseBudget,
) -> list[str]:
    """Return one bounded array of printable public strings."""

    if type(value) not in {list, tuple} or len(value) > MAX_PUBLIC_ARRAY_VALUES:
        raise serving_unavailable()
    normalized_values = [
        public_text(
            member,
            budget,
            optional=False,
            maximum_bytes=MAX_PUBLIC_ARRAY_MEMBER_BYTES,
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


def validate_public_rate_value(rate_value: object) -> None:
    """Reject rates that cannot become one bounded exact JSON number."""

    if type(rate_value) not in {Decimal, float, int, str} or isinstance(
        rate_value,
        bool,
    ):
        raise serving_unavailable()
    if type(rate_value) is int and abs(rate_value).bit_length() > 512:
        raise serving_unavailable()
    encoded_rate = str(rate_value)
    if (
        not encoded_rate
        or not encoded_rate.isascii()
        or len(encoded_rate) > MAX_PUBLIC_NUMERIC_BYTES
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
    if (
        not decimal_rate.is_finite()
        or _projected_plain_decimal_characters(decimal_rate) > MAX_PUBLIC_NUMERIC_BYTES
    ):
        raise serving_unavailable()


def public_timestamp(
    value: object,
    budget: PublicResponseBudget,
) -> str:
    """Return one bounded timezone-aware RFC 3339 timestamp."""

    retained_timestamp = public_text(
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


def validate_total_text_budget(value: object) -> None:
    """Recheck aggregate response text after public shaping."""

    retained_text_bytes = 0
    pending_values = [value]
    while pending_values:
        current_value = pending_values.pop()
        if type(current_value) is str:
            retained_text_bytes += len(current_value.encode("utf-8"))
            if retained_text_bytes > MAX_PUBLIC_TOTAL_TEXT_BYTES:
                raise serving_unavailable()
        elif type(current_value) is dict:
            pending_values.extend(current_value.values())
        elif type(current_value) in {list, tuple}:
            pending_values.extend(current_value)


def validated_response_page(
    service_result: BillingSearchServiceResult,
    next_cursor: object,
) -> tuple[BillingSearchServiceResult, str | None]:
    """Bind a central sealed cursor to one validated service page."""

    result = validate_service_result(service_result)
    provider_count = len(result.providers)
    if provider_count > result.request.limit or (
        result.has_more and provider_count != result.request.limit
    ):
        raise serving_unavailable()
    if not result.has_more:
        if next_cursor is not None:
            raise serving_unavailable()
        return result, None
    if (
        type(next_cursor) is not str
        or not 1 <= len(next_cursor) <= MAX_PUBLIC_CURSOR_CHARACTERS
        or not next_cursor.isascii()
        or not next_cursor.isprintable()
    ):
        raise serving_unavailable()
    return result, next_cursor


__all__ = [
    "MAX_PUBLIC_PRICE_ATOMS",
    "PUBLIC_RELEASE_METADATA_FIELDS",
    "PublicResponseBudget",
    "public_text",
    "public_text_array",
    "public_timestamp",
    "validate_public_rate_value",
    "validate_total_text_budget",
    "validated_response_page",
]
