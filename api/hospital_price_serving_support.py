# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Small fail-closed helpers for public packed hospital-price responses."""

from __future__ import annotations

from typing import Any, Mapping

import orjson


MAX_HOSPITAL_PRICE_PUBLIC_BYTES = 2 << 20
HOSPITAL_PRICE_PUBLIC_DATA_BYTES = MAX_HOSPITAL_PRICE_PUBLIC_BYTES - (64 << 10)


class HospitalPriceServingUnavailableError(RuntimeError):
    """Fail closed when packed serving evidence cannot be trusted."""


def consume_public_bytes(byte_budget: list[int] | None, value: object) -> None:
    """Fail before retaining decoded rows that cannot fit the public response."""

    if byte_budget is None:
        return
    try:
        byte_budget[0] -= len(orjson.dumps(value))
    except TypeError:
        raise HospitalPriceServingUnavailableError(
            "hospital price public payload is invalid"
        ) from None
    if byte_budget[0] < 0:
        raise HospitalPriceServingUnavailableError(
            "hospital price public payload exceeds its bound"
        )


def validate_payer_page_coverage(
    selector_records: tuple[Mapping[str, Any], ...],
    page_indexes: list[int],
    range_count: int,
) -> None:
    """Require a contiguous stored selector-page segment for each fact range."""

    if len(selector_records) != len(page_indexes):
        raise HospitalPriceServingUnavailableError(
            "hospital price payer selector pages are incomplete"
        )
    if selector_records:
        key_page_counts = {
            selector_record.get("key_page_count")
            for selector_record in selector_records
        }
        if (
            len(key_page_counts) != 1
            or type(next(iter(key_page_counts))) is not int
            or next(iter(key_page_counts)) <= 0
            or 0 not in page_indexes
            or any(
                selector_record.get("page_count") not in key_page_counts
                for selector_record in selector_records
            )
        ):
            raise HospitalPriceServingUnavailableError(
                "hospital price payer selector pages are incomplete"
            )
    range_pages = [[] for _index in range(range_count)]
    for selector_record, page_index in zip(selector_records, page_indexes, strict=True):
        range_indexes = selector_record.get("range_indexes")
        if (
            type(range_indexes) not in (list, tuple)
            or list(range_indexes) != sorted(set(range_indexes))
            or any(
                type(range_index) is not int
                or not 0 <= range_index < range_count
                for range_index in range_indexes
            )
        ):
            raise HospitalPriceServingUnavailableError(
                "hospital price payer selector coverage is invalid"
            )
        for range_index in range_indexes:
            range_pages[range_index].append(page_index)
    if any(
        pages != list(range(pages[0], pages[-1] + 1))
        for pages in range_pages if pages
    ):
        raise HospitalPriceServingUnavailableError(
            "hospital price payer selector pages are incomplete"
        )


def public_hospital_price_item(
    service: Mapping[str, Any],
    charge: Mapping[str, Any],
    facts: list[dict[str, Any]],
) -> dict[str, object]:
    """Project source-hidden service, charge, and negotiated-price fields."""

    return {
        "service": {
            field: service.get(field)
            for field in (
                "service_ordinal", "description", "drug_unit", "drug_type", "codes"
            )
        },
        "charge": {
            field: charge.get(field)
            for field in (
                "charge_ordinal", "setting", "billing_class", "modifier_codes",
                "gross_charge", "discounted_cash", "minimum", "maximum",
                "additional_generic_notes",
            )
        },
        "negotiated_prices": [
            {field: value for field, value in fact.items() if field != "charge_key"}
            for fact in facts
        ],
    }
