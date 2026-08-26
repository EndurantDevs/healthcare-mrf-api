# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded reads from one immutable packed hospital-price version."""

from __future__ import annotations

import asyncio
import bisect
from typing import Any, Mapping

from sqlalchemy import text

from api.hospital_price_serving_sql import CODE_SELECTOR_SQL
from api.hospital_price_serving_sql import FACT_BLOCK_SQL
from api.hospital_price_serving_sql import PAYER_SELECTOR_SQL
from api.hospital_price_serving_sql import SERVICE_BLOCK_SQL
from api.hospital_price_serving_sql import VERSION_SQL
from api.hospital_price_request import decode_hospital_price_cursor
from api.hospital_price_request import encode_hospital_price_cursor
from api.hospital_price_request import HospitalPriceCursorStaleError
from api.hospital_price_request import HospitalPriceInvalidRequestError
from api.hospital_price_request import HospitalPriceNotFoundError
from api.hospital_price_request import HospitalPriceQuery
from api.hospital_price_request import validate_hospital_price_query
from api.hospital_price_serving_support import consume_public_bytes
from api.hospital_price_serving_support import HOSPITAL_PRICE_PUBLIC_DATA_BYTES
from api.hospital_price_serving_support import HospitalPriceServingUnavailableError
from api.hospital_price_serving_support import MAX_HOSPITAL_PRICE_PUBLIC_BYTES
from api.hospital_price_serving_support import public_hospital_price_item
from api.hospital_price_serving_support import validate_payer_page_coverage
from support.hospital_price_native_validation import (
    HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
)

try:
    import ptg2_address_canon as _NATIVE
except ImportError:
    _NATIVE = None


MAX_HOSPITAL_PRICE_MATCHING_FACTS = 10_000
HOSPITAL_PRICE_CACHE_CONTROL = "private, no-store"
_SOURCE_FORMATS = frozenset({"json", "csv-tall", "csv-wide"})
_NATIVE_FUNCTIONS = (
    "hospital_price_selector_sha256", "hospital_price_decode_selector_page",
    "hospital_price_decode_service_block", "hospital_price_decode_fact_block",
)
_READ_TRANSACTION_SQL = text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")


def _native_module():
    if _NATIVE is None or any(not hasattr(_NATIVE, name) for name in _NATIVE_FUNCTIONS):
        raise HospitalPriceServingUnavailableError(
            "hospital price native reader is unavailable"
        )
    return _NATIVE


async def _native_call(name: str, *args: object):
    try:
        return await asyncio.to_thread(getattr(_native_module(), name), *args)
    except HospitalPriceServingUnavailableError:
        raise
    except Exception:
        raise HospitalPriceServingUnavailableError(
            "hospital price packed block is invalid"
        ) from None


def _mappings(result: Any) -> tuple[Mapping[str, Any], ...]:
    try:
        return tuple(result.mappings().all())
    except (AttributeError, TypeError):
        raise HospitalPriceServingUnavailableError(
            "hospital price database response is invalid"
        ) from None


def _validated_version(
    version_records_by_field: tuple[Mapping[str, Any], ...],
) -> Mapping[str, Any]:
    if not version_records_by_field:
        raise HospitalPriceNotFoundError("hospital price resource is unavailable")
    if len(version_records_by_field) != 1:
        raise HospitalPriceServingUnavailableError("hospital price version is ambiguous")
    version_record_by_field = version_records_by_field[0]
    expected_counts = (
        version_record_by_field.get("version_service_count"),
        version_record_by_field.get("version_charge_count"),
        version_record_by_field.get("version_fact_count"),
    )
    if (
        type(version_record_by_field.get("version_id")) is not str
        or len(version_record_by_field["version_id"]) != 64
        or any(
            character not in "0123456789abcdef"
            for character in version_record_by_field["version_id"]
        )
        or version_record_by_field.get("parser_contract_sha256")
        != HOSPITAL_MRF_PARSER_CONTRACT_SHA256
        or version_record_by_field.get("source_format") not in _SOURCE_FORMATS
        or version_record_by_field.get("template_version") != "3.0.0"
        or version_record_by_field.get("format_version") != 1
        or expected_counts != (
            version_record_by_field.get("service_count"),
            version_record_by_field.get("charge_count"),
            version_record_by_field.get("fact_count"),
        )
        or expected_counts != (
            version_record_by_field.get("current_service_count"),
            version_record_by_field.get("current_charge_count"),
            version_record_by_field.get("current_fact_count"),
        )
        or any(type(count) is not int or count < 0 for count in expected_counts)
        or expected_counts[0] < 1 or expected_counts[1] < 1
    ):
        raise HospitalPriceServingUnavailableError(
            "hospital price packed version is invalid"
        )
    return version_record_by_field


def _validated_selector_page(
    selector_record: Mapping[str, Any],
    decoded_page_by_field: object,
) -> tuple[list[int], int, int, bool]:
    if type(decoded_page_by_field) is not dict:
        raise HospitalPriceServingUnavailableError(
            "hospital price selector metadata is invalid"
        )
    selected_refs = list(decoded_page_by_field.get("refs", ()))
    page_index = decoded_page_by_field.get("page_index")
    page_count = decoded_page_by_field.get("page_count")
    if (
        type(page_index) is not int or page_index < 0
        or type(page_count) is not int or page_count <= page_index
        or page_index != selector_record.get("page_index")
        or page_count != selector_record.get("page_count")
        or decoded_page_by_field.get("ref_count")
        != selector_record.get("secondary_count")
        or decoded_page_by_field.get("first_ref")
        != selector_record.get("secondary_first")
        or type(decoded_page_by_field.get("truncated")) is not bool
        or any(type(reference) is not int or reference < 0 for reference in selected_refs)
    ):
        raise HospitalPriceServingUnavailableError(
            "hospital price selector metadata is invalid"
        )
    return selected_refs, page_index, page_count, decoded_page_by_field["truncated"]


async def _selector_refs(
    selector_records: tuple[Mapping[str, Any], ...],
    kind: str,
    first: str,
    second: str,
    ranges: list[tuple[int, int]],
    max_refs: int,
) -> tuple[list[int], bool, list[int], int | None]:
    """Decode bounded selector references while checking stored metadata."""

    selected_refs: list[int] = []
    is_truncated = False
    page_indexes: list[int] = []
    page_count = None
    for selector_record in selector_records:
        remaining = max_refs - len(selected_refs)
        if remaining <= 0:
            is_truncated = True
            break
        decoded_page_by_field = await _native_call(
            "hospital_price_decode_selector_page",
            bytes(selector_record.get("payload") or b""), kind, first, second,
            ranges, remaining,
        )
        page_refs, page_index, decoded_page_count, page_is_truncated = (
            _validated_selector_page(selector_record, decoded_page_by_field)
        )
        if page_count is None:
            page_count = decoded_page_count
        elif page_count != decoded_page_count:
            raise HospitalPriceServingUnavailableError(
                "hospital price selector metadata is invalid"
            )
        page_indexes.append(page_index)
        selected_refs.extend(page_refs)
        if page_is_truncated:
            is_truncated = True
            break
    if selected_refs != sorted(set(selected_refs)):
        raise HospitalPriceServingUnavailableError(
            "hospital price selector references are invalid"
        )
    range_starts = [range_start for range_start, _range_end in ranges]
    for reference in selected_refs:
        range_index = bisect.bisect_right(range_starts, reference) - 1
        if range_index < 0 or reference >= ranges[range_index][1]:
            raise HospitalPriceServingUnavailableError(
                "hospital price selector references are invalid"
            )
    if page_indexes != sorted(set(page_indexes)):
        raise HospitalPriceServingUnavailableError(
            "hospital price selector pages are invalid"
        )
    return selected_refs, is_truncated, page_indexes, page_count


async def _charge_page(
    session: Any,
    query: HospitalPriceQuery,
    version_id: str,
    after_key: int,
) -> tuple[list[int], bool]:
    native = _native_module()
    key_sha256 = bytes(native.hospital_price_selector_sha256(
        "code", query.code_type, query.code
    ))
    code_selector_records = _mappings(await session.execute(
        CODE_SELECTOR_SQL,
        {"version_id": version_id, "key_sha256": key_sha256, "after_key": after_key},
    ))
    selected_refs, is_truncated, page_indexes, page_count = await _selector_refs(
        code_selector_records, "code", query.code_type, query.code,
        [(after_key + 1, 1 << 32)], query.limit + 1,
    )
    if code_selector_records and (
        page_indexes != list(range(page_indexes[0], page_indexes[-1] + 1))
        or (after_key < 0 and page_indexes[0] != 0)
        or (
            after_key >= 0
            and code_selector_records[0].get("secondary_first") > after_key
        )
    ):
        raise HospitalPriceServingUnavailableError(
            "hospital price selector pages are incomplete"
        )
    candidates = selected_refs[: query.limit]
    has_more = is_truncated or len(selected_refs) > query.limit
    if page_indexes and not has_more:
        has_more = page_indexes[-1] + 1 < int(page_count)
    if has_more and not candidates:
        raise HospitalPriceServingUnavailableError(
            "hospital price selector continuation is incomplete"
        )
    return candidates, has_more


def _validated_service_block(record: Mapping[str, Any], services: object):
    if type(services) is not list or len(services) != record.get("logical_count"):
        raise HospitalPriceServingUnavailableError("hospital price service block is invalid")
    charges = [charge for service in services for charge in service.get("charges", ())]
    charge_keys = [charge.get("charge_key") for charge in charges]
    first = record.get("secondary_first")
    count = record.get("secondary_count")
    if (
        not services or services[0].get("service_ordinal") != record.get("logical_first")
        or type(first) is not int or type(count) is not int
        or charge_keys != list(range(first, first + count))
    ):
        raise HospitalPriceServingUnavailableError("hospital price service block is invalid")
    return services


async def _charges_by_key(
    session: Any,
    version_id: str,
    charge_keys: list[int],
    code_type: str,
    code: str,
    public_byte_budget: list[int] | None = None,
):
    if not charge_keys:
        return {}
    service_block_records = _mappings(await session.execute(
        SERVICE_BLOCK_SQL, {"version_id": version_id, "charge_keys": charge_keys},
    ))
    charge_by_key: dict[int, tuple[dict[str, Any], dict[str, Any]]] = {}
    for service_block_record in service_block_records:
        decoded_services = _validated_service_block(service_block_record, await _native_call(
            "hospital_price_decode_service_block",
            bytes(service_block_record.get("payload") or b"")
        ))
        for service in decoded_services:
            service_metadata_by_field = {
                field: service.get(field)
                for field in (
                    "service_ordinal", "description", "drug_unit", "drug_type", "codes"
                )
            }
            selected_charges = [
                charge for charge in service["charges"]
                if charge["charge_key"] in charge_keys
            ]
            if selected_charges and not any(
                code_by_field.get("code_type") == code_type
                and code_by_field.get("code") == code
                for code_by_field in service_metadata_by_field["codes"]
            ):
                raise HospitalPriceServingUnavailableError(
                    "hospital price charge selector identity is invalid"
                )
            for charge in selected_charges:
                consume_public_bytes(
                    public_byte_budget,
                    {"service": service_metadata_by_field, "charge": charge},
                )
                charge_by_key[charge["charge_key"]] = (
                    service_metadata_by_field, charge
                )
    if set(charge_by_key) != set(charge_keys):
        raise HospitalPriceServingUnavailableError(
            "hospital price charge coverage is incomplete"
        )
    return charge_by_key


def _fact_ranges(charges_by_key, charge_keys: list[int]):
    ranges = []
    previous_end = 0
    for charge_key in charge_keys:
        charge = charges_by_key[charge_key][1]
        first, count = charge.get("first_fact_ordinal"), charge.get("fact_count")
        if (
            type(first) is not int or type(count) is not int
            or first < previous_end or count < 0 or first + count >= 1 << 63
        ):
            raise HospitalPriceServingUnavailableError(
                "hospital price fact range is invalid"
            )
        if count:
            ranges.append((first, first + count, charge_key))
        previous_end = first + count
    return ranges


async def _selected_fact_ordinals(
    session: Any,
    query: HospitalPriceQuery,
    version_id: str,
    ranges: list[tuple[int, int, int]],
) -> dict[int, int]:
    if not ranges or query.payer_name is None or query.plan_name is None:
        return {}
    native = _native_module()
    key_sha256 = bytes(native.hospital_price_selector_sha256(
        "payer_plan", query.payer_name, query.plan_name
    ))
    payer_selector_records = _mappings(await session.execute(
        PAYER_SELECTOR_SQL,
        {
            "version_id": version_id, "key_sha256": key_sha256,
            "fact_starts": [fact_range[0] for fact_range in ranges],
            "fact_ends": [fact_range[1] for fact_range in ranges],
        },
    ))
    selected_refs, is_truncated, page_indexes, _page_count = await _selector_refs(
        payer_selector_records, "payer_plan", query.payer_name, query.plan_name,
        [(fact_range[0], fact_range[1]) for fact_range in ranges],
        MAX_HOSPITAL_PRICE_MATCHING_FACTS + 1,
    )
    if is_truncated or len(selected_refs) > MAX_HOSPITAL_PRICE_MATCHING_FACTS:
        raise HospitalPriceServingUnavailableError(
            "hospital price matching fact fanout exceeds its bound"
        )
    validate_payer_page_coverage(
        payer_selector_records, page_indexes, len(ranges)
    )
    starts = [fact_range[0] for fact_range in ranges]
    charge_by_fact: dict[int, int] = {}
    for reference in selected_refs:
        index = bisect.bisect_right(starts, reference) - 1
        charge_by_fact[reference] = ranges[index][2]
    return charge_by_fact


async def _facts_by_charge(
    session: Any,
    query: HospitalPriceQuery,
    version_id: str,
    charge_by_fact: dict[int, int],
    public_byte_budget: list[int] | None = None,
) -> dict[int, list[dict[str, Any]]]:
    if not charge_by_fact:
        return {}
    fact_ordinals = sorted(charge_by_fact)
    fact_block_records = _mappings(await session.execute(
        FACT_BLOCK_SQL,
        {"version_id": version_id, "fact_ordinals": fact_ordinals},
    ))
    fact_by_ordinal: dict[int, dict[str, Any]] = {}
    for fact_block_record in fact_block_records:
        facts = await _native_call(
            "hospital_price_decode_fact_block",
            bytes(fact_block_record.get("payload") or b"")
        )
        first = fact_block_record.get("logical_first")
        count = fact_block_record.get("logical_count")
        if type(facts) is not list or type(first) is not int or len(facts) != count:
            raise HospitalPriceServingUnavailableError("hospital price fact block is invalid")
        start = bisect.bisect_left(fact_ordinals, first)
        stop = bisect.bisect_left(fact_ordinals, first + count)
        for fact_ordinal in fact_ordinals[start:stop]:
            fact = facts[fact_ordinal - first]
            expected_charge = charge_by_fact[fact_ordinal]
            if (
                fact.get("charge_key") != expected_charge
                or fact.get("payer_name") != query.payer_name
                or fact.get("plan_name") != query.plan_name
            ):
                raise HospitalPriceServingUnavailableError(
                    "hospital price fact identity is invalid"
                )
            consume_public_bytes(public_byte_budget, fact)
            fact_by_ordinal[fact_ordinal] = fact
    if set(fact_by_ordinal) != set(fact_ordinals):
        raise HospitalPriceServingUnavailableError(
            "hospital price fact coverage is incomplete"
        )
    facts_by_charge_key: dict[int, list[dict[str, Any]]] = {}
    for ordinal in fact_ordinals:
        facts_by_charge_key.setdefault(charge_by_fact[ordinal], []).append(
            fact_by_ordinal[ordinal]
        )
    return facts_by_charge_key


async def read_hospital_price_page(session: Any, query: HospitalPriceQuery) -> dict[str, Any]:
    """Return one source-version-bound page whose pagination unit is charges."""

    _native_module()
    public_byte_budgets = [HOSPITAL_PRICE_PUBLIC_DATA_BYTES]
    async with session.begin():
        await session.execute(_READ_TRANSACTION_SQL)
        version = _validated_version(_mappings(await session.execute(
            VERSION_SQL,
            {"hospital_id": query.hospital_id, "version_id": query.version_id},
        )))
        version_id = str(version["version_id"])
        after_key = decode_hospital_price_cursor(query, version_id)
        charge_keys, has_more = await _charge_page(
            session, query, version_id, after_key
        )
        charges_by_key = await _charges_by_key(
            session, version_id, charge_keys, query.code_type, query.code,
            public_byte_budgets,
        )
        facts_by_charge = await _facts_by_charge(
            session, query, version_id,
            await _selected_fact_ordinals(
                session, query, version_id,
                _fact_ranges(charges_by_key, charge_keys),
            ),
            public_byte_budgets,
        )
    has_payer_filter = query.payer_name is not None
    public_items = [
        public_hospital_price_item(
            *charges_by_key[key], facts_by_charge.get(key, [])
        )
        for key in charge_keys
        if not has_payer_filter or key in facts_by_charge
    ]
    next_cursor = (
        encode_hospital_price_cursor(query, version_id, charge_keys[-1])
        if has_more and charge_keys else None
    )
    return {
        "hospital_id": query.hospital_id,
        "version": {
            "version_id": version_id,
            "source_format": version["source_format"],
            "schema_version": version["template_version"],
        },
        "query": {
            "code_type": query.code_type, "code": query.code,
            "payer_name": query.payer_name, "plan_name": query.plan_name,
            "negotiated_prices_requested": has_payer_filter,
        },
        "pagination": {
            "unit": "charges", "limit": query.limit,
            "scanned": len(charge_keys), "next_cursor": next_cursor,
        },
        "items": public_items,
    }
