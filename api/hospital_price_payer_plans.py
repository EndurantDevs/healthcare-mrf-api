# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read observed payer/plan pairs without scanning hospital price facts."""

from __future__ import annotations

import asyncio
from typing import Any, Mapping

from api.hospital_price_request import (
    HospitalPayerPlanQuery,
    HospitalPriceInvalidRequestError,
    decode_hospital_price_cursor,
    encode_hospital_price_cursor,
)
from api.hospital_price_serving import (
    _READ_TRANSACTION_SQL,
    _mappings,
    _native_call,
    _validated_version,
)
from api.hospital_price_serving_sql import PAYER_PLAN_CATALOG_SQL, PAYER_PLAN_LEGACY_CATALOG_SQL, VERSION_SQL
from api.hospital_price_serving_support import (
    HOSPITAL_PRICE_PUBLIC_DATA_BYTES,
    HospitalPriceServingUnavailableError,
    consume_public_bytes,
)
from process.hospital_hpt_registry import hospital_hpt_group_ids


def _selector_key_bounds(version: Mapping[str, Any]) -> tuple[int, int]:
    """Bound key ordinals, accounting for legacy interleaved code/payer keys."""

    code_count = version.get("code_selector_key_count")
    payer_count = version.get("payer_plan_selector_key_count")
    if (
        version.get("format_version") not in {1, 2}
        or type(code_count) is not int
        or code_count < 1
        or type(payer_count) is not int
        or payer_count < 0
        or code_count + payer_count >= 1 << 32
        or (payer_count == 0) != (version["fact_count"] == 0)
    ):
        raise HospitalPriceServingUnavailableError("hospital payer-plan root is invalid")
    return 0 if version["format_version"] == 1 else code_count, code_count + payer_count


def _validated_dictionary_keys(
    record_by_field: Mapping[str, Any],
    decoded_by_field: Any,
    format_version: int,
) -> list[Mapping[str, Any]]:
    """Bind decoded exact keys to the authenticated selector block metadata."""

    if not isinstance(decoded_by_field, dict) or not isinstance(decoded_by_field.get("items"), list):
        raise HospitalPriceServingUnavailableError("hospital payer-plan dictionary is invalid")
    keys = decoded_by_field["items"]
    digests = [key.get("key_sha256") for key in keys if isinstance(key, dict)]
    if (
        not keys
        or len(keys) != record_by_field.get("logical_count")
        or len(keys) != len(digests)
        or any(type(digest) is not bytes or len(digest) != 32 for digest in digests)
        or digests != sorted(set(digests))
        or record_by_field.get("page_index") != 0
        or decoded_by_field.get("page_index") != 0
        or decoded_by_field.get("page_count") != record_by_field.get("page_count")
        or type(record_by_field.get("page_count")) is not int
        or record_by_field["page_count"] < 1
        or record_by_field.get("key_sha256") != digests[0]
        or (format_version == 2 and record_by_field.get("parent_sha256") != digests[-1])
        or (format_version == 1 and len(keys) != 1)
        or (record_by_field["page_count"] > 1 and len(keys) != 1)
    ):
        raise HospitalPriceServingUnavailableError("hospital payer-plan dictionary is invalid")
    for key in keys:
        for name in ("payer_name", "plan_name"):
            field_text = key.get(name)
            if name == "plan_name" and name in key and field_text is None:
                continue
            if (
                type(field_text) is not str
                or not field_text
                or field_text != field_text.strip()
                or len(field_text.encode("utf-8")) > 4096
            ):
                raise HospitalPriceServingUnavailableError("hospital payer-plan key is invalid")
    return keys


async def _read_dictionary_slice(
    session: Any,
    version_by_field: Mapping[str, Any],
    next_key: int,
    key_end: int,
    limit: int,
) -> tuple[list[dict[str, Any]], int, bool]:
    """Decode at most one existing 4 MiB selector block for a bounded page."""

    if next_key == key_end:
        return [], next_key - 1, False
    selector_records = _mappings(
        await session.execute(
            PAYER_PLAN_CATALOG_SQL, {"version_id": version_by_field["version_id"], "after_key": next_key - 1}
        )
    )
    if len(selector_records) != 1:
        raise HospitalPriceServingUnavailableError("hospital payer-plan coverage is incomplete")
    record_by_field = selector_records[0]
    first_key, key_count = record_by_field.get("logical_first"), record_by_field.get("logical_count")
    if (
        type(first_key) is not int
        or type(key_count) is not int
        or first_key < version_by_field["code_selector_key_count"]
        or not first_key <= next_key < first_key + key_count <= key_end
    ):
        raise HospitalPriceServingUnavailableError("hospital payer-plan coverage is incomplete")
    decoded_by_field = await _native_call(
        "hospital_price_decode_payer_plan_keys", bytes(record_by_field.get("payload") or b"")
    )
    keys = _validated_dictionary_keys(record_by_field, decoded_by_field, version_by_field["format_version"])
    selected_keys = keys[next_key - first_key : next_key - first_key + limit]
    last_key = next_key + len(selected_keys) - 1
    return _public_dictionary_items(selected_keys), last_key, last_key + 1 < key_end


def _public_dictionary_items(selected_keys: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Strip internal digests and bound the exact observed pair payload."""

    public_items = [
        {"payer_name": key["payer_name"], "plan_name": key["plan_name"], "plan_missing": key["plan_name"] is None}
        for key in selected_keys
    ]
    public_byte_budgets = [HOSPITAL_PRICE_PUBLIC_DATA_BYTES]
    for public_item in public_items:
        consume_public_bytes(public_byte_budgets, public_item)
    return public_items


async def _read_legacy_dictionary_key(
    session: Any,
    version_by_field: Mapping[str, Any],
    next_key: int,
    key_end: int,
) -> tuple[list[dict[str, Any]], int, bool]:
    """Use indexed successor seeks for interleaved legacy selector ordinals."""

    if version_by_field["payer_plan_selector_key_count"] == 0:
        return [], next_key - 1, False
    selector_records = _mappings(
        await session.execute(
            PAYER_PLAN_LEGACY_CATALOG_SQL,
            {"version_id": version_by_field["version_id"], "after_key": next_key - 1},
        )
    )
    if len(selector_records) != 1:
        raise HospitalPriceServingUnavailableError("hospital payer-plan coverage is incomplete")
    record_by_field = selector_records[0]
    ordinal = record_by_field.get("logical_first")
    has_more = record_by_field.get("has_more")
    if (
        type(ordinal) is not int
        or not next_key <= ordinal < key_end
        or record_by_field.get("logical_count") != 1
        or type(has_more) is not bool
        or (has_more and ordinal + 1 == key_end)
    ):
        raise HospitalPriceServingUnavailableError("hospital payer-plan coverage is incomplete")
    decoded_by_field = await _native_call(
        "hospital_price_decode_payer_plan_keys", bytes(record_by_field.get("payload") or b"")
    )
    keys = _validated_dictionary_keys(record_by_field, decoded_by_field, 1)
    return _public_dictionary_items(keys), ordinal, has_more


async def read_hospital_payer_plan_page(
    session: Any,
    query: HospitalPayerPlanQuery,
) -> dict[str, Any]:
    """Return version-bound observed pairs; missing plans remain explicit nulls."""

    hospital_ids = await asyncio.to_thread(hospital_hpt_group_ids, query.hospital_id)
    async with session.begin():
        await session.execute(_READ_TRANSACTION_SQL)
        version_by_field = _validated_version(
            _mappings(
                await session.execute(VERSION_SQL, {"hospital_ids": hospital_ids, "version_id": query.version_id})
            )
        )
        first_key, key_end = _selector_key_bounds(version_by_field)
        after_key = decode_hospital_price_cursor(query, version_by_field["version_id"])
        next_key = first_key if after_key == -1 else after_key + 1
        if not first_key <= next_key <= key_end or (query.cursor and next_key == key_end):
            raise HospitalPriceInvalidRequestError("hospital payer-plan cursor is invalid")
        if version_by_field["format_version"] == 1:
            public_items, last_key, has_more = await _read_legacy_dictionary_key(
                session, version_by_field, next_key, key_end
            )
        else:
            public_items, last_key, has_more = await _read_dictionary_slice(
                session, version_by_field, next_key, key_end, query.limit
            )
    return {
        "hospital_id": query.hospital_id,
        "version": {
            "version_id": version_by_field["version_id"],
            "source_format": version_by_field["source_format"],
            "schema_version": version_by_field["template_version"],
        },
        "pagination": {
            "unit": "payer_plans",
            "limit": query.limit,
            "scanned": len(public_items),
            "next_cursor": encode_hospital_price_cursor(query, version_by_field["version_id"], last_key)
            if has_more
            else None,
        },
        "items": public_items,
    }
